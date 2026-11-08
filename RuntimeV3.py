import threading
import time
from decimal import ROUND_DOWN

from bitfinex import APP_VERSION, Bitfinex, BitfinexAmbiguousWriteError, BitfinexApiError, currency_to_symbol
from DomainTypes import WriteOutcome, WriteResult
from MarketDataStream import BitfinexMarketDataHub
from MarketHistory import MarketHistoryService
from V36Validation import validation_status
from Recovery import classify_runtime_error
from StateStore import InsufficientReservedBalance, StaleStrategyVersion
from ExchangeModels import (
    FRR_OFFSET_OFFER_TYPES,
    extract_submitted_offer_id,
    funding_offer_effective_rate,
    parse_book as parse_book_v3,
    parse_credit_rows as parse_credit_rows_v3,
    parse_funding_stats,
    parse_funding_trade_history as parse_funding_trade_rows_v3,
    parse_funding_trades,
    parse_loan_rows as parse_loan_rows_v3,
    parse_offer_rows as parse_offer_rows_v3,
    parse_wallet_rows as parse_wallet_rows_v3,
)
from StrategyV3 import (
    D,
    DUST_REINVEST_MINIMUM,
    EXACT_TERM_EXPLORATION_CURVE,
    EXACT_TERM_EXPLORATION_CURVES,
    EXECUTABLE_QUICK_MODE,
    POOLS,
    SATOSHI,
    StrategyPolicyV3,
    USD_ORDER_CHUNK,
    build_market_signals_v3,
    build_strategy_plan_v3,
    ceil_rate_tick,
    competitive_rate_for_period,
    evenly_distributed_amounts,
    gross_daily_floor,
    json_decimal,
    period_pricing_context,
    pool_for_period,
    policy_v3_with_overrides,
    rate_below_floor,
    replay_strategy_v3,
    validate_policy_v3,
)

CREDIT_DISPLAY_GROUPS = ("short", "medium", "long")
AUTH_FUNDING_HISTORY_LIMIT = 500
AMBIGUOUS_HISTORY_MIN_AGE_MS = 60_000
FUNDING_SUBMISSION_WINDOW_MS = 60_000
MAX_FUNDING_SUBMISSIONS_PER_WINDOW = 60


def order_sizing_payload():
    return {
        "chunkBaseAmount": format(USD_ORDER_CHUNK, "f"),
        "remainderPolicy": "EVENLY_DISTRIBUTE",
        "maxSubmissionsPer60Seconds": MAX_FUNDING_SUBMISSIONS_PER_WINDOW,
    }


def _active_lending_rows(credits, loans):
    """Return provider-side funding exactly once across credit/loan states."""

    merged = {}
    for row in [*(credits or []), *(loans or [])]:
        if str(row.get("currency") or "USD").upper() != "USD":
            continue
        # Bitfinex SIDE=-1 is borrower exposure and is not an asset in this
        # funding wallet. Historical/local rows predate SIDE and default to 0.
        if int(row.get("side") or 0) < 0:
            continue
        row_id = row.get("id", row.get("credit_id"))
        key = str(row_id) if row_id is not None else f"{row.get('funding_state')}:{len(merged)}"
        merged[key] = row
    return list(merged.values())


def _reprice_stage_type(stage, stage_count=6):
    market_stage_count = max(1, int(stage_count) // 2)
    return "FLOOR" if int(stage) > market_stage_count else "MARKET"


def _age_stage_target(stage, chain, benchmark, floor_rate, fallback_anchor=None, stage_count=6):
    stage = int(stage)
    stage_count = int(stage_count)
    market_stage_count = max(1, stage_count // 2)
    floor_rate = D(floor_rate)
    if stage <= market_stage_count:
        fraction = D(stage) / D(market_stage_count)
        origin_rate = D(chain["origin_rate"])
        gap = max(D("0"), origin_rate - D(benchmark))
        return ceil_rate_tick(max(floor_rate, origin_rate - gap * fraction))
    anchor_value = chain.get("market_anchor_rate")
    if anchor_value is None:
        anchor_value = fallback_anchor
    anchor_rate = max(floor_rate, D(anchor_value if anchor_value is not None else chain["origin_rate"]))
    floor_stage = min(market_stage_count, max(1, stage - market_stage_count))
    fraction = D(floor_stage) / D(market_stage_count)
    return ceil_rate_tick(max(floor_rate, anchor_rate - (anchor_rate - floor_rate) * fraction))


def _exploration_age_stage_target(
    stage,
    chain,
    benchmark,
    floor_rate,
    landing_stage,
    stage_count=10,
):
    """Walk an exact-term offer through its market landing and on to the hard floor."""

    floor_rate = D(floor_rate)
    origin_rate = max(floor_rate, D(chain["origin_rate"]))
    landing_value = chain.get("fixed_landing_rate")
    landing_rate = max(
        floor_rate,
        D(landing_value if landing_value is not None else benchmark),
    )
    stage_count = max(1, int(stage_count))
    landing_stage = min(stage_count, max(1, int(landing_stage)))
    progress_stage = min(max(0, int(stage)), stage_count)
    if progress_stage <= landing_stage:
        gap = max(D("0"), origin_rate - landing_rate)
        fraction = D(progress_stage) / D(landing_stage)
        return ceil_rate_tick(max(landing_rate, origin_rate - gap * fraction))
    floor_stage_count = max(1, stage_count - landing_stage)
    floor_progress = progress_stage - landing_stage
    fraction = D(floor_progress) / D(floor_stage_count)
    return ceil_rate_tick(max(floor_rate, landing_rate - (landing_rate - floor_rate) * fraction))


def _credit_display_group(credit):
    pool = str(credit.get("pool") or "").lower()
    if pool in CREDIT_DISPLAY_GROUPS:
        return pool
    period = int(credit.get("period") or 0)
    if period <= 7:
        return "short"
    if period <= 30:
        return "medium"
    return "long"


def build_active_credit_dashboard(credits, total_principal, policy, now_ms):
    """Build JSON-safe active-credit rows and amount-weighted dashboard summaries."""

    now = int(now_ms)
    total_principal = max(D("0"), D(total_principal))
    records = []
    rows = []
    for raw in credits or []:
        if str(raw.get("currency") or "USD").upper() != "USD":
            continue
        if str(raw.get("status") or "").upper() == "CLOSED":
            continue
        amount = abs(D(raw.get("amount") or 0))
        if amount <= 0:
            continue
        effective_rate = D(raw.get("rate_real") if raw.get("rate_real") is not None else raw.get("rate") or 0)
        period = max(0, int(raw.get("period") or 0))
        opening = int(raw.get("mts_opening") or raw.get("mts_created") or 0)
        elapsed_days = None
        contract_end = None
        if opening > 0:
            elapsed_days = D(max(0, now - opening)) / D("86400000")
            contract_end = opening + period * 86_400_000
        hidden = bool(raw.get("hidden"))
        fee_rate = policy.hidden_fee_rate if hidden else policy.normal_fee_rate
        net_daily_income = amount * effective_rate * (D("1") - fee_rate)
        display_group = _credit_display_group(raw)
        row = {
            **raw,
            "displayPool": display_group,
            "effectiveRate": effective_rate,
            "dailyRatePercent": effective_rate * D("100"),
            "feeRatePercent": fee_rate * D("100"),
            "netAprPercent": effective_rate * (D("1") - fee_rate) * D("36500"),
            "elapsedDays": elapsed_days,
            "contractEndAtMs": contract_end,
            "managedByBot": bool(raw.get("managed")),
        }
        rows.append(row)
        records.append(
            {
                "amount": amount,
                "rate": effective_rate,
                "period": D(period),
                "elapsedDays": elapsed_days,
                "netDailyIncome": net_daily_income,
                "displayPool": display_group,
            }
        )

    def summarize(selected):
        principal = sum((record["amount"] for record in selected), D("0"))
        elapsed = [record for record in selected if record["elapsedDays"] is not None]
        elapsed_principal = sum((record["amount"] for record in elapsed), D("0"))
        net_daily_income = sum((record["netDailyIncome"] for record in selected), D("0"))
        return {
            "orderCount": len(selected),
            "principal": principal,
            "utilizationPercent": (None if total_principal <= 0 else principal / total_principal * D("100")),
            "shareOfLentPercent": (
                None if not records else principal / sum((record["amount"] for record in records), D("0")) * D("100")
            ),
            "averageDailyRatePercent": (
                None
                if principal <= 0
                else sum((record["amount"] * record["rate"] for record in selected), D("0")) / principal * D("100")
            ),
            "estimatedNetAprPercent": (None if principal <= 0 else net_daily_income / principal * D("36500")),
            "averageContractDays": (
                None
                if principal <= 0
                else sum((record["amount"] * record["period"] for record in selected), D("0")) / principal
            ),
            "averageElapsedDays": (
                None
                if elapsed_principal <= 0
                else sum(
                    (record["amount"] * record["elapsedDays"] for record in elapsed),
                    D("0"),
                )
                / elapsed_principal
            ),
            "estimatedNetIncomePerDay": net_daily_income,
        }

    overall = summarize(records)
    groups = {
        group: summarize([record for record in records if record["displayPool"] == group])
        for group in CREDIT_DISPLAY_GROUPS
    }
    return json_decimal(
        {
            "credits": rows,
            "summary": {
                "overall": overall,
                "groups": groups,
            },
        }
    )


def _write_result(client, result_method, legacy_method, *args, **kwargs):
    method = getattr(client, result_method, None)
    if method is not None:
        return method(*args, **kwargs)
    try:
        response = getattr(client, legacy_method)(*args, **kwargs)
        return WriteResult(WriteOutcome.CONFIRMED, response=response)
    except BitfinexAmbiguousWriteError as exc:
        return WriteResult(WriteOutcome.UNKNOWN, error=str(exc), category=exc.category)
    except BitfinexApiError as exc:
        return WriteResult(
            WriteOutcome.DEFINITE_REJECT,
            error=str(exc),
            category=exc.category,
            retryable=exc.retryable,
        )


def parse_ledger_rows_v3(rows):
    result = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 9:
            continue
        try:
            result.append(
                {
                    "id": int(row[0]),
                    "currency": str(row[1]).upper(),
                    "wallet": None if row[2] is None else str(row[2]).lower(),
                    "mts": int(row[3]),
                    "amount": D(str(row[5])),
                    "balance": None if row[6] is None else D(str(row[6])),
                    "description": str(row[8] or ""),
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


class LendingRuntimeV3:
    def __init__(
        self,
        client,
        policy,
        store,
        log=None,
        hub=None,
        auto_transfer_wallets=(),
        on_policy_activated=None,
        clock=time.time,
        history_service=None,
    ):
        self.client = client
        self.policy = validate_policy_v3(policy)
        self.store = store
        self.log = log
        self.history = history_service
        if self.history is None and hub is None and isinstance(client, Bitfinex):
            self.history = MarketHistoryService(store, Bitfinex("", ""), clock=clock)
        self.hub = hub or BitfinexMarketDataHub(
            client.api_key,
            client.api_secret,
            symbol="fUSD",
            store=store,
            fallback_seconds=policy.ws_fallback_seconds,
            rest_stale_seconds=policy.rest_stale_seconds,
            history_service=self.history,
        )
        if self.history is not None:
            self.hub.history_service = self.history
        self.auto_transfer_wallets = tuple(str(item).lower() for item in auto_transfer_wallets)
        self.on_policy_activated = on_policy_activated
        self.clock = clock
        self._last_rest_sync_ms = 0
        self._last_history_sync_ms = 0
        self._stats = []
        self._bootstrapped = False
        self._income_sync_stop = threading.Event()
        self._income_sync_thread = None
        self._pending_cancel_requested = set()
        self._last_dust_check_bucket = None
        self._history_status = {"state": "UNAVAILABLE", "usable": False}
        self._validation_status = {"approved": False, "reasons": ["V3.5 mode"]}

    def _market_signals(self, snapshot, policy, now_ms=None):
        window = None if self.history is None else self.history.snapshot(policy, now_ms)
        # LIVE observes the window after REST and after copying the published
        # snapshot. A cycle-start timestamp can predate that snapshot. Explicit
        # times remain authoritative for deterministic callers and replay.
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        self._validation_status = validation_status(self.store, policy)
        requested = policy.quick_pricing_mode == EXECUTABLE_QUICK_MODE
        enabled = requested and self._validation_status["approved"]
        signals = build_market_signals_v3(
            snapshot["book"], snapshot["trades"], self._stats, policy, now,
            window_snapshot=window if enabled else None,
        )
        signals["quickPricingEnabled"] = bool(enabled and signals.get("completeWindowUsable"))
        reason = None
        if not requested:
            reason = "V35_CONFIGURED"
        elif not enabled:
            reason = "VALIDATION_GATE_NOT_PASSED"
        elif not signals.get("completeWindowUsable"):
            reason = (window or {}).get("state", "HISTORY_UNAVAILABLE")
        self._history_status = {
            key: value for key, value in (window or {}).items()
            if key not in {"global", "windows", "periodDemand", "byPeriod", "periodWindows"}
        }
        self._history_status.update(
            inputMode=signals["historyMode"], quickPricingMode=(
                EXECUTABLE_QUICK_MODE if signals["quickPricingEnabled"] else "EXPLORATION_V4"
            ), fallbackReason=reason,
        )
        return signals

    def _log(self, message):
        if self.log is not None:
            self.log.log(message)

    def _reconcile_ambiguous_dust_consolidation(self, active_offer_ids, now_ms):
        """Advance an uncertain dust-cancel after an authoritative snapshot.

        A controlled restart may clear the protected PAUSED mode before this
        reconciliation branch runs.  Do not leave that stale cancellation
        record blocking all later LIVE submissions.  An uncertain replacement
        submission is still protected by its AMBIGUOUS order intent, so it is
        deliberately left untouched here.
        """
        consolidation = self.store.consolidation_status()
        if consolidation.get("state") != "AMBIGUOUS" or consolidation.get("offer_id") is None:
            return
        dust_hash = f"dust-{consolidation.get('started_at_ms')}-{consolidation.get('offer_id')}"
        related = [row for row in self.store.intents() if row.get("plan_hash") == dust_hash]
        if any(row.get("state") == "AMBIGUOUS" for row in related):
            return
        if int(consolidation["offer_id"]) in active_offer_ids:
            self.store.clear_consolidation(now_ms)
        else:
            self.store.update_consolidation("READY", now_ms=now_ms)

    def _close_absent_external_takeover_observations(self, active_offer_ids, now_ms):
        """Close stale two-snapshot candidates after an authoritative read."""

        active_offer_ids = {int(offer_id) for offer_id in active_offer_ids}
        for takeover in self.store.external_takeovers(states={"OBSERVED", "CONFIRMED"}):
            offer_id = int(takeover["offer_id"])
            if offer_id not in active_offer_ids:
                self.store.update_external_takeover(offer_id, "CLOSED", now_ms=now_ms)

    def _write_recovery_status(self):
        """Expose every durable write blocker through one status payload.

        The persistence model intentionally remains specialised (intents,
        consolidation and external takeover), but callers must not need to
        understand those stores to decide whether a LIVE cycle can write.
        """
        blockers = []
        recovery = self.store.recovery_status()
        if recovery.get("active"):
            blockers.append(
                {
                    "kind": "RUNTIME_RECOVERY",
                    "state": "RECOVERING",
                    "reason": recovery.get("reason"),
                    "nextActionAt": recovery.get("nextProbeAt"),
                }
            )
        for intent in self.store.intents(states={"PLANNED", "SUBMITTING", "AMBIGUOUS"}):
            blockers.append(
                {
                    "kind": "SUBMIT",
                    "state": intent["state"],
                    "intentId": intent["id"],
                    "offerId": intent.get("exchange_offer_id"),
                    "reason": intent.get("error_text"),
                    "createdAt": intent.get("created_at_ms"),
                }
            )
        consolidation = self.store.consolidation_status()
        if consolidation.get("state") not in {None, "IDLE"}:
            blockers.append(
                {
                    "kind": "DUST_CONSOLIDATION",
                    "state": consolidation.get("state"),
                    "offerId": consolidation.get("offer_id"),
                    "reason": consolidation.get("last_error"),
                    "createdAt": consolidation.get("started_at_ms"),
                }
            )
        for takeover in self.store.external_takeovers(states={"OBSERVED", "CONFIRMED", "CANCELLING", "AMBIGUOUS"}):
            blockers.append(
                {
                    "kind": "EXTERNAL_TAKEOVER",
                    "state": takeover.get("state"),
                    "offerId": takeover.get("offer_id"),
                    "reason": takeover.get("last_error"),
                    "createdAt": takeover.get("first_seen_ms"),
                }
            )
        blocking_states = {"RECOVERING", "PLANNED", "SUBMITTING", "AMBIGUOUS", "CANCELLING", "CONFIRMED", "READY"}
        for item in blockers:
            item["blocking"] = item.get("state") in blocking_states
        return {
            "canSubmit": not any(item["blocking"] for item in blockers),
            "blockers": blockers,
        }

    @staticmethod
    def _offer_display_type(offer):
        display = str(offer.get("display_type") or "").upper()
        if display:
            return display
        offer_type = str(offer.get("offer_type") or "LIMIT").upper()
        if offer_type == "FRRDELTAFIX":
            return "FRR_DELTA_FIXED"
        if offer_type == "FRRDELTAVAR":
            return "FRR" if D(offer.get("rate") or 0) == 0 else "FRR_DELTA_VARIABLE"
        return offer_type

    @staticmethod
    def _display_type_enabled(policy, display_type):
        return {
            "LIMIT": policy.enable_limit,
            "FRR": policy.enable_frr,
            "FRR_DELTA_FIXED": policy.enable_frr_delta_fixed,
            "FRR_DELTA_VARIABLE": policy.enable_frr_delta_variable,
        }.get(str(display_type).upper(), False)

    def _apply_policy_runtime_settings(self):
        self.hub.fallback_ms = int(self.policy.ws_fallback_seconds) * 1000
        self.hub.rest_stale_ms = int(self.policy.rest_stale_seconds) * 1000

    def bootstrap(self, start_websocket=True):
        if self._bootstrapped:
            return
        recovery = self.store.recover_incomplete_writes()
        if recovery["ambiguousAfterSend"]:
            self._log(f"检测到 {recovery['ambiguousAfterSend']} 个进程中断时未确认的写入；已进入自动对账 PAUSED。")
        if self.store.strategy("ACTIVE") is None:
            self.store.save_strategy(json_decimal(self.policy.__dict__), status="ACTIVE")
        else:
            self.policy = validate_policy_v3(
                policy_v3_with_overrides(StrategyPolicyV3(), self.store.strategy("ACTIVE")["policy"])
            )
        self._apply_policy_runtime_settings()
        self.sync_rest(include_history=True)
        auto_resolved = self.store.reconcile_ambiguous_candidates()
        if auto_resolved:
            self._log(f"已通过唯一 Bitfinex 记录自动恢复 {len(auto_resolved)} 个未知写入。")
        if start_websocket:
            self.hub.start()
        if self.history is not None:
            self.history.snapshot(self.policy)
            self.history.start()
        self.start_income_history_sync()
        self._bootstrapped = True

    def shutdown(self):
        self._income_sync_stop.set()
        if self._income_sync_thread is not None:
            self._income_sync_thread.join(timeout=5)
            self._income_sync_thread = None
        self.hub.stop()
        if self.history is not None:
            self.history.stop(timeout=5)

    def start_income_history_sync(self):
        if self._income_sync_thread is not None and self._income_sync_thread.is_alive():
            return
        self._income_sync_stop.clear()
        self._income_sync_thread = threading.Thread(
            target=self._income_history_worker,
            name="bitfinex-income-history",
            daemon=True,
        )
        self._income_sync_thread.start()

    def _income_history_worker(self):
        """Backfill ledgers without making a statistics outage a trading outage."""
        while not self._income_sync_stop.is_set():
            try:
                state = self.store.income_sync_state("USD")
                self.sync_income_history_once()
                updated = self.store.income_sync_state("USD")
                if updated["status"] == "COMPLETE":
                    wait_seconds = 900
                elif state.get("next_end_ms") == updated.get("next_end_ms"):
                    wait_seconds = 60
                else:
                    # Stay below the documented authenticated endpoint rate limit.
                    wait_seconds = 0.75
            except Exception as exc:
                self.store.update_income_sync_state("USD", status="ERROR", error=str(exc)[:500])
                self._log(f"USD 历史收益同步失败（不影响交易）：{exc}")
                wait_seconds = 60
            self._income_sync_stop.wait(wait_seconds)

    def sync_income_history_once(self, now_ms=None, page_limit=2500):
        """Fetch one historical page or one incremental page; safe to resume."""
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        limit = min(2500, max(1, int(page_limit)))
        state = self.store.income_sync_state("USD")
        if state["status"] == "COMPLETE":
            start = max(0, int(state.get("last_success_ms") or now) - 60_000)
            rows = self.client.ledgers("USD", start=start, end=now, limit=limit, wallet="funding", category=28)
            parsed = [
                row for row in parse_ledger_rows_v3(rows) if row["currency"] == "USD" and row["wallet"] == "funding"
            ]
            self.store.upsert_income_ledgers(parsed, category=28)
            earliest = state.get("earliest_mts")
            if parsed:
                earliest = min([row["mts"] for row in parsed] + ([earliest] if earliest is not None else []))
            return self.store.update_income_sync_state(
                "USD",
                status="COMPLETE",
                earliest_mts=earliest,
                last_success_ms=now,
                error=None,
            )

        end = int(state.get("next_end_ms") or now)
        self.store.update_income_sync_state("USD", status="BACKFILLING", error=None)
        rows = self.client.ledgers("USD", end=end, limit=limit, wallet="funding", category=28)
        parsed = [row for row in parse_ledger_rows_v3(rows) if row["currency"] == "USD" and row["wallet"] == "funding"]
        self.store.upsert_income_ledgers(parsed, category=28)
        earliest = state.get("earliest_mts")
        if parsed:
            page_earliest = min(row["mts"] for row in parsed)
            earliest = page_earliest if earliest is None else min(int(earliest), page_earliest)
        completed = len(rows or []) < limit
        if completed:
            return self.store.update_income_sync_state(
                "USD",
                status="COMPLETE",
                next_end_ms=None,
                earliest_mts=earliest,
                last_success_ms=now,
                completed_at_ms=now,
                error=None,
            )
        if not parsed:
            raise BitfinexApiError("income history page contained no usable USD funding ledgers")
        next_end = min(row["mts"] for row in parsed) - 1
        previous_end = state.get("next_end_ms")
        if previous_end is not None and next_end >= int(previous_end):
            raise BitfinexApiError("income history cursor did not move backwards")
        return self.store.update_income_sync_state(
            "USD",
            status="BACKFILLING",
            next_end_ms=next_end,
            earliest_mts=earliest,
            last_success_ms=now,
            error=None,
        )

    def sync_rest(self, include_history=False, now_ms=None):
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        symbol = currency_to_symbol("USD")
        raw_book = self.client.funding_book(symbol, 250)
        book_observed_at_ms = int(self.clock() * 1000)
        # The public endpoint applies ``limit`` before we build the short market
        # windows.  On busy markets an ascending seven-day query can therefore
        # contain only the oldest part of the range and silently omit the latest
        # five-minute/hour signals.  Fetch newest-first; the parser restores
        # chronological order for signal construction.
        trade_read_due = self.history is None or self.history.public_read_limiter.try_acquire()
        raw_trades = (
            self.client.funding_trades(symbol, start=now - 7 * 86_400_000, end=now, limit=10000, sort=-1)
            if trade_read_due else []
        )
        raw_stats = self.client.funding_stats(symbol, start=now - 7 * 86_400_000, end=now, limit=250)
        raw_wallets = self.client.wallets()
        if self.auto_transfer_wallets and self.store.runtime()["mode"] == "LIVE":
            transferred = False
            for wallet in parse_wallet_rows_v3(raw_wallets):
                if (
                    wallet["wallet_type"] not in self.auto_transfer_wallets
                    or wallet.get("available") is None
                    or wallet["available"] <= 0
                ):
                    continue
                result = _write_result(
                    self.client,
                    "transfer_between_wallets_result",
                    "transfer_between_wallets",
                    wallet["wallet_type"],
                    "funding",
                    "USD",
                    format(wallet["available"], "f"),
                )
                if result.outcome == WriteOutcome.CONFIRMED:
                    transferred = True
                    self._log(f"USD 已从 {wallet['wallet_type']} 钱包自动转入 funding：{result.response}")
                elif result.outcome == WriteOutcome.UNKNOWN:
                    # A complete wallet snapshot makes retrying this sweep
                    # idempotent: an already-transferred source balance is zero.
                    self.store.enter_protected_pause("AMBIGUOUS_WALLET_TRANSFER")
                    raise BitfinexAmbiguousWriteError(result.error)
                else:
                    self._log(f"USD 自动转入被明确拒绝：{result.error}")
            if transferred:
                raw_wallets = self.client.wallets()
        raw_offers = self.client.active_funding_offers(symbol)
        raw_credits = self.client.active_funding_credits(symbol)
        try:
            raw_loans = self.client.active_funding_loans(symbol)
        except AttributeError:
            raw_loans = []
        book = parse_book_v3(raw_book)
        trades = parse_funding_trades(raw_trades)
        self._stats = parse_funding_stats(raw_stats)
        offers = parse_offer_rows_v3(raw_offers)
        credits = parse_credit_rows_v3(raw_credits)
        loans = parse_loan_rows_v3(raw_loans)
        active_lending = _active_lending_rows(credits, loans)
        previously_managed = {int(row["offer_id"]) for row in self.store.offers(active_only=True) if row["managed"]}
        managed_ids = {int(row["offer_id"]) for row in self.store.offers() if row["managed"]}
        for offer in offers:
            offer["managed"] = offer["id"] in managed_ids
            offer["pool"] = pool_for_period(offer["period"])
            offer["display_type"] = self._offer_display_type(offer)
        authoritative_account = self._account(
            {
                "wallets": parse_wallet_rows_v3(raw_wallets),
                "offers": offers,
                "credits": credits,
                "loans": loans,
            }
        )
        active_offer_ids = {int(row["id"]) for row in offers}
        known_credit_ids = {int(row["credit_id"]) for row in self.store.credits()}
        if (previously_managed - active_offer_ids) and any(
            int(row["id"]) not in known_credit_ids for row in active_lending
        ):
            try:
                recent_rows = self.client.funding_trades_history(
                    symbol, start=now - 600_000, end=now, limit=250, sort=1
                )
                self._store_funding_trade_history(parse_funding_trade_rows_v3(recent_rows))
            except (BitfinexApiError, AttributeError) as exc:
                self._log(f"成交归属即时同步失败，将保持待归属并安全重试：{exc}")
        self.store.reconcile_offers(offers, now)
        # A confirmed submit can first appear at the exchange one REST cycle
        # after its intent was written. Refresh ownership from the durable
        # intent binding before external-offer observation so a robot order is
        # never briefly classified as a takeover candidate.
        stored_offer_map = {int(row["offer_id"]): row for row in self.store.offers(active_only=True)}
        for offer in offers:
            stored = stored_offer_map.get(int(offer["id"]))
            if stored is not None:
                offer["managed"] = bool(stored["managed"])
                offer["pool"] = stored.get("pool") or offer.get("pool")
                offer["layer"] = stored.get("layer") or offer.get("layer")
                offer["display_type"] = stored.get("display_type") or offer.get("display_type")
                protection = self.store.manual_protection_for_offer(offer, now)
                if offer["managed"] and not (protection and protection["needsTakeover"]):
                    self.store.discard_unconfirmed_external_takeover(offer["id"])
        takeover_snapshot_safe = (
            authoritative_account["walletAvailableKnown"] and authoritative_account["reconciliationStatus"] == "MATCHED"
        )
        if self.policy.adopt_external_offers and takeover_snapshot_safe:
            self._close_absent_external_takeover_observations(active_offer_ids, now)
            active_strategy = self.store.strategy("ACTIVE")
            strategy_version = active_strategy["version_id"] if active_strategy else "3"
            confirmed_external = []
            for offer in offers:
                protection = self.store.manual_protection_for_offer(offer, now)
                if str(offer.get("currency") or "USD").upper() != "USD":
                    continue
                if offer.get("managed") and not (protection and protection["needsTakeover"]):
                    continue
                takeover = self.store.observe_external_takeover(offer, now)
                if takeover.get("state") == "CONFIRMED":
                    if offer.get("managed"):
                        self.store.queue_manual_retake(offer, now)
                    else:
                        confirmed_external.append(offer)
            adopted = self.store.adopt_external_offers(confirmed_external, strategy_version, now_ms=now)
            if adopted:
                adopted_ids = set(adopted)
                for offer in offers:
                    if int(offer["id"]) in adopted_ids:
                        offer["managed"] = True
                self.store.reconcile_offers(offers, now)
        elif self.policy.adopt_external_offers:
            self.store.reset_unconfirmed_external_takeovers()
        for takeover in self.store.external_takeovers(states={"CANCELLING"}):
            if int(takeover["offer_id"]) not in active_offer_ids:
                self.store.update_external_takeover(takeover["offer_id"], "CLOSED", now_ms=now)
        self._pending_cancel_requested.intersection_update(active_offer_ids)
        self.store.reconcile_credits(active_lending, now)
        stored_credits = {int(row["credit_id"]): row for row in self.store.credits(active_only=True)}
        for credit in active_lending:
            stored = stored_credits.get(int(credit["id"]))
            if stored:
                credit["managed"] = bool(stored["managed"])
                credit["pool"] = stored.get("pool") or pool_for_period(credit["period"])
                credit["layer"] = stored.get("layer")
                credit["display_type"] = stored.get("display_type")
        if not isinstance(self.hub, BitfinexMarketDataHub):
            # Injected adapters need not implement the unified ingestion path.
            self.store.upsert_market_trades(trades)
        self.store.upsert_funding_stats(self._stats)
        self.store.record_book_snapshot(book, source="REST", now_ms=book_observed_at_ms)
        self.hub.apply_rest_snapshot(
            book=book,
            trades=trades,
            wallets=raw_wallets,
            offers=offers,
            credits=credits,
            loans=loans,
            synced_at_ms=now,
        )
        self._last_rest_sync_ms = now
        ambiguous = self.store.intents(states={"AMBIGUOUS"})
        history_complete = self.sync_ambiguous_write_history(ambiguous, now) if ambiguous else True
        if include_history or now - self._last_history_sync_ms >= 900_000:
            self.sync_history(now)
        # Reconcile write uncertainty only after the complete account snapshot is
        # stored. A unique match is safe even if history was temporarily
        # unavailable; confirming absence requires authoritative trade history.
        resolved_submits = self.store.reconcile_ambiguous_candidates(
            confirm_absent=history_complete,
            now_ms=now,
        )
        if resolved_submits:
            # The first reconciliation pass happened before the uncertain intent
            # was bound. Refresh ownership in both durable state and this cycle's
            # in-memory snapshot before LIVE planning resumes.
            self.store.reconcile_offers(offers, now)
            self.store.reconcile_credits(active_lending, now)
            stored_offers = {int(row["offer_id"]): row for row in self.store.offers(active_only=True)}
            for offer in offers:
                stored = stored_offers.get(int(offer["id"]))
                if stored:
                    offer["managed"] = bool(stored["managed"])
                    offer["pool"] = stored.get("pool") or offer.get("pool")
                    offer["layer"] = stored.get("layer")
                    offer["display_type"] = stored.get("display_type") or offer.get("display_type")
            stored_credits = {int(row["credit_id"]): row for row in self.store.credits(active_only=True)}
            for credit in active_lending:
                stored = stored_credits.get(int(credit["id"]))
                if stored:
                    credit["managed"] = bool(stored["managed"])
                    credit["pool"] = stored.get("pool") or credit.get("pool")
                    credit["layer"] = stored.get("layer")
                    credit["display_type"] = stored.get("display_type") or credit.get("display_type")
            self.hub.apply_rest_snapshot(offers=offers, credits=credits, loans=loans, synced_at_ms=now)
        snapshot = self.hub.snapshot(now)
        account = self._account(snapshot)
        current = self.store.runtime()
        if current["mode"] == "PAUSED" and str(current.get("safe_reason") or "").startswith("AMBIGUOUS_CANCEL:"):
            resolved_runtime = self.store.observe_ambiguous_cancel(active_offer_ids, now)
            if not resolved_runtime.get("safe_reason"):
                self._reconcile_ambiguous_dust_consolidation(active_offer_ids, now)
                for takeover in self.store.external_takeovers(states={"AMBIGUOUS"}):
                    offer_id = int(takeover["offer_id"])
                    self.store.update_external_takeover(
                        offer_id,
                        "ADOPTED" if offer_id in active_offer_ids else "CLOSED",
                        now_ms=now,
                    )
        elif account["reconciliationStatus"] == "MATCHED" and not snapshot.get("safeRequired"):
            self._reconcile_ambiguous_dust_consolidation(active_offer_ids, now)
            # Transient data/transport/runtime failures recover after two complete
            # snapshots. Manual ambiguous submits are handled only above.
            self.store.record_consistent_sync(now)
        elif self.store.recovery_status()["active"]:
            reason = (
                "MARKET_DATA_STALE"
                if snapshot.get("safeRequired")
                else "ACCOUNT_AVAILABLE_BALANCE_UNKNOWN"
                if not account["walletAvailableKnown"]
                else "ACCOUNT_RECONCILIATION_MISMATCH"
            )
            self.store.record_recovery_failure(reason, now_ms=now)
        elif current["mode"] == "LIVE":
            reason = (
                "ACCOUNT_AVAILABLE_BALANCE_UNKNOWN"
                if not account["walletAvailableKnown"]
                else "ACCOUNT_RECONCILIATION_MISMATCH"
            )
            self.store.enter_protected_pause(reason)
        return snapshot

    def sync_ambiguous_write_history(self, intents, now_ms=None):
        """Fetch the narrow authoritative history window for uncertain submits.

        The regular 90-day history query is capped and can omit recent records on
        busy accounts. Recovery therefore queries each request's own time window.
        """
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        symbol = currency_to_symbol("USD")
        complete = True
        for intent in intents:
            request_ms = int(intent.get("request_started_at_ms") or intent.get("updated_at_ms") or now)
            start = request_ms - 300_000
            end = min(now, request_ms + 600_000)
            try:
                funding_rows = (
                    self.client.funding_trades_history(
                        symbol,
                        start=start,
                        end=end,
                        limit=AUTH_FUNDING_HISTORY_LIMIT,
                        sort=1,
                    )
                    or []
                )
            except (BitfinexApiError, AttributeError) as exc:
                complete = False
                self._log(f"未知挂单 Funding Trades 对账失败，将保持 PAUSED 并重试：{exc}")
            else:
                self._store_funding_trade_history(parse_funding_trade_rows_v3(funding_rows))
                if len(funding_rows) >= AUTH_FUNDING_HISTORY_LIMIT:
                    complete = False
                    self._log("未知挂单 Funding Trades 达到 500 条上限，无法证明历史窗口完整。")
            try:
                offer_rows = (
                    self.client.funding_offers_history(
                        symbol,
                        start=start,
                        end=end,
                        limit=AUTH_FUNDING_HISTORY_LIMIT,
                    )
                    or []
                )
            except (BitfinexApiError, AttributeError) as exc:
                complete = False
                self._log(f"未知挂单 Funding Offers 对账失败，将保持 PAUSED 并重试：{exc}")
            else:
                self.store.upsert_offer_history(parse_offer_rows_v3(offer_rows))
                if len(offer_rows) >= AUTH_FUNDING_HISTORY_LIMIT:
                    complete = False
                    self._log("未知挂单 Funding Offers 达到 500 条上限，无法证明历史窗口完整。")
            if now - request_ms < AMBIGUOUS_HISTORY_MIN_AGE_MS:
                complete = False
        return complete

    def _store_funding_trade_history(self, parsed_funding):
        with self.store.transaction(immediate=True) as connection:
            for row in parsed_funding:
                managed = (
                    connection.execute(
                        "SELECT 1 FROM order_intents WHERE exchange_offer_id = ?", (row["offer_id"],)
                    ).fetchone()
                    is not None
                )
                connection.execute(
                    """INSERT OR REPLACE INTO funding_trades(
                        trade_id, currency, offer_id, amount, rate, period, mts, managed
                    ) VALUES(?, 'USD', ?, ?, ?, ?, ?, ?)""",
                    (
                        row["id"],
                        row["offer_id"],
                        format(row["amount"], "f"),
                        format(row["rate"], "f"),
                        row["period"],
                        row["mts"],
                        int(managed),
                    ),
                )

    def sync_history(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        symbol = currency_to_symbol("USD")
        start = now - 90 * 86_400_000
        try:
            funding_rows = (
                self.client.funding_trades_history(
                    symbol,
                    start=start,
                    end=now,
                    limit=AUTH_FUNDING_HISTORY_LIMIT,
                    sort=1,
                )
                or []
            )
        except BitfinexApiError:
            funding_rows = []
            funding_history_complete = False
        else:
            funding_history_complete = len(funding_rows) < AUTH_FUNDING_HISTORY_LIMIT
        self._store_funding_trade_history(parse_funding_trade_rows_v3(funding_rows))
        try:
            offer_rows = self.client.funding_offers_history(
                symbol, start=start, end=now, limit=AUTH_FUNDING_HISTORY_LIMIT
            )
        except (BitfinexApiError, AttributeError):
            offer_rows = []
        try:
            credit_rows = self.client.funding_credits_history(
                symbol, start=start, end=now, limit=AUTH_FUNDING_HISTORY_LIMIT
            )
        except (BitfinexApiError, AttributeError):
            credit_rows = []
        self.store.upsert_offer_history(parse_offer_rows_v3(offer_rows))
        self.store.upsert_credit_history(parse_credit_rows_v3(credit_rows))
        self.store.prune_market_data(self.policy.market_retention_days, now)
        self._last_history_sync_ms = now
        return funding_history_complete

    @staticmethod
    def _account(snapshot):
        funding_wallets = [
            row
            for row in snapshot.get("wallets", [])
            if row.get("wallet_type") == "funding" and row.get("currency") == "USD"
        ]
        wallet_available_known = bool(funding_wallets) and all(
            row.get("available") is not None for row in funding_wallets
        )
        wallet = sum((D(row["available"]) for row in funding_wallets), D("0")) if wallet_available_known else D("0")
        wallet_balance_available = bool(funding_wallets) and all(
            row.get("balance") is not None for row in funding_wallets
        )
        wallet_balance = (
            sum((D(row["balance"]) for row in funding_wallets), D("0")) if wallet_balance_available else None
        )
        offers = [row for row in snapshot.get("offers", []) if row.get("currency") == "USD"]
        lending_rows = _active_lending_rows(snapshot.get("credits", []), snapshot.get("loans", []))
        offer_total = sum((D(row["amount"]) for row in offers), D("0"))
        credit_total = sum(
            (D(row["amount"]) for row in lending_rows if row.get("funding_state") != "loan"),
            D("0"),
        )
        loan_total = sum(
            (D(row["amount"]) for row in lending_rows if row.get("funding_state") == "loan"),
            D("0"),
        )
        lent_total = credit_total + loan_total
        component_total = wallet + offer_total + lent_total if wallet_available_known else None
        total = (
            wallet_balance
            if wallet_balance is not None
            else component_total
            if component_total is not None
            else offer_total + lent_total
        )
        reconciliation_difference = (
            wallet_balance - component_total if wallet_balance is not None and component_total is not None else None
        )
        reconciliation_status = (
            "UNAVAILABLE"
            if reconciliation_difference is None
            else "MATCHED"
            if abs(reconciliation_difference) <= D("0.01")
            else "MISMATCH"
        )
        exposure = {pool: D("0") for pool in POOLS}
        managed_offer_exposure = {pool: D("0") for pool in POOLS}
        managed_offer_period_exposure = {}
        exposure_by_layer = {layer: D("0") for layer in ("quick", "balanced", "high")}
        managed_offer_layer_exposure = {layer: D("0") for layer in ("quick", "balanced", "high")}
        variable = D("0")
        hidden = D("0")
        offer_objects = {id(row) for row in offers}
        for row in [*offers, *lending_rows]:
            pool = row.get("pool") or pool_for_period(row.get("period", 0))
            if pool in exposure:
                exposure[pool] += D(row["amount"])
            amount = D(row["amount"])
            raw_type = str(row.get("display_type") or row.get("rate_type") or row.get("offer_type") or "").upper()
            if raw_type in {"FRR", "FRR_DELTA_VARIABLE", "FRRDELTAVAR", "VAR", "VARIABLE", "VARIABLE_UNKNOWN"}:
                variable += amount
            if bool(row.get("hidden")) or int(row.get("flags") or 0) & 64:
                hidden += amount
            if row.get("managed"):
                layer = row.get("layer") or "balanced"
                if layer in exposure_by_layer:
                    exposure_by_layer[layer] += amount
                if id(row) in offer_objects:
                    if pool in managed_offer_exposure:
                        managed_offer_exposure[pool] += amount
                    period = int(row.get("period") or 0)
                    managed_offer_period_exposure[period] = managed_offer_period_exposure.get(period, D("0")) + amount
                    if layer in managed_offer_layer_exposure:
                        managed_offer_layer_exposure[layer] += amount
        return {
            "wallet": wallet,
            "walletAvailableKnown": wallet_available_known,
            "walletBalance": wallet_balance,
            "offers": offer_total,
            "credits": lent_total,
            "creditPrincipal": credit_total,
            "loanPrincipal": loan_total,
            "componentTotal": component_total,
            "total": total,
            "reconciliationDifference": reconciliation_difference,
            "reconciliationStatus": reconciliation_status,
            "exposure": exposure,
            "exposureByLayer": exposure_by_layer,
            "managedOfferExposure": managed_offer_exposure,
            "managedOfferPeriodExposure": managed_offer_period_exposure,
            "managedOfferLayerExposure": managed_offer_layer_exposure,
            "existingExposure": {
                "total": offer_total + lent_total,
                "variable": variable,
                "hidden": hidden,
            },
        }

    @staticmethod
    def _build_plan(account, policy, signals, strategy_version):
        return build_strategy_plan_v3(
            account["total"],
            account["wallet"],
            account["exposure"],
            policy,
            signals,
            strategy_version,
            existing_exposure=account.get("existingExposure"),
            exposure_by_layer=account.get("exposureByLayer"),
            offer_exposure_by_pool=account.get("managedOfferExposure"),
            offer_exposure_by_layer=account.get("managedOfferLayerExposure"),
            offer_exposure_by_period=account.get("managedOfferPeriodExposure"),
        )

    def _net_interest_total(self):
        return self.store.realized_income("USD")

    def _persist_period_selection(self, signals, strategy_version, now_ms):
        selection = signals.get("periodSelection") or signals.get("period_selection") or {}
        for pool, payload in selection.get("byPool", {}).items():
            insufficient = bool(payload.get("insufficientMarketData"))
            pool_confirmation = self.store.observe_demand_confirmation(
                strategy_version,
                "pool",
                pool,
                payload.get("absoluteDemandShare"),
                None if insufficient else payload.get("belowDemandThreshold", False),
                now_ms,
            )
            payload["lowDemandCycles"] = pool_confirmation["cycles"]
            payload["lowDemandConfirmed"] = pool_confirmation["confirmed"]
            for row in payload.get("scores", []):
                term_confirmation = self.store.observe_demand_confirmation(
                    strategy_version,
                    "period",
                    f"{pool}:{int(row['period'])}",
                    row.get("relativeDemandShare"),
                    None if insufficient else row.get("belowDemandThreshold", False),
                    now_ms,
                )
                row["lowDemandCycles"] = term_confirmation["cycles"]
                row["lowDemandConfirmed"] = term_confirmation["confirmed"]
                row["allocationEligible"] = not term_confirmation["confirmed"]
            ranked_for_leader = sorted(
                (row for row in payload.get("scores", []) if not row.get("lowDemandConfirmed")),
                key=lambda row: (
                    D(row.get("totalScore") or 0),
                    D(row.get("fillScore") or 0),
                    D(row.get("demandScore") or 0),
                    -int(row["period"]),
                ),
                reverse=True,
            )
            if not ranked_for_leader:
                ranked_for_leader = sorted(
                    payload.get("scores", []),
                    key=lambda row: (D(row.get("totalScore") or 0), -int(row["period"])),
                    reverse=True,
                )
            payload["leaderPeriod"] = (
                None
                if insufficient
                else ranked_for_leader[0]["period"]
                if ranked_for_leader
                else payload.get("fallbackPeriod")
            )
            state = self.store.observe_period_selection(
                strategy_version,
                pool,
                payload.get("leaderPeriod", payload.get("selectedPeriod")),
                payload.get("scores", []),
                now_ms,
            )
            payload["leaderPeriod"] = state["leaderPeriod"]
            payload["selectedPeriod"] = state["selectedPeriod"]
            since = int(state["selectedSinceMs"])
            payload["selectedSinceMs"] = since
            payload["selectedDurationMs"] = max(0, int(now_ms) - since)
            payload["challengerPeriod"] = state["challengerPeriod"]
            payload["challengerSinceMs"] = state["challengerSinceMs"]
            payload["challengerDurationMs"] = state["challengerDurationMs"]
            payload["selectionMature"] = bool(state["promoted"])
            ranked_eligible = [
                int(row["period"])
                for row in sorted(
                    (row for row in payload.get("scores", []) if not row.get("lowDemandConfirmed")),
                    key=lambda row: (
                        D(row.get("totalScore") or 0),
                        D(row.get("fillScore") or 0),
                        D(row.get("demandScore") or 0),
                        -int(row["period"]),
                    ),
                    reverse=True,
                )
            ]
            payload["runnerUpPeriod"] = next(
                (period for period in ranked_eligible if period != payload["selectedPeriod"]),
                None,
            )
        signals["period_selection"] = selection
        signals["periodSelection"] = selection
        return signals

    def _record_variable_floor_violations(self, now_ms):
        violations = []
        for credit in self.store.credits(active_only=True):
            if not credit["managed"] or str(credit.get("rate_type") or "").upper() not in {
                "VAR",
                "VARIABLE",
                "FRRDELTAVAR",
            }:
                continue
            pool = credit.get("pool") or pool_for_period(credit["period"])
            if pool not in POOLS or self.policy.floor_apr(pool) is None:
                continue
            fee = self.policy.hidden_fee_rate if credit.get("hidden") else self.policy.normal_fee_rate
            floor_rate = gross_daily_floor(self.policy.floor_apr(pool), fee)
            observed = D(credit.get("rate_real") or credit["rate"])
            if rate_below_floor(observed, floor_rate):
                violations.append(
                    {
                        "credit_id": credit["credit_id"],
                        "pool": pool,
                        "floor_rate": floor_rate,
                        "observed_rate": observed,
                    }
                )
        self.store.record_rate_floor_violations(violations, now_ms)

    def _repricing_status(self, signals, now_ms):
        rows = []
        for offer in self.store.offers(active_only=True):
            offer_id = int(offer["offer_id"])
            chain = self.store.reprice_chain_for_offer(offer_id)
            pool = offer.get("pool") or pool_for_period(offer["period"])
            layer = offer.get("layer") or "balanced"
            if chain is None or pool not in POOLS or self.policy.floor_apr(pool) is None:
                continue
            observed_rate = funding_offer_effective_rate(offer, signals.get("frr_daily_rate"))
            if observed_rate is None:
                continue
            stage = int(chain.get("current_stage") or 0)
            curve_version = str(chain.get("pricing_curve_version") or "LEGACY")
            stages = self.policy.reprice_stages(pool, layer, curve_version)
            exploration_curve = curve_version in EXACT_TERM_EXPLORATION_CURVES
            landing_stage = min(
                len(stages),
                int(
                    self.policy.balanced_landing_stage
                    if layer in {"quick", "balanced"}
                    else self.policy.high_landing_stage
                ),
            ) if exploration_curve else None
            next_stage = stage + 1 if stage < len(stages) else None
            next_at = (
                int(chain["started_at_ms"]) + int(stages[next_stage - 1]) * 60_000 if next_stage is not None else None
            )
            hidden = bool(int(offer.get("flags") or 0) & 64)
            fee = self.policy.hidden_fee_rate if hidden else self.policy.normal_fee_rate
            floor_rate = ceil_rate_tick(gross_daily_floor(self.policy.floor_apr(pool), fee))
            benchmark = competitive_rate_for_period(
                layer, pool, int(offer["period"]), signals, floor_rate, curve_version
            )
            fixed_landing = (
                max(floor_rate, min(D(chain["origin_rate"]), D(chain["fixed_landing_rate"])))
                if chain.get("fixed_landing_rate") is not None
                else max(floor_rate, min(D(chain["origin_rate"]), benchmark))
            )
            pricing = period_pricing_context(signals, pool, int(offer["period"]), floor_rate)
            floor_gap = max(D("0"), observed_rate - floor_rate)
            landing_gap = max(D("0"), observed_rate - fixed_landing)
            final_due_at = int(chain["started_at_ms"]) + int(stages[-1]) * 60_000
            pending_action = chain.get("pending_action")
            below_repost_minimum = D(offer.get("amount") or 0) < USD_ORDER_CHUNK
            if below_repost_minimum and floor_gap >= self.policy.minimum_rate_change:
                floor_state = "BELOW_REPOST_MINIMUM"
            elif pending_action == "AGE_STAGE" and stage >= len(stages):
                floor_state = "REPRICE_PENDING"
            elif floor_gap < self.policy.minimum_rate_change:
                floor_state = "SATISFIED_WITHIN_TOLERANCE"
            elif now_ms >= final_due_at:
                floor_state = "REPRICE_REQUIRED"
            else:
                floor_state = "NOT_DUE"
            market_anchor = chain.get("market_anchor_rate")
            if market_anchor is None and stage >= 3:
                market_anchor = observed_rate
            next_target = None
            if next_stage is not None:
                if exploration_curve:
                    next_target = _exploration_age_stage_target(
                        next_stage, chain, benchmark, floor_rate, landing_stage, len(stages)
                    )
                else:
                    next_target = _age_stage_target(
                        next_stage,
                        chain,
                        benchmark,
                        floor_rate,
                        fallback_anchor=observed_rate,
                        stage_count=len(stages),
                    )
            stage_type = (
                "FLOOR" if exploration_curve and stage > landing_stage else "MARKET"
                if exploration_curve
                else _reprice_stage_type(stage, len(stages))
            )
            next_stage_type = (
                None
                if next_stage is None
                else "FLOOR"
                if exploration_curve and next_stage > landing_stage
                else "MARKET"
                if exploration_curve
                else _reprice_stage_type(next_stage, len(stages))
            )
            rows.append(
                {
                    "offerId": offer_id,
                    "pool": pool,
                    "layer": layer,
                    "stage": stage,
                    "elapsedMinutes": max(D("0"), D(now_ms - int(chain["started_at_ms"])) / D("60000")),
                    "nextStageAtMs": next_at,
                    "nextTargetRate": next_target,
                    "benchmarkRate": benchmark,
                    "currentMarketRate": benchmark,
                    "curveVersion": curve_version,
                    "explorationStartRate": D(chain["origin_rate"]),
                    "landingRate": fixed_landing,
                    "landingPolicy": "FIXED_AT_CREATION" if exploration_curve else "LEGACY_DYNAMIC",
                    "landingStage": landing_stage,
                    "benchmarkSource": pricing["source"],
                    "periodBestBorrowRate": pricing["bestBorrowRate"],
                    "periodAnchorRate": pricing["anchorRate"],
                    "floorRate": floor_rate,
                    "floorState": floor_state,
                    "landingState": (
                        "SATISFIED_WITHIN_TOLERANCE"
                        if exploration_curve and landing_gap < self.policy.minimum_rate_change
                        else "REPRICE_REQUIRED"
                        if exploration_curve
                        and now_ms
                        >= int(chain["started_at_ms"]) + int(stages[landing_stage - 1]) * 60_000
                        else "NOT_DUE"
                        if exploration_curve
                        else None
                    ),
                    "marketAnchorRate": market_anchor,
                    "stageType": stage_type,
                    "nextStageType": next_stage_type,
                    "totalStages": len(stages),
                    "marketStageCount": landing_stage if exploration_curve else len(stages) // 2,
                    "pendingAction": chain.get("pending_action"),
                    "repriceBlockedReason": "BELOW_REPOST_MINIMUM" if below_repost_minimum else None,
                }
            )
        return json_decimal(rows)

    def _strategy_status(self, snapshot, signals, result=None):
        account = self._account(snapshot)
        runtime = self.store.runtime()
        realized_income = self.store.realized_income_summary("USD")
        income_sync = self.store.income_history_sync_payload("USD")
        now = int(snapshot.get("now") or self.clock() * 1000)
        repricing = self._repricing_status(signals, now)
        repricing_by_offer = {int(row["offerId"]): row for row in repricing}
        open_offers = json_decimal(snapshot["offers"])
        active_lending = _active_lending_rows(snapshot.get("credits", []), snapshot.get("loans", []))
        active_credit_dashboard = build_active_credit_dashboard(
            active_lending,
            account["total"],
            self.policy,
            now,
        )
        for offer in open_offers:
            offer["manualProtection"] = self.store.manual_protection_for_offer(offer, now)
            effective = funding_offer_effective_rate(offer, signals.get("frr_daily_rate"))
            offer["dailyRatePercent"] = None if effective is None else format(effective * 100, "f")
            offer_id = int(offer.get("id") or offer.get("offer_id") or 0)
            if offer_id in repricing_by_offer:
                offer["repriceState"] = repricing_by_offer[offer_id]
        strategy_payload = None if result is None else json_decimal(result)
        if strategy_payload is not None:
            strategy_payload["repricing"] = repricing
            strategy_payload["orderSizing"] = order_sizing_payload()
            selection_payload = json_decimal(signals.get("periodSelection") or signals.get("period_selection") or {})
            for pool, payload in selection_payload.get("byPool", {}).items():
                payload["poolCapPercent"] = strategy_payload.get("poolCapPercentages", {}).get(pool)
                payload["poolAllocation"] = strategy_payload.get("poolAllocation", {}).get(pool, {})
                payload["termAllocation"] = strategy_payload.get("termAllocations", {}).get(pool, {})
                payload["allocationCurve"] = "100/0,90/10,75/25,60/40"
            strategy_payload["periodSelection"] = selection_payload
            strategy_payload["dustConsolidation"] = json_decimal(self.store.consolidation_status())
            strategy_payload["externalTakeover"] = json_decimal(
                {"automatic": bool(self.policy.adopt_external_offers), "offers": self.store.external_takeovers()}
            )
            strategy_payload["periodActivity"] = json_decimal(self.store.period_activity(now - 86_400_000, "USD"))
        status = {
            "schemaVersion": 3,
            "stateSchemaVersion": 19,
            "applicationVersion": APP_VERSION,
            "marketHistory": json_decimal(self._history_status),
            "v36Validation": json_decimal(self._validation_status),
            "operationMode": runtime["mode"],
            "runtime": runtime,
            "recovery": self.store.recovery_status(),
            "marketData": {
                key: value
                for key, value in snapshot.items()
                if key not in {"book", "trades", "wallets", "offers", "credits", "loans", "fundingTrades"}
            },
            "market": json_decimal(signals),
            "account": json_decimal(account),
            "openOffers": open_offers,
            "credits": active_credit_dashboard["credits"],
            "activeCreditSummary": active_credit_dashboard["summary"],
            "realizedIncome": realized_income,
            "incomeHistorySync": income_sync,
            "releaseComparison": json_decimal(self.store.release_comparison(now, "USD")),
            "writeRecovery": self._write_recovery_status(),
            "strategyV3": strategy_payload,
            "activeStrategy": self.store.strategy("ACTIVE"),
            "draftStrategy": self.store.strategy("DRAFT"),
            "pendingStrategy": self.store.strategy("PENDING"),
            "statistics": {
                "1d": self.store.statistics(1),
                "7d": self.store.statistics(7),
                "30d": self.store.statistics(30),
                "90d": self.store.statistics(90),
                "all": self.store.statistics(None),
            },
        }
        if self.log is not None:
            for key, value in status.items():
                if key not in {"openOffers", "credits"}:
                    self.log.updateMetaValue(key, value)
            self.log.updateMetaValue("openOffers", status["openOffers"])
            self.log.updateMetaValue("credits", status["credits"])
            if runtime["mode"] == "PAUSED" and runtime.get("safe_reason"):
                self.log.refreshStatus(f"PAUSED：{runtime.get('safe_reason') or '策略已暂停'}")
            else:
                self.log.refreshStatus(f"v{APP_VERSION} {runtime['mode']} 状态已同步。")
        return status

    def _submit_plan(
        self,
        plan_result,
        wallet_available,
        strategy_version,
        continuation_offer_id=None,
    ):
        submitted = []
        remaining = D(wallet_available)
        now = int(self.clock() * 1000)
        recent_attempts = self.store.submission_attempt_count_since(
            now - FUNDING_SUBMISSION_WINDOW_MS,
            "USD",
        )
        attempt_budget = max(0, MAX_FUNDING_SUBMISSIONS_PER_WINDOW - recent_attempts)
        attempts = 0
        plan_hash = str(plan_result.get("plan_hash") or "unhashed")
        for row in plan_result["plan"]:
            if attempts >= attempt_budget:
                break
            base_slice_key = f"{strategy_version}:{plan_hash}:{row['pool']}:{row['layer']}:{row['slice_index']}"
            submit_row = dict(row)
            if submit_row["amount"] > remaining:
                break
            order = {
                **submit_row,
                "currency": "USD",
                "slice_key": self.store.replenishment_slice_key(base_slice_key),
                "strategy_version": strategy_version,
                "pricing_curve_version": submit_row.get("pricing_curve_version")
                or EXACT_TERM_EXPLORATION_CURVE,
                "fixed_landing_rate": submit_row.get("fixed_landing_rate"),
            }
            try:
                created, intent = self.store.reserve_intent(order, remaining)
            except InsufficientReservedBalance:
                break
            if not created:
                continue
            attempts += 1
            self.store.mark_submitting(intent["id"])
            result = _write_result(
                self.client,
                "submit_funding_offer_result",
                "submit_funding_offer",
                "fUSD",
                format(submit_row["amount"], "f"),
                format(submit_row["submitted_rate"], "f"),
                submit_row["period"],
                submit_row["offer_type"],
                flags=submit_row["flags"],
            )
            if result.outcome == WriteOutcome.CONFIRMED:
                response = result.response
                offer_id = extract_submitted_offer_id(response)
                if offer_id is None:
                    self.store.mark_ambiguous(intent["id"], "successful notification omitted offer id")
                    break
                intent = self.store.confirm_intent(intent["id"], offer_id)
                if continuation_offer_id is not None:
                    rebound = self.store.bind_consolidation_replacement(
                        continuation_offer_id,
                        offer_id,
                    )
                    if rebound is not None:
                        self.store.record_ownership_event(
                            "DUST_CHAIN_BOUND",
                            offer_id=offer_id,
                            details={
                                "sourceOfferId": int(continuation_offer_id),
                                "chainKey": rebound["chain_key"],
                            },
                        )
                    elif self.store.reprice_chain_for_offer(offer_id) is None:
                        sibling_chain = self.store.ensure_reprice_chain(
                            {
                                "offer_id": offer_id,
                                "rate": intent["submitted_rate"],
                                "rate_real": intent["effective_rate"],
                                "pool": intent["pool"],
                                "layer": intent["layer"],
                                "mts_created": intent["updated_at_ms"],
                            },
                            strategy_version,
                            intent["updated_at_ms"],
                        )
                        if sibling_chain is not None:
                            self.store.record_ownership_event(
                                "DUST_CHAIN_STARTED",
                                offer_id=offer_id,
                                details={
                                    "sourceOfferId": int(continuation_offer_id),
                                    "chainKey": sibling_chain["chain_key"],
                                    "stage": 0,
                                },
                            )
                remaining -= submit_row["amount"]
                submitted.append({"intentId": intent["id"], "offerId": offer_id, **submit_row})
            elif result.outcome == WriteOutcome.UNKNOWN:
                self.store.mark_ambiguous(intent["id"], result.error)
                break
            else:
                self.store.reject_intent(intent["id"], result.error)
                self._log(f"USD v3 挂单被明确拒绝：{result.error}")
                if result.category == "BALANCE_DRIFT":
                    self.store.set_mode("PAUSED", "AUTO_RECOVERY:BALANCE_DRIFT")
                    self.store.begin_recovery(
                        "BALANCE_DRIFT",
                        result.error,
                        origin_mode="LIVE",
                        target_mode="LIVE",
                    )
                    self.store.record_recovery_failure(result.error, "BALANCE_DRIFT", now)
                    break
        return submitted

    def _submit_pending_reprices(
        self,
        wallet_available,
        strategy_version,
        cap_limited_available=None,
        *,
        frr_daily_rate=None,
    ):
        """Restore canceled offers before new wallet cash is allocated."""

        submitted = []
        remaining = D(wallet_available)
        pending_rows = self.store.pending_reprices(strategy_version)
        replacement_principal = sum((D(row["source_amount"]) for row in pending_rows), D("0"))
        cap_available = D(
            wallet_available if cap_limited_available is None else cap_limited_available
        )
        replacement_surplus = min(
            max(D("0"), remaining - replacement_principal),
            max(D("0"), cap_available - replacement_principal),
        ).quantize(SATOSHI, rounding=ROUND_DOWN)
        absorption_chain = None
        if D("0") < replacement_surplus < USD_ORDER_CHUNK:
            absorption_chain = next(
                (
                    row["chain_key"]
                    for row in pending_rows
                    if str(
                        row.get("source_display_type")
                        or row.get("source_offer_type")
                        or "LIMIT"
                    ).upper()
                    == "LIMIT"
                    and str(row.get("source_offer_type") or "LIMIT").upper() == "LIMIT"
                    and not (int(row.get("source_flags") or 0) & 64)
                ),
                None,
            )
        now = int(self.clock() * 1000)
        recent_attempts = self.store.submission_attempt_count_since(
            now - FUNDING_SUBMISSION_WINDOW_MS,
            "USD",
        )
        attempt_budget = max(0, MAX_FUNDING_SUBMISSIONS_PER_WINDOW - recent_attempts)
        attempts = 0
        frr = D(frr_daily_rate or 0)
        for pending in pending_rows:
            if attempts >= attempt_budget:
                break
            amount = D(pending["source_amount"])
            absorbed_remainder = D("0")
            if pending["chain_key"] == absorption_chain:
                absorbed_remainder = replacement_surplus
                amount += absorbed_remainder
            if amount > remaining:
                break
            pool = str(pending["pool"])
            flags = int(pending.get("source_flags") or 0)
            hidden = bool(flags & 64)
            fee = self.policy.hidden_fee_rate if hidden else self.policy.normal_fee_rate
            floor_rate = ceil_rate_tick(gross_daily_floor(self.policy.floor_apr(pool), fee))
            display_type = str(
                pending.get("source_display_type") or pending.get("source_offer_type") or "LIMIT"
            ).upper()
            offer_type = str(pending.get("source_offer_type") or "LIMIT").upper()
            if offer_type in FRR_OFFSET_OFFER_TYPES and frr <= 0:
                continue
            source_effective = funding_offer_effective_rate(
                {"offer_type": offer_type, "rate": pending["source_rate"], "rate_real": pending["source_rate_real"]},
                frr,
            )
            row = self._apply_pending_reprice(
                {
                    "amount": amount,
                    "pool": pool,
                    "layer": str(pending["layer"]),
                    "period": int(pending["source_period"]),
                    "offer_type": offer_type,
                    "display_type": display_type,
                    "flags": flags,
                    "submitted_rate": D(pending["source_rate"]),
                    "effective_rate": source_effective,
                    "target_rate": source_effective,
                    "gross_daily_floor": floor_rate,
                    "plan_hash": f"reprice:{pending['chain_key']}",
                    "absorbed_remainder": absorbed_remainder,
                    "remainder_absorption_pool": pool if absorbed_remainder > 0 else None,
                },
                pending,
                frr,
            )
            if rate_below_floor(row["effective_rate"], floor_rate):
                continue
            order = {
                **row,
                "currency": str(pending.get("source_currency") or "USD").upper(),
                "slice_key": self.store.replenishment_slice_key(pending["base_slice_key"]),
                "strategy_version": strategy_version,
                "pricing_curve_version": str(
                    pending.get("pricing_curve_version") or EXACT_TERM_EXPLORATION_CURVE
                ),
                "fixed_landing_rate": pending.get("fixed_landing_rate"),
            }
            try:
                created, intent = self.store.reserve_intent(order, remaining)
            except InsufficientReservedBalance:
                break
            if not created:
                continue
            attempts += 1
            self.store.mark_submitting(intent["id"])
            result = _write_result(
                self.client,
                "submit_funding_offer_result",
                "submit_funding_offer",
                f"f{order['currency']}",
                format(row["amount"], "f"),
                format(row["submitted_rate"], "f"),
                row["period"],
                row["offer_type"],
                flags=row["flags"],
            )
            if result.outcome == WriteOutcome.CONFIRMED:
                offer_id = extract_submitted_offer_id(result.response)
                if offer_id is None:
                    self.store.mark_ambiguous(intent["id"], "successful notification omitted offer id")
                    break
                self.store.confirm_intent(intent["id"], offer_id)
                self.store.bind_reprice_replacement_chain(
                    pending["chain_key"],
                    offer_id,
                    row["effective_rate"],
                )
                remaining -= row["amount"]
                submitted.append({"intentId": intent["id"], "offerId": offer_id, **row})
            elif result.outcome == WriteOutcome.UNKNOWN:
                self.store.mark_ambiguous(intent["id"], result.error)
                break
            else:
                self.store.reject_intent(intent["id"], result.error)
                self._log(f"USD 调价重挂被明确拒绝：{result.error}")
                if result.category == "BALANCE_DRIFT":
                    self.store.set_mode("PAUSED", "AUTO_RECOVERY:BALANCE_DRIFT")
                    self.store.begin_recovery(
                        "BALANCE_DRIFT",
                        result.error,
                        origin_mode="LIVE",
                        target_mode="LIVE",
                    )
                    self.store.record_recovery_failure(result.error, "BALANCE_DRIFT", now)
                break
        outstanding = sum(
            (D(row["source_amount"]) for row in self.store.pending_reprices(strategy_version)),
            D("0"),
        )
        return submitted, max(D("0"), remaining - outstanding)

    @staticmethod
    def _apply_pending_reprice(row, pending, frr_daily_rate):
        if not pending or pending.get("pending_target_rate") is None:
            return dict(row)
        adjusted = dict(row)
        floor_rate = ceil_rate_tick(adjusted.get("gross_daily_floor") or 0)
        source_effective = D(adjusted.get("effective_rate") or adjusted.get("submitted_rate") or 0)
        desired = ceil_rate_tick(
            max(
                floor_rate,
                min(source_effective, D(pending["pending_target_rate"])),
            )
        )
        display_type = str(adjusted.get("display_type") or adjusted.get("offer_type") or "LIMIT").upper()
        if display_type == "LIMIT":
            adjusted["submitted_rate"] = desired
            adjusted["effective_rate"] = desired
            adjusted["target_rate"] = desired
        elif adjusted["offer_type"] in FRR_OFFSET_OFFER_TYPES:
            offset = D("0") if display_type == "FRR" else ceil_rate_tick(max(D("0"), desired - frr_daily_rate))
            adjusted["submitted_rate"] = offset
            adjusted["effective_rate"] = frr_daily_rate + offset
            adjusted["target_rate"] = adjusted["effective_rate"]
        return adjusted

    def _cancel_reprice_candidates(self, snapshot, plan_result, now_ms=None, strategy_version=None):
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        signals = snapshot.get("market", {})
        frr = D(signals.get("frr_daily_rate") or 0)
        canceled = []
        offers = self.store.offers(active_only=True)

        def reprice_priority(offer):
            pool = offer.get("pool") or pool_for_period(offer.get("period") or 0)
            layer = offer.get("layer") or "balanced"
            chain = self.store.reprice_chain_for_offer(int(offer["offer_id"]))
            started_at = int(
                (chain or {}).get("started_at_ms")
                or offer.get("mts_created")
                or now
            )
            curve_version = str((chain or {}).get("pricing_curve_version") or "LEGACY")
            stages = (
                self.policy.reprice_stages(pool, layer, curve_version)
                if pool in POOLS
                else (0,)
            )
            final_due_at = started_at + int(stages[-1]) * 60_000
            return (
                0 if now >= final_due_at else 1,
                final_due_at,
                started_at,
                int(offer["offer_id"]),
            )

        protections = self.store.manual_protections(now)
        for offer in sorted(offers, key=reprice_priority):
            protection = protections.get(int(offer["offer_id"]))
            if protection and protection["needsTakeover"]:
                continue
            if not offer["managed"] or offer["currency"] != "USD":
                continue
            offer_id = int(offer["offer_id"])
            if offer_id in self._pending_cancel_requested:
                continue
            if D(offer.get("amount") or 0) < USD_ORDER_CHUNK:
                continue
            pool = offer.get("pool") or pool_for_period(offer["period"])
            layer = offer.get("layer") or "balanced"
            if pool not in POOLS:
                continue
            is_frr = offer["offer_type"] in FRR_OFFSET_OFFER_TYPES
            old_rate = funding_offer_effective_rate(offer, frr)
            if old_rate is None or (is_frr and frr <= 0):
                continue
            chain = self.store.ensure_reprice_chain(offer, strategy_version, now, frr_daily_rate=frr)
            if chain is None or chain.get("pending_action"):
                continue

            hidden = bool(int(offer.get("flags") or 0) & 64)
            fee = self.policy.hidden_fee_rate if hidden else self.policy.normal_fee_rate
            floor_rate = ceil_rate_tick(gross_daily_floor(self.policy.floor_apr(pool), fee))
            curve_version = str(chain.get("pricing_curve_version") or "LEGACY")
            benchmark = competitive_rate_for_period(
                layer, pool, int(offer["period"]), signals, floor_rate, curve_version
            )
            exploration_curve = curve_version in EXACT_TERM_EXPLORATION_CURVES
            if exploration_curve and chain.get("fixed_landing_rate") is None:
                fixed_landing = max(
                    floor_rate,
                    min(D(chain["origin_rate"]), benchmark),
                )
                chain = self.store.set_fixed_landing_rate(
                    chain["chain_key"],
                    fixed_landing,
                    now,
                )
            # The new-cash plan ranks order types for new offers only. An existing
            # offer keeps its type, period and flags while following its age curve.
            display_type = str(offer.get("display_type") or self._offer_display_type(offer)).upper()
            current_stage = int(chain.get("current_stage") or 0)
            stages = self.policy.reprice_stages(pool, layer, curve_version)
            if current_stage >= len(stages):
                next_stage = len(stages)
                desired_rate = floor_rate
            else:
                next_stage = current_stage + 1
                due_at = int(chain["started_at_ms"]) + int(stages[next_stage - 1]) * 60_000
                if now < due_at:
                    continue
                if not exploration_curve and next_stage > 3 and chain.get("market_anchor_rate") is None:
                    chain = self.store.set_reprice_market_anchor(chain["chain_key"], old_rate, now)
                if exploration_curve:
                    landing_stage = min(
                        len(stages),
                        int(
                            self.policy.balanced_landing_stage
                            if layer in {"quick", "balanced"}
                            else self.policy.high_landing_stage
                        ),
                    )
                    desired_rate = _exploration_age_stage_target(
                        next_stage, chain, benchmark, floor_rate, landing_stage, len(stages)
                    )
                else:
                    desired_rate = _age_stage_target(
                        next_stage,
                        chain,
                        benchmark,
                        floor_rate,
                        fallback_anchor=old_rate,
                        stage_count=len(stages),
                    )
            desired_rate = ceil_rate_tick(max(floor_rate, min(old_rate, desired_rate)))
            can_reduce_offset = True
            if is_frr:
                offset = ceil_rate_tick(max(D("0"), desired_rate - frr))
                desired_rate = frr + offset
                can_reduce_offset = offset < D(offer["rate"])
            if (
                not can_reduce_offset
                or display_type == "FRR"
                or old_rate <= desired_rate
                or old_rate - desired_rate < self.policy.minimum_rate_change
            ):
                if current_stage < len(stages):
                    self.store.complete_reprice_stage(
                        chain["chain_key"],
                        next_stage,
                        now,
                        market_anchor_rate=(
                            old_rate if not exploration_curve and next_stage == 3 else None
                        ),
                    )
                    self.store.record_ownership_event(
                        "REPRICE_STAGE_SKIPPED",
                        offer_id=offer_id,
                        details={
                            "stage": next_stage,
                            "stageType": (
                                "FLOOR"
                                if exploration_curve and next_stage > landing_stage
                                else "MARKET"
                                if exploration_curve
                                else _reprice_stage_type(next_stage, len(stages))
                            ),
                            "benchmarkRate": format(benchmark, "f"),
                            "targetRate": format(desired_rate, "f"),
                            "marketAnchorRate": format(
                                D(chain.get("market_anchor_rate") or old_rate),
                                "f",
                            )
                            if next_stage >= 3
                            else None,
                        },
                    )
                continue
            stage = next_stage
            reason = f"age_stage_{stage}"
            final_floor_priority = stage >= len(stages)
            if (
                not final_floor_priority
                and self.store.reprice_count_since(now - 3_600_000) >= self.policy.max_reprices_per_hour
            ):
                continue
            last_family_reprice = self.store.last_reprice_for_family(pool, layer)
            if (
                not final_floor_priority
                and
                last_family_reprice is not None
                and now - int(last_family_reprice) < self.policy.reprice_cooldown_minutes * 60_000
            ):
                continue
            if not self._manual_cancel_allowed(offer, now):
                continue
            write = _write_result(self.client, "cancel_funding_offer_result", "cancel_funding_offer", offer_id)
            if write.outcome == WriteOutcome.UNKNOWN:
                self.store.record_ownership_event(
                    "CANCEL_UNKNOWN",
                    offer_id=offer_id,
                    details={"reason": reason, "error": write.error},
                )
                self.store.enter_protected_pause(f"AMBIGUOUS_CANCEL:{offer_id}")
                self._log(write.error)
                break
            if write.outcome == WriteOutcome.DEFINITE_REJECT:
                self.store.record_ownership_event(
                    "CANCEL_REJECTED",
                    offer_id=offer_id,
                    details={"reason": reason, "error": write.error},
                )
                self._log(f"撤销 Offer {offer_id} 被明确拒绝：{write.error}")
                continue
            self.store.mark_reprice_pending(
                chain["chain_key"],
                "AGE_STAGE",
                desired_rate,
                stage=stage,
                now_ms=now,
                market_anchor_rate=(
                    desired_rate
                    if stage == 3 and not exploration_curve
                    else None
                ),
                source_offer_id=offer_id,
            )
            self.store.record_reprice(
                offer_id,
                reason,
                old_rate,
                desired_rate,
                strategy_version=strategy_version,
                plan_hash=plan_result.get("plan_hash"),
                display_type=display_type,
                chain_key=chain["chain_key"],
                stage=stage,
                benchmark_rate=benchmark,
                floor_rate=floor_rate,
            )
            canceled.append(offer_id)
            self._pending_cancel_requested.add(offer_id)
            self.store.record_ownership_event("CANCEL_CONFIRMED", offer_id=offer_id, details={"reason": reason})
            self._log(
                f"挂单 {offer_id} 累计等待达到第 {stage} 阶段，"
                f"利率由 {format(old_rate, 'f')} 调整为 {format(desired_rate, 'f')}。"
            )
        return canceled

    def _manual_cancel_allowed(self, offer, now_ms, allow_retake=False, safety=False):
        from ManualProtection import parameter_digest

        offer_id = int(offer.get("id") or offer.get("offer_id"))
        latest = offer
        read = getattr(type(self.client), "active_funding_offers", None)
        if callable(read):
            try:
                rows = parse_offer_rows_v3(self.client.active_funding_offers("fUSD"))
            except Exception as exc:
                self._log(f"撤单前无法核对挂单 {offer_id}，本轮跳过：{exc}")
                return False
            latest = next((row for row in rows if int(row["id"]) == offer_id), None)
            if latest is None:
                return False
        now_ms = max(now_ms, int(self.clock() * 1000))
        latest = dict(latest, managed=bool(offer.get("managed")))
        protection = self.store.observe_manual_offer(latest, now_ms)
        if parameter_digest(latest) != parameter_digest(offer):
            return False  # Rebuild safety/allocation decisions against the new parameters.
        if safety:
            return True
        if protection and (protection["protected"] or (protection["needsTakeover"] and not allow_retake)):
            return False
        return True

    def _cancel_external_takeovers(self, now_ms, strategy_version, plan_hash=None):
        """Cancel only explicitly adopted external offers before global replanning."""

        if self.store.reprice_count_since(now_ms - 3_600_000) >= self.policy.max_reprices_per_hour:
            return []
        active = {int(row["offer_id"]): row for row in self.store.offers(active_only=True)}
        canceled = []
        remaining = max(0, self.policy.max_reprices_per_hour - self.store.reprice_count_since(now_ms - 3_600_000))
        for takeover in self.store.external_takeovers(states={"ADOPTED"})[:remaining]:
            offer_id = int(takeover["offer_id"])
            offer = active.get(offer_id)
            if offer is None:
                self.store.update_external_takeover(offer_id, "CLOSED", now_ms=now_ms)
                continue
            if offer_id in self._pending_cancel_requested:
                continue
            if not self._manual_cancel_allowed(offer, now_ms, allow_retake=True):
                continue
            result = _write_result(self.client, "cancel_funding_offer_result", "cancel_funding_offer", offer_id)
            if result.outcome == WriteOutcome.UNKNOWN:
                self.store.update_external_takeover(offer_id, "AMBIGUOUS", result.error, now_ms)
                self.store.record_ownership_event(
                    "CANCEL_UNKNOWN", offer_id=offer_id, details={"reason": "external_takeover", "error": result.error}
                )
                self.store.enter_protected_pause(f"AMBIGUOUS_CANCEL:{offer_id}")
                break
            if result.outcome == WriteOutcome.DEFINITE_REJECT:
                self.store.update_external_takeover(offer_id, "ERROR", result.error, now_ms)
                continue
            self.store.record_reprice(
                offer_id,
                "external_takeover",
                offer.get("rate_real") or offer["rate"],
                None,
                created_at_ms=now_ms,
                strategy_version=strategy_version,
                plan_hash=plan_hash,
                display_type=offer.get("display_type"),
            )
            self.store.record_ownership_event(
                "CANCEL_CONFIRMED", offer_id=offer_id, details={"reason": "external_takeover"}
            )
            self.store.finish_manual_protection(offer_id, "external_takeover", now_ms)
            self.store.update_external_takeover(offer_id, "CANCELLING", now_ms=now_ms)
            self._pending_cancel_requested.add(offer_id)
            canceled.append(offer_id)
        return canceled

    def _dust_consolidation(self, account, signals, now_ms, strategy_version):
        """Merge wallet dust without changing the selected offer's strategy identity."""

        status = self.store.consolidation_status()
        state = status.get("state", "IDLE")
        active_offers = {int(row["offer_id"]): row for row in self.store.offers(active_only=True)}
        if state == "CANCELLING":
            if int(status["offer_id"]) not in active_offers and account["reconciliationStatus"] == "MATCHED":
                self.store.update_consolidation("READY", now_ms=now_ms)
                return {"blocking": True, "state": "READY", "canceled": [], "submitted": []}
            return {"blocking": True, "state": state, "canceled": [], "submitted": []}
        if state in {"READY", "SUBMITTING", "AMBIGUOUS"}:
            wallet = D(account["wallet"])
            expected = D(status["captured_wallet"]) + D(status["captured_offer_amount"])
            dust_hash = f"dust-{status.get('started_at_ms')}-{status.get('offer_id')}"
            related = [row for row in self.store.intents() if row.get("plan_hash") == dust_hash]
            ambiguous = [row for row in related if row["state"] == "AMBIGUOUS"]
            if ambiguous:
                self.store.update_consolidation("AMBIGUOUS", "unknown replacement submit", now_ms)
                return {"blocking": True, "state": "AMBIGUOUS", "canceled": [], "submitted": []}
            if any(row["state"] in {"PLANNED", "SUBMITTING"} for row in related):
                self.store.update_consolidation("SUBMITTING", now_ms=now_ms)
                return {"blocking": True, "state": "SUBMITTING", "canceled": [], "submitted": []}

            # A process may stop after exchange confirmation but before the
            # source chain is rebound.  Replaying this compare-and-set is safe;
            # only the first successful split replacement can inherit it.
            for intent in sorted(related, key=lambda row: int(row["id"])):
                replacement_id = intent.get("exchange_offer_id")
                if replacement_id is None or intent["state"] not in {"CONFIRMED", "CLOSED"}:
                    continue
                rebound = self.store.bind_consolidation_replacement(
                    status["offer_id"],
                    replacement_id,
                    now_ms,
                )
                if rebound is not None:
                    self.store.record_ownership_event(
                        "DUST_CHAIN_BOUND",
                        offer_id=replacement_id,
                        details={
                            "sourceOfferId": int(status["offer_id"]),
                            "chainKey": rebound["chain_key"],
                            "recovered": True,
                        },
                    )
                elif self.store.reprice_chain_for_offer(replacement_id) is None:
                    self.store.ensure_reprice_chain(
                        {
                            "offer_id": replacement_id,
                            "rate": intent["submitted_rate"],
                            "rate_real": intent["effective_rate"],
                            "pool": intent["pool"],
                            "layer": intent["layer"],
                            "mts_created": intent["updated_at_ms"],
                        },
                        strategy_version,
                        intent["updated_at_ms"],
                    )

            active_offer_ids = set(active_offers)
            invisible_confirmed = [
                row
                for row in related
                if row["state"] == "CONFIRMED" and int(row.get("exchange_offer_id") or 0) not in active_offer_ids
            ]
            if invisible_confirmed:
                self.store.update_consolidation("SUBMITTING", now_ms=now_ms)
                return {"blocking": True, "state": "SUBMITTING", "canceled": [], "submitted": []}

            deployed = sum(
                (D(row["amount"]) for row in related if row["state"] in {"CONFIRMED", "CLOSED"}),
                D("0"),
            )
            expected_remaining = max(D("0"), expected - deployed)
            if expected_remaining == 0:
                self.store.clear_consolidation(now_ms)
                return {"blocking": True, "state": "CONFIRMED", "canceled": [], "submitted": []}
            if wallet < expected_remaining or expected_remaining < USD_ORDER_CHUNK:
                if deployed > 0 and expected_remaining < USD_ORDER_CHUNK:
                    self.store.clear_consolidation(now_ms)
                    return {"blocking": True, "state": "CONFIRMED", "canceled": [], "submitted": []}
                self.store.update_consolidation("READY", now_ms=now_ms)
                return {"blocking": True, "state": "READY", "canceled": [], "submitted": []}

            source_offer = next(
                (
                    row
                    for row in self.store.offers()
                    if int(row["offer_id"]) == int(status["offer_id"])
                ),
                None,
            )
            if source_offer is None:
                self.store.update_consolidation("AMBIGUOUS", "source offer metadata missing", now_ms)
                self.store.enter_protected_pause(f"DUST_SOURCE_MISSING:{status['offer_id']}")
                return {"blocking": True, "state": "AMBIGUOUS", "canceled": [], "submitted": []}

            source_chain = self.store.reprice_chain_for_offer(int(status["offer_id"]))
            if source_chain is None:
                for intent in sorted(related, key=lambda row: int(row["id"])):
                    replacement_id = intent.get("exchange_offer_id")
                    if replacement_id is None:
                        continue
                    source_chain = self.store.reprice_chain_for_offer(int(replacement_id))
                    if source_chain is not None:
                        break
            if source_chain is None:
                self.store.update_consolidation("AMBIGUOUS", "source reprice chain missing", now_ms)
                self.store.enter_protected_pause(f"DUST_CHAIN_MISSING:{status['offer_id']}")
                return {"blocking": True, "state": "AMBIGUOUS", "canceled": [], "submitted": []}

            pool = str(source_offer.get("pool") or pool_for_period(source_offer["period"]))
            layer = str(source_offer.get("layer") or "balanced")
            hidden = bool(int(source_offer.get("flags") or 0) & 64)
            fee = self.policy.hidden_fee_rate if hidden else self.policy.normal_fee_rate
            floor_rate = ceil_rate_tick(gross_daily_floor(self.policy.floor_apr(pool), fee))
            submitted_rate = D(source_offer["rate"])
            effective_rate = D(source_offer.get("rate_real") or source_offer["rate"])
            slice_count = int(expected_remaining // USD_ORDER_CHUNK)
            amounts = evenly_distributed_amounts(expected_remaining, slice_count)
            slice_offset = len(related)
            dust_plan = {
                "plan_hash": dust_hash,
                "plan": [
                    {
                        "slice_index": slice_offset + index,
                        "pool": pool,
                        "layer": layer,
                        "amount": amount,
                        "period": int(source_offer["period"]),
                        "offer_type": str(source_offer.get("offer_type") or "LIMIT"),
                        "display_type": str(
                            source_offer.get("display_type")
                            or source_offer.get("offer_type")
                            or "LIMIT"
                        ),
                        "flags": int(source_offer.get("flags") or 0),
                        "submitted_rate": submitted_rate,
                        "effective_rate": effective_rate,
                        "target_rate": effective_rate,
                        "gross_daily_floor": floor_rate,
                        "plan_hash": dust_hash,
                        "pricing_curve_version": source_chain.get("pricing_curve_version"),
                        "fixed_landing_rate": source_chain.get("fixed_landing_rate"),
                    }
                    for index, amount in enumerate(amounts)
                ],
            }
            self.store.update_consolidation("SUBMITTING", now_ms=now_ms)
            submitted = self._submit_plan(
                dust_plan,
                expected_remaining,
                strategy_version,
                continuation_offer_id=status["offer_id"],
            )
            related = [row for row in self.store.intents() if row.get("plan_hash") == dust_hash]
            if any(row["state"] == "AMBIGUOUS" for row in related):
                self.store.update_consolidation("AMBIGUOUS", "unknown replacement submit", now_ms)
                result_state = "AMBIGUOUS"
            elif submitted:
                result_state = "SUBMITTING"
            else:
                self.store.update_consolidation("READY", now_ms=now_ms)
                result_state = "READY"
            return {"blocking": True, "state": result_state, "canceled": [], "submitted": submitted}

        wallet = D(account["wallet"])
        bucket = now_ms // 300_000
        if self._last_dust_check_bucket == bucket:
            return {"blocking": False, "state": "IDLE", "canceled": [], "submitted": []}
        self._last_dust_check_bucket = bucket
        if wallet < DUST_REINVEST_MINIMUM or wallet >= USD_ORDER_CHUNK:
            return {"blocking": False, "state": "IDLE", "canceled": [], "submitted": []}
        if self.store.reprice_count_since(now_ms - 3_600_000) >= self.policy.max_reprices_per_hour:
            return {"blocking": False, "state": "RATE_LIMITED", "canceled": [], "submitted": []}
        candidates = []
        protections = self.store.manual_protections(now_ms)
        for offer in active_offers.values():
            if (protections.get(int(offer["offer_id"])) or {}).get("needsTakeover"):
                continue
            display = str(offer.get("display_type") or offer.get("offer_type") or "LIMIT").upper()
            age = now_ms - int(offer.get("mts_created") or now_ms)
            amount = D(offer["amount"])
            pool = str(offer.get("pool") or pool_for_period(offer["period"]))
            layer = str(offer.get("layer") or "balanced")
            last_family = self.store.last_reprice_for_family(pool, layer)
            cooling_down = (
                last_family is not None
                and now_ms - int(last_family) < self.policy.reprice_cooldown_minutes * 60_000
            )
            if (
                offer.get("managed")
                and display == "LIMIT"
                and int(offer["offer_id"]) not in self._pending_cancel_requested
                and not (self.store.reprice_chain_for_offer(int(offer["offer_id"])) or {}).get("pending_action")
                and not cooling_down
                and age >= self.policy.minimum_offer_minutes * 60_000
                and amount + wallet >= USD_ORDER_CHUNK
            ):
                candidates.append(offer)
        if not candidates:
            return {"blocking": False, "state": "NO_CANDIDATE", "canceled": [], "submitted": []}
        candidate = min(
            candidates,
            key=lambda offer: (
                {"short": 0, "medium": 1, "long": 2}.get(
                    offer.get("pool") or pool_for_period(offer["period"]),
                    3,
                ),
                D(offer["amount"]),
                int(offer.get("mts_created") or now_ms),
            ),
        )
        offer_id = int(candidate["offer_id"])
        source_chain = self.store.ensure_reprice_chain(candidate, strategy_version, now_ms)
        if source_chain is None or source_chain.get("pending_action"):
            return {"blocking": False, "state": "NO_CHAIN", "canceled": [], "submitted": []}
        if not self._manual_cancel_allowed(candidate, now_ms):
            return {"blocking": False, "state": "MANUAL_PROTECTED", "canceled": [], "submitted": []}
        self.store.begin_consolidation(
            offer_id,
            wallet,
            candidate["amount"],
            candidate["period"],
            strategy_version,
            now_ms,
        )
        result = _write_result(self.client, "cancel_funding_offer_result", "cancel_funding_offer", offer_id)
        if result.outcome == WriteOutcome.UNKNOWN:
            self.store.update_consolidation("AMBIGUOUS", result.error, now_ms)
            self.store.record_ownership_event(
                "CANCEL_UNKNOWN", offer_id=offer_id, details={"reason": "dust_consolidation", "error": result.error}
            )
            self.store.enter_protected_pause(f"AMBIGUOUS_CANCEL:{offer_id}")
            return {"blocking": True, "state": "AMBIGUOUS", "canceled": [], "submitted": []}
        if result.outcome == WriteOutcome.DEFINITE_REJECT:
            self.store.clear_consolidation(now_ms)
            return {"blocking": False, "state": "REJECTED", "canceled": [], "submitted": []}
        self.store.record_reprice(
            offer_id,
            "dust_consolidation",
            candidate.get("rate_real") or candidate["rate"],
            None,
            created_at_ms=now_ms,
            strategy_version=strategy_version,
            display_type=candidate.get("display_type"),
        )
        self.store.record_ownership_event(
            "CANCEL_CONFIRMED",
            offer_id=offer_id,
            details={
                "reason": "dust_consolidation",
                "wallet": format(wallet, "f"),
                "pool": source_chain["pool"],
                "layer": source_chain["layer"],
                "period": int(candidate["period"]),
                "chainKey": source_chain["chain_key"],
            },
        )
        self.store.update_consolidation("CANCELLING", now_ms=now_ms)
        self._pending_cancel_requested.add(offer_id)
        return {"blocking": True, "state": "CANCELLING", "canceled": [offer_id], "submitted": []}

    def _pending_adjustments(self, signals, account, pending_policy, pending_result, now_ms):
        """Return hard incompatibilities that must disappear before activation.

        These checks intentionally do not use the ordinary reprice age/cooldown.
        A PENDING policy is a safety boundary, not a market-timing suggestion.
        """
        adjustments = []
        confirmed_external = {int(row["offer_id"]) for row in self.store.external_takeovers(states={"CONFIRMED"})}
        managed_offers = [
            row for row in self.store.offers(active_only=True)
            if row["currency"] == "USD" and (row["managed"] or row["offer_id"] in confirmed_external)
        ]
        cap_excess = max(
            D("0"),
            D(account["existingExposure"]["total"]) - D(pending_result["funding_cap"]),
        )
        for offer in managed_offers:
            pool = offer.get("pool") or pool_for_period(offer["period"])
            display_type = offer.get("display_type") or self._offer_display_type(offer)
            is_hidden = bool(int(offer.get("flags") or 0) & 64)
            fee = pending_policy.hidden_fee_rate if is_hidden else pending_policy.normal_fee_rate
            floor_apr = pending_policy.floor_apr(pool) if pool in POOLS else None
            effective = funding_offer_effective_rate(offer, signals.get("frr_daily_rate"))
            offer["effective_rate"] = effective
            reasons = []
            if not self._display_type_enabled(pending_policy, display_type):
                reasons.append("disabled_type")
            if is_hidden and not pending_policy.enable_hidden:
                reasons.append("hidden_disabled")
            if pool not in POOLS or int(offer["period"]) not in pending_policy.periods(pool):
                reasons.append("period_not_allowed")
            if (floor_apr is not None and effective is not None
                    and rate_below_floor(effective, gross_daily_floor(floor_apr, fee))):
                reasons.append("below_new_floor")
            if reasons:
                adjustments.append((offer, None, "+".join(reasons)))

        if cap_excess > 0:
            already = {int(item[0]["offer_id"]) for item in adjustments}
            for offer in sorted(managed_offers, key=lambda row: D(row["amount"]), reverse=True):
                if cap_excess <= 0:
                    break
                if int(offer["offer_id"]) in already:
                    cap_excess -= D(offer["amount"])
                    continue
                adjustments.append((offer, None, "funding_cap_exceeded"))
                cap_excess -= D(offer["amount"])
        return adjustments

    def _cancel_hard_adjustments(
        self,
        adjustments,
        now_ms,
        reason_prefix="policy",
        strategy_version=None,
        plan_hash=None,
    ):
        canceled = []
        for offer, target, reason in adjustments[:10]:
            offer_id = int(offer["offer_id"])
            if offer_id in self._pending_cancel_requested:
                continue
            if not self._manual_cancel_allowed(offer, now_ms, safety=True):
                continue
            if not offer["managed"]:
                adopted = self.store.adopt_external_offers(
                    [dict(offer, id=offer_id)], strategy_version or "3", now_ms=now_ms, safety_reason=reason
                )
                if not adopted:
                    continue
            write = _write_result(self.client, "cancel_funding_offer_result", "cancel_funding_offer", offer_id)
            if write.outcome == WriteOutcome.UNKNOWN:
                self.store.record_ownership_event(
                    "CANCEL_UNKNOWN",
                    offer_id=offer_id,
                    details={"reason": f"{reason_prefix}:{reason}", "error": write.error},
                )
                self.store.enter_protected_pause(f"AMBIGUOUS_CANCEL:{offer_id}")
                self._log(f"撤销 Offer {offer_id} 结果未知，将自动对账后重试：{write.error}")
                break
            if write.outcome == WriteOutcome.DEFINITE_REJECT:
                self.store.record_ownership_event(
                    "CANCEL_REJECTED",
                    offer_id=offer_id,
                    details={"reason": f"{reason_prefix}:{reason}", "error": write.error},
                )
                self._log(f"撤销 Offer {offer_id} 被明确拒绝：{write.error}")
                continue
            self.store.record_reprice(
                offer_id,
                f"{reason_prefix}:{reason}",
                offer.get("effective_rate", funding_offer_effective_rate(offer)),
                None if target is None else target["effective_rate"],
                created_at_ms=now_ms,
                strategy_version=strategy_version,
                plan_hash=plan_hash,
                display_type=(offer.get("display_type") or self._offer_display_type(offer)),
            )
            self.store.finish_manual_protection(offer_id, f"{reason_prefix}:{reason}", now_ms)
            self._pending_cancel_requested.add(offer_id)
            self.store.record_ownership_event(
                "CANCEL_CONFIRMED", offer_id=offer_id, details={"reason": f"{reason_prefix}:{reason}"}
            )
            self._log(f"挂单 {offer_id} 正在执行策略强制调整：{reason_prefix}:{reason}。")
            canceled.append(offer_id)
        return canceled

    def _advance_pending_strategy(self, snapshot, account, signals, now_ms, *, refresh_market_clock=False):
        pending = self.store.strategy("PENDING")
        if pending is None:
            return None
        pending_policy = validate_policy_v3(
            policy_v3_with_overrides(StrategyPolicyV3(), pending["policy"]),
            require_live_floors=True,
        )
        v36_details = None
        if pending_policy.quick_pricing_mode == EXECUTABLE_QUICK_MODE:
            gate = validation_status(self.store, pending_policy)
            if not gate["approved"]:
                return {"pending": True, "blockedReason": "V36_VALIDATION_REQUIRED", "validation": gate}
            v36_details = {"strategyVersion": pending["version_id"], "reportDigest": gate["reportDigest"]}
        pending_signals = self._market_signals(
            snapshot, pending_policy, None if refresh_market_clock else now_ms
        )
        self._persist_period_selection(pending_signals, pending["version_id"], now_ms)
        pending_result = self._build_plan(account, pending_policy, pending_signals, pending["version_id"])
        adjustments = self._pending_adjustments(pending_signals, account, pending_policy, pending_result, now_ms)
        if not adjustments:
            try:
                self.store.activate_pending_strategy(
                    pending["version_id"], plan_hash=pending_result.get("plan_hash"),
                    reason="incompatible offers confirmed absent", v36_details=v36_details,
                )
            except StaleStrategyVersion:
                return {"pending": True, "blockedReason": "PREVIEW_STALE"}
            self.policy = pending_policy
            self._apply_policy_runtime_settings()
            if self.on_policy_activated is not None:
                try:
                    self.on_policy_activated(pending_policy, pending["version_id"])
                except Exception as exc:
                    self._log(f"V3 ACTIVE 已切换，但配置镜像写入失败：{exc}")
            self._log(f"v3 策略 {pending['version_id']} 已完成调整并生效。")
            return {"activated": pending["version_id"], "pending": False, "signals": pending_signals}
        latest = self.store.strategy("PENDING")
        if latest is None or latest["version_id"] != pending["version_id"]:
            return {"pending": True, "blockedReason": "PREVIEW_STALE"}
        canceled = self._cancel_hard_adjustments(
            adjustments,
            now_ms,
            "pending_strategy",
            strategy_version=pending["version_id"],
            plan_hash=pending_result.get("plan_hash"),
        )
        return {
            "pending": True,
            "versionId": pending["version_id"],
            "remainingAdjustments": max(0, len(adjustments) - len(canceled)),
            "canceled": canceled,
            "signals": pending_signals,
            "plan": pending_result,
        }

    def cycle(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        self.store.touch_heartbeat(now)
        if not self._bootstrapped:
            self.bootstrap(start_websocket=True)
        recovery = self.store.recovery_status()
        rest_due = now - self._last_rest_sync_ms >= self.policy.rest_stale_seconds * 1000
        if recovery["active"]:
            rest_due = self.store.recovery_probe_due(now)
        if rest_due:
            try:
                snapshot = self.sync_rest(now_ms=now)
            except BitfinexApiError as exc:
                decision = classify_runtime_error(exc)
                if not decision.retryable:
                    raise
                runtime = self.store.runtime()
                recovery = self.store.recovery_status()
                origin = recovery.get("targetMode") or runtime["mode"]
                if runtime["mode"] != "PAUSED":
                    self.store.set_mode("PAUSED", f"AUTO_RECOVERY:{decision.category}")
                self.store.begin_recovery(
                    decision.category,
                    str(exc),
                    origin_mode=origin,
                    target_mode=origin,
                )
                self.store.record_recovery_failure(str(exc), decision.category, now)
                snapshot = self.hub.snapshot(now)
                self._log(f"v3 REST 同步失败：{exc}")
        else:
            snapshot = self.hub.snapshot(now)
        if now_ms is None:
            now = int(self.clock() * 1000)
            snapshot = self.hub.snapshot(now)
        if snapshot["safeRequired"]:
            self.store.enter_protected_pause("MARKET_DATA_STALE")
        for offer in snapshot.get("offers", []):
            self.store.observe_manual_offer(offer, now)
        account = self._account(snapshot)
        runtime = self.store.runtime()
        if runtime["mode"] == "LIVE" and account["reconciliationStatus"] != "MATCHED":
            reason = (
                "ACCOUNT_AVAILABLE_BALANCE_UNKNOWN"
                if not account["walletAvailableKnown"]
                else "ACCOUNT_RECONCILIATION_MISMATCH"
            )
            self.store.enter_protected_pause(reason)
        signals_policy = self.policy
        signals = self._market_signals(snapshot, signals_policy, None if now_ms is None else now)
        self.store.record_market_bars(signals.get("windows"), now)
        self._record_variable_floor_violations(now)
        runtime = self.store.runtime()
        resume_barrier = self.store.consume_resume_barrier()
        if runtime["mode"] == "LIVE" and not resume_barrier:
            validate_policy_v3(self.policy, require_live_floors=True)
            pending_status = self._advance_pending_strategy(
                snapshot, account, signals, now, refresh_market_clock=now_ms is None
            )
            if pending_status and pending_status.get("pending"):
                signals = pending_status.get("signals") or signals
                result = pending_status.pop("plan", None)
                if result is None:
                    active = self.store.strategy("ACTIVE")
                    result = self._build_plan(account, self.policy, signals, active["version_id"] if active else "3")
                result["pendingStrategy"] = pending_status
                result["submitted"] = []
                canceled = pending_status.get("canceled", [])
            else:
                # Activation can happen above. Always reload ACTIVE and rebuild;
                # never submit the plan produced by the previous policy.
                strategy = self.store.strategy("ACTIVE")
                if strategy is not None:
                    self.policy = validate_policy_v3(
                        policy_v3_with_overrides(StrategyPolicyV3(), strategy["policy"]),
                        require_live_floors=True,
                    )
                    self._apply_policy_runtime_settings()
                version = strategy["version_id"] if strategy else "3"
                if self.policy != signals_policy:
                    if pending_status and pending_status.get("activated"):
                        signals = pending_status["signals"]
                    else:
                        signals = self._market_signals(snapshot, self.policy, None if now_ms is None else now)
                self._persist_period_selection(signals, version, now)
                result = self._build_plan(account, self.policy, signals, version)
                canceled = self._cancel_external_takeovers(now, version, result.get("plan_hash"))
                dust = {"blocking": False, "canceled": [], "submitted": [], "state": "IDLE"}
                hard_adjustments = []
                if not canceled:
                    dust = self._dust_consolidation(account, signals, now, version)
                    canceled = dust.get("canceled", [])
                    result["dustConsolidation"] = dust
                if not canceled and not dust.get("blocking"):
                    hard_adjustments = self._pending_adjustments(signals, account, self.policy, result, now)
                    canceled = self._cancel_hard_adjustments(
                        hard_adjustments,
                        now,
                        "active_policy",
                        strategy_version=version,
                        plan_hash=result.get("plan_hash"),
                    )
                    if not hard_adjustments:
                        canceled = self._cancel_reprice_candidates(
                            {"market": signals}, result, now, strategy_version=version
                        )
                if not canceled and not dust.get("blocking") and self.store.runtime()["mode"] == "LIVE":
                    # Requested hard cancellations remain blocking until a fresh
                    # authoritative account snapshot confirms the offers vanished.
                    if hard_adjustments:
                        result["submitted"] = []
                    else:
                        replacement_submitted, new_cash_available = self._submit_pending_reprices(
                            account["wallet"],
                            version,
                            result.get("cap_limited_available"),
                            frr_daily_rate=signals.get("frr_daily_rate"),
                        )
                        result["submitted"] = replacement_submitted + self._submit_plan(
                            result, new_cash_available, version
                        )
                elif dust.get("blocking"):
                    result["submitted"] = dust.get("submitted", [])
                else:
                    result["submitted"] = []
            result["canceledForReprice"] = canceled
        elif runtime["mode"] == "REPLAY":
            result = replay_strategy_v3(
                self.policy, snapshot["trades"], self._stats, account["total"], snapshot["book"], now
            )
        else:
            strategy = self.store.strategy("ACTIVE")
            version = strategy["version_id"] if strategy else "3"
            self._persist_period_selection(signals, version, now)
            result = self._build_plan(account, self.policy, signals, version)
            if resume_barrier:
                result["recoveryResumeBarrier"] = True
                result["submitted"] = []
        self.store.record_account_sample(
            account["total"], account["wallet"], account["offers"], account["credits"], self._net_interest_total(), now
        )
        status = self._strategy_status(snapshot, signals, result)
        if self.log is not None:
            self.log.persistStatus()
        self.store.touch_heartbeat(now)
        return status
