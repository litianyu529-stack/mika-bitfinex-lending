"""Deterministic, offline funding replay with an explicit cash and order ledger.

Public prints and aggregate books cannot reveal the exchange's actual queue.
Queue and repayment settings here are named sensitivity assumptions, never
historical fills or confidence bounds.  This module has no exchange client.
"""

from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from decimal import Decimal
from bisect import bisect_right
from types import SimpleNamespace
import heapq
import random
import time

from StrategyV3 import (
    EXACT_TERM_EXPLORATION_CURVE,
    EXACT_TERM_EXPLORATION_CURVES,
    POOLS,
    USD_ORDER_CHUNK,
    build_market_signals_v3,
    build_strategy_plan_v3,
    ceil_rate_tick,
    competitive_rate_for_period,
    gross_daily_floor,
    json_decimal,
    pool_for_period,
    validate_policy_v3,
)

D = Decimal
DAY_MS = 86_400_000
MINUTE_MS = 60_000
WEEK_MS = 7 * DAY_MS


class ReplayStrategyState:
    """In-memory store adapter for the exact LIVE state/payload transitions."""
    def __init__(self):
        self.periods, self.demand = {}, {}

    def observe_demand_confirmation(self, version, scope, key, share, below_threshold, now_ms):
        from StrategyState import demand_confirmation_transition
        identity = (scope, key)
        state = demand_confirmation_transition(self.demand.get(identity), below_threshold, now_ms)
        self.demand[identity] = state
        return {"cycles": state["consecutive_cycles"], "confirmed": state["confirmed"], "cycle": state["last_cycle"]}

    def observe_period_selection(self, version, pool, selected_period, scores, now_ms):
        from StrategyState import period_selection_transition
        state = period_selection_transition(self.periods.get(pool), selected_period, scores, now_ms)
        self.periods[pool] = state
        since = state["challenger_since_ms"]
        return {"strategyVersion": version, "pool": pool, "selectedPeriod": state["selected_period"],
                "selectedSinceMs": state["selected_since_ms"], "leaderPeriod": state["leader_period"],
                "challengerPeriod": state["challenger_period"], "challengerSinceMs": since,
                "challengerDurationMs": 0 if since is None else max(0, now_ms - since),
                "promoted": state["promoted"], "updatedAtMs": now_ms}

    def apply(self, signals, now_ms):
        from RuntimeV3 import LendingRuntimeV3
        return LendingRuntimeV3._persist_period_selection(SimpleNamespace(store=self), signals, "event-replay", now_ms)


@dataclass(frozen=True)
class ReplayAssumptions:
    queue_multiplier: int = 1
    delay_seconds: int = 60
    repayment_mode: str = "contract"
    signal_mode: str = "full_7d"
    control_seconds: int = 60
    fresh_book_seconds: int = 60
    seed: int = 307
    collect_events: bool = True
    require_full_window: bool = True
    interest_settlement_hour_utc: int = 0

    def __post_init__(self):
        if self.queue_multiplier not in (0, 1, 2):
            raise ValueError("queue_multiplier must be 0, 1, or 2")
        if self.delay_seconds < 0 or self.control_seconds <= 0 or self.fresh_book_seconds <= 0:
            raise ValueError("replay clock intervals must be positive (delay may be zero)")
        if self.repayment_mode not in {"contract", "calibrated"}:
            raise ValueError("unknown repayment mode")
        if self.signal_mode not in {"legacy_20k", "full_7d"}:
            raise ValueError("unknown signal mode")
        if not 0 <= self.interest_settlement_hour_utc <= 23:
            raise ValueError("settlement hour must be in UTC 0..23")


def _assumptions(value):
    if isinstance(value, ReplayAssumptions):
        return value
    return ReplayAssumptions(**(value or {}))


def snapshot_visible_at(row):
    """Old minute buckets become visible at their end, never their beginning."""
    observed = row.get("observed_at_ms", row.get("observedAtMs"))
    return int(observed) if observed is not None else int(row["mts"]) + MINUTE_MS


def _iter_ordered(rows, key):
    # Lists are a convenient fixture/API input; store iterators are already
    # ordered and deliberately stay streaming for multi-million-print histories.
    values = sorted(rows, key=key) if isinstance(rows, (list, tuple)) else rows
    previous = None
    for row in values or ():
        current = key(row)
        if previous is not None and current < previous:
            raise ValueError("replay inputs must be chronological")
        previous = current
        yield row


class _Peek:
    def __init__(self, rows, key):
        self.rows = iter(_iter_ordered(rows, key))
        self.key = key
        self.row = next(self.rows, None)

    @property
    def timestamp(self):
        return self.key(self.row) if self.row is not None else float("inf")

    def pop(self):
        row = self.row
        self.row = next(self.rows, None)
        return row


def calibrate_repayments(records, train_start_ms, train_end_ms, minimum_samples=30):
    """Freeze empirical lifetimes using only closures observable in training.

    Closure observations are upper bounds on the actual repayment time.  Open
    credits are censored, not observations of early repayment.  The report keeps
    that limitation instead of treating this as a fitted borrower survival model.
    """
    samples = defaultdict(list)
    for row in records or ():
        opened = row.get("opened_at_ms", row.get("mts_opening"))
        closed = row.get("closed_at_ms", row.get("mts_closed"))
        observed = row.get("observed_at_ms", closed)
        if opened is None or closed is None or observed is None:
            continue
        opened, closed, observed = int(opened), int(closed), int(observed)
        if not (int(train_start_ms) <= closed < int(train_end_ms) and observed < int(train_end_ms)):
            continue
        period = int(row.get("period", 0))
        if period <= 0 or opened >= closed:
            continue
        duration = min(period * DAY_MS, closed - opened)
        samples[period].append(duration)
    return {
        "trainingStartMs": int(train_start_ms),
        "trainingEndMs": int(train_end_ms),
        "minimumSamples": int(minimum_samples),
        "periods": {
            str(period): {"durationsMs": sorted(values), "samples": len(values),
                          "usable": len(values) >= minimum_samples}
            for period, values in sorted(samples.items())
        },
        "limitation": "Observed closures bound repayment time; closed-loan samples have survivor-selection bias.",
    }


def book_coverage(book_snapshots, start_ms, end_ms, *, segments=None, fresh_book_seconds=60):
    """Compute union of fresh observation intervals without decoding book JSON."""
    boundaries = segments or {"all": (int(start_ms), int(end_ms))}
    intervals = []
    for row in _iter_ordered(book_snapshots, snapshot_visible_at):
        visible = snapshot_visible_at(row)
        expiry = visible + int(fresh_book_seconds) * 1000
        left, right = max(int(start_ms), visible), min(int(end_ms), expiry)
        if right <= left:
            continue
        if intervals and left <= intervals[-1][1]:
            intervals[-1] = (intervals[-1][0], max(right, intervals[-1][1]))
        else:
            intervals.append((left, right))
    result = {}
    for name, (start, end) in boundaries.items():
        covered = 0
        days = set()
        cursor = int(start)
        max_gap = 0
        for left, right in intervals:
            left, right = max(left, int(start)), min(right, int(end))
            if right <= left:
                continue
            max_gap = max(max_gap, left - cursor)
            covered += right - left
            cursor = right
            days.update(range(left // DAY_MS, (right - 1) // DAY_MS + 1))
        max_gap = max(max_gap, int(end) - cursor)
        span = max(0, int(end) - int(start))
        result[name] = {
            "freshMs": covered,
            "freshDays": D(covered) / D(DAY_MS),
            "freshCoveragePercent": None if span == 0 else D(covered) * D(100) / D(span),
            "freshNaturalDays": len(days),
            "maximumStaleGapMs": max_gap,
            "meetsMinimum": covered >= WEEK_MS and len(days) >= 7,
        }
    return json_decimal(result)


def run_event_replay(
    policy, trades, stats, principal, *, book_snapshots=None, now_ms=None,
    window_ms=None, assumptions=None, repayment_events=None, start_ms=None,
    segments=None, initial_orders=None, signal_provider=None, plan_builder=None,
    repayment_calibration=None, progress=None,
    market_coverage=None,
    confirmation_events=None,
):
    """Replay one continuous account; split boundaries never reset cash/loans.

    ``signal_provider(asof, book, history, stats, policy, signal_mode)`` may supply
    a shared feature tape. Inputs before start are signal warmup only. Optional
    ``initial_orders`` and explicit repayment events support deterministic audits
    and tests; they are not inferred counterfactual historical fills.
    """
    validate_policy_v3(policy)
    cfg = _assumptions(assumptions)
    end = int(now_ms if now_ms is not None else time.time() * 1000)
    start = int(start_ms if start_ms is not None else end - int(window_ms or WEEK_MS))
    if end <= start:
        raise ValueError("replay window must be positive")
    principal = D(str(principal))
    if principal <= 0:
        raise ValueError("replay principal must be positive")
    boundaries = segments or {"all": (start, end)}
    if any(not start <= int(left) < int(right) <= end for left, right in boundaries.values()):
        raise ValueError("segment bounds must be inside replay")
    planner = plan_builder or build_strategy_plan_v3
    market = _Peek(trades, lambda row: int(row["mts"]))
    books = _Peek(book_snapshots or (), snapshot_visible_at)
    stat = _Peek(stats or (), lambda row: int(row.get("mts", row.get("timestamp", 0)) or 0))
    repayments = _Peek(repayment_events or (), lambda row: int(row["mts"]))
    confirmations = _Peek(confirmation_events or (), lambda row: int(row["mts"]))
    has_books = books.row is not None
    # Full-window histograms retain their own exact events. The raw deque only
    # serves legacy pricing unless a caller explicitly provides a raw-history
    # function, so normal research does not duplicate a seven-day event tree.
    raw_full_history = signal_provider is not None and not hasattr(signal_provider, "get_signal")
    history = deque(maxlen=None if raw_full_history and cfg.signal_mode == "full_7d" else 20_000)
    rolling = None
    legacy_rolling = None
    detailed_periods = {period for pool in POOLS for period in policy.periods(pool)}
    if signal_provider is None or getattr(signal_provider, "needs_snapshot", False):
        from MarketHistory import RollingMarketWindows
        legacy_rolling = RollingMarketWindows(max_trades=20_000, cleaning_mode="legacy_global",
                                              detailed_periods=detailed_periods)
    if cfg.signal_mode == "full_7d" and (market_coverage or {}).get("coveredIntervals") and (
        signal_provider is None or getattr(signal_provider, "needs_snapshot", False)
    ):
        from MarketHistory import RollingMarketWindows
        rolling = RollingMarketWindows(detailed_periods=detailed_periods)
    latest_book = []
    latest_book_time = None
    latest_stat = None
    first_trade_mts = None
    unknown_side = 0
    trade_count = 0
    last_signals = None
    strategy_state = ReplayStrategyState()
    orders, credits, order_log, fills, returns, events = {}, {}, [], [], [], []
    match_orders = {}
    queues = {}
    delayed = []
    serial = 0
    cash = principal
    earned = D(0)
    interest_due = D(0)
    cash_interest = D(0)
    rate_income = D(0)
    active_amount = D(0)
    long_amount = D(0)
    total_loan_time = D(0)
    total_idle_time = D(0)
    reserved = D(0)
    randomizer = random.Random(cfg.seed)
    last_reprices = {}
    reprice_times = deque()
    calibration_missing = set()
    window_fallback_decisions = 0
    complete_intervals = []
    for interval in (market_coverage or {}).get("coveredIntervals", []):
        left = int(interval.get("startMs", interval.get("start_ms", 0)))
        right = int(interval.get("endMs", interval.get("end_ms", 0)))
        if right > left:
            if complete_intervals and left <= complete_intervals[-1][1]:
                complete_intervals[-1] = (complete_intervals[-1][0], max(right, complete_intervals[-1][1]))
            else:
                complete_intervals.append((left, right))
    interval_starts = [item[0] for item in complete_intervals]

    def window_complete(mts):
        position = bisect_right(interval_starts, mts - WEEK_MS) - 1
        return position >= 0 and complete_intervals[position][1] >= mts
    daily = []
    daily_start = start
    previous_daily_interest = D(0)
    control_ms = cfg.control_seconds * 1000
    tick = start
    current = start
    next_day = (start // DAY_MS + 1) * DAY_MS
    phase = cfg.interest_settlement_hour_utc * 3_600_000
    settlement = ((start - phase) // DAY_MS + 1) * DAY_MS + phase
    metrics = {
        name: {"startMs": int(bounds[0]), "endMs": int(bounds[1]), "netInterest": D(0),
               "principalTime": D(0), "loanPrincipalTime": D(0), "longPrincipalTime": D(0),
               "offerPrincipalTime": D(0), "idlePrincipalTime": D(0), "cashInterest": D(0),
               "fillCount": 0, "fillAmount": D(0), "weightedWaitMs": D(0),
               "submittedCount": 0, "cancelRequested": 0, "cancelConfirmed": 0,
               "freshBookMs": 0, "freshDates": set()}
        for name, bounds in boundaries.items()
    }

    def emit(kind, mts, **details):
        if cfg.collect_events:
            events.append({"type": kind, "mts": int(mts), **details})

    def segment_event(mts, key, value=1):
        for value_metrics in metrics.values():
            if value_metrics["startMs"] <= mts < value_metrics["endMs"]:
                value_metrics[key] += value

    def schedule(mts, kind, payload):
        nonlocal serial
        serial += 1
        heapq.heappush(delayed, (int(mts), serial, kind, payload))

    def queue_key(order):
        return (int(order["period"]), D(order["effective_rate"]))

    def curve(order):
        return str(order.get("pricing_curve_version") or EXACT_TERM_EXPLORATION_CURVE)

    def initialize_queue(order):
        period, rate = queue_key(order)
        first = not any(other["order_id"] != order["order_id"] and int(other["period"]) == period
                        for other in match_orders.values())
        if first:
            for key in [key for key in queues if key[0] == period]:
                queues.pop(key, None)
        # External supply is represented once at each price level, rather than
        # copying cumulative depth into every differently priced robot order.
        for row in latest_book:
            if int(row.get("period", 0)) != period or D(row.get("amount", 0)) <= 0:
                continue
            external_rate = D(row["rate"])
            if external_rate <= rate:
                key = (period, external_rate)
                if key not in queues:
                    queues[key] = abs(D(row["amount"])) * cfg.queue_multiplier

    def submit(row, mts, *, predecessor=None):
        nonlocal serial, cash, reserved
        amount = min(D(row["amount"]), cash)
        if amount < USD_ORDER_CHUNK:
            return None
        serial += 1
        ident = str(row.get("order_id") or f"o{serial}")
        if ident in orders:
            raise ValueError("duplicate replay order id")
        pool = row.get("pool") or pool_for_period(int(row["period"]))
        fee = policy.hidden_fee_rate if row.get("hidden") or int(row.get("flags", 0)) & 64 else policy.normal_fee_rate
        floor = ceil_rate_tick(gross_daily_floor(policy.floor_apr(pool), fee))
        rate = D(row.get("effective_rate", row.get("rate", 0)))
        if rate < floor:
            raise ValueError("replay order below configured floor")
        inherited = predecessor or {}
        order = {
            **row, "order_id": ident, "amount": amount, "remaining": amount,
            "pool": pool, "layer": row.get("layer", "quick"), "effective_rate": rate,
            "fee": fee, "state": "SUBMITTING", "created_at_ms": int(mts),
            "chain_started_at_ms": int(inherited.get("chain_started_at_ms", mts)),
            "origin_rate": D(inherited.get("origin_rate", rate)),
            "fixed_landing_rate": inherited.get("fixed_landing_rate", row.get("fixed_landing_rate")),
            "pricing_curve_version": inherited.get("pricing_curve_version", row.get("pricing_curve_version"))
            or EXACT_TERM_EXPLORATION_CURVE,
            "stage": int(inherited.get("stage", 0)),
            "cancel_confirmation_complete": False,
        }
        orders[ident] = order
        order_log.append(order)
        cash -= amount
        reserved += amount
        emit("ORDER_SUBMITTED", mts, orderId=ident, amount=amount, rate=rate, period=order["period"])
        segment_event(mts, "submittedCount")
        schedule(mts + cfg.delay_seconds * 1000, "ACCEPT", ident)
        return order

    for row in initial_orders or ():
        # Explicit initial orders are known active at start, unlike new submits.
        order = submit(row, start)
        if order:
            order["state"] = "ACTIVE"
            order["chain_started_at_ms"] = int(row.get("chain_started_at_ms", row.get("created_at_ms", start)))

    def add_fill(order, amount, mts, trade_id):
        nonlocal reserved, rate_income, active_amount, long_amount, serial
        amount = min(amount, order["remaining"])
        if amount <= 0:
            return
        advance(mts)
        order["remaining"] -= amount
        reserved -= amount
        if order["remaining"] == 0:
            order["state"] = "FILLED"
            match_orders.pop(order["order_id"], None)
        serial += 1
        ident = f"c{serial}"
        duration = int(order["period"]) * DAY_MS
        if cfg.repayment_mode == "calibrated":
            entry = (repayment_calibration or {}).get("periods", {}).get(str(order["period"]), {})
            # A training-fitted distribution must not be used before it existed.
            trained_at = int((repayment_calibration or {}).get("trainingEndMs", end + 1))
            if entry.get("usable") and len(entry.get("durationsMs", [])) >= 30 and mts >= trained_at:
                duration = min(duration, int(randomizer.choice(entry["durationsMs"])))
            elif mts >= trained_at:
                calibration_missing.add(int(order["period"]))
        credit = {"credit_id": ident, "order_id": order["order_id"], "amount": amount,
                  "original_amount": amount, "pool": order["pool"], "layer": order["layer"],
                  "period": int(order["period"]), "effective_rate": order["effective_rate"],
                  "fee": order["fee"], "hidden": bool(order.get("hidden", False)),
                  "offer_type": order.get("offer_type", "LIMIT"),
                  "fill_mts": int(mts), "return_mts": int(mts) + duration}
        credits[ident] = credit
        rate_income += amount * credit["effective_rate"] * (D(1) - credit["fee"])
        active_amount += amount
        if credit["pool"] == "long":
            long_amount += amount
        fill = {**credit, "trade_id": trade_id, "wait_ms": max(0, mts - order["chain_started_at_ms"])}
        fills.append(fill)
        emit("PARTIAL_FILL" if order["remaining"] > 0 else "FILL", mts, **fill)
        segment_event(mts, "fillCount")
        segment_event(mts, "fillAmount", amount)
        segment_event(mts, "weightedWaitMs", amount * fill["wait_ms"])
        schedule(credit["return_mts"], "RETURN", {"credit_id": ident})

    def process_trade(row, mts):
        if not match_orders:
            return
        volume = abs(D(row["amount"]))
        period, rate = int(row.get("period", 2)), D(row["rate"])
        eligible = sorted((o for o in match_orders.values()
                           if o["state"] in {"ACTIVE", "CANCEL_PENDING", "CANCEL_UNKNOWN"}
                           and o["remaining"] > 0 and int(o["period"]) == period and o["effective_rate"] <= rate),
                          key=lambda o: (o["effective_rate"], o["created_at_ms"], o["order_id"]))
        for order in eligible:
            external_levels = sorted(key for key, amount in queues.items()
                                     if key[0] == period and key[1] <= order["effective_rate"] and amount > 0)
            for key in external_levels:
                ahead = min(volume, queues[key])
                queues[key] -= ahead
                volume -= ahead
                if volume <= 0:
                    break
            if volume <= 0:
                break
            amount = min(volume, order["remaining"])
            add_fill(order, amount, mts, row.get("id"))
            volume -= amount

    def return_credit(payload, mts):
        nonlocal cash, rate_income, active_amount, long_amount
        if payload.get("credit_id") is None and payload.get("order_id") is not None:
            remaining = D(payload.get("amount", sum((c["amount"] for c in credits.values()
                                                      if c["order_id"] == str(payload["order_id"])), D(0))))
            for child in sorted(list(credits.values()), key=lambda c: (c["fill_mts"], c["credit_id"])):
                if child["order_id"] == str(payload["order_id"]) and remaining > 0:
                    amount = min(child["amount"], remaining)
                    return_credit({"credit_id": child["credit_id"], "amount": amount}, mts)
                    remaining -= amount
            return
        ident = str(payload.get("credit_id", ""))
        credit = credits.get(ident)
        if credit is None or credit["amount"] <= 0:
            return
        amount = min(credit["amount"], D(payload.get("amount", credit["amount"])))
        if amount <= 0:
            return
        advance(mts)
        credit["amount"] -= amount
        rate_income -= amount * credit["effective_rate"] * (D(1) - credit["fee"])
        active_amount -= amount
        if credit["pool"] == "long":
            long_amount -= amount
        cash += amount
        record = {**credit, "amount": amount, "returned_at_ms": int(mts),
                  "early": int(mts) < credit["fill_mts"] + credit["period"] * DAY_MS}
        returns.append(record)
        emit("PRINCIPAL_RETURN", mts, **record)
        if credit["amount"] == 0:
            credits.pop(ident, None)
        if not credits:
            rate_income = D(0)

    def advance(mts):
        nonlocal current, earned, interest_due, total_loan_time, total_idle_time
        if mts <= current:
            return
        duration = D(mts - current) / D(DAY_MS)
        increment = rate_income * duration
        earned += increment
        interest_due += increment
        total_loan_time += active_amount * duration
        total_idle_time += (cash + reserved) * duration
        for item in metrics.values():
            left, right = max(current, item["startMs"]), min(mts, item["endMs"])
            if right <= left:
                continue
            days = D(right - left) / D(DAY_MS)
            item["netInterest"] += rate_income * days
            item["principalTime"] += principal * days
            item["loanPrincipalTime"] += active_amount * days
            item["longPrincipalTime"] += long_amount * days
            item["offerPrincipalTime"] += reserved * days
            item["idlePrincipalTime"] += (cash + reserved) * days
            if latest_book_time is not None:
                fresh_end = min(right, latest_book_time + cfg.fresh_book_seconds * 1000)
                fresh_start = max(left, latest_book_time)
                if fresh_end > fresh_start:
                    item["freshBookMs"] += fresh_end - fresh_start
                    item["freshDates"].update(range(fresh_start // DAY_MS, (fresh_end - 1) // DAY_MS + 1))
        current = mts

    def account_point(mts):
        return {"mts": int(mts), "cash": cash, "reserved": reserved,
                "loanPrincipal": active_amount, "accruedNetInterest": interest_due,
                "cashInterest": cash_interest, "netInterest": earned,
                "equity": cash + reserved + active_amount + interest_due}

    def check_ledger():
        tolerance = D("0.00000001")
        if min(cash, reserved, active_amount) < -tolerance:
            raise AssertionError("replay negative cash, reservation or loan principal")
        difference = cash + reserved + active_amount + interest_due - principal - earned
        if abs(difference) > tolerance:
            raise AssertionError(f"replay equity conservation failed: {difference}")

    def confirm(action, outcome, ident, mts):
        nonlocal cash, reserved
        order = orders.get(str(ident))
        if order is None:
            raise ValueError("confirmation references an unknown replay order")
        action, outcome = str(action).upper(), str(outcome).upper()
        if action not in {"SUBMIT", "CANCEL"} or outcome not in {"UNKNOWN", "CONFIRMED", "ABSENT"}:
            raise ValueError("unknown replay confirmation action/outcome")
        if outcome == "UNKNOWN":
            order["unknown_confirmation"] = action
            order["state"] = "SUBMIT_UNKNOWN" if action == "SUBMIT" else "CANCEL_UNKNOWN"
            if action == "SUBMIT":
                match_orders.pop(order["order_id"], None)
            emit(f"{action}_UNKNOWN", mts, orderId=order["order_id"])
            return
        order.pop("unknown_confirmation", None)
        if action == "SUBMIT" and outcome == "CONFIRMED":
            if order["state"] in {"SUBMITTING", "SUBMIT_UNKNOWN"}:
                order["state"] = "ACTIVE"
                match_orders[order["order_id"]] = order
                initialize_queue(order)
                emit("ORDER_CONFIRMED", mts, orderId=order["order_id"])
        elif action == "SUBMIT":
            if order["state"] not in {"SUBMITTING", "SUBMIT_UNKNOWN"}:
                return
            cash += order["remaining"]
            reserved -= order["remaining"]
            order["state"] = "ABSENT"
            emit("SUBMIT_ABSENT", mts, orderId=order["order_id"], releasedAmount=order["remaining"])
        elif (not order.get("cancel_confirmation_complete")
              and order["state"] in {"CANCEL_PENDING", "CANCEL_UNKNOWN", "FILLED"}):
            remainder = order["remaining"]
            cash += remainder
            reserved -= remainder
            order["state"] = ("REPLACE_READY" if remainder >= USD_ORDER_CHUNK
                              else "CANCELED" if remainder > 0 else "FILLED")
            order["cancel_confirmation_complete"] = True
            match_orders.pop(order["order_id"], None)
            segment_event(mts, "cancelConfirmed")
            emit("CANCEL_CONFIRMED", mts, orderId=order["order_id"], releasedAmount=remainder)

    def get_signals(mts):
        nonlocal last_signals, window_fallback_decisions
        visible_stats = [] if latest_stat is None else [latest_stat]
        rows = list(history)
        if cfg.signal_mode == "legacy_20k":
            rows = rows[-20_000:]
        complete = cfg.signal_mode == "full_7d" and window_complete(mts)
        if cfg.signal_mode == "full_7d" and not complete:
            window_fallback_decisions += 1

        def snapshot_factory():
            if complete:
                snapshot = rolling.snapshot(policy, mts, coverage_complete=complete)
                snapshot.update(usable=complete, state="READY" if complete else "WARMING")
                return snapshot
            snapshot = legacy_rolling.snapshot(policy, mts, coverage_complete=False)
            snapshot.update(usable=True, state="LEGACY_INPUT")
            return snapshot

        if signal_provider and hasattr(signal_provider, "get_signal"):
            last_signals = signal_provider.get_signal(mts, latest_book, rows, visible_stats, policy,
                                                      cfg.signal_mode, snapshot_factory=snapshot_factory)
        elif signal_provider:
            last_signals = signal_provider(mts, latest_book, rows, visible_stats, policy, cfg.signal_mode)
        else:
            last_signals = build_market_signals_v3(latest_book, [], visible_stats, policy, mts,
                                                  window_snapshot=snapshot_factory())
        return last_signals

    def decide(mts):
        nonlocal last_signals
        fresh = latest_book_time is not None and mts - latest_book_time < cfg.fresh_book_seconds * 1000
        if not fresh:
            return
        signals = strategy_state.apply(get_signals(mts), mts)
        if any(order.get("unknown_confirmation") for order in orders.values()):
            return
        while history.maxlen is None and history and int(history[0]["mts"]) < mts - WEEK_MS:
            history.popleft()
        due = []
        for order in match_orders.values():
            if order["state"] != "ACTIVE" or order["remaining"] < USD_ORDER_CHUNK:
                continue
            stages = policy.reprice_stages(order["pool"], order["layer"], curve(order))
            stage = int(order["stage"])
            next_stage = min(stage + 1, len(stages))
            if mts >= order["chain_started_at_ms"] + int(stages[next_stage - 1]) * MINUTE_MS:
                floor = ceil_rate_tick(gross_daily_floor(policy.floor_apr(order["pool"]), order["fee"]))
                if stage < len(stages) or order["effective_rate"] - floor >= policy.minimum_rate_change:
                    due.append(order)
        pending = [o for o in orders.values() if o["state"] == "REPLACE_READY"]
        if not due and cash < USD_ORDER_CHUNK:
            return
        # RuntimeV3 exports this pure curve calculation; importing it does not
        # construct a runtime or exchange client.
        from RuntimeV3 import _age_stage_target, _exploration_age_stage_target
        while reprice_times and reprice_times[0] <= mts - 3_600_000:
            reprice_times.popleft()
        due.sort(key=lambda o: (o["chain_started_at_ms"] + policy.reprice_stages(
            o["pool"], o["layer"], curve(o))[-1] * MINUTE_MS,
                              o["chain_started_at_ms"], o["order_id"]))
        for order in due:
            stages = policy.reprice_stages(order["pool"], order["layer"], curve(order))
            stage = min(int(order["stage"]) + 1, len(stages))
            floor = ceil_rate_tick(gross_daily_floor(policy.floor_apr(order["pool"]), order["fee"]))
            benchmark = competitive_rate_for_period(order["layer"], order["pool"], int(order["period"]),
                                                    signals, floor, curve(order))
            exploration = curve(order) in EXACT_TERM_EXPLORATION_CURVES
            if exploration and order["fixed_landing_rate"] is None:
                order["fixed_landing_rate"] = max(floor, min(order["origin_rate"], benchmark))
            landing = (policy.balanced_landing_stage if order["layer"] in {"quick", "balanced"}
                       else policy.high_landing_stage)
            if exploration:
                target = _exploration_age_stage_target(stage, order, benchmark, floor, landing, len(stages))
            else:
                if stage > 3 and order.get("market_anchor_rate") is None:
                    order["market_anchor_rate"] = order["effective_rate"]
                target = _age_stage_target(stage, order, benchmark, floor,
                                           fallback_anchor=order["effective_rate"], stage_count=len(stages))
            target = ceil_rate_tick(max(floor, min(order["effective_rate"], target)))
            if order["effective_rate"] - target < policy.minimum_rate_change:
                order["stage"] = stage
                if not exploration and stage == 3:
                    order["market_anchor_rate"] = order["effective_rate"]
                emit("REPRICE_STAGE_SKIPPED", mts, orderId=order["order_id"], stage=stage)
                continue
            family = (order["pool"], order["layer"])
            final = stage >= len(stages)
            if not final and (len(reprice_times) >= policy.max_reprices_per_hour or
                              mts - last_reprices.get(family, -10**18) < policy.reprice_cooldown_minutes * MINUTE_MS):
                continue
            order["state"] = "CANCEL_PENDING"
            order["replacement_rate"] = target
            order["replacement_stage"] = stage
            if not exploration and stage == 3:
                order["market_anchor_rate"] = target
            reprice_times.append(mts)
            last_reprices[family] = mts
            segment_event(mts, "cancelRequested")
            emit("CANCEL_REQUEST", mts, orderId=order["order_id"], targetRate=target, stage=stage)
            schedule(mts + cfg.delay_seconds * 1000, "CANCEL", order["order_id"])
        for source in pending:
            if source["remaining"] >= USD_ORDER_CHUNK and cash >= source["remaining"]:
                replacement = submit({**source, "order_id": None, "amount": source["remaining"],
                                      "effective_rate": source["replacement_rate"]}, mts, predecessor={
                                          **source, "stage": source["replacement_stage"]})
                if replacement:
                    source["state"] = "REPLACED"
            elif source["remaining"] < USD_ORDER_CHUNK:
                source["state"] = "CANCELED"
        # A canceled replacement's reserved cash is protected from global
        # replanning until its original term/layer can be restored.
        protected = sum((o["remaining"] for o in orders.values() if o["state"] == "REPLACE_READY"), D(0))
        available = max(D(0), cash - protected)
        if available < USD_ORDER_CHUNK:
            return
        exposure = {pool: D(0) for pool in POOLS}
        layers = defaultdict(lambda: D(0))
        offer_pools = {pool: D(0) for pool in POOLS}
        offer_layers = defaultdict(lambda: D(0))
        offer_periods = defaultdict(lambda: D(0))
        hidden_exposure, variable_exposure = D(0), D(0)
        for credit in credits.values():
            exposure[credit["pool"]] += credit["amount"]
            layers[credit["layer"]] += credit["amount"]
            if credit.get("hidden"):
                hidden_exposure += credit["amount"]
            if credit.get("offer_type") == "FRRDELTAVAR":
                variable_exposure += credit["amount"]
        for order in orders.values():
            if order["state"] not in {"ACTIVE", "SUBMITTING", "CANCEL_PENDING"}:
                continue
            amount, pool, layer = order["remaining"], order["pool"], order["layer"]
            exposure[pool] += amount
            layers[layer] += amount
            offer_pools[pool] += amount
            offer_layers[layer] += amount
            offer_periods[int(order["period"])] += amount
            if order.get("hidden") or int(order.get("flags", 0)) & 64:
                hidden_exposure += amount
            if order.get("offer_type") == "FRRDELTAVAR":
                variable_exposure += amount
        plan = planner(cash + reserved + active_amount, available, exposure, policy, signals, "event-replay",
                       existing_exposure={"total": reserved + active_amount, "hidden": hidden_exposure,
                                          "variable": variable_exposure}, exposure_by_layer=layers,
                       offer_exposure_by_pool=offer_pools, offer_exposure_by_layer=offer_layers,
                       offer_exposure_by_period=offer_periods)
        for row in plan.get("plan", []):
            submit(row, mts)

    # Consume warmup only. Nothing before start can create simulated profit.
    while market.timestamp < start:
        row = market.pop()
        timestamp = int(row["mts"])
        if first_trade_mts is None:
            first_trade_mts = timestamp
        if timestamp >= start - WEEK_MS:
            history.append(row)
            if rolling is not None:
                rolling.add([row])
            if legacy_rolling is not None:
                legacy_rolling.add([row])
    while books.timestamp <= start:
        row = books.pop()
        latest_book, latest_book_time = row.get("book", []), snapshot_visible_at(row)
    while stat.timestamp <= start:
        latest_stat = stat.pop()
    while repayments.timestamp < start:
        repayments.pop()
    while confirmations.timestamp < start:
        confirmations.pop()
    for order in orders.values():
        if order["state"] == "ACTIVE":
            match_orders[order["order_id"]] = order
            initialize_queue(order)

    while current < end:
        next_internal = delayed[0][0] if delayed else float("inf")
        mts = int(min(end, tick, next_day, settlement, market.timestamp, books.timestamp,
                      stat.timestamp, repayments.timestamp, confirmations.timestamp, next_internal))
        # Most public prints do not change this account. Integrate cash/interest
        # only at fills, book changes and clock/loan events, rather than doing
        # Decimal principal-time arithmetic for millions of unrelated prints.
        if mts in (end, tick, next_day, settlement, books.timestamp, next_internal,
                   repayments.timestamp, confirmations.timestamp):
            advance(mts)
        # Existing orders can still fill at a cancel-confirmation timestamp;
        # newly confirmed orders cannot consume a print from that timestamp.
        while market.timestamp == mts:
            row = market.pop()
            if mts >= end:
                continue
            if first_trade_mts is None:
                first_trade_mts = mts
            trade_count += 1
            if row.get("side") is None and row.get("raw_signed_amount") is None:
                unknown_side += 1
            process_trade(row, mts)
            history.append(row)
            if rolling is not None:
                rolling.add([row])
            if legacy_rolling is not None:
                legacy_rolling.add([row])
        while books.timestamp == mts:
            row = books.pop()
            latest_book, latest_book_time = row.get("book", []), mts
        while stat.timestamp == mts:
            latest_stat = stat.pop()
        while repayments.timestamp == mts:
            return_credit(repayments.pop(), mts)
        while confirmations.timestamp == mts:
            event = confirmations.pop()
            confirm(event["action"], event["outcome"], event["order_id"], mts)
        while delayed and delayed[0][0] <= mts:
            _, _, kind, payload = heapq.heappop(delayed)
            if kind == "RETURN":
                return_credit(payload, mts)
            elif kind == "ACCEPT":
                order = orders[payload]
                if order["state"] == "SUBMITTING":
                    confirm("SUBMIT", "CONFIRMED", payload, mts)
            elif kind == "CANCEL":
                order = orders[payload]
                if order.get("unknown_confirmation") or order["state"] not in {"CANCEL_PENDING", "FILLED"}:
                    continue
                confirm("CANCEL", "CONFIRMED", payload, mts)
        if mts == settlement:
            cash += interest_due
            cash_interest += interest_due
            segment_event(mts, "cashInterest", interest_due)
            emit("INTEREST_SETTLED", mts, amount=interest_due)
            interest_due = D(0)
            settlement += DAY_MS
        if mts == next_day or mts == end:
            if rolling is not None:
                rolling.advance(mts)
            if legacy_rolling is not None:
                legacy_rolling.advance(mts)
            point = account_point(mts)
            point.update({"startMs": daily_start, "endMs": mts, "dailyNetInterest": earned - previous_daily_interest,
                          "returnOnPrincipalTime": (earned - previous_daily_interest) /
                          (principal * D(mts - daily_start) / D(DAY_MS))})
            daily.append(point)
            previous_daily_interest, daily_start = earned, mts
            if progress:
                progress({"asOfMs": mts, "endMs": end, "processedTrades": trade_count})
            next_day += DAY_MS
        if mts == tick and mts < end:
            if rolling is not None:
                rolling.advance(mts)
            if legacy_rolling is not None:
                legacy_rolling.advance(mts)
            decide(mts)
            tick += control_ms
            if latest_book_time is None or tick - latest_book_time >= cfg.fresh_book_seconds * 1000:
                # No decision can write without a fresh observation. Skip
                # provably idle control ticks, while market, loan, confirmation
                # and daily accounting events continue at their own times.
                if books.timestamp == float("inf"):
                    tick = end
                elif books.timestamp > tick:
                    tick = start + ((int(books.timestamp) - start + control_ms - 1) // control_ms) * control_ms
        while history.maxlen is None and history and int(history[0]["mts"]) < mts - WEEK_MS:
            history.popleft()
        check_ledger()

    summary = {}
    for name, item in metrics.items():
        total = item["principalTime"]
        fill_amount = item["fillAmount"]
        summary[name] = {**{key: value for key, value in item.items() if key != "freshDates"},
                         "returnOnPrincipalTime": item["netInterest"] / total if total > 0 else None,
                         "netAprPercent": item["netInterest"] / total * D(36500) if total > 0 else None,
                         "utilizationPercent": item["loanPrincipalTime"] / total * D(100) if total > 0 else None,
                         "longOccupancyPercent": item["longPrincipalTime"] / total * D(100) if total > 0 else None,
                         "averageWaitMinutes": (item["weightedWaitMs"] / fill_amount / D(MINUTE_MS)
                                                if fill_amount > 0 else None),
                         "cancellationRatePercent": D(item["cancelConfirmed"]) * D(100) / D(item["submittedCount"])
                         if item["submittedCount"] else None,
                         "freshBookDays": D(item["freshBookMs"]) / D(DAY_MS),
                         "freshNaturalDays": len(item["freshDates"]),
                         "coverageEligible": item["freshBookMs"] >= WEEK_MS and len(item["freshDates"]) >= 7}
    total_time = principal * D(end - start) / D(DAY_MS)
    # Segments may overlap; whole-run integrals come from non-overlapping daily
    # accounting only when requested segments cover the full run. Store direct
    # whole-run interest and final balances regardless of segmentation.
    mode = "OBSERVED_BOOK" if has_books else "NO_OBSERVED_BOOK"
    return json_decimal({
        "schemaVersion": 2, "mode": "REPLAY", "marketMode": mode,
        "startMs": start, "endMs": end, "sampleCount": trade_count,
        "netInterest": earned, "actualNetAprPercent": earned / total_time * D(36500),
        "estimatedUtilizationPercent": total_loan_time / total_time * D(100),
        "idlePrincipalTime": total_idle_time,
        "orders": order_log, "fills": fills, "returns": returns,
        "activeCredits": [c for c in credits.values() if c["amount"] > 0],
        "events": events, "daily": daily, "segments": summary,
        "metrics": summary.get("all", summary), "finalState": account_point(end),
        "signals": last_signals, "assumptions": asdict(cfg),
        "dataQuality": {"historicalBook": has_books, "unknownTradeSideCount": unknown_side,
                        "modelZeroBecauseNoEligibleOrders": not bool(order_log),
                        "unavailableMetrics": (["No observed book: orders/reprices and price comparisons unavailable."]
                                               if not has_books else []),
                        "queueObservable": False, "queueIsSensitivityAssumption": True,
                        "calibrationMissingPeriods": sorted(calibration_missing),
                        "repaymentCalibrationUsable": cfg.repayment_mode == "contract" or not calibration_missing,
                        "dailyInterestSettlementIsAssumption": True,
                        "dailyInterestSettlementHourUtc": cfg.interest_settlement_hour_utc,
                        "trainingRepaymentsMode": "contract_until_training_end",
                        "legacyBookVisibleAt": "minute_bucket_end",
                        "tradeCoverageComplete": any(left <= start and right >= end
                                                     for left, right in complete_intervals),
                        "windowFallbackDecisions": window_fallback_decisions,
                        "noBookNewOrdersOrReprices": "blocked",
                        "promotionEligible": False},
        "unmodeledRuntimeControls": ["External offer adoption has no counterfactual initial account evidence.",
                                     "Historical unknown-write outages cannot be inferred from public market data; "
                                     "explicit confirmation_events may inject them."],
    })
