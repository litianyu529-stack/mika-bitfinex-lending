import argparse
import datetime
import hashlib
import json
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
import traceback
from decimal import Decimal, getcontext
from http.server import ThreadingHTTPServer

from bitfinex import APP_VERSION, Bitfinex, BitfinexApiError
from Logger import Logger
from AppContext import AppContext
from ExchangeModels import funding_offer_effective_rate, parse_funding_stats, parse_funding_trades, parse_loan_rows
from StateStore import LendingStateStore, StaleStrategyVersion, StateStoreError
from RuntimeV3 import (
    LendingRuntimeV3,
    build_active_credit_dashboard,
    order_sizing_payload,
    parse_book_v3,
    parse_credit_rows_v3,
    parse_offer_rows_v3,
    parse_wallet_rows_v3,
)
from MarketDataStream import BitfinexMarketDataHub, websocket_dependency_available
from Recovery import WORKER_HEARTBEAT_TIMEOUT_MS, classify_runtime_error
from StrategyV3 import (
    USD_ORDER_CHUNK,
    build_market_signals_v3,
    gross_daily_floor,
    json_decimal,
    pool_for_period,
    policy_v3_to_json,
    policy_v3_with_overrides,
    rate_below_floor,
    replay_strategy_v3,
    validate_policy_v3,
)
from StrategyResearch import (
    backfill_public_market_data,
    evaluate_strategies,
    write_research_report,
)
import StrategyV3 as strategy_layer
from DashboardServer import (
    ApiRequestError,
    DashboardApplication,
    DashboardRequestHandler,
    load_static_snapshot,
)
import Configuration as config_layer


ConfigError = config_layer.ConfigError
StrategyPolicyV3 = strategy_layer.StrategyPolicyV3
Settings = config_layer.Settings
V3_BOOL_FIELDS = config_layer.V3_BOOL_FIELDS
V3_CONFIG_FIELDS = config_layer.V3_CONFIG_FIELDS
V3_INT_FIELDS = config_layer.V3_INT_FIELDS
V3_LIST_FIELDS = config_layer.V3_LIST_FIELDS
V3_PERCENT_FIELDS = config_layer.V3_PERCENT_FIELDS
backup_strategy_state = config_layer.backup_strategy_state
build_settings = config_layer.build_settings
config_api_payload = config_layer.config_api_payload
decimal_percent_to_config = config_layer.decimal_percent_to_config
decimal_to_config = config_layer.decimal_to_config
ensure_active_strategy_v3 = config_layer.ensure_active_strategy_v3
ensure_config_file = config_layer.ensure_config_file
get_boolean = config_layer.get_boolean
get_decimal = config_layer.get_decimal
get_decimal_percent = config_layer.get_decimal_percent
get_option = config_layer.get_option
mirror_active_strategy_v3 = config_layer.mirror_active_strategy_v3
normalize_current_active_strategy = config_layer.normalize_current_active_strategy
read_config = config_layer.read_config
split_csv = config_layer.split_csv
status_decimal = config_layer.status_decimal
strategy_v3_api_values = config_layer.strategy_v3_api_values
strategy_v3_config_values = config_layer.strategy_v3_config_values
strategy_v3_from_api_payload = config_layer.strategy_v3_from_api_payload
strategy_v3_from_config = config_layer.strategy_v3_from_config
strategy_v3_from_record = config_layer.strategy_v3_from_record
strategy_v3_semantically_equal = config_layer.strategy_v3_semantically_equal
strategy_v3_version_id = config_layer.strategy_v3_version_id
update_config_file_preserving_comments = config_layer.update_config_file_preserving_comments
validate_settings = config_layer.validate_settings


getcontext().prec = 28

DEFAULT_CONFIG = "default.cfg"
DEFAULT_CONFIG_EXAMPLE = "default.cfg.example"
DEFAULT_DASHBOARD_JSON = os.path.join("www", "botlog.json")
DEFAULT_V3_STATE_DB = os.path.join(".state", "lendingbot-v3.sqlite3")
STATUS_SCHEMA_VERSION = 3
PREFLIGHT_TTL_SECONDS = 300
DASHBOARD_SERVICE_ID = "mika-lending-dashboard-v3"
DASHBOARD_BUILD_PLACEHOLDER = "__MIKA_DASHBOARD_BUILD_ID__"
DASHBOARD_CSRF_PLACEHOLDER = "__MIKA_DASHBOARD_CSRF_TOKEN__"
WORKER_BUILD_FILES = (
    "lendingbot.py",
    "DashboardServer.py",
    "bitfinex.py",
    "FileUtils.py",
    "Logger.py",
    "MarketDataStream.py",
    "MarketHistory.py",
    "ManualProtection.py",
    "EventReplay.py",
    "V36Validation.py",
    "AppContext.py",
    "Configuration.py",
    "DomainTypes.py",
    "ExchangeModels.py",
    "RuntimeV3.py",
    "StateStore.py",
    "StrategyV3.py",
    "StrategyState.py",
    "StrategyResearch.py",
    "WriteRecovery.py",
    "Recovery.py",
)
DASHBOARD_ASSET_FILES = (
    os.path.join("www", "lendingbot.html"),
    os.path.join("www", "lendingbot.js"),
    os.path.join("www", "v3-dashboard.js"),
    os.path.join("www", "lendingbot.css"),
)
DASHBOARD_BUILD_FILES = WORKER_BUILD_FILES + DASHBOARD_ASSET_FILES


def _files_build_id(files, root=None):
    root = os.path.abspath(root or os.path.dirname(__file__))
    digest = hashlib.sha256()
    for relative in sorted(files):
        path = os.path.join(root, relative)
        digest.update(relative.replace("\\", "/").encode("utf-8"))
        try:
            with open(path, "rb") as file:
                digest.update(file.read())
        except OSError:
            digest.update(b"<missing>")
    return digest.hexdigest()[:20]


def worker_build_id(root=None):
    return _files_build_id(WORKER_BUILD_FILES, root)


def dashboard_build_id(root=None):
    return _files_build_id(DASHBOARD_BUILD_FILES, root)


class LiveProcessLock:
    """Cross-process metadata lock held for a dashboard or LIVE worker."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.handle = None

    @staticmethod
    def _lock(handle, blocking=False):
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            msvcrt.locking(handle.fileno(), mode, 1)
        else:
            import fcntl

            flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            fcntl.flock(handle.fileno(), flags)

    @staticmethod
    def _unlock(handle):
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _read_metadata(handle):
        try:
            handle.seek(1)
            raw = handle.read().decode("utf-8").strip("\x00\r\n ")
            return json.loads(raw) if raw else {}
        except (OSError, UnicodeDecodeError, ValueError):
            return {}

    def acquire(self, config_path, metadata=None):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        handle = open(self.path, "a+b", buffering=0)
        if os.path.getsize(self.path) == 0:
            handle.write(b"\x00")
            handle.flush()
        try:
            self._lock(handle)
        except (OSError, BlockingIOError):
            handle.close()
            return False
        metadata = dict(metadata or {})
        build_id = worker_build_id() if metadata.get("role") == "live_worker" else dashboard_build_id()
        lock_metadata = {
            "pid": os.getpid(),
            "startedAt": timestamp(),
            "configPath": os.path.abspath(config_path),
            "projectRoot": os.path.abspath(os.path.dirname(__file__)),
            "executablePath": os.path.abspath(sys.executable),
            "buildId": build_id,
        }
        lock_metadata.update(metadata)
        handle.seek(1)
        handle.truncate(1)
        handle.write(json.dumps(lock_metadata, ensure_ascii=False).encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
        self.handle = handle
        return True

    def release(self):
        if self.handle is None:
            return
        try:
            self._unlock(self.handle)
        finally:
            self.handle.close()
            self.handle = None

    @classmethod
    def inspect(cls, path):
        absolute = os.path.abspath(path)
        if not os.path.exists(absolute):
            return {"locked": False, "metadata": {}}
        handle = open(absolute, "a+b", buffering=0)
        metadata = cls._read_metadata(handle)
        try:
            cls._lock(handle)
        except (OSError, BlockingIOError):
            handle.close()
            return {"locked": True, "metadata": metadata}
        try:
            return {"locked": False, "metadata": metadata}
        finally:
            cls._unlock(handle)
            handle.close()


def timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Bitfinex Lending Bot")
    parser.add_argument("-cfg", "--config", default=DEFAULT_CONFIG, help="configuration file path")
    parser.add_argument("--dashboard", action="store_true", help="start only the local web dashboard")
    parser.add_argument("--live", action="store_true", help="submit and cancel real Bitfinex funding offers")
    parser.add_argument("--confirmed-preflight", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument(
        "--migrate-legacy", action="store_true", help="offline one-time migration of legacy ownership state"
    )
    parser.add_argument(
        "--backfill-market-data", action="store_true", help="backfill at least 90 days of public USD funding data"
    )
    parser.add_argument(
        "--evaluate-strategy", action="store_true", help="run the offline chronological strategy evaluation"
    )
    parser.add_argument(
        "--apply-validated-v36", metavar="REPORT_JSON",
        help="offline: verify the complete V3.6 evidence and activate its exact candidate policy",
    )
    parser.add_argument("--research-days", type=int, default=90, help="number of days used for backfill/evaluation")
    parser.add_argument("--principal", default="10000", help="offline evaluation principal in USD")
    parser.add_argument("-key", "--apikey", help="Bitfinex API key")
    parser.add_argument("-secret", "--apisecret", help="Bitfinex API secret")
    parser.add_argument("-sleepactive", "--sleeptimeactive", help="seconds between active iterations")
    parser.add_argument("-sleepinactive", "--sleeptimeinactive", help="seconds between inactive iterations")
    parser.add_argument("-json", "--json", "--jsonfile", dest="jsonfile", help="path to json status log")
    parser.add_argument(
        "-jsonsize", "--jsonsize", "--jsonlogsize", dest="jsonlogsize", help="number of json log lines to keep"
    )
    parser.add_argument(
        "-server",
        "--server",
        "--startwebserver",
        dest="startwebserver",
        action="store_true",
        help="serve ./www on 127.0.0.1:8000",
    )
    parser.add_argument("--no-server", action="store_true", help="disable config-driven web server startup")
    return parser.parse_args(argv)


def load_v3_account_context(client, store, now_ms):
    """Read the live account shape without changing exchange or local state."""
    basis = {"source": "REAL_ACCOUNT", "timestamp": int(now_ms), "stale": False, "warnings": []}
    try:
        wallets = parse_wallet_rows_v3(client.wallets())
        try:
            offers = parse_offer_rows_v3(client.active_funding_offers("fUSD"))
        except AttributeError:
            offers = []
        try:
            credits = parse_credit_rows_v3(client.active_funding_credits("fUSD"))
        except AttributeError:
            credits = []
        try:
            loans = parse_loan_rows(client.active_funding_loans("fUSD"))
        except AttributeError:
            loans = []
    except Exception as exc:
        basis.update({"source": "HISTORICAL_SNAPSHOT", "stale": True})
        basis["warnings"].append(f"实时账户快照不可用，使用最近已保存快照：{exc}")
        with store.read_connection() as connection:
            sample = connection.execute("SELECT * FROM account_samples ORDER BY mts DESC LIMIT 1").fetchone()
        offers = [dict(row, id=row["offer_id"]) for row in store.offers(active_only=True)]
        credits = [dict(row, id=row["credit_id"]) for row in store.credits(active_only=True)]
        loans = []
        if sample is None:
            wallets = []
            basis["timestamp"] = None
        else:
            wallets = [
                {
                    "wallet_type": "funding",
                    "currency": "USD",
                    "balance": Decimal(sample["total_principal"]),
                    "available": Decimal(sample["wallet_available"]),
                    "unsettled_interest": Decimal("0"),
                }
            ]
            basis["timestamp"] = int(sample["mts"])

    managed_offers = {int(row["offer_id"]): row for row in store.offers() if row["managed"]}
    for offer in offers:
        managed = managed_offers.get(int(offer["id"]))
        offer["managed"] = managed is not None
        offer["layer"] = (managed or {}).get("layer")
        offer["pool"] = pool_for_period(offer["period"])
        offer["display_type"] = v3_offer_display_type(offer)
        offer["manualProtection"] = store.manual_protection_for_offer(offer, now_ms)
    stored_credits = {int(row["credit_id"]): row for row in store.credits()}
    for credit in [*credits, *loans]:
        stored = stored_credits.get(int(credit["id"]))
        credit["managed"] = bool(stored and stored["managed"])
        credit["pool"] = (stored or {}).get("pool") or pool_for_period(credit["period"])
        credit["layer"] = (stored or {}).get("layer")
        credit["display_type"] = v3_credit_display_type({**credit, **(stored or {})})
    snapshot = {"wallets": wallets, "offers": offers, "credits": credits, "loans": loans}
    return LendingRuntimeV3._account(snapshot), snapshot, basis


def provider_funding_rows(snapshot):
    """Return lender-side active funding once across credit/loan states."""

    merged = {}
    for row in [*snapshot.get("credits", []), *snapshot.get("loans", [])]:
        if int(row.get("side") or 0) < 0:
            continue
        row_id = row.get("id", row.get("credit_id"))
        key = str(row_id) if row_id is not None else f"{row.get('funding_state')}:{len(merged)}"
        merged[key] = row
    return list(merged.values())


def proposed_external_adoption(account_snapshot, policy):
    candidates = []
    simulated = {
        key: [dict(row) for row in account_snapshot.get(key, [])] for key in ("wallets", "offers", "credits", "loans")
    }
    if policy.adopt_external_offers:
        for offer in simulated["offers"]:
            if (offer.get("currency") != "USD" or offer.get("managed")
                    or (offer.get("manualProtection") or {}).get("protected")):
                continue
            candidates.append({**offer, "display_type": v3_offer_display_type(offer)})
            offer["managed"] = True
            offer["pool"] = offer.get("pool") or pool_for_period(offer["period"])
            offer["layer"] = offer.get("layer") or "balanced"
            offer["display_type"] = v3_offer_display_type(offer)
    candidates.sort(key=lambda row: int(row["id"]))
    return LendingRuntimeV3._account(simulated), simulated, candidates


def load_v3_market_context(client, policy, now_ms):
    warnings = []
    try:
        book = parse_book_v3(client.funding_book("fUSD", 250))
    except Exception as exc:
        book = []
        warnings.append(f"Funding Book 不可用：{exc}")
    try:
        trades = parse_funding_trades(
            client.funding_trades("fUSD", start=now_ms - 7 * 86_400_000, end=now_ms, limit=10000, sort=-1)
        )
    except Exception as exc:
        trades = []
        warnings.append(f"Funding Trades 不可用：{exc}")
    try:
        stats = parse_funding_stats(client.funding_stats("fUSD", start=now_ms - 7 * 86_400_000, end=now_ms, limit=250))
    except Exception as exc:
        stats = []
        warnings.append(f"Funding Stats 不可用：{exc}")
    return book, trades, stats, build_market_signals_v3(book, trades, stats, policy, now_ms), warnings


strategy_preview_tokens = {}
strategy_apply_tokens = {}
strategy_token_lock = threading.RLock()
STRATEGY_TOKEN_CACHE_LIMIT = 512


def _prune_strategy_tokens(current_time=None):
    current = time.time() if current_time is None else float(current_time)
    removed = 0
    with strategy_token_lock:
        for cache in (strategy_preview_tokens, strategy_apply_tokens):
            expired = [token for token, context in cache.items() if current > float(context["expiresAt"])]
            for token in expired:
                cache.pop(token, None)
                removed += 1
            overflow = max(0, len(cache) - STRATEGY_TOKEN_CACHE_LIMIT)
            if overflow:
                oldest = sorted(cache, key=lambda token: float(cache[token]["expiresAt"]))[:overflow]
                for token in oldest:
                    cache.pop(token, None)
                    removed += 1
    return removed


def _canonical_sha256(payload):
    serialized = json.dumps(json_decimal(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _account_context_digest(account, snapshot):
    def compact(rows, fields):
        return [
            {field: json_decimal(row.get(field)) for field in fields}
            for row in sorted(
                rows, key=lambda item: int(item.get("id") or item.get("offer_id") or item.get("credit_id") or 0)
            )
        ]

    return _canonical_sha256(
        {
            "account": account,
            "offers": compact(
                snapshot.get("offers", []),
                ("id", "amount", "rate", "rate_real", "period", "display_type", "flags", "managed"),
            ),
            "credits": compact(
                snapshot.get("credits", []),
                ("id", "amount", "rate", "rate_real", "period", "display_type", "hidden", "managed"),
            ),
            "loans": compact(
                snapshot.get("loans", []),
                ("id", "amount", "rate", "rate_real", "period", "display_type", "hidden", "managed"),
            ),
        }
    )


def _token_expiry_iso(expires_at):
    return datetime.datetime.fromtimestamp(expires_at, datetime.timezone.utc).isoformat(timespec="seconds")


def strategy_v3_preview(
    config_path,
    payload,
    client_factory=None,
    now_ms=None,
    issue_token=True,
    app_context=None,
):
    app_context = process_context(config_path, app_context, client_factory=client_factory)
    _prune_strategy_tokens(app_context.now())
    client_factory = client_factory or app_context.client_factory or Bitfinex
    store, settings = v3_store_for_config(config_path)
    active, active_policy = ensure_active_strategy_v3(store, settings)
    policy = strategy_v3_from_api_payload(payload.get("strategyV3", {}), base=active_policy)
    proposed_version = (
        active["version_id"]
        if strategy_v3_semantically_equal(policy, active_policy)
        else strategy_v3_version_id(policy)
    )
    now = int(now_ms if now_ms is not None else app_context.now() * 1000)
    client = client_factory(settings.api_key, settings.api_secret)
    account, account_snapshot, basis = load_v3_account_context(client, store, now)
    account, planned_snapshot, adoption_candidates = proposed_external_adoption(account_snapshot, policy)
    book, trades, stats, signals, warnings = load_v3_market_context(client, policy, now)
    warnings = [*basis["warnings"], *warnings]
    result = LendingRuntimeV3._build_plan(account, policy, signals, proposed_version)
    if not policy_v3_to_json(policy)["floorsConfigured"]:
        warnings.append("三个最低净年化尚未全部填写；允许预览，但 LIVE 将被阻止。")
    proposed_record = {"version_id": proposed_version, "policy": json_decimal(policy.__dict__)}
    incompatible = []
    for offer in planned_snapshot["offers"]:
        violations = v3_offer_violations(offer, policy, signals.get("frr_daily_rate"))
        if violations:
            incompatible.append({**json_decimal(offer), "violations": violations})
    non_changeable_credits = []
    for credit in provider_funding_rows(account_snapshot):
        violations = v3_credit_violations(credit, policy)
        if violations:
            non_changeable_credits.append({**json_decimal(credit), "violations": violations})
    account_digest = _account_context_digest(account, account_snapshot)
    response = {
        "currency": "USD",
        "principal": status_decimal(account["total"]),
        "available": status_decimal(account["wallet"]),
        "accountSnapshot": basis,
        "activeVersion": active["version_id"],
        "proposedVersion": proposed_version,
        "strategyDiff": v3_strategy_diff(active, proposed_record),
        "policy": strategy_v3_api_values(policy),
        "orderSizing": order_sizing_payload(),
        "periodSelection": json_decimal(signals.get("periodSelection", {})),
        "periodActivity": json_decimal(store.period_activity(now - 86_400_000, "USD")),
        "signals": json_decimal(signals),
        "plan": json_decimal(result),
        "fundingLimit": {
            "amount": None if policy.max_lend_amount is None else status_decimal(policy.max_lend_amount),
            "percent": decimal_to_config(policy.max_lend_percent),
            "effectiveCap": status_decimal(result["funding_cap"]),
            "existingExposure": status_decimal(result["existing_exposure"]),
        },
        "incompatibleOffers": incompatible,
        "nonChangeableCredits": non_changeable_credits,
        "externalAdoptionCandidates": json_decimal(adoption_candidates),
        "externalAdoptionDigest": _canonical_sha256(adoption_candidates),
        "ratioRebalanceCancellations": [],
        "replay": replay_strategy_v3(policy, trades, stats, account["total"], book, now),
        "warnings": list(dict.fromkeys(warnings)),
        "accountDigest": account_digest,
        "buildId": dashboard_build_id(),
    }
    if issue_token:
        issued_at = app_context.now()
        token = secrets.token_urlsafe(24)
        context = {
            "token": token,
            "expiresAt": issued_at + PREFLIGHT_TTL_SECONDS,
            "buildId": dashboard_build_id(),
            "configPath": os.path.abspath(config_path),
            "activeVersion": active["version_id"],
            "proposedVersion": proposed_version,
            "policyHash": _canonical_sha256(policy.__dict__),
            "accountDigest": account_digest,
            "planHash": result["plan_hash"],
        }
        with strategy_token_lock:
            strategy_preview_tokens[token] = context
        _prune_strategy_tokens(issued_at)
        response["previewToken"] = token
        response["expiresAt"] = _token_expiry_iso(context["expiresAt"])
    return response


def v3_store_for_config(config_path):
    config, _ = read_config(config_path)
    settings = build_settings(parse_args(["--config", config_path]), config)
    return LendingStateStore(settings.state_db_file), settings


def v3_strategy_diff(left, right):
    if left is None or right is None:
        return []
    left_policy = left.get("policy", {})
    right_policy = right.get("policy", {})
    return [
        {"field": key, "from": left_policy.get(key), "to": right_policy.get(key)}
        for key in sorted(set(left_policy) | set(right_policy))
        if left_policy.get(key) != right_policy.get(key)
    ]


def v3_offer_display_type(offer):
    return LendingRuntimeV3._offer_display_type(offer)


def v3_credit_display_type(credit):
    display = str(credit.get("display_type") or "").upper()
    if display:
        return display
    rate_type = str(credit.get("rate_type") or "").upper()
    if rate_type in {"FIXED", "LIMIT"}:
        return "LIMIT"
    if rate_type in {"VAR", "VARIABLE", "FRRDELTAVAR"}:
        return "VARIABLE_UNKNOWN"
    return rate_type or "UNKNOWN"


def v3_credit_violations(credit, policy):
    display_type = v3_credit_display_type(credit)
    pseudo_offer = {
        **credit,
        "offer_type": display_type,
        "display_type": display_type,
        "flags": 64 if credit.get("hidden") else 0,
    }
    violations = v3_offer_violations(pseudo_offer, policy)
    if display_type == "VARIABLE_UNKNOWN":
        violations = [item for item in violations if item != "disabled_type"]
    return violations


def v3_offer_violations(offer, policy, frr_daily_rate=None):
    pool = offer.get("pool") or pool_for_period(int(offer.get("period") or 0))
    display_type = v3_offer_display_type(offer)
    hidden = bool(int(offer.get("flags") or 0) & 64) or bool(offer.get("hidden"))
    violations = []
    if not LendingRuntimeV3._display_type_enabled(policy, display_type):
        violations.append("disabled_type")
    if hidden and not policy.enable_hidden:
        violations.append("hidden_disabled")
    if pool not in {"short", "medium", "long"} or int(offer.get("period") or 0) not in policy.periods(pool):
        violations.append("period_not_allowed")
    floor_apr = policy.floor_apr(pool) if pool in {"short", "medium", "long"} else None
    if floor_apr is not None:
        fee = policy.hidden_fee_rate if hidden else policy.normal_fee_rate
        effective = funding_offer_effective_rate(offer, frr_daily_rate)
        if effective is not None and rate_below_floor(effective, gross_daily_floor(floor_apr, fee)):
            violations.append("below_new_floor")
    return violations


def runtime_v3_payload(config_path, context=None):
    context = process_context(config_path, context)
    _prune_strategy_tokens(context.now())
    store, settings = v3_store_for_config(config_path)
    market_snapshot = None
    if context.process_state.market_hub is not None:
        market_snapshot = json_decimal(context.process_state.market_hub.snapshot())
    runtime = store.runtime()
    active, active_policy = ensure_active_strategy_v3(store, settings)
    draft = store.strategy("DRAFT")
    pending = store.strategy("PENDING")
    proposed = pending or draft or active
    proposed_policy = strategy_v3_from_record(proposed) if proposed else active_policy
    incompatible = []
    for offer in store.offers(active_only=True):
        violations = v3_offer_violations(offer, proposed_policy)
        if violations:
            incompatible.append({**offer, "display_type": v3_offer_display_type(offer), "violations": violations})
    return {
        "runtime": runtime,
        "displayMode": "APPLYING" if runtime["mode"] == "LIVE" and pending is not None else runtime["mode"],
        "process": controlled_bot_status(config_path, context),
        "activeStrategy": active,
        "draftStrategy": draft,
        "pendingStrategy": pending,
        "effectiveStrategy": active,
        "policy": strategy_v3_api_values(active_policy),
        "strategyDiff": v3_strategy_diff(active, proposed),
        "incompatibleOffers": incompatible,
        "marketSnapshot": market_snapshot,
        "realizedIncome": store.realized_income_summary("USD"),
        "incomeHistorySync": store.income_history_sync_payload("USD"),
    }


def stats_v3_payload(store):
    return {
        "statistics": {
            "1d": store.statistics(1),
            "7d": store.statistics(7),
            "30d": store.statistics(30),
            "90d": store.statistics(90),
            "all": store.statistics(None),
        },
        "realizedIncome": store.realized_income_summary("USD"),
        "incomeHistorySync": store.income_history_sync_payload("USD"),
    }


def save_strategy_v3_draft(config_path, payload, app_context=None):
    now = app_context.now if app_context is not None else time.time
    _prune_strategy_tokens(now())
    store, settings = v3_store_for_config(config_path)
    active, active_policy = ensure_active_strategy_v3(store, settings)
    policy = strategy_v3_from_api_payload(payload.get("strategyV3", {}), base=active_policy)
    token = str(payload.get("previewToken") or "")
    with strategy_token_lock:
        context = strategy_preview_tokens.pop(token, None)
    if not context or now() > context["expiresAt"]:
        raise ApiRequestError("策略预览已过期，请重新计算", "PREVIEW_STALE", 409)
    if context["buildId"] != dashboard_build_id() or context["configPath"] != os.path.abspath(config_path):
        raise ApiRequestError("Dashboard build 或配置路径已变化，请重新计算", "PREVIEW_STALE", 409)
    if active["version_id"] != context["activeVersion"] or _canonical_sha256(policy.__dict__) != context["policyHash"]:
        raise ApiRequestError("ACTIVE 或拟议策略已变化，请重新计算", "PREVIEW_STALE", 409)
    if strategy_v3_semantically_equal(policy, active_policy):
        return {
            "versionId": active["version_id"],
            "draftVersionId": active["version_id"],
            "status": "UNCHANGED",
            "strategy": active,
            "diff": [],
        }
    try:
        version_id = store.save_strategy(
            json_decimal(policy.__dict__), status="DRAFT", expected_active_id=active["version_id"],
        )
    except StaleStrategyVersion as exc:
        raise ApiRequestError("策略已变化，请重新预览", "PREVIEW_STALE", 409) from exc
    if version_id == active["version_id"]:
        return {
            "versionId": version_id,
            "draftVersionId": version_id,
            "status": "UNCHANGED",
            "strategy": active,
            "diff": [],
        }
    draft = store.strategy("DRAFT")
    if draft is None or draft["version_id"] != version_id:
        raise ApiRequestError("草稿已变化，请重新预览", "PREVIEW_STALE", 409)
    apply_token = secrets.token_urlsafe(24)
    apply_context = {
        **context,
        "token": apply_token,
        "draftVersionId": version_id,
        "expiresAt": now() + PREFLIGHT_TTL_SECONDS,
    }
    with strategy_token_lock:
        strategy_apply_tokens[apply_token] = apply_context
    _prune_strategy_tokens(now())
    return {
        "versionId": version_id,
        "draftVersionId": version_id,
        "applyToken": apply_token,
        "expiresAt": _token_expiry_iso(apply_context["expiresAt"]),
        "status": "DRAFT",
        "strategy": draft,
        "diff": v3_strategy_diff(active, draft),
    }


def apply_strategy_v3_draft(config_path, payload, client_factory=None, app_context=None):
    app_context = process_context(config_path, app_context, client_factory=client_factory)
    _prune_strategy_tokens(app_context.now())
    client_factory = client_factory or app_context.client_factory or Bitfinex
    store, settings = v3_store_for_config(config_path)
    active, _ = ensure_active_strategy_v3(store, settings)
    draft = store.strategy("DRAFT")
    if draft is None:
        return {"status": "UNCHANGED", "strategy": active, "versionId": active["version_id"]}
    draft_version = str(payload.get("draftVersionId") or "")
    token = str(payload.get("applyToken") or "")
    with strategy_token_lock:
        context = strategy_apply_tokens.pop(token, None)
    if not context or app_context.now() > context["expiresAt"]:
        raise ApiRequestError("应用确认已过期，请重新预览并保存草稿", "PREVIEW_STALE", 409)
    if context["buildId"] != dashboard_build_id() or context["activeVersion"] != active["version_id"]:
        raise ApiRequestError("Dashboard build 或 ACTIVE 已变化，请重新预览", "PREVIEW_STALE", 409)
    if draft_version != draft["version_id"] or context["draftVersionId"] != draft["version_id"]:
        raise ApiRequestError("待应用草稿与已确认版本不一致", "PREVIEW_STALE", 409)
    draft_policy = strategy_v3_from_record(draft)
    if draft_policy.quick_pricing_mode == "EXECUTABLE_PREMIUM_V1":
        from V36Validation import validation_status

        gate = validation_status(store, draft_policy)
        if not gate["approved"]:
            raise ApiRequestError("V3.6 候选尚未通过真实数据验收", "V36_VALIDATION_REQUIRED", 409, details=gate)
    refreshed = strategy_v3_preview(
        config_path,
        {"strategyV3": strategy_v3_api_values(draft_policy)},
        client_factory=client_factory,
        issue_token=False,
        app_context=app_context,
    )
    if refreshed["accountDigest"] != context["accountDigest"] or refreshed["plan"]["plan_hash"] != context["planHash"]:
        raise ApiRequestError(
            "账户或计划已变化，请检查新预览后再次确认",
            "PREVIEW_STALE",
            409,
            details={"preview": refreshed},
        )
    with app_context.process_state.lock, strategy_token_lock:
        queue = controlled_bot_running(config_path, app_context) or store.runtime()["mode"] == "LIVE"
        live_lock = LiveProcessLock(app_context.live_lock_path)
        if not queue:
            queue = not live_lock.acquire(config_path, {"role": "strategy_activation"})
        try:
            strategy = store.apply_confirmed_draft(
                draft_version, active["version_id"], queue=queue,
                validated_policy=draft_policy if draft_policy.quick_pricing_mode == "EXECUTABLE_PREMIUM_V1" else None,
            )
            if not queue:
                mirror_active_strategy_v3(config_path, strategy_v3_from_record(strategy))
        except StaleStrategyVersion as exc:
            raise ApiRequestError(
                "策略版本已变化或已有策略正在应用，请刷新后重新确认",
                "PREVIEW_STALE", 409, details={"reason": str(exc)},
            ) from exc
        finally:
            live_lock.release()
    return {"status": "PENDING" if queue else "ACTIVE", "strategy": strategy}


def discard_strategy_v3_draft(config_path, app_context=None):
    app_context = process_context(config_path, app_context)
    store, _ = v3_store_for_config(config_path)
    discarded = []
    with app_context.process_state.lock, strategy_token_lock:
        draft = store.strategy("DRAFT")
        pending = store.strategy("PENDING")
        running = controlled_bot_running(config_path, app_context) or store.runtime()["mode"] == "LIVE"
        live_lock = LiveProcessLock(app_context.live_lock_path)
        if pending is not None and not running:
            running = not live_lock.acquire(config_path, {"role": "strategy_discard"})
        try:
            if pending is not None and draft is None and running:
                raise ApiRequestError(
                    "待应用策略正在由机器人处理；请先停止机器人再撤销",
                    "PENDING_STRATEGY_RUNNING", 409,
                )
            if draft is not None:
                store.discard_strategy("DRAFT")
                discarded.append("DRAFT")
            if pending is not None and not running:
                store.discard_strategy("PENDING")
                discarded.append("PENDING")
        finally:
            live_lock.release()
    return {
        "status": "DISCARDED" if discarded else "UNCHANGED",
        "discarded": discarded,
        "activeStrategy": store.strategy("ACTIVE"),
        "pendingStrategy": store.strategy("PENDING"),
    }


def replay_v3_from_store(config_path, now_ms=None, context=None):
    context = process_context(config_path, context)
    store, settings = v3_store_for_config(config_path)
    if store.runtime()["mode"] == "LIVE" or controlled_bot_running(config_path, context):
        raise ConfigError("必须先暂停 LIVE 才能进入 REPLAY")
    store.set_mode("REPLAY", "dashboard_replay")
    now = int(now_ms if now_ms is not None else context.now() * 1000)
    trades = store.iter_market_trades(now - 7 * 86_400_000, now)
    with store.read_connection() as connection:
        latest = connection.execute("SELECT total_principal FROM account_samples ORDER BY mts DESC LIMIT 1").fetchone()
    principal = Decimal(latest["total_principal"]) if latest else Decimal("10000")
    _, active_policy = ensure_active_strategy_v3(store, settings)
    return replay_strategy_v3(
        active_policy, trades, store.funding_stats(now - 7 * 86_400_000, now), principal, now_ms=now,
        book_snapshots=store.iter_book_snapshots(now - 7 * 86_400_000, now),
    )


def empty_status_payload():
    active_credit_dashboard = build_active_credit_dashboard(
        [],
        Decimal("0"),
        StrategyPolicyV3(),
        0,
    )
    return {
        "schemaVersion": STATUS_SCHEMA_VERSION,
        "applicationVersion": APP_VERSION,
        "operationMode": "PAUSED",
        "last_status": "当前已暂停；进入 LIVE 前必须完成预检并人工确认。",
        "last_update": "",
        "log": [],
        "outputCurrency": {"currency": "USD", "highestBid": "1"},
        "platformFeeRate": "15",
        "raw_data": {},
        "openOffers": [],
        "credits": active_credit_dashboard["credits"],
        "activeCreditSummary": active_credit_dashboard["summary"],
        "strategyDecision": {},
        "recovery": {
            "active": False,
            "category": None,
            "reason": None,
            "originMode": None,
            "targetMode": None,
            "attempts": 0,
            "successfulSnapshots": 0,
            "requiredSnapshots": 2,
            "lastProbeAt": None,
            "nextProbeAt": None,
            "lastError": None,
            "heartbeatAt": None,
            "manualRequired": False,
        },
        "legacyIgnored": False,
    }


def read_status_payload(status_path):
    if not os.path.exists(status_path):
        return empty_status_payload()
    try:
        with open(status_path, "r", encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, ValueError):
        payload = None
    valid_modes = {"live", "LIVE", "PAUSED", "REPLAY", "APPLYING"}
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != STATUS_SCHEMA_VERSION
        or payload.get("operationMode") not in valid_modes
    ):
        ignored = empty_status_payload()
        ignored["last_status"] = "旧版状态已忽略，等待实盘机器人首次同步。"
        ignored["legacyIgnored"] = True
        return ignored
    payload.setdefault("openOffers", [])
    payload.setdefault("raw_data", {})
    payload.setdefault("log", [])
    payload.setdefault("strategyDecision", {})
    payload["legacyIgnored"] = False
    return payload


STOP_REASON_MESSAGES = {
    "worker_build_mismatch": "检测到新版本，旧实盘进程已安全停止",
    "worker_build_mismatch_stopped": "检测到新版本，旧实盘进程已安全停止",
    "dashboard_started_without_live_process": "控制台启动时未发现实盘进程",
    "stopped_by_dashboard": "实盘进程已由控制台停止",
    "paused_by_operator": "策略已由操作员暂停",
}


def stop_reason_message(reason):
    reason = str(reason or "").strip()
    if not reason:
        return "原因未记录"
    return STOP_REASON_MESSAGES.get(reason, reason)


def dashboard_status_payload(status_path, config_path, context=None):
    payload = read_status_payload(status_path)
    payload["applicationVersion"] = APP_VERSION
    try:
        store, _ = v3_store_for_config(config_path)
        runtime = store.runtime()
        recovery = store.recovery_status()
        process = controlled_bot_status(config_path, context)
        if not process["running"]:
            mode_event = store.latest_mode_event()
            stop_reason = process.get("stopReason")
            if not stop_reason and mode_event and mode_event.get("to_mode") == "PAUSED":
                stop_reason = mode_event.get("reason")
            process["stopReason"] = stop_reason
            empty = empty_status_payload()
            empty["log"] = list(payload.get("log") or [])
            empty["runtime"] = runtime
            empty["recovery"] = recovery
            empty["operationMode"] = "PAUSED"
            empty["last_status"] = f"机器人进程已停止（{stop_reason_message(stop_reason)}）；账户与挂单快照当前不可用。"
            empty["lastStopReason"] = stop_reason
            empty["snapshotAvailable"] = False
            empty["process"] = process
            return empty
        payload["runtime"] = runtime
        payload["recovery"] = recovery
        payload["process"] = process
        payload["operationMode"] = runtime["mode"]
        account = payload.get("account")
        payload["snapshotAvailable"] = bool(
            isinstance(account, dict)
            and account.get("walletAvailableKnown") is True
            and account.get("total") is not None
        )
        if runtime["mode"] == "PAUSED":
            payload["last_status"] = (
                f"PAUSED：{runtime.get('safe_reason')}"
                if runtime.get("safe_reason")
                else "策略已暂停；实盘进程仍在运行。"
            )
        # The worker publishes statistics with the account snapshot. Rebuilding
        # every history window here blocks frequent UI polls on large databases
        # and mixes newer statistics with an older account snapshot. Explicit
        # history requests can still use /api/stats/v3.
    except Exception as exc:
        unavailable = empty_status_payload()
        unavailable["log"] = list(payload.get("log") or [])
        unavailable["operationMode"] = "UNKNOWN"
        unavailable["runtime"] = {
            "mode": "UNKNOWN",
            "safe_reason": None,
            "status_error": type(exc).__name__,
        }
        unavailable["process"] = {"running": False, "statusAvailable": False}
        unavailable["snapshotAvailable"] = False
        unavailable["statusUnavailable"] = True
        unavailable["last_status"] = "控制台无法读取当前运行状态；实时账户与挂单数据已隐藏。"
        return unavailable
    return payload


def process_context(config_path, context=None, client_factory=None, now=None):
    if context is not None:
        return context
    return AppContext.for_project(
        os.path.abspath(os.path.dirname(__file__)),
        config_path=config_path,
        client_factory=client_factory or Bitfinex,
        now=now or time.time,
    )


def reconcile_orphaned_live_runtime(config_path, context=None):
    context = process_context(config_path, context)
    store, _ = v3_store_for_config(config_path)
    external = external_live_process(config_path, context)
    if external and external.get("buildMismatch"):
        if external.get("stateError"):
            store.enter_protected_pause("WORKER_BUILD_MISMATCH_UNVERIFIED")
            return store.runtime()
        identity_error = _live_process_identity_error(external)
        if identity_error:
            store.enter_protected_pause("WORKER_BUILD_MISMATCH_UNVERIFIED")
            return store.runtime()
        stop_controlled_bot(config_path, reason="worker_build_mismatch", context=context)
        store.set_mode("PAUSED", "worker_build_mismatch_stopped")
        return store.runtime()
    runtime = store.runtime()
    if runtime["mode"] == "LIVE" and not controlled_bot_running(config_path, context):
        return store.set_mode("PAUSED", "dashboard_started_without_live_process")
    return runtime


def cleanup_controlled_bot_handle(context):
    state = context.process_state
    if state.log_handle is not None:
        try:
            state.log_handle.close()
        except Exception:
            pass
        state.log_handle = None


def _pid_is_alive(pid):
    try:
        pid = int(pid)
        if pid <= 0:
            return False
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if not process:
                return False
            try:
                exit_code = wintypes.DWORD()
                if not ctypes.windll.kernel32.GetExitCodeProcess(process, ctypes.byref(exit_code)):
                    return False
                return exit_code.value == 259
            finally:
                ctypes.windll.kernel32.CloseHandle(process)
        os.kill(pid, 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _process_identity(pid):
    try:
        pid = int(pid)
        if os.name == "nt":
            command = (
                '$p=Get-CimInstance Win32_Process -Filter "ProcessId=%d"; '
                "if($p){[pscustomobject]@{ExecutablePath=$p.ExecutablePath;"
                "CommandLine=$p.CommandLine}|ConvertTo-Json -Compress}"
            ) % pid
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return json.loads(result.stdout.strip()) if result.stdout.strip() else {}
        command_line = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\x00", b" ").decode("utf-8", "replace")
        executable = os.path.realpath(f"/proc/{pid}/exe")
        return {"ExecutablePath": executable, "CommandLine": command_line}
    except (OSError, ValueError, subprocess.SubprocessError, json.JSONDecodeError):
        return {}


def _live_process_identity_error(metadata):
    pid = metadata.get("pid")
    identity = _process_identity(pid) if pid else {}
    command_line = str(identity.get("CommandLine") or "")
    executable = str(identity.get("ExecutablePath") or "")
    if not command_line or "lendingbot.py" not in command_line.lower() or "--live" not in command_line.lower():
        return "实盘锁 PID 的命令行无法验证为 lendingbot.py --live"
    recorded_executable = str(metadata.get("executablePath") or "")
    if (
        recorded_executable
        and executable
        and os.path.normcase(os.path.abspath(recorded_executable)) != os.path.normcase(os.path.abspath(executable))
    ):
        return "实盘锁记录的 Python 可执行文件与实际进程不一致"
    return None


def external_live_process(config_path=DEFAULT_CONFIG, context=None):
    context = process_context(config_path, context)
    inspection = LiveProcessLock.inspect(context.live_lock_path)
    if not inspection["locked"]:
        return None
    metadata = inspection["metadata"]
    state_error = None
    if not metadata.get("pid"):
        state_error = "实盘锁已占用，但缺少 PID 元数据"
    elif not _pid_is_alive(metadata["pid"]):
        state_error = "实盘锁已占用，但记录的 PID 无法验证"
    expected_config = os.path.abspath(config_path)
    if metadata.get("configPath") and os.path.abspath(metadata["configPath"]) != expected_config:
        state_error = f"实盘进程使用其他配置：{metadata['configPath']}"
    current_build = worker_build_id()
    worker_build = metadata.get("buildId")
    return {
        **metadata,
        "stateError": state_error,
        "workerBuildId": worker_build,
        "currentBuildId": current_build,
        "buildMismatch": worker_build != current_build,
    }


def controlled_bot_running(config_path=DEFAULT_CONFIG, context=None):
    context = process_context(config_path, context)
    process = context.process_state.process
    internal = process is not None and process.poll() is None
    return internal or external_live_process(config_path, context) is not None


def controlled_bot_status(config_path=DEFAULT_CONFIG, context=None):
    context = process_context(config_path, context)
    state = context.process_state
    with state.lock:
        process = state.process
        internal_running = process is not None and process.poll() is None
        external = None if internal_running else external_live_process(config_path, context)
        internal_worker_build = None
        if internal_running:
            inspection = LiveProcessLock.inspect(context.live_lock_path)
            metadata = inspection.get("metadata") or {}
            if inspection.get("locked") and int(metadata.get("pid") or 0) == int(process.pid):
                internal_worker_build = metadata.get("buildId")
        current_worker_build = worker_build_id()
        running = internal_running or external is not None
        return_code = None
        if process is not None:
            return_code = process.poll()
        if process is not None and return_code is not None:
            cleanup_controlled_bot_handle(context)
        return {
            "running": running,
            "pid": process.pid if internal_running else (external or {}).get("pid"),
            "startedAt": state.started_at if internal_running else (external or {}).get("startedAt"),
            "returnCode": return_code,
            "stopReason": state.stop_reason,
            "managedExternally": bool(external),
            "dashboardBuildId": dashboard_build_id(),
            "workerBuildId": internal_worker_build if internal_running else (external or {}).get("workerBuildId"),
            "buildMismatch": bool(
                (internal_running and internal_worker_build and internal_worker_build != current_worker_build)
                or (external and external.get("buildMismatch"))
            ),
            "watchdogAuthorized": bool(state.auto_restart_authorization),
            **({"stateError": external["stateError"]} if external and external.get("stateError") else {}),
        }


def config_sha256(config_path):
    digest = hashlib.sha256()
    with open(config_path, "rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_key_permissions(rows):
    permissions = {}
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        permissions[str(row[0]).lower()] = {
            "read": str(row[1]).strip().lower() in {"1", "true"},
            "write": str(row[2]).strip().lower() in {"1", "true"},
        }
    return permissions


def permission_enabled(permissions, scope, access):
    return bool(permissions.get(scope, {}).get(access, False))


def evaluate_live_preflight(config_path, client_factory=None, context=None):
    """Perform the single V3 preflight used by both dashboard and live child."""
    context = process_context(config_path, context, client_factory=client_factory)
    client_factory = client_factory or context.client_factory or Bitfinex
    checks = []
    warnings = []

    def add_check(check_id, label, passed, detail):
        checks.append({"id": check_id, "label": label, "status": "pass" if passed else "fail", "detail": detail})

    try:
        config, _ = read_config(config_path)
        settings = build_settings(parse_args(["--config", config_path, "--live", "--no-server"]), config)
        store = LendingStateStore(settings.state_db_file)
        active, policy = ensure_active_strategy_v3(store, settings)
        validate_policy_v3(policy, require_live_floors=True)
        add_check("config", "V3 策略配置", True, f"SQLite ACTIVE {active['version_id']} 有效")
    except Exception as exc:
        add_check("config", "V3 策略配置", False, str(exc))
        return {"checks": checks, "warnings": warnings, "summary": {"strategyVersion": 3}}

    draft = store.strategy("DRAFT")
    pending = store.strategy("PENDING")
    clean_state = draft is None and pending is None
    add_check(
        "strategy_state",
        "策略版本状态",
        clean_state,
        "没有未应用草稿或待切换策略" if clean_state else "存在 DRAFT 或 PENDING；请先应用或放弃后再启动",
    )
    if not strategy_v3_semantically_equal(settings.strategy_v3, policy):
        warnings.append(
            {
                "code": "CONFIG_MIRROR_DIFFERS",
                "message": "配置文件中的 V3 镜像与 SQLite ACTIVE 不一致；本次预检和实盘只使用 ACTIVE。",
            }
        )
    add_check("v3_usd_only", "V3 币种范围", settings.currencies == ["USD"], "V3 仅允许 currencies = USD")
    add_check(
        "v3_websocket_dependency",
        "WebSocket 运行库",
        websocket_dependency_available(),
        "websockets 已安装" if websocket_dependency_available() else "请先安装 websockets 依赖",
    )

    client = client_factory(settings.api_key, settings.api_secret)
    if not client.has_credentials():
        add_check("credentials", "API 凭据", False, "未配置有效的 Bitfinex API key/secret")
        return {
            "checks": checks,
            "warnings": warnings,
            "summary": {"strategyVersion": 3, "activeStrategyVersion": active["version_id"]},
        }
    try:
        permissions = parse_key_permissions(client.key_permissions())
        add_check("credentials", "API 凭据", True, "凭据有效，权限接口可访问")
    except Exception as exc:
        add_check("credentials", "API 凭据", False, f"权限读取失败：{exc}")
        return {
            "checks": checks,
            "warnings": warnings,
            "summary": {"strategyVersion": 3, "activeStrategyVersion": active["version_id"]},
        }

    wallets_read = permission_enabled(permissions, "wallets", "read")
    funding_read = permission_enabled(permissions, "funding", "read")
    funding_write = permission_enabled(permissions, "funding", "write")
    add_check("wallets_read", "钱包读取权限", wallets_read, "wallets 需要读取权限")
    add_check("funding_read", "放贷读取权限", funding_read, "funding 需要读取权限")
    add_check("funding_write", "放贷写入权限", funding_write, "funding 需要写入权限以挂单和撤单")
    if settings.transferable_currencies:
        add_check(
            "wallets_write",
            "钱包转账权限",
            permission_enabled(permissions, "wallets", "write"),
            "启用自动转入时 wallets 需要写权限",
        )
    add_check(
        "withdraw_disabled",
        "API 提现权限",
        not permission_enabled(permissions, "withdraw", "write"),
        "withdraw 写权限必须关闭",
    )
    add_check(
        "ui_withdraw_disabled",
        "界面提现权限",
        not permission_enabled(permissions, "ui_withdraw", "write"),
        "ui_withdraw 写权限必须关闭",
    )

    now = int(context.now() * 1000)
    account, snapshot, basis = load_v3_account_context(client, store, now)
    original_snapshot = snapshot
    account, snapshot, adoption_candidates = proposed_external_adoption(snapshot, policy)
    add_check(
        "account_snapshot",
        "真实账户快照",
        basis["source"] == "REAL_ACCOUNT",
        "已读取实时 Funding 钱包、挂单和贷款"
        if basis["source"] == "REAL_ACCOUNT"
        else "实时账户读取失败，不能使用历史快照启动实盘",
    )
    account_reconciled = bool(account.get("walletAvailableKnown")) and account.get("reconciliationStatus") == "MATCHED"
    add_check(
        "account_reconciliation",
        "Funding 账户对账",
        account_reconciled,
        "Funding 钱包余额、可用余额、挂单及贷款已完整对账"
        if account_reconciled
        else "Funding 可用余额尚未计算或账户组件金额不一致；为避免重复放贷已阻止启动",
    )
    for message in basis["warnings"]:
        warnings.append({"code": "ACCOUNT_SNAPSHOT_WARNING", "message": message})
    book, trades, stats, signals, market_warnings = load_v3_market_context(client, policy, now)
    add_check("book_usd", "USD Funding 市场", bool(book), "Funding Book 可用" if book else "Funding Book 没有可用报价")
    for message in market_warnings:
        warnings.append({"code": "MARKET_DATA_WARNING", "message": message})

    result = LendingRuntimeV3._build_plan(account, policy, signals, active["version_id"])
    incompatible_managed = []
    incompatible_external = []
    for offer in snapshot["offers"]:
        violations = v3_offer_violations(offer, policy, signals.get("frr_daily_rate"))
        if not violations:
            continue
        item = {**json_decimal(offer), "display_type": v3_offer_display_type(offer), "violations": violations}
        (incompatible_managed if offer.get("managed") else incompatible_external).append(item)
    incompatible_credits = []
    for credit in provider_funding_rows(snapshot):
        violations = v3_credit_violations(credit, policy)
        if violations:
            incompatible_credits.append({**json_decimal(credit), "violations": violations})

    if account["wallet"] <= 0:
        warnings.append(
            {"code": "NO_AVAILABLE_BALANCE", "message": "USD 当前没有可用资金；仍可先撤销不兼容的机器人挂单。"}
        )
    elif account["wallet"] < USD_ORDER_CHUNK:
        warnings.append({"code": "BELOW_MINIMUM", "message": "USD 可用余额低于 V3 最低单笔金额。"})
    if incompatible_managed:
        warnings.append(
            {
                "code": "INCOMPATIBLE_MANAGED_OFFERS",
                "message": f"启动后将先撤销 {len(incompatible_managed)} 笔不兼容机器人挂单，确认消失后才创建新单。",
            }
        )
    if policy.adopt_external_offers and adoption_candidates:
        warnings.append(
            {
                "code": "EXTERNAL_ADOPTION_REQUIRES_CONFIRMATION",
                "message": (
                    f"启动后将核验并接管 {len(adoption_candidates)} 笔已到保护期限的外部 USD Funding 挂单；"
                    "仍需两次快照确认。"
                ),
            }
        )
    elif incompatible_external:
        warnings.append(
            {
                "code": "INCOMPATIBLE_EXTERNAL_OFFERS",
                "message": (
                    f"发现 {len(incompatible_external)} 笔外部挂单不符合策略；机器人不会撤销，但会计入资金上限。"
                ),
            }
        )
    if incompatible_credits:
        warnings.append(
            {
                "code": "INCOMPATIBLE_ACTIVE_CREDITS",
                "message": (
                    f"发现 {len(incompatible_credits)} 笔已成交贷款不符合新策略；这些旧贷款本身无法修改，"
                    "需等待借款人归还；这不会阻止修改策略，新订单仍严格按新策略生成。"
                ),
            }
        )
    if result["over_cap"]:
        warnings.append(
            {
                "code": "FUNDING_CAP_EXCEEDED",
                "message": "当前账户敞口超过 V3 资金上限；不会创建新单，只能撤销机器人挂单。",
            }
        )

    enabled_types = [
        label
        for label, enabled in (
            ("LIMIT", policy.enable_limit),
            ("FRR", policy.enable_frr),
            ("FRR_DELTA_FIXED", policy.enable_frr_delta_fixed),
            ("FRR_DELTA_VARIABLE", policy.enable_frr_delta_variable),
        )
        if enabled
    ]
    summary = {
        "strategyVersion": 3,
        "buildId": worker_build_id(),
        "strategySource": "SQLITE_ACTIVE",
        "activeStrategyVersion": active["version_id"],
        "policyHash": strategy_v3_version_id(policy),
        "enabledOrderTypes": enabled_types,
        "onlyLimit": enabled_types == ["LIMIT"],
        "policy": strategy_v3_api_values(policy),
        "orderSizing": order_sizing_payload(),
        "periodSelection": json_decimal(signals.get("periodSelection", {})),
        "periodActivity": json_decimal(store.period_activity(now - 86_400_000, "USD")),
        "accountSnapshot": basis,
        "accountDigest": _account_context_digest(LendingRuntimeV3._account(original_snapshot), original_snapshot),
        "account": json_decimal(account),
        "fundingPools": {
            pool: {
                "share": decimal_to_config(policy.pool_shares()[pool]),
                "netFloorAprPercent": decimal_percent_to_config(policy.floor_apr(pool)),
                "periodRange": ",".join(str(value) for value in getattr(policy, f"{pool}_periods")),
            }
            for pool in ("short", "medium", "long")
        },
        "executionLayers": {layer: decimal_to_config(value) for layer, value in policy.layer_shares().items()},
        "fundingLimit": {
            "maxAmount": None if policy.max_lend_amount is None else status_decimal(policy.max_lend_amount),
            "maxPercent": decimal_to_config(policy.max_lend_percent),
            "effectiveCap": status_decimal(result["funding_cap"]),
            "existingExposure": status_decimal(result["existing_exposure"]),
            "capRemaining": status_decimal(result["cap_remaining"]),
        },
        "targetSlices": result["target_slice_count"],
        "actualSlices": len(result["plan"]),
        "planHash": result["plan_hash"],
        "strategyPlan": json_decimal(result["plan"]),
        "pendingCancellations": incompatible_managed,
        "externalIncompatibilities": incompatible_external,
        "externalAdoptionCandidates": json_decimal(adoption_candidates),
        "externalAdoptionDigest": _canonical_sha256(adoption_candidates),
        "ratioRebalanceCancellations": [],
        "offerPoolAllocation": {
            "basis": result.get("allocation_basis"),
            "target": json_decimal(result.get("target_offer_amounts", {})),
            "current": json_decimal(result.get("current_offer_amounts", {})),
            "deviation": json_decimal(result.get("deviation_amounts", {})),
            "tolerance": status_decimal(result.get("ratio_tolerance", 0)),
        },
        "offerLayerAllocation": {
            "basis": result.get("layer_allocation_basis"),
            "target": json_decimal(result.get("target_layer_amounts", {})),
            "current": json_decimal(result.get("current_layer_amounts", {})),
            "deviation": json_decimal(result.get("layer_deviation_amounts", {})),
            "tolerance": status_decimal(result.get("ratio_tolerance", 0)),
        },
        "nonChangeableCredits": incompatible_credits,
        "marketSignals": json_decimal(signals),
    }
    return {"checks": checks, "warnings": warnings, "summary": summary}


def create_controlled_bot_preflight(config_path, client_factory=None, now=None, context=None):
    context = process_context(config_path, context, client_factory=client_factory)
    state = context.process_state
    client_factory = client_factory or context.client_factory or Bitfinex
    with state.lock:
        if controlled_bot_running(config_path, context):
            raise ConfigError("机器人已在运行")
    read_config(config_path)
    digest_before = config_sha256(config_path)
    result = evaluate_live_preflight(config_path, client_factory=client_factory, context=context)
    digest_after = config_sha256(config_path)
    if digest_before != digest_after:
        result["checks"].append(
            {
                "id": "config_changed",
                "label": "策略配置稳定性",
                "status": "fail",
                "detail": "策略配置在预检过程中发生变化，请重新运行预检",
            }
        )
    can_start = bool(result["checks"]) and all(check["status"] == "pass" for check in result["checks"])
    issued_at = context.now() if now is None else float(now)
    expires_at = issued_at + PREFLIGHT_TTL_SECONDS
    preflight_id = secrets.token_urlsafe(24) if can_start else None
    response = {
        "preflightId": preflight_id,
        "expiresAt": datetime.datetime.fromtimestamp(expires_at, datetime.timezone.utc).isoformat(timespec="seconds"),
        "canStart": can_start,
        **result,
    }
    with state.lock:
        state.preflight = None
        if controlled_bot_running(config_path, context):
            response["canStart"] = False
            response["preflightId"] = None
            response["checks"].append(
                {"id": "process", "label": "机器人进程", "status": "fail", "detail": "机器人已在运行"}
            )
        elif can_start:
            summary = result.get("summary", {})
            state.preflight = {
                "preflightId": preflight_id,
                "expiresAt": expires_at,
                "configDigest": digest_after,
                "activeStrategyVersion": summary.get("activeStrategyVersion"),
                "policyHash": summary.get("policyHash"),
                "planHash": summary.get("planHash"),
                "accountDigest": summary.get("accountDigest"),
                "buildId": summary.get("buildId"),
                "cancellationDigest": _canonical_sha256(summary.get("pendingCancellations") or []),
                "externalAdoptionDigest": summary.get("externalAdoptionDigest"),
                "externalAdoptionIds": [int(row["id"]) for row in summary.get("externalAdoptionCandidates") or []],
                "clientFactory": client_factory,
            }
    return response


def consume_controlled_bot_preflight(config_path, preflight_id, now=None, context=None):
    context = process_context(config_path, context)
    state = context.process_state
    current = state.preflight
    state.preflight = None
    if not current or not preflight_id or current["preflightId"] != preflight_id:
        raise ConfigError("预检令牌无效或已使用，请重新运行预检")
    checked_at = context.now() if now is None else float(now)
    if checked_at > current["expiresAt"]:
        raise ConfigError("预检已过期，请重新运行预检")
    if config_sha256(config_path) != current["configDigest"]:
        raise ConfigError("策略配置在预检后发生变化，请重新运行预检")
    store, settings = v3_store_for_config(config_path)
    active, policy = ensure_active_strategy_v3(store, settings)
    if store.strategy("DRAFT") is not None or store.strategy("PENDING") is not None:
        raise ConfigError("预检后出现 DRAFT 或 PENDING 策略，请先应用或放弃并重新预检")
    if active["version_id"] != current.get("activeStrategyVersion"):
        raise ConfigError("ACTIVE 策略在预检后发生变化，请重新运行预检")
    if strategy_v3_version_id(policy) != current.get("policyHash"):
        raise ConfigError("ACTIVE 策略哈希在预检后发生变化，请重新运行预检")
    if worker_build_id() != current.get("buildId"):
        raise ConfigError("Worker build 在预检后发生变化，请重新运行预检")
    refreshed = evaluate_live_preflight(
        config_path,
        client_factory=current.get("clientFactory") or context.client_factory or Bitfinex,
        context=context,
    )
    summary = refreshed.get("summary", {})
    # Report an account mutation specifically even when the mutation itself
    # makes the refreshed preflight fail (for example a newly unreconciled
    # available balance). This is both more actionable and preserves the
    # single-use confirmation contract.
    if summary.get("accountDigest") != current.get("accountDigest"):
        raise ConfigError("账户快照在预检后发生变化，请重新运行预检")
    failed = [check for check in refreshed.get("checks", []) if check.get("status") != "pass"]
    if failed:
        raise ConfigError("启动前重新核验失败，请重新运行预检")
    if summary.get("planHash") != current.get("planHash"):
        raise ConfigError("实际 V3 计划在预检后发生变化，请重新运行预检")
    if _canonical_sha256(summary.get("pendingCancellations") or []) != current.get("cancellationDigest"):
        raise ConfigError("待撤销挂单集合在预检后发生变化，请重新运行预检")
    if summary.get("externalAdoptionDigest") != current.get("externalAdoptionDigest"):
        raise ConfigError("待接管外部挂单集合在预检后发生变化，请重新运行预检")
    current_ids = [int(row["id"]) for row in summary.get("externalAdoptionCandidates") or []]
    if current_ids != current.get("externalAdoptionIds"):
        raise ConfigError("待接管外部挂单集合在预检后发生变化，请重新运行预检")
    # Starting a worker never substitutes for two authoritative observations.
    # The worker persists protection and adopts eligible offers after confirmation.
    return {"adoptedOfferIds": [], "queuedOfferIds": current_ids}


def start_controlled_bot(config_path, status_path, preflight_id, context=None, preserve_recovery=False):
    context = process_context(config_path, context)
    state = context.process_state
    with state.lock:
        if controlled_bot_running(config_path, context):
            raise ConfigError("机器人已在运行")
        consume_controlled_bot_preflight(config_path, preflight_id, context=context)
        store, _ = v3_store_for_config(config_path)
        # A restart must never discard a durable recovery episode.  The new
        # worker first obtains authoritative snapshots and only resumes writes
        # through the normal recovery barrier.
        runtime = store.runtime()
        recovery = store.recovery_status()
        if runtime.get("safe_manual"):
            raise ConfigError("存在需要人工确认的未决写入，不能启动实盘")
        if runtime.get("safe_reason") or recovery.get("active"):
            store.set_mode("PAUSED", "restart_preserves_recovery")
        os.makedirs(os.path.dirname(status_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(context.process_log_path) or ".", exist_ok=True)

        command = [
            sys.executable,
            os.path.abspath(__file__),
            "--config",
            config_path,
            "--no-server",
            "--live",
            "--confirmed-preflight",
            "--json",
            status_path,
            "--jsonsize",
            "200",
        ]
        startupinfo = None
        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        cleanup_controlled_bot_handle(context)
        state.log_handle = open(context.process_log_path, "ab", buffering=0)
        try:
            state.process = subprocess.Popen(
                command,
                cwd=context.project_root,
                stdin=subprocess.DEVNULL,
                stdout=state.log_handle,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
                startupinfo=startupinfo,
            )
        except Exception:
            cleanup_controlled_bot_handle(context)
            raise
        state.started_at = timestamp()
        state.stop_reason = None
        active = store.strategy("ACTIVE") or {}
        state.auto_restart_authorization = {
            "session": state.supervisor_session,
            "configDigest": config_sha256(config_path),
            "buildId": worker_build_id(),
            "strategyVersion": active.get("version_id"),
            "authorizedAt": context.now(),
        }
        return controlled_bot_status(config_path, context)


def stop_controlled_bot(
    config_path=DEFAULT_CONFIG,
    reason="stopped_by_dashboard",
    context=None,
    preserve_authorization=False,
):
    context = process_context(config_path, context)
    state = context.process_state
    with state.lock:
        state.preflight = None
        if not preserve_authorization:
            state.auto_restart_authorization = None
        process = state.process
        internal_running = process is not None and process.poll() is None
        external = None if internal_running else external_live_process(config_path, context)
        if not internal_running and external is None:
            cleanup_controlled_bot_handle(context)
            return controlled_bot_status(config_path, context)
        if external is not None:
            if external.get("stateError"):
                raise ConfigError(external["stateError"])
            identity_error = _live_process_identity_error(external)
            if identity_error:
                raise ConfigError(identity_error)
            pid = int(external["pid"])
            os.kill(pid, signal.SIGTERM)
            deadline = context.now() + 5
            while context.now() < deadline and external_live_process(config_path, context) is not None:
                time.sleep(0.05)
            if external_live_process(config_path, context) is not None:
                raise ConfigError("实盘进程未在超时时间内释放单实例锁")
            state.stop_reason = reason
            return controlled_bot_status(config_path, context)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        cleanup_controlled_bot_handle(context)
        state.stop_reason = reason
        return controlled_bot_status(config_path, context)


def _watchdog_authorization_valid(config_path, context, authorization):
    if not authorization or authorization.get("session") != context.process_state.supervisor_session:
        return False
    if authorization.get("configDigest") != config_sha256(config_path):
        return False
    if authorization.get("buildId") != worker_build_id():
        return False
    store, _ = v3_store_for_config(config_path)
    active = store.strategy("ACTIVE") or {}
    return authorization.get("strategyVersion") == active.get("version_id")


def worker_supervisor_loop(config_path, status_path, context):
    state = context.process_state
    while not state.supervisor_stop.wait(5):
        authorization = state.auto_restart_authorization
        if not authorization:
            continue
        try:
            store, _ = v3_store_for_config(config_path)
            recovery = store.recovery_status()
            status = controlled_bot_status(config_path, context)
            now_ms = int(context.now() * 1000)
            if status["running"]:
                heartbeat = recovery.get("heartbeatAt")
                authorized_ms = int(float(authorization.get("authorizedAt") or context.now()) * 1000)
                # A heartbeat belongs to the worker that wrote it.  After a
                # controlled restart the persisted value may be older than the
                # new process, so never start the five-minute timeout before
                # this session's authorization time.
                baseline = max(int(heartbeat or 0), authorized_ms)
                if now_ms - baseline < WORKER_HEARTBEAT_TIMEOUT_MS:
                    continue
                runtime = store.runtime()
                target = recovery.get("targetMode") or runtime.get("previous_mode") or runtime["mode"]
                if target not in {"LIVE", "PAUSED", "REPLAY"}:
                    target = "LIVE"
                if runtime["mode"] != "PAUSED":
                    store.set_mode("PAUSED", "AUTO_RECOVERY:WORKER_HEARTBEAT_TIMEOUT")
                store.begin_recovery(
                    "WORKER_HEARTBEAT_TIMEOUT",
                    "Worker heartbeat has not advanced for five minutes",
                    origin_mode=target,
                    target_mode=target,
                )
                stop_controlled_bot(
                    config_path,
                    reason="watchdog_heartbeat_timeout",
                    context=context,
                    preserve_authorization=True,
                )
                continue
            if not recovery["active"] or recovery["manualRequired"]:
                state.auto_restart_authorization = None
                if not recovery["manualRequired"]:
                    state.stop_reason = state.stop_reason or "unexpected_worker_exit"
                continue
            if recovery.get("nextProbeAt") and now_ms < int(recovery["nextProbeAt"]):
                continue
            if not _watchdog_authorization_valid(config_path, context, authorization):
                store.begin_recovery(
                    "SUPERVISOR_AUTHORIZATION_INVALID",
                    "Dashboard session, build, configuration, or strategy changed",
                    origin_mode=recovery.get("originMode") or "LIVE",
                    target_mode=recovery.get("targetMode") or "LIVE",
                )
                # The same Dashboard session can safely refresh its own
                # authorization after a build/configuration change. A fresh
                # preflight below still proves account, credentials and plan
                # before any worker restart.
                active = store.strategy("ACTIVE") or {}
                authorization = {
                    "session": state.supervisor_session,
                    "configDigest": config_sha256(config_path),
                    "buildId": worker_build_id(),
                    "strategyVersion": active.get("version_id"),
                    "authorizedAt": context.now(),
                }
                state.auto_restart_authorization = authorization
            preflight = create_controlled_bot_preflight(config_path, context=context)
            if not preflight.get("canStart") or not preflight.get("preflightId"):
                store.record_recovery_failure("watchdog preflight did not pass", "WATCHDOG_PREFLIGHT", now_ms)
                continue
            start_controlled_bot(
                config_path,
                status_path,
                preflight["preflightId"],
                context=context,
                preserve_recovery=True,
            )
            state.stop_reason = None
        except Exception as exc:
            try:
                store, _ = v3_store_for_config(config_path)
                decision = classify_runtime_error(exc)
                store.record_recovery_failure(str(exc), decision.category)
            except Exception:
                state.auto_restart_authorization = None


def load_dashboard_static_snapshot(directory, build_id, csrf_token=""):
    return load_static_snapshot(
        directory,
        build_id,
        csrf_token,
        DASHBOARD_BUILD_PLACEHOLDER,
        DASHBOARD_CSRF_PLACEHOLDER,
        APP_VERSION,
    )


def make_dashboard_handler(directory, config_path, status_path, build_id=None, context=None):
    context = process_context(config_path, context)
    build_id = build_id or dashboard_build_id()
    csrf_token = secrets.token_urlsafe(32)
    static_assets = load_dashboard_static_snapshot(directory, build_id, csrf_token)

    class Handler(DashboardRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

    Handler.config_path = config_path
    Handler.status_path = status_path
    Handler.build_id = build_id
    Handler.static_assets = static_assets
    Handler.csrf_token = csrf_token
    Handler.app_context = context
    Handler.dashboard_started_at = timestamp()
    Handler.application = DashboardApplication(
        project_root=os.path.abspath(os.path.dirname(__file__)),
        service_id=DASHBOARD_SERVICE_ID,
        timestamp=timestamp,
        status_payload=dashboard_status_payload,
        config_payload=config_api_payload,
        controlled_status=controlled_bot_status,
        runtime_payload=runtime_v3_payload,
        store_for_config=v3_store_for_config,
        stats_payload=stats_v3_payload,
        strategy_preview=strategy_v3_preview,
        save_strategy_draft=save_strategy_v3_draft,
        apply_strategy_draft=apply_strategy_v3_draft,
        discard_strategy_draft=discard_strategy_v3_draft,
        controlled_running=controlled_bot_running,
        stop_controlled=stop_controlled_bot,
        replay_from_store=replay_v3_from_store,
        create_preflight=create_controlled_bot_preflight,
        start_controlled=start_controlled_bot,
    )
    return Handler


def start_web_server(log, config_path, status_path, context=None, raise_errors=False):
    context = process_context(config_path, context)
    state = context.process_state
    port = 8000
    host = "127.0.0.1"
    directory = os.path.join(os.getcwd(), "www")
    state.supervisor_session = secrets.token_urlsafe(24)
    state.auto_restart_authorization = None
    state.supervisor_stop.clear()
    state.supervisor_thread = threading.Thread(
        target=worker_supervisor_loop,
        args=(config_path, status_path, context),
        daemon=True,
        name="v3-worker-supervisor",
    )
    state.supervisor_thread.start()
    handler = make_dashboard_handler(directory, config_path, status_path, context=context)
    try:
        if state.market_hub is None and websocket_dependency_available():
            store, settings = v3_store_for_config(config_path)
            _, active_policy = ensure_active_strategy_v3(store, settings)
            state.market_hub = BitfinexMarketDataHub(
                "",
                "",
                symbol="fUSD",
                store=store,
                fallback_seconds=active_policy.ws_fallback_seconds,
                rest_stale_seconds=active_policy.rest_stale_seconds,
            )
            state.market_hub.start()
        server = ThreadingHTTPServer((host, port), handler)
        state.dashboard_server = server
        try:
            log.log(f"网页控制台已启动：http://{host}:{port}/lendingbot.html")
            server.serve_forever()
        finally:
            server.server_close()
            if state.dashboard_server is server:
                state.dashboard_server = None
            state.supervisor_stop.set()
            if state.supervisor_thread is not None:
                state.supervisor_thread.join(timeout=5)
                state.supervisor_thread = None
    except Exception as exc:
        log.log(f"网页控制台启动失败：{exc}")
        if raise_errors:
            raise
    finally:
        state.supervisor_stop.set()
        if state.supervisor_thread is not None and state.supervisor_thread is not threading.current_thread():
            state.supervisor_thread.join(timeout=5)
            state.supervisor_thread = None


def stop_web_server(log, context=None):
    context = process_context(DEFAULT_CONFIG, context)
    state = context.process_state
    if state.market_hub is not None:
        state.market_hub.stop()
        state.market_hub = None
    server = state.dashboard_server
    if server is None:
        return
    try:
        log.log("正在停止网页控制台")
        server.shutdown()
        server.server_close()
        if state.dashboard_server is server:
            state.dashboard_server = None
    except Exception as exc:
        log.log(f"停止网页控制台失败：{exc}")


def publish_safe_status(log, store, exc):
    decision = classify_runtime_error(exc)
    reason = f"{decision.category}:{type(exc).__name__}"
    current = store.runtime()
    if decision.retryable:
        recovery = store.recovery_status()
        origin = recovery.get("targetMode") or current["previous_mode"] or current["mode"]
        if current["mode"] != "PAUSED":
            runtime = store.set_mode("PAUSED", f"AUTO_RECOVERY:{decision.category}")
        else:
            runtime = current
        store.begin_recovery(
            decision.category,
            str(exc),
            origin_mode=origin,
            target_mode=origin,
            manual_required=False,
        )
        store.record_recovery_failure(str(exc), decision.category)
    else:  # Defensive fallback if a future classifier adds a non-retryable type.
        runtime = store.set_mode("PAUSED", reason)
        store.begin_recovery(decision.category, str(exc), origin_mode=current["mode"], target_mode=current["mode"])
    log.updateMetaValue("schemaVersion", STATUS_SCHEMA_VERSION)
    log.updateMetaValue("operationMode", runtime["mode"])
    log.updateMetaValue("runtime", runtime)
    log.updateMetaValue("recovery", store.recovery_status())
    log.refreshStatus(f"{runtime['mode']}：{type(exc).__name__}：{exc}")
    log.persistStatus()
    return runtime


def build_app_context(args, settings):
    root = os.path.abspath(os.path.dirname(__file__))
    context = AppContext.for_project(
        root,
        config_path=args.config,
        status_path=settings.json_file or DEFAULT_DASHBOARD_JSON,
        state_db_path=settings.state_db_file,
        client_factory=Bitfinex,
    )
    args.config = context.config_path
    settings.state_db_file = context.state_db_path
    if settings.json_file:
        settings.json_file = context.status_path
    return context


def ensure_offline_database_access(context):
    inspection = LiveProcessLock.inspect(context.live_lock_path)
    if inspection["locked"]:
        pid = inspection.get("metadata", {}).get("pid")
        raise ConfigError(f"offline command rejected while LIVE worker lock is held (PID {pid})")


def migrate_legacy_state(context, store):
    migration_id = "managed-offers-v2-to-schema-v4"
    existing = store.legacy_migration(migration_id)
    if existing is not None:
        return {**existing["result"], "migrationId": migration_id, "idempotent": True}
    source_path = os.path.join(context.project_root, ".state", "managed-offers.json")
    imported = store.import_legacy_managed_offers(source_path)
    result = {
        "migrationId": migration_id,
        "sourcePath": source_path,
        "sourcePresent": os.path.exists(source_path),
        "importedOffers": imported,
        "idempotent": False,
    }
    store.record_legacy_migration(migration_id, source_path, result)
    return result


def apply_validated_v36(report_path, settings, context):
    from V36Validation import build_validation_certificate

    live_lock = LiveProcessLock(context.live_lock_path)
    if not live_lock.acquire(context.config_path, {"role": "offline_v36_activation"}):
        raise ConfigError("offline activation rejected while the LIVE worker lock is held")
    try:
        store = LendingStateStore(context.state_db_path, clock=context.now)
        with open(report_path, encoding="utf-8") as evidence:
            report = json.load(evidence)
        selection = report.get("selection") if isinstance(report, dict) else None
        parameters = selection.get("candidatePolicy") if isinstance(selection, dict) else None
        if not isinstance(parameters, dict):
            raise ValueError("V3.6 activation requires a report with a complete candidate policy")
        candidate = policy_v3_with_overrides(StrategyPolicyV3(), parameters)
        validate_policy_v3(candidate, require_live_floors=True)
        active = store.strategy("ACTIVE")
        base = strategy_v3_from_record(active) if active else settings.strategy_v3
        unchanged = set(vars(base)) - {"quick_pricing_mode", "quick_start_premium_apr_points"}
        if any(getattr(base, name) != getattr(candidate, name) for name in unchanged):
            raise ValueError("V3.6 activation cannot change allocation, floors, limits or other existing controls")
        certificate = build_validation_certificate(report, candidate, int(context.now() * 1000))
        activated = store.install_validated_strategy(
            json_decimal(vars(candidate)), certificate, None if active is None else active["version_id"],
        )
        try:
            mirror_active_strategy_v3(context.config_path, candidate)
        except OSError as exc:
            raise ConfigError(f"策略已启用，但配置镜像写入失败。修复文件权限后可重试本命令。{exc}") from exc
        return {"strategyVersion": activated["version_id"], "reportDigest": certificate["reportDigest"]}
    finally:
        live_lock.release()


def run_offline_commands(args, settings, context):
    ensure_offline_database_access(context)
    results = {}
    if getattr(args, "apply_validated_v36", None):
        results["v36Activation"] = apply_validated_v36(args.apply_validated_v36, settings, context)
    if args.migrate_legacy or args.backfill_market_data or args.evaluate_strategy:
        store = LendingStateStore(context.state_db_path, clock=context.now)
    if args.migrate_legacy:
        results["legacyMigration"] = migrate_legacy_state(context, store)
    if args.backfill_market_data:
        results["marketBackfill"] = backfill_public_market_data(Bitfinex("", ""), store, days=args.research_days)
    if args.evaluate_strategy:
        _, active_policy = ensure_active_strategy_v3(store, settings)
        report = evaluate_strategies(
            store,
            active_policy,
            Decimal(str(args.principal)),
            days=args.research_days,
        )
        report_path = os.path.join(context.project_root, ".state", "research", "strategy-validation-report.json")
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        write_research_report(report_path, report)
        results["strategyEvaluation"] = {
            "reportPath": report_path,
            "selection": report["selection"],
            "data": report["data"],
        }
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    return 0


def main(argv=None):
    args = parse_args(argv)
    offline_requested = any((
        args.migrate_legacy, args.backfill_market_data, args.evaluate_strategy, args.apply_validated_v36,
    ))
    project_root = os.path.abspath(os.path.dirname(__file__))
    args.config = AppContext.for_project(project_root, config_path=args.config).config_path
    if args.dashboard and args.live:
        print("Configuration error: --dashboard 不能与 --live 同时使用", file=sys.stderr)
        return 1
    if offline_requested and (args.dashboard or args.live):
        print("Configuration error: offline commands cannot be combined with --dashboard or --live", file=sys.stderr)
        return 1
    if not offline_requested and not args.dashboard and not args.live:
        print("Configuration error: 运行机器人必须显式传入 --live；如只需控制台请使用 --dashboard", file=sys.stderr)
        return 1
    config, config_created = read_config(args.config)
    try:
        settings = build_settings(args, config, config_created)
        validate_settings(settings)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    if args.dashboard:
        settings.web_server = True
    if settings.web_server and not settings.json_file:
        settings.json_file = DEFAULT_DASHBOARD_JSON.replace("\\", "/")
        settings.json_log_size = 200

    context = build_app_context(args, settings)
    if offline_requested:
        try:
            return run_offline_commands(args, settings, context)
        except (ConfigError, StateStoreError, ValueError, RuntimeError, BitfinexApiError) as exc:
            print(f"Offline command failed: {exc}", file=sys.stderr)
            return 1

    log = Logger(
        settings.json_file,
        settings.json_log_size,
        sensitive_values=(settings.api_key, settings.api_secret),
    )
    if config_created:
        log.log("已从 default.cfg.example 复制出 default.cfg，请在里面填写 Bitfinex API 密钥。")

    if args.dashboard:
        dashboard_lock = LiveProcessLock(context.dashboard_lock_path)
        if not dashboard_lock.acquire(args.config, {"role": "dashboard", "service": DASHBOARD_SERVICE_ID}):
            print("Dashboard startup rejected: another dashboard instance holds the lock", file=sys.stderr)
            return 1
        reconcile_orphaned_live_runtime(args.config, context)
        normalization = normalize_current_active_strategy(args.config)
        if normalization["changed"]:
            log.log(
                f"V3 ACTIVE 已规范化：{normalization['fromVersion']} -> {normalization['versionId']}；"
                f"配置和数据库备份位于 {os.path.dirname(normalization['backup']['database'])}"
            )
        log.log("控制台已就绪；只有完成只读预检并确认后才会启动实盘机器人。")
        exit_code = 0
        try:
            # The dashboard server owns this process. Running it on the main
            # thread makes bind/serve failures terminate the process and release
            # the single-instance lock instead of leaving a headless lock holder.
            start_web_server(log, args.config, settings.json_file, context, raise_errors=True)
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            exit_code = 1
            log.log(f"控制台进程因网页服务失败而退出：{exc}")
        finally:
            stop_web_server(log, context)
            dashboard_lock.release()
            log.log("已退出")
        return exit_code

    normalization = normalize_current_active_strategy(args.config)
    if normalization["changed"]:
        log.log(f"V3 ACTIVE 已规范化：{normalization['fromVersion']} -> {normalization['versionId']}")
    log.log("欢迎使用 Bitfinex 自动放贷机器人（实盘运行）")
    preflight = evaluate_live_preflight(args.config)
    failed_checks = [check for check in preflight["checks"] if check["status"] == "fail"]
    if failed_checks:
        for check in failed_checks:
            log.log(f"实盘预检失败：{check['label']} — {check['detail']}")
        return 1
    for warning in preflight["warnings"]:
        log.log("实盘预检警告：" + warning["message"])
    if not args.confirmed_preflight:
        try:
            confirmation = input("只读预检已通过。输入 LIVE 确认本次实盘启动：").strip()
        except (EOFError, KeyboardInterrupt):
            confirmation = ""
        if confirmation != "LIVE":
            log.log("未完成人工确认，保持 PAUSED。")
            return 1
    live_lock = LiveProcessLock(context.live_lock_path)
    if not live_lock.acquire(args.config, {"role": "live_worker", "service": "mika-lending-worker-v3"}):
        log.log("实盘启动被拒绝：另一个机器人进程已持有单实例锁。")
        return 1
    state_store_v3 = LendingStateStore(settings.state_db_file, clock=context.now)
    active, active_policy = ensure_active_strategy_v3(state_store_v3, settings)
    if active["version_id"] != preflight["summary"].get("activeStrategyVersion"):
        live_lock.release()
        log.log("实盘启动被拒绝：预检后 ACTIVE 已变化，请重新预检。")
        return 1
    log.log("实盘预检通过，开始同步账户并执行策略。")
    client = Bitfinex(settings.api_key, settings.api_secret)
    # Preserve a durable protected PAUSED across process restarts so bootstrap can reconcile
    # the uncertain exchange write from authoritative account data. Normal
    # PAUSED starts still transition directly to LIVE after the confirmed preflight.
    if not state_store_v3.runtime().get("safe_reason") and not state_store_v3.recovery_status().get("active"):
        state_store_v3.set_mode("LIVE", "live_preflight_confirmed")
    runtime_v3 = LendingRuntimeV3(
        client,
        active_policy,
        state_store_v3,
        log=log,
        auto_transfer_wallets=(settings.transfer_from_wallets if "USD" in settings.transferable_currencies else ()),
        on_policy_activated=lambda policy, _version: mirror_active_strategy_v3(args.config, policy),
        clock=context.now,
    )

    if settings.web_server:
        thread = threading.Thread(
            target=start_web_server,
            args=(log, args.config, settings.json_file, context),
            daemon=True,
        )
        thread.start()

    sleep_time = settings.sleep_active
    deferred_recovery_error = None
    try:
        while True:
            if deferred_recovery_error is not None:
                try:
                    publish_safe_status(log, state_store_v3, deferred_recovery_error)
                except Exception as persist_exc:
                    if not classify_runtime_error(persist_exc).retryable:
                        return 1
                    time.sleep(30)
                    continue
                deferred_recovery_error = None
                # Persisting recovery is a read-only cycle. Strategy writes are
                # deferred until a later normal cycle.
                time.sleep(30)
                continue
            try:
                runtime_v3.cycle()
                sleep_time = 30 if state_store_v3.recovery_status()["active"] else settings.sleep_active
                if settings.once:
                    break
                time.sleep(sleep_time)
            except Exception as exc:
                log.log("错误：" + str(exc))
                decision = classify_runtime_error(exc)
                try:
                    publish_safe_status(log, state_store_v3, exc)
                except Exception as persist_exc:
                    if decision.retryable and classify_runtime_error(persist_exc).retryable:
                        deferred_recovery_error = exc
                    else:
                        return 1
                print(timestamp())
                print(traceback.format_exc())
                if settings.once:
                    return 1
                if deferred_recovery_error is None:
                    try:
                        if state_store_v3.recovery_status()["manualRequired"]:
                            return 1
                    except Exception as status_exc:
                        if decision.retryable and classify_runtime_error(status_exc).retryable:
                            deferred_recovery_error = exc
                        else:
                            return 1
                sleep_time = 30
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        pass
    finally:
        runtime_v3.shutdown()
        if not state_store_v3.runtime().get("safe_reason"):
            state_store_v3.set_mode("PAUSED", "live_process_stopped")
        if settings.web_server:
            stop_web_server(log, context)
        live_lock.release()
        log.log("已退出")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
