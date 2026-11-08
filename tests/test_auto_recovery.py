import http.client
import sqlite3

import pytest

import lendingbot
from Recovery import classify_runtime_error, recovery_category_for_reason, recovery_delay_seconds
from StateStore import LendingStateStore
from bitfinex import BitfinexAmbiguousWriteError, BitfinexApiError, BitfinexTransientError


class StatusLogger:
    def __init__(self):
        self.values = {}

    def updateMetaValue(self, key, value):
        self.values[key] = value

    def refreshStatus(self, value):
        self.values["status"] = value

    def persistStatus(self):
        return None


def test_recovery_backoff_caps_at_five_minutes():
    assert [recovery_delay_seconds(item) for item in range(6)] == [30, 60, 120, 300, 300, 300]


@pytest.mark.parametrize(
    ("error", "category", "retryable"),
    [
        (http.client.IncompleteRead(b""), "NETWORK_TRANSPORT", True),
        (BitfinexTransientError("limited", category="BITFINEX_HTTP_TRANSIENT"), "BITFINEX_HTTP_TRANSIENT", True),
        (BitfinexApiError("forbidden", category="AUTH_PERMISSION", manual_required=True), "AUTH_PERMISSION", True),
        (BitfinexAmbiguousWriteError("connection ended after send"), "AMBIGUOUS_WRITE", True),
        (sqlite3.OperationalError("database is locked"), "DATABASE_BUSY", True),
        (sqlite3.OperationalError("database is malformed"), "DATABASE_ERROR", True),
        (sqlite3.DatabaseError("database disk image is malformed"), "UNEXPECTED_RUNTIME_ERROR", True),
        (TypeError("bug"), "PROGRAM_ERROR", True),
        (KeyError("unknown"), "UNEXPECTED_RUNTIME_ERROR", True),
    ],
)
def test_runtime_error_classification(error, category, retryable):
    decision = classify_runtime_error(error)
    assert (decision.category, decision.retryable) == (category, retryable)


@pytest.mark.parametrize(
    ("reason", "category"),
    [
        ("MARKET_DATA_STALE", "MARKET_DATA"),
        ("ACCOUNT_AVAILABLE_BALANCE_UNKNOWN", "ACCOUNT_DATA"),
        ("ACCOUNT_RECONCILIATION_MISMATCH", "ACCOUNT_DATA"),
        ("AMBIGUOUS_WALLET_TRANSFER", "AMBIGUOUS_WRITE"),
        ("AMBIGUOUS_SUBMIT:9", "AMBIGUOUS_WRITE"),
        ("AMBIGUOUS_CANCEL:9", "AMBIGUOUS_WRITE"),
        ("WORKER_BUILD_MISMATCH_UNVERIFIED", "WORKER_BUILD"),
        ("ordinary pause", None),
    ],
)
def test_recovery_category_for_pause_reason(reason, category):
    assert recovery_category_for_reason(reason) == category


def test_incomplete_read_pauses_then_resumes_live_after_two_clean_snapshots(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: 1000)
    store.set_mode("LIVE", "test")
    logger = StatusLogger()

    lendingbot.publish_safe_status(logger, store, BitfinexTransientError("IncompleteRead(0 bytes read)"))

    assert store.runtime()["mode"] == "PAUSED"
    recovery = store.recovery_status()
    assert recovery["active"] and recovery["targetMode"] == "LIVE" and not recovery["manualRequired"]
    store.record_consistent_sync(1_030_000)
    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_060_000)
    assert store.runtime()["mode"] == "LIVE"
    assert store.consume_resume_barrier() is True
    assert store.consume_resume_barrier() is False


def test_failed_probe_resets_consecutive_snapshot_count(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: 1000)
    store.set_mode("LIVE", "test")
    store.set_mode("PAUSED", "AUTO_RECOVERY:NETWORK_TRANSPORT")
    store.begin_recovery("NETWORK_TRANSPORT", "offline", origin_mode="LIVE", target_mode="LIVE", now_ms=1_000_000)
    store.record_consistent_sync(1_030_000)
    assert store.recovery_status()["successfulSnapshots"] == 1
    store.record_recovery_failure("offline again", now_ms=1_040_000)
    assert store.recovery_status()["successfulSnapshots"] == 0
    store.record_consistent_sync(1_070_000)
    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_100_000)
    assert store.runtime()["mode"] == "LIVE"


def test_secondary_market_stale_pause_keeps_original_live_recovery_target(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: 1000)
    store.set_mode("LIVE", "test")
    store.set_mode("PAUSED", "AUTO_RECOVERY:NETWORK_TRANSPORT")
    store.begin_recovery(
        "NETWORK_TRANSPORT",
        "socket timed out",
        origin_mode="LIVE",
        target_mode="LIVE",
        now_ms=1_000_000,
    )

    # This is the exact production sequence: the failed REST probe leaves the
    # cached market stale, which adds a protected pause while runtime is PAUSED.
    store.enter_protected_pause("MARKET_DATA_STALE")

    recovery = store.recovery_status()
    assert store.runtime()["mode"] == "PAUSED"
    assert recovery["originMode"] == "LIVE"
    assert recovery["targetMode"] == "LIVE"
    store.record_consistent_sync(1_030_000)
    store.record_consistent_sync(1_060_000)
    assert store.runtime()["mode"] == "LIVE"


def test_program_error_attempts_automatic_read_only_recovery(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    lendingbot.publish_safe_status(StatusLogger(), store, TypeError("broken invariant"))

    recovery = store.recovery_status()
    assert store.runtime()["mode"] == "PAUSED"
    assert recovery["active"] and not recovery["manualRequired"] and recovery["targetMode"] == "LIVE"
    base = recovery["lastProbeAt"]
    store.record_consistent_sync(base + 30_000)
    store.record_consistent_sync(base + 60_000)
    assert store.runtime()["mode"] == "LIVE"


def test_auth_failure_attempts_automatic_read_only_recovery(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")

    lendingbot.publish_safe_status(
        StatusLogger(),
        store,
        BitfinexApiError("forbidden", category="AUTH_PERMISSION", manual_required=True),
    )

    recovery = store.recovery_status()
    assert recovery["active"] and not recovery["manualRequired"] and recovery["targetMode"] == "LIVE"
    base = recovery["lastProbeAt"]
    store.record_consistent_sync(base + 30_000)
    store.record_consistent_sync(base + 60_000)
    assert store.runtime()["mode"] == "LIVE"


def test_ambiguous_submit_stays_read_only_then_automatically_recovers_after_reconciliation(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    _, intent = store.reserve_intent(
        {
            "currency": "USD",
            "amount": "150",
            "submitted_rate": "0.0003",
            "effective_rate": "0.0003",
            "period": 2,
            "offer_type": "LIMIT",
            "flags": 0,
            "pool": "short",
            "layer": "quick",
            "strategy_version": "test",
            "slice_key": "test:short:quick:0",
        },
        "150",
    )
    store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection ended after send")

    recovery = store.recovery_status()
    assert recovery["active"] and not recovery["manualRequired"]
    request_ms = int(store.intent(intent["id"])["request_started_at_ms"])
    store.reconcile_offers(
        [
            {
                "id": 9001,
                "currency": "USD",
                "amount": "150",
                "amount_original": "150",
                "rate": "0.0003",
                "period": 2,
                "offer_type": "LIMIT",
                "flags": 0,
                "status": "ACTIVE",
                "mts_created": request_ms,
                "mts_updated": request_ms,
            }
        ]
    )
    assert store.reconcile_ambiguous_candidates()
    recovery = store.recovery_status()
    base = recovery["lastProbeAt"]
    store.record_consistent_sync(base + 30_000)
    store.record_consistent_sync(base + 60_000)
    assert store.runtime()["mode"] == "LIVE"


def test_manual_dashboard_pause_clears_auto_recovery(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    store.set_mode("PAUSED", "AUTO_RECOVERY:NETWORK_TRANSPORT")
    store.begin_recovery("NETWORK_TRANSPORT", "offline", origin_mode="LIVE", target_mode="LIVE")
    assert store.recovery_status()["active"]

    store.set_mode("PAUSED", "dashboard_pause")

    assert not store.recovery_status()["active"]
    store.record_consistent_sync(9_999_999_999_999)
    assert store.runtime()["mode"] == "PAUSED"


def test_schema_16_preserves_runtime_and_adds_recovery_and_allocation_state(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = LendingStateStore(path)
    store.set_mode("PAUSED", "dashboard_stop")
    with store.read_connection() as connection:
        version = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        recovery_rows = connection.execute("SELECT COUNT(*) FROM recovery_state").fetchone()[0]
    assert (version, integrity, recovery_rows) == ("19", "ok", 1)


class OneSupervisorIteration:
    def __init__(self):
        self.calls = 0

    def wait(self, _seconds):
        self.calls += 1
        return self.calls > 1


def test_watchdog_hung_worker_enters_recovery_and_stops_exact_process(tmp_path, monkeypatch):
    context = lendingbot.AppContext.for_project(tmp_path, now=lambda: 400)
    context.process_state.supervisor_stop = OneSupervisorIteration()
    context.process_state.supervisor_session = "session"
    context.process_state.auto_restart_authorization = {
        "session": "session",
        "authorizedAt": 1,
    }
    store = LendingStateStore(context.state_db_path, clock=lambda: 400)
    store.set_mode("LIVE", "test")
    monkeypatch.setattr(lendingbot, "v3_store_for_config", lambda _path: (store, None))
    monkeypatch.setattr(lendingbot, "controlled_bot_status", lambda *_args, **_kwargs: {"running": True})
    stopped = []
    monkeypatch.setattr(
        lendingbot,
        "stop_controlled_bot",
        lambda *_args, **kwargs: stopped.append(kwargs.get("preserve_authorization")),
    )

    lendingbot.worker_supervisor_loop(context.config_path, context.status_path, context)

    recovery = store.recovery_status()
    assert stopped == [True]
    assert recovery["active"] and recovery["targetMode"] == "LIVE"
    assert recovery["category"] == "WORKER_HEARTBEAT_TIMEOUT"


def test_watchdog_ignores_stale_heartbeat_from_previous_worker(tmp_path, monkeypatch):
    context = lendingbot.AppContext.for_project(tmp_path, now=lambda: 400)
    context.process_state.supervisor_stop = OneSupervisorIteration()
    context.process_state.supervisor_session = "session"
    context.process_state.auto_restart_authorization = {
        "session": "session",
        "authorizedAt": 399,
    }
    store = LendingStateStore(context.state_db_path, clock=lambda: 400)
    store.set_mode("PAUSED", "dashboard_stop")
    store.touch_heartbeat(1_000)
    monkeypatch.setattr(lendingbot, "v3_store_for_config", lambda _path: (store, None))
    monkeypatch.setattr(lendingbot, "controlled_bot_status", lambda *_args, **_kwargs: {"running": True})
    stopped = []
    monkeypatch.setattr(
        lendingbot,
        "stop_controlled_bot",
        lambda *_args, **kwargs: stopped.append(kwargs.get("preserve_authorization")),
    )

    lendingbot.worker_supervisor_loop(context.config_path, context.status_path, context)

    assert stopped == []
    assert store.recovery_status()["active"] is False


def test_watchdog_restarts_only_after_valid_session_preflight(tmp_path, monkeypatch):
    context = lendingbot.AppContext.for_project(tmp_path, now=lambda: 100)
    context.process_state.supervisor_stop = OneSupervisorIteration()
    context.process_state.supervisor_session = "session"
    authorization = {"session": "session", "authorizedAt": 0}
    context.process_state.auto_restart_authorization = authorization
    store = LendingStateStore(context.state_db_path, clock=lambda: 100)
    store.set_mode("PAUSED", "AUTO_RECOVERY:NETWORK_TRANSPORT")
    store.begin_recovery(
        "NETWORK_TRANSPORT",
        "offline",
        origin_mode="LIVE",
        target_mode="LIVE",
        now_ms=0,
    )
    monkeypatch.setattr(lendingbot, "v3_store_for_config", lambda _path: (store, None))
    monkeypatch.setattr(lendingbot, "controlled_bot_status", lambda *_args, **_kwargs: {"running": False})
    monkeypatch.setattr(lendingbot, "_watchdog_authorization_valid", lambda *_args: True)
    monkeypatch.setattr(
        lendingbot,
        "create_controlled_bot_preflight",
        lambda *_args, **_kwargs: {"canStart": True, "preflightId": "fresh"},
    )
    started = []
    monkeypatch.setattr(
        lendingbot,
        "start_controlled_bot",
        lambda *_args, **kwargs: started.append(kwargs.get("preserve_recovery")),
    )

    lendingbot.worker_supervisor_loop(context.config_path, context.status_path, context)

    assert started == [True]
    assert context.process_state.stop_reason is None


def test_watchdog_invalid_build_or_config_refreshes_authorization_and_retries(tmp_path, monkeypatch):
    context = lendingbot.AppContext.for_project(tmp_path, now=lambda: 100)
    context.process_state.supervisor_stop = OneSupervisorIteration()
    context.process_state.supervisor_session = "session"
    context.process_state.auto_restart_authorization = {"session": "session", "authorizedAt": 0}
    store = LendingStateStore(context.state_db_path, clock=lambda: 100)
    store.set_mode("PAUSED", "AUTO_RECOVERY:NETWORK_TRANSPORT")
    store.begin_recovery(
        "NETWORK_TRANSPORT",
        "offline",
        origin_mode="LIVE",
        target_mode="LIVE",
        now_ms=0,
    )
    monkeypatch.setattr(lendingbot, "v3_store_for_config", lambda _path: (store, None))
    monkeypatch.setattr(lendingbot, "controlled_bot_status", lambda *_args, **_kwargs: {"running": False})
    monkeypatch.setattr(lendingbot, "_watchdog_authorization_valid", lambda *_args: False)

    lendingbot.worker_supervisor_loop(context.config_path, context.status_path, context)

    assert not store.recovery_status()["manualRequired"]
    assert context.process_state.auto_restart_authorization is not None
