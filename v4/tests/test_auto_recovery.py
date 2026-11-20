from __future__ import annotations

import http.client
import sqlite3
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from mika_v4.bitfinex import BitfinexError
from mika_v4.config import V4Policy, V4Settings
from mika_v4.domain import AccountSnapshot, IntentState, OfferSnapshot, RuntimeMode
from mika_v4.locks import LiveLockError
from mika_v4.recovery import HEARTBEAT_TIMEOUT_MS, classify_error, delay_seconds
from mika_v4.runtime import LendingRuntime
from mika_v4.store import V4Store
from mika_v4.supervisor import V4Supervisor


D = Decimal


def make_settings(tmp_path: Path) -> V4Settings:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = tmp_path / "default.cfg"
    config.write_text("[BITFINEX]\n[BOT_V4]\n[STRATEGY_V4]\n", encoding="utf-8")
    return V4Settings(
        api_key="key",
        api_secret="secret",
        policy=V4Policy(),
        state_db=tmp_path / "state.sqlite3",
        status_file=tmp_path / "status.json",
        config_file=config,
        repository_root=tmp_path,
    )


def test_fixed_infinite_backoff_and_error_classification() -> None:
    assert [delay_seconds(value) for value in range(7)] == [30, 60, 120, 300, 300, 300, 300]
    assert classify_error(http.client.IncompleteRead(b"x")).retryable
    assert classify_error(BitfinexError("429", retryable=True)).retryable
    assert classify_error(sqlite3.OperationalError("database is locked")).category == "DATABASE_BUSY"
    assert classify_error(sqlite3.DatabaseError("malformed")).manual_required
    assert classify_error(TypeError("bug")).manual_required


def test_schema_two_and_two_clean_snapshots_restore_shadow_next_cycle(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    with store.connect() as db:
        assert db.execute("SELECT version FROM schema_info").fetchone()[0] == 2
    store.enter_safe("temporary network failure", category="NETWORK_TRANSPORT")
    assert not store.record_consistent_snapshot(1_000_000)
    assert store.record_consistent_snapshot(1_031_000)
    assert store.mode() == RuntimeMode.SHADOW
    assert store.consume_resume_barrier()
    assert not store.consume_resume_barrier()


def test_schema_one_migration_creates_local_backup_and_rejects_future_schema(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE schema_info(version INTEGER NOT NULL)")
        db.execute("INSERT INTO schema_info VALUES(1)")
        db.commit()
    store = V4Store(path)
    with store.connect() as db:
        assert db.execute("SELECT version FROM schema_info").fetchone()[0] == 2
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert len(list((tmp_path / "backups").glob("schema-v1-*.sqlite3"))) == 1

    future = tmp_path / "future.sqlite3"
    with closing(sqlite3.connect(future)) as db:
        db.execute("CREATE TABLE schema_info(version INTEGER NOT NULL)")
        db.execute("INSERT INTO schema_info VALUES(3)")
        db.commit()
    try:
        V4Store(future)
    except RuntimeError as exc:
        assert "newer than supported" in str(exc)
    else:  # pragma: no cover - the assertion explains the safety boundary
        raise AssertionError("future schemas must fail closed")


def test_failure_resets_success_count_and_manual_pause_revokes_recovery(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.enter_safe("network", category="NETWORK_TRANSPORT")
    assert not store.record_consistent_snapshot(1_000_000)
    store.record_recovery_failure("again", now_ms=1_010_000)
    recovery = store.recovery_status()
    assert recovery["successfulSnapshots"] == 0
    assert recovery["attempts"] == 1
    store.set_mode(RuntimeMode.PAUSED)
    assert not store.recovery_status()["active"]


def test_multiple_unknown_submit_matches_require_a_human(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    created = int(time.time() * 1000)
    assert store.create_intent(
        "ambiguous",
        "SUBMIT",
        amount=D("150"),
        rate=D("0.0003"),
        period=2,
    )
    store.set_intent_state("ambiguous", IntentState.AMBIGUOUS)
    store.enter_safe("unknown submit", category="AMBIGUOUS_WRITE")
    offers = tuple(
        OfferSnapshot(
            offer_id=value,
            currency="USD",
            amount=D("150"),
            amount_original=D("150"),
            rate=D("0.0003"),
            period=2,
            status="ACTIVE",
            mts_created=created,
        )
        for value in (10, 11)
    )
    snapshot = AccountSnapshot(created + 1_000, D("0"), D("300"), offers=offers)
    assert not store.reconcile_ambiguous(snapshot)
    assert store.recovery_status()["manualRequired"]
    assert store.unresolved_intents()


def test_dashboard_restart_invalidates_previous_live_authorization(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    store = V4Store(settings.state_db)
    store.set_mode(RuntimeMode.LIVE)
    supervisor = V4Supervisor(settings)
    assert supervisor.store.mode() == RuntimeMode.PAUSED
    assert not supervisor.status_payload()["worker"]["liveRecoveryAuthorized"]


def test_runtime_incomplete_read_enters_read_only_recovery(tmp_path: Path) -> None:
    class Client:
        def account_snapshot(self, _currency: str) -> AccountSnapshot:
            raise BitfinexError(
                "IncompleteRead",
                category="NETWORK_TRANSPORT",
                retryable=True,
            )

    runtime = LendingRuntime(make_settings(tmp_path), client=Client())
    status = runtime.cycle(force_full=True)
    assert status.mode == RuntimeMode.SAFE
    recovery = runtime.store.recovery_status()
    assert recovery["active"] and not recovery["manualRequired"]
    assert recovery["targetMode"] == "SHADOW"
    assert runtime.last_account is None


class RecoveryClient:
    def __init__(self, now_ms: int, *, market_ok: bool = True) -> None:
        self.now_ms = now_ms
        self.market_ok = market_ok

    def account_snapshot(self, _currency: str) -> AccountSnapshot:
        return AccountSnapshot(self.now_ms, D("1000"), D("1000"))

    def funding_book(self, *_args) -> list[list[object]]:
        return [[0.00035, 2, 2, -5000], [0.00030, 2, 2, 5000]]

    def funding_trades(self, *_args, **_kwargs) -> list[list[object]]:
        if not self.market_ok:
            return []
        return [[value, self.now_ms - value * 60_000, 100, 0.00034, 2] for value in range(100)]

    def funding_offers_history(self, *_args, **_kwargs) -> list[list[object]]:
        return []

    def funding_trades_history(self, *_args, **_kwargs) -> list[list[object]]:
        return []


def make_probe_due(store: V4Store) -> None:
    with store.connect() as db:
        db.execute("UPDATE recovery_state SET next_probe_at_ms=0 WHERE singleton=1")


def test_runtime_two_clean_probes_restore_shadow_without_same_cycle_write(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    client = RecoveryClient(now)
    runtime = LendingRuntime(make_settings(tmp_path), client=client)
    runtime.store.enter_safe("network", category="NETWORK_TRANSPORT")
    make_probe_due(runtime.store)
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SAFE
    assert runtime.store.recovery_status()["successfulSnapshots"] == 1
    client.now_ms += 31_000
    make_probe_due(runtime.store)
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SHADOW
    assert runtime.last_action == "RECOVERED_NO_WRITE"
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SHADOW
    assert runtime.last_action == "RECOVERED_NO_WRITE"


def test_runtime_recovery_failure_resets_probe_and_auth_is_manual(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    client = RecoveryClient(now, market_ok=False)
    runtime = LendingRuntime(make_settings(tmp_path), client=client)
    runtime.store.enter_safe("network", category="NETWORK_TRANSPORT")
    make_probe_due(runtime.store)
    runtime.cycle(force_full=True)
    recovery = runtime.store.recovery_status()
    assert recovery["category"] == "MARKET_STALE"
    assert recovery["attempts"] == 1

    class AuthClient:
        def account_snapshot(self, _currency: str):
            raise BitfinexError("forbidden", category="AUTH_PERMISSION", manual_required=True)

    manual = LendingRuntime(make_settings(tmp_path / "manual"), client=AuthClient())
    manual.cycle(force_full=True)
    assert manual.store.recovery_status()["manualRequired"]


def test_runtime_account_mismatch_is_recoverable_and_read_only(tmp_path: Path) -> None:
    now = int(time.time() * 1000)

    class MismatchClient(RecoveryClient):
        def account_snapshot(self, _currency: str) -> AccountSnapshot:
            return AccountSnapshot(self.now_ms, D("10"), D("1000"))

    runtime = LendingRuntime(make_settings(tmp_path), client=MismatchClient(now))
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SAFE
    assert runtime.last_action == "RECOVERY_ACCOUNT_MISMATCH"
    assert runtime.store.recovery_status()["category"] == "ACCOUNT_MISMATCH"


def test_runtime_backoff_manual_and_unresolved_write_paths(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    runtime = LendingRuntime(make_settings(tmp_path / "backoff"), client=RecoveryClient(now))
    runtime.store.enter_safe("network", category="NETWORK_TRANSPORT")
    assert runtime.cycle().mode == RuntimeMode.SAFE
    assert runtime.last_action == "RECOVERY_BACKOFF"
    runtime.store.begin_recovery(
        "PROGRAM_ERROR",
        "bug",
        origin_mode="SHADOW",
        target_mode="SHADOW",
        manual_required=True,
    )
    runtime.cycle()
    assert runtime.last_action == "SAFE_MANUAL_REQUIRED"

    unresolved = LendingRuntime(make_settings(tmp_path / "unresolved"), client=RecoveryClient(now))
    assert unresolved.store.create_intent(
        "unknown",
        "SUBMIT",
        amount=D("150"),
        rate=D("0.0003"),
        period=2,
    )
    unresolved.store.set_intent_state("unknown", IntentState.AMBIGUOUS)
    make_probe_due(unresolved.store)
    unresolved.cycle(force_full=True)
    assert unresolved.last_action == "RECOVERY_RECONCILING_WRITE"


def test_runtime_probe_exceptions_split_retryable_and_manual(tmp_path: Path) -> None:
    now = int(time.time() * 1000)

    class BrokenHistory(RecoveryClient):
        error: BaseException = BitfinexError("timeout", category="NETWORK_TRANSPORT", retryable=True)

        def funding_offers_history(self, *_args, **_kwargs):
            raise self.error

    for name, error, expected in (
        ("retry", BitfinexError("timeout", category="NETWORK_TRANSPORT", retryable=True), False),
        ("manual", TypeError("bad parser"), True),
    ):
        client = BrokenHistory(now)
        client.error = error
        runtime = LendingRuntime(make_settings(tmp_path / name), client=client)
        assert runtime.store.create_intent(
            "unknown",
            "SUBMIT",
            amount=D("150"),
            rate=D("0.0003"),
            period=2,
        )
        runtime.store.set_intent_state("unknown", IntentState.AMBIGUOUS)
        runtime.store.enter_safe("unknown write", category="AMBIGUOUS_WRITE")
        make_probe_due(runtime.store)
        runtime.cycle(force_full=True)
        assert runtime.store.recovery_status()["manualRequired"] is expected


class FakeLock:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.acquired = 0
        self.released = 0

    def acquire(self) -> None:
        if self.error:
            raise self.error
        self.acquired += 1

    def release(self) -> None:
        self.released += 1


def prepare_runtime_run(runtime: LendingRuntime, monkeypatch) -> tuple[FakeLock, FakeLock]:
    worker = FakeLock()
    live = FakeLock()
    runtime.worker_lock = worker
    runtime.live_lock = live
    monkeypatch.setattr(runtime, "bootstrap_market", lambda: None)
    monkeypatch.setattr(runtime.market_stream, "start", lambda: None)
    monkeypatch.setattr(runtime.market_stream, "stop", lambda: None)
    monkeypatch.setattr("mika_v4.runtime.signal.signal", lambda *_args: None)
    return worker, live


def test_runtime_worker_loop_handles_program_error_as_manual(tmp_path: Path, monkeypatch) -> None:
    runtime = LendingRuntime(make_settings(tmp_path), client=RecoveryClient(int(time.time() * 1000)))
    worker, live = prepare_runtime_run(runtime, monkeypatch)
    monkeypatch.setattr(runtime, "cycle", lambda: (_ for _ in ()).throw(TypeError("bug")))
    runtime.run()
    assert runtime.last_action == "SAFE_MANUAL_REQUIRED"
    assert runtime.store.recovery_status()["manualRequired"]
    assert worker.acquired == worker.released == 1
    assert live.released == 1


def test_runtime_worker_lock_and_live_lock_fail_closed(tmp_path: Path, monkeypatch) -> None:
    rejected = LendingRuntime(make_settings(tmp_path / "worker"), client=RecoveryClient(int(time.time() * 1000)))
    rejected.worker_lock = FakeLock(LiveLockError("already running"))
    rejected.run()
    assert rejected.last_action == "WORKER_LOCK_REJECTED"

    live_runtime = LendingRuntime(make_settings(tmp_path / "live"), client=RecoveryClient(int(time.time() * 1000)))
    live_runtime.store.set_mode(RuntimeMode.LIVE)
    worker, _live = prepare_runtime_run(live_runtime, monkeypatch)
    live_runtime.live_lock = FakeLock(LiveLockError("other version is LIVE"))
    live_runtime.run()
    assert live_runtime.last_action == "SAFE_LIVE_LOCK"
    assert worker.released == 1
    assert live_runtime.store.recovery_status()["manualRequired"]


def test_runtime_worker_normal_stop_and_external_pid_request(tmp_path: Path, monkeypatch) -> None:
    runtime = LendingRuntime(make_settings(tmp_path), client=RecoveryClient(int(time.time() * 1000)))
    worker, _live = prepare_runtime_run(runtime, monkeypatch)

    def one_cycle():
        runtime.request_stop()

    monkeypatch.setattr(runtime, "cycle", one_cycle)
    runtime.run()
    assert worker.released == 1
    assert not runtime._external_stop_requested()
    runtime.stop_request_file.write_text('{"pid": -1}', encoding="utf-8")
    assert not runtime._external_stop_requested()
    runtime.stop_request_file.write_text("not-json", encoding="utf-8")
    assert not runtime._external_stop_requested()


class FakeProcess:
    def __init__(self, pid: int = 4321, returncode: int | None = None) -> None:
        self.pid = pid
        self.returncode = returncode
        self.terminated = False

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float) -> int:
        self.returncode = 0
        return 0

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.returncode = -9


def test_supervisor_starts_and_cooperatively_stops_exact_worker(tmp_path: Path, monkeypatch) -> None:
    supervisor = V4Supervisor(make_settings(tmp_path))
    fake = FakeProcess()
    monkeypatch.setattr("mika_v4.supervisor.subprocess.Popen", lambda *args, **kwargs: fake)
    supervisor.start_worker()
    assert supervisor.status_payload()["worker"]["pid"] == 4321
    supervisor.stop_worker()
    assert supervisor.process is None
    assert not supervisor.stop_request_file.exists()


def test_supervisor_watchdog_restarts_stale_worker(tmp_path: Path, monkeypatch) -> None:
    supervisor = V4Supervisor(make_settings(tmp_path))
    supervisor.process = FakeProcess()
    supervisor._started_at_ms = 1
    monkeypatch.setattr(supervisor, "_authorization_valid", lambda: True)
    calls = SimpleNamespace(stopped=0, started=0)

    def stopped(*, preserve_authorization: bool = False) -> None:
        assert preserve_authorization
        calls.stopped += 1
        supervisor.process = None

    def started() -> None:
        calls.started += 1

    monkeypatch.setattr(supervisor, "stop_worker", stopped)
    monkeypatch.setattr(supervisor, "start_worker", started)
    supervisor._watch_once(HEARTBEAT_TIMEOUT_MS + 2)
    assert (calls.stopped, calls.started) == (1, 0)
    assert supervisor.store.recovery_status()["category"] == "WORKER_HEARTBEAT"
    make_probe_due(supervisor.store)
    supervisor._watch_once(HEARTBEAT_TIMEOUT_MS + 32_000)
    assert calls.started == 1


def test_supervisor_build_change_and_unknown_exit_are_manual(tmp_path: Path, monkeypatch) -> None:
    changed = V4Supervisor(make_settings(tmp_path / "changed"))
    changed.process = FakeProcess()
    monkeypatch.setattr(changed, "_authorization_valid", lambda: False)
    changed._watch_once()
    assert changed.store.mode() == RuntimeMode.PAUSED
    assert changed.store.recovery_status()["manualRequired"]

    crashed = V4Supervisor(make_settings(tmp_path / "crashed"))
    crashed.process = FakeProcess(returncode=7)
    monkeypatch.setattr(crashed, "_authorization_valid", lambda: True)
    crashed._watch_once()
    recovery = crashed.store.recovery_status()
    assert recovery["category"] == "WORKER_UNEXPECTED_EXIT"
    assert recovery["manualRequired"]


def test_supervisor_restarts_only_authorized_recoverable_exit(tmp_path: Path, monkeypatch) -> None:
    supervisor = V4Supervisor(make_settings(tmp_path))
    supervisor.store.enter_safe("network", category="NETWORK_TRANSPORT")
    make_probe_due(supervisor.store)
    supervisor.process = FakeProcess(returncode=3)
    monkeypatch.setattr(supervisor, "_authorization_valid", lambda: True)
    restarted: list[bool] = []
    monkeypatch.setattr(supervisor, "start_worker", lambda: restarted.append(True))
    supervisor._watch_once()
    assert restarted == [True]


def test_supervisor_mode_changes_manage_session_authorization(tmp_path: Path, monkeypatch) -> None:
    supervisor = V4Supervisor(make_settings(tmp_path))
    calls: list[str] = []
    monkeypatch.setattr(
        supervisor.runtime,
        "enable_live",
        lambda confirmation, acquire_lock=False: calls.append(f"live:{confirmation}:{acquire_lock}"),
    )
    monkeypatch.setattr(supervisor.runtime, "disable_live", lambda mode: calls.append(f"disable:{mode.value}"))
    monkeypatch.setattr(supervisor, "stop_worker", lambda preserve_authorization=False: calls.append("stop"))
    monkeypatch.setattr(supervisor, "start_worker", lambda: calls.append("start"))
    supervisor.enable_live("ENABLE V4 LIVE")
    assert supervisor._live_authorized
    supervisor.disable_live(RuntimeMode.PAUSED)
    assert calls == ["stop", "live:ENABLE V4 LIVE:False", "start", "stop", "disable:PAUSED", "start"]
