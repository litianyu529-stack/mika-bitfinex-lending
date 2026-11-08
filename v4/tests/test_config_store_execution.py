from __future__ import annotations

import sqlite3
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from mika_v4.bitfinex import BitfinexClient, SlidingWindowLimiter, submitted_offer_id
from mika_v4.config import (
    ConfigError,
    V4Policy,
    load_settings,
    migrate_v3_config,
    update_editable_policy,
    validate_policy,
)
from mika_v4.domain import (
    AccountSnapshot,
    AllocationPlan,
    IntentState,
    OfferSnapshot,
    PlannedOffer,
    PlannerState,
    RuntimeMode,
    WriteOutcome,
    WriteResult,
)
from mika_v4.execution import ExecutionBlocked, SafeExecutor
from mika_v4.locks import CrossVersionLiveLock, LiveLockError
from mika_v4.migration import MigrationBlocked, import_v3_history
from mika_v4.store import V4Store, plan_from_json, plan_to_json


D = Decimal


class FakeClient:
    def __init__(self, submit_outcome: WriteOutcome = WriteOutcome.CONFIRMED) -> None:
        self.submit_outcome = submit_outcome
        self.submits: list[tuple[D, D, int]] = []
        self.cancels: list[int] = []
        self.next_id = 100

    def submit_offer(self, amount: D, rate: D, period: int) -> WriteResult:
        self.submits.append((amount, rate, period))
        if self.submit_outcome == WriteOutcome.UNKNOWN:
            return WriteResult(WriteOutcome.UNKNOWN, error="timeout")
        if self.submit_outcome == WriteOutcome.DEFINITE_REJECT:
            return WriteResult(WriteOutcome.DEFINITE_REJECT, error="rejected")
        self.next_id += 1
        return WriteResult(WriteOutcome.CONFIRMED, response=[0, 0, 0, 0, [self.next_id], 0, "SUCCESS", "ok"])

    def cancel_offer(self, offer_id: int) -> WriteResult:
        self.cancels.append(offer_id)
        return WriteResult(WriteOutcome.CONFIRMED, response=[0, 0, 0, 0, [offer_id], 0, "SUCCESS", "ok"])


def offer(offer_id: int, amount: str, rate: str, period: int, mts: int = 1_000) -> OfferSnapshot:
    return OfferSnapshot(
        offer_id=offer_id,
        currency="USD",
        amount=D(amount),
        amount_original=D(amount),
        rate=D(rate),
        period=period,
        status="ACTIVE",
        mts_created=mts,
    )


def account(mts: int = 1_000, available: str = "1000", offers: tuple[OfferSnapshot, ...] = ()) -> AccountSnapshot:
    return AccountSnapshot(mts, D(available), D("1000"), offers=offers, authoritative=True)


def plan(mts: int = 1_000, short_rate: str = "0.0003", medium: bool = False) -> AllocationPlan:
    orders = [PlannedOffer(f"s{mts}:short:0", "short", 0, D("300"), D(short_rate), 2)]
    if medium:
        orders.append(PlannedOffer(f"m{mts}:medium:0", "medium", 0, D("300"), D("0.00035"), 14))
    return AllocationPlan(
        as_of_ms=mts,
        anchor=D(short_rate),
        step=D("0.00001"),
        deployable=sum((item.amount for item in orders), D("0")),
        planned_amount=sum((item.amount for item in orders), D("0")),
        idle_amount=D("0"),
        long_tier=0,
        orders=tuple(orders),
        state=PlannerState(),
    )


def test_config_load_update_and_validation(tmp_path: Path) -> None:
    target = tmp_path / "default.cfg"
    target.write_text(
        "[BITFINEX]\napikey=key\nsecret=secret\n[BOT_V4]\nstate_db=.state/test.sqlite3\n"
        "[STRATEGY_V4]\nshort_floor_apr=7\nmedium_floor_apr=8\nlong_floor_apr=10\n",
        encoding="utf-8",
    )
    settings = load_settings(target)
    assert settings.policy.short_floor_apr_percent == 7
    candidate = update_editable_policy(settings, {"short_periods": "2,7", "max_lend_amount": "500"})
    assert candidate.short_periods == (2, 7)
    assert candidate.max_lend_amount == 500
    with pytest.raises(ConfigError):
        update_editable_policy(settings, {"normal_fee_percent": "99"})
    with pytest.raises(ConfigError):
        validate_policy(replace(V4Policy(), currency="USDT"))
    with pytest.raises(ConfigError):
        validate_policy(replace(V4Policy(), max_authenticated_requests_per_minute=46))


def test_v3_config_migration_omits_unsupported_modes(tmp_path: Path) -> None:
    source = tmp_path / "v3.cfg"
    source.write_text(
        "[STRATEGY_V3]\nshort_floor_apr=7.1\nshort_share=60\nmedium_share=20\nfrr_enabled=true\nhidden=true\n",
        encoding="utf-8",
    )
    target = migrate_v3_config(source, tmp_path / "v4.cfg")
    text = target.read_text(encoding="utf-8")
    assert "short_floor_apr = 7.1" in text
    assert "frr_enabled" not in text.lower()
    assert "hidden =" not in text.lower()


def test_plan_json_round_trip() -> None:
    original = plan()
    assert plan_from_json(plan_to_json(original)) == original


def test_shadow_records_without_exchange_writes(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    client = FakeClient()
    executor = SafeExecutor(client, store, V4Policy())
    assert executor.reconcile(plan(), account()) == "SHADOW_RECORDED"
    assert not client.submits and not client.cancels
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM shadow_plans").fetchone()[0] == 1


def test_live_submit_then_two_phase_specific_cancel_and_rebuild(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    client = FakeClient()
    executor = SafeExecutor(client, store, V4Policy())
    first = plan(medium=True)
    assert executor.reconcile(first, account(available="600")) == "SUBMITTED"
    active = store.active_rungs()
    ids = {row["pool"]: int(row["offer_id"]) for row in active}
    open_offers = (
        offer(ids["short"], "300", "0.0003", 2),
        offer(ids["medium"], "300", "0.00035", 14),
        offer(999, "200", "0.0004", 30),
    )
    executor.sync_account(account(2_000, "0", open_offers))
    changed = plan(2_000, "0.00031", medium=True)
    assert executor.reconcile(changed, account(2_000, "0", open_offers)) == "CANCELS_SUBMITTED"
    assert client.cancels == [ids["short"]]
    assert 999 not in client.cancels

    remaining = (offer(ids["medium"], "300", "0.00035", 14), offer(999, "200", "0.0004", 30))
    after_cancel = account(63_000, "300", remaining)
    executor.sync_account(after_cancel)
    assert executor.reconcile(changed, after_cancel) == "SUBMITTED"
    assert client.submits[-1] == (D("300"), D("0.00031"), 2)
    assert ids["medium"] in store.managed_offer_ids()


def test_unknown_submit_enters_safe_and_does_not_retry(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    client = FakeClient(WriteOutcome.UNKNOWN)
    executor = SafeExecutor(client, store, V4Policy())
    assert executor.reconcile(plan(), account()) == "SAFE_UNKNOWN_SUBMIT"
    assert store.mode() == RuntimeMode.SAFE
    assert len(client.submits) == 1
    assert executor.reconcile(plan(), account()) == "SAFE"
    assert len(client.submits) == 1


def test_definite_reject_marks_rung_rejected_without_ambiguity(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    client = FakeClient(WriteOutcome.DEFINITE_REJECT)
    result = SafeExecutor(client, store, V4Policy()).reconcile(plan(), account())
    assert result == "SUBMITTED"
    assert store.mode() == RuntimeMode.LIVE
    assert not store.active_rungs()
    assert not store.unresolved_intents()


def test_ambiguous_submit_needs_two_spaced_authoritative_snapshots(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    current = plan()
    store.save_plan_rungs(current, "TEST")
    fingerprint = "ambiguous"
    key = current.orders[0].key
    assert store.create_intent(
        fingerprint,
        "SUBMIT",
        offer_key=key,
        amount=D("300"),
        rate=D("0.0003"),
        period=2,
    )
    store.set_intent_state(fingerprint, IntentState.AMBIGUOUS)
    store.enter_safe("unknown submit")
    now = int(time.time() * 1000) + 1_000
    first = account(now, "700", (offer(777, "300", "0.0003", 2, now),))
    second = account(now + 31_000, "700", (offer(777, "300", "0.0003", 2, now),))
    store.reconcile_ambiguous(first)
    assert not store.record_consistent_snapshot(first.as_of_ms)
    assert store.unresolved_intents()
    store.reconcile_ambiguous(second)
    assert not store.record_consistent_snapshot(second.as_of_ms)
    assert not store.record_consistent_snapshot(second.as_of_ms + 31_000)
    assert store.record_consistent_snapshot(second.as_of_ms + 62_000)
    assert store.mode() == RuntimeMode.SHADOW
    assert store.managed_offer_ids() == {777}


def test_non_authoritative_snapshot_blocks_execution(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    snapshot = replace(account(), authoritative=False)
    with pytest.raises(ExecutionBlocked):
        SafeExecutor(FakeClient(), store, V4Policy()).reconcile(plan(), snapshot)


def test_external_offer_adoption_is_explicit_and_persisted(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    external = offer(9, "150", "0.0003", 2)
    assert store.adopt_offers([external], 1_000) == 1
    assert store.managed_offer_ids() == {9}
    assert store.adopt_offers([external], 2_000) == 0


def test_cross_version_lock_is_mutually_exclusive(tmp_path: Path) -> None:
    first = CrossVersionLiveLock(tmp_path, "v3-test")
    second = CrossVersionLiveLock(tmp_path, "v4-test")
    first.acquire()
    try:
        with pytest.raises(LiveLockError):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_submitted_offer_id_and_rate_limiter() -> None:
    assert submitted_offer_id([0, 0, 0, 0, [123], 0, "SUCCESS", "ok"]) == 123
    assert submitted_offer_id([0]) is None
    limiter = SlidingWindowLimiter(2, 0.01)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()


def test_bitfinex_write_response_classification() -> None:
    confirmed = BitfinexClient._write_result(lambda: [0, 0, 0, 0, [1], 0, "SUCCESS", "ok"])
    rejected = BitfinexClient._write_result(lambda: [0, 0, 0, 0, None, 0, "ERROR", "bad"])
    unknown = BitfinexClient._write_result(lambda: {"unexpected": True})
    assert confirmed.outcome == WriteOutcome.CONFIRMED
    assert rejected.outcome == WriteOutcome.DEFINITE_REJECT
    assert unknown.outcome == WriteOutcome.UNKNOWN


def make_v3_db(path: Path, mode: str = "SHADOW", unresolved: bool = False) -> None:
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE runtime_state(singleton INTEGER PRIMARY KEY,mode TEXT,safe_reason TEXT);
        CREATE TABLE order_intents(state TEXT);
        CREATE TABLE market_trades(trade_id TEXT,mts INTEGER,amount TEXT,rate TEXT,period INTEGER);
        CREATE TABLE funding_stats(mts INTEGER,payload_json TEXT);
        CREATE TABLE book_snapshots(mts INTEGER,book_json TEXT);
        CREATE TABLE account_samples(
            mts INTEGER,total_principal TEXT,wallet_available TEXT,open_offers TEXT,active_credits TEXT
        );
    """)
    db.execute("INSERT INTO runtime_state VALUES(1,?,NULL)", (mode,))
    if unresolved:
        db.execute("INSERT INTO order_intents VALUES('SUBMITTING')")
    db.execute("INSERT INTO market_trades VALUES('1',1,'10','0.0003',2)")
    db.execute("INSERT INTO funding_stats VALUES(1,'{}')")
    db.execute(
        "INSERT INTO book_snapshots VALUES(1,?)",
        ('[{"rate":"0.0003","period":2,"count":1,"amount":"-100"}]',),
    )
    db.execute("INSERT INTO account_samples VALUES(1,'1000','100','200','700')")
    db.commit()
    db.close()


def test_v3_history_import_is_read_only_and_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "v3.sqlite3"
    make_v3_db(source)
    before = source.read_bytes()
    target = V4Store(tmp_path / "v4.sqlite3")
    counts = import_v3_history(source, target)
    assert counts == {"market_trades": 1, "funding_stats": 1, "book_snapshots": 1, "account_samples": 1}
    assert import_v3_history(source, target) == {key: 0 for key in counts}
    assert source.read_bytes() == before


@pytest.mark.parametrize(("mode", "unresolved"), [("SAFE", False), ("SHADOW", True)])
def test_v3_history_import_blocks_unsafe_source(tmp_path: Path, mode: str, unresolved: bool) -> None:
    source = tmp_path / "v3.sqlite3"
    make_v3_db(source, mode, unresolved)
    with pytest.raises(MigrationBlocked):
        import_v3_history(source, V4Store(tmp_path / "v4.sqlite3"))
