from __future__ import annotations

import asyncio
import json
import sys
import time
import types
import urllib.request
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from mika_v4.bitfinex import BitfinexClient, BitfinexError
from mika_v4.config import ConfigError, V4Policy, V4Settings, load_settings, validate_policy
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
from mika_v4.execution import SafeExecutor
from mika_v4.market import MarketBuffer, PublicMarketStream
from mika_v4.runtime import LendingRuntime
from mika_v4.store import V4Store
from mika_v4.strategy import gross_daily_floor


D = Decimal


def make_plan(rate: str = "0.0003", amount: str = "300", orders: bool = True) -> AllocationPlan:
    planned = (PlannedOffer(f"key-{rate}-{amount}", "short", 0, D(amount), D(rate), 2),) if orders else ()
    return AllocationPlan(
        1_000,
        D(rate),
        D("0.00001"),
        D(amount),
        sum((item.amount for item in planned), D("0")),
        D("0") if planned else D(amount),
        0,
        planned,
        PlannerState(),
    )


def snapshot(available: str = "300", offers: tuple[OfferSnapshot, ...] = (), mts: int = 1_000) -> AccountSnapshot:
    return AccountSnapshot(mts, D(available), D("1000"), offers=offers, authoritative=True)


def active_offer(offer_id: int = 10, rate: str = "0.0003", amount: str = "300") -> OfferSnapshot:
    return OfferSnapshot(offer_id, "USD", D(amount), D("300"), D(rate), 2, "ACTIVE", 1_000)


class BranchClient:
    def __init__(
        self,
        *,
        cancel: WriteOutcome = WriteOutcome.CONFIRMED,
        submit: WriteOutcome = WriteOutcome.CONFIRMED,
        missing_id: bool = False,
    ) -> None:
        self.cancel_outcome = cancel
        self.submit_outcome = submit
        self.missing_id = missing_id
        self.cancels: list[int] = []
        self.submits = 0

    def cancel_offer(self, offer_id: int) -> WriteResult:
        self.cancels.append(offer_id)
        return WriteResult(self.cancel_outcome, error="cancel error")

    def submit_offer(self, *_: object) -> WriteResult:
        self.submits += 1
        response = [0, 0, 0, 0, [] if self.missing_id else [99], 0, "SUCCESS", "ok"]
        return WriteResult(self.submit_outcome, response=response, error="submit error")


def seed_open(store: V4Store, plan: AllocationPlan, offer_id: int = 10) -> None:
    store.save_plan_rungs(plan, "SEED")
    store.update_rung_offer(plan.orders[0].key, offer_id)


def test_executor_unchanged_waiting_balance_and_empty_branches(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    original = make_plan()
    seed_open(store, original)
    client = BranchClient()
    executor = SafeExecutor(client, store, V4Policy())
    current = snapshot("0", (active_offer(),))
    executor.sync_account(current)
    assert executor.reconcile(original, current) == "UNCHANGED"

    changed = make_plan("0.00031")
    assert executor.reconcile(changed, current) == "CANCELS_SUBMITTED"
    assert executor.reconcile(changed, current) == "WAITING_FOR_CANCEL_CONFIRMATION"

    other = V4Store(tmp_path / "other.sqlite3")
    other.set_mode(RuntimeMode.LIVE)
    assert SafeExecutor(client, other, V4Policy()).reconcile(make_plan(amount="500"), snapshot("300")) == (
        "BALANCE_CHANGED_REPLAN_REQUIRED"
    )
    assert SafeExecutor(client, other, V4Policy()).reconcile(make_plan(orders=False), snapshot()) == "NO_ORDERS"


@pytest.mark.parametrize(
    ("outcome", "expected", "mode"),
    [
        (WriteOutcome.UNKNOWN, "SAFE_UNKNOWN_CANCEL", RuntimeMode.SAFE),
        (WriteOutcome.DEFINITE_REJECT, "CANCEL_REJECTED", RuntimeMode.LIVE),
    ],
)
def test_executor_cancel_failure_classification(
    tmp_path: Path, outcome: WriteOutcome, expected: str, mode: RuntimeMode
) -> None:
    store = V4Store(tmp_path / f"{outcome}.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    original = make_plan()
    seed_open(store, original)
    client = BranchClient(cancel=outcome)
    current = snapshot("0", (active_offer(),))
    result = SafeExecutor(client, store, V4Policy()).reconcile(make_plan("0.00031"), current)
    assert result == expected
    assert store.mode() == mode


def test_executor_missing_submit_id_and_rebuild_cap(tmp_path: Path) -> None:
    missing_store = V4Store(tmp_path / "missing.sqlite3")
    missing_store.set_mode(RuntimeMode.LIVE)
    result = SafeExecutor(BranchClient(missing_id=True), missing_store, V4Policy()).reconcile(make_plan(), snapshot())
    assert result == "SAFE_MISSING_OFFER_ID"

    capped = V4Store(tmp_path / "capped.sqlite3")
    capped.set_mode(RuntimeMode.LIVE)
    original = make_plan()
    seed_open(capped, original)
    now = int(time.time() * 1000)
    with capped.connect() as db:
        for _ in range(6):
            db.execute("INSERT INTO rebuild_events VALUES('short',?,'TEST')", (now,))
    account = snapshot("0", (active_offer(),), now)
    assert SafeExecutor(BranchClient(), capped, V4Policy()).reconcile(make_plan("0.00031"), account) == (
        "REBUILD_RATE_LIMIT"
    )


def test_store_cancel_ambiguity_and_floor_persistence(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "state.sqlite3")
    plan = make_plan()
    seed_open(store, plan)
    assert store.create_intent("cancel-x", "CANCEL", offer_id=10)
    store.set_intent_state("cancel-x", IntentState.AMBIGUOUS)
    first = snapshot("300", (), 10_000)
    second = snapshot("300", (), 41_000)
    store.reconcile_ambiguous(first)
    assert store.unresolved_intents()
    store.reconcile_ambiguous(second)
    assert not store.unresolved_intents()
    store.mark_floor_reached(plan.orders[0].key, 50_000)
    assert store.active_rungs()[0]["floor_reached_at_ms"] == 50_000
    assert store.create_intent("unsent", "SUBMIT")
    assert store.close_unsent_planned_intents() == 1
    assert not store.unresolved_intents()


def test_market_buffer_update_replace_and_stream_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    current = int(time.time() * 1000)
    buffer = MarketBuffer(retention_minutes=1)
    buffer.replace_book([{"rate": D("0.1"), "period": 2, "count": 1, "amount": D("-1")}], current)
    buffer.update_book({"rate": D("0.2"), "period": 2, "count": 1, "amount": D("-2")}, current + 1)
    buffer.update_book({"rate": D("0.1"), "period": 2, "count": 0, "amount": D("-1")}, current + 2)
    buffer.add_trade({"id": "old", "mts": current - 70_000, "amount": D("1"), "rate": D("0.1"), "period": 2})
    buffer.add_trade({"id": "new", "mts": current, "amount": D("1"), "rate": D("0.2"), "period": 2})
    buffer.add_trade({"id": "new", "mts": current, "amount": D("1"), "rate": D("9"), "period": 2})
    book, trades, updated = buffer.snapshot()
    assert len(book) == 1 and [item["id"] for item in trades] == ["new"] and updated >= current

    stream = PublicMarketStream(buffer)
    messages = [
        {"event": "subscribed", "chanId": 1, "channel": "book"},
        {"event": "subscribed", "chanId": 2, "channel": "trades"},
        [1, [[0.0003, 2, 1, -100]]],
        [1, [0.00031, 2, 1, -50]],
        [1, "hb"],
        [2, "te", [7, current + 3, 10, 0.0003, 2]],
    ]

    class Socket:
        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            message = messages.pop(0)
            if not messages:
                stream._stop.set()
            return json.dumps(message)

    class Connect:
        async def __aenter__(self) -> Socket:
            return Socket()

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=lambda *_args, **_kwargs: Connect()))
    asyncio.run(stream._run())
    book, trades, _ = buffer.snapshot()
    assert len(book) == 2 and trades[-1]["id"] == "7"


def make_settings(tmp_path: Path) -> V4Settings:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = tmp_path / "default.cfg"
    config.write_text("[BITFINEX]\n[BOT_V4]\n[STRATEGY_V4]\n", encoding="utf-8")
    return V4Settings("key", "secret", V4Policy(), tmp_path / "db.sqlite3", tmp_path / "status.json", config, tmp_path)


class LoopClient:
    def __init__(self, now: int, fail_account: bool = False) -> None:
        self.now = now
        self.fail_account = fail_account

    def funding_book(self, *_: object):
        return [[0.00035, 2, 1, -5000]]

    def funding_trades(self, *_: object, **__: object):
        return [[1, self.now, 1000, 0.00034, 2]]

    def account_snapshot(self, *_: object):
        if self.fail_account:
            raise BitfinexError("offline")
        return AccountSnapshot(int(time.time() * 1000), D("500"), D("500"), authoritative=True)


def test_runtime_account_failure_run_loop_modes_and_live_lock(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    runtime = LendingRuntime(make_settings(tmp_path), client=LoopClient(now, fail_account=True))
    runtime.bootstrap_market()
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SAFE

    loop = LendingRuntime(make_settings(tmp_path / "loop"), client=LoopClient(now))
    loop.settings.config_file.parent.mkdir(parents=True, exist_ok=True)
    loop.market_stream = types.SimpleNamespace(start=lambda: None, stop=lambda: None)
    original_cycle = loop.cycle

    def one_cycle(*args: object, **kwargs: object):
        result = original_cycle(*args, **kwargs)
        loop.request_stop()
        return result

    loop.cycle = one_cycle  # type: ignore[method-assign]
    loop.run()

    modes = LendingRuntime(make_settings(tmp_path / "modes"), client=LoopClient(now))
    modes.settings.config_file.parent.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError):
        modes.enable_live("wrong")
    shadow_plan = make_plan()
    modes.store.record_shadow_plan(replace(shadow_plan, as_of_ms=now - 7 * 86_400_000))
    modes.store.record_shadow_plan(replace(shadow_plan, as_of_ms=now))
    modes.store.record_validation_report(True, "{}", now)
    modes.enable_live("ENABLE V4 LIVE")
    assert modes.store.mode() == RuntimeMode.LIVE and modes.live_lock.held
    modes.disable_live(RuntimeMode.PAUSED)
    assert modes.store.mode() == RuntimeMode.PAUSED and not modes.live_lock.held


@pytest.mark.parametrize(
    "policy",
    [
        replace(V4Policy(), short_weight=D("0")),
        replace(V4Policy(), short_floor_apr_percent=D("0")),
        replace(V4Policy(), short_periods=(1,)),
        replace(V4Policy(), medium_periods=(31,)),
        replace(V4Policy(), long_period=30),
        replace(V4Policy(), short_max_rungs=0),
        replace(V4Policy(), grid_min_step_percent=D("0.1"), grid_max_step_percent=D("0.01")),
        replace(V4Policy(), partial_fill_trigger_percent=D("0")),
        replace(V4Policy(), idle_merge_trigger=D("150")),
        replace(V4Policy(), max_group_rebuilds_per_hour=13),
        replace(V4Policy(), normal_fee_percent=D("100")),
        replace(V4Policy(), max_lend_amount=D("-1")),
        replace(V4Policy(), max_lend_percent=D("101")),
        replace(V4Policy(), fast_sync_seconds=1),
        replace(V4Policy(), full_replan_seconds=1),
    ],
)
def test_policy_invalid_boundaries(policy: V4Policy) -> None:
    with pytest.raises(ConfigError):
        validate_policy(policy)


def test_load_settings_missing_file_and_environment_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ConfigError):
        load_settings(tmp_path / "missing.cfg")
    config = tmp_path / "default.cfg"
    config.write_text("[BITFINEX]\napikey=file\nsecret=file\n", encoding="utf-8")
    monkeypatch.setenv("BITFINEX_API_KEY", "environment")
    monkeypatch.setenv("BITFINEX_API_SECRET", "secret")
    assert load_settings(config).api_key == "environment"


class Response:
    def __init__(self, payload: object) -> None:
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def test_bitfinex_public_auth_and_parsers() -> None:
    requests: list[urllib.request.Request] = []

    def opener(request: urllib.request.Request, timeout: float):
        requests.append(request)
        url = request.full_url
        if url.endswith("wallets"):
            return Response([["funding", "USD", 1000, 0, 100]])
        if url.endswith("offers"):
            row = [0] * 21
            row[0], row[1], row[2], row[4], row[5] = 4, "fUSD", 1, 150, 300
            row[6], row[9], row[10], row[14], row[15] = "LIMIT", 0, "PARTIALLY FILLED", 0.0003, 2
            return Response([row])
        if url.endswith("credits") or url.endswith("loans"):
            row = [0] * 20
            row[0], row[1], row[3], row[5], row[11], row[12] = 5, "fUSD", 1, 200, 0.0003, 2
            return Response([row])
        if "/offer/submit" in url:
            return Response([0, 0, 0, 0, [77], 0, "SUCCESS", "ok"])
        if "/offer/cancel" in url:
            return Response([0, 0, 0, 0, [77], 0, "SUCCESS", "ok"])
        return Response([])

    client = BitfinexClient("key", "secret", opener=opener)
    assert client.public("book/fUSD/P0") == []
    account = client.account_snapshot()
    assert account.offers[0].fill_fraction == D("0.5")
    assert account.credits[0].funding_state == "credit"
    assert not account.loans  # Duplicate lender-side rows are counted only once.
    assert client.funding_offers_history(start=1, end=2) == []
    assert client.funding_trades_history(start=1, end=2) == []
    assert client.submit_offer(D("150"), D("0.0003"), 2).outcome == WriteOutcome.CONFIRMED
    assert client.cancel_offer(77).outcome == WriteOutcome.CONFIRMED
    assert requests[-1].headers["Bfx-apikey"] == "key"


def test_bitfinex_auth_requires_credentials_and_write_errors() -> None:
    with pytest.raises(BitfinexError, match="credentials"):
        BitfinexClient().auth("v2/auth/r/wallets")

    def unknown():
        raise BitfinexError("authenticated request outcome unknown: timeout")

    def rejected():
        raise BitfinexError("authenticated request rejected (400): bad")

    assert BitfinexClient._write_result(unknown).outcome == WriteOutcome.UNKNOWN
    assert BitfinexClient._write_result(rejected).outcome == WriteOutcome.DEFINITE_REJECT


def test_floor_helpers_are_available_for_all_pools() -> None:
    policy = V4Policy()
    assert all(
        gross_daily_floor(policy.floor_apr_percent(pool), policy.normal_fee_percent) > 0
        for pool in ("short", "medium", "long")
    )
