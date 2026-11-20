from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from mika_v4.bitfinex import BitfinexClient, BitfinexError
from mika_v4.config import V4Policy, V4Settings
from mika_v4.dashboard import DashboardServer
from mika_v4.domain import AccountSnapshot, OfferSnapshot, RuntimeMode, WriteOutcome, WriteResult
from mika_v4.history import HistoricalCollector
from mika_v4.runtime import LendingRuntime, parse_book, parse_trades
from mika_v4.store import V4Store
from mika_v4.strategy import build_plan
from mika_v4.validation import (
    DAY_MS,
    EvidenceInsufficient,
    chronological_boundaries,
    load_real_evidence,
    paired_bootstrap_lower,
    shadow_audit,
    validate_90_days,
)


D = Decimal


def settings(tmp_path: Path, policy: V4Policy | None = None) -> V4Settings:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = tmp_path / "default.cfg"
    config.write_text("[BITFINEX]\n[BOT_V4]\n[STRATEGY_V4]\n", encoding="utf-8")
    return V4Settings(
        api_key="key",
        api_secret="secret",
        policy=policy or V4Policy(),
        state_db=tmp_path / "v4.sqlite3",
        status_file=tmp_path / "status.json",
        config_file=config,
        repository_root=tmp_path,
    )


def market_rows(now: int) -> tuple[list[list[object]], list[list[object]]]:
    book = [
        [0.00035, 2, 2, -5000],
        [0.00034, 14, 2, -5000],
        [0.00030, 2, 2, 5000],
    ]
    trades = [[index, now - index * 60_000, 100, 0.00034, 2 if index % 2 else 14] for index in range(100)]
    return book, trades


class RuntimeClient:
    def __init__(self, snapshot: AccountSnapshot, now: int) -> None:
        self.snapshot = snapshot
        self.book, self.trades = market_rows(now)
        self.submits: list[tuple[D, D, int]] = []
        self.cancels: list[int] = []

    def account_snapshot(self, _currency: str) -> AccountSnapshot:
        return replace(self.snapshot, as_of_ms=int(time.time() * 1000))

    def funding_book(self, _symbol: str, _length: int) -> list[list[object]]:
        return self.book

    def funding_trades(self, _symbol: str, _start: int, _limit: int, sort: int = -1) -> list[list[object]]:
        return self.trades

    def submit_offer(self, amount: D, rate: D, period: int) -> WriteResult:
        self.submits.append((amount, rate, period))
        offer_id = 100 + len(self.submits)
        return WriteResult(WriteOutcome.CONFIRMED, [0, 0, 0, 0, [offer_id], 0, "SUCCESS", "ok"])

    def cancel_offer(self, offer_id: int) -> WriteResult:
        self.cancels.append(offer_id)
        return WriteResult(WriteOutcome.CONFIRMED, [0, 0, 0, 0, [offer_id], 0, "SUCCESS", "ok"])


def test_parse_public_rows() -> None:
    assert parse_book([["0.1", 2, 1, "-5"], [1]]) == [{"rate": D("0.1"), "period": 2, "count": 1, "amount": D("-5")}]
    assert parse_trades([[1, 2, "3", "0.1", 4], [1]]) == [
        {"id": "1", "mts": 2, "amount": D("3"), "rate": D("0.1"), "period": 4}
    ]


def test_shadow_runtime_cycle_writes_status_not_exchange(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    snapshot = AccountSnapshot(now, D("1000"), D("1000"), authoritative=True)
    client = RuntimeClient(snapshot, now)
    runtime = LendingRuntime(settings(tmp_path), client=client)
    runtime.bootstrap_market()
    status = runtime.cycle(force_full=True)
    assert status.mode == RuntimeMode.SHADOW
    assert runtime.last_action == "SHADOW_RECORDED"
    assert runtime.last_plan and runtime.last_plan.orders
    assert not client.submits
    assert json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))["strategy"] == "V4"


def test_live_runtime_stale_market_enters_safe(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    snapshot = AccountSnapshot(now, D("1000"), D("1000"), authoritative=True)
    client = RuntimeClient(snapshot, now)
    client.trades = []
    client.book = [[0.0003, 2, 1, -1000]]
    store = V4Store(tmp_path / "v4.sqlite3")
    store.set_mode(RuntimeMode.LIVE)
    runtime = LendingRuntime(settings(tmp_path), client=client, store=store)
    runtime.bootstrap_market()
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SAFE
    assert "market data" in (store.safe_reason() or "")

    shadow_store = V4Store(tmp_path / "shadow.sqlite3")
    shadow_runtime = LendingRuntime(settings(tmp_path / "shadow"), client=client, store=shadow_store)
    shadow_runtime.bootstrap_market()
    assert shadow_runtime.cycle(force_full=True).mode == RuntimeMode.SAFE


def test_runtime_unknown_available_balance_enters_safe_before_reconciliation(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    snapshot = AccountSnapshot(now, D("0"), D("1000"), authoritative=False)
    runtime = LendingRuntime(settings(tmp_path), client=RuntimeClient(snapshot, now))
    assert runtime.cycle(force_full=True).mode == RuntimeMode.SAFE
    assert runtime.last_action == "RECOVERY_ACCOUNT_UNKNOWN"


def test_runtime_detects_small_idle_preferred_pool(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    snapshot = AccountSnapshot(now, D("10"), D("1000"), authoritative=True)
    runtime = LendingRuntime(settings(tmp_path), client=RuntimeClient(snapshot, now))
    base = build_plan(V4Policy(), replace_market(now), D("300"), D("1000"), target_pool="short")
    runtime.store.save_plan_rungs(base, "TEST")
    runtime.store.update_rung_offer(base.orders[0].key, 7)
    assert runtime._small_idle_target(snapshot) == "short"


def replace_market(now: int):
    book, trades = market_rows(now)
    from mika_v4.market import build_market_snapshot

    return build_market_snapshot(parse_book(book), parse_trades(trades), V4Policy(), now, now)


def test_runtime_adoption_requires_pause_config_and_per_offer_confirmation(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    external = OfferSnapshot(44, "USD", D("150"), D("150"), D("0.0003"), 2, "ACTIVE", now)
    snapshot = AccountSnapshot(now, D("0"), D("150"), offers=(external,), authoritative=True)
    policy = replace(V4Policy(), adopt_external_offers=True)
    runtime = LendingRuntime(settings(tmp_path, policy), client=RuntimeClient(snapshot, now))
    runtime.last_account = snapshot
    runtime.store.set_mode(RuntimeMode.PAUSED)
    with pytest.raises(ValueError, match="逐笔确认"):
        runtime.adopt_external([44], {})
    assert runtime.adopt_external([44], {"44": "ADOPT 44"}) == 1


def call_json(url: str, method: str = "GET", body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_dashboard_status_mode_and_config_api(tmp_path: Path) -> None:
    now = int(time.time() * 1000)
    snapshot = AccountSnapshot(now, D("1000"), D("1000"), authoritative=True)
    runtime = LendingRuntime(settings(tmp_path), client=RuntimeClient(snapshot, now))
    server = DashboardServer(runtime, port=0)
    server.start()
    base = f"http://127.0.0.1:{server.port}"
    try:
        code, status = call_json(f"{base}/api/status")
        assert code == 200 and status["version"] == "0.4.0"
        code, result = call_json(f"{base}/api/mode", "POST", {"mode": "PAUSED"})
        assert code == 200 and result["mode"] == "PAUSED"
        code, result = call_json(f"{base}/api/config", "POST", {"short_floor_apr_percent": "7.2"})
        assert code == 200 and result["restart_required"]
        code, error = call_json(f"{base}/api/mode", "POST", {"mode": "LIVE", "confirmation": "wrong"})
        assert code == 400 and "ENABLE V4 LIVE" in error["error"]
        with urllib.request.urlopen(f"{base}/", timeout=5) as response:
            assert b"V4" in response.read()
    finally:
        server.close()


class HistoryClient:
    def __init__(self) -> None:
        self.trade_calls = 0
        self.stats_calls = 0

    def funding_trades(self, *_: object, **__: object) -> list[list[object]]:
        self.trade_calls += 1
        return [[1, 1_000, 10, 0.0003, 2]] if self.trade_calls == 1 else []

    def funding_stats(self, *_: object) -> list[list[object]]:
        self.stats_calls += 1
        return [[1_000, 0.1]] if self.stats_calls == 1 else []

    def funding_book(self, *_: object) -> list[list[object]]:
        return [[0.0003, 2, 1, -100]]


def test_history_collector_and_real_evidence_loader(tmp_path: Path) -> None:
    store = V4Store(tmp_path / "v4.sqlite3")
    collector = HistoricalCollector(HistoryClient(), store)
    counts = collector.backfill(90, now_ms=90 * DAY_MS)
    assert counts == {"market_trades": 1, "funding_stats": 1}
    assert collector.capture_real_book(2_000) == 1
    trades, books = load_real_evidence(store)
    assert len(trades) == 1 and books[0][0] == 2_000


def test_validation_uses_chronological_real_windows() -> None:
    start = 1_000_000
    points = [start, start + 60 * DAY_MS, start + 75 * DAY_MS, start + 90 * DAY_MS - 60_000]
    trades = [
        {"id": index, "mts": mts, "amount": D("1000"), "rate": D("0.0004"), "period": 2}
        for index, mts in enumerate(points)
    ]
    books = [(mts, [{"rate": D("0.0004"), "period": 2, "count": 1, "amount": D("-20000")}]) for mts in points]
    report = validate_90_days(V4Policy(), trades, books)
    assert report.training.name == "training-60d"
    assert report.validation.name == "validation-15d"
    assert report.test.name == "test-15d"
    assert chronological_boundaries(start)[-1] == start + 90 * DAY_MS
    with pytest.raises(EvidenceInsufficient):
        validate_90_days(V4Policy(), trades, books[:-1])


def test_bootstrap_and_shadow_audit(tmp_path: Path) -> None:
    assert paired_bootstrap_lower([D("2"), D("2")], [D("1"), D("1")], 100) > 0
    store = V4Store(tmp_path / "v4.sqlite3")
    base = build_plan(V4Policy(), replace_market(1_000_000), D("1000"), D("1000"))
    store.record_shadow_plan(replace(base, as_of_ms=1_000_000))
    store.record_shadow_plan(replace(base, as_of_ms=1_000_000 + 7 * DAY_MS))
    report = shadow_audit(store)
    assert report["duration_days"] == "7.000"
    assert report["ready_for_manual_review"]


class WalletClient(BitfinexClient):
    def __init__(self, wallet_available: object) -> None:
        super().__init__("key", "secret")
        self.wallet_available = wallet_available

    def auth(self, path: str, body: dict | None = None):
        if path.endswith("wallets"):
            return [["funding", "USD", 1000, 0, self.wallet_available]]
        return []


def test_account_snapshot_requires_known_available_balance() -> None:
    assert WalletClient("100").account_snapshot().authoritative
    unknown = WalletClient(None).account_snapshot()
    assert not unknown.authoritative and unknown.wallet_available == 0


def test_public_client_error_is_wrapped() -> None:
    def fail(*_: object, **__: object):
        raise urllib.error.URLError("offline")

    client = BitfinexClient(opener=fail)
    with pytest.raises(BitfinexError, match="public request failed"):
        client.public("ticker/fUSD")
