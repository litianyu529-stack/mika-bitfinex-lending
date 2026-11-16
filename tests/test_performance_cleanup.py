import random
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

import lendingbot
import RuntimeV3
import StateStore
from StateStore import LendingStateStore
from StrategyResearch import _coverage
from StrategyV3 import (
    WINDOWS_MS,
    StrategyPolicyV3,
    _weighted_quantiles,
    _window_statistics,
    policy_v3_to_json,
)


def reference_quantile(rows, quantile):
    valid = []
    for row in rows:
        try:
            value, weight = D(row["rate"]), abs(D(row["amount"]))
            if value > 0 and weight > 0:
                valid.append((value, weight))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            pass
    if not valid:
        return D("0")
    valid.sort()
    target = sum((weight for _, weight in valid), D("0")) * quantile
    seen = D("0")
    for value, weight in valid:
        seen += weight
        if seen >= target:
            return value
    return valid[-1][0]


@pytest.mark.parametrize("empty", [False, True])
def test_batched_quantiles_preserve_weighting_boundaries_and_invalid_row_handling(empty):
    rng = random.Random(352)
    rows = [] if empty else [
        {"rate": str(D(rng.randrange(1, 50)) / 10000), "amount": str(rng.randrange(-20, 20))}
        for _ in range(500)
    ] + [{}, {"rate": "invalid", "amount": "100"}, {"rate": "0", "amount": "150"}]
    quantiles = tuple(D(q) for q in (".75", "0", "1", ".25", ".5", ".5", "-1", "2"))
    assert _weighted_quantiles(rows, quantiles) == tuple(reference_quantile(rows, q) for q in quantiles)


def test_shared_window_statistics_preserve_each_exact_time_boundary():
    now = 2_000_000_000_000
    rows = [
        {"mts": now - window + offset, "amount": D(index + 1), "rate": D(index + 1) / 10000}
        for index, window in enumerate(WINDOWS_MS.values()) for offset in (-1, 0, 1)
    ]
    result = _window_statistics(rows, now)
    for window, duration in WINDOWS_MS.items():
        selected = [row for row in rows if row["mts"] >= now - duration]
        assert result[window]["count"] == len(selected)
        assert result[window]["volume"] == sum((row["amount"] for row in selected), D("0"))
        for key, quantile in (("q25", ".25"), ("median", ".5"), ("q75", ".75")):
            assert result[window][key] == reference_quantile(selected, D(quantile))


@pytest.mark.parametrize("start,end", [(0, None), (60_000, 120_000), (180_000, 180_000), (240_000, None)])
def test_book_coverage_matches_previous_report_without_decoding_payloads(tmp_path, monkeypatch, start, end):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    for stamp in (0, 60_000, 120_000, 180_000):
        store.record_book_snapshot([{"rate": "0.001", "amount": "150", "period": 2}], now_ms=stamp)
    expected = _coverage(store.book_snapshots(start, end))

    def reject_decode(*_args, **_kwargs):
        raise AssertionError("Coverage must not decode historical books")

    monkeypatch.setattr(StateStore.json, "loads", reject_decode)
    assert store.book_snapshot_coverage(start, end) == expected


def test_performance_indexes_upgrade_once_without_replaying_state_migrations(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    original = LendingStateStore(path)
    original.set_mode("LIVE", "test")
    with original.transaction(immediate=True) as connection:
        connection.execute("DELETE FROM schema_meta WHERE key='v35_performance_indexes'")
        connection.execute("DROP INDEX market_bars_bucket_idx")
        connection.execute("DROP INDEX funding_trades_currency_managed_mts_idx")
    before = original.runtime()

    def reject_migration(*_args):
        raise AssertionError("An initialized V3.5 database must not replay state migrations")

    monkeypatch.setattr(LendingStateStore, "_migrate_v13_unattended_recovery", reject_migration)
    upgraded = LendingStateStore(path)
    assert upgraded.runtime() == before
    with upgraded.read_connection() as connection:
        details = connection.execute(
            "EXPLAIN QUERY PLAN DELETE FROM market_bars WHERE bucket_ms < ?", (123,)
        ).fetchall()
        assert "market_bars_bucket_idx" in str([tuple(row) for row in details])
        details = connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM funding_trades WHERE currency=? AND managed=1 AND mts>=? AND mts<?",
            ("USD", 0, 123),
        ).fetchall()
        assert "funding_trades_currency_managed_mts_idx" in str([tuple(row) for row in details])
    writer = sqlite3.connect(path)
    try:
        writer.execute("BEGIN IMMEDIATE")
        assert LendingStateStore(path).runtime() == before
    finally:
        writer.rollback()
        writer.close()


def test_statistics_windows_share_one_read_and_measure_coverage_across_gaps(tmp_path, monkeypatch):
    now = 2_000_000_000_000
    start = now - 86_400_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    for mts, credits in ((start - 60_000, 1000), (start + 60_000, 0), (now - 60_000, 1000), (now + 1, 0)):
        store.record_account_sample(1000, 1000 - credits, 0, credits, 0, mts)
    store.upsert_income_ledgers([
        {"id": index, "currency": "USD", "wallet": "funding", "amount": amount, "mts": mts}
        for index, (mts, amount) in enumerate(((start - 1, 2), (now - 1, 3), (now, -7), (now + 1, 99)), 1)
    ])
    read = store.read_connection
    statements = []

    @contextmanager
    def traced_read():
        with read() as connection:
            connection.set_trace_callback(statements.append)
            yield connection

    monkeypatch.setattr(store, "read_connection", traced_read)
    windows = store.statistics_windows(now_ms=now)
    assert list(windows) == ["1d", "7d", "30d", "90d", "all"]
    assert all(row["calculatedAtMs"] == now for row in windows.values())
    day = windows["1d"]
    assert day["sampleFrom"] == start + 60_000
    assert day["sampleTo"] == now - 60_000
    assert day["netInterest"] == "3"
    assert windows["all"]["netInterest"] == "5"
    assert D(day["sampleCoveragePercent"]) == D(240_000) / D(86_400_000) * 100
    assert D(day["observedUtilizationPercent"]) == 50
    assert statements.count("BEGIN") == 1
    for table in ("funding_trades", "ledger_entries", "credit_closures"):
        assert sum(f"FROM {table}" in statement for statement in statements) == 1


def test_worker_caches_history_for_five_minutes_and_explicit_query_reads_fresh(tmp_path):
    now = 2_000_000_000_000
    clock_ms = [now]
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: clock_ms[0] / 1000)
    runtime = RuntimeV3.LendingRuntimeV3(SimpleNamespace(), StrategyPolicyV3(), store, hub=SimpleNamespace())
    first = runtime._statistics_snapshot(now)
    clock_ms[0] = now + 299_999
    store.upsert_income_ledgers([{"id": 1, "wallet": "funding", "amount": 3, "mts": clock_ms[0]}])

    assert runtime._statistics_snapshot(clock_ms[0]) is first
    fresh = lendingbot.stats_v3_payload(store)["statistics"]
    assert fresh["all"]["calculatedAtMs"] == clock_ms[0]
    assert fresh["all"]["netInterest"] == "3"
    assert first["all"]["netInterest"] == "0"
    updated = runtime._statistics_snapshot(now + 300_000)
    assert updated["all"]["netInterest"] == "3"
    assert updated["all"]["calculatedAtMs"] == now + 300_000


@pytest.mark.parametrize("activate_pending", [False, True])
def test_cycle_reuses_signals_and_still_prices_with_newly_activated_policy(tmp_path, monkeypatch, activate_pending):
    now = 2_000_000_000_000
    policy = StrategyPolicyV3(short_floor_apr=D(".05"), medium_floor_apr=D(".06"), long_floor_apr=D(".07"))
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    store.save_strategy(policy_v3_to_json(policy), "ACTIVE")
    target = replace(policy, short_floor_apr=D(".055")) if activate_pending else policy
    if activate_pending:
        store.save_strategy(policy_v3_to_json(target), "PENDING")
    store.set_mode("LIVE", "test")

    class Hub:
        def snapshot(self, at):
            return {
                "now": at, "safeRequired": False, "book": [], "trades": [],
                "wallets": [{"wallet_type": "funding", "currency": "USD", "balance": D("0"), "available": D("0")}],
                "offers": [], "credits": [], "loans": [], "fundingTrades": [],
            }

    runtime = RuntimeV3.LendingRuntimeV3(
        SimpleNamespace(api_key="", api_secret=""), policy, store, hub=Hub(), clock=lambda: now / 1000
    )
    runtime._bootstrapped = True
    runtime._last_rest_sync_ms = now
    calls = []
    build = RuntimeV3.build_market_signals_v3

    def counted_build(book, trades, stats, active_policy, at, **kwargs):
        calls.append(active_policy)
        return build(book, trades, stats, active_policy, at, **kwargs)

    monkeypatch.setattr(RuntimeV3, "build_market_signals_v3", counted_build)
    status = runtime.cycle(now)
    assert calls == ([policy, target] if activate_pending else [policy])
    assert runtime.policy == target
    assert status["market"]["periodSelection"]["byPool"]["short"]["grossDailyFloor"] == str(
        build([], [], [], target, now)["periodSelection"]["byPool"]["short"]["grossDailyFloor"]
    )
