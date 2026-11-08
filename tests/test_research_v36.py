from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
import copy

import pytest

import StrategyResearch
from EventReplay import DAY_MS, ReplayAssumptions
from StrategyResearch import (
    ReplaySignalCache, backfill_public_market_data, evaluate_strategies, paired_moving_block_interval,
)
from StateStore import LendingStateStore
from StrategyV3 import StrategyPolicyV3, build_market_signals_v3, build_strategy_plan_v3
from MarketHistory import RollingMarketWindows


NOW = 200 * DAY_MS


def policy():
    return replace(StrategyPolicyV3(), short_floor_apr=D(5), medium_floor_apr=D(7), long_floor_apr=D(8),
                   enable_frr=False, enable_frr_delta_fixed=False, enable_frr_delta_variable=False)


class Store:
    def iter_market_trades(self, start, end):
        return iter([])

    def iter_book_snapshots(self, start, end):
        return iter([])

    def iter_funding_stats(self, start, end):
        return iter([])

    def repayment_calibration(self, start, end):
        return []


def ready_coverage(rows, start, end, **kwargs):
    return {name: {"meetsMinimum": True, "freshDays": "8", "freshNaturalDays": 8}
            for name in kwargs["segments"]}


def runner_calls(calls, *, missing=False, flat=False):
    def runner(active, trades, stats, principal, **kwargs):
        cfg = kwargs["assumptions"]
        label = ("C" if active.quick_pricing_mode == "EXECUTABLE_PREMIUM_V1"
                 else "A" if cfg.signal_mode == "legacy_20k" else "B")
        calls.append((label, kwargs["start_ms"], kwargs["now_ms"], dict(kwargs["segments"]), cfg))
        value = D(".001") if flat else {"A": D(".001"), "B": D(".002"), "C": D(".003")}[label]
        segments = {name: {"returnOnPrincipalTime": str(value), "netInterest": str(value * principal)}
                    for name in kwargs["segments"]}
        daily = [{"startMs": timestamp, "endMs": timestamp + DAY_MS, "returnOnPrincipalTime": str(value)}
                 for timestamp in range(kwargs["start_ms"], kwargs["now_ms"], DAY_MS)]
        return {"segments": segments, "daily": daily, "assumptions": dict(vars(cfg)),
                "finalState": {"cash": "0", "loanPrincipal": str(principal)},
                "dataQuality": {"historicalBook": True, "repaymentCalibrationUsable": not missing,
                                "tradeCoverageComplete": True}}
    return runner


def test_abc_has_twelve_scenarios_and_one_continuous_run_per_variant(monkeypatch):
    monkeypatch.setattr(StrategyResearch, "book_coverage", ready_coverage)
    calls = []
    result = evaluate_strategies(Store(), policy(), 1000, now_ms=NOW, replay_runner=runner_calls(calls))
    assert len(calls) == 36
    assert len(result["comparison"]["scenarioResults"]) == 12
    assert result["engineVersion"] == "event-v36-v1"
    assert result["selection"]["eligibleForLiveCandidate"]
    assert all(start == NOW - 90 * DAY_MS and end == NOW for _, start, end, _, _ in calls)
    assert all(bounds["validation"][0] == NOW - 30 * DAY_MS for _, _, _, bounds, _ in calls)
    assert all(row["pass"] and row["available"] for row in result["comparison"]["scenarioResults"])


@pytest.mark.parametrize("missing,flat", [(True, False), (False, True)])
def test_missing_calibration_or_no_gain_never_promotes(monkeypatch, missing, flat):
    monkeypatch.setattr(StrategyResearch, "book_coverage", ready_coverage)
    result = evaluate_strategies(Store(), policy(), 1000, now_ms=NOW,
                                 replay_runner=runner_calls([], missing=missing, flat=flat))
    assert not result["selection"]["eligibleForLiveCandidate"]
    assert result["selection"]["reasons"]


def test_incomplete_book_coverage_does_not_claim_pass(monkeypatch):
    result = evaluate_strategies(Store(), policy(), 1000, now_ms=NOW, replay_runner=runner_calls([]))
    assert not result["selection"]["eligibleForLiveCandidate"]
    assert all(not item["meetsMinimum"] for item in result["data"]["bookCoverage"].values())


def test_partial_scenario_subset_cannot_issue_validation_certificate(monkeypatch):
    monkeypatch.setattr(StrategyResearch, "book_coverage", ready_coverage)
    result = evaluate_strategies(Store(), policy(), 1000, now_ms=NOW, replay_runner=runner_calls([]),
                                 scenarios=[ReplayAssumptions(queue_multiplier=0)])
    assert not result["selection"]["eligibleForLiveCandidate"]
    assert any("twelve" in reason for reason in result["selection"]["reasons"])


def test_paired_moving_blocks_are_seeded_and_unknown_with_too_few_days():
    args = ([D(".02")] * 15, [D(".01")] * 15)
    first = paired_moving_block_interval(*args)
    assert first == paired_moving_block_interval(*args)
    assert first["lower"] == D(".01")
    assert first["blockDays"] == 3 and first["iterations"] == 2000 and first["seed"] == 307
    assert paired_moving_block_interval([D(1)] * 6, [D(0)] * 6)["lower"] is None
    with pytest.raises(ValueError, match="matching"):
        paired_moving_block_interval([D(1)], [])


def test_feature_cache_shares_only_market_data_and_returns_independent_trees():
    cache = ReplaySignalCache()
    try:
        base = policy()
        candidate = replace(base, quick_pricing_mode="EXECUTABLE_PREMIUM_V1")
        first = cache.get_signal(NOW, [], [], [], base, "full_7d", snapshot_factory=lambda: {"usable": False})
        first["regime"] = "MUTATED"
        second = cache.get_signal(NOW, [], [], [], candidate, "full_7d",
                                  snapshot_factory=lambda: pytest.fail("cache hit recomputed snapshot"))
        assert second["regime"] != "MUTATED"
        assert cache.hits == 1 and cache.misses == 1
    finally:
        cache.close()


def tracked_caches(monkeypatch, tmp_path):
    created = []

    class TrackedCache(ReplaySignalCache):
        def __init__(self, directory=None):
            super().__init__(directory=tmp_path)
            self.saved_path = Path(self._directory.name)
            self.close_calls = 0
            created.append(self)

        def close(self):
            self.close_calls += 1
            super().close()

    monkeypatch.setattr(StrategyResearch, "ReplaySignalCache", TrackedCache)
    return created


@pytest.mark.parametrize("failure", [RuntimeError("replay failed"), KeyboardInterrupt()])
def test_owned_real_cache_is_closed_when_replay_raises_or_is_interrupted(monkeypatch, tmp_path, failure):
    created = tracked_caches(monkeypatch, tmp_path)

    def failing_replay(*args, **kwargs):
        assert created[0].saved_path.is_dir()
        assert created[0].connection is not None
        raise failure

    monkeypatch.setattr(StrategyResearch, "run_event_replay", failing_replay)
    with pytest.raises(type(failure)):
        evaluate_strategies(Store(), policy(), 1000, now_ms=NOW,
                            scenarios=[ReplayAssumptions(queue_multiplier=0)])
    assert len(created) == 1
    assert created[0].close_calls == 1
    assert created[0].connection is None
    assert not created[0].saved_path.exists()
    created[0].close()  # A caller or finalizer may close an already closed cache.
    assert created[0].close_calls == 2


def test_owned_real_cache_is_closed_after_normal_evaluation(monkeypatch, tmp_path):
    created = tracked_caches(monkeypatch, tmp_path)
    monkeypatch.setattr(StrategyResearch, "run_event_replay", runner_calls([]))
    report = evaluate_strategies(Store(), policy(), 1000, now_ms=NOW,
                                 scenarios=[ReplayAssumptions(queue_multiplier=0)])
    assert report["schemaVersion"] == 2
    assert created[0].close_calls == 1
    assert created[0].connection is None
    assert not created[0].saved_path.exists()


@pytest.mark.parametrize("failure", [None, RuntimeError("replay failed"), KeyboardInterrupt()])
def test_injected_provider_remains_owned_by_caller_on_every_exit(monkeypatch, failure):
    class Provider:
        close_calls = 0

        def __bool__(self):
            return False  # Injection is determined by identity, not truthiness.

        def close(self):
            self.close_calls += 1

    provider = Provider()

    def forbid_owned_cache(*args, **kwargs):
        pytest.fail("an injected provider must not create an owned cache")

    normal = runner_calls([])

    def replay(*args, **kwargs):
        assert kwargs["signal_provider"] is provider
        if failure is not None:
            raise failure
        return normal(*args, **kwargs)

    monkeypatch.setattr(StrategyResearch, "ReplaySignalCache", forbid_owned_cache)
    monkeypatch.setattr(StrategyResearch, "run_event_replay", replay)
    if failure is None:
        evaluate_strategies(Store(), policy(), 1000, now_ms=NOW, signal_provider=provider,
                            scenarios=[ReplayAssumptions(queue_multiplier=0)])
    else:
        with pytest.raises(type(failure)):
            evaluate_strategies(Store(), policy(), 1000, now_ms=NOW, signal_provider=provider,
                                scenarios=[ReplayAssumptions(queue_multiplier=0)])
    assert provider.close_calls == 0


def test_backfill_certifies_empty_intervals_without_inventing_trades(tmp_path):
    store = LendingStateStore(tmp_path / "empty.sqlite3")
    client = SimpleNamespace(funding_trades=lambda *args, **kwargs: [], funding_stats=lambda *args, **kwargs: [])
    report = backfill_public_market_data(client, store, now_ms=NOW,
                                         rate_limiter=SimpleNamespace(wait=lambda: None))
    assert report["complete"]
    assert report["trades"]["count"] == 0
    assert store.market_coverage(NOW - 90 * DAY_MS, NOW)["complete"]


def test_backfill_saturated_millisecond_stays_unresolved_and_never_skips_ids(tmp_path):
    store = LendingStateStore(tmp_path / "saturated.sqlite3")
    timestamp = NOW - 45 * DAY_MS
    rows = [[index, timestamp, "1000", ".001", 2] for index in range(1, 4)]
    calls = []

    def public_trades(_symbol, start, end, limit, sort):
        calls.append((start, end))
        return [row for row in rows if start <= row[1] <= end][:limit]

    client = SimpleNamespace(funding_trades=public_trades, funding_stats=lambda *args, **kwargs: [])
    report = backfill_public_market_data(client, store, now_ms=NOW, page_limit=2,
                                         rate_limiter=SimpleNamespace(wait=lambda: None))
    assert not report["complete"]
    assert (timestamp, timestamp) in calls
    assert any(row["status"] == "UNRESOLVED" for row in report["tradeCoverage"]["unresolvedIntervals"])
    assert report["trades"]["count"] == 2


def test_backfill_minimum_and_maximum_existing_rows_do_not_prove_the_middle(tmp_path):
    store = LendingStateStore(tmp_path / "gapped.sqlite3")
    start = NOW - 90 * DAY_MS
    store.upsert_market_trades([
        {"id": "edge1", "mts": start + 1, "amount": "1000", "rate": ".001", "period": 2},
        {"id": "edge2", "mts": NOW - 1, "amount": "1000", "rate": ".001", "period": 2}])
    client = SimpleNamespace(
        funding_trades=lambda *args, **kwargs: [[123, NOW - 45 * DAY_MS, "1000", ".001", 2]],
        funding_stats=lambda *args, **kwargs: [])
    report = backfill_public_market_data(client, store, now_ms=NOW,
                                         rate_limiter=SimpleNamespace(wait=lambda: None))
    assert report["complete"]
    assert "123" in {row["id"] for row in store.market_trades()}


@pytest.mark.parametrize("cleaning", ["legacy_global", "exact_term"])
def test_research_details_subset_keeps_all_term_market_demand_quotes_and_plan_exact(cleaning):
    active = replace(policy(), short_floor_apr=D(".05"), medium_floor_apr=D(".07"), long_floor_apr=D(".08"))
    configured = {period for pool in ("short", "medium", "long") for period in active.periods(pool)}
    rows = [{"id": f"{period}-{index}", "period": period, "mts": NOW - index * 60_000 - period,
             "amount": D(100 + index * 3), "rate": D(".001") + D(period) / D(10_000_000)}
            for period in range(2, 121) for index in range(35)]
    full = RollingMarketWindows(cleaning_mode=cleaning)
    compact = RollingMarketWindows(cleaning_mode=cleaning, detailed_periods=configured)
    full.add(rows)
    compact.add(rows)
    original = full.snapshot(active, NOW, coverage_complete=True, usable=True)
    optimized = compact.snapshot(active, NOW, coverage_complete=True, usable=True)
    assert len(original["byPeriod"]) == 119 and len(optimized["byPeriod"]) == 5
    assert original["global"] == optimized["global"]
    assert len(optimized["global"]["periodDemand"]) == 119
    assert original["rawTradeCount"] == optimized["rawTradeCount"]
    assert original["validTradeCount"] == optimized["validTradeCount"]
    assert all(original["byPeriod"][period] == optimized["byPeriod"][period] for period in configured)
    book = [{"rate": D(".001"), "period": period, "amount": D(-10_000)} for period in configured]
    left = build_market_signals_v3(book, [], [], active, NOW, window_snapshot=original)
    right = build_market_signals_v3(book, [], [], active, NOW, window_snapshot=optimized)
    comparable = copy.deepcopy(right)
    comparable["historyQuality"].pop("detailPeriods")
    assert left == comparable
    before = build_strategy_plan_v3(D(10_000), D(10_000), {}, active, left)
    after = build_strategy_plan_v3(D(10_000), D(10_000), {}, active, right)
    assert before == after
