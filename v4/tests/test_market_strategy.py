from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from mika_v4.config import V4Policy
from mika_v4.domain import LongGateState, PeriodChoice, PlannerState
from mika_v4.market import RATE_TICK, build_market_snapshot, ceil_tick, weighted_quantile
from mika_v4.strategy import (
    MIN_OFFER,
    bottom_rung_triggered,
    build_plan,
    equal_amounts,
    fast_shift_allowed,
    floor_stale,
    gross_daily_floor,
    net_apr_percent,
    next_long_gate,
    next_period_choice,
    raw_long_tier,
)


D = Decimal


def trade(mts: int, rate: str, amount: str = "100", period: int = 2) -> dict:
    return {"mts": mts, "rate": D(rate), "amount": D(amount), "period": period}


def test_rate_tick_and_floor_round_trip(policy: V4Policy) -> None:
    floor = gross_daily_floor(D("10"), D("15"))
    assert floor == D("0.0003224")
    assert net_apr_percent(floor, D("15")) >= D("10")
    assert ceil_tick(D("0.00032231")) == floor
    assert ceil_tick(D("0")) == 0


def test_weighted_quantile_and_empty() -> None:
    rows = [{"rate": D("1"), "amount": D("1")}, {"rate": D("2"), "amount": D("3")}]
    assert weighted_quantile(rows, D("0.5")) == 2
    assert weighted_quantile([], D("0.5")) == 0


def test_anchor_filters_isolated_outlier_and_needs_two_signals(policy: V4Policy) -> None:
    now = 100_000_000
    trades = [trade(now - i * 60_000, "0.00030") for i in range(120)]
    book = [{"rate": D("0.01"), "period": 2, "count": 1, "amount": D("-1000")}]
    snapshot = build_market_snapshot(book, trades, policy, now, now)
    assert snapshot.valid_components == 2
    assert snapshot.robust_anchor == D("0.00030")
    assert snapshot.fresh

    insufficient = build_market_snapshot(book, [], policy, now, now)
    assert insufficient.valid_components == 1
    assert insufficient.robust_anchor == 0
    assert not insufficient.fresh


@pytest.mark.parametrize(
    ("spread", "expected"),
    [("0", "0.00001"), ("0.000004", "0.00001"), ("0.001", "0.00005")],
)
def test_dynamic_step_is_clamped(policy: V4Policy, spread: str, expected: str) -> None:
    now = 100_000_000
    low = D("0.00030")
    high = low + D(spread)
    trades = [trade(now - i * 60_000, str(low if i % 2 else high)) for i in range(100)]
    book = [{"rate": low, "period": 2, "count": 1, "amount": D("-1000")}]
    assert build_market_snapshot(book, trades, policy, now, now).grid_step == D(expected)


def test_equal_amounts_conserves_cents_and_places_remainder_lowest() -> None:
    amounts = equal_amounts(D("1000.00000003"), 5)
    assert len(amounts) == 5
    assert sum(amounts) == D("1000.00000003")
    assert amounts[0] >= amounts[-1]
    assert equal_amounts(D("299"), 2) == ()
    assert equal_amounts(D("150"), 1) == (D("150"),)


def test_period_switch_requires_twenty_percent_and_two_cycles() -> None:
    current = PeriodChoice(current=2)
    weak = next_period_choice(current, (2, 4), {2: D("1"), 4: D("1.19")})
    assert weak.current == 2 and weak.candidate is None
    first = next_period_choice(current, (2, 4), {2: D("1"), 4: D("1.2")})
    assert first == PeriodChoice(current=2, candidate=4, confirmations=1)
    second = next_period_choice(first, (2, 4), {2: D("1"), 4: D("1.2")})
    assert second == PeriodChoice(current=4)
    assert next_period_choice(PeriodChoice(), (), {}) == PeriodChoice()


def test_long_three_tiers_two_up_one_down(policy: V4Policy, market) -> None:
    threshold = gross_daily_floor(policy.long_floor_apr_percent, policy.normal_fee_percent)
    tier3_market = replace(market, robust_anchor=threshold + D("0.00002"), supported_ceiling=D("0.001"))
    assert raw_long_tier(policy, tier3_market) == 3
    first = next_long_gate(policy, tier3_market, LongGateState())
    assert first.tier == 0 and first.confirmations == 1
    second = next_long_gate(policy, tier3_market, first)
    assert second.tier == 3
    down = next_long_gate(policy, replace(tier3_market, robust_anchor=threshold - RATE_TICK), second)
    assert down.tier == 0
    not_rising = replace(tier3_market, median_5m=D("0.0002"))
    assert next_long_gate(policy, not_rising, LongGateState()).tier == 0


def test_grid_allocation_minimum_rungs_and_floor(policy: V4Policy, market) -> None:
    plan = build_plan(policy, market, D("3000"), D("3000"))
    short = [item for item in plan.orders if item.pool == "short"]
    medium = [item for item in plan.orders if item.pool == "medium"]
    assert len(short) == 5
    assert len(medium) == 4
    assert sum(item.amount for item in short) / sum(item.amount for item in medium) == 3
    assert all(item.amount >= MIN_OFFER for item in plan.orders)
    assert all(
        item.rate >= gross_daily_floor(policy.floor_apr_percent(item.pool), policy.normal_fee_percent)
        for item in plan.orders
    )
    assert sum(item.amount for item in plan.orders) == plan.planned_amount
    assert plan.planned_amount <= plan.deployable


def test_long_allocation_is_one_120_day_offer(policy: V4Policy, market) -> None:
    threshold = gross_daily_floor(policy.long_floor_apr_percent, policy.normal_fee_percent)
    strong = replace(market, robust_anchor=threshold + D("0.00002"), supported_ceiling=D("0.001"))
    state = PlannerState(long_gate=LongGateState(tier=3))
    plan = build_plan(policy, strong, D("10000"), D("10000"), state)
    long_orders = [item for item in plan.orders if item.pool == "long"]
    assert len(long_orders) == 1
    assert long_orders[0].period == 120
    assert long_orders[0].amount == D("3000")


def test_pool_redistribution_and_small_idle_target(policy: V4Policy, market) -> None:
    too_low_for_medium = replace(market, supported_ceiling=gross_daily_floor(D("7"), D("15")))
    plan = build_plan(policy, too_low_for_medium, D("600"), D("600"))
    assert {item.pool for item in plan.orders} == {"short"}
    targeted = build_plan(policy, market, D("610"), D("1000"), target_pool="medium")
    assert {item.pool for item in targeted.orders} == {"medium"}
    assert sum(item.amount for item in targeted.orders) == D("610")
    one_minimum = build_plan(policy, market, D("150"), D("150"))
    assert {item.pool for item in one_minimum.orders} == {"short"}
    with pytest.raises(ValueError):
        build_plan(policy, market, D("610"), D("1000"), target_pool="long")


def test_insufficient_market_or_cap_keeps_idle(policy: V4Policy, market) -> None:
    stale = replace(market, fresh=False)
    assert build_plan(policy, stale, D("500"), D("500")).orders == ()
    capped = build_plan(replace(policy, max_lend_amount=D("300")), market, D("1000"), D("1000"))
    assert capped.planned_amount <= D("300")
    assert "LENDING_CAP_APPLIED" in capped.reasons
    exposure_capped = build_plan(
        replace(policy, max_lend_percent=D("50")),
        market,
        D("500"),
        D("1000"),
        existing_committed=D("400"),
    )
    assert exposure_capped.planned_amount <= D("100")


@pytest.mark.parametrize(
    ("original", "remaining", "expected"),
    [("300", "151", False), ("300", "150", True), ("300", "149", True), ("300", "0", True)],
)
def test_bottom_rung_partial_fill_rules(original: str, remaining: str, expected: bool) -> None:
    assert bottom_rung_triggered(D(original), D(remaining), D("50")) is expected


def test_fast_shift_and_floor_stale(policy: V4Policy, market) -> None:
    assert fast_shift_allowed(D("0.00034"), market)
    assert not fast_shift_allowed(D("0.00030"), market)
    floor = gross_daily_floor(policy.short_floor_apr_percent, policy.normal_fee_percent)
    assert floor_stale("short", floor, floor, 0, 60 * 60_000, policy)
    assert not floor_stale("short", floor + D("0.00001"), floor, 0, 999_999_999, policy)
