import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
import configparser

import pytest

import lendingbot

from Logger import Logger
from MarketDataStream import BitfinexMarketDataHub
from RuntimeV3 import (
    LendingRuntimeV3,
    _age_stage_target,
    _exploration_age_stage_target,
    build_active_credit_dashboard,
    parse_ledger_rows_v3,
)
from StateStore import InsufficientReservedBalance, LendingStateStore
from StrategyV3 import (
    EXACT_TERM_EXPLORATION_CURVE,
    StrategyPolicyV3,
    _candidate_target_rate,
    _candidate_types,
    _winner_target_shares,
    build_market_signals_v3,
    build_strategy_plan_v3,
    ceil_rate_tick,
    competitive_rate_for_period,
    evenly_distributed_amounts,
    filter_supported_trades,
    gross_daily_floor,
    exploration_start_rate_for_period,
    json_decimal,
    period_pricing_context,
    pool_for_period,
    rate_below_floor,
    replay_strategy_v3,
    validate_policy_v3,
)
from bitfinex import Bitfinex, BitfinexAmbiguousWriteError, BitfinexApiError


D = Decimal


def policy(**overrides):
    values = {
        "short_floor_apr": D("0.05"),
        "medium_floor_apr": D("0.06"),
        "long_floor_apr": D("0.07"),
        **overrides,
    }
    return replace(StrategyPolicyV3(), **values)


def signals(**overrides):
    result = {
        "regime": "neutral",
        "best_bid": D("0.00040"),
        "best_offer": D("0.00042"),
        "anchor_rate": D("0.00040"),
        "frr_daily_rate": D("0.00035"),
        "utilization": D("0.8"),
        "volume_ratio_5m": D("1"),
        "trend_threshold": D("0.00002"),
        "windows": {
            "1h": {"median": D("0.00040")},
            "24h": {"q25": D("0.00030"), "q75": D("0.00050")},
        },
    }
    result.update(overrides)
    return result


def selection_row(periods, selected, demands, *, qualified=True, low_confirmed=False):
    scores = []
    for period, demand in zip(periods, demands):
        demand = D(str(demand))
        scores.append(
            {
                "period": period,
                "relativeDemandShare": demand,
                "absoluteDemandShare": demand,
                "demandScore": demand,
                "fillScore": demand,
                "totalScore": demand,
                "marketQualified": qualified,
                "additionalQualified": qualified,
                "lowDemandConfirmed": low_confirmed and period != selected,
            }
        )
    return {
        "selectedPeriod": selected,
        "eligiblePeriods": list(periods) if selected is not None else [],
        "scores": scores,
        "marketQualified": qualified,
        "additionalQualified": qualified,
        "lowDemandConfirmed": low_confirmed and not qualified,
        "totalScore": max((row["totalScore"] for row in scores), default=D("0")),
    }


def limit_policy(**overrides):
    return policy(
        enable_frr=False,
        enable_frr_delta_fixed=False,
        enable_frr_delta_variable=False,
        **overrides,
    )


def legacy_six_stage_policy(**overrides):
    return limit_policy(
        short_reprice_stages_minutes=(10, 30, 60, 90, 120, 180),
        medium_reprice_stages_minutes=(20, 60, 120, 180, 240, 360),
        long_reprice_stages_minutes=(60, 180, 360, 480, 720, 1440),
        **overrides,
    )


def intent_order(index=0, amount="150"):
    return {
        "currency": "USD",
        "slice_key": f"v3:short:quick:{index}",
        "pool": "short",
        "layer": "quick",
        "amount": D(amount),
        "submitted_rate": D("0.0002"),
        "effective_rate": D("0.0002"),
        "period": 2,
        "offer_type": "LIMIT",
        "flags": 0,
        "strategy_version": "v3",
    }


def test_active_credit_dashboard_summarizes_overall_and_term_groups():
    now = 2_000_000_000_000
    dashboard = build_active_credit_dashboard(
        [
            {
                "id": 1,
                "currency": "USD",
                "amount": D("100"),
                "rate": D("0.0001"),
                "period": 7,
                "status": "ACTIVE",
                "managed": True,
                "pool": "short",
                "mts_opening": now - 86_400_000,
            },
            {
                "id": 2,
                "currency": "USD",
                "amount": D("300"),
                "rate": D("0.0002"),
                "period": 8,
                "status": "ACTIVE",
                "hidden": True,
                "managed": True,
                "pool": "medium",
                "mts_opening": now - 3 * 86_400_000,
            },
            {
                "id": 3,
                "currency": "USD",
                "amount": D("600"),
                "rate": D("0.0003"),
                "rate_real": D("0.0004"),
                "period": 60,
                "status": "ACTIVE",
                "managed": False,
                "pool": "external",
                "mts_opening": now - 5 * 86_400_000,
            },
        ],
        D("2000"),
        policy(),
        now,
    )

    overall = dashboard["summary"]["overall"]
    assert overall["orderCount"] == 3
    assert overall["principal"] == "1000"
    assert overall["utilizationPercent"] == "50.0"
    assert overall["averageDailyRatePercent"] == "0.03100"
    assert overall["averageContractDays"] == "39.1"
    assert overall["averageElapsedDays"] == "4"
    assert overall["estimatedNetIncomePerDay"] == "0.261700"
    assert overall["estimatedNetAprPercent"] == "9.5520500"

    groups = dashboard["summary"]["groups"]
    assert groups["short"]["orderCount"] == 1
    assert groups["short"]["averageDailyRatePercent"] == "0.0100"
    assert groups["medium"]["averageContractDays"] == "8"
    assert groups["long"]["principal"] == "600"
    assert dashboard["credits"][2]["displayPool"] == "long"
    assert dashboard["credits"][2]["effectiveRate"] == "0.0004"


@pytest.mark.parametrize(
    ("period", "expected"),
    [(7, "short"), (8, "medium"), (30, "medium"), (31, "long"), (120, "long")],
)
def test_active_credit_dashboard_term_boundaries(period, expected):
    dashboard = build_active_credit_dashboard(
        [
            {
                "id": period,
                "currency": "USD",
                "amount": D("100"),
                "rate": D("0.0002"),
                "period": period,
                "status": "ACTIVE",
            }
        ],
        D("100"),
        policy(),
        2_000_000_000_000,
    )
    assert dashboard["credits"][0]["displayPool"] == expected
    assert dashboard["summary"]["groups"][expected]["orderCount"] == 1


def test_active_credit_dashboard_empty_and_missing_opening_time():
    empty = build_active_credit_dashboard([], D("0"), policy(), 2_000_000_000_000)
    assert empty["summary"]["overall"] == {
        "orderCount": 0,
        "principal": "0",
        "utilizationPercent": None,
        "shareOfLentPercent": None,
        "averageDailyRatePercent": None,
        "estimatedNetAprPercent": None,
        "averageContractDays": None,
        "averageElapsedDays": None,
        "estimatedNetIncomePerDay": "0",
    }
    missing_opening = build_active_credit_dashboard(
        [
            {
                "id": 1,
                "currency": "USD",
                "amount": D("100"),
                "rate": D("0.0002"),
                "period": 14,
                "status": "ACTIVE",
            }
        ],
        D("100"),
        policy(),
        2_000_000_000_000,
    )
    assert missing_opening["credits"][0]["elapsedDays"] is None
    assert missing_opening["credits"][0]["contractEndAtMs"] is None
    assert missing_opening["summary"]["overall"]["averageElapsedDays"] is None


def test_fixed_150_base_creates_maximum_even_slices_and_conserves_amount():
    first = build_strategy_plan_v3(D("10000"), D("10000"), {}, limit_policy(), signals(), "same")
    second = build_strategy_plan_v3(D("10000"), D("10000"), {}, limit_policy(), signals(), "same")
    assert len(first["plan"]) == 66
    assert [row["amount"] for row in first["plan"]] == [row["amount"] for row in second["plan"]]
    assert sum((row["amount"] for row in first["plan"]), D("0")) == D("10000.00000000")
    assert all(row["amount"] >= D("150") for row in first["plan"])
    for pool in ("short", "medium", "long"):
        amounts = [row["amount"] for row in first["plan"] if row["pool"] == pool]
        assert max(amounts) - min(amounts) <= D("0.00000001")


def test_insufficient_balance_reduces_slice_count_and_never_overallocates():
    result = build_strategy_plan_v3(D("1000"), D("1000"), {}, limit_policy(), signals(), "small")
    assert result["target_slice_count"] == 6
    assert len(result["plan"]) == 6
    assert result["planned_amount"] == D("1000.00000000")


@pytest.mark.parametrize(
    ("available", "expected_count", "expected_pools"),
    [
        ("149.99", 0, set()),
        ("150", 1, {"short"}),
        ("299.99", 1, {"short"}),
        ("300", 2, {"short", "medium"}),
        ("449.99", 2, {"short", "medium"}),
        ("450", 3, {"short", "medium", "long"}),
    ],
)
def test_small_balance_respects_minimum_and_pool_caps(available, expected_count, expected_pools):
    amount = D(available)
    result = build_strategy_plan_v3(D("10000"), amount, {}, limit_policy(), signals(), f"ladder:{available}")

    assert {row["pool"] for row in result["plan"]} == expected_pools
    assert len(result["plan"]) == expected_count
    if expected_count:
        assert result["planned_amount"] <= amount
        assert all(row["amount"] >= D("150") for row in result["plan"])
    else:
        assert result["planned_amount"] == D("0")
        assert result["empty_reason"] in {"BELOW_MINIMUM", "CONCENTRATION_CAP_OR_MINIMUM"}


def test_fragmented_pool_deficits_use_one_order_within_allocation_tolerance():
    available = D("240.12179174")
    result = build_strategy_plan_v3(
        D("13169.46811969"),
        available,
        {"short": D("2711.01654548"), "medium": D("2196.12350893"), "long": D("8022.20627354")},
        limit_policy(short_share=D("34"), medium_share=D("33"), long_share=D("33")),
        signals(),
        "live-small-balance",
        existing_exposure={"total": D("12929.34632795")},
        offer_exposure_by_pool={
            "short": D("218.34722822"),
            "medium": D("211.92525092"),
            "long": D("211.92525091"),
        },
    )

    assert len(result["plan"]) == 1
    row = result["plan"][0]
    assert row["amount"] >= D("150")
    assert result["planned_amount"] + result["idle_amount"] == available
    pool = row["pool"]
    assert result["current_offer_amounts"][pool] + row["amount"] <= result["target_offer_amounts"][pool] + D("150")


@pytest.mark.parametrize("cap", [None, D("13600")])
def test_166_wallet_fragmentation_regression(monkeypatch, cap):
    monkeypatch.setattr("StrategyV3._pool_targets_v33", lambda *_args: (
        {"short": D("287.72823020"), "medium": D("180.74051923"), "long": D("150")},
        ("short", "medium", "long"),
    ))
    result = build_strategy_plan_v3(
        D("13778.90784513"), D("166.00781590"), {},
        limit_policy(max_lend_amount=cap), signals(), "fragmented-166",
        existing_exposure={"total": D("13612.90002923")},
        offer_exposure_by_pool={"short": D("150"), "medium": D("152.46093353"), "long": D("150")},
    )
    if cap is None:
        assert len(result["plan"]) == 1
        assert result["plan"][0]["pool"] == "short"
        assert result["planned_amount"] == D("166.00781590")
        assert result["idle_amount"] == 0
    else:
        assert result["plan"] == []


def test_small_balance_uses_largest_eligible_deficit_instead_of_pool_order():
    configured = limit_policy(short_share=D("0"), medium_share=D("0"), long_share=D("100"))

    result = build_strategy_plan_v3(
        D("10000"),
        D("150"),
        {},
        configured,
        signals(),
        "only-long-small-balance",
        offer_exposure_by_pool={"short": D("0"), "medium": D("0"), "long": D("0")},
    )

    assert len(result["plan"]) == 1
    assert result["plan"][0]["pool"] == "long"
    assert result["plan"][0]["amount"] == D("150.00000000")


def test_plan_absorbs_the_exact_live_remainder_into_an_existing_safe_order(tmp_path, monkeypatch):
    target = D("153.43173419")
    available = D("153.85005852")
    monkeypatch.setattr(
        "StrategyV3._pool_targets_v33",
        lambda *_args: (
            {"short": target, "medium": D("150"), "long": D("0")},
            ("short", "medium"),
        ),
    )

    result = build_strategy_plan_v3(
        D("13726.38596911"),
        available,
        {},
        limit_policy(),
        signals(),
        "live-remainder",
        offer_exposure_by_pool={"short": D("0"), "medium": D("0"), "long": D("150")},
    )

    assert len(result["plan"]) == 1
    assert result["plan"][0]["amount"] == available
    assert result["planned_amount"] == available
    assert result["idle_amount"] == 0
    assert result["absorbed_remainder"] == D("0.41832433")
    assert result["absorbedRemainder"] == D("0.41832433")
    assert result["remainder_absorption_pool"] == "short"
    assert result["remainderAbsorptionPool"] == "short"


@pytest.mark.parametrize(
    ("remainder", "absorbed", "idle"),
    (
        ("0.00000001", "0.00000001", "0"),
        ("1", "1", "0"),
        ("149.99999999", "149.99999999", "0"),
        ("150", "0", "150"),
    ),
)
def test_plan_absorbs_only_sub_150_remainders(monkeypatch, remainder, absorbed, idle):
    monkeypatch.setattr(
        "StrategyV3._pool_targets_v33",
        lambda *_args: ({"short": D("150"), "medium": D("0"), "long": D("0")}, ("short",)),
    )
    available = D("150") + D(remainder)

    result = build_strategy_plan_v3(
        D("1000"),
        available,
        {},
        limit_policy(short_share=D("100"), medium_share=D("0"), long_share=D("0")),
        signals(),
        f"remainder:{remainder}",
    )

    assert result["absorbed_remainder"] == D(absorbed)
    assert result["idle_amount"] == D(idle)
    assert result["planned_amount"] + result["idle_amount"] == available


def test_remainder_absorption_prefers_the_highest_scoring_planned_pool(monkeypatch):
    monkeypatch.setattr(
        "StrategyV3._pool_targets_v33",
        lambda *_args: ({"short": D("150"), "medium": D("150"), "long": D("0")}, ("short", "medium")),
    )
    selection = {
        "byPool": {
            "short": selection_row((2,), 2, ("0.1",)),
            "medium": selection_row((30,), 30, ("0.9",)),
            "long": selection_row((120,), 120, ("0",), qualified=False),
        }
    }

    result = build_strategy_plan_v3(
        D("1000"),
        D("300.4"),
        {},
        limit_policy(),
        signals(periodSelection=selection),
        "scored-remainder",
    )
    amounts = {row["pool"]: row["amount"] for row in result["plan"]}

    assert amounts == {"short": D("150.00000000"), "medium": D("150.40000000")}
    assert result["remainder_absorption_pool"] == "medium"
    assert result["idle_amount"] == 0


def test_remainder_absorption_never_exceeds_the_hard_funding_cap(monkeypatch):
    monkeypatch.setattr(
        "StrategyV3._pool_targets_v33",
        lambda *_args: ({"short": D("150"), "medium": D("0"), "long": D("0")}, ("short",)),
    )
    result = build_strategy_plan_v3(
        D("1000"),
        D("200"),
        {},
        limit_policy(
            short_share=D("100"),
            medium_share=D("0"),
            long_share=D("0"),
            max_lend_percent=D("50"),
        ),
        signals(),
        "capped-remainder",
        existing_exposure={"total": D("349")},
    )

    assert result["cap_limited_available"] == D("151")
    assert result["planned_amount"] == D("151")
    assert result["absorbed_remainder"] == D("1")
    assert result["idle_amount"] == D("49")


def test_variable_remainder_stays_idle_when_the_variable_cap_is_full(monkeypatch):
    monkeypatch.setattr(
        "StrategyV3._pool_targets_v33",
        lambda *_args: ({"short": D("150"), "medium": D("0"), "long": D("0")}, ("short",)),
    )
    result = build_strategy_plan_v3(
        D("1000"),
        D("150.4"),
        {},
        policy(
            short_share=D("100"),
            medium_share=D("0"),
            long_share=D("0"),
            enable_limit=False,
            enable_frr_delta_fixed=False,
            variable_max_share=D("15"),
        ),
        signals(),
        "variable-remainder-cap",
    )

    assert result["plan"][0]["offer_type"] == "FRRDELTAVAR"
    assert result["planned_amount"] == D("150")
    assert result["absorbed_remainder"] == 0
    assert result["idle_amount"] == D("0.4")


def test_sub_minimum_wallet_without_a_safe_order_remains_idle():
    result = build_strategy_plan_v3(
        D("1000"),
        D("0.41832433"),
        {},
        limit_policy(),
        signals(),
        "standalone-sub-dollar",
    )

    assert result["plan"] == []
    assert result["absorbed_remainder"] == 0
    assert result["idle_amount"] == D("0.41832433")


@pytest.mark.parametrize(
    ("total", "expected"),
    [
        ("150", ["150.00000000"]),
        ("299.99", ["299.99000000"]),
        ("300", ["150.00000000", "150.00000000"]),
        ("301", ["150.50000000", "150.50000000"]),
        ("449.99", ["224.99500000", "224.99500000"]),
    ],
)
def test_even_amount_allocator_spreads_remainder_across_every_order(total, expected):
    count = int(D(total) // D("150"))
    amounts = evenly_distributed_amounts(D(total), count)

    assert amounts == [D(value) for value in expected]
    assert sum(amounts, D("0")) == D(total)
    assert all(amount >= D("150") for amount in amounts)


def test_even_amount_allocator_distributes_satoshis_without_losing_value():
    amounts = evenly_distributed_amounts(D("1000"), 6)

    assert sum(amounts, D("0")) == D("1000")
    assert max(amounts) - min(amounts) == D("0.00000001")


def test_pool_and_layer_splits_use_primary_runner_up_and_long_exception():
    result = build_strategy_plan_v3(D("10000"), D("10000"), {}, limit_policy(), signals(), "split")
    pools = {name: [row for row in result["plan"] if row["pool"] == name] for name in ("short", "medium", "long")}
    assert [len(pools[name]) for name in ("short", "medium", "long")] == [33, 23, 10]
    assert {row["period"] for row in pools["short"]} == {2}
    assert {row["period"] for row in pools["medium"]} == {14}
    assert {row["period"] for row in pools["long"]} == {120}
    assert len({row["effective_rate"] for row in result["plan"]}) > 1


def test_selected_period_is_capped_and_runner_up_receives_remainder():
    period_selection = {
        "byPool": {
            "short": selection_row((2, 7), 2, ("0.80", "0.20")),
            "medium": selection_row((30, 14), 30, ("0.80", "0.20")),
            "long": selection_row((120,), 120, ("1",)),
        }
    }
    result = build_strategy_plan_v3(
        D("10000"),
        D("10000"),
        {},
        limit_policy(short_share=D("40"), medium_share=D("35"), long_share=D("25")),
        signals(periodSelection=period_selection),
        "market-weighted",
    )
    pools = {
        name: [row["period"] for row in result["plan"] if row["pool"] == name] for name in ("short", "medium", "long")
    }
    assert set(pools["short"]) == {2, 7}
    assert set(pools["medium"]) == {14, 30}
    assert set(pools["long"]) == {120}


def test_low_demand_pools_keep_150_and_release_the_rest_to_short():
    period_selection = {
        "byPool": {
            "short": selection_row((2, 7), 2, ("0.986", "0.014"), low_confirmed=True),
            "medium": selection_row((14, 30), 14, ("0.03", "0.01"), qualified=False, low_confirmed=True),
            "long": selection_row((120,), 120, ("0.01",), qualified=False, low_confirmed=True),
        }
    }

    result = build_strategy_plan_v3(
        D("10000"),
        D("1000"),
        {},
        limit_policy(),
        signals(periodSelection=period_selection),
        "redistribute-blocked-long",
    )

    assert result["pool_redistribution_basis"] == "CONFIGURED_TARGET_WITH_150_MINIMUM_AND_GLOBAL_SCORE_RELEASE"
    assert result["target_offer_amounts"] == {
        "short": D("700.00000000"),
        "medium": D("150.00000000"),
        "long": D("150.00000000"),
    }
    assert {row["period"] for row in result["plan"] if row["pool"] == "short"} == {2}
    assert result["planned_amount"] == D("1000.00000000")


def test_blocked_pool_does_not_redistribute_when_no_pool_has_an_eligible_period():
    period_selection = {
        "byPool": {
            pool: {"selectedPeriod": None, "scores": [], "insufficientMarketData": True}
            for pool in ("short", "medium", "long")
        }
    }

    result = build_strategy_plan_v3(
        D("10000"),
        D("555"),
        {},
        limit_policy(),
        signals(periodSelection=period_selection),
        "no-qualified-pool",
    )

    assert result["plan"] == []
    assert result["empty_reason"] == "MARKET_DATA_INSUFFICIENT"


def test_two_day_986_percent_demand_gets_all_new_short_money():
    selection = {
        "byPool": {
            "short": selection_row((2, 7), 2, ("0.986", "0.014"), low_confirmed=True),
            "medium": selection_row((14, 30), 14, ("0.03", "0.01"), qualified=False, low_confirmed=True),
            "long": selection_row((120,), 120, ("0.01",), qualified=False, low_confirmed=True),
        }
    }
    result = build_strategy_plan_v3(
        D("1000"), D("1000"), {}, limit_policy(), signals(periodSelection=selection), "short-only"
    )
    short_total = sum((row["amount"] for row in result["plan"] if row["pool"] == "short"), D("0"))
    two_day = sum((row["amount"] for row in result["plan"] if row["period"] == 2), D("0"))

    assert short_total == D("700.00000000")
    assert two_day == short_total
    assert not any(row["period"] == 7 for row in result["plan"])
    assert result["idle_amount"] == D("0")


def test_single_configured_short_candidate_can_receive_its_pool():
    short_only = {
        "byPool": {
            "short": selection_row((2,), 2, ("1",)),
            "medium": {"selectedPeriod": None, "eligiblePeriods": [], "scores": []},
            "long": {"selectedPeriod": None, "eligiblePeriods": [], "scores": []},
        }
    }
    short_result = build_strategy_plan_v3(
        D("1000"),
        D("1000"),
        {},
        limit_policy(short_share=D("100"), medium_share=D("0"), long_share=D("0")),
        signals(periodSelection=short_only),
        "one-short",
    )

    assert {row["period"] for row in short_result["plan"]} == {2}
    assert short_result["planned_amount"] == D("1000.00000000")


def test_existing_overweight_term_receives_no_new_money_and_is_not_canceled():
    selection = {
        "byPool": {
            "short": {"selectedPeriod": 2, "eligiblePeriods": [2, 7], "scores": []},
            "medium": {"selectedPeriod": 14, "eligiblePeriods": [14, 30], "scores": []},
            "long": {"selectedPeriod": 120, "eligiblePeriods": [120], "scores": []},
        }
    }
    result = build_strategy_plan_v3(
        D("1000"),
        D("300"),
        {},
        limit_policy(),
        signals(periodSelection=selection),
        "existing-overweight",
        offer_exposure_by_pool={"short": D("700"), "medium": D("0"), "long": D("0")},
        offer_exposure_by_period={2: D("700")},
    )

    assert all(row["pool"] != "short" for row in result["plan"])
    assert result["rebalance_cancellations"] == []


def test_open_offer_ratios_ignore_credit_exposure():
    configured = limit_policy(short_share=D("60"), medium_share=D("30"), long_share=D("10"))
    result = build_strategy_plan_v3(
        D("10000"),
        D("1000"),
        {"long": D("9000")},
        configured,
        signals(),
        "offers-only",
        existing_exposure={"total": D("9000")},
        offer_exposure_by_pool={"short": D("0"), "medium": D("0"), "long": D("0")},
    )
    amounts = {
        pool: sum((row["amount"] for row in result["plan"] if row["pool"] == pool), D("0"))
        for pool in ("short", "medium", "long")
    }
    assert result["allocationBasis"] == "AUTHORITATIVE_WALLET_PLUS_MANAGED_OPEN_OFFERS"
    assert amounts["short"] > amounts["medium"] > amounts["long"]
    assert sum(amounts.values(), D("0")) == D("1000.00000000")


def test_offer_ratio_diagnostics_include_existing_offers_and_new_balance():
    result = build_strategy_plan_v3(
        D("3000"),
        D("1000"),
        {},
        limit_policy(short_share=D("60"), medium_share=D("30"), long_share=D("10")),
        signals(),
        "diagnostics",
        existing_exposure={"total": D("1000")},
        offer_exposure_by_pool={"short": D("600"), "medium": D("300"), "long": D("100")},
    )
    assert result["targetOfferAmounts"] == {"short": D("1200"), "medium": D("600"), "long": D("200")}
    assert result["currentOfferAmounts"]["short"] == D("600")
    assert result["ratio_tolerance"] == D("150")


def test_layer_allocation_uses_managed_credits_and_offers_for_isolated_repayment():
    result = build_strategy_plan_v3(
        D("10000"),
        D("150"),
        {},
        limit_policy(
            short_share=D("100"),
            medium_share=D("0"),
            long_share=D("0"),
            quick_share=D("30"),
            balanced_share=D("40"),
            high_share=D("30"),
        ),
        signals(),
        "cumulative-layers",
        existing_exposure={"total": D("9850")},
        exposure_by_layer={"quick": D("3000"), "balanced": D("4000"), "high": D("2850")},
        offer_exposure_by_pool={"short": D("0"), "medium": D("0"), "long": D("0")},
        offer_exposure_by_layer={"quick": D("0"), "balanced": D("0"), "high": D("0")},
    )

    assert result["layer_allocation_basis"] == "ATTRIBUTED_MANAGED_EXPOSURE_PLUS_AVAILABLE"
    assert result["current_layer_amounts"] == {
        "quick": D("3000"),
        "balanced": D("4000"),
        "high": D("2850"),
    }
    assert len(result["plan"]) == 1
    assert result["plan"][0]["layer"] == "high"


def test_unattributed_legacy_credits_do_not_inflate_every_layer_target():
    result = build_strategy_plan_v3(
        D("10000"),
        D("150"),
        {},
        limit_policy(short_share=D("100"), medium_share=D("0"), long_share=D("0")),
        signals(),
        "unattributed-layers",
        exposure_by_layer={"quick": D("100"), "balanced": D("100"), "high": D("100")},
        offer_exposure_by_pool={"short": D("0"), "medium": D("0"), "long": D("0")},
    )

    assert sum(result["target_layer_amounts"].values(), D("0")) == D("450")
    assert result["unattributed_layer_amount"] == D("9550")


def test_new_balanced_offer_uses_q75_start_guard_before_age_repricing():
    market = signals(
        anchor_rate=D("0.0001500"),
        windows={
            "1h": {"median": D("0.0001580")},
            "24h": {"q25": D("0.0001800"), "q75": D("0.0002191")},
        },
    )

    target = _candidate_target_rate(
        {"layer": "balanced", "slice_index": 0},
        market,
        D("0.0002088"),
    )

    assert target == D("0.0002191")


def test_new_balanced_offer_q75_guard_never_weakens_floor():
    market = signals(
        anchor_rate=D("0.0001500"),
        windows={
            "1h": {"median": D("0.0001580")},
            "24h": {"q25": D("0.0001500"), "q75": D("0.0001800")},
        },
    )

    target = _candidate_target_rate(
        {"layer": "balanced", "slice_index": 0},
        market,
        D("0.0002088"),
    )

    assert target == D("0.0002088")


def test_net_apr_floors_and_fee_difference_are_enforced():
    normal = gross_daily_floor(D("0.05"), D("0.15"))
    hidden = gross_daily_floor(D("0.05"), D("0.18"))
    assert normal == D("0.05") / D("365") / D("0.85")
    assert hidden > normal
    result = build_strategy_plan_v3(D("10000"), D("10000"), {}, limit_policy(), signals(), "floors")
    for row in result["plan"]:
        assert row["effective_rate"] >= gross_daily_floor(result_floor(row["pool"]), D("0.15"))


def test_floor_comparison_tolerates_exchange_float_tail_but_rejects_real_shortfall():
    floor = gross_daily_floor(D("0.0897"), D("0.15"))
    observed = D("0.00028912167606768733")
    assert rate_below_floor(observed, floor) is False
    assert rate_below_floor(floor - D("0.000000000000000002"), floor) is True
    assert ceil_rate_tick(floor) >= floor
    offer = {
        "period": 8,
        "pool": "medium",
        "display_type": "LIMIT",
        "flags": 0,
        "rate": observed,
    }
    assert "below_new_floor" not in lendingbot.v3_offer_violations(
        offer,
        limit_policy(medium_floor_apr=D("0.0897")),
    )


def test_live_requires_all_three_positive_floors_but_paused_does_not():
    validate_policy_v3(StrategyPolicyV3())
    with pytest.raises(ValueError):
        validate_policy_v3(StrategyPolicyV3(), require_live_floors=True)
    validate_policy_v3(policy(), require_live_floors=True)


def test_term_pool_defaults_and_boundary_fallback_are_consistent():
    configured = StrategyPolicyV3()
    assert configured.short_share == D("50")
    assert configured.medium_share == D("35")
    assert configured.long_share == D("15")
    assert configured.short_periods == (2, 7)
    assert configured.medium_periods == (14, 30)
    assert configured.long_periods == (120,)
    assert configured.periods("short") == (2, 7)
    assert configured.periods("medium") == (14, 30)
    assert configured.periods("long") == (120,)
    validate_policy_v3(configured)

    assert [pool_for_period(period) for period in (2, 7, 8, 30, 31, 120)] == [
        "short",
        "short",
        "medium",
        "medium",
        "long",
        "long",
    ]
    assert pool_for_period(1) == "external"
    assert pool_for_period(121) == "external"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"short_periods": (2, 8)}, "short periods must be unique increasing days within 2-7"),
        ({"medium_periods": (6, 30)}, "medium periods must be unique increasing days within 7-30"),
        ({"long_periods": (29, 120)}, "long periods must be unique increasing days within 30-120"),
        ({"long_periods": (30, 121)}, "long periods must be unique increasing days within 30-120"),
        ({"medium_periods": (14, 14, 30)}, "medium periods must be unique increasing days within 7-30"),
        ({"medium_periods": (30, 14)}, "medium periods must be unique increasing days within 7-30"),
    ],
)
def test_term_pool_periods_reject_values_outside_configured_ranges(overrides, message):
    with pytest.raises(ValueError, match=message):
        validate_policy_v3(policy(**overrides))


def test_empty_max_lend_amount_config_means_unlimited_amount():
    config = configparser.ConfigParser()
    config.read_dict(
        {
            "STRATEGY_V3": {
                "max_lend_amount": "",
                "max_lend_percent": "100",
                "short_periods": "2-7",
                "medium_periods": "7-30",
                "long_periods": "30-120",
            }
        }
    )
    parsed = lendingbot.strategy_v3_from_config(config)
    assert parsed.max_lend_amount is None
    assert parsed.max_lend_percent == D("100")
    assert lendingbot.strategy_v3_api_values(parsed)["short_periods"] == [2, 7]
    assert parsed.medium_periods == (14, 30)
    assert parsed.long_periods == (120,)


def test_old_strategy_record_without_quick_stage_fields_uses_v352_defaults():
    legacy_policy = lendingbot.json_decimal(limit_policy().__dict__)
    for pool in ("short", "medium", "long"):
        legacy_policy.pop(f"quick_{pool}_reprice_stages_minutes")

    parsed = lendingbot.strategy_v3_from_record({"policy": legacy_policy})
    api_values = lendingbot.strategy_v3_api_values(parsed)

    assert api_values["quick_short_reprice_stages_minutes"] == [5, 10, 15, 20, 30, 40, 50, 60, 75, 90]
    assert api_values["quick_medium_reprice_stages_minutes"] == [5, 10, 20, 30, 60, 75, 90, 120, 150, 180]
    assert api_values["quick_long_reprice_stages_minutes"] == [15, 30, 60, 90, 180, 240, 360, 480, 600, 720]


def test_v4_chain_stage_count_uses_ten_stage_default_for_old_raw_policy(tmp_path):
    store = LendingStateStore(tmp_path / "old-policy-stage-count.sqlite3")
    legacy_policy = lendingbot.json_decimal(limit_policy().__dict__)
    for pool in ("short", "medium", "long"):
        legacy_policy.pop(f"quick_{pool}_reprice_stages_minutes")
    version = store.save_strategy(legacy_policy, status="ACTIVE")

    with store.read_connection() as connection:
        assert store._strategy_reprice_stage_count(
            connection,
            version,
            "short",
            "quick",
            EXACT_TERM_EXPLORATION_CURVE,
        ) == 10
        assert store._strategy_reprice_stage_count(
            connection,
            version,
            "short",
            "quick",
            "EXACT_TERM_EXPLORATION_V3",
        ) == 10


def test_legacy_order_sizing_fields_are_ignored_and_not_serialized():
    parsed = lendingbot.strategy_v3_from_api_payload(
        {
            "target_slices": 3,
            "min_order_amount": "500",
            "amount_jitter": "10",
        },
        base=limit_policy(),
    )
    api_values = lendingbot.strategy_v3_api_values(parsed)

    assert not {"target_slices", "min_order_amount", "amount_jitter"} & set(api_values)
    assert "max_pool_shift" not in api_values
    assert api_values["fixedSafety"]["demandWeightPercent"] == "70"
    assert api_values["fixedSafety"]["fillProbabilityWeightPercent"] == "30"
    assert api_values["fixedSafety"]["lowDemandThresholdPercent"] == "5"
    result = build_strategy_plan_v3(D("1000"), D("1000"), {}, parsed, signals(), "legacy-fields")
    assert result["target_slice_count"] == 6
    assert len(result["plan"]) == 6


def test_editable_discrete_period_ladders_are_preserved():
    parsed = lendingbot.strategy_v3_from_api_payload(
        {
            "short_periods": [2, 3, 5, 7],
            "medium_periods": [8, 14, 21, 30],
            "long_periods": [120],
        }
    )
    assert parsed.short_periods == (2, 3, 5, 7)
    assert parsed.medium_periods == (8, 14, 21, 30)
    assert parsed.long_periods == (120,)
    api_values = lendingbot.strategy_v3_api_values(parsed)
    assert {name: api_values[name] for name in ("short_periods", "medium_periods", "long_periods")} == {
        "short_periods": [2, 3, 5, 7],
        "medium_periods": [8, 14, 21, 30],
        "long_periods": [120],
    }


@pytest.mark.parametrize("legacy", ([14, 30], (14, 30), "14,30", "14/30"))
def test_two_item_period_ladder_is_preserved(legacy):
    parsed = lendingbot.strategy_v3_from_api_payload({"medium_periods": legacy})
    assert parsed.medium_periods == (14, 30)


def test_nonlegacy_hyphenated_period_input_is_rejected():
    with pytest.raises(Exception, match="medium periods must be comma-separated days within 7-30"):
        lendingbot.strategy_v3_from_api_payload({"medium_periods": "14-30"})


def test_tiered_reprice_stages_default_parse_and_validate():
    parsed = lendingbot.strategy_v3_from_api_payload(
        {
            "short_reprice_stages_minutes": [10, 30, 60],
            "medium_reprice_stages_minutes": "20,60,120",
            "long_reprice_stages_minutes": [60, 180, 360],
            "quick_short_reprice_stages_minutes": [5, 30, 90],
            "quick_medium_reprice_stages_minutes": "10,60,180",
            "quick_long_reprice_stages_minutes": [30, 180, 720],
        }
    )
    assert parsed.reprice_stages("short") == (10, 30, 60, 90, 120, 180)
    assert parsed.reprice_stages("medium") == (20, 60, 120, 180, 240, 360)
    assert parsed.reprice_stages("long") == (60, 180, 360, 480, 720, 1440)
    assert parsed.reprice_stages("short", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        5, 30, 90, 135, 180, 270
    )
    assert parsed.reprice_stages("medium", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        10, 60, 180, 270, 360, 540
    )
    assert parsed.reprice_stages("long", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        30, 180, 720, 960, 1440, 2880
    )
    with pytest.raises(Exception):
        lendingbot.strategy_v3_from_api_payload({"short_reprice_stages_minutes": [10, 10, 60, 90, 120, 180]})
    with pytest.raises(Exception):
        lendingbot.strategy_v3_from_api_payload(
            {"quick_short_reprice_stages_minutes": [5, 5, 15, 20, 30, 40, 50, 60, 75, 90]}
        )


def test_six_stage_targets_preserve_market_stages_then_converge_to_floor():
    chain = {
        "origin_rate": D("0.0006"),
        "market_anchor_rate": D("0.00045"),
    }
    benchmark = D("0.00045")
    floor = D("0.0003")
    assert [_age_stage_target(stage, chain, benchmark, floor) for stage in range(1, 7)] == [
        D("0.00055"),
        D("0.00050"),
        D("0.00045"),
        D("0.00040"),
        D("0.00035"),
        D("0.00030"),
    ]


def test_ten_stage_defaults_and_targets_preserve_final_horizons():
    configured = validate_policy_v3(policy())
    assert configured.reprice_stages("short") == (5, 10, 20, 30, 60, 75, 90, 120, 150, 180)
    assert configured.reprice_stages("medium") == (10, 20, 40, 60, 120, 150, 180, 240, 300, 360)
    assert configured.reprice_stages("long") == (30, 60, 120, 180, 360, 480, 720, 960, 1200, 1440)
    assert configured.reprice_stages("short", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        5, 10, 15, 20, 30, 40, 50, 60, 75, 90
    )
    assert configured.reprice_stages("medium", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        5, 10, 20, 30, 60, 75, 90, 120, 150, 180
    )
    assert configured.reprice_stages("long", "quick", EXACT_TERM_EXPLORATION_CURVE) == (
        15, 30, 60, 90, 180, 240, 360, 480, 600, 720
    )
    assert configured.reprice_stages("short", "quick", "EXACT_TERM_EXPLORATION_V3") == configured.reprice_stages(
        "short"
    )
    assert configured.minimum_offer_minutes == 5
    assert configured.reprice_cooldown_minutes == 5
    assert configured.max_reprices_per_hour == 12
    assert configured.minimum_rate_change == D("0.00001")

    chain = {"origin_rate": D("0.0006"), "market_anchor_rate": D("0.00045")}
    targets = [
        _age_stage_target(
            stage,
            chain,
            D("0.00045"),
            D("0.0003"),
            stage_count=10,
        )
        for stage in range(1, 11)
    ]
    assert targets == [
        D("0.00057"),
        D("0.00054"),
        D("0.00051"),
        D("0.00048"),
        D("0.00045"),
        D("0.00042"),
        D("0.00039"),
        D("0.00036"),
        D("0.00033"),
        D("0.00030"),
    ]


def test_reprice_stage_migration_maps_six_stage_progress_without_regression():
    assert [LendingStateStore._map_reprice_stage(stage, 6, 10) for stage in range(7)] == [0, 2, 4, 5, 7, 9, 10]
    assert LendingStateStore._map_reprice_stage(10, 10, 6) == 6


def result_floor(pool_name):
    return {"short": D("0.05"), "medium": D("0.06"), "long": D("0.07")}[pool_name]


def test_unsupported_long_floor_keeps_only_the_150_dollar_minimum():
    selection = {
        "byPool": {
            "short": selection_row((2, 7), 2, ("0.8", "0.2")),
            "medium": selection_row((14, 30), 14, ("0.8", "0.2")),
            "long": selection_row((120,), 120, ("1",), qualified=False, low_confirmed=True),
        }
    }
    result = build_strategy_plan_v3(
        D("10000"),
        D("10000"),
        {},
        limit_policy(long_floor_apr=D("1")),
        signals(periodSelection=selection),
        "idle-long",
    )
    long_rows = [row for row in result["plan"] if row["pool"] == "long"]
    assert sum((row["amount"] for row in long_rows), D("0")) == D("150.00000000")
    assert all(row["effective_rate"] >= gross_daily_floor(D("1"), D("0.15")) for row in long_rows)
    assert result["planned_amount"] == D("10000.00000000")
    assert result["idle_amount"] == D("0")


def test_all_offer_types_are_generated_and_plain_frr_is_zero_variable_delta():
    generated = _candidate_types(policy(), D("0.00035"), D("0.00042"))
    displays = {row[3] for row in generated}
    assert displays == {"LIMIT", "FRR", "FRR_DELTA_FIXED", "FRR_DELTA_VARIABLE"}
    plain = next(row for row in generated if row[3] == "FRR")
    assert plain[:3] == ("FRRDELTAVAR", D("0"), D("0.00035"))


def test_frr_delta_fixed_never_generates_bitfinex_rejected_negative_offset():
    generated = _candidate_types(policy(), D("0.00042"), D("0.00035"))
    fixed = next(row for row in generated if row[3] == "FRR_DELTA_FIXED")
    assert fixed[:3] == ("FRRDELTAFIX", D("0"), D("0.00042"))


def test_bitfinex_offer_payload_maps_frr_and_hidden_flag():
    client = Bitfinex("key", "secret")
    writes = []
    client._auth_write = lambda path, payload: writes.append((path, payload)) or [0, "SUCCESS"]
    client.submit_funding_offer("fUSD", "150", "0.0003", 2, "FRR", hidden=True)
    client.submit_funding_offer("fUSD", "150", "0.0001", 14, "FRRDELTAFIX")
    client.submit_funding_offer("fUSD", "150", "0.0001", 30, "FRRDELTAVAR")
    assert writes[0][1]["type"] == "FRRDELTAVAR"
    assert writes[0][1]["rate"] == "0"
    assert writes[0][1]["flags"] == 64
    assert [row[1]["type"] for row in writes[1:]] == ["FRRDELTAFIX", "FRRDELTAVAR"]


def test_bitfinex_rejects_negative_frr_delta_before_network_write():
    client = Bitfinex("key", "secret")
    client._auth_write = lambda *_args, **_kwargs: pytest.fail("invalid delta must not reach Bitfinex")
    with pytest.raises(BitfinexApiError, match="cannot be negative"):
        client.submit_funding_offer("fUSD", "150", "-0.0001", 14, "FRRDELTAFIX")


def test_variable_share_cap_includes_plain_frr():
    only_variable = policy(enable_limit=False, enable_frr_delta_fixed=False, variable_max_share=D("10"))
    result = build_strategy_plan_v3(D("10000"), D("10000"), {}, only_variable, signals(), "variable")
    assert result["variable_amount"] <= D("1000")
    assert all(row["offer_type"] == "FRRDELTAVAR" for row in result["plan"])


def test_hidden_requires_cap_and_never_beats_visible_on_equal_economics():
    with pytest.raises(ValueError):
        validate_policy_v3(policy(enable_hidden=True, hidden_max_share=None))
    result = build_strategy_plan_v3(
        D("10000"), D("10000"), {}, limit_policy(enable_hidden=True, hidden_max_share=D("10")), signals(), "hidden"
    )
    assert result["hidden_amount"] == 0
    assert all(row["flags"] == 0 for row in result["plan"])


def test_small_high_rate_outlier_is_removed_without_removing_supported_levels():
    now = int(time.time() * 1000)
    trades = [
        {"mts": now, "rate": D("0.0003"), "amount": D("1000"), "period": 2},
        {"mts": now, "rate": D("0.00031"), "amount": D("1000"), "period": 2},
        {"mts": now, "rate": D("0.02"), "amount": D("1"), "period": 2},
    ]
    filtered = filter_supported_trades(trades, policy())
    assert len(filtered) == 2
    assert max(row["rate"] for row in filtered) == D("0.00031")


def test_market_windows_and_spike_detection():
    now = 1_900_000_000_000
    historical = [
        {"id": i, "mts": now - 3_600_000 + i * 60_000, "rate": D("0.00020"), "amount": D("100"), "period": 2}
        for i in range(50)
    ]
    recent = [
        {"id": 100 + i, "mts": now - i * 20_000, "rate": D("0.00060"), "amount": D("500"), "period": 14}
        for i in range(10)
    ]
    result = build_market_signals_v3([], historical + recent, [], policy(), now)
    assert set(result["windows"]) == {"1m", "5m", "15m", "1h", "6h", "24h", "7d"}
    assert result["spike"] is True
    assert result["regime"] == "spike"
    assert result["period_demand"][2]["trade_count"] == 50
    assert result["period_demand"][14]["trade_count"] == 10
    assert result["period_demand"][2]["score"] == D("0.70")
    assert result["period_demand"][14]["score"] == D("0.30")


def test_period_selection_combines_windowed_demand_and_exact_period_book_scores():
    now = 1_900_000_000_000
    trades = []
    for period, count, amount in ((2, 6, D("100")), (4, 2, D("400")), (7, 2, D("100"))):
        trades.extend(
            {"mts": now - index * 1_000, "rate": D("0.0005"), "amount": amount, "period": period}
            for index in range(count)
        )
    book = [
        {"period": 2, "rate": D("0.0005"), "amount": D("-1000")},
        {"period": 4, "rate": D("0.0005"), "amount": D("-500")},
        {"period": 7, "rate": D("0.0005"), "amount": D("-100")},
    ]

    configured = policy(short_periods=(2, 4, 7))
    selection = build_market_signals_v3(book, trades, [], configured, now)["periodSelection"]["byPool"]["short"]
    rows = {row["period"]: row for row in selection["scores"]}

    assert selection["selectedPeriod"] == 2
    assert rows[2]["demandScore"] == D("0.51")
    assert rows[4]["demandScore"] == D("0.32")
    assert rows[7]["demandScore"] == D("0.17")
    assert rows[2]["fillScore"] == D("1.00")
    assert rows[4]["fillScore"] == D("0.650")
    assert rows[7]["fillScore"] == D("0.370")
    assert rows[2]["totalScore"] == D("0.6570")


def test_exact_period_pricing_isolates_book_windows_anchor_and_q75():
    now = 1_900_000_000_000
    bases = {2: D("0.00020"), 7: D("0.00030"), 14: D("0.00040"), 30: D("0.00050"), 120: D("0.00060")}
    trades = [
        {
            "mts": now - index * 60_000,
            "rate": base + D(index) * D("0.00001"),
            "amount": D("1000"),
            "period": period,
        }
        for period, base in bases.items()
        for index in range(4)
    ]
    book = [
        {"period": period, "rate": base - D("0.00001"), "amount": D("-1000")}
        for period, base in bases.items()
    ]
    market = build_market_signals_v3(book, trades, [], limit_policy(), now)
    floor = D("0.00018")

    two_day = period_pricing_context(market, "short", 2, floor)
    seven_day = period_pricing_context(market, "short", 7, floor)

    assert two_day["source"] == "EXACT_PERIOD"
    assert two_day["bestBorrowRate"] == D("0.00019")
    assert two_day["q75_24h"] == D("0.00022")
    assert seven_day["bestBorrowRate"] == D("0.00029")
    assert seven_day["q75_24h"] == D("0.00032")
    assert competitive_rate_for_period("quick", "short", 2, market, floor) == competitive_rate_for_period(
        "balanced", "short", 2, market, floor
    )
    assert competitive_rate_for_period("quick", "short", 7, market, floor) == competitive_rate_for_period(
        "balanced", "short", 7, market, floor
    )
    assert competitive_rate_for_period(
        "quick", "short", 2, market, floor, "EXACT_TERM_EXPLORATION_V3"
    ) == D("0.0001899")
    assert competitive_rate_for_period("high", "long", 120, market, floor) > D("0.00060")

    market["best_bid"] = D("0.00999")
    market["windows"]["24h"]["q75"] = D("0.00888")
    assert competitive_rate_for_period("quick", "short", 2, market, floor) == D("0.00021")
    assert competitive_rate_for_period("balanced", "short", 2, market, floor) < D("0.001")


def test_quick_and_balanced_new_offers_share_the_exact_period_q75_guard():
    now = 1_900_000_000_000
    trades = [
        {"mts": now - index * 60_000, "rate": D("0.00022"), "amount": D("1000"), "period": 2}
        for index in range(4)
    ] + [
        {"mts": now - index * 60_000, "rate": D("0.00090"), "amount": D("1000"), "period": 7}
        for index in range(4)
    ]
    book = [
        {"period": 2, "rate": D("0.00019"), "amount": D("-1000")},
        {"period": 7, "rate": D("0.00089"), "amount": D("-1000")},
    ]
    market = build_market_signals_v3(book, trades, [], limit_policy(), now)

    balanced = _candidate_target_rate(
        {"pool": "short", "period": 2, "layer": "balanced", "slice_index": 0},
        market,
        D("0.00018"),
    )
    quick = _candidate_target_rate(
        {"pool": "short", "period": 2, "layer": "quick", "slice_index": 0},
        market,
        D("0.00018"),
    )

    assert quick == balanced == D("0.00022")


def test_exact_period_pricing_with_no_term_data_falls_back_to_its_floor():
    now = 1_900_000_000_000
    configured = limit_policy(short_periods=(2, 4, 7))
    market = build_market_signals_v3(
        [{"period": 2, "rate": D("0.0005"), "amount": D("-1000")}],
        [{"mts": now, "rate": D("0.0005"), "amount": D("1000"), "period": 2}],
        [],
        configured,
        now,
    )
    floor = D("0.0002088")

    context = period_pricing_context(market, "short", 4, floor)

    assert context["source"] == "EXACT_PERIOD"
    assert context["rateDataAvailable"] is False
    assert competitive_rate_for_period("quick", "short", 4, market, floor) == floor
    assert competitive_rate_for_period("balanced", "short", 4, market, floor) == floor
    assert competitive_rate_for_period("high", "short", 4, market, floor) == floor


@pytest.mark.parametrize(
    ("pool", "period", "term_rate"),
    [
        ("short", 2, "0.00022"),
        ("short", 7, "0.00027"),
        ("medium", 14, "0.00032"),
        ("medium", 30, "0.00037"),
        ("long", 120, "0.00042"),
    ],
)
def test_every_supported_term_uses_only_its_exact_period_landing(pool, period, term_rate):
    now = 1_900_000_000_000
    configured = limit_policy()
    rates = {
        2: D("0.00022"),
        7: D("0.00027"),
        14: D("0.00032"),
        30: D("0.00037"),
        120: D("0.00042"),
    }
    book = [
        {"period": candidate, "rate": rate - D("0.00001"), "amount": D("-1000")}
        for candidate, rate in rates.items()
    ]
    trades = [
        {"mts": now, "period": candidate, "rate": rate, "amount": D("1000")}
        for candidate, rate in rates.items()
    ]
    market = build_market_signals_v3(book, trades, [], configured, now)
    floor = ceil_rate_tick(
        gross_daily_floor(configured.floor_apr(pool), configured.normal_fee_rate)
    )
    context = period_pricing_context(market, pool, period, floor)

    assert context["source"] == "EXACT_PERIOD"
    assert context["period"] == period
    assert context["anchorRate"] == D(term_rate)
    assert competitive_rate_for_period("balanced", pool, period, market, floor) == D(term_rate)


def test_low_same_period_market_still_starts_balanced_and_high_above_floor():
    now = 1_900_000_000_000
    configured = policy(short_floor_apr=D("0.065"), normal_fee_rate=D("0.147"))
    market = build_market_signals_v3(
        [{"period": 2, "rate": D("0.00015"), "amount": D("-1000")}],
        [
            {"mts": now - 10_000, "rate": D("0.00015"), "amount": D("1000"), "period": 2},
            {"mts": now - 20_000, "rate": D("0.00019"), "amount": D("1000"), "period": 2},
        ],
        [],
        configured,
        now,
    )
    floor = ceil_rate_tick(gross_daily_floor(configured.short_floor_apr, configured.normal_fee_rate))

    balanced_start = ceil_rate_tick(floor * D("1.05"))
    assert exploration_start_rate_for_period("quick", "short", 2, market, floor, configured) == balanced_start
    assert exploration_start_rate_for_period("balanced", "short", 2, market, floor, configured) == balanced_start
    assert exploration_start_rate_for_period(
        "quick", "short", 2, market, floor, configured, "EXACT_TERM_EXPLORATION_V3"
    ) == floor
    assert exploration_start_rate_for_period("high", "short", 2, market, floor, configured) == ceil_rate_tick(
        floor * D("1.12")
    )


def test_no_same_period_data_still_uses_configured_exploration_premiums():
    now = 1_900_000_000_000
    configured = policy(short_floor_apr=D("0.065"), normal_fee_rate=D("0.147"))
    market = build_market_signals_v3([], [], [], configured, now)
    floor = ceil_rate_tick(gross_daily_floor(configured.short_floor_apr, configured.normal_fee_rate))

    assert exploration_start_rate_for_period("balanced", "short", 2, market, floor, configured) == ceil_rate_tick(
        floor * D("1.05")
    )
    assert exploration_start_rate_for_period("quick", "short", 2, market, floor, configured) == ceil_rate_tick(
        floor * D("1.05")
    )
    assert exploration_start_rate_for_period("high", "short", 2, market, floor, configured) == ceil_rate_tick(
        floor * D("1.12")
    )


def test_exploration_curve_reaches_balanced_and_high_landings_at_selected_stages():
    chain = {"origin_rate": "0.0002400"}
    floor = D("0.0002000")
    landing = D("0.0002100")

    balanced = [
        _exploration_age_stage_target(stage, chain, landing, floor, 5) for stage in range(1, 7)
    ]
    high = [_exploration_age_stage_target(stage, chain, landing, floor, 7) for stage in range(1, 8)]

    assert balanced == [
        D("0.0002340"),
        D("0.0002280"),
        D("0.0002220"),
        D("0.0002160"),
        landing,
        D("0.0002080"),
    ]
    assert high[-1] == landing
    assert all(left >= right for left, right in zip(high, high[1:]))
    assert _exploration_age_stage_target(10, chain, landing, floor, 5) == floor
    assert _exploration_age_stage_target(10, chain, landing, floor, 7) == floor


@pytest.mark.parametrize(
    ("landing_stage", "stage_count"),
    [(5, 10), (7, 10)],
)
def test_exploration_curve_never_increases_and_finishes_at_hard_floor(landing_stage, stage_count):
    chain = {"origin_rate": "0.0003400"}
    values = [
        _exploration_age_stage_target(
            stage,
            chain,
            D("0.0002800"),
            D("0.0002088"),
            landing_stage,
            stage_count,
        )
        for stage in range(1, stage_count + 1)
    ]

    assert all(left >= right for left, right in zip(values, values[1:]))
    assert values[landing_stage - 1] == D("0.0002800")
    assert values[-1] == D("0.0002088")


def test_fixed_landing_rate_survives_restart_and_ignores_later_market_rise(tmp_path):
    now = 1_900_000_000_000
    path = tmp_path / "fixed-landing.sqlite3"
    store = LendingStateStore(path)
    order = {
        **intent_order(),
        "submitted_rate": D("0.00034"),
        "effective_rate": D("0.00034"),
        "fixed_landing_rate": D("0.00028"),
        "pricing_curve_version": EXACT_TERM_EXPLORATION_CURVE,
    }
    _, intent = store.reserve_intent(order, D("1000"))
    offer = {
        "id": 7701,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.00034"),
        "rate_real": D("0.00034"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now - 5 * 60_000,
    }
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    assert D(chain["fixed_landing_rate"]) == D("0.00028")

    restarted = LendingStateStore(path)
    chain = restarted.reprice_chain_for_offer(offer["id"])
    assert D(chain["fixed_landing_rate"]) == D("0.00028")
    rising = signals(best_bid=D("0.0005"), anchor_rate=D("0.0005"))

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), restarted, hub=object())
    status = runtime._repricing_status(rising, now)[0]
    assert D(status["landingRate"]) == D("0.00028")
    assert D(status["currentMarketRate"]) == D("0.0005")
    assert status["landingPolicy"] == "FIXED_AT_CREATION"
    assert runtime._cancel_reprice_candidates(
        {"market": rising},
        {"plan_hash": "fixed", "plan": []},
        now,
        "v3",
    ) == [offer["id"]]
    pending = restarted.reprice_chains(active_only=True)[0]
    assert D(pending["pending_target_rate"]) == _exploration_age_stage_target(
        1,
        pending,
        D("0.0005"),
        ceil_rate_tick(gross_daily_floor(D("0.05"), D("0.15"))),
        5,
        10,
    )
    assert D(pending["pending_target_rate"]) < D(offer["rate"])


def test_new_intents_create_exploration_chains_while_explicit_legacy_intents_stay_legacy(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")

    def create_chain(offer_id, curve=None):
        order = {
            "currency": "USD",
            "slice_key": f"v3:hash:short:balanced:{offer_id}",
            "pool": "short",
            "layer": "balanced",
            "amount": D("150"),
            "submitted_rate": D("0.00022"),
            "effective_rate": D("0.00022"),
            "period": 2,
            "offer_type": "LIMIT",
            "display_type": "LIMIT",
            "flags": 0,
            "strategy_version": "v3",
            "plan_hash": "hash",
        }
        if curve is not None:
            order["pricing_curve_version"] = curve
        created, intent = store.reserve_intent(order, D("1000"))
        assert created
        store.confirm_intent(intent["id"], offer_id)
        return store.ensure_reprice_chain(
            {
                "offer_id": offer_id,
                "rate": D("0.00022"),
                "period": 2,
                "pool": "short",
                "layer": "balanced",
                "mts_created": 1_900_000_000_000,
            },
            "v3",
            1_900_000_000_001,
        )

    assert create_chain(8801)["pricing_curve_version"] == EXACT_TERM_EXPLORATION_CURVE
    assert create_chain(8802, "LEGACY")["pricing_curve_version"] == "LEGACY"


def test_runtime_uses_new_balanced_curve_and_exposes_landing_status(tmp_path):
    now = 1_900_000_000_000
    configured = limit_policy(short_reprice_stages_minutes=(5, 10, 20, 30, 60, 75, 90, 120, 150, 180))
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 8810,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0002400"),
        "rate_real": D("0.0002400"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now - 6 * 60_000,
    }
    order = {
        **intent_order(),
        "slice_key": "v3:hash:short:balanced:0",
        "pool": "short",
        "layer": "balanced",
        "submitted_rate": offer["rate"],
        "effective_rate": offer["rate"],
    }
    _, intent = store.reserve_intent(order, D("1000"))
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    market = build_market_signals_v3([], [], [], configured, now)
    floor = ceil_rate_tick(gross_daily_floor(configured.short_floor_apr, configured.normal_fee_rate))
    expected = _exploration_age_stage_target(1, chain, floor, floor, configured.balanced_landing_stage)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, configured, store, hub=object())
    status = runtime._repricing_status(market, now)[0]
    canceled = runtime._cancel_reprice_candidates(
        {"market": market}, {"plan": [], "plan_hash": "exploration"}, now, "v3"
    )
    pending = store.reprice_chains(active_only=True)[0]

    assert status["curveVersion"] == EXACT_TERM_EXPLORATION_CURVE
    assert D(status["explorationStartRate"]) == offer["rate"]
    assert D(status["landingRate"]) == floor
    assert status["landingStage"] == 5
    assert D(status["nextTargetRate"]) == expected
    assert canceled == [offer["id"]]
    assert D(pending["pending_target_rate"]) == expected


def test_v4_quick_schedule_is_faster_without_accelerating_existing_v3_chains(tmp_path):
    now = 1_900_000_000_000
    configured = limit_policy()
    market = build_market_signals_v3([], [], [], configured, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    def exercise(curve, offer_id):
        store = LendingStateStore(tmp_path / f"quick-{offer_id}.sqlite3")
        offer = {
            "id": offer_id,
            "currency": "USD",
            "amount": D("150"),
            "amount_original": D("150"),
            "rate": D("0.0003000"),
            "rate_real": D("0.0003000"),
            "period": 2,
            "offer_type": "LIMIT",
            "display_type": "LIMIT",
            "flags": 0,
            "status": "ACTIVE",
            "managed": True,
            "pool": "short",
            "layer": "quick",
            "mts_created": now - 16 * 60_000,
        }
        _, intent = store.reserve_intent(
            {
                **intent_order(),
                "slice_key": f"v3:quick:{offer_id}",
                "pool": "short",
                "layer": "quick",
                "submitted_rate": offer["rate"],
                "effective_rate": offer["rate"],
                "fixed_landing_rate": D("0.0002500"),
                "pricing_curve_version": curve,
            },
            D("1000"),
        )
        store.confirm_intent(intent["id"], offer_id)
        store.reconcile_offers([offer], now)
        chain = store.ensure_reprice_chain({**offer, "offer_id": offer_id}, "v3", now)
        store.complete_reprice_stage(chain["chain_key"], 2, now - 1)
        client = Client()
        runtime = LendingRuntimeV3(client, configured, store, hub=object())
        status = runtime._repricing_status(market, now)[0]
        canceled = runtime._cancel_reprice_candidates(
            {"market": market}, {"plan": [], "plan_hash": "quick-v4"}, now, "v3"
        )
        return offer, status, canceled, client

    old_offer, old_status, old_canceled, old_client = exercise("EXACT_TERM_EXPLORATION_V3", 8820)
    new_offer, new_status, new_canceled, new_client = exercise(EXACT_TERM_EXPLORATION_CURVE, 8821)

    assert old_status["nextStageAtMs"] == old_offer["mts_created"] + 20 * 60_000
    assert old_canceled == []
    assert old_client.canceled == []
    assert new_status["nextStageAtMs"] == new_offer["mts_created"] + 15 * 60_000
    assert new_canceled == [new_offer["id"]]
    assert new_client.canceled == [new_offer["id"]]


def test_exploration_status_distinguishes_market_landing_from_hard_floor(tmp_path):
    now = 1_900_000_000_000
    configured = limit_policy()
    store = LendingStateStore(tmp_path / "landing-status.sqlite3")
    offer = {
        "id": 8811,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0002805"),
        "rate_real": D("0.0002805"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now - 61 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(),
            "slice_key": "v3:landing:short:balanced:0",
            "layer": "balanced",
            "submitted_rate": offer["rate"],
            "effective_rate": offer["rate"],
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.complete_reprice_stage(chain["chain_key"], 5, now)
    runtime = LendingRuntimeV3(object(), configured, store, hub=object())

    status = runtime._repricing_status(
        signals(
            anchor_rate=D("0.0002800"),
            windows={"1h": {"median": D("0.0002800")}, "24h": {"q25": D("0.00027"), "q75": D("0.00028")}},
        ),
        now,
    )[0]

    assert status["landingState"] == "SATISFIED_WITHIN_TOLERANCE"
    assert status["floorState"] == "NOT_DUE"
    assert status["stageType"] == "MARKET"
    assert status["nextStageType"] == "FLOOR"


@pytest.mark.parametrize(
    ("runner_demand", "winner_share"),
    [
        ("0.0499", "1"),
        ("0.05", "0.90"),
        ("0.1999", "0.90"),
        ("0.20", "0.75"),
        ("0.3499", "0.75"),
        ("0.35", "0.60"),
    ],
)
def test_v33_term_allocation_curve_boundaries(runner_demand, winner_share):
    runner = D(runner_demand)
    selection = selection_row((2, 7), 2, (D("1") - runner, runner))
    shares = _winner_target_shares(selection)
    assert shares[2] == D(winner_share)
    assert shares.get(7, D("0")) == D("1") - D(winner_share)


def test_global_absolute_demand_prevents_single_candidate_pool_from_looking_dominant():
    now = 1_900_000_000_000
    trades = [
        {"mts": now - index * 1_000, "rate": D("0.0005"), "amount": D("100"), "period": 2} for index in range(100)
    ] + [{"mts": now - index * 1_000, "rate": D("0.0005"), "amount": D("100"), "period": 120} for index in range(2)]
    book = [{"period": period, "rate": D("0.0005"), "amount": D("-100")} for period in (2, 7, 14, 30, 120)]
    by_pool = build_market_signals_v3(book, trades, [], limit_policy(), now)["periodSelection"]["byPool"]
    short_two = next(row for row in by_pool["short"]["scores"] if row["period"] == 2)
    long_120 = by_pool["long"]["scores"][0]

    assert short_two["absoluteDemandShare"] > D("0.95")
    assert long_120["absoluteDemandShare"] < D("0.05")
    assert short_two["totalScore"] > long_120["totalScore"]
    assert by_pool["long"]["belowDemandThreshold"] is True


def test_period_selection_filters_candidates_below_pool_floor():
    now = 1_900_000_000_000
    configured = policy(short_floor_apr=D("0.05"), short_periods=(2, 4, 7))
    book = [
        {"period": 2, "rate": D("0.0005"), "amount": D("-100")},
        {"period": 4, "rate": D("0.0001"), "amount": D("-10000")},
        {"period": 7, "rate": D("0.0005"), "amount": D("-100")},
    ]

    selection = build_market_signals_v3(book, [], [], configured, now)["periodSelection"]["byPool"]["short"]
    rows = {row["period"]: row for row in selection["scores"]}

    assert rows[4]["eligible"] is False
    assert rows[4]["eligibilityReason"] == "MARKET_BELOW_FLOOR"
    assert selection["selectedPeriod"] in {2, 7}


def test_period_selection_blocks_empty_market_and_uses_shorter_tie_break():
    now = 1_900_000_000_000
    empty = build_market_signals_v3([], [], [], policy(), now)["periodSelection"]["byPool"]
    assert {pool: row["selectedPeriod"] for pool, row in empty.items()} == {
        "short": None,
        "medium": None,
        "long": None,
    }
    assert all(row["insufficientMarketData"] for row in empty.values())

    configured = policy(short_periods=(2, 4, 7))
    equal_book = [{"period": period, "rate": D("0.0005"), "amount": D("-100")} for period in (2, 4, 7, 14, 30, 120)]
    tied = build_market_signals_v3(equal_book, [], [], configured, now)["periodSelection"]["byPool"]
    assert tied["short"]["selectedPeriod"] == 2
    assert tied["medium"]["selectedPeriod"] == 14
    assert tied["long"]["selectedPeriod"] == 120


def test_replay_without_historical_book_preserves_cash_and_discloses_missing_evidence():
    now = 1_900_000_000_000
    replay_policy = limit_policy(short_share=D("100"), medium_share=D("0"), long_share=D("0"))
    trades = [
        {
            "id": index,
            "mts": now - 3 * 86_400_000 + index * 900_000,
            "rate": D("0.001"),
            "amount": D("1000"),
            "period": 2 if index % 2 == 0 else 7,
        }
        for index in range(3 * 96)
    ]
    result = replay_strategy_v3(replay_policy, trades, [], D("900"), [], now)
    assert result["mode"] == "REPLAY"
    assert result["fills"] == []
    assert result["returns"] == []
    assert D(result["finalState"]["cash"]) == D("900")
    assert result["marketMode"] == "NO_OBSERVED_BOOK"
    assert D(result["idlePrincipalTime"]) >= 0


def test_state_store_intent_idempotency_and_overallocation_guard():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        created, first = store.reserve_intent(intent_order(), D("200"))
        duplicate, second = store.reserve_intent(intent_order(), D("200"))
        assert created is True and duplicate is False and first["id"] == second["id"]
        with pytest.raises(InsufficientReservedBalance):
            store.reserve_intent(intent_order(index=1, amount="100"), D("200"))


def test_concurrent_duplicate_intent_creates_one_row():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: store.reserve_intent(intent_order(), D("1000"))[0], range(8)))
        assert results.count(True) == 1
        assert len(store.intents()) == 1


def test_period_selection_hold_time_survives_restart(tmp_path):
    path = tmp_path / "state.sqlite3"
    first = LendingStateStore(path)
    initial_scores = [{"period": 4, "totalScore": D("0.50"), "eligible": True}]
    observed = first.observe_period_selection("strategy-a", "short", 4, initial_scores, 1_000)
    assert observed["selectedSinceMs"] == 1_000

    restarted = LendingStateStore(path)
    unchanged = restarted.observe_period_selection("strategy-a", "short", 4, initial_scores, 500_000)
    challenge_scores = [
        {"period": 4, "totalScore": D("0.50"), "eligible": True},
        {"period": 7, "totalScore": D("0.61"), "eligible": True},
    ]
    changed = restarted.observe_period_selection("strategy-a", "short", 7, challenge_scores, 700_000)
    promoted = LendingStateStore(path).observe_period_selection("strategy-a", "short", 7, challenge_scores, 1_300_000)

    assert unchanged["selectedSinceMs"] == 1_000
    assert changed["selectedPeriod"] == 4
    assert changed["challengerSinceMs"] == 700_000
    assert promoted["selectedPeriod"] == 7
    assert promoted["promoted"] is True


def test_saving_identical_active_policy_as_draft_is_a_noop():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        payload = {"version": 3, "enable_limit": True}
        active_id = store.save_strategy(payload, "ACTIVE")
        draft_id = store.save_strategy(payload, "DRAFT")
        assert draft_id == active_id
        assert store.strategy("ACTIVE")["version_id"] == active_id
        assert store.strategy("DRAFT") is None


def test_amount_percent_and_existing_account_exposure_caps_new_plan():
    capped = limit_policy(max_lend_amount=D("3000"), max_lend_percent=D("50"))
    result = build_strategy_plan_v3(
        D("10000"),
        D("4000"),
        {},
        capped,
        signals(),
        "cap",
        existing_exposure={"total": D("2500"), "variable": D("0"), "hidden": D("0")},
    )
    assert result["funding_cap"] == D("3000")
    assert result["cap_remaining"] == D("500")
    assert result["planned_amount"] <= D("500")
    over = build_strategy_plan_v3(
        D("10000"),
        D("1000"),
        {},
        capped,
        signals(),
        "over-cap",
        existing_exposure={"total": D("3500"), "variable": D("0"), "hidden": D("0")},
    )
    assert over["over_cap"] is True
    assert over["plan"] == []


def test_existing_variable_and_hidden_exposure_count_against_account_caps():
    variable_only = policy(
        enable_limit=False,
        enable_frr=False,
        enable_frr_delta_fixed=False,
        enable_frr_delta_variable=True,
        variable_max_share=D("10"),
    )
    result = build_strategy_plan_v3(
        D("10000"),
        D("1000"),
        {},
        variable_only,
        signals(),
        "existing-variable",
        existing_exposure={"total": D("1000"), "variable": D("1000"), "hidden": D("0")},
    )
    assert result["plan"] == []


def test_pending_limit_switch_never_submits_old_frr_plan_and_waits_for_confirmation():
    now = 1_900_000_000_000

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    class Hub:
        fallback_ms = 300_000
        rest_stale_ms = 60_000

        def __init__(self):
            self.offers = [
                {
                    "id": 99,
                    "currency": "USD",
                    "amount": D("200"),
                    "amount_original": D("200"),
                    "rate": D("0"),
                    "rate_real": D("0.00035"),
                    "period": 2,
                    "offer_type": "FRRDELTAVAR",
                    "display_type": "FRR",
                    "flags": 0,
                    "status": "ACTIVE",
                    "managed": True,
                    "pool": "short",
                    "layer": "quick",
                    "mts_created": now - 60_000,
                }
            ]

        def snapshot(self, at):
            return {
                "as_of": at,
                "source": "WEBSOCKET",
                "safeRequired": False,
                "book": [
                    {"rate": D("0.0004"), "period": 2, "count": 1, "amount": D("5000")},
                    {"rate": D("0.0003"), "period": 2, "count": 1, "amount": D("-5000")},
                    {"rate": D("0.0004"), "period": 7, "count": 1, "amount": D("5000")},
                    {"rate": D("0.0003"), "period": 7, "count": 1, "amount": D("-5000")},
                ],
                "trades": [
                    {"id": 1, "mts": at, "rate": D("0.0004"), "amount": D("1000"), "period": 2},
                    {"id": 2, "mts": at, "rate": D("0.0004"), "amount": D("1000"), "period": 7},
                ],
                "wallets": [
                    {
                        "wallet_type": "funding",
                        "currency": "USD",
                        "balance": D("1000"),
                        "available": D("800") if self.offers else D("1000"),
                    }
                ],
                "offers": list(self.offers),
                "credits": [],
                "fundingTrades": [],
            }

        def stop(self):
            pass

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        old = policy()
        new = limit_policy()
        old_id = store.save_strategy(json_decimal(old.__dict__), "ACTIVE")
        new_id = store.save_strategy(json_decimal(new.__dict__), "PENDING")
        order = {
            **intent_order(amount="200"),
            "offer_type": "FRRDELTAVAR",
            "display_type": "FRR",
            "submitted_rate": D("0"),
            "effective_rate": D("0.00035"),
            "strategy_version": old_id,
        }
        _, intent = store.reserve_intent(order, D("1000"))
        store.confirm_intent(intent["id"], 99)
        store.reconcile_offers(
            [
                {
                    "id": 99,
                    "currency": "USD",
                    "amount": D("200"),
                    "amount_original": D("200"),
                    "rate": D("0"),
                    "rate_real": D("0.00035"),
                    "period": 2,
                    "offer_type": "FRRDELTAVAR",
                    "display_type": "FRR",
                    "flags": 0,
                    "status": "ACTIVE",
                    "managed": True,
                    "pool": "short",
                    "layer": "quick",
                    "mts_created": now - 60_000,
                }
            ],
            now,
        )
        store.set_mode("LIVE", "test")
        client = Client()
        hub = Hub()
        runtime = LendingRuntimeV3(client, old, store, hub=hub)
        runtime._bootstrapped = True
        runtime._last_rest_sync_ms = now
        submitted_types = []
        runtime._submit_plan = lambda result, wallet, version: (
            submitted_types.extend(row["display_type"] for row in result["plan"]) or []
        )

        first = runtime.cycle(now)
        assert client.canceled == [99]
        assert submitted_types == []
        assert store.strategy("PENDING")["version_id"] == new_id
        assert first["strategyV3"]["submitted"] == []

        runtime.cycle(now + 1)
        assert client.canceled == [99]
        assert submitted_types == []

        store.reconcile_offers([], now + 2)
        hub.offers = []
        runtime.cycle(now + 2)
        assert store.strategy("ACTIVE")["version_id"] == new_id
        assert submitted_types and set(submitted_types) == {"LIMIT"}


def test_closed_slice_gets_new_generation_while_open_slice_remains_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        base = "v3:short:quick:0"
        assert store.replenishment_slice_key(base) == base
        _, intent = store.reserve_intent(intent_order(), D("1000"))
        assert store.replenishment_slice_key(base) == base
        store.confirm_intent(intent["id"], 77)
        store.reconcile_offers([], 2000)
        assert store.replenishment_slice_key(base) == base + ":r1"
        replacement = {**intent_order(), "slice_key": base + ":r1"}
        created, _ = store.reserve_intent(replacement, D("1000"))
        assert created is True


def test_age_reprice_chain_preserves_elapsed_time_across_replacement_and_restart(tmp_path):
    now = 1_900_000_000_000
    state_path = tmp_path / "state.sqlite3"
    store = LendingStateStore(state_path)
    strategy = "v3"
    offer = {
        "id": 701,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0006"),
        "rate_real": D("0.0006"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 10 * 60_000,
    }
    legacy_order = {
        **intent_order(),
        "slice_key": "old:short:quick:0",
        "strategy_version": "old",
        "pricing_curve_version": "EXACT_TERM_EXPLORATION_V3",
    }
    _, intent = store.reserve_intent(legacy_order, D("1000"))
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    plan = {
        "plan_hash": "plan",
        # A fully allocated account has no new-order plan. Existing offers must
        # still age through repricing using the deterministic market benchmark.
        "plan": [],
    }
    market = signals(best_bid=D("0.0003"), anchor_rate=D("0.0003"))
    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    canceled = runtime._cancel_reprice_candidates({"market": market}, plan, now, strategy)
    assert canceled == [701]
    chain = store.reprice_chains(active_only=True)[0]
    assert chain["current_stage"] == 1
    assert D(chain["pending_target_rate"]) == D("0.00054")
    assert chain["started_at_ms"] == offer["mts_created"]

    store.reconcile_offers([], now + 1)
    replacement_order = {
        **intent_order(),
        "slice_key": "v3:short:quick:0:r1",
        "submitted_rate": D("0.00054"),
        "effective_rate": D("0.00054"),
    }
    _, replacement_intent = store.reserve_intent(replacement_order, D("1000"))
    store.confirm_intent(replacement_intent["id"], 702)
    store.bind_reprice_replacement(
        "v3:short:quick:0",
        strategy,
        702,
        D("0.00054"),
        now_ms=now + 2,
    )
    replacement_offer = {
        **offer,
        "id": 702,
        "rate": D("0.00054"),
        "rate_real": D("0.00054"),
        "mts_created": now + 2,
    }
    store.reconcile_offers([replacement_offer], now + 2)

    restarted_store = LendingStateStore(state_path)
    restarted_chain = restarted_store.reprice_chain_for_offer(702)
    assert restarted_chain["started_at_ms"] == offer["mts_created"]
    assert restarted_chain["current_stage"] == 1
    second_client = Client()
    restarted_runtime = LendingRuntimeV3(second_client, limit_policy(), restarted_store, hub=object())
    second = restarted_runtime._cancel_reprice_candidates(
        {"market": market},
        plan,
        now + 30 * 60_000,
        strategy,
    )
    assert second == [702]
    assert D(restarted_store.reprice_chains(active_only=True)[0]["pending_target_rate"]) == D("0.00048")


def test_existing_quick_offer_waits_for_configured_stage_and_never_uses_early_correction(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "period-correction.sqlite3")
    configured = limit_policy(short_floor_apr=D("0.065"))
    offer = {
        "id": 711,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0006"),
        "rate_real": D("0.0006"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 10 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(),
            "submitted_rate": D("0.0006"),
            "effective_rate": D("0.0006"),
            "pricing_curve_version": "EXACT_TERM_EXPLORATION_V3",
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    market = build_market_signals_v3(
        [{"period": 2, "rate": D("0.00015"), "amount": D("-1000")}],
        [{"mts": now, "rate": D("0.00024"), "amount": D("1000"), "period": 2}],
        [],
        configured,
        now,
    )
    client = Client()
    runtime = LendingRuntimeV3(client, configured, store, hub=object())

    canceled = runtime._cancel_reprice_candidates(
        {"market": market}, {"plan": [], "plan_hash": "period-aware"}, now, "v3"
    )

    floor = ceil_rate_tick(gross_daily_floor(configured.short_floor_apr, configured.normal_fee_rate))
    chain = store.reprice_chains(active_only=True)[0]
    assert canceled == [offer["id"]]
    assert client.canceled == [offer["id"]]
    assert chain["pending_action"] == "AGE_STAGE"
    assert chain["current_stage"] == 1
    assert D(chain["fixed_landing_rate"]) == floor
    assert D(chain["pending_target_rate"]) == _exploration_age_stage_target(
        1,
        chain,
        floor,
        floor,
        configured.balanced_landing_stage,
        len(configured.short_reprice_stages_minutes),
    )


def test_period_switch_requires_20_percent_advantage_without_canceling_old_offer(tmp_path):
    now = 1_900_000_000_000
    state_path = tmp_path / "state.sqlite3"
    store = LendingStateStore(state_path)
    strategy = "period-strategy"
    offer = {
        "id": 801,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0005"),
        "rate_real": D("0.0005"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now - 11 * 60_000,
    }
    order = {**intent_order(), "strategy_version": strategy, "slice_key": "period:short:balanced:0"}
    _, intent = store.reserve_intent(order, D("1000"))
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    store.observe_period_selection(
        strategy,
        "short",
        2,
        [
            {"period": 2, "totalScore": D("0.50"), "eligible": True},
            {"period": 4, "totalScore": D("0.40"), "eligible": True},
        ],
        now - 60_000,
    )

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    configured = limit_policy(
        short_reprice_stages_minutes=(60, 120, 180, 240, 360, 720),
    )
    period_selection = {
        "basis": "test",
        "byPool": {
            "short": {
                "selectedPeriod": 4,
                "scores": [
                    {"period": 2, "totalScore": D("0.50"), "eligible": True},
                    {"period": 4, "totalScore": D("0.61"), "eligible": True},
                ],
            }
        },
    }
    market = signals(periodSelection=period_selection)
    runtime = LendingRuntimeV3(Client(), configured, store, hub=object())
    runtime._persist_period_selection(market, strategy, now)

    assert runtime._cancel_reprice_candidates({"market": market}, {"plan": [], "plan_hash": "p"}, now, strategy) == []
    assert market["periodSelection"]["byPool"]["short"]["selectionMature"] is False

    restarted_store = LendingStateStore(state_path)
    client = Client()
    restarted = LendingRuntimeV3(client, configured, restarted_store, hub=object())
    restarted._persist_period_selection(market, strategy, now + 10 * 60_000)
    canceled = restarted._cancel_reprice_candidates(
        {"market": market}, {"plan": [], "plan_hash": "p"}, now + 10 * 60_000, strategy
    )

    assert market["periodSelection"]["byPool"]["short"]["selectionMature"] is True
    assert market["periodSelection"]["byPool"]["short"]["selectedPeriod"] == 4
    assert canceled == []
    assert client.canceled == []
    assert restarted_store.reprice_chains(active_only=True)[0]["pending_action"] is None


def test_stage_three_skip_records_actual_rate_as_market_anchor(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 751,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 60 * 60_000,
    }
    _, intent = store.reserve_intent(
        {**intent_order(), "pricing_curve_version": "LEGACY"},
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.complete_reprice_stage(chain["chain_key"], 2, now - 1)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    canceled = runtime._cancel_reprice_candidates(
        {"market": signals(best_bid=D("0.0004002"), anchor_rate=D("0.0004002"))},
        {"plan_hash": "skip-stage-three", "plan": []},
        now,
        "v3",
    )
    updated = store.reprice_chain_for_offer(offer["id"])
    assert canceled == []
    assert client.canceled == []
    assert updated["current_stage"] == 3
    assert D(updated["market_anchor_rate"]) == D("0.0004")


def test_legacy_stage_three_chain_uses_current_offer_as_floor_phase_anchor(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 752,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0006"),
        "rate_real": D("0.0006"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 90 * 60_000,
    }
    _, intent = store.reserve_intent(
        {**intent_order(), "pricing_curve_version": "LEGACY"},
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.complete_reprice_stage(chain["chain_key"], 3, now - 1)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    runtime = LendingRuntimeV3(Client(), legacy_six_stage_policy(), store, hub=object())
    market = signals(best_bid=D("0.0003"), anchor_rate=D("0.0003"))
    assert runtime._cancel_reprice_candidates(
        {"market": market},
        {"plan_hash": "legacy-floor-stage", "plan": []},
        now,
        "v3",
    ) == [offer["id"]]
    pending = store.reprice_chains(active_only=True)[0]
    floor = ceil_rate_tick(gross_daily_floor(D("0.05"), D("0.15")))
    expected = _age_stage_target(4, pending, D("0.0002999"), floor, D("0.0006"))
    assert pending["current_stage"] == 4
    assert D(pending["market_anchor_rate"]) == D("0.0006")
    assert D(pending["pending_target_rate"]) == expected


def test_stage_six_respects_minimum_change_threshold(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 753,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003001"),
        "rate_real": D("0.0003001"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 180 * 60_000,
    }
    _, intent = store.reserve_intent(
        {**intent_order(), "pricing_curve_version": "EXACT_TERM_EXPLORATION_V3"}, D("1000")
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.complete_reprice_stage(chain["chain_key"], 5, now - 1, market_anchor_rate=D("0.0004"))

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    client = Client()
    exact_floor_policy = legacy_six_stage_policy(short_floor_apr=D("0.093075"))
    runtime = LendingRuntimeV3(client, exact_floor_policy, store, hub=object())
    assert (
        runtime._cancel_reprice_candidates(
            {"market": signals(best_bid=D("0.0003"), anchor_rate=D("0.0003"))},
            {"plan_hash": "stage-six-threshold", "plan": []},
            now,
            "v3",
        )
        == []
    )
    updated = store.reprice_chain_for_offer(offer["id"])
    assert client.canceled == []
    assert updated["current_stage"] == 6
    assert D(updated["market_anchor_rate"]) == D("0.0004")
    status = runtime._repricing_status(signals(best_bid=D("0.0003")), now)[0]
    assert status["stageType"] == "FLOOR"
    assert status["nextStageType"] is None
    assert D(status["marketAnchorRate"]) == D("0.0004")
    assert status["floorState"] == "SATISFIED_WITHIN_TOLERANCE"


@pytest.mark.parametrize("completed_stage", [5, 6])
def test_final_floor_reprice_ignores_dynamic_threshold_and_self_heals(tmp_path, completed_stage):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / f"stage-{completed_stage}.sqlite3")
    offer_id = 754 + completed_stage
    offer = {
        "id": offer_id,
        "currency": "USD",
        "amount": D("218.34722822"),
        "amount_original": D("218.34722822"),
        "rate": D("0.0002806"),
        "rate_real": D("0.0002806"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 180 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(amount="218.34722822"),
            "pricing_curve_version": "EXACT_TERM_EXPLORATION_V3",
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer_id)
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer_id}, "v3", now)
    store.complete_reprice_stage(
        chain["chain_key"],
        completed_stage,
        now - 1,
        market_anchor_rate=D("0.0002806"),
    )

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, value):
            self.canceled.append(value)
            return [0, "SUCCESS"]

    configured = legacy_six_stage_policy(
        short_floor_apr=D("0.0792"),
        minimum_rate_change=D("0.00002"),
        max_reprices_per_hour=0,
        reprice_cooldown_minutes=60,
    )
    market = signals(
        best_bid=D("0.0002809"),
        anchor_rate=D("0.0002809"),
        trend_threshold=D("0.00003"),
    )
    client = Client()
    runtime = LendingRuntimeV3(client, configured, store, hub=object())
    store.record_reprice(
        offer_id,
        "ordinary_stage_immediately_before_final",
        D("0.0003"),
        D("0.00029"),
        created_at_ms=now - 1,
    )

    assert runtime._repricing_status(market, now)[0]["floorState"] == "REPRICE_REQUIRED"
    assert runtime._cancel_reprice_candidates(
        {"market": market},
        {"plan_hash": "force-final-floor", "plan": []},
        now,
        "v3",
    ) == [offer_id]
    pending = store.reprice_chains(active_only=True)[0]
    assert client.canceled == [offer_id]
    assert pending["current_stage"] == 6
    assert pending["pending_action"] == "AGE_STAGE"
    assert D(pending["pending_target_rate"]) == D("0.0002553")


def test_partial_offer_below_exchange_minimum_is_not_canceled_and_is_reported(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "partial-reprice.sqlite3")
    offer = {
        "id": 7901,
        "currency": "USD",
        "amount": D("99.50"),
        "amount_original": D("150"),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now - 180 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(amount="150"),
            "layer": "balanced",
            "submitted_rate": D("0.0004"),
            "effective_rate": D("0.0004"),
            "fixed_landing_rate": D("0.00028"),
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    market = signals(best_bid=D("0.00028"), anchor_rate=D("0.00028"))
    assert runtime._cancel_reprice_candidates(
        {"market": market},
        {"plan_hash": "partial", "plan": []},
        now,
        "v3",
    ) == []
    status = runtime._repricing_status(market, now)[0]
    assert client.canceled == []
    assert status["floorState"] == "BELOW_REPOST_MINIMUM"
    assert status["repriceBlockedReason"] == "BELOW_REPOST_MINIMUM"


def test_market_rise_does_not_raise_or_reset_existing_chain(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 801,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 20 * 60_000,
    }
    _, intent = store.reserve_intent(intent_order(), D("1000"))
    store.confirm_intent(intent["id"], 801)
    store.reconcile_offers([offer], now)

    class Client:
        api_key = ""
        api_secret = ""

        def cancel_funding_offer(self, offer_id):
            return [0, "SUCCESS"]

    plan = {
        "plan_hash": "rise",
        "plan": [
            {
                **intent_order(),
                "slice_index": 0,
                "display_type": "LIMIT",
                "effective_rate": D("0.0005"),
                "submitted_rate": D("0.0005"),
                "target_rate": D("0.0005"),
                "gross_daily_floor": gross_daily_floor(D("0.05"), D("0.15")),
            }
        ],
    }
    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())
    assert runtime._cancel_reprice_candidates(
        {"market": signals(best_bid=D("0.0005"), anchor_rate=D("0.0005"))},
        plan,
        now,
        "v3",
    ) == []
    chain = store.reprice_chain_for_offer(801)
    assert chain["pending_action"] is None
    assert chain["current_stage"] == 1
    assert chain["started_at_ms"] == offer["mts_created"]
    assert D(chain["origin_rate"]) == D("0.0003")
    assert D(chain["fixed_landing_rate"]) == D("0.0003")


def test_pending_stage_target_is_used_for_replacement_submission(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 901,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0006"),
        "rate_real": D("0.0006"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 10 * 60_000,
    }
    _, intent = store.reserve_intent(intent_order(), D("1000"))
    store.confirm_intent(intent["id"], 901)
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": 901}, "new", now)
    store.mark_reprice_pending(
        chain["chain_key"],
        "AGE_STAGE",
        D("0.0005"),
        stage=1,
        now_ms=now,
        source_offer_id=offer["id"],
    )
    store.reconcile_offers([], now + 1)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.rate = None
            self.amount = None
            self.period = None

        def submit_funding_offer(self, _symbol, amount, rate, period, _offer_type, flags=0):
            self.amount = D(amount)
            self.rate = D(rate)
            self.period = int(period)
            return [0, "on-req", None, None, [902]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    submitted, available = runtime._submit_pending_reprices(D("150"), "new")
    assert client.rate == D("0.0005")
    assert client.amount == offer["amount"]
    assert client.period == offer["period"]
    assert submitted[0]["effective_rate"] == D("0.0005")
    assert available == 0
    rebound = store.reprice_chain_for_offer(902)
    assert rebound["current_stage"] == 1
    assert rebound["started_at_ms"] == offer["mts_created"]


def test_visible_limit_reprice_absorbs_sub_150_surplus_and_preserves_chain(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "reprice-remainder.sqlite3", clock=lambda: now / 1000)
    offer = {
        "id": 910,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": 120,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "long",
        "layer": "high",
        "mts_created": now - 360 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(),
            "slice_key": "v3:long:high:0",
            "pool": "long",
            "layer": "high",
            "period": 120,
            "fixed_landing_rate": D("0.00031"),
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    chain = store.complete_reprice_stage(chain["chain_key"], 3, now - 1, market_anchor_rate=D("0.00035"))
    store.mark_reprice_pending(
        chain["chain_key"],
        "AGE_STAGE",
        D("0.00035"),
        stage=3,
        now_ms=now,
        market_anchor_rate=D("0.00035"),
        source_offer_id=offer["id"],
    )
    store.reconcile_offers([], now + 1)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.amount = None

        def submit_funding_offer(self, _symbol, amount, *_args, **_kwargs):
            self.amount = D(amount)
            return [0, "on-req", None, None, [911]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    submitted, available = runtime._submit_pending_reprices(
        D("150.41832433"),
        "v3",
        D("150.41832433"),
    )
    rebound = store.reprice_chain_for_offer(911)

    assert client.amount == D("150.41832433")
    assert submitted[0]["absorbed_remainder"] == D("0.41832433")
    assert submitted[0]["remainder_absorption_pool"] == "long"
    assert available == 0
    assert rebound["current_stage"] == 3
    assert rebound["started_at_ms"] == chain["started_at_ms"]
    assert D(rebound["market_anchor_rate"]) == D("0.00035")
    assert D(rebound["fixed_landing_rate"]) == D("0.00031")


@pytest.mark.parametrize(("offer_type", "display_type", "offset"), [
    ("FRRDELTAFIX", "FRR_DELTA_FIXED", "0"),
    ("FRRDELTAFIX", "FRR_DELTA_FIXED", "0.0000002"),
    ("FRRDELTAVAR", "FRR", "0"),
    ("FRRDELTAVAR", "FRR_DELTA_VARIABLE", "0.0000002"),
])
def test_frr_offer_floor_uses_effective_rate_in_live_and_preflight(tmp_path, offer_type, display_type, offset):
    now = 1_900_000_000_000
    configured = policy(enable_frr=True, enable_frr_delta_fixed=True, enable_frr_delta_variable=True,
                        short_floor_apr=D("0.06"), normal_fee_rate=D("0.147"))
    offer = {"id": 9001, "currency": "USD", "amount": D("150"), "rate": D(offset), "rate_real": None,
             "offer_type": offer_type, "display_type": display_type, "period": 2, "managed": True}
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.reconcile_offers([offer], now)
    runtime = LendingRuntimeV3(object(), configured, store, hub=object())
    account = {"existingExposure": {"total": D("150")}}
    result = {"funding_cap": D("1000")}
    for frr, expected in [(D("0.00029930"), []), (None, []), (D("0.00010"), ["below_new_floor"])]:
        assert lendingbot.v3_offer_violations(offer, configured, frr) == expected
        adjustments = runtime._pending_adjustments({"frr_daily_rate": frr}, account, configured, result, now)
        assert [reason for _, _, reason in adjustments] == expected
    assert lendingbot.v3_offer_violations(dict(offer, rate_real=D("0.00015")), configured, D("0.00030")) == [
        "below_new_floor"
    ]


@pytest.mark.parametrize("offset", [D("0"), D("0.0001")])
def test_fixed_delta_reprices_on_schedule_only_when_offset_can_decrease(tmp_path, offset):
    started = 1_900_000_000_000
    frr = D("0.00029930")
    configured = policy(enable_frr_delta_fixed=True, minimum_offer_minutes=1)
    stages = configured.reprice_stages("short", "quick", EXACT_TERM_EXPLORATION_CURVE)
    due = started + stages[0] * 60_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: due / 1000)
    order = dict(intent_order(), submitted_rate=offset, effective_rate=frr + offset,
                 offer_type="FRRDELTAFIX", display_type="FRR_DELTA_FIXED",
                 fixed_landing_rate=D("0.00023"), pricing_curve_version=EXACT_TERM_EXPLORATION_CURVE)
    _, intent = store.reserve_intent(order, D("1000"))
    store.confirm_intent(intent["id"], 9100)
    store.reconcile_offers([dict(order, id=9100, rate=offset, rate_real=None, mts_created=started)], started)

    class Client:
        api_key = api_secret = ""

        def __init__(self):
            self.canceled = []
            self.submitted = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

        def submit_funding_offer(self, symbol, amount, rate, period, offer_type, flags=0):
            self.submitted.append((D(rate), period, offer_type))
            return [0, "on-req", None, None, [9101]]

    client = Client()
    runtime = LendingRuntimeV3(client, configured, store, hub=object(), clock=lambda: due / 1000)
    market = {"market": signals(frr_daily_rate=frr)}
    plan = {"plan": [{"pool": "short", "layer": "quick", "display_type": "LIMIT", "period": 2, "flags": 0}]}
    assert runtime._cancel_reprice_candidates(market, plan, due - 1, "v3") == []
    chain = store.reprice_chain_for_offer(9100)
    assert D(chain["origin_rate"]) == frr + offset
    assert chain["current_stage"] == 0

    if offset == 0:
        for minutes in stages:
            assert runtime._cancel_reprice_candidates(market, plan, started + minutes * 60_000, "v3") == []
        assert client.canceled == []
        assert store.reprice_chain_for_offer(9100)["current_stage"] == len(stages)
        return

    assert runtime._cancel_reprice_candidates(market, plan, due, "v3") == [9100]
    store.reconcile_offers([], due + 1)
    assert runtime._submit_pending_reprices(D("150"), "v3") == ([], D("0"))
    new_frr = D("0.00030500")
    submitted, available = runtime._submit_pending_reprices(D("150"), "v3", frr_daily_rate=new_frr)
    new_offset, period, offer_type = client.submitted[0]
    assert 0 <= new_offset < offset
    assert (period, offer_type, available) == (2, "FRRDELTAFIX", 0)
    assert submitted[0]["effective_rate"] == new_frr + new_offset
    assert submitted[0]["effective_rate"] >= submitted[0]["gross_daily_floor"]
    replacement_chain = store.reprice_chain_for_offer(9101)
    assert (replacement_chain["current_stage"], replacement_chain["started_at_ms"]) == (1, started)


def test_shape_replacement_never_raises_and_preserves_chain_timing_and_landing(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "shape.sqlite3")
    offer = {
        "id": 920,
        "currency": "USD",
        "amount": D("175"),
        "amount_original": D("175"),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": 30,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "medium",
        "layer": "high",
        "mts_created": now - 120 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(amount="175"),
            "slice_key": "v3:medium:high:0",
            "pool": "medium",
            "layer": "high",
            "period": 30,
            "submitted_rate": D("0.0004"),
            "effective_rate": D("0.0004"),
            "fixed_landing_rate": D("0.00031"),
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.complete_reprice_stage(chain["chain_key"], 2, now - 1)
    store.mark_reprice_pending(
        chain["chain_key"],
        "SHAPE_CHANGE",
        D("0.0005"),
        now_ms=now,
        source_offer_id=offer["id"],
    )
    store.reconcile_offers([], now + 1)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.submitted = None

        def submit_funding_offer(self, _symbol, amount, rate, period, offer_type, flags=0):
            self.submitted = (D(amount), D(rate), int(period), offer_type, int(flags))
            return [0, "on-req", None, None, [921]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    submitted, available = runtime._submit_pending_reprices(D("175"), "v3")
    rebound = store.reprice_chain_for_offer(921)

    assert submitted[0]["effective_rate"] == D("0.0004")
    assert client.submitted[:3] == (D("175"), D("0.0004"), 30)
    assert available == 0
    assert rebound["current_stage"] == 2
    assert rebound["started_at_ms"] == offer["mts_created"]
    assert D(rebound["origin_rate"]) == D("0.0004")
    assert D(rebound["fixed_landing_rate"]) == D("0.00031")


def test_stage_three_replacement_preserves_market_anchor(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 903,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0006"),
        "rate_real": D("0.0006"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 60 * 60_000,
    }
    _, intent = store.reserve_intent(intent_order(), D("1000"))
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "v3", now)
    store.mark_reprice_pending(
        chain["chain_key"],
        "AGE_STAGE",
        D("0.00045"),
        stage=3,
        now_ms=now,
        market_anchor_rate=D("0.00045"),
    )
    store.bind_reprice_replacement(
        "v3:short:quick:0",
        "v3",
        904,
        D("0.00045"),
        now_ms=now + 1,
    )
    rebound = store.reprice_chain_for_offer(904)
    assert rebound["current_stage"] == 3
    assert D(rebound["market_anchor_rate"]) == D("0.00045")
    assert rebound["started_at_ms"] == offer["mts_created"]


def test_historical_strategy_slices_with_same_index_keep_distinct_chains(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    for index, offer_id in enumerate((1001, 1002)):
        amount = D("150") + index
        order = {
            **intent_order(amount=format(amount, "f")),
            "slice_key": "old:short:quick:0",
            "strategy_version": "old",
        }
        _, intent = store.reserve_intent(order, D("1000"))
        store.confirm_intent(intent["id"], offer_id)
        offer = {
            "id": offer_id,
            "currency": "USD",
            "amount": amount,
            "amount_original": amount,
            "rate": D("0.0004"),
            "rate_real": D("0.0004"),
            "period": 2,
            "offer_type": "LIMIT",
            "display_type": "LIMIT",
            "flags": 0,
            "status": "ACTIVE",
            "managed": True,
            "pool": "short",
            "layer": "quick",
            "mts_created": now,
        }
        store.reconcile_offers([offer], now)
        store.ensure_reprice_chain({**offer, "offer_id": offer_id}, "new-active", now)
    chains = store.reprice_chains(active_only=True)
    assert len(chains) == 2
    assert len({row["chain_key"] for row in chains}) == 2
    assert {row["current_offer_id"] for row in chains} == {1001, 1002}


def test_schema_normalization_preserves_active_reprice_chain_state(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    old_payload = json_decimal(limit_policy().__dict__)
    old_version = store.save_strategy(old_payload, "ACTIVE")
    offer = {
        "id": 1101,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.00045"),
        "rate_real": D("0.00045"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 75 * 60_000,
    }
    _, intent = store.reserve_intent(intent_order(), D("1000"))
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, old_version, now)
    store.complete_reprice_stage(
        chain["chain_key"],
        3,
        now_ms=now,
        market_anchor_rate=D("0.00045"),
    )

    new_payload = {**old_payload, "market_data_retention_days": 91}
    new_version = store.normalize_active_strategy(new_payload)
    inherited = store.ensure_reprice_chain(
        {**offer, "offer_id": offer["id"]},
        new_version,
        now + 1,
    )

    assert inherited["strategy_version"] == new_version
    assert inherited["current_stage"] == 3
    assert inherited["started_at_ms"] == offer["mts_created"]
    assert D(inherited["origin_rate"]) == D("0.00045")
    assert D(inherited["market_anchor_rate"]) == D("0.00045")

    with store.transaction(immediate=True) as connection:
        connection.execute(
            "DELETE FROM reprice_chains WHERE strategy_version=?",
            (new_version,),
        )
    assert store.repair_normalized_reprice_chains(new_version, now_ms=now + 2) == 1
    repaired = [row for row in store.reprice_chains(active_only=True) if row["strategy_version"] == new_version][0]
    assert repaired["current_stage"] == 3
    assert repaired["started_at_ms"] == offer["mts_created"]
    assert D(repaired["market_anchor_rate"]) == D("0.00045")


def test_strategy_activation_maps_completed_six_stage_chain_to_ten_stages(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    legacy = replace(
        limit_policy(),
        short_reprice_stages_minutes=(10, 30, 60, 90, 120, 180),
        medium_reprice_stages_minutes=(20, 60, 120, 180, 240, 360),
        long_reprice_stages_minutes=(60, 180, 360, 480, 720, 1440),
    )
    old_version = store.save_strategy(json_decimal(legacy.__dict__), "ACTIVE")
    new_version = store.save_strategy(json_decimal(limit_policy().__dict__), "PENDING")
    offer = {
        "id": 1201,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "quick",
        "mts_created": now - 180 * 60_000,
    }
    _, intent = store.reserve_intent(
        {**intent_order(), "pricing_curve_version": "LEGACY"},
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    old_chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, old_version, now)
    store.complete_reprice_stage(old_chain["chain_key"], 6, now, market_anchor_rate=D("0.0004"))

    inherited = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, new_version, now + 1)

    assert inherited["current_stage"] == 10
    assert inherited["started_at_ms"] == offer["mts_created"]
    assert D(inherited["market_anchor_rate"]) == D("0.0004")


def test_new_strategy_chain_recovers_latest_predecessor_for_existing_offer(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 1102,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": 8,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "medium",
        "layer": "quick",
        "mts_created": now - 180 * 60_000,
    }
    _, intent = store.reserve_intent(
        {
            **intent_order(),
            "slice_key": "v3:medium:quick:0",
            "pool": "medium",
            "period": 8,
        },
        D("1000"),
    )
    store.confirm_intent(intent["id"], offer["id"])
    store.reconcile_offers([offer], now)
    old_chain = store.ensure_reprice_chain({**offer, "offer_id": offer["id"]}, "old", now)
    store.complete_reprice_stage(
        old_chain["chain_key"],
        3,
        now_ms=now,
        market_anchor_rate=D("0.0004"),
    )

    inherited = store.ensure_reprice_chain(
        {**offer, "offer_id": offer["id"]},
        "already-normalized",
        now + 1,
    )

    assert inherited["strategy_version"] == "already-normalized"
    assert inherited["current_stage"] == 3
    assert inherited["started_at_ms"] == offer["mts_created"]
    assert D(inherited["market_anchor_rate"]) == D("0.0004")


def test_ambiguous_resolution_continues_read_only_recovery_and_returns_paused():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.set_mode("LIVE", "test")
        _, intent = store.reserve_intent(intent_order(), D("1000"))
        store.mark_submitting(intent["id"])
        store.mark_ambiguous(intent["id"], "timeout")
        assert store.runtime()["mode"] == "PAUSED" and store.runtime()["safe_manual"] == 0
        store.enter_protected_pause("MARKET_DATA_STALE")
        assert store.runtime()["safe_manual"] == 0
        assert store.recovery_status()["targetMode"] == "LIVE"
        result = store.resolve_ambiguous_intent(intent["id"], close=True)
        assert result["intent"]["state"] == "CLOSED"
        assert result["runtime"]["mode"] == "PAUSED"


def test_safe_recovers_only_after_two_consistent_samples_thirty_seconds_apart():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.set_mode("LIVE", "test")
        store.enter_protected_pause("MARKET_DATA_STALE")
        store.record_consistent_sync(1_000_000)
        store.record_consistent_sync(1_020_000)
        assert store.runtime()["mode"] == "PAUSED"
        store.record_consistent_sync(1_030_000)
        assert store.runtime()["mode"] == "LIVE"


def test_unknown_runtime_pause_reason_never_auto_resumes_live():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.set_mode("LIVE", "test")
        store.enter_protected_pause("UNEXPECTED_RUNTIME_ERROR:TypeError")
        store.record_consistent_sync(1_000_000)
        store.record_consistent_sync(1_030_000)
        assert store.runtime()["mode"] == "PAUSED"


def test_websocket_snapshot_increment_and_five_minute_safe_boundary():
    hub = BitfinexMarketDataHub(fallback_seconds=300, rest_stale_seconds=60)
    hub.handle_public_message({"event": "subscribed", "chanId": 7, "channel": "book"})
    hub.handle_public_message([7, [["0.0003", 2, 1, "500"], ["0.0004", 7, 1, "-200"]]])
    assert len(hub.snapshot()["book"]) == 2
    hub.handle_public_message([7, ["0.0003", 2, 0, "1"]])
    assert len(hub.snapshot()["book"]) == 1
    now = int(time.time() * 1000)
    hub._set_connected("public", False)
    hub.apply_rest_snapshot(book=[], synced_at_ms=now)
    hub._public_disconnected_since_ms = now - 299_000
    assert hub.snapshot(now)["safeRequired"] is False
    hub._public_disconnected_since_ms = now - 300_000
    assert hub.snapshot(now)["safeRequired"] is True


def test_market_trade_snapshots_merge_by_id_and_keep_newest_records():
    hub = BitfinexMarketDataHub(max_trades=3)
    hub.apply_rest_snapshot(
        trades=[
            {"id": "1", "mts": 1000, "amount": "10", "rate": "0.0001", "period": 2},
            {"id": "2", "mts": 2000, "amount": "20", "rate": "0.0002", "period": 2},
        ]
    )
    hub.apply_rest_snapshot(
        trades=[
            {"id": "2", "mts": 2000, "amount": "25", "rate": "0.00025", "period": 2},
            {"id": "3", "mts": 3000, "amount": "30", "rate": "0.0003", "period": 2},
            {"id": "4", "mts": 4000, "amount": "40", "rate": "0.0004", "period": 2},
        ]
    )
    trades = hub.snapshot(4000)["trades"]
    assert [row["id"] for row in trades] == ["2", "3", "4"]
    assert trades[0]["amount"] == D("25")


def test_websocket_and_rest_trade_overlap_is_not_double_counted():
    hub = BitfinexMarketDataHub(max_trades=10)
    hub.apply_rest_snapshot(
        trades=[
            {"id": "9", "mts": 9000, "amount": "9", "rate": "0.0009", "period": 2},
        ]
    )
    hub.handle_public_message({"event": "subscribed", "chanId": 8, "channel": "trades"})
    hub.handle_public_message([8, "fte", [9, 9000, 10, "0.0010", 2]])
    trades = hub.snapshot(9000)["trades"]
    assert len(trades) == 1
    assert trades[0]["amount"] == D("10")
    assert trades[0]["rate"] == D("0.0010")


def test_authenticated_funding_trade_updates_are_deduplicated():
    hub = BitfinexMarketDataHub(api_key="key", api_secret="secret")
    base = [77, "fUSD", 1000, 55, "10", "0.0002", 2, 1]
    updated = [77, "fUSD", 2000, 55, "12", "0.0003", 2, 1]
    hub.handle_auth_message([0, "fte", base])
    hub.handle_auth_message([0, "ftu", updated])
    trades = hub.snapshot(2000)["fundingTrades"]
    assert len(trades) == 1
    assert trades[0]["amount"] == D("12")
    assert trades[0]["rate"] == D("0.0003")


def test_statistics_include_idle_principal_and_external_attribution():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        start = 1_000_000_000
        store.record_account_sample("1000", "500", "0", "500", "0", start)
        store.record_account_sample("1000", "500", "0", "500", "1", start + 86_400_000)
        store.upsert_income_ledgers(
            [
                {
                    "id": 1,
                    "currency": "USD",
                    "wallet": "funding",
                    "amount": D("1"),
                    "balance": D("1001"),
                    "description": "Margin Funding Payment",
                    "mts": start + 86_400_000,
                }
            ]
        )
        stats = store.statistics(None, start + 86_400_000)
        assert stats["utilizationPercent"] == "50.0"
        assert stats["actualNetAprPercent"] == "36.500"
        assert D(stats["idlePrincipalTime"]) == D("500")


def test_realized_income_counts_all_positive_category_28_and_excludes_transfers():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        now = int(time.time() * 1000)
        store.upsert_income_ledgers(
            [
                {
                    "id": 1,
                    "currency": "USD",
                    "wallet": "funding",
                    "amount": D("1.25"),
                    "balance": None,
                    "description": "robot loan interest",
                    "mts": now - 1000,
                },
                {
                    "id": 2,
                    "currency": "USD",
                    "wallet": "funding",
                    "amount": D("2.75"),
                    "balance": None,
                    "description": "external loan interest",
                    "mts": now - 500,
                },
                {
                    "id": 3,
                    "currency": "USD",
                    "wallet": "funding",
                    "amount": D("-0.10"),
                    "balance": None,
                    "description": "negative adjustment",
                    "mts": now,
                },
            ]
        )
        with store.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO ledger_entries(
                    ledger_id, currency, wallet, amount, description, category, mts
                ) VALUES(4, 'USD', 'funding', '1000', 'Wallet transfer', 51, ?)""",
                (now,),
            )
        summary = store.realized_income_summary("USD", now)
        assert summary["today"] == "4.00"
        assert summary["thirtyDays"] == "4.00"
        assert summary["lifetime"] == "4.00"


def test_income_history_backfill_pages_resume_and_upsert_duplicates():
    class LedgerHistoryClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.calls = []

        def ledgers(self, currency, **kwargs):
            self.calls.append((currency, kwargs))
            if len(self.calls) == 1:
                return [
                    [3, "USD", "funding", 3000, None, "3", "6", None, "interest"],
                    [2, "USD", "funding", 2000, None, "2", "3", None, "interest"],
                ]
            return [
                [2, "USD", "funding", 2000, None, "2", "3", None, "interest"],
                [1, "USD", "funding", 1000, None, "1", "1", None, "interest"],
            ]

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        client = LedgerHistoryClient()
        runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
        first = runtime.sync_income_history_once(now_ms=4000, page_limit=2)
        assert first["status"] == "BACKFILLING"
        assert first["next_end_ms"] == 1999
        # A new runtime simulates process restart and resumes the persisted cursor.
        resumed = LendingRuntimeV3(client, limit_policy(), store, hub=object())
        second = resumed.sync_income_history_once(now_ms=5000, page_limit=3)
        assert second["status"] == "COMPLETE"
        assert second["earliest_mts"] == 1000
        assert client.calls[1][1]["end"] == 1999
        assert store.realized_income("USD") == D("6")


def test_income_sync_failure_is_reporting_only_and_keeps_existing_income():
    class FailingClient:
        api_key = ""
        api_secret = ""

        def ledgers(self, *args, **kwargs):
            raise RuntimeError("rate limited")

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.upsert_income_ledgers(
            [
                {
                    "id": 1,
                    "currency": "USD",
                    "wallet": "funding",
                    "amount": D("2"),
                    "balance": None,
                    "description": "interest",
                    "mts": 1000,
                }
            ]
        )
        runtime = LendingRuntimeV3(FailingClient(), limit_policy(), store, hub=object())
        with pytest.raises(RuntimeError):
            runtime.sync_income_history_once(now_ms=2000)
        # Worker error handling updates only the reporting state; runtime mode is unchanged.
        store.update_income_sync_state("USD", status="ERROR", error="rate limited")
        assert store.runtime()["mode"] == "PAUSED"
        assert store.realized_income("USD") == D("2")


def test_paused_and_replay_cycles_never_call_exchange_writes():
    now = int(time.time() * 1000)

    class FakeClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.writes = 0

        def submit_funding_offer(self, *args, **kwargs):
            self.writes += 1
            raise AssertionError("mode must not submit")

        def cancel_funding_offer(self, *args, **kwargs):
            self.writes += 1
            raise AssertionError("mode must not cancel")

    class FakeHub:
        def snapshot(self, at):
            return {
                "as_of": at,
                "source": "WEBSOCKET",
                "safeRequired": False,
                "book": [
                    {"rate": D("0.0004"), "period": 2, "count": 1, "amount": D("5000")},
                    {"rate": D("0.0003"), "period": 2, "count": 1, "amount": D("-5000")},
                ],
                "trades": [],
                "wallets": [
                    {
                        "wallet_type": "funding",
                        "currency": "USD",
                        "balance": D("1000"),
                        "available": D("1000"),
                    }
                ],
                "offers": [],
                "credits": [],
                "fundingTrades": [],
            }

        def stop(self):
            pass

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.save_strategy({}, "ACTIVE")
        client = FakeClient()
        runtime = LendingRuntimeV3(client, limit_policy(), store, hub=FakeHub())
        runtime._bootstrapped = True
        runtime._last_rest_sync_ms = now
        status = runtime.cycle(now)
        assert "1d" in status["statistics"]
        assert status["credits"] == []
        assert status["activeCreditSummary"]["overall"]["orderCount"] == 0
        assert set(status["activeCreditSummary"]["groups"]) == {"short", "medium", "long"}
        store.set_mode("REPLAY", "test")
        runtime.cycle(now + 1)
        assert client.writes == 0


@pytest.mark.parametrize(
    ("available", "safe_reason"),
    [
        (None, "ACCOUNT_AVAILABLE_BALANCE_UNKNOWN"),
        (D("900"), "ACCOUNT_RECONCILIATION_MISMATCH"),
    ],
)
def test_live_cycle_enters_safe_when_account_cannot_be_reconciled(available, safe_reason):
    now = int(time.time() * 1000)

    class FakeClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.writes = 0

        def submit_funding_offer(self, *args, **kwargs):
            self.writes += 1
            raise AssertionError("unknown available balance must block writes")

    class FakeHub:
        def snapshot(self, at):
            return {
                "as_of": at,
                "source": "WEBSOCKET",
                "safeRequired": False,
                "book": [
                    {"rate": D("0.0004"), "period": 2, "count": 1, "amount": D("5000")},
                    {"rate": D("0.0003"), "period": 2, "count": 1, "amount": D("-5000")},
                ],
                "trades": [],
                "wallets": [
                    {
                        "wallet_type": "funding",
                        "currency": "USD",
                        "balance": D("1000"),
                        "available": available,
                    }
                ],
                "offers": [],
                "credits": [],
                "loans": [],
                "fundingTrades": [],
            }

        def stop(self):
            pass

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        store.save_strategy(json_decimal(limit_policy().__dict__), "ACTIVE")
        store.set_mode("LIVE", "test")
        client = FakeClient()
        runtime = LendingRuntimeV3(client, limit_policy(), store, hub=FakeHub())
        runtime._bootstrapped = True
        runtime._last_rest_sync_ms = now

        status = runtime.cycle(now)

        assert client.writes == 0
        assert status["operationMode"] == "PAUSED"
        assert status["runtime"]["safe_reason"] == safe_reason
        assert status["account"]["walletAvailableKnown"] is (available is not None)


def test_logger_redacts_credentials_from_logs_and_nested_status():
    with tempfile.TemporaryDirectory() as directory:
        path = f"{directory}/status.json"
        logger = Logger(path, 20, sensitive_values=("actual-key", "actual-secret"))
        logger.log("api_key=actual-key secret=actual-secret")
        logger.updateMetaValue("nested", {"error": "bfx-apikey: actual-key"})
        logger.persistStatus()
        text = open(path, "r", encoding="utf-8").read()
        assert "actual-key" not in text
        assert "actual-secret" not in text
        assert text.count("[REDACTED]") >= 3


def test_ledger_parser_uses_official_amount_balance_and_description_columns():
    rows = [[42, "USD", "funding", 1_900_000_000_000, None, "1.25", "101.25", None, "Margin Funding Payment"]]
    parsed = parse_ledger_rows_v3(rows)
    assert parsed == [
        {
            "id": 42,
            "currency": "USD",
            "wallet": "funding",
            "mts": 1_900_000_000_000,
            "amount": D("1.25"),
            "balance": D("101.25"),
            "description": "Margin Funding Payment",
        }
    ]


def test_credit_is_attributed_to_managed_offer_through_funding_trade():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        _, intent = store.reserve_intent(intent_order(), D("1000"))
        store.confirm_intent(intent["id"], 9001)
        opening = 1_900_000_000_000
        with store.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO funding_trades(trade_id, currency, offer_id, amount, rate, period, mts, managed)
                   VALUES(1, 'USD', 9001, '150', '0.0002', 2, ?, 1)""",
                (opening,),
            )
        store.reconcile_credits(
            [
                {
                    "id": 8001,
                    "currency": "USD",
                    "amount": D("150"),
                    "rate": D("0.0002"),
                    "period": 2,
                    "status": "ACTIVE",
                    "mts_opening": opening,
                }
            ],
            opening,
        )
        with store.read_connection() as connection:
            credit = dict(connection.execute("SELECT * FROM credits WHERE credit_id=8001").fetchone())
        assert credit["managed"] == 1
        assert credit["offer_id"] == 9001
        assert credit["pool"] == "short"


def test_preflight_adoption_creates_managed_intent_and_is_idempotent():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        offer = {
            "id": 9901,
            "currency": "USD",
            "amount": D("175"),
            "amount_original": D("175"),
            "rate": D("0.00025"),
            "rate_real": None,
            "period": 14,
            "offer_type": "LIMIT",
            "display_type": "LIMIT",
            "flags": 0,
            "status": "ACTIVE",
            "pool": "medium",
            "mts_created": 1_900_000_000_000,
            "mts_updated": 1_900_000_000_000,
        }
        now = offer["mts_created"] + 7_200_000
        store.reconcile_offers([offer], now)
        store.observe_external_takeover(offer, now - 30_000)
        store.observe_external_takeover(offer, now)
        first = store.adopt_external_offers([offer], "active-v3", now_ms=now)
        second = store.adopt_external_offers([offer], "active-v3", now_ms=now)
        assert first == [9901]
        assert second == []
        assert store.offers(active_only=True)[0]["managed"] == 1
        with store.read_connection() as connection:
            intent = connection.execute("SELECT * FROM order_intents WHERE exchange_offer_id=9901").fetchone()
            events = connection.execute(
                "SELECT COUNT(*) FROM ownership_events WHERE offer_id=9901 AND event_type='ADOPT_EXTERNAL'"
            ).fetchone()[0]
        assert intent["resolution"] == "PREFLIGHT_ADOPTED"
        assert intent["pool"] == "medium"
        assert events == 1


def test_credit_unique_direct_match_avoids_external_misattribution():
    with tempfile.TemporaryDirectory() as directory:
        opening = 1_900_000_100_000
        store = LendingStateStore(f"{directory}/state.sqlite3", clock=lambda: opening / 1000)
        _, intent = store.reserve_intent(intent_order(amount="150"), D("1000"))
        store.confirm_intent(intent["id"], 9910)
        store.reconcile_credits(
            [
                {
                    "id": 8810,
                    "currency": "USD",
                    "amount": D("150"),
                    "rate": D("0.0002"),
                    "period": 2,
                    "rate_type": "FIXED",
                    "status": "ACTIVE",
                    "mts_opening": opening,
                }
            ],
            opening,
        )
        credit = store.credits(active_only=True)[0]
        assert credit["managed"] == 1
        assert credit["attribution_state"] == "MANAGED"
        assert credit["offer_id"] == 9910


def test_variable_floor_violation_tracks_start_update_and_end():
    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(f"{directory}/state.sqlite3")
        violation = {"credit_id": 10, "pool": "short", "floor_rate": D("0.0002"), "observed_rate": D("0.0001")}
        store.record_rate_floor_violations([violation], 1000)
        store.record_rate_floor_violations([{**violation, "observed_rate": D("0.00009")}], 2000)
        store.record_rate_floor_violations([], 3000)
        with store.read_connection() as connection:
            rows = [dict(row) for row in connection.execute("SELECT * FROM rate_floor_violations").fetchall()]
        assert len(rows) == 1
        assert rows[0]["started_at_ms"] == 1000
        assert rows[0]["ended_at_ms"] == 3000
        assert rows[0]["observed_rate"] == "0.00009"


def test_low_demand_confirmation_requires_two_distinct_cycles_and_resets(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    first = store.observe_demand_confirmation("v3.3", "term", "7", D("0.014"), True, 300_000)
    duplicate = store.observe_demand_confirmation("v3.3", "term", "7", D("0.014"), True, 301_000)
    second = store.observe_demand_confirmation("v3.3", "term", "7", D("0.014"), True, 600_000)
    reset = store.observe_demand_confirmation("v3.3", "term", "7", None, None, 900_000)

    assert first == {"cycles": 1, "confirmed": False, "cycle": 1}
    assert duplicate == first
    assert second["cycles"] == 2 and second["confirmed"] is True
    assert reset["cycles"] == 0 and reset["confirmed"] is False


def _managed_short_offer(store, offer_id, amount, period, now_ms):
    order = {
        **intent_order(amount=str(amount)),
        "slice_key": f"managed-short:{offer_id}",
        "period": int(period),
        "strategy_version": "v3.3",
    }
    _, intent = store.reserve_intent(order, D("1000"))
    store.confirm_intent(intent["id"], offer_id)
    offer = {
        "id": offer_id,
        "currency": "USD",
        "amount": D(str(amount)),
        "amount_original": D(str(amount)),
        "rate": D("0.0004"),
        "rate_real": D("0.0004"),
        "period": int(period),
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "managed": True,
        "pool": "short",
        "layer": "balanced",
        "mts_created": now_ms - 20 * 60_000,
    }
    store.reconcile_offers(
        [
            *[
                {
                    "id": int(row["offer_id"]),
                    "currency": row["currency"],
                    "amount": D(row["amount"]),
                    "amount_original": D(row["amount_original"]),
                    "rate": D(row["rate"]),
                    "rate_real": D(row["rate_real"] or row["rate"]),
                    "period": int(row["period"]),
                    "offer_type": row["offer_type"],
                    "display_type": row["display_type"],
                    "flags": int(row["flags"]),
                    "status": "ACTIVE",
                    "managed": True,
                    "pool": row["pool"],
                    "layer": row["layer"],
                    "mts_created": row["mts_created"],
                }
                for row in store.offers(active_only=True)
            ],
            offer,
        ],
        now_ms,
    )
    return offer


def _ready_dust_consolidation(store, offer_id, wallet, offer_amount, period, now_ms):
    offer = _managed_short_offer(store, offer_id, offer_amount, period, now_ms)
    chain = store.ensure_reprice_chain(offer, "v3.3", now_ms)
    store.begin_consolidation(offer_id, D(wallet), D(offer_amount), period, "v3.3", now_ms)
    store.update_consolidation("READY", now_ms=now_ms)
    store.reconcile_offers([], now_ms + 1)
    return offer, chain


def test_dust_reinvestment_cancels_smallest_short_and_restores_its_strategy(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    _managed_short_offer(store, 7101, "170", 2, now)
    _managed_short_offer(store, 7102, "160", 7, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []
            self.submitted = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

        def submit_funding_offer(self, symbol, amount, rate, period, offer_type, flags=0):
            self.submitted.append((symbol, D(amount), D(rate), int(period), offer_type, flags))
            return [0, "on-req", None, None, [7201]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(
        periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.986", "0.014"), low_confirmed=True)}}
    )
    account = {"wallet": D("1"), "reconciliationStatus": "MATCHED"}

    first = runtime._dust_consolidation(account, market, now, "v3.3")
    assert first["state"] == "CANCELLING"
    assert client.canceled == [7102]

    store.reconcile_offers(
        [
            {
                "id": 7101,
                "currency": "USD",
                "amount": D("170"),
                "amount_original": D("170"),
                "rate": D("0.0004"),
                "rate_real": D("0.0004"),
                "period": 2,
                "offer_type": "LIMIT",
                "display_type": "LIMIT",
                "flags": 0,
                "status": "ACTIVE",
                "managed": True,
                "pool": "short",
                "layer": "balanced",
                "mts_created": now - 20 * 60_000,
            }
        ],
        now + 30_000,
    )
    ready = runtime._dust_consolidation(
        {"wallet": D("161"), "reconciliationStatus": "MATCHED"}, market, now + 30_000, "v3.3"
    )
    assert ready["state"] == "READY"
    submitted = runtime._dust_consolidation(
        {"wallet": D("161"), "reconciliationStatus": "MATCHED"}, market, now + 60_000, "v3.3"
    )
    assert submitted["submitted"][0]["amount"] == D("161")
    assert client.submitted[0][1] == D("161")
    assert client.submitted[0][2] == D("0.0004")
    assert client.submitted[0][3] == 7
    replacement_chain = store.reprice_chain_for_offer(7201)
    assert replacement_chain is not None
    assert replacement_chain["current_offer_id"] == 7201
    assert store.consolidation_status()["state"] == "SUBMITTING"


def test_dust_reinvestment_prioritizes_short_then_medium_then_long(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "pool-priority.sqlite3", clock=lambda: now / 1000)
    _managed_short_offer(store, 7121, "190", 2, now)
    _managed_short_offer(store, 7122, "160", 30, now)
    _managed_short_offer(store, 7123, "150", 120, now)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE offers SET pool='medium' WHERE offer_id=7122")
        connection.execute("UPDATE offers SET pool='long' WHERE offer_id=7123")

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(
        periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.986", "0.014"), low_confirmed=True)}}
    )

    result = runtime._dust_consolidation(
        {"wallet": D("1"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )

    assert result["state"] == "CANCELLING"
    assert client.canceled == [7121]


def test_dust_reinvestment_can_consolidate_a_long_offer_when_no_short_candidate(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "long-candidate.sqlite3", clock=lambda: now / 1000)
    _managed_short_offer(store, 7150, "150", 2, now)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE offers SET pool='long', period=120 WHERE offer_id=7150")
    source = next(row for row in store.offers() if row["offer_id"] == 7150)
    source_chain = store.ensure_reprice_chain(source, "v3.3", now)
    store.complete_reprice_stage(source_chain["chain_key"], 3, now - 1, market_anchor_rate=D("0.00035"))

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []
            self.submitted = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

        def submit_funding_offer(self, symbol, amount, rate, period, offer_type, flags=0):
            self.submitted.append((symbol, D(amount), D(rate), int(period), offer_type, flags))
            return [0, "on-req", None, None, [7151]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(
        periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.986", "0.014"), low_confirmed=True)}}
    )

    result = runtime._dust_consolidation(
        {"wallet": D("3.16089453"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )

    assert result["state"] == "CANCELLING"
    assert client.canceled == [7150]

    store.reconcile_offers([], now + 30_000)
    ready = runtime._dust_consolidation(
        {"wallet": D("153.16089453"), "reconciliationStatus": "MATCHED"},
        market,
        now + 30_000,
        "v3.3",
    )
    submitted = runtime._dust_consolidation(
        {"wallet": D("153.16089453"), "reconciliationStatus": "MATCHED"},
        market,
        now + 60_000,
        "v3.3",
    )

    assert ready["state"] == "READY"
    assert submitted["submitted"][0]["period"] == 120
    assert submitted["submitted"][0]["pool"] == "long"
    replacement_chain = store.reprice_chain_for_offer(7151)
    assert replacement_chain["chain_key"] == source_chain["chain_key"]
    assert replacement_chain["current_stage"] == 3
    assert replacement_chain["started_at_ms"] == source_chain["started_at_ms"]


@pytest.mark.parametrize(
    ("wallet", "expected"),
    (
        ("299.99", (D("299.99"),)),
        ("300", (D("150"), D("150"))),
        ("328.60719001", (D("164.30359501"), D("164.30359500"))),
        ("600.00000003", (D("150.00000001"), D("150.00000001"), D("150.00000001"), D("150"))),
    ),
)
def test_dust_reinvestment_evenly_splits_into_maximum_valid_orders(tmp_path, wallet, expected):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / f"split-{wallet}.sqlite3", clock=lambda: now / 1000)
    _ready_dust_consolidation(store, 7200, "1", D(wallet) - D("1"), 7, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.submitted = []

        def submit_funding_offer(self, symbol, amount, rate, period, offer_type, flags=0):
            self.submitted.append((symbol, D(amount), D(rate), int(period), offer_type, flags))
            return [0, "on-req", None, None, [8000 + len(self.submitted)]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    result = runtime._dust_consolidation(
        {"wallet": D(wallet), "reconciliationStatus": "MATCHED"},
        signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}}),
        now,
        "v3.3",
    )

    assert tuple(row[1] for row in client.submitted) == expected
    assert sum((row[1] for row in client.submitted), D("0")) == D(wallet)
    assert all(row[1] >= D("150") and row[2] == D("0.0004") and row[3] == 7 for row in client.submitted)
    assert result["state"] == "SUBMITTING"
    assert store.consolidation_status()["state"] == "SUBMITTING"


def test_dust_reinvestment_first_split_continues_chain_and_siblings_start_at_zero(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "split-chains.sqlite3", clock=lambda: now / 1000)
    _, source_chain = _ready_dust_consolidation(
        store,
        7205,
        "8.60719001",
        "320",
        30,
        now,
    )
    source_chain = store.complete_reprice_stage(
        source_chain["chain_key"],
        3,
        now - 1,
        market_anchor_rate=D("0.00035"),
    )

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.next_id = 8050

        def submit_funding_offer(self, *_args, **_kwargs):
            self.next_id += 1
            return [0, "on-req", None, None, [self.next_id]]

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    result = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"},
        signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}}),
        now,
        "v3.3",
    )

    assert len(result["submitted"]) == 2
    continued = store.reprice_chain_for_offer(8051)
    sibling = store.reprice_chain_for_offer(8052)
    assert continued["chain_key"] == source_chain["chain_key"]
    assert continued["current_stage"] == 3
    assert continued["started_at_ms"] == source_chain["started_at_ms"]
    assert sibling["chain_key"] != source_chain["chain_key"]
    assert sibling["current_stage"] == 0
    assert sibling["started_at_ms"] == now


def test_dust_reinvestment_waits_for_snapshot_then_completes_after_multiple_submits(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "multi-complete.sqlite3", clock=lambda: now / 1000)
    _ready_dust_consolidation(store, 7201, "8.60719001", "320", 2, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.next_id = 8100

        def submit_funding_offer(self, *_args, **_kwargs):
            self.next_id += 1
            return [0, "on-req", None, None, [self.next_id]]

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}})
    submitted = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )
    waiting = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"}, market, now + 30_000, "v3.3"
    )
    replacement_offers = [
        {
            "id": row["offerId"],
            "currency": "USD",
            "amount": row["amount"],
            "amount_original": row["amount"],
            "rate": row["submitted_rate"],
            "rate_real": row["effective_rate"],
            "period": row["period"],
            "offer_type": row["offer_type"],
            "display_type": row["display_type"],
            "flags": row["flags"],
            "status": "ACTIVE",
            "managed": True,
            "pool": row["pool"],
            "layer": row["layer"],
            "mts_created": now,
        }
        for row in submitted["submitted"]
    ]
    store.reconcile_offers(replacement_offers, now + 60_000)
    completed = runtime._dust_consolidation(
        {"wallet": D("0"), "reconciliationStatus": "MATCHED"}, market, now + 60_000, "v3.3"
    )

    assert len(submitted["submitted"]) == 2
    assert waiting["state"] == "SUBMITTING"
    assert completed["state"] == "CONFIRMED"
    assert store.consolidation_status()["state"] == "IDLE"


def test_dust_reinvestment_retries_only_remaining_wallet_with_new_slice_indexes(tmp_path, monkeypatch):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "partial.sqlite3", clock=lambda: now / 1000)
    _ready_dust_consolidation(store, 7202, "8.60719001", "320", 2, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.submitted = []

        def submit_funding_offer(self, symbol, amount, rate, period, offer_type, flags=0):
            self.submitted.append(D(amount))
            return [0, "on-req", None, None, [8200 + len(self.submitted)]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}})
    monkeypatch.setattr("RuntimeV3.MAX_FUNDING_SUBMISSIONS_PER_WINDOW", 2)

    first = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )
    assert len(first["submitted"]) == 1
    assert first["submitted"][0]["slice_index"] == 0
    first_row = first["submitted"][0]
    store.reconcile_offers(
        [
            {
                "id": first_row["offerId"],
                "currency": "USD",
                "amount": first_row["amount"],
                "amount_original": first_row["amount"],
                "rate": first_row["submitted_rate"],
                "rate_real": first_row["effective_rate"],
                "period": first_row["period"],
                "offer_type": first_row["offer_type"],
                "display_type": first_row["display_type"],
                "flags": first_row["flags"],
                "status": "ACTIVE",
                "managed": True,
                "pool": first_row["pool"],
                "layer": first_row["layer"],
                "mts_created": now,
            }
        ],
        now + 61_000,
    )

    runtime.clock = lambda: (now + 61_000) / 1000
    second = runtime._dust_consolidation(
        {"wallet": D("164.30359500"), "reconciliationStatus": "MATCHED"}, market, now + 61_000, "v3.3"
    )
    assert len(second["submitted"]) == 1
    assert second["submitted"][0]["slice_index"] == 1
    assert client.submitted == [D("164.30359501"), D("164.30359500")]


def test_dust_reinvestment_stops_after_ambiguous_split_submit(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "ambiguous-split.sqlite3", clock=lambda: now / 1000)
    _ready_dust_consolidation(store, 7203, "8.60719001", "320", 2, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.calls = 0

        def submit_funding_offer(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 2:
                raise BitfinexAmbiguousWriteError("connection ended after send")
            return [0, "on-req", None, None, [8300 + self.calls]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}})
    first = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )
    repeated = runtime._dust_consolidation(
        {"wallet": D("164.30359500"), "reconciliationStatus": "MATCHED"}, market, now + 30_000, "v3.3"
    )

    assert len(first["submitted"]) == 1
    assert first["state"] == "AMBIGUOUS"
    assert repeated["state"] == "AMBIGUOUS"
    assert client.calls == 2
    assert store.consolidation_status()["state"] == "AMBIGUOUS"


def test_dust_reinvestment_leaves_external_funds_out_of_partial_retry(tmp_path, monkeypatch):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "external-funds.sqlite3", clock=lambda: now / 1000)
    _ready_dust_consolidation(store, 7204, "8.60719001", "320", 2, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.submitted = []

        def submit_funding_offer(self, _symbol, amount, *_args, **_kwargs):
            self.submitted.append(D(amount))
            return [0, "on-req", None, None, [8400 + len(self.submitted)]]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object(), clock=lambda: now / 1000)
    market = signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}})
    monkeypatch.setattr("RuntimeV3.MAX_FUNDING_SUBMISSIONS_PER_WINDOW", 2)
    first = runtime._dust_consolidation(
        {"wallet": D("328.60719001"), "reconciliationStatus": "MATCHED"}, market, now, "v3.3"
    )
    first_row = first["submitted"][0]
    store.reconcile_offers(
        [
            {
                "id": first_row["offerId"],
                "currency": "USD",
                "amount": first_row["amount"],
                "amount_original": first_row["amount"],
                "rate": first_row["submitted_rate"],
                "rate_real": first_row["effective_rate"],
                "period": first_row["period"],
                "offer_type": first_row["offer_type"],
                "display_type": first_row["display_type"],
                "flags": first_row["flags"],
                "status": "ACTIVE",
                "managed": True,
                "pool": first_row["pool"],
                "layer": first_row["layer"],
                "mts_created": now,
            }
        ],
        now + 61_000,
    )
    runtime.clock = lambda: (now + 61_000) / 1000
    retried = runtime._dust_consolidation(
        {"wallet": D("314.30359500"), "reconciliationStatus": "MATCHED"}, market, now + 61_000, "v3.3"
    )

    assert retried["state"] == "SUBMITTING"
    assert client.submitted == [D("164.30359501"), D("164.30359500")]
    assert sum(client.submitted, D("0")) == D("328.60719001")
    assert store.consolidation_status()["state"] == "SUBMITTING"


@pytest.mark.parametrize("wallet", ("0.99", "150"))
def test_dust_reinvestment_does_not_cancel_outside_the_one_to_14999_range(tmp_path, wallet):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / f"dust-{wallet}.sqlite3")
    _managed_short_offer(store, 7301, "160", 2, now)

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(offer_id)
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    result = runtime._dust_consolidation(
        {"wallet": D(wallet), "reconciliationStatus": "MATCHED"},
        signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}}),
        now,
        "v3.3",
    )
    assert result["state"] == "IDLE"
    assert client.canceled == []


def test_dust_unknown_cancel_enters_safe_without_replacement(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    _managed_short_offer(store, 7401, "160", 7, now)
    store.set_mode("LIVE", "test")

    class Client:
        api_key = ""
        api_secret = ""

        def cancel_funding_offer(self, _offer_id):
            raise BitfinexAmbiguousWriteError("connection ended after send")

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())
    result = runtime._dust_consolidation(
        {"wallet": D("1"), "reconciliationStatus": "MATCHED"},
        signals(periodSelection={"byPool": {"short": selection_row((2, 7), 2, ("0.9", "0.1"))}}),
        now,
        "v3.3",
    )
    assert result["state"] == "AMBIGUOUS"
    assert store.runtime()["mode"] == "PAUSED"
    assert store.consolidation_status()["state"] == "AMBIGUOUS"
    assert store.intents(states={"PLANNED", "SUBMITTING", "AMBIGUOUS"}) == []


def test_stale_ambiguous_dust_cancel_recovers_after_restart_snapshot(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.begin_consolidation(7402, D("12"), D("160"), 2, "v3.3", now)
    store.update_consolidation("AMBIGUOUS", "connection ended after send", now)

    class Client:
        api_key = ""
        api_secret = ""

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())
    runtime._reconcile_ambiguous_dust_consolidation(set(), now + 30_000)

    assert store.consolidation_status()["state"] == "READY"


def test_write_recovery_status_reports_durable_submit_and_dust_blockers(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    _, intent = store.reserve_intent(
        {
            "currency": "USD",
            "slice_key": "v3:short:quick:0",
            "pool": "short",
            "layer": "quick",
            "amount": D("150"),
            "submitted_rate": D("0.0003"),
            "effective_rate": D("0.0003"),
            "period": 2,
            "offer_type": "LIMIT",
            "display_type": "LIMIT",
            "flags": 0,
            "strategy_version": "v3.3",
            "plan_hash": "plan",
        },
        D("150"),
    )
    store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection ended after send")
    store.begin_consolidation(7403, D("5"), D("160"), 2, "v3.3", now)
    store.update_consolidation("AMBIGUOUS", "cancel result unknown", now)

    class Client:
        api_key = ""
        api_secret = ""

    status = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())._write_recovery_status()

    assert status["canSubmit"] is False
    assert {(item["kind"], item["state"]) for item in status["blockers"]} >= {
        ("SUBMIT", "AMBIGUOUS"),
        ("DUST_CONSOLIDATION", "AMBIGUOUS"),
    }
    assert all(item["blocking"] for item in status["blockers"])


def test_observed_external_takeover_is_reported_but_does_not_block_writes(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 5074345800,
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "flags": 0,
    }
    store.observe_external_takeover(offer, now)

    class Client:
        api_key = ""
        api_secret = ""

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())
    status = runtime._write_recovery_status()

    assert status["canSubmit"] is True
    assert status["blockers"] == [
        {
            "kind": "EXTERNAL_TAKEOVER",
            "state": "OBSERVED",
            "offerId": 5074345800,
            "reason": None,
            "createdAt": now,
            "blocking": False,
        }
    ]


def test_absent_external_takeover_observation_is_closed_after_authoritative_snapshot(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 5074345801,
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "flags": 0,
    }
    store.observe_external_takeover(offer, now)

    class Client:
        api_key = ""
        api_secret = ""

    runtime = LendingRuntimeV3(Client(), limit_policy(), store, hub=object())
    runtime._close_absent_external_takeover_observations(set(), now + 30_000)

    assert store.external_takeovers()[0]["state"] == "CLOSED"


def test_v35_release_boundary_is_immutable_and_splits_equal_windows(tmp_path):
    path = tmp_path / "release-boundary.sqlite3"
    activated = 1_900_000_000_000
    first = LendingStateStore(path, clock=lambda: activated / 1000)

    assert first.release_boundary() == {
        "version": "0.3.6.0",
        "label": "V3.6",
        "baselineVersion": "0.3.5.2",
        "baselineLabel": "V3.5",
        "v36ActivatedAtMs": None,
        "activatedAtMs": activated,
        "boundarySource": "FIRST_V35_START",
        "evidenceAtMs": None,
    }

    restarted = LendingStateStore(path, clock=lambda: (activated + 3_600_000) / 1000)
    comparison = restarted.release_comparison(activated + 3_600_000)
    assert comparison["activatedAtMs"] == activated
    assert comparison["comparisonDurationMs"] == 3_600_000
    assert comparison["before"]["fromMs"] == activated - 3_600_000
    assert comparison["before"]["toMs"] == activated
    assert comparison["after"]["fromMs"] == activated
    assert comparison["after"]["toMs"] == activated + 3_600_001


def test_v35_release_boundary_moves_back_to_exact_term_live_session(tmp_path):
    path = tmp_path / "historical-release-boundary.sqlite3"
    live_at = 1_787_548_590_850
    first_exact_order_at = 1_787_551_007_941
    published_at = 1_787_798_000_000
    store = LendingStateStore(path, clock=lambda: published_at / 1000)
    with store.transaction(immediate=True) as connection:
        connection.execute(
            "INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms) VALUES('PAUSED', 'LIVE', ?, ?)",
            ("live_preflight_confirmed", live_at),
        )
        connection.execute(
            """INSERT INTO order_intents(
                   fingerprint, currency, slice_key, pool, layer, amount,
                   submitted_rate, effective_rate, period, offer_type, flags,
                   strategy_version, state, pricing_curve_version,
                   created_at_ms, updated_at_ms
               ) VALUES(?, 'USD', ?, 'short', 'balanced', '150',
                        '0.0002194', '0.0002194', 2, 'LIMIT', 0,
                        'historical-v2', 'CLOSED', 'EXACT_TERM_EXPLORATION_V2', ?, ?)""",
            ("historical-v2-fingerprint", "historical-v2-slice", first_exact_order_at, first_exact_order_at),
        )

    # Reconstruct the pre-upgrade fixture: only the schema-16 migration may
    # repair missing historical evidence; normal schema-18 reads are immutable.
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE schema_meta SET value='16' WHERE key='schema_version'")
        connection.execute("DELETE FROM schema_meta WHERE key IN ('schema_family','v36_initialization_complete')")

    restarted = LendingStateStore(path, clock=lambda: (published_at + 60_000) / 1000)
    assert restarted.release_boundary() == {
        "version": "0.3.6.0",
        "label": "V3.6",
        "baselineVersion": "0.3.5.2",
        "baselineLabel": "V3.5",
        "v36ActivatedAtMs": None,
        "activatedAtMs": live_at,
        "boundarySource": "LIVE_SESSION_BEFORE_EXACT_TERM_EXPLORATION",
        "evidenceAtMs": first_exact_order_at,
    }

    restarted_again = LendingStateStore(path, clock=lambda: (published_at + 120_000) / 1000)
    assert restarted_again.release_boundary()["activatedAtMs"] == live_at


def test_external_takeover_requires_two_matching_snapshots_and_cancels_exact_id(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 5074345865,
        "currency": "USD",
        "amount": D("368.94"),
        "amount_original": D("368.94"),
        "rate": D("0.000301"),
        "rate_real": D("0.000301"),
        "period": 120,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "pool": "long",
        "mts_created": now - 7_200_000,
    }
    store.reconcile_offers([offer], now)
    assert store.observe_external_takeover(offer, now)["state"] == "OBSERVED"
    assert store.observe_external_takeover(offer, now + 30_000)["state"] == "CONFIRMED"
    assert store.adopt_external_offers([offer], "v3.3", now_ms=now + 30_000) == [5074345865]

    class Client:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.canceled = []

        def cancel_funding_offer(self, offer_id):
            self.canceled.append(int(offer_id))
            return [0, "SUCCESS"]

    client = Client()
    runtime = LendingRuntimeV3(client, limit_policy(), store, hub=object())
    assert runtime._cancel_external_takeovers(now + 60_000, "v3.3") == [5074345865]
    assert client.canceled == [5074345865]
    assert store.external_takeovers()[0]["state"] == "CANCELLING"


def test_robot_offer_is_removed_from_unconfirmed_external_takeover_candidates(tmp_path):
    now = 1_900_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3")
    offer = {
        "id": 7501,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 30,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "pool": "medium",
        "mts_created": now,
    }
    assert store.observe_external_takeover(offer, now)["state"] == "OBSERVED"
    store.discard_unconfirmed_external_takeover(offer["id"])
    assert store.external_takeovers() == []
