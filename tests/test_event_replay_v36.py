from dataclasses import replace
from decimal import Decimal as D

import pytest

import EventReplay
from EventReplay import (
    DAY_MS, ReplayAssumptions, book_coverage, calibrate_repayments,
    run_event_replay, snapshot_visible_at,
)
from StrategyV3 import QUICK_EXECUTION_CURVE, StrategyPolicyV3


START = 100 * DAY_MS + 12 * 3_600_000


def policy(**changes):
    return replace(StrategyPolicyV3(), short_floor_apr=D(".01"), medium_floor_apr=D(".01"),
                   long_floor_apr=D(".01"), enable_limit=True, enable_frr=False,
                   enable_frr_delta_fixed=False, enable_frr_delta_variable=False, **changes)


def order(amount="150", ident="initial", period=2, **changes):
    return {"order_id": ident, "amount": D(amount), "period": period,
            "effective_rate": D(".001"), "pool": "short" if period < 7 else "long",
            "layer": "quick", **changes}


def trade(offset, amount, ident="t1", rate=".001", period=2):
    return {"id": ident, "mts": START + offset, "amount": D(amount), "rate": D(rate), "period": period}


def replay(trades=(), *, duration=240_000, initial=None, principal="150", books=None, assumptions=None, **kwargs):
    return run_event_replay(policy(), trades, [], D(principal), start_ms=START, now_ms=START + duration,
                            initial_orders=[order()] if initial is None else initial,
                            book_snapshots=books,
                            assumptions=assumptions or ReplayAssumptions(queue_multiplier=0, require_full_window=False),
                            **kwargs)


def test_partial_fills_40_and_110_are_persistent_and_amount_weighted():
    result = replay([trade(10_000, "40"), trade(70_000, "110", "t2")])
    assert [D(row["amount"]) for row in result["fills"]] == [D(40), D(110)]
    assert len(result["orders"]) == 1
    assert D(result["finalState"]["reserved"]) == 0
    assert D(result["finalState"]["loanPrincipal"]) == 150
    assert D(result["metrics"]["averageWaitMinutes"]) == (D(40) * 10_000 + D(110) * 70_000) / 150 / 60_000


def test_one_print_cannot_be_reused_by_multiple_orders():
    result = replay([trade(1000, "200")], principal="300", initial=[order(ident="a"), order(ident="b")])
    assert sum((D(row["amount"]) for row in result["fills"]), D(0)) == 200
    assert D(result["finalState"]["reserved"]) == 100
    assert D(result["finalState"]["loanPrincipal"]) == 200


def test_external_depth_is_consumed_once_across_differently_priced_own_orders():
    result = replay([trade(1000, "400", rate=".002")], principal="300",
                    initial=[order(ident="a"), order(ident="b", effective_rate=D(".002"))],
                    books=[{"mts": START, "observed_at_ms": START,
                            "book": [{"rate": D(".001"), "period": 2, "amount": D(150)}]}],
                    assumptions=ReplayAssumptions(queue_multiplier=1, require_full_window=False))
    assert sum((D(row["amount"]) for row in result["fills"]), D(0)) == 250
    assert D(result["finalState"]["reserved"]) == 50


def test_real_strategy_planner_accepts_flat_period_and_layer_exposure_maps():
    warmup = [{"id": f"warm{index}", "mts": START - 7 * DAY_MS + index * DAY_MS // 5,
               "rate": D(".001"), "amount": D(1000), "period": 2} for index in range(35)]
    result = replay([*warmup, trade(70_000, "900", rate=".002")], initial=[], principal="900",
                    books=[{"mts": START, "observed_at_ms": START,
                            "book": [{"rate": D(".001"), "period": period, "amount": D(5000)}
                                     for period in (2, 14, 120)]}],
                    market_coverage={"coveredIntervals": [{"startMs": START - 7 * DAY_MS,
                                                           "endMs": START + 240_000}]})
    assert result["orders"]
    assert result["fills"]
    assert all(D(row["amount"]) >= 150 for row in result["orders"])
    assert D(result["finalState"]["cash"]) + D(result["finalState"]["reserved"]) \
        + D(result["finalState"]["loanPrincipal"]) == 900


def test_price_through_is_required_and_trade_timestamp_is_not_rounded_back():
    result = replay([trade(1000, "150", rate=".0009"), trade(149_999, "150", "t2")])
    assert len(result["fills"]) == 1
    assert result["fills"][0]["fill_mts"] == START + 149_999
    expected = D(150) * D(".001") * (D(1) - policy().normal_fee_rate) * D(240_000 - 149_999) / DAY_MS
    assert D(result["netInterest"]) == pytest.approx(expected, abs=D("1e-18"))


@pytest.mark.parametrize("delay", [60, 120])
def test_cancel_race_retains_fills_and_replacement_waits_for_confirmation(monkeypatch, delay):
    monkeypatch.setattr(EventReplay, "competitive_rate_for_period", lambda *args: D(".0002"))
    active = policy(quick_short_reprice_stages_minutes=tuple(range(1, 11)),
                    minimum_rate_change=D(".0000001"))
    decision = START + 60_000
    cancel_confirm = decision + delay * 1000
    accept = cancel_confirm + delay * 1000
    result = run_event_replay(
        active,
        [trade(90_000, "40"), trade(accept - START - 1, "260", "too_early"),
         trade(accept - START + 1, "260", "after_accept")], [], D(300),
        start_ms=START, now_ms=accept + 2000, initial_orders=[order("300")],
        book_snapshots=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                        for index in range(10)],
        assumptions=ReplayAssumptions(queue_multiplier=0, delay_seconds=delay, require_full_window=False),
        signal_provider=lambda *args: {}, plan_builder=lambda *args, **kwargs: {"plan": []})
    assert sum((D(row["amount"]) for row in result["fills"]), D(0)) == 300
    assert all(row["trade_id"] != "too_early" for row in result["fills"])
    cancels = [row for row in result["events"] if row["type"] == "CANCEL_CONFIRMED"]
    assert D(cancels[0]["releasedAmount"]) == 260
    assert result["orders"][1]["chain_started_at_ms"] == START


def test_no_fresh_book_blocks_new_orders_and_repricing():
    calls = []
    result = replay(initial=[], principal="300", duration=900_000,
                    plan_builder=lambda *args, **kwargs: calls.append(args) or {"plan": [order()]})
    assert not calls
    assert not result["orders"]
    assert result["marketMode"] == "NO_OBSERVED_BOOK"


def test_legacy_book_becomes_visible_at_bucket_end():
    assert snapshot_visible_at({"mts": START}) == START + 60_000
    observed = []
    result = replay(initial=[], principal="300", books=[{"mts": START, "book": []}],
                    signal_provider=lambda now, *args: observed.append(now) or {},
                    plan_builder=lambda *args, **kwargs: {"plan": []})
    assert min(observed) == START + 60_000
    assert result["sampleCount"] == 0


def test_loan_and_interest_persist_across_days_and_partial_early_repayment():
    result = replay([trade(0, "150")], duration=3 * DAY_MS,
                    repayment_events=[{"mts": START + DAY_MS, "order_id": "initial", "amount": "40"}],
                    segments={"one": (START, START + DAY_MS), "two": (START + DAY_MS, START + 3 * DAY_MS)})
    assert D(result["segments"]["one"]["loanPrincipalTime"]) == pytest.approx(D(150), abs=D("1e-18"))
    assert D(result["segments"]["two"]["loanPrincipalTime"]) == pytest.approx(D(110), abs=D("1e-18"))
    assert [D(row["amount"]) for row in result["returns"]] == [D(40), D(110)]
    expected = D(260) * D(".001") * (D(1) - policy().normal_fee_rate)
    assert D(result["netInterest"]) == pytest.approx(expected, abs=D("1e-18"))
    final = result["finalState"]
    assert D(final["equity"]) == pytest.approx(D(150) + expected, abs=D("1e-18"))
    assert D(final["cashInterest"]) > 0
    assert len(result["daily"]) == 4


def test_long_credit_carries_through_split_and_is_not_counted_by_fill_count():
    result = replay([trade(0, "150", period=120)], duration=3 * DAY_MS,
                    initial=[order(period=120)],
                    segments={"validation": (START, START + DAY_MS), "test": (START + DAY_MS, START + 3 * DAY_MS)})
    assert result["segments"]["test"]["fillCount"] == 0
    assert D(result["segments"]["test"]["longOccupancyPercent"]) == 100
    assert D(result["segments"]["test"]["netInterest"]) > 0


def test_no_fill_wait_is_null_and_future_events_do_not_change_past():
    first = replay(duration=DAY_MS)
    later = replay([trade(DAY_MS + 1000, "150")], duration=2 * DAY_MS)
    assert first["metrics"]["averageWaitMinutes"] is None
    assert first["daily"][0] == later["daily"][0]
    assert D(first["idlePrincipalTime"]) == 150


def test_training_calibration_rejects_future_and_censored_records():
    samples = [{"period": 2, "opened_at_ms": START, "closed_at_ms": START + DAY_MS + index}
               for index in range(30)]
    samples += [{"period": 120, "opened_at_ms": START, "closed_at_ms": START + 80 * DAY_MS},
                {"period": 120, "opened_at_ms": START, "closed_at_ms": None}]
    calibrated = calibrate_repayments(samples, START, START + 60 * DAY_MS)
    assert calibrated["periods"]["2"]["usable"]
    assert "120" not in calibrated["periods"]


def test_calibrated_repayment_uses_only_model_available_at_fill():
    model = {"trainingEndMs": START, "periods": {"2": {"usable": True, "durationsMs": [DAY_MS] * 30}}}
    result = replay([trade(0, "150")], duration=2 * DAY_MS,
                    assumptions=ReplayAssumptions(queue_multiplier=0, repayment_mode="calibrated",
                                                  require_full_window=False),
                    repayment_calibration=model)
    assert result["returns"][0]["returned_at_ms"] == START + DAY_MS
    assert result["returns"][0]["early"]
    assert result["dataQuality"]["repaymentCalibrationUsable"]


def test_book_coverage_unions_intervals_and_preserves_gaps():
    rows = [{"mts": START, "observed_at_ms": START}, {"mts": START + 60_000, "observed_at_ms": START + 60_000},
            {"mts": START + 600_000, "observed_at_ms": START + 600_000}]
    coverage = book_coverage(rows, START, START + DAY_MS)["all"]
    assert coverage["freshMs"] == 180_000
    assert not coverage["meetsMinimum"]
    assert coverage["maximumStaleGapMs"] == DAY_MS - 660_000


@pytest.mark.parametrize("verified", [False, True])
def test_seven_day_timestamp_span_is_not_a_coverage_proof(verified):
    warmup = [{"id": f"warm{index}", "mts": START - 7 * DAY_MS + index * DAY_MS // 5,
               "rate": D(".001"), "amount": D(1000), "period": 2} for index in range(35)]
    result = replay(warmup, duration=60_000, initial=[], principal="300",
                    books=[{"mts": START, "observed_at_ms": START, "book": []}],
                    market_coverage={"coveredIntervals": [{"startMs": START - 7 * DAY_MS,
                                                           "endMs": START + 60_000}]} if verified else None,
                    plan_builder=lambda *args, **kwargs: {"plan": []})
    assert result["signals"]["completeWindowUsable"] is verified
    assert result["dataQuality"]["tradeCoverageComplete"] is verified


def test_calibrated_mode_reports_unavailable_when_term_has_fewer_than_30_samples():
    model = {"trainingEndMs": START, "periods": {"2": {"usable": True, "durationsMs": [DAY_MS] * 29}}}
    result = replay([trade(0, "150")], duration=2 * DAY_MS,
                    assumptions=ReplayAssumptions(queue_multiplier=0, repayment_mode="calibrated",
                                                  require_full_window=False),
                    repayment_calibration=model)
    assert result["returns"][0]["returned_at_ms"] == START + 2 * DAY_MS
    assert result["dataQuality"]["calibrationMissingPeriods"] == [2]
    assert not result["dataQuality"]["repaymentCalibrationUsable"]


@pytest.mark.parametrize("outcome", [None, "CONFIRMED", "ABSENT"])
def test_unknown_submit_never_releases_or_reposts_without_explicit_resolution(outcome):
    planned = []

    def once(*args, **kwargs):
        if planned:
            return {"plan": []}
        planned.append(True)
        return {"plan": [order(ident="uncertain")]}

    confirmations = [{"mts": START + 30_000, "order_id": "uncertain", "action": "SUBMIT", "outcome": "UNKNOWN"}]
    if outcome:
        confirmations.append({"mts": START + 120_000, "order_id": "uncertain", "action": "SUBMIT",
                              "outcome": outcome})
    result = replay([trade(90_000, "150"), trade(121_000, "150", "after_resolution")], initial=[], duration=180_000,
                    books=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                           for index in range(4)], signal_provider=lambda *args: {}, plan_builder=once,
                    confirmation_events=confirmations)
    assert len(result["orders"]) == 1
    assert not any(row["trade_id"] == "t1" for row in result["fills"])
    if outcome == "CONFIRMED":
        assert D(result["finalState"]["loanPrincipal"]) == 150
    elif outcome == "ABSENT":
        assert D(result["finalState"]["cash"]) == 150
        assert D(result["finalState"]["reserved"]) == 0
    else:
        assert D(result["finalState"]["cash"]) == 0
        assert D(result["finalState"]["reserved"]) == 150


@pytest.mark.parametrize("resolve", [False, True])
def test_unknown_cancel_keeps_funds_reserved_and_old_offer_fillable(monkeypatch, resolve):
    monkeypatch.setattr(EventReplay, "competitive_rate_for_period", lambda *args: D(".0002"))
    active = policy(quick_short_reprice_stages_minutes=tuple(range(1, 11)),
                    minimum_rate_change=D(".0000001"))
    confirmations = [{"mts": START + 90_000, "order_id": "initial", "action": "CANCEL", "outcome": "UNKNOWN"}]
    if resolve:
        confirmations.append({"mts": START + 180_000, "order_id": "initial", "action": "CANCEL",
                              "outcome": "CONFIRMED"})
    result = run_event_replay(
        active, [trade(100_000, "40")], [], D(300), start_ms=START, now_ms=START + 200_000,
        initial_orders=[order("300")], confirmation_events=confirmations,
        book_snapshots=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                        for index in range(4)],
        assumptions=ReplayAssumptions(queue_multiplier=0, require_full_window=False),
        signal_provider=lambda *args: {}, plan_builder=lambda *args, **kwargs: {"plan": []})
    assert D(result["fills"][0]["amount"]) == 40
    assert not any(row["type"] == "CANCEL_CONFIRMED" and row["mts"] < START + 180_000
                   for row in result["events"])
    assert D(result["finalState"]["loanPrincipal"]) == 40
    assert D(result["finalState"]["reserved"]) == 260
    assert len(result["orders"]) == (2 if resolve else 1)
    final = result["finalState"]
    assert D(final["equity"]) == pytest.approx(D(300) + D(result["netInterest"]), abs=D("1e-8"))


def test_period_challenger_clock_advances_even_when_all_cash_is_lent():
    def signals(now, *args):
        scores = [{"period": 2, "totalScore": D(1), "fillScore": D(1), "demandScore": D(1),
                   "relativeDemandShare": D(".5"), "belowDemandThreshold": False},
                  {"period": 7, "totalScore": D(0) if now == START else D(2), "fillScore": D(1),
                   "demandScore": D(1), "relativeDemandShare": D(".5"), "belowDemandThreshold": False}]
        return {"periodSelection": {"byPool": {"short": {
            "insufficientMarketData": False, "absoluteDemandShare": D(1), "belowDemandThreshold": False,
            "selectedPeriod": 2, "fallbackPeriod": 2, "scores": scores}}}}

    result = replay([trade(0, "150")], duration=13 * 60_000,
                    books=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                           for index in range(14)], signal_provider=signals,
                    plan_builder=lambda *args, **kwargs: {"plan": []})
    selected = result["signals"]["periodSelection"]["byPool"]["short"]
    assert D(result["finalState"]["loanPrincipal"]) == 150
    assert selected["selectedPeriod"] == 7
    assert selected["selectedSinceMs"] == START + 11 * 60_000


def test_legacy_chain_walks_shared_curve_without_raising_rate_or_changing_identity(monkeypatch):
    monkeypatch.setattr(EventReplay, "competitive_rate_for_period", lambda *args: D(".0002"))
    active = policy(short_reprice_stages_minutes=tuple(range(1, 11)), reprice_cooldown_minutes=1,
                    max_reprices_per_hour=90, minimum_rate_change=D(".0000001"))
    result = run_event_replay(
        active, [], [], D(300), start_ms=START, now_ms=START + 25 * 60_000,
        initial_orders=[order("300", pricing_curve_version="LEGACY")],
        book_snapshots=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                        for index in range(26)],
        assumptions=ReplayAssumptions(queue_multiplier=0, delay_seconds=0),
        signal_provider=lambda *args: {}, plan_builder=lambda *args, **kwargs: {"plan": []})
    assert len(result["orders"]) > 3
    assert all(row["pricing_curve_version"] == "LEGACY" for row in result["orders"])
    assert all(row["chain_started_at_ms"] == START for row in result["orders"])
    rates = [D(row["effective_rate"]) for row in result["orders"]]
    assert rates == sorted(rates, reverse=True)
    assert any(row["type"] == "REPRICE_STAGE_SKIPPED" for row in result["events"])
    assert max(row.get("stage", 0) for row in result["events"]) == 10
    assert D(result["finalState"]["reserved"]) == 300


def test_partial_fill_during_cancel_cannot_repost_subminimum_remainder(monkeypatch):
    monkeypatch.setattr(EventReplay, "competitive_rate_for_period", lambda *args: D(".0002"))
    active = policy(quick_short_reprice_stages_minutes=tuple(range(1, 11)),
                    minimum_rate_change=D(".0000001"))
    result = run_event_replay(
        active, [trade(90_000, "200")], [], D(300), start_ms=START, now_ms=START + 240_000,
        initial_orders=[order("300")],
        book_snapshots=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                        for index in range(5)], assumptions=ReplayAssumptions(queue_multiplier=0),
        signal_provider=lambda *args: {}, plan_builder=lambda *args, **kwargs: {"plan": []})
    assert len(result["orders"]) == 1
    assert result["orders"][0]["state"] == "CANCELED"
    assert D(result["finalState"]["cash"]) == 100
    assert D(result["finalState"]["reserved"]) == 0


def test_active_orders_and_credits_both_consume_global_type_and_layer_exposure():
    observations = []
    result = replay([trade(1000, "150")], principal="450",
                    initial=[order(hidden=True, flags=64, offer_type="FRRDELTAVAR")],
                    books=[{"mts": START + index * 60_000, "observed_at_ms": START + index * 60_000, "book": []}
                           for index in range(5)], signal_provider=lambda *args: {},
                    plan_builder=lambda *args, **kwargs: observations.append(kwargs) or {"plan": []})
    assert len(observations) > 1
    assert observations[0]["offer_exposure_by_period"] == {2: D(150)}
    assert observations[0]["offer_exposure_by_layer"] == {"quick": D(150)}
    assert observations[-1]["existing_exposure"] == {"total": D(150), "hidden": D(150), "variable": D(150)}
    assert observations[-1]["exposure_by_layer"] == {"quick": D(150)}
    assert D(result["finalState"]["loanPrincipal"]) == 150


def test_real_new_quick_planner_uses_verified_window_and_preserves_its_curve():
    active = replace(policy(), quick_pricing_mode="EXECUTABLE_PREMIUM_V1")
    warmup = [{"id": f"warm{index}", "mts": START - 7 * DAY_MS + index * DAY_MS // 5,
               "rate": D(".001"), "amount": D(1000), "period": 2} for index in range(35)]
    result = run_event_replay(
        active, warmup, [], D(900), start_ms=START, now_ms=START + 60_000,
        book_snapshots=[{"mts": START, "observed_at_ms": START,
                         "book": [{"rate": D(".0011"), "period": 2, "amount": D(5000)},
                                  {"rate": D(".001"), "period": 2, "amount": D(-5000)}]}],
        market_coverage={"coveredIntervals": [{"startMs": START - 7 * DAY_MS, "endMs": START + 60_000}]},
        assumptions=ReplayAssumptions(queue_multiplier=0))
    quick = [row for row in result["orders"] if row["layer"] == "quick" and row["pool"] == "short"]
    assert quick
    assert all(row["pricing_curve_version"] == QUICK_EXECUTION_CURVE for row in quick)
    assert all(D(row["effective_rate"]) > D(row["fixed_landing_rate"]) for row in quick)
    assert result["signals"]["completeWindowUsable"]
