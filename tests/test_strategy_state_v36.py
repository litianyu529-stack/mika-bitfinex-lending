import copy
import json
from decimal import Decimal

import pytest

from StateStore import LendingStateStore
from StrategyState import demand_confirmation_transition, period_selection_transition


D = Decimal


def _scores(old="0.50", new="0.60", other="0.40"):
    return [{"period": 2, "totalScore": D(old)}, {"period": 7, "totalScore": D(new)},
            {"period": 4, "totalScore": D(other)}]


def test_period_challenger_requires_twenty_percent_and_full_uninterrupted_ten_minutes():
    first = period_selection_transition(None, 2, _scores(), 1000)
    weak = period_selection_transition(first, 7, _scores(new="0.599"), 60_000)
    assert weak["selected_period"] == 2 and weak["challenger_period"] is None
    started = period_selection_transition(weak, 7, _scores(), 120_000)
    assert started["challenger_since_ms"] == 120_000
    waiting = period_selection_transition(started, 7, _scores(), 719_999)
    assert waiting["selected_period"] == 2 and not waiting["promoted"]
    promoted = period_selection_transition(waiting, 7, _scores(), 720_000)
    assert promoted == {
        "selected_period": 7, "selected_since_ms": 720_000, "challenger_period": None,
        "challenger_since_ms": None, "leader_period": 7, "promoted": True, "updated_at_ms": 720_000,
    }


@pytest.mark.parametrize("interruption", ["old_winner", "weak_lead", "another_challenger"])
def test_period_interrupted_challenge_restarts_hold_time(interruption):
    first = period_selection_transition(None, 2, _scores(), 0)
    challenged = period_selection_transition(first, 7, _scores(), 60_000)
    if interruption == "old_winner":
        reset = period_selection_transition(challenged, 2, _scores(), 600_000)
    elif interruption == "weak_lead":
        reset = period_selection_transition(challenged, 7, _scores(new=".59"), 600_000)
    else:
        reset = period_selection_transition(challenged, 4, _scores(other=".70"), 600_000)
        assert reset["challenger_period"] == 4
    restarted = period_selection_transition(reset, 7, _scores(), 660_000)
    assert restarted["challenger_since_ms"] == 660_000
    assert restarted["selected_period"] == 2
    assert period_selection_transition(restarted, 7, _scores(), 1_260_000)["selected_period"] == 7


def test_period_reset_missing_eligible_score_and_configurable_hold_match_live_rules():
    selected = period_selection_transition(None, 2, _scores(), 1)
    missing_new = period_selection_transition(selected, 7, [{"period": 2, "totalScore": D(".5")}], 2)
    assert missing_new["challenger_period"] is None
    absent_old = period_selection_transition(selected, 7, [{"period": 7, "totalScore": D(".1")}], 3,
                                            hold_ms=0)
    assert absent_old["selected_period"] == 7 and absent_old["promoted"]
    disabled_new = period_selection_transition(
        selected, 7, [{"period": 7, "totalScore": D(1), "eligible": False}], 4,
    )
    assert disabled_new["challenger_period"] is None
    reset = period_selection_transition(selected, None, [], 5)
    assert reset["selected_period"] is None and reset["selected_since_ms"] == 5
    assert period_selection_transition(reset, 4, [], 6)["selected_period"] == 4
    equal = period_selection_transition(selected, 7, _scores(old="0", new="0"), 7, hold_ms=0)
    assert equal["selected_period"] == 2
    customized = period_selection_transition(selected, 7, _scores(new=".55"), 8,
                                             advantage=D(".10"), hold_ms=0)
    assert customized["selected_period"] == 7


def test_pure_period_state_and_persisted_state_match_across_restarts_without_mutating_inputs(tmp_path):
    path = tmp_path / "state.db"
    prior = None
    timeline = [(1000, 2, ".50"), (60_000, 7, ".60"), (659_999, 7, ".60"),
                (660_000, 7, ".60"), (700_000, None, ".60"), (800_000, 4, ".60")]
    for now, leader, score in timeline:
        scores = _scores(new=score)
        before_scores, before_prior = copy.deepcopy(scores), copy.deepcopy(prior)
        pure = period_selection_transition(prior, leader, scores, now)
        assert scores == before_scores and prior == before_prior
        store = LendingStateStore(path)
        result = store.observe_period_selection("strategy", "short", leader, scores, now)
        assert result == {
            "strategyVersion": "strategy", "pool": "short", "selectedPeriod": pure["selected_period"],
            "selectedSinceMs": pure["selected_since_ms"], "leaderPeriod": leader,
            "challengerPeriod": pure["challenger_period"], "challengerSinceMs": pure["challenger_since_ms"],
            "challengerDurationMs": (
                0 if pure["challenger_since_ms"] is None else now - pure["challenger_since_ms"]
            ), "promoted": pure["promoted"], "updatedAtMs": now,
        }
        with store.read_connection() as connection:
            row = connection.execute("SELECT * FROM period_selection_state").fetchone()
            assert row["selected_period"] == pure["selected_period"]
            assert D(json.loads(row["scores_json"])[1]["totalScore"]) == D(score)
        prior = pure


def test_demand_needs_consecutive_distinct_five_minute_buckets_and_resets_on_gaps():
    first = demand_confirmation_transition(None, True, 300_000)
    assert first == {"consecutive_cycles": 1, "confirmed": False, "last_cycle": 1}
    duplicate = demand_confirmation_transition(first, True, 599_999)
    assert duplicate == first
    second = demand_confirmation_transition(duplicate, True, 600_000)
    assert second == {"consecutive_cycles": 2, "confirmed": True, "last_cycle": 2}
    gap = demand_confirmation_transition(second, True, 1_200_000)
    assert gap == {"consecutive_cycles": 1, "confirmed": False, "last_cycle": 4}
    reset = demand_confirmation_transition(gap, False, 1_200_001)
    assert reset["consecutive_cycles"] == 0
    same_bucket = demand_confirmation_transition(reset, True, 1_200_002)
    assert same_bucket["consecutive_cycles"] == 0
    unknown = demand_confirmation_transition(second, None, 900_000)
    assert unknown["consecutive_cycles"] == 0 and not unknown["confirmed"]


def test_demand_pure_and_persisted_transitions_match_and_keep_sql_share_fields(tmp_path):
    path = tmp_path / "state.db"
    prior = None
    for now, low, share in [(300_000, True, D(".01")), (301_000, True, D(".02")),
                            (600_000, True, D(".01")), (900_000, None, None),
                            (1_200_000, True, D(".03")), (1_800_000, True, D(".01"))]:
        before = copy.deepcopy(prior)
        pure = demand_confirmation_transition(prior, low, now, required_cycles=2)
        assert prior == before
        store = LendingStateStore(path)
        result = store.observe_demand_confirmation("strategy", "term", "7", share, low, now)
        assert result == {"cycles": pure["consecutive_cycles"], "confirmed": pure["confirmed"],
                          "cycle": pure["last_cycle"]}
        with store.read_connection() as connection:
            row = connection.execute("SELECT * FROM demand_confirmation_state").fetchone()
            assert row["consecutive_cycles"] == pure["consecutive_cycles"]
            assert row["last_share"] == (None if share is None else format(share, "f"))
            assert demand_confirmation_transition(row, low, now) == pure
        prior = pure


def test_demand_required_cycles_remains_configurable_and_state_keys_are_independent(tmp_path):
    first = demand_confirmation_transition(None, True, 300_000, required_cycles=3)
    second = demand_confirmation_transition(first, True, 600_000, required_cycles=3)
    third = demand_confirmation_transition(second, True, 900_000, required_cycles=3)
    assert not second["confirmed"] and third["confirmed"]
    store = LendingStateStore(tmp_path / "state.db")
    store.observe_demand_confirmation("a", "pool", "short", D(".01"), True, 300_000)
    assert not store.observe_demand_confirmation("a", "pool", "long", D(".01"), True, 600_000)["confirmed"]
    assert not store.observe_demand_confirmation("b", "pool", "short", D(".01"), True, 600_000)["confirmed"]
    assert store.observe_demand_confirmation("a", "pool", "short", D(".01"), True, 600_000)["confirmed"]
