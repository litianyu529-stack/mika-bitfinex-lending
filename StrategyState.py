"""Pure persistent decision transitions shared by LIVE and event replay.

Inputs are previous state snapshots; returned dictionaries are independent new
states. Persistence, market signal construction and clocks belong to callers.
"""

from decimal import Decimal


D = Decimal
DEMAND_CYCLE_MS = 300_000


def period_selection_transition(previous, selected_period, scores, now_ms, *,
                                advantage=D("0.20"), hold_ms=600_000):
    """Hold a challenger with a 20% lead continuously before switching terms."""
    now = int(now_ms)
    previous = None if previous is None else dict(previous)
    period = None if selected_period is None else int(selected_period)
    score_by_period = {
        int(row["period"]): D(row.get("totalScore") or 0)
        for row in scores or []
        if row.get("period") is not None and row.get("eligible", True)
    }
    active = None if previous is None else previous.get("selected_period")
    selected_since = (
        now if previous is None or previous.get("selected_since_ms") is None
        else int(previous["selected_since_ms"])
    )
    challenger = None
    challenger_since = None
    promoted = False
    if period is None:
        active = None
        selected_since = now
    elif previous is None or active is None:
        active = period
        selected_since = now
    elif int(active) != period:
        old_score = score_by_period.get(int(active))
        new_score = score_by_period.get(period)
        qualifies = new_score is not None and (
            old_score is None or (new_score > old_score and new_score >= old_score * (D(1) + D(advantage)))
        )
        if qualifies:
            challenger = period
            challenger_since = (
                int(previous["challenger_since_ms"])
                if previous.get("challenger_period") == period and previous.get("challenger_since_ms") is not None
                else now
            )
            if now - challenger_since >= int(hold_ms):
                active = period
                selected_since = now
                challenger = None
                challenger_since = None
                promoted = True
    return {
        "selected_period": None if active is None else int(active), "selected_since_ms": selected_since,
        "challenger_period": challenger, "challenger_since_ms": challenger_since,
        "leader_period": period, "promoted": promoted, "updated_at_ms": now,
    }


def demand_confirmation_transition(previous, below_threshold, now_ms, *, required_cycles=2):
    """Confirm only distinct, consecutive five-minute low-demand buckets."""
    previous = {} if previous is None else dict(previous)
    cycle = int(now_ms) // DEMAND_CYCLE_MS
    count = int(previous.get("consecutive_cycles") or 0)
    last_cycle = previous.get("last_cycle")
    if below_threshold is None or not bool(below_threshold):
        count = 0
    elif last_cycle != cycle:
        count = count + 1 if last_cycle == cycle - 1 else 1
    return {"consecutive_cycles": count, "confirmed": bool(count >= int(required_cycles)), "last_cycle": cycle}
