from __future__ import annotations

import hashlib
from decimal import ROUND_DOWN, Decimal

from .config import V4Policy, validate_policy
from .domain import AllocationPlan, LongGateState, MarketSnapshot, PeriodChoice, PlannedOffer, PlannerState
from .market import RATE_TICK, ceil_tick


D = Decimal
CENT = D("0.00000001")
MIN_OFFER = D("150")
LONG_TIER_STEP = D("0.001") / D("100")


def gross_daily_floor(apr_percent: D, fee_percent: D) -> D:
    fee = D(fee_percent) / D("100")
    if fee < 0 or fee >= 1:
        raise ValueError("fee must be in [0, 100)")
    return ceil_tick(D(apr_percent) / D("100") / D("365") / (D("1") - fee))


def net_apr_percent(daily_rate: D, fee_percent: D) -> D:
    return D(daily_rate) * D("365") * (D("1") - D(fee_percent) / D("100")) * D("100")


def equal_amounts(total: D, count: int) -> tuple[D, ...]:
    total = max(D("0"), D(total)).quantize(CENT, rounding=ROUND_DOWN)
    count = max(0, int(count))
    if count <= 0 or total < MIN_OFFER * count:
        return ()
    units = int(total / CENT)
    base, remainder = divmod(units, count)
    amounts = [D(base) * CENT for _ in range(count)]
    amounts[0] += D(remainder) * CENT
    return tuple(amounts)


def next_period_choice(
    current: PeriodChoice,
    candidates: tuple[int, ...],
    scores: dict[int, D],
    advantage: D = D("0.20"),
) -> PeriodChoice:
    if not candidates:
        return PeriodChoice()
    best = max(candidates, key=lambda period: (D(scores.get(period, 0)), -period))
    if current.current not in candidates:
        return PeriodChoice(current=best)
    if best == current.current:
        return PeriodChoice(current=current.current)
    current_score = D(scores.get(current.current, 0))
    best_score = D(scores.get(best, 0))
    required = current_score * (D("1") + advantage)
    if best_score <= 0 or best_score < required:
        return PeriodChoice(current=current.current)
    confirmations = current.confirmations + 1 if current.candidate == best else 1
    if confirmations >= 2:
        return PeriodChoice(current=best)
    return PeriodChoice(current=current.current, candidate=best, confirmations=confirmations)


def raw_long_tier(policy: V4Policy, market: MarketSnapshot) -> int:
    threshold = gross_daily_floor(policy.long_floor_apr_percent, policy.normal_fee_percent)
    if market.robust_anchor < threshold:
        return 0
    return min(3, 1 + int((market.robust_anchor - threshold) // LONG_TIER_STEP))


def next_long_gate(policy: V4Policy, market: MarketSnapshot, current: LongGateState) -> LongGateState:
    raw = raw_long_tier(policy, market)
    if raw <= current.tier:
        return LongGateState(tier=raw)
    if not market.rising:
        return LongGateState(tier=current.tier)
    confirmations = current.confirmations + 1 if current.candidate_tier == raw else 1
    if confirmations >= 2:
        return LongGateState(tier=raw)
    return LongGateState(tier=current.tier, candidate_tier=raw, confirmations=confirmations)


def _pool_eligible(policy: V4Policy, market: MarketSnapshot, pool: str) -> bool:
    return bool(
        market.fresh
        and market.valid_components >= 2
        and market.supported_ceiling >= gross_daily_floor(policy.floor_apr_percent(pool), policy.normal_fee_percent)
    )


def _allocate_pool_amounts(policy: V4Policy, market: MarketSnapshot, deployable: D, long_tier: int) -> dict[str, D]:
    eligible = {pool: _pool_eligible(policy, market, pool) for pool in ("short", "medium", "long")}
    long_share = D(long_tier * 10) if eligible["long"] else D("0")
    long_amount = (deployable * long_share / D("100")).quantize(CENT, rounding=ROUND_DOWN)
    if long_amount < MIN_OFFER:
        long_amount = D("0")
    remainder = deployable - long_amount
    weights = {
        "short": policy.short_weight if eligible["short"] else D("0"),
        "medium": policy.medium_weight if eligible["medium"] else D("0"),
    }
    total_weight = sum(weights.values(), D("0"))
    amounts = {"short": D("0"), "medium": D("0"), "long": long_amount}
    if total_weight <= 0:
        return amounts
    amounts["short"] = (remainder * weights["short"] / total_weight).quantize(CENT, rounding=ROUND_DOWN)
    amounts["medium"] = remainder - amounts["short"]
    # A pool that cannot create one valid offer releases its amount to the other
    # eligible pool. This preserves utilization without violating the floor.
    undersized = [pool for pool in ("short", "medium") if D("0") < amounts[pool] < MIN_OFFER]
    if len(undersized) == 2:
        preferred = max(("short", "medium"), key=lambda pool: (weights[pool], pool == "short"))
        other = "medium" if preferred == "short" else "short"
        amounts[preferred] = amounts["short"] + amounts["medium"]
        amounts[other] = D("0")
    for pool, other in (("short", "medium"), ("medium", "short")):
        if D("0") < amounts[pool] < MIN_OFFER:
            amounts[other] += amounts[pool]
            amounts[pool] = D("0")
    if D("0") < amounts["long"] < MIN_OFFER:
        amounts["short"] += amounts["long"]
        amounts["long"] = D("0")
    return amounts


def _generation_key(pool: str, as_of_ms: int, amount: D, period: int) -> str:
    payload = f"{pool}:{as_of_ms}:{format(amount, 'f')}:{period}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def build_plan(
    policy: V4Policy,
    market: MarketSnapshot,
    deployable: D,
    total_principal: D | None = None,
    state: PlannerState | None = None,
    target_pool: str | None = None,
    existing_committed: D = D("0"),
) -> AllocationPlan:
    validate_policy(policy)
    state = state or PlannerState()
    requested = max(D("0"), D(deployable))
    principal = requested if total_principal is None else max(D("0"), D(total_principal))
    committed = max(D("0"), D(existing_committed))
    cap = principal * policy.max_lend_percent / D("100")
    if policy.max_lend_amount is not None:
        cap = min(cap, policy.max_lend_amount)
    available = min(requested, max(D("0"), cap - committed))
    short_choice = next_period_choice(state.short_period, policy.short_periods, market.period_scores)
    medium_choice = next_period_choice(state.medium_period, policy.medium_periods, market.period_scores)
    gate = next_long_gate(policy, market, state.long_gate)
    next_state = PlannerState(short_period=short_choice, medium_period=medium_choice, long_gate=gate)
    reasons: list[str] = []
    if not market.fresh or market.valid_components < 2:
        reasons.append("MARKET_DATA_INSUFFICIENT")
        return AllocationPlan(
            as_of_ms=market.as_of_ms,
            anchor=market.robust_anchor,
            step=market.grid_step,
            deployable=requested,
            planned_amount=D("0"),
            idle_amount=requested,
            long_tier=gate.tier,
            orders=(),
            state=next_state,
            reasons=tuple(reasons),
        )
    if target_pool is not None:
        if target_pool not in {"short", "medium"}:
            raise ValueError("target_pool must be short or medium")
        amounts = {"short": D("0"), "medium": D("0"), "long": D("0")}
        if _pool_eligible(policy, market, target_pool):
            amounts[target_pool] = available
    else:
        amounts = _allocate_pool_amounts(policy, market, available, gate.tier)
    orders: list[PlannedOffer] = []
    choices = {"short": short_choice.current, "medium": medium_choice.current}
    for pool in ("short", "medium"):
        amount = amounts[pool]
        period = choices[pool]
        if amount < MIN_OFFER or period is None:
            continue
        count = min(policy.max_rungs(pool), int(amount // MIN_OFFER))
        split = equal_amounts(amount, count)
        floor = gross_daily_floor(policy.floor_apr_percent(pool), policy.normal_fee_percent)
        bottom = ceil_tick(max(market.robust_anchor, floor))
        generation = _generation_key(pool, market.as_of_ms, amount, period)
        for index, chunk in enumerate(split):
            orders.append(
                PlannedOffer(
                    key=f"{generation}:{pool}:{index}",
                    pool=pool,
                    rung_index=index,
                    amount=chunk,
                    rate=ceil_tick(bottom + market.grid_step * D(index)),
                    period=period,
                )
            )
    if amounts["long"] >= MIN_OFFER and gate.tier > 0:
        floor = gross_daily_floor(policy.long_floor_apr_percent, policy.normal_fee_percent)
        generation = _generation_key("long", market.as_of_ms, amounts["long"], policy.long_period)
        orders.append(
            PlannedOffer(
                key=f"{generation}:long:0",
                pool="long",
                rung_index=0,
                amount=amounts["long"],
                rate=ceil_tick(max(floor, market.robust_anchor)),
                period=policy.long_period,
            )
        )
    planned = sum((order.amount for order in orders), D("0"))
    if requested > available:
        reasons.append("LENDING_CAP_APPLIED")
    if not orders and requested >= MIN_OFFER:
        reasons.append("NO_ELIGIBLE_POOL")
    return AllocationPlan(
        as_of_ms=market.as_of_ms,
        anchor=market.robust_anchor,
        step=market.grid_step,
        deployable=requested,
        planned_amount=planned,
        idle_amount=max(D("0"), requested - planned),
        long_tier=gate.tier,
        orders=tuple(orders),
        state=next_state,
        reasons=tuple(reasons),
    )


def bottom_rung_triggered(amount_original: D, amount_remaining: D, trigger_percent: D) -> bool:
    original = D(amount_original)
    remaining = max(D("0"), D(amount_remaining))
    if original <= 0:
        return False
    filled_percent = (original - remaining) / original * D("100")
    return bool(filled_percent >= D(trigger_percent) or (remaining > 0 and remaining < MIN_OFFER) or remaining == 0)


def fast_shift_allowed(old_anchor: D, market: MarketSnapshot) -> bool:
    return bool(market.fresh and market.robust_anchor <= D(old_anchor) + RATE_TICK)


def floor_stale(
    pool: str,
    rate: D,
    floor_rate: D,
    floor_reached_at_ms: int | None,
    now_ms: int,
    policy: V4Policy,
) -> bool:
    if floor_reached_at_ms is None or D(rate) > D(floor_rate) + RATE_TICK:
        return False
    minutes = getattr(policy, f"{pool}_floor_stale_minutes")
    return int(now_ms) - int(floor_reached_at_ms) >= int(minutes) * 60_000
