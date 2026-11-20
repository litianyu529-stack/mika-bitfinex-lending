from __future__ import annotations

import json
import random
from collections import deque
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Iterable

from .config import V4Policy
from .market import build_market_snapshot
from .store import V4Store
from .strategy import MIN_OFFER, build_plan, gross_daily_floor


D = Decimal
DAY_MS = 86_400_000


class EvidenceInsufficient(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayMetrics:
    net_interest: D
    mean_utilization: D
    api_writes: int
    safety_violations: int
    interval_returns: tuple[D, ...]


@dataclass(frozen=True)
class SplitResult:
    name: str
    v3: ReplayMetrics
    v4: ReplayMetrics
    improvement_percent: D
    bootstrap_lower_percent: D
    utilization_change_points: D


@dataclass(frozen=True)
class ValidationReport:
    training: SplitResult
    validation: SplitResult
    test: SplitResult
    passed: bool
    reasons: tuple[str, ...]

    def as_json(self) -> str:
        return json.dumps(asdict(self), default=lambda value: format(value, "f"), ensure_ascii=False, indent=2)


def chronological_boundaries(start_ms: int) -> tuple[int, int, int, int]:
    return start_ms, start_ms + 60 * DAY_MS, start_ms + 75 * DAY_MS, start_ms + 90 * DAY_MS


def paired_bootstrap_lower(v4: Iterable[D], v3: Iterable[D], samples: int = 2_000, seed: int = 404) -> D:
    pairs = [(D(a), D(b)) for a, b in zip(v4, v3)]
    if not pairs:
        return D("0")
    generator = random.Random(seed)
    estimates: list[D] = []
    for _ in range(samples):
        selected = [pairs[generator.randrange(len(pairs))] for _ in pairs]
        base = sum((item[1] for item in selected), D("0"))
        delta = sum((item[0] - item[1] for item in selected), D("0"))
        estimates.append(D("0") if base <= 0 else delta / base * D("100"))
    estimates.sort()
    return estimates[max(0, int(len(estimates) * 0.025) - 1)]


def _funded(orders: Iterable[object], book: list[dict], queue_fraction: D = D("0.5")) -> tuple[D, D, int]:
    borrower_capacity = [
        {
            "period": int(row["period"]),
            "rate": D(row["rate"]),
            "remaining": abs(D(row["amount"])) * queue_fraction,
        }
        for row in book
        if D(row["amount"]) < 0
    ]
    funded = D("0")
    weighted_rate = D("0")
    violations = 0
    # High-rate rungs consume scarce compatible depth first. Each book row is
    # consumed once, so overlapping depth cannot inflate simulated utilization.
    for order in sorted(orders, key=lambda item: (item.period, -item.rate)):
        if order.amount < MIN_OFFER or order.rate <= 0:
            violations += 1
            continue
        needed = order.amount
        for level in sorted(borrower_capacity, key=lambda item: item["rate"], reverse=True):
            if level["period"] != order.period or level["rate"] < order.rate or needed <= 0:
                continue
            amount = min(needed, level["remaining"])
            level["remaining"] -= amount
            needed -= amount
            funded += amount
            weighted_rate += amount * order.rate
    return funded, weighted_rate, violations


def _replay(
    policy: V4Policy,
    trades: list[dict],
    books: list[tuple[int, list[dict]]],
    start_ms: int,
    end_ms: int,
    version: str,
    principal: D = D("10000"),
) -> ReplayMetrics:
    history: deque[dict] = deque()
    trade_index = 0
    returns: list[D] = []
    utilizations: list[D] = []
    writes = 0
    violations = 0
    previous_shape: tuple = ()
    state = None
    eligible_books = [(mts, rows) for mts, rows in books if start_ms <= mts < end_ms]
    for index, (mts, book) in enumerate(eligible_books):
        while trade_index < len(trades) and int(trades[trade_index]["mts"]) <= mts:
            history.append(trades[trade_index])
            trade_index += 1
        while history and int(history[0]["mts"]) < mts - DAY_MS:
            history.popleft()
        market = build_market_snapshot(book, list(history), policy, now_ms=mts, last_update_ms=mts)
        plan = build_plan(policy, market, principal, principal, state)
        state = plan.state
        orders = plan.orders
        if version == "v3" and orders:
            short = [item for item in orders if item.pool == "short"]
            chosen = min(short or list(orders), key=lambda item: (item.rate, item.period))
            orders = (
                type(chosen)(
                    key="v3-baseline",
                    pool="short",
                    rung_index=0,
                    amount=principal,
                    rate=chosen.rate,
                    period=chosen.period,
                    group_generation=1,
                ),
            )
        shape = tuple((item.pool, item.amount, item.rate, item.period) for item in orders)
        if shape != previous_shape:
            writes += len(previous_shape) + len(shape)
            previous_shape = shape
        funded, weighted_rate, invalid = _funded(orders, book)
        violations += invalid
        for item in orders:
            if item.rate < gross_daily_floor(policy.floor_apr_percent(item.pool), policy.normal_fee_percent):
                violations += 1
        next_mts = eligible_books[index + 1][0] if index + 1 < len(eligible_books) else min(end_ms, mts + 60_000)
        elapsed_days = D(max(0, min(300_000, next_mts - mts))) / D(DAY_MS)
        returns.append(weighted_rate * elapsed_days * (D("1") - policy.fee_fraction))
        utilizations.append(funded / principal if principal > 0 else D("0"))
    return ReplayMetrics(
        net_interest=sum(returns, D("0")),
        mean_utilization=sum(utilizations, D("0")) / D(len(utilizations)) if utilizations else D("0"),
        api_writes=writes,
        safety_violations=violations,
        interval_returns=tuple(returns),
    )


def _split(
    name: str, policy: V4Policy, trades: list[dict], books: list[tuple[int, list[dict]]], start: int, end: int
) -> SplitResult:
    v3 = _replay(policy, trades, books, start, end, "v3")
    v4 = _replay(policy, trades, books, start, end, "v4")
    improvement = D("0") if v3.net_interest <= 0 else (v4.net_interest / v3.net_interest - D("1")) * D("100")
    return SplitResult(
        name=name,
        v3=v3,
        v4=v4,
        improvement_percent=improvement,
        bootstrap_lower_percent=paired_bootstrap_lower(v4.interval_returns, v3.interval_returns),
        utilization_change_points=(v4.mean_utilization - v3.mean_utilization) * D("100"),
    )


def validate_90_days(policy: V4Policy, trades: list[dict], books: list[tuple[int, list[dict]]]) -> ValidationReport:
    if not trades or not books:
        raise EvidenceInsufficient("需要真实 Funding Trades 与分钟盘口快照")
    if any(not rows for _, rows in books):
        raise EvidenceInsufficient("盘口快照不能为空")
    start = max(min(int(row["mts"]) for row in trades), min(mts for mts, _ in books))
    boundaries = chronological_boundaries(start)
    if max(mts for mts, _ in books) < boundaries[-1] - 60_000:
        raise EvidenceInsufficient("真实分钟盘口覆盖不足 90 天；不会使用合成盘口替代")
    training = _split("training-60d", policy, trades, books, boundaries[0], boundaries[1])
    validation = _split("validation-15d", policy, trades, books, boundaries[1], boundaries[2])
    test = _split("test-15d", policy, trades, books, boundaries[2], boundaries[3])
    reasons: list[str] = []
    for result in (validation, test):
        if result.improvement_percent < D("3"):
            reasons.append(f"{result.name}: net improvement below 3%")
        if result.bootstrap_lower_percent <= 0:
            reasons.append(f"{result.name}: paired bootstrap 95% lower bound is not positive")
        if result.utilization_change_points < D("-2"):
            reasons.append(f"{result.name}: utilization dropped by more than 2 percentage points")
        if result.v4.safety_violations:
            reasons.append(f"{result.name}: {result.v4.safety_violations} safety violation(s)")
    return ValidationReport(training, validation, test, not reasons, tuple(reasons))


def load_real_evidence(store: V4Store) -> tuple[list[dict], list[tuple[int, list[dict]]]]:
    with store.connect() as db:
        trades = [
            {
                "id": row["trade_id"],
                "mts": row["mts"],
                "amount": D(row["amount"]),
                "rate": D(row["rate"]),
                "period": row["period"],
            }
            for row in db.execute("SELECT * FROM market_trades ORDER BY mts,trade_id")
        ]
        rows = db.execute(
            "SELECT mts,rate,period,count,amount,source FROM book_snapshots "
            "WHERE source IN ('real','v3') ORDER BY mts,id"
        ).fetchall()
    grouped: list[tuple[int, list[dict]]] = []
    for row in rows:
        if not grouped or grouped[-1][0] != row["mts"]:
            grouped.append((row["mts"], []))
        grouped[-1][1].append(
            {
                "rate": D(row["rate"]),
                "period": row["period"],
                "count": row["count"],
                "amount": D(row["amount"]),
            }
        )
    return trades, grouped


def shadow_audit(store: V4Store, required_days: int = 7, policy: V4Policy | None = None) -> dict[str, object]:
    policy = policy or V4Policy()
    with store.connect() as db:
        span = db.execute("SELECT MIN(mts),MAX(mts),COUNT(*) FROM shadow_plans").fetchone()
        duplicates = db.execute(
            "SELECT COUNT(*) FROM (SELECT mts,fingerprint,COUNT(*) n FROM shadow_plans "
            "GROUP BY mts,fingerprint HAVING n>1)"
        ).fetchone()[0]
        writes = db.execute(
            "SELECT COUNT(*) FROM execution_intents WHERE created_at_ms BETWEEN COALESCE(?,0) AND COALESCE(?,0)",
            (span[0], span[1]),
        ).fetchone()[0]
        max_rebuilds = db.execute(
            "SELECT COALESCE(MAX(n),0) FROM (SELECT pool,mts/3600000 hour,COUNT(*) n "
            "FROM rebuild_events WHERE mts BETWEEN COALESCE(?,0) AND COALESCE(?,0) GROUP BY pool,hour)",
            (span[0], span[1]),
        ).fetchone()[0]
        payloads = [row[0] for row in db.execute("SELECT payload FROM shadow_plans ORDER BY mts")]
        reconciliation = db.execute(
            "SELECT COALESCE(MAX(ABS(CAST(wallet_total AS REAL) - "
            "(CAST(wallet_available AS REAL)+CAST(offers_total AS REAL)+CAST(credits_total AS REAL)+"
            "CAST(loans_total AS REAL)))),0) FROM account_samples "
            "WHERE authoritative=1 AND mts BETWEEN COALESCE(?,0) AND COALESCE(?,0)",
            (span[0], span[1]),
        ).fetchone()[0]
    safety_violations = 0
    from .store import plan_from_json

    for payload in payloads:
        plan = plan_from_json(payload)
        if (
            plan.planned_amount > plan.deployable
            or sum((item.amount for item in plan.orders), D("0")) != plan.planned_amount
        ):
            safety_violations += 1
        for order in plan.orders:
            floor = gross_daily_floor(policy.floor_apr_percent(order.pool), policy.normal_fee_percent)
            if order.amount < MIN_OFFER or order.rate < floor:
                safety_violations += 1
    duration_days = D("0") if not span[0] or not span[1] else D(span[1] - span[0]) / D(DAY_MS)
    ready = bool(
        duration_days >= required_days
        and not duplicates
        and not writes
        and max_rebuilds <= 6
        and not safety_violations
        and D(str(reconciliation)) <= D("0.01")
    )
    return {
        "start_ms": int(span[0] or 0),
        "end_ms": int(span[1] or 0),
        "duration_days": format(duration_days, ".3f"),
        "plans": int(span[2]),
        "duplicate_intents": int(duplicates),
        "exchange_write_intents": int(writes),
        "max_rebuilds_per_pool_hour": int(max_rebuilds),
        "safety_violations": safety_violations,
        "max_account_reconciliation_difference": format(D(str(reconciliation)), "f"),
        "ready_for_manual_review": ready,
    }
