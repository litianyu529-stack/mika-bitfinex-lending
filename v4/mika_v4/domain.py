from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


D = Decimal


class RuntimeMode(str, Enum):
    PAUSED = "PAUSED"
    SHADOW = "SHADOW"
    LIVE = "LIVE"
    SAFE = "SAFE"


class IntentState(str, Enum):
    PLANNED = "PLANNED"
    SUBMITTING = "SUBMITTING"
    CONFIRMED = "CONFIRMED"
    CLOSED = "CLOSED"
    AMBIGUOUS = "AMBIGUOUS"
    REJECTED = "REJECTED"


class WriteOutcome(str, Enum):
    CONFIRMED = "CONFIRMED"
    DEFINITE_REJECT = "DEFINITE_REJECT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class WriteResult:
    outcome: WriteOutcome
    response: Any = None
    error: str | None = None
    category: str = ""
    retryable: bool = False


@dataclass(frozen=True)
class OfferSnapshot:
    offer_id: int
    currency: str
    amount: D
    amount_original: D
    rate: D
    period: int
    status: str
    mts_created: int
    offer_type: str = "LIMIT"
    flags: int = 0
    hidden: bool = False

    @property
    def filled_amount(self) -> D:
        return max(D("0"), self.amount_original - self.amount)

    @property
    def fill_fraction(self) -> D:
        return D("0") if self.amount_original <= 0 else self.filled_amount / self.amount_original


@dataclass(frozen=True)
class CreditSnapshot:
    credit_id: int
    currency: str
    amount: D
    rate: D
    period: int
    mts_opening: int
    funding_state: str = "credit"


@dataclass(frozen=True)
class AccountSnapshot:
    as_of_ms: int
    wallet_available: D
    wallet_total: D
    offers: tuple[OfferSnapshot, ...] = ()
    credits: tuple[CreditSnapshot, ...] = ()
    loans: tuple[CreditSnapshot, ...] = ()
    authoritative: bool = True


@dataclass(frozen=True)
class MarketSnapshot:
    as_of_ms: int
    best_borrower_rate: D
    vwap_5m: D
    median_5m: D
    median_1h: D
    median_6h: D
    q25_24h: D
    q75_24h: D
    robust_anchor: D
    grid_step: D
    supported_ceiling: D
    period_scores: dict[int, D] = field(default_factory=dict)
    valid_components: int = 0
    fresh: bool = False

    @property
    def rising(self) -> bool:
        return bool(
            self.median_5m > 0
            and self.median_1h > 0
            and self.median_6h > 0
            and self.median_5m >= self.median_1h >= self.median_6h
        )


@dataclass(frozen=True)
class PeriodChoice:
    current: int | None = None
    candidate: int | None = None
    confirmations: int = 0


@dataclass(frozen=True)
class LongGateState:
    tier: int = 0
    candidate_tier: int = 0
    confirmations: int = 0


@dataclass(frozen=True)
class PlannerState:
    short_period: PeriodChoice = field(default_factory=PeriodChoice)
    medium_period: PeriodChoice = field(default_factory=PeriodChoice)
    long_gate: LongGateState = field(default_factory=LongGateState)


@dataclass(frozen=True)
class PlannedOffer:
    key: str
    pool: str
    rung_index: int
    amount: D
    rate: D
    period: int
    group_generation: int = 1


@dataclass(frozen=True)
class AllocationPlan:
    as_of_ms: int
    anchor: D
    step: D
    deployable: D
    planned_amount: D
    idle_amount: D
    long_tier: int
    orders: tuple[PlannedOffer, ...]
    state: PlannerState
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutionIntent:
    fingerprint: str
    action: str
    offer_key: str | None = None
    offer_id: int | None = None
    amount: D | None = None
    rate: D | None = None
    period: int | None = None
    state: IntentState = IntentState.PLANNED
    created_at_ms: int = 0


@dataclass(frozen=True)
class GridRung:
    group_id: str
    generation: int
    pool: str
    rung_index: int
    amount_original: D
    amount_remaining: D
    rate: D
    period: int
    offer_id: int | None = None
    status: str = "PLANNED"
    floor_reached_at_ms: int | None = None


@dataclass(frozen=True)
class GridGroup:
    group_id: str
    generation: int
    pool: str
    anchor: D
    step: D
    period: int
    created_at_ms: int
    rungs: tuple[GridRung, ...]


@dataclass(frozen=True)
class StrategyStatus:
    mode: RuntimeMode
    market: MarketSnapshot | None
    account: AccountSnapshot | None
    plan: AllocationPlan | None
    safe_reason: str | None = None
