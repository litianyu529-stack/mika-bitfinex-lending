from __future__ import annotations

from decimal import Decimal

import pytest

from mika_v4.config import V4Policy
from mika_v4.domain import MarketSnapshot


D = Decimal


@pytest.fixture
def policy() -> V4Policy:
    return V4Policy()


@pytest.fixture
def market() -> MarketSnapshot:
    return MarketSnapshot(
        as_of_ms=1_000_000,
        best_borrower_rate=D("0.00035"),
        vwap_5m=D("0.00034"),
        median_5m=D("0.00034"),
        median_1h=D("0.00033"),
        median_6h=D("0.00032"),
        q25_24h=D("0.00030"),
        q75_24h=D("0.00036"),
        robust_anchor=D("0.00034"),
        grid_step=D("0.00001"),
        supported_ceiling=D("0.00040"),
        period_scores={2: D("1"), 4: D("0.5"), 7: D("0.2"), 14: D("1"), 30: D("0.4")},
        valid_components=3,
        fresh=True,
    )
