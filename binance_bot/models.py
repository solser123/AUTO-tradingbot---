from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TradeSignal:
    symbol: str
    side: str
    entry_price: float
    stop_price: float
    target_price: float
    rr: float
    setup_type: str
    reason: str
    strategy_data: dict[str, float | str | bool]


@dataclass(frozen=True)
class AIReview:
    approved: bool
    confidence: float
    reason: str


@dataclass
class Position:
    symbol: str
    side: str
    quantity: float
    entry_price: float
    stop_price: float
    target_price: float
    opened_at: datetime
    mode: str
    status: str = "OPEN"
    id: int | None = None


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class MarketScan:
    symbol: str
    signal: TradeSignal | None
    reasons: list[str]
    metrics: dict[str, float | str | bool]
