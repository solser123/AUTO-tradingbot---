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
    entry_profile: str
    reason: str
    strategy_data: dict[str, float | str | bool]


@dataclass(frozen=True)
class AIReview:
    approved: bool
    confidence: float
    recommended_action: str
    reason: str
    committee: dict[str, float | str | bool]


@dataclass(frozen=True)
class AIScanReview:
    approved: bool
    confidence: float
    suggested_side: str
    setup_bias: str
    reason: str
    committee: dict[str, float | str | bool]


@dataclass
class Position:
    symbol: str
    side: str
    quantity: float
    entry_price: float
    stop_price: float
    target_price: float
    entry_profile: str
    profile_stage: str
    half_defense_trigger: float
    full_defense_trigger: float
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
