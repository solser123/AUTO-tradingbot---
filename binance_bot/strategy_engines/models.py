from __future__ import annotations

from dataclasses import dataclass

from ..models import TradeSignal


@dataclass(frozen=True)
class EngineAssessment:
    engine_key: str
    engine_family: str
    priority: int
    confidence_hint: float
    exploratory_preferred: bool
    reason: str


@dataclass(frozen=True)
class EngineSignalDecision:
    engine_key: str
    engine_family: str
    priority: int
    signal: TradeSignal | None
    reasons: tuple[str, ...]
    score: float = 0.0
