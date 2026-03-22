from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EngineAssessment:
    engine_key: str
    engine_family: str
    priority: int
    confidence_hint: float
    exploratory_preferred: bool
    reason: str
