from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PortfolioSnapshot:
    equity: float
    open_risk_pct: float
    open_positions: int
