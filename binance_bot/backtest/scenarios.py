from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BacktestScenario:
    name: str
    fee_enabled: bool = True
    funding_enabled: bool = True
    slippage_enabled: bool = True
