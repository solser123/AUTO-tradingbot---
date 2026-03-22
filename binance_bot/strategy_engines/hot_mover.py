from __future__ import annotations

from .models import EngineAssessment


def assess_hot_mover(signal, hot_mover_candidate) -> EngineAssessment | None:
    if signal is None or hot_mover_candidate is None:
        return None
    confidence_hint = min(0.92, 0.55 + min(abs(float(hot_mover_candidate.pct_change_24h or 0.0)) / 100.0, 1.5) * 0.18)
    return EngineAssessment(
        engine_key="hot_mover",
        engine_family="scout",
        priority=95,
        confidence_hint=confidence_hint,
        exploratory_preferred=True,
        reason="Hot mover engine selected a dynamic high-volatility candidate.",
    )
