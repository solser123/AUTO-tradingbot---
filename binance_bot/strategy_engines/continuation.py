from __future__ import annotations

from .models import EngineAssessment


CONTINUATION_SETUPS = {
    "continuation",
    "pullback_recovery",
    "breakout_confirmation",
    "breakdown_confirmation",
}


def assess_continuation(signal, scan) -> EngineAssessment | None:
    if signal is None:
        return None
    if signal.setup_type not in CONTINUATION_SETUPS:
        return None
    metrics = scan.metrics
    volume_ratio = float(metrics.get("volume_ratio", 0.0) or 0.0)
    impulse = bool(signal.strategy_data.get("impulse_confirmed", False))
    resume = bool(signal.strategy_data.get("resume_confirmed", False))
    confidence_hint = min(0.95, 0.52 + (0.08 if impulse else 0.0) + (0.06 if resume else 0.0) + min(volume_ratio, 2.0) * 0.06)
    return EngineAssessment(
        engine_key="continuation",
        engine_family="continuation",
        priority=70,
        confidence_hint=confidence_hint,
        exploratory_preferred=False,
        reason="Continuation engine selected a structured trend-following setup.",
    )
