from __future__ import annotations

from .models import EngineAssessment


REVERSAL_SETUPS = {
    "early_reversal",
    "smc_reversal",
}


def assess_reversal(signal, scan) -> EngineAssessment | None:
    if signal is None:
        return None
    if signal.setup_type not in REVERSAL_SETUPS:
        return None
    metrics = scan.metrics
    zscore = abs(float(metrics.get("session_vwap_zscore", 0.0) or 0.0))
    squeeze_release = bool(metrics.get("squeeze_off", False))
    confidence_hint = min(0.9, 0.50 + min(zscore, 3.0) * 0.05 + (0.08 if squeeze_release else 0.0))
    return EngineAssessment(
        engine_key="reversal",
        engine_family="reversal",
        priority=75,
        confidence_hint=confidence_hint,
        exploratory_preferred=True,
        reason="Reversal engine selected an early or SMC-style reversal setup.",
    )
