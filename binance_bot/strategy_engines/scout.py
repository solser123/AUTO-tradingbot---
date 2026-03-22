from __future__ import annotations

from .models import EngineAssessment


def assess_scout(signal, scan, ai_scan_review) -> EngineAssessment | None:
    if signal is None or ai_scan_review is None:
        return None
    if not signal.setup_type.startswith("ai_") and signal.setup_type not in {"context_recovery"}:
        return None
    confidence_hint = min(0.9, max(float(ai_scan_review.confidence or 0.0), 0.45))
    return EngineAssessment(
        engine_key="scout",
        engine_family="scout",
        priority=80,
        confidence_hint=confidence_hint,
        exploratory_preferred=True,
        reason="Scout engine promoted an AI-assisted exploratory candidate.",
    )
