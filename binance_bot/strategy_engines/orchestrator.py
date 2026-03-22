from __future__ import annotations

from .continuation import assess_continuation
from .hot_mover import assess_hot_mover
from .models import EngineAssessment
from .reversal import assess_reversal
from .scout import assess_scout


class StrategyEngineOrchestrator:
    def assess(self, *, signal, scan, hot_mover_candidate=None, ai_scan_review=None) -> EngineAssessment | None:
        candidates: list[EngineAssessment] = []
        for assessment in (
            assess_hot_mover(signal, hot_mover_candidate),
            assess_reversal(signal, scan),
            assess_continuation(signal, scan),
            assess_scout(signal, scan, ai_scan_review),
        ):
            if assessment is not None:
                candidates.append(assessment)
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item.priority, item.confidence_hint))

    def annotate_signal(self, signal, assessment: EngineAssessment | None) -> None:
        if signal is None or assessment is None:
            return
        signal.strategy_data["engine_key"] = assessment.engine_key
        signal.strategy_data["engine_family"] = assessment.engine_family
        signal.strategy_data["engine_priority"] = assessment.priority
        signal.strategy_data["engine_confidence_hint"] = round(assessment.confidence_hint, 4)
        signal.strategy_data["engine_exploratory_preferred"] = assessment.exploratory_preferred
        signal.strategy_data["engine_reason"] = assessment.reason
