from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from ..sizing import SizingDecision
from .event_score import macro_importance_penalty


@dataclass(frozen=True)
class MacroOverlay:
    blocked: bool
    penalty: float
    size_multiplier: float
    reason: str
    event_title: str
    importance: str


def build_macro_risk_overlay(now: datetime, events: list[dict]) -> MacroOverlay:
    reference = now.astimezone(timezone.utc)
    strongest = MacroOverlay(
        blocked=False,
        penalty=0.0,
        size_multiplier=1.0,
        reason="No macro event pressure.",
        event_title="",
        importance="",
    )
    for event in events:
        try:
            scheduled_at = datetime.fromisoformat(str(event.get("scheduled_at")))
            if scheduled_at.tzinfo is None:
                scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        minutes_to_event = (scheduled_at - reference).total_seconds() / 60.0
        penalty = macro_importance_penalty(str(event.get("importance") or ""), minutes_to_event)
        if penalty == 0:
            continue
        blocked = penalty <= -100.0
        size_multiplier = 0.5 if penalty <= -12.0 and not blocked else 1.0
        candidate = MacroOverlay(
            blocked=blocked,
            penalty=penalty,
            size_multiplier=size_multiplier,
            reason=f"Macro overlay from {event.get('title', 'event')}",
            event_title=str(event.get("title") or ""),
            importance=str(event.get("importance") or ""),
        )
        if abs(candidate.penalty) > abs(strongest.penalty):
            strongest = candidate
    return strongest


def adjust_sizing_for_macro(decision: SizingDecision, overlay: MacroOverlay) -> SizingDecision:
    if overlay.blocked:
        return SizingDecision(
            allowed=False,
            score=decision.score,
            bucket="NO_TRADE",
            risk_pct=0.0,
            risk_multiple=0.0,
            notional=0.0,
            risk_notional_cap=decision.risk_notional_cap,
            stage_cap_notional=decision.stage_cap_notional,
            reason=f"Macro blocked: {overlay.reason}",
            components={**decision.components, "macro_penalty": overlay.penalty},
        )
    if overlay.size_multiplier >= 0.999:
        return decision
    return SizingDecision(
        allowed=decision.allowed,
        score=decision.score,
        bucket=decision.bucket,
        risk_pct=decision.risk_pct,
        risk_multiple=decision.risk_multiple,
        notional=decision.notional * overlay.size_multiplier,
        risk_notional_cap=decision.risk_notional_cap,
        stage_cap_notional=decision.stage_cap_notional,
        reason=f"{decision.reason} Macro scaled: {overlay.reason}",
        components={**decision.components, "macro_penalty": overlay.penalty},
    )
