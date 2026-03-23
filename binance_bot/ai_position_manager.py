from __future__ import annotations

import json
from typing import Any

from .config import BotConfig
from .models import AIManageDecision, Position


def _json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return str(value)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


class AIPositionManager:
    def __init__(self, config: BotConfig, client: Any | None = None) -> None:
        self.enabled = config.ai_validation and bool(config.openai_api_key) and client is not None
        self.model = config.ai_model
        self.client = client if self.enabled else None

    def review_position(
        self,
        *,
        position: Position,
        current_price: float,
        current_progress_r: float,
        unrealized_pnl: float,
        unrealized_pnl_pct: float,
        daily_realized_pnl: float,
        daily_profit_target: float,
        scan_metrics: dict[str, object],
        horizon_context: dict[str, object],
        sector_context: dict[str, object],
        external_context: dict[str, object],
        microstructure: dict[str, object],
    ) -> AIManageDecision:
        if not self.enabled or self.client is None:
            return AIManageDecision(
                action="hold",
                confidence=0.0,
                reason="AI position manager disabled.",
                committee={},
            )

        prompt = (
            "You are an AI crypto position manager. "
            "You are managing an already-open futures position, not deciding whether to enter. "
            "The stop-loss basis is fixed and must not be widened or removed. "
            "Your goal is not to find the perfect top or bottom. "
            "Your goal is to improve steady daily profitability by managing open risk and letting strong trends continue modestly. "
            "Never propose unlimited target raising. "
            "You may only choose exactly one action from: hold, exit_now, reduce_25, reduce_50, "
            "tighten_to_balanced, tighten_to_conservative, raise_target_small, raise_target_medium. "
            "Use raise_target actions only when trend continuation is still healthy and there is room to continue. "
            "Use reduce/exit when the move is losing quality, daily profit is already close to target, or reversal risk is rising. "
            "Return strict JSON with keys: action, confidence, reason, trend_score, trend_reason, "
            "risk_score, risk_reason, management_score, management_reason."
        )
        payload = {
            "position": {
                "symbol": position.symbol,
                "side": position.side,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "stop_price": position.stop_price,
                "target_price": position.target_price,
                "entry_profile": position.entry_profile,
                "profile_stage": position.profile_stage,
                "half_defense_trigger": position.half_defense_trigger,
                "full_defense_trigger": position.full_defense_trigger,
                "opened_at": position.opened_at.isoformat(),
            },
            "current_price": current_price,
            "current_progress_r": current_progress_r,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "daily_realized_pnl": daily_realized_pnl,
            "daily_profit_target": daily_profit_target,
            "scan_metrics": scan_metrics,
            "horizon_context": horizon_context,
            "sector_context": sector_context,
            "external_context": external_context,
            "microstructure": microstructure,
        }
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps(_json_safe(payload), ensure_ascii=False)},
                ],
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content or "{}"
            parsed = json.loads(raw)
            action = str(parsed.get("action", "hold")).strip().lower()
            if action not in {
                "hold",
                "exit_now",
                "reduce_25",
                "reduce_50",
                "tighten_to_balanced",
                "tighten_to_conservative",
                "raise_target_small",
                "raise_target_medium",
            }:
                action = "hold"
            confidence = max(0.0, min(_safe_float(parsed.get("confidence", 0.0), 0.0), 1.0))
            committee = {
                "trend_score": max(0.0, min(_safe_float(parsed.get("trend_score", 0.0), 0.0), 1.0)),
                "trend_reason": str(parsed.get("trend_reason", "")),
                "risk_score": max(0.0, min(_safe_float(parsed.get("risk_score", 0.0), 0.0), 1.0)),
                "risk_reason": str(parsed.get("risk_reason", "")),
                "management_score": max(0.0, min(_safe_float(parsed.get("management_score", 0.0), 0.0), 1.0)),
                "management_reason": str(parsed.get("management_reason", "")),
            }
            return AIManageDecision(
                action=action,
                confidence=confidence,
                reason=str(parsed.get("reason", "No reason provided.")),
                committee=committee,
            )
        except Exception as exc:
            return AIManageDecision(
                action="hold",
                confidence=0.0,
                reason=f"AI position management failed: {exc}",
                committee={},
            )
