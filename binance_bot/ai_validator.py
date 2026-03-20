from __future__ import annotations

import json
from typing import Any

from .config import BotConfig
from .models import AIReview, TradeSignal


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


class AIValidator:
    def __init__(self, config: BotConfig) -> None:
        self.enabled = config.ai_validation and bool(config.openai_api_key)
        self.model = config.ai_model
        self.client: Any | None = None

        if self.enabled:
            try:
                from openai import OpenAI

                self.client = OpenAI(api_key=config.openai_api_key)
            except Exception:
                self.enabled = False

    def review(self, signal: TradeSignal, advisory: bool = False) -> AIReview:
        if not self.enabled or self.client is None:
            return AIReview(
                approved=True,
                confidence=0.0,
                reason="AI validation disabled.",
                committee={},
            )

        prompt = (
            "You are a three-person crypto trade review committee. "
            "Reviewer 1 is Trend, Reviewer 2 is Risk, Reviewer 3 is Execution Timing. "
            "Each reviewer must independently score the candidate from 0.0 to 1.0 and explain briefly. "
            "Use short-term, medium-term, and long-term horizon context together. "
            "Also use sector flow / capital rotation context if it is present in strategy_data. "
            "Short-term is for timing, medium-term is for continuation quality, long-term is for structural bias. "
            "Approve only if the setup has clear edge, not just a signal. "
            "Reject trades that are late, stretched, noisy, reversal-prone, weak on follow-through, "
            "or structurally poor after fees and slippage. "
            "If advisory mode is true, treat this as a promotion candidate review for a non-core symbol and be stricter. "
            "Return strict JSON with keys: approved, confidence, reason, trend_score, trend_reason, "
            "risk_score, risk_reason, execution_score, execution_reason."
        )
        payload = {
            "advisory_mode": advisory,
            "symbol": signal.symbol,
            "side": signal.side,
            "entry_price": signal.entry_price,
            "stop_price": signal.stop_price,
            "target_price": signal.target_price,
            "rr": signal.rr,
            "setup_type": signal.setup_type,
            "reason": signal.reason,
            "strategy_data": signal.strategy_data,
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
            confidence = float(parsed.get("confidence", 0.0))
            committee = {
                "trend_score": max(0.0, min(float(parsed.get("trend_score", 0.0)), 1.0)),
                "trend_reason": str(parsed.get("trend_reason", "")),
                "risk_score": max(0.0, min(float(parsed.get("risk_score", 0.0)), 1.0)),
                "risk_reason": str(parsed.get("risk_reason", "")),
                "execution_score": max(0.0, min(float(parsed.get("execution_score", 0.0)), 1.0)),
                "execution_reason": str(parsed.get("execution_reason", "")),
                "advisory_mode": advisory,
            }
            return AIReview(
                approved=bool(parsed.get("approved", False)),
                confidence=max(0.0, min(confidence, 1.0)),
                reason=str(parsed.get("reason", "No reason provided.")),
                committee=committee,
            )
        except Exception as exc:
            return AIReview(
                approved=False,
                confidence=0.0,
                reason=f"AI validation failed: {exc}",
                committee={},
            )

    def healthcheck(self) -> tuple[bool, str]:
        if not self.enabled:
            return True, "AI validation disabled."
        if self.client is None:
            return False, "OpenAI client is not available."

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": "reply exactly OK"}],
                max_tokens=5,
            )
            text = (response.choices[0].message.content or "").strip()
            return True, f"OpenAI responded: {text}"
        except Exception as exc:
            return False, f"OpenAI validation failed: {exc}"
