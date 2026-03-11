from __future__ import annotations

import json
from typing import Any

from .config import BotConfig
from .models import AIReview, TradeSignal


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

    def review(self, signal: TradeSignal) -> AIReview:
        if not self.enabled or self.client is None:
            return AIReview(approved=True, confidence=0.0, reason="AI validation disabled.")

        prompt = (
            "You are validating a crypto trade candidate. "
            "Approve only if the setup is clean, trend aligned, and the risk/reward is sensible. "
            "Reject noisy or weak setups. "
            'Return strict JSON with keys: approved, confidence, reason.'
        )
        payload = {
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
                    {"role": "user", "content": json.dumps(payload)},
                ],
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content or "{}"
            parsed = json.loads(raw)
            confidence = float(parsed.get("confidence", 0.0))
            return AIReview(
                approved=bool(parsed.get("approved", False)),
                confidence=max(0.0, min(confidence, 1.0)),
                reason=str(parsed.get("reason", "No reason provided.")),
            )
        except Exception as exc:
            return AIReview(
                approved=False,
                confidence=0.0,
                reason=f"AI validation failed: {exc}",
            )
