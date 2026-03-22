from __future__ import annotations

import json
from typing import Any

from .config import BotConfig
from .models import AIReview, AIScanReview, MarketScan, TradeSignal


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


def _safe_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return default


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
            confidence = _safe_float(parsed.get("confidence", 0.0), 0.0)
            committee = {
                "trend_score": max(0.0, min(_safe_float(parsed.get("trend_score", 0.0), 0.0), 1.0)),
                "trend_reason": str(parsed.get("trend_reason", "")),
                "risk_score": max(0.0, min(_safe_float(parsed.get("risk_score", 0.0), 0.0), 1.0)),
                "risk_reason": str(parsed.get("risk_reason", "")),
                "execution_score": max(0.0, min(_safe_float(parsed.get("execution_score", 0.0), 0.0), 1.0)),
                "execution_reason": str(parsed.get("execution_reason", "")),
                "advisory_mode": advisory,
            }
            return AIReview(
                approved=_safe_bool(parsed.get("approved", False), False),
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

    def review_scan(
        self,
        *,
        symbol: str,
        scan: MarketScan,
        horizon_context: dict[str, object],
        external_context: dict[str, dict[str, float | int]],
        sector_context: dict[str, object],
        microstructure: dict[str, object],
        advisory: bool = False,
    ) -> AIScanReview:
        if not self.enabled or self.client is None:
            return AIScanReview(
                approved=False,
                confidence=0.0,
                suggested_side="none",
                setup_bias="neutral",
                reason="AI scan assist disabled.",
                committee={},
            )

        prompt = (
            "You are a crypto pre-signal review committee that works before final trade approval. "
            "Use technical metrics, no-entry reasons, multi-horizon bias, sector flow, external/news alignment, "
            "and microstructure together. "
            "Your job is not to predict perfectly but to detect whether the market is in an actionable early transition "
            "that deserves either: no trade, long exploratory review, short exploratory review, or confirmed signal support. "
            "Approve only when there is a plausible early edge versus waiting. "
            "If the scan has no signal, you may still approve an exploratory setup with smaller size if the context suggests "
            "the move is starting before rules fully confirm. "
            "Do not answer vaguely. If you approve, you must choose exactly one side: long or short. "
            "If the market is broadly bearish with weak bounce attempts, prefer short exploratory over no-trade. "
            "If the market is broadly bullish with weak pullback recovery attempts, prefer long exploratory over no-trade. "
            "Use no-trade only when both sides lack edge. "
            "Return strict JSON with keys: approved, confidence, suggested_side, setup_bias, reason, "
            "trend_score, trend_reason, context_score, context_reason, timing_score, timing_reason."
        )
        payload = {
            "advisory_mode": advisory,
            "symbol": symbol,
            "scan_has_signal": scan.signal is not None,
            "scan_signal_side": scan.signal.side if scan.signal is not None else "none",
            "scan_reasons": scan.reasons[:8],
            "scan_metrics": scan.metrics,
            "horizon_context": horizon_context,
            "external_context": external_context,
            "sector_context": sector_context,
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
            suggested_side = str(parsed.get("suggested_side", "none")).strip().lower()
            if suggested_side not in {"long", "short", "none"}:
                suggested_side = "none"
            confidence = max(0.0, min(_safe_float(parsed.get("confidence", 0.0), 0.0), 1.0))
            approved = _safe_bool(parsed.get("approved", False), False)
            if suggested_side == "none":
                approved = False
            committee = {
                "trend_score": max(0.0, min(_safe_float(parsed.get("trend_score", 0.0), 0.0), 1.0)),
                "trend_reason": str(parsed.get("trend_reason", "")),
                "context_score": max(0.0, min(_safe_float(parsed.get("context_score", 0.0), 0.0), 1.0)),
                "context_reason": str(parsed.get("context_reason", "")),
                "timing_score": max(0.0, min(_safe_float(parsed.get("timing_score", 0.0), 0.0), 1.0)),
                "timing_reason": str(parsed.get("timing_reason", "")),
                "advisory_mode": advisory,
            }
            return AIScanReview(
                approved=approved,
                confidence=confidence,
                suggested_side=suggested_side,
                setup_bias=str(parsed.get("setup_bias", "neutral")),
                reason=str(parsed.get("reason", "No reason provided.")),
                committee=committee,
            )
        except Exception as exc:
            return AIScanReview(
                approved=False,
                confidence=0.0,
                suggested_side="none",
                setup_bias="neutral",
                reason=f"AI scan review failed: {exc}",
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
