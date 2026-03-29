from __future__ import annotations

from ..models import TradeSignal
from .models import EngineAssessment, EngineSignalDecision


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


def build_continuation_signal(
    *,
    symbol: str,
    side: str,
    ctx: dict[str, object],
    metrics: dict[str, float | str | bool],
    config,
    signal_bar_time: str,
) -> EngineSignalDecision:
    bullish = side == "long"
    prefix = "long" if bullish else "short"

    higher_bias = bool(ctx[f"{prefix}_higher_bias"])
    near_vwap = bool(ctx[f"{prefix}_near_vwap"])
    rsi_ok = bool(ctx[f"{prefix}_rsi_ok"])
    stoch_ok = bool(ctx[f"{prefix}_stoch_ok"])
    stoch_aligned = bool(ctx[f"{prefix}_stoch_aligned"])
    candle_ok = bool(ctx[f"{prefix}_candle_ok"])
    setup_ready = bool(ctx[f"{prefix}_continuation_ready"])
    resume_confirmed = bool(ctx[f"{prefix}_resume_confirmed"])
    impulse_confirmed = bool(ctx[f"{prefix}_impulse_confirmed"])
    room_ok = bool(ctx[f"{prefix}_room_ok"])
    trend_aligned = bool(ctx[f"{prefix}_trend"])
    breakout = bool(ctx[f"{prefix}_breakout"])
    continuation = bool(ctx[f"{prefix}_continuation"])
    pullback = bool(ctx[f"{prefix}_pullback"])
    trend_reclaim = bool(ctx[f"{prefix}_trend_reclaim"])
    vwap_reclaim = bool(ctx[f"{prefix}_vwap_reclaim"])
    bollinger_reclaim = bool(ctx[f"{prefix}_bollinger_reclaim"])
    bos = bool(ctx[f"{prefix}_bos"])
    choch = bool(ctx[f"{prefix}_choch"])
    fvg = bool(ctx[f"{prefix}_recent_fvg"])
    entry = float(ctx["close_now"])
    atr_value = float(ctx["atr_value"])
    risk_stop = float(ctx[f"{prefix}_stop"])
    room_pct = float(ctx[f"{prefix}_room_pct"])
    estimated_risk_pct = float(ctx[f"{prefix}_estimated_risk_pct"])
    volume_ratio = float(ctx["volume_ratio"])
    session_vwap_zscore = float(ctx["session_vwap_zscore"])
    max_stop_pct = float(config.max_stop_pct)
    side_label = "Long" if bullish else "Short"
    side_word = "bullish" if bullish else "bearish"

    reasons: list[str] = []
    if not higher_bias:
        reasons.append(f"{side_label} continuation rejected: higher timeframe continuation bias is too weak.")
    if not near_vwap:
        reasons.append(f"{side_label} continuation rejected: price is too stretched from VWAP/EMA for trend continuation.")
    if not rsi_ok:
        reasons.append(f"{side_label} continuation rejected: RSI is outside the continuation zone.")
    if not stoch_ok:
        reasons.append(f"{side_label} continuation rejected: stochastic is not aligned for a clean continuation.")
    if config.require_signal_candle_confirmation and not candle_ok:
        reasons.append(f"{side_label} continuation rejected: signal candle does not confirm {side_word} continuation.")
    if not setup_ready:
        reasons.append(f"{side_label} continuation rejected: no breakout, pullback recovery, or trend reclaim trigger.")
    if not resume_confirmed:
        reasons.append(f"{side_label} continuation rejected: resume candle confirmation is still weak.")
    if not impulse_confirmed:
        reasons.append(f"{side_label} continuation rejected: follow-through momentum is not strong enough.")
    if not room_ok:
        reasons.append(f"{side_label} continuation rejected: nearby reward path is too tight.")

    if not (higher_bias and near_vwap and rsi_ok and setup_ready):
        return EngineSignalDecision(
            engine_key="continuation",
            engine_family="continuation",
            priority=70,
            signal=None,
            reasons=tuple(reasons),
            score=0.0,
        )

    if not stoch_ok:
        return EngineSignalDecision("continuation", "continuation", 70, None, tuple(reasons), 0.0)
    if config.require_signal_candle_confirmation and not candle_ok:
        return EngineSignalDecision("continuation", "continuation", 70, None, tuple(reasons), 0.0)
    if not (resume_confirmed and impulse_confirmed and room_ok):
        return EngineSignalDecision("continuation", "continuation", 70, None, tuple(reasons), 0.0)

    risk = abs(entry - risk_stop)
    if risk <= 0:
        return EngineSignalDecision("continuation", "continuation", 70, None, tuple(reasons + [f"{side_label} continuation rejected: invalid stop distance."]), 0.0)
    if (risk / entry) > max_stop_pct:
        return EngineSignalDecision(
            "continuation",
            "continuation",
            70,
            None,
            tuple(reasons + [f"{side_label} continuation rejected: stop distance is too wide for configured risk."]),
            0.0,
        )

    rr = float(config.min_rr)
    target = entry + (risk * rr) if bullish else entry - (risk * rr)
    base_score = 0.0
    base_score += 0.22 if trend_aligned else 0.12 if higher_bias else 0.0
    base_score += 0.12 if near_vwap else 0.0
    base_score += min(max(volume_ratio / max(config.min_volume_ratio, 0.1), 0.0), 2.0) / 2.0 * 0.18
    base_score += 0.16 if stoch_aligned else 0.10 if stoch_ok else 0.0
    base_score += 0.10 if candle_ok else 0.0
    base_score += 0.10 if continuation or breakout else 0.07 if pullback or trend_reclaim or vwap_reclaim or bollinger_reclaim else 0.0
    base_score += 0.08 if resume_confirmed else 0.0
    base_score += 0.08 if impulse_confirmed else 0.0
    base_score += 0.06 if room_ok else 0.0
    base_score += 0.07 if bos else 0.04 if choch else 0.0
    base_score += 0.04 if fvg else 0.0
    if bullish and session_vwap_zscore <= -1.5 and candle_ok:
        base_score += 0.03
    if (not bullish) and session_vwap_zscore >= 1.5 and candle_ok:
        base_score += 0.03

    if continuation:
        setup_type = "continuation"
    elif pullback:
        setup_type = "pullback_recovery"
    else:
        setup_type = "breakout_confirmation" if bullish else "breakdown_confirmation"

    entry_profile = "aggressive" if base_score >= config.aggressive_entry_score else "balanced" if base_score >= config.balanced_entry_score else "conservative"
    signal = TradeSignal(
        symbol=symbol,
        side=side,
        entry_price=entry,
        stop_price=risk_stop,
        target_price=target,
        rr=rr,
        setup_type=setup_type,
        entry_profile=entry_profile,
        reason=f"{side_label} continuation: trend, VWAP location, and impulse are aligned.",
        strategy_data={
            **metrics,
            "engine_key": "continuation",
            "engine_family": "continuation",
            "engine_priority": 70,
            "entry_profile_score": round(base_score, 4),
            "entry_profile": entry_profile,
            "resume_confirmed": resume_confirmed,
            "impulse_confirmed": impulse_confirmed,
            "confirmation_score": int(ctx[f"{prefix}_confirmation_score"]),
            "transition_ready": False,
            "higher_trend": "bullish" if bullish else "bearish" if higher_bias else "neutral",
            "signal_bar_time": signal_bar_time,
            "continuation_room_pct": round(room_pct * 100, 2),
            "estimated_risk_pct": round(estimated_risk_pct * 100, 2),
        },
    )
    return EngineSignalDecision(
        engine_key="continuation",
        engine_family="continuation",
        priority=70,
        signal=signal,
        reasons=(f"{side_label} continuation setup found.",),
        score=base_score,
    )
