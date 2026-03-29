from __future__ import annotations

from ..models import TradeSignal
from .models import EngineAssessment, EngineSignalDecision


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


def build_reversal_signal(
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
    reversal_trigger = bool(ctx[f"{prefix}_reversal_trigger"])
    rsi_ok = bool(ctx[f"{prefix}_reversal_rsi_ok"])
    stoch_ok = bool(ctx[f"{prefix}_reversal_stoch_ok"])
    candle_ok = bool(ctx[f"{prefix}_candle_ok"])
    resume_confirmed = bool(ctx[f"{prefix}_resume_confirmed"])
    impulse_confirmed = bool(ctx[f"{prefix}_impulse_confirmed"])
    room_ok = bool(ctx[f"{prefix}_reversal_room_ok"])
    transition_ready = bool(ctx[f"{prefix}_transition_ready"])
    early_reversal = bool(ctx[f"{prefix}_early_reversal"])
    smc_reversal = bool(ctx[f"{prefix}_smc_reversal"])
    choch = bool(ctx[f"{prefix}_choch"])
    sweep = bool(ctx[f"{prefix}_sweep"])
    recent_fvg = bool(ctx[f"{prefix}_recent_fvg"])
    squeeze_bias = bool(ctx[f"{prefix}_squeeze_bias"])
    session_reclaim = bool(ctx[f"{prefix}_session_reclaim"])
    macd_confirmed = bool(ctx[f"{prefix}_macd"])
    confirmation_score = int(ctx[f"{prefix}_confirmation_score"])
    trend_aligned = bool(ctx[f"{prefix}_trend"])
    entry = float(ctx["close_now"])
    risk_stop = float(ctx[f"{prefix}_stop"])
    risk_pct = float(ctx[f"{prefix}_estimated_risk_pct"])
    room_pct = float(ctx[f"{prefix}_room_pct"])
    volume_ratio = float(ctx["volume_ratio"])
    session_vwap_zscore = float(ctx["session_vwap_zscore"])
    body_to_atr = float(ctx["body_to_atr"])
    max_stop_pct = float(config.max_stop_pct)
    side_label = "Long" if bullish else "Short"
    side_word = "bullish" if bullish else "bearish"

    reasons: list[str] = []
    if not higher_bias:
        reasons.append(f"{side_label} reversal rejected: higher timeframe transition bias is still too weak.")
    if not near_vwap:
        reasons.append(f"{side_label} reversal rejected: price is too stretched away from the mean.")
    if not reversal_trigger:
        reasons.append(f"{side_label} reversal rejected: no CHoCH, sweep, FVG, or VWAP reclaim trigger.")
    if not rsi_ok:
        reasons.append(f"{side_label} reversal rejected: RSI is outside the reversal zone.")
    if not stoch_ok:
        reasons.append(f"{side_label} reversal rejected: stochastic/MACD confirmation is too weak.")
    if config.require_signal_candle_confirmation and not candle_ok and not transition_ready:
        reasons.append(f"{side_label} reversal rejected: signal candle does not confirm the {side_word} response.")
    if not resume_confirmed:
        reasons.append(f"{side_label} reversal rejected: reclaim/resume confirmation is still weak.")
    if not impulse_confirmed:
        reasons.append(f"{side_label} reversal rejected: rebound momentum has not expanded enough.")
    if not room_ok:
        reasons.append(f"{side_label} reversal rejected: reversal reward path is too tight.")

    if not (higher_bias and near_vwap and reversal_trigger and rsi_ok):
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons), 0.0)
    if not stoch_ok and not transition_ready:
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons), 0.0)
    if config.require_signal_candle_confirmation and not candle_ok and not transition_ready:
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons), 0.0)
    if not (resume_confirmed or impulse_confirmed or transition_ready):
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons), 0.0)
    if not room_ok and not transition_ready:
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons), 0.0)

    risk = abs(entry - risk_stop)
    if risk <= 0:
        return EngineSignalDecision("reversal", "reversal", 75, None, tuple(reasons + [f"{side_label} reversal rejected: invalid stop distance."]), 0.0)
    if (risk / entry) > max_stop_pct:
        return EngineSignalDecision(
            "reversal",
            "reversal",
            75,
            None,
            tuple(reasons + [f"{side_label} reversal rejected: stop distance is too wide for configured risk."]),
            0.0,
        )

    rr = float(config.min_rr)
    target = entry + (risk * rr) if bullish else entry - (risk * rr)
    base_score = 0.0
    base_score += 0.16 if trend_aligned else 0.12 if higher_bias else 0.0
    base_score += 0.10 if near_vwap else 0.0
    base_score += 0.11 if early_reversal else 0.09 if smc_reversal else 0.0
    base_score += 0.08 if choch else 0.06 if sweep else 0.0
    base_score += 0.05 if recent_fvg else 0.0
    base_score += 0.05 if session_reclaim else 0.0
    base_score += 0.05 if squeeze_bias else 0.0
    base_score += 0.08 if stoch_ok else 0.0
    base_score += 0.06 if macd_confirmed else 0.0
    base_score += 0.06 if resume_confirmed else 0.0
    base_score += 0.06 if impulse_confirmed else 0.0
    base_score += 0.04 if room_ok else 0.0
    base_score += min(max(volume_ratio / max(config.min_volume_ratio, 0.1), 0.0), 2.0) / 2.0 * 0.10
    base_score += min(max(body_to_atr / 0.25, 0.0), 1.5) * 0.04
    if bullish and session_vwap_zscore <= -2.0:
        base_score += 0.04
    if (not bullish) and session_vwap_zscore >= 2.0:
        base_score += 0.04

    entry_profile = "exploratory"
    if base_score >= config.balanced_entry_score and confirmation_score >= 3 and room_ok:
        entry_profile = "balanced"
    if base_score >= config.aggressive_entry_score and trend_aligned and confirmation_score >= 4 and room_ok:
        entry_profile = "aggressive"

    setup_type = "smc_reversal" if smc_reversal and not early_reversal else "early_reversal"
    signal = TradeSignal(
        symbol=symbol,
        side=side,
        entry_price=entry,
        stop_price=risk_stop,
        target_price=target,
        rr=rr,
        setup_type=setup_type,
        entry_profile=entry_profile,
        reason=f"{side_label} reversal: structure shift and mean-reclaim conditions are aligned.",
        strategy_data={
            **metrics,
            "engine_key": "reversal",
            "engine_family": "reversal",
            "engine_priority": 75,
            "entry_profile_score": round(base_score, 4),
            "entry_profile": entry_profile,
            "resume_confirmed": resume_confirmed,
            "impulse_confirmed": impulse_confirmed,
            "confirmation_score": confirmation_score,
            "transition_ready": transition_ready,
            "higher_trend": "bullish" if bullish else "bearish" if higher_bias else "neutral",
            "signal_bar_time": signal_bar_time,
            f"smc_reversal_{side}": smc_reversal,
            "reversal_room_pct": round(room_pct * 100, 2),
            "estimated_risk_pct": round(risk_pct * 100, 2),
        },
    )
    return EngineSignalDecision(
        engine_key="reversal",
        engine_family="reversal",
        priority=75,
        signal=signal,
        reasons=(f"{side_label} reversal setup found.",),
        score=base_score,
    )
