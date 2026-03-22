from __future__ import annotations

from dataclasses import dataclass

from .config import BotConfig
from .models import Position, TradeSignal
from .sectors import sector_for_symbol


@dataclass(frozen=True)
class SizingDecision:
    allowed: bool
    score: float
    bucket: str
    risk_pct: float
    risk_multiple: float
    notional: float
    risk_notional_cap: float
    stage_cap_notional: float
    reason: str
    components: dict[str, float]


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def build_sizing_decision(
    *,
    signal: TradeSignal,
    config: BotConfig,
    account_equity: float,
    open_positions: list[Position],
    horizon_context: dict[str, object],
    sector_context: dict[str, object],
    external_alignment: dict[str, float | int],
    microstructure: dict[str, float | int | str],
) -> SizingDecision:
    entry_score = float(signal.strategy_data.get("entry_profile_score", 0.0) or 0.0)
    stop_pct = abs(signal.entry_price - signal.stop_price) / signal.entry_price if signal.entry_price > 0 else 0.0
    atr_regime = float(signal.strategy_data.get("atr_regime_ratio", 0.0) or 0.0)
    volume_ratio = float(signal.strategy_data.get("volume_ratio", 0.0) or 0.0)
    spread_pct = float(microstructure.get("spread_pct", 0.0) or 0.0)
    total_depth = float(microstructure.get("total_depth_usdt", 0.0) or 0.0)
    trade_flow = float(microstructure.get("trade_flow_score", 0.0) or 0.0)
    depth_imbalance = float(microstructure.get("depth_imbalance", 0.0) or 0.0)
    sector_flow = float(sector_context.get("flow_score", 0.0) or 0.0)
    external_score = float(external_alignment.get("alignment_score", 0.0) or 0.0)
    same_side_count = int(horizon_context.get("same_side_count", 0) or 0)
    opposite_side_count = int(horizon_context.get("opposite_side_count", 0) or 0)
    event_penalty = _clamp(float(signal.strategy_data.get("event_risk_penalty", 0.0) or 0.0), -20.0, 0.0)

    strategy_points = _clamp(entry_score, 0.0, 1.0) * 30.0

    timeframe_points = 0.0
    if same_side_count >= 2:
        timeframe_points = 15.0
    elif same_side_count == 1:
        timeframe_points = 10.0
    elif opposite_side_count == 0:
        timeframe_points = 6.0
    timeframe_points -= opposite_side_count * 4.0
    timeframe_points = _clamp(timeframe_points, 0.0, 15.0)

    volatility_points = 15.0
    if atr_regime > 0:
        volatility_points -= min(abs(atr_regime - 1.0) * 8.0, 8.0)
    if stop_pct > 0.020:
        volatility_points -= 6.0
    elif stop_pct > 0.015:
        volatility_points -= 3.0
    elif 0.0035 <= stop_pct <= 0.012:
        volatility_points += 1.0
    volatility_points = _clamp(volatility_points, 0.0, 15.0)

    liquidity_points = 0.0
    liquidity_points += _clamp((volume_ratio / max(config.min_volume_ratio, 0.1)) * 6.0, 0.0, 6.0)
    liquidity_points += _clamp((total_depth / max(config.microstructure_min_total_depth_usdt, 1.0)) * 4.0, 0.0, 4.0)
    liquidity_points += _clamp((config.microstructure_max_spread_pct - spread_pct) / max(config.microstructure_max_spread_pct, 1e-9) * 5.0, 0.0, 5.0)
    liquidity_points = _clamp(liquidity_points, 0.0, 15.0)

    sector_points = 0.0
    if signal.side == "long":
        sector_points += _clamp((sector_flow + 0.25) * 14.0, 0.0, 6.0)
        sector_points += _clamp((external_score + 0.25) * 8.0, 0.0, 4.0)
    else:
        sector_points += _clamp(((-sector_flow) + 0.25) * 14.0, 0.0, 6.0)
        sector_points += _clamp((external_score + 0.25) * 8.0, 0.0, 4.0)
    sector_points = _clamp(sector_points, 0.0, 10.0)

    same_sector_same_side = 0
    same_cluster_same_side = 0
    current_sector = sector_for_symbol(signal.symbol)
    for position in open_positions:
        if position.side != signal.side:
            continue
        if sector_for_symbol(position.symbol) == current_sector:
            same_sector_same_side += 1
        if position.symbol != signal.symbol:
            same_cluster_same_side += 1
    correlation_penalty = -min(20.0, same_sector_same_side * 8.0 + same_cluster_same_side * 3.0)

    total_score = (
        strategy_points
        + timeframe_points
        + volatility_points
        + liquidity_points
        + sector_points
        + event_penalty
        + correlation_penalty
    )
    total_score = _clamp(total_score, 0.0, 100.0)

    if total_score >= 80.0:
        bucket = "1.0R"
        risk_multiple = 1.0
        risk_pct = config.sizing_risk_pct_full
        cap_multiplier = 1.0
    elif total_score >= 70.0:
        bucket = "0.7R"
        risk_multiple = 0.7
        risk_pct = config.sizing_risk_pct_high
        cap_multiplier = 0.7
    elif total_score >= 60.0:
        bucket = "0.45R"
        risk_multiple = 0.45
        risk_pct = config.sizing_risk_pct_medium
        cap_multiplier = 0.45
    elif total_score >= 48.0:
        bucket = "0.25R"
        risk_multiple = 0.25
        risk_pct = config.sizing_risk_pct_low
        cap_multiplier = 0.25
    else:
        return SizingDecision(
            allowed=False,
            score=round(total_score, 2),
            bucket="NO_TRADE",
            risk_pct=0.0,
            risk_multiple=0.0,
            notional=0.0,
            risk_notional_cap=0.0,
            stage_cap_notional=0.0,
            reason="Sizing rejected: composite score is below 48.",
            components={
                "strategy_confidence": round(strategy_points, 2),
                "timeframe_alignment": round(timeframe_points, 2),
                "volatility_fit": round(volatility_points, 2),
                "liquidity_quality": round(liquidity_points, 2),
                "sector_market_alignment": round(sector_points, 2),
                "event_penalty": round(event_penalty, 2),
                "correlation_penalty": round(correlation_penalty, 2),
            },
        )

    stage_cap_notional = config.stage_notional(signal.symbol) * cap_multiplier
    if signal.side == "long" and sector_flow >= config.sector_flow_positive_threshold:
        stage_cap_notional *= 1.0 + config.sector_alignment_notional_boost_pct
    elif signal.side == "short" and sector_flow <= config.sector_flow_negative_threshold:
        stage_cap_notional *= 1.0 + config.sector_alignment_notional_boost_pct

    if account_equity <= 0 or stop_pct <= 0:
        return SizingDecision(
            allowed=False,
            score=round(total_score, 2),
            bucket=bucket,
            risk_pct=risk_pct,
            risk_multiple=risk_multiple,
            notional=0.0,
            risk_notional_cap=0.0,
            stage_cap_notional=stage_cap_notional,
            reason="Sizing rejected: invalid equity or stop distance.",
            components={
                "strategy_confidence": round(strategy_points, 2),
                "timeframe_alignment": round(timeframe_points, 2),
                "volatility_fit": round(volatility_points, 2),
                "liquidity_quality": round(liquidity_points, 2),
                "sector_market_alignment": round(sector_points, 2),
                "event_penalty": round(event_penalty, 2),
                "correlation_penalty": round(correlation_penalty, 2),
            },
        )

    risk_notional_cap = (account_equity * risk_pct) / stop_pct
    final_notional = min(stage_cap_notional, risk_notional_cap)
    if final_notional <= 0:
        return SizingDecision(
            allowed=False,
            score=round(total_score, 2),
            bucket=bucket,
            risk_pct=risk_pct,
            risk_multiple=risk_multiple,
            notional=0.0,
            risk_notional_cap=risk_notional_cap,
            stage_cap_notional=stage_cap_notional,
            reason="Sizing rejected: notional resolved to zero.",
            components={
                "strategy_confidence": round(strategy_points, 2),
                "timeframe_alignment": round(timeframe_points, 2),
                "volatility_fit": round(volatility_points, 2),
                "liquidity_quality": round(liquidity_points, 2),
                "sector_market_alignment": round(sector_points, 2),
                "event_penalty": round(event_penalty, 2),
                "correlation_penalty": round(correlation_penalty, 2),
            },
        )

    return SizingDecision(
        allowed=True,
        score=round(total_score, 2),
        bucket=bucket,
        risk_pct=risk_pct,
        risk_multiple=risk_multiple,
        notional=final_notional,
        risk_notional_cap=risk_notional_cap,
        stage_cap_notional=stage_cap_notional,
        reason="Sizing approved.",
        components={
            "strategy_confidence": round(strategy_points, 2),
            "timeframe_alignment": round(timeframe_points, 2),
            "volatility_fit": round(volatility_points, 2),
            "liquidity_quality": round(liquidity_points, 2),
            "sector_market_alignment": round(sector_points, 2),
            "event_penalty": round(event_penalty, 2),
            "correlation_penalty": round(correlation_penalty, 2),
        },
    )
