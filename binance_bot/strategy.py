import math
from datetime import datetime, timezone

import pandas as pd

from .config import BotConfig
from .models import MarketScan, Position, TradeSignal


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gains = delta.clip(lower=0).rolling(period).mean()
    losses = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gains / losses.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def _bollinger(series: pd.Series, period: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = mid + (std * num_std)
    lower = mid - (std * num_std)
    return mid, upper, lower


def _stochastic(df: pd.DataFrame, period: int = 14, smooth: int = 3) -> tuple[pd.Series, pd.Series]:
    lowest_low = df["low"].rolling(period).min()
    highest_high = df["high"].rolling(period).max()
    k = ((df["close"] - lowest_low) / (highest_high - lowest_low).replace(0, float("nan"))) * 100
    d = k.rolling(smooth).mean()
    return k, d


def _vwap(df: pd.DataFrame) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_value = (typical_price * df["volume"]).cumsum()
    cumulative_volume = df["volume"].cumsum().replace(0, pd.NA)
    return cumulative_value / cumulative_volume


def _enrich(df: pd.DataFrame, breakout_lookback: int) -> pd.DataFrame:
    enriched = df.copy()
    enriched["ema_20"] = _ema(enriched["close"], 20)
    enriched["ema_50"] = _ema(enriched["close"], 50)
    enriched["rsi_14"] = _rsi(enriched["close"], 14)
    enriched["atr_14"] = _atr(enriched, 14)
    enriched["vwap"] = _vwap(enriched)
    enriched["bb_mid"], enriched["bb_upper"], enriched["bb_lower"] = _bollinger(enriched["close"], 20, 2.0)
    enriched["stoch_k"], enriched["stoch_d"] = _stochastic(enriched, 14, 3)
    enriched["volume_sma_20"] = enriched["volume"].rolling(20).mean()
    enriched["atr_sma_20"] = enriched["atr_14"].rolling(20).mean()
    enriched["breakout_high"] = enriched["high"].shift(1).rolling(breakout_lookback).max()
    enriched["breakdown_low"] = enriched["low"].shift(1).rolling(breakout_lookback).min()
    return enriched


def scan_market(
    symbol: str,
    execution_df: pd.DataFrame,
    higher_df: pd.DataFrame,
    config: BotConfig,
) -> MarketScan:
    if len(execution_df) < 60 or len(higher_df) < 60:
        return MarketScan(
            symbol=symbol,
            signal=None,
            reasons=["Not enough candles yet for indicator warmup."],
            metrics={},
        )

    low_df = _enrich(execution_df, config.breakout_lookback)
    high_df = _enrich(higher_df, config.breakout_lookback)

    current = low_df.iloc[-1]
    previous = low_df.iloc[-2]
    higher = high_df.iloc[-1]
    higher_previous = high_df.iloc[-2]
    reasons: list[str] = []
    required_values = [
        current["ema_20"],
        current["ema_50"],
        current["rsi_14"],
        current["atr_14"],
        current["vwap"],
        current["bb_mid"],
        current["bb_upper"],
        current["bb_lower"],
        current["stoch_k"],
        current["stoch_d"],
        current["volume_sma_20"],
        current["atr_sma_20"],
        higher["ema_20"],
        higher["ema_50"],
    ]
    if any(pd.isna(value) for value in required_values):
        return MarketScan(
            symbol=symbol,
            signal=None,
            reasons=["Indicators are not fully initialized yet."],
            metrics={},
        )

    higher_bullish = higher["ema_20"] > higher["ema_50"]
    higher_bearish = higher["ema_20"] < higher["ema_50"]
    lower_bullish = current["ema_20"] > current["ema_50"]
    lower_bearish = current["ema_20"] < current["ema_50"]
    bullish_trend = higher_bullish and lower_bullish
    bearish_trend = higher_bearish and lower_bearish
    higher_ema_rising = float(higher["ema_20"]) >= float(higher_previous["ema_20"])
    higher_ema_falling = float(higher["ema_20"]) <= float(higher_previous["ema_20"])
    higher_long_bias = higher_bullish or (
        float(higher["close"]) >= float(higher["ema_20"]) and higher_ema_rising
    )
    higher_short_bias = higher_bearish or (
        float(higher["close"]) <= float(higher["ema_20"]) and higher_ema_falling
    )
    volume_ratio = current["volume"] / current["volume_sma_20"] if float(current["volume_sma_20"]) > 0 else 0.0
    atr_value = float(current["atr_14"]) if not math.isnan(float(current["atr_14"])) else 0.0
    candle_is_bullish = float(current["close"]) > float(current["open"])
    candle_is_bearish = float(current["close"]) < float(current["open"])
    near_vwap_long = float(current["close"]) >= float(current["vwap"]) * 0.997
    near_vwap_short = float(current["close"]) <= float(current["vwap"]) * 1.003
    short_stoch_aligned = (
        config.short_stoch_min <= float(current["stoch_k"]) <= config.short_stoch_max
        and float(current["stoch_k"]) < float(current["stoch_d"])
    )
    long_stoch_aligned = (
        config.long_stoch_min <= float(current["stoch_k"]) <= config.long_stoch_max
        and float(current["stoch_k"]) > float(current["stoch_d"])
    )
    metrics = {
        "close": round(float(current["close"]), 6),
        "open": round(float(current["open"]), 6),
        "ema_20": round(float(current["ema_20"]), 6),
        "ema_50": round(float(current["ema_50"]), 6),
        "higher_ema_20": round(float(higher["ema_20"]), 6),
        "higher_ema_50": round(float(higher["ema_50"]), 6),
        "vwap": round(float(current["vwap"]), 6),
        "bb_mid": round(float(current["bb_mid"]), 6),
        "bb_upper": round(float(current["bb_upper"]), 6),
        "bb_lower": round(float(current["bb_lower"]), 6),
        "stoch_k": round(float(current["stoch_k"]), 2),
        "stoch_d": round(float(current["stoch_d"]), 2),
        "rsi_14": round(float(current["rsi_14"]), 2),
        "atr_14": round(float(atr_value), 6),
        "atr_regime_ratio": round(
            float(current["atr_14"] / current["atr_sma_20"]) if float(current["atr_sma_20"]) > 0 else 0.0,
            2,
        ),
        "volume_ratio": round(float(volume_ratio), 2),
        "candle_is_bullish": candle_is_bullish,
        "candle_is_bearish": candle_is_bearish,
        "higher_ema_rising": higher_ema_rising,
        "higher_ema_falling": higher_ema_falling,
        "near_vwap_long": near_vwap_long,
        "near_vwap_short": near_vwap_short,
    }

    def classify_entry_profile(base_score: float) -> str:
        if base_score >= config.aggressive_entry_score:
            return "aggressive"
        if base_score >= config.balanced_entry_score:
            return "balanced"
        return "conservative"

    pullback_recovery_long = (
        previous["low"] <= previous["ema_20"] * (1 + config.pullback_tolerance)
        and current["close"] > current["ema_20"]
    )
    breakout_long = current["close"] > current["breakout_high"] and volume_ratio > config.min_volume_ratio
    continuation_long = (
        float(current["close"]) > float(previous["high"])
        and float(current["close"]) > float(current["ema_20"])
        and volume_ratio >= (config.min_volume_ratio * 0.6)
    )
    early_reversal_long = (
        higher_long_bias
        and current["close"] > current["ema_20"]
        and float(current["stoch_k"]) > float(current["stoch_d"])
        and max(config.long_rsi_min - 8, 44) <= float(current["rsi_14"]) <= min(config.long_rsi_max + 8, 82)
    )
    long_rsi_ok = config.long_rsi_min <= current["rsi_14"] <= config.long_rsi_max or early_reversal_long
    long_setup_ready = pullback_recovery_long or breakout_long or continuation_long or early_reversal_long
    if not higher_long_bias:
        reasons.append("Long rejected: higher timeframe bias is still too weak.")
    if not near_vwap_long:
        reasons.append("Long rejected: price is too far below VWAP.")
    if not long_rsi_ok:
        reasons.append("Long rejected: RSI is outside the long trend zone.")
    if not long_stoch_aligned:
        reasons.append("Long rejected: stochastic is not aligned for a profitable long entry.")
    if config.require_signal_candle_confirmation and not candle_is_bullish:
        reasons.append("Long rejected: signal candle does not confirm bullish continuation.")
    if not long_setup_ready:
        reasons.append("Long rejected: no recovery, continuation, or breakout confirmation.")
    if higher_long_bias and near_vwap_long and long_rsi_ok:
        if long_setup_ready:
            if not long_stoch_aligned:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if config.require_signal_candle_confirmation and not candle_is_bullish:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            entry = float(current["close"])
            swing_stop = float(low_df["low"].tail(5).min())
            stop = min(swing_stop, entry - atr_value * config.atr_stop_multiplier) if atr_value > 0 else swing_stop
            risk = entry - stop
            if risk > 0 and (risk / entry) <= config.max_stop_pct:
                target = entry + (risk * config.min_rr)
                base_score = 0.0
                base_score += 0.20 if bullish_trend else 0.12 if early_reversal_long else 0.08 if higher_long_bias else 0.0
                base_score += 0.10 if float(current["close"]) >= float(current["vwap"]) else 0.06 if near_vwap_long else 0.0
                base_score += min(
                    max((float(current["rsi_14"]) - max(config.long_rsi_min - 6, 42)) / max(config.long_rsi_max - config.long_rsi_min + 6, 1.0), 0.0),
                    1.0,
                ) * 0.15
                base_score += min(max(volume_ratio / max(config.min_volume_ratio, 0.1), 0.0), 2.0) / 2.0 * 0.20
                base_score += 0.15 if long_stoch_aligned else 0.0
                base_score += 0.10 if candle_is_bullish else 0.0
                base_score += 0.10 if breakout_long or pullback_recovery_long or continuation_long else 0.0
                entry_profile = classify_entry_profile(base_score)
                if early_reversal_long and not bullish_trend:
                    setup_type = "early_reversal"
                elif continuation_long:
                    setup_type = "continuation"
                else:
                    setup_type = "pullback_recovery" if pullback_recovery_long else "breakout_confirmation"
                signal = TradeSignal(
                    symbol=symbol,
                    side="long",
                    entry_price=entry,
                    stop_price=stop,
                    target_price=target,
                    rr=config.min_rr,
                    setup_type=setup_type,
                    entry_profile=entry_profile,
                    reason="Bullish trend alignment with VWAP support and momentum confirmation.",
                    strategy_data={
                        **metrics,
                        "higher_trend": "bullish" if higher_long_bias else "neutral",
                        "entry_profile_score": round(base_score, 4),
                        "entry_profile": entry_profile,
                    },
                )
                return MarketScan(symbol=symbol, signal=signal, reasons=["Long setup found."], metrics=metrics)
            reasons.append("Long rejected: stop distance is too wide for configured risk.")

    pullback_recovery_short = (
        previous["high"] >= previous["ema_20"] * (1 - config.pullback_tolerance)
        and current["close"] < current["ema_20"]
    )
    breakdown_short = current["close"] < current["breakdown_low"] and volume_ratio > config.min_volume_ratio
    continuation_short = (
        float(current["close"]) < float(previous["low"])
        and float(current["close"]) < float(current["ema_20"])
        and volume_ratio >= (config.min_volume_ratio * 0.6)
    )
    early_reversal_short = (
        higher_short_bias
        and current["close"] < current["ema_20"]
        and float(current["stoch_k"]) < float(current["stoch_d"])
        and max(config.short_rsi_min - 8, 24) <= float(current["rsi_14"]) <= min(config.short_rsi_max + 10, 74)
    )
    short_rsi_ok = config.short_rsi_min <= current["rsi_14"] <= config.short_rsi_max or early_reversal_short
    short_setup_ready = pullback_recovery_short or breakdown_short or continuation_short or early_reversal_short

    if config.allow_short and higher_short_bias and near_vwap_short and short_rsi_ok:
        if not short_stoch_aligned:
            reasons.append("Short rejected: stochastic is not aligned for a profitable short entry.")
        if config.require_signal_candle_confirmation and not candle_is_bearish:
            reasons.append("Short rejected: signal candle does not confirm bearish continuation.")
        if short_setup_ready:
            if not short_stoch_aligned:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if config.require_signal_candle_confirmation and not candle_is_bearish:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            entry = float(current["close"])
            swing_stop = float(low_df["high"].tail(5).max())
            stop = max(swing_stop, entry + atr_value * config.atr_stop_multiplier) if atr_value > 0 else swing_stop
            risk = stop - entry
            if risk > 0 and (risk / entry) <= config.max_stop_pct:
                target = entry - (risk * config.min_rr)
                base_score = 0.0
                base_score += 0.20 if bearish_trend else 0.12 if early_reversal_short else 0.08 if higher_short_bias else 0.0
                base_score += 0.10 if float(current["close"]) <= float(current["vwap"]) else 0.06 if near_vwap_short else 0.0
                if early_reversal_short:
                    base_score += 0.10
                else:
                    base_score += min(
                        max((min(config.short_rsi_max + 8, 76) - float(current["rsi_14"])) / max(config.short_rsi_max - config.short_rsi_min + 8, 1.0), 0.0),
                        1.0,
                    ) * 0.15
                base_score += min(max(volume_ratio / max(config.min_volume_ratio, 0.1), 0.0), 2.0) / 2.0 * 0.20
                base_score += 0.15 if short_stoch_aligned else 0.0
                base_score += 0.10 if candle_is_bearish else 0.0
                base_score += 0.10 if breakdown_short or pullback_recovery_short or continuation_short else 0.0
                entry_profile = classify_entry_profile(base_score)
                if early_reversal_short and not bearish_trend:
                    setup_type = "early_reversal"
                elif continuation_short:
                    setup_type = "continuation"
                else:
                    setup_type = "pullback_recovery" if pullback_recovery_short else "breakdown_confirmation"
                signal = TradeSignal(
                    symbol=symbol,
                    side="short",
                    entry_price=entry,
                    stop_price=stop,
                    target_price=target,
                    rr=config.min_rr,
                    setup_type=setup_type,
                    entry_profile=entry_profile,
                    reason="Bearish trend alignment with VWAP resistance and momentum confirmation.",
                    strategy_data={
                        **metrics,
                        "higher_trend": "bearish" if higher_short_bias else "neutral",
                        "entry_profile_score": round(base_score, 4),
                        "entry_profile": entry_profile,
                    },
                )
                return MarketScan(symbol=symbol, signal=signal, reasons=["Short setup found."], metrics=metrics)
            reasons.append("Short rejected: stop distance is too wide for configured risk.")
    elif config.allow_short:
        if not higher_short_bias:
            reasons.append("Short rejected: higher timeframe bias is still too strong for a short.")
        if not near_vwap_short:
            reasons.append("Short rejected: price is too far above VWAP for a clean short.")
        if not short_rsi_ok:
            reasons.append("Short rejected: RSI is outside the short trend zone.")
        if not short_setup_ready:
            reasons.append("Short rejected: no recovery, continuation, or breakdown confirmation.")

    return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)


def build_signal(
    symbol: str,
    execution_df: pd.DataFrame,
    higher_df: pd.DataFrame,
    config: BotConfig,
) -> TradeSignal | None:
    return scan_market(symbol, execution_df, higher_df, config).signal


def should_exit(
    position: Position,
    current_price: float,
    max_hold_minutes: int,
    now_time: datetime | None = None,
) -> str | None:
    reference_time = now_time or datetime.now(timezone.utc)
    age_minutes = (reference_time - position.opened_at).total_seconds() / 60
    if position.side == "long":
        if current_price <= position.stop_price:
            return "stop_loss"
        if position.profile_stage == "aggressive" and current_price <= position.half_defense_trigger:
            return "rebalance_to_balanced"
        if position.profile_stage in {"aggressive", "balanced"} and current_price <= position.full_defense_trigger:
            return "rebalance_to_conservative"
        if current_price >= position.target_price:
            return "take_profit"
    else:
        if current_price >= position.stop_price:
            return "stop_loss"
        if position.profile_stage == "aggressive" and current_price >= position.half_defense_trigger:
            return "rebalance_to_balanced"
        if position.profile_stage in {"aggressive", "balanced"} and current_price >= position.full_defense_trigger:
            return "rebalance_to_conservative"
        if current_price <= position.target_price:
            return "take_profit"

    if age_minutes >= max_hold_minutes:
        return "time_exit"

    return None
