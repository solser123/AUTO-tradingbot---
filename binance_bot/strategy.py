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

    bullish_trend = higher["ema_20"] > higher["ema_50"] and current["ema_20"] > current["ema_50"]
    bearish_trend = higher["ema_20"] < higher["ema_50"] and current["ema_20"] < current["ema_50"]
    volume_ratio = current["volume"] / current["volume_sma_20"] if float(current["volume_sma_20"]) > 0 else 0.0
    atr_value = float(current["atr_14"]) if not math.isnan(float(current["atr_14"])) else 0.0
    candle_is_bullish = float(current["close"]) > float(current["open"])
    candle_is_bearish = float(current["close"]) < float(current["open"])
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
    }

    pullback_recovery_long = (
        previous["low"] <= previous["ema_20"] * (1 + config.pullback_tolerance)
        and current["close"] > current["ema_20"]
    )
    breakout_long = current["close"] > current["breakout_high"] and volume_ratio > config.min_volume_ratio
    if not bullish_trend:
        reasons.append("Long rejected: trend is not bullish on both timeframes.")
    if current["close"] <= current["vwap"]:
        reasons.append("Long rejected: price is below VWAP.")
    if not (config.long_rsi_min <= current["rsi_14"] <= config.long_rsi_max):
        reasons.append("Long rejected: RSI is outside the long trend zone.")
    if not long_stoch_aligned:
        reasons.append("Long rejected: stochastic is not aligned for a profitable long entry.")
    if config.require_signal_candle_confirmation and not candle_is_bullish:
        reasons.append("Long rejected: signal candle does not confirm bullish continuation.")
    if not (pullback_recovery_long or breakout_long):
        reasons.append("Long rejected: no pullback recovery or breakout confirmation.")
    if bullish_trend and current["close"] > current["vwap"] and config.long_rsi_min <= current["rsi_14"] <= config.long_rsi_max:
        if pullback_recovery_long or breakout_long:
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
                signal = TradeSignal(
                    symbol=symbol,
                    side="long",
                    entry_price=entry,
                    stop_price=stop,
                    target_price=target,
                    rr=config.min_rr,
                    setup_type="pullback_recovery" if pullback_recovery_long else "breakout_confirmation",
                    reason="Bullish trend alignment with VWAP support and momentum confirmation.",
                    strategy_data={
                        **metrics,
                        "higher_trend": "bullish",
                    },
                )
                return MarketScan(symbol=symbol, signal=signal, reasons=["Long setup found."], metrics=metrics)
            reasons.append("Long rejected: stop distance is too wide for configured risk.")

    if config.allow_short and bearish_trend and current["close"] < current["vwap"] and config.short_rsi_min <= current["rsi_14"] <= config.short_rsi_max:
        pullback_recovery_short = (
            previous["high"] >= previous["ema_20"] * (1 - config.pullback_tolerance)
            and current["close"] < current["ema_20"]
        )
        breakdown_short = current["close"] < current["breakdown_low"] and volume_ratio > config.min_volume_ratio
        if not short_stoch_aligned:
            reasons.append("Short rejected: stochastic is not aligned for a profitable short entry.")
        if config.require_signal_candle_confirmation and not candle_is_bearish:
            reasons.append("Short rejected: signal candle does not confirm bearish continuation.")
        if pullback_recovery_short or breakdown_short:
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
                signal = TradeSignal(
                    symbol=symbol,
                    side="short",
                    entry_price=entry,
                    stop_price=stop,
                    target_price=target,
                    rr=config.min_rr,
                    setup_type="pullback_recovery" if pullback_recovery_short else "breakdown_confirmation",
                    reason="Bearish trend alignment with VWAP resistance and momentum confirmation.",
                    strategy_data={
                        **metrics,
                        "higher_trend": "bearish",
                    },
                )
                return MarketScan(symbol=symbol, signal=signal, reasons=["Short setup found."], metrics=metrics)
            reasons.append("Short rejected: stop distance is too wide for configured risk.")
    elif config.allow_short:
        reasons.append("Short rejected: bearish short criteria are not aligned.")

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
        if current_price >= position.target_price:
            return "take_profit"
    else:
        if current_price >= position.stop_price:
            return "stop_loss"
        if current_price <= position.target_price:
            return "take_profit"

    if age_minutes >= max_hold_minutes:
        return "time_exit"

    return None
