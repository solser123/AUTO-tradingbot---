import math
from datetime import datetime, timezone

import numpy as np
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


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[pd.Series, pd.Series, pd.Series]:
    macd_line = _ema(series, fast) - _ema(series, slow)
    signal_line = _ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def _stochastic(df: pd.DataFrame, period: int = 14, smooth: int = 3) -> tuple[pd.Series, pd.Series]:
    lowest_low = df["low"].rolling(period).min()
    highest_high = df["high"].rolling(period).max()
    k = ((df["close"] - lowest_low) / (highest_high - lowest_low).replace(0, float("nan"))) * 100
    d = k.rolling(smooth).mean()
    return k, d


def _linreg_last(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return float("nan")
    if arr.size == 1:
        return float(arr[-1])
    x = np.arange(arr.size, dtype=float)
    slope, intercept = np.polyfit(x, arr, 1)
    return float((slope * x[-1]) + intercept)


def _vwap(df: pd.DataFrame) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_value = (typical_price * df["volume"]).cumsum()
    cumulative_volume = df["volume"].cumsum().replace(0, pd.NA)
    return cumulative_value / cumulative_volume


def _squeeze_features(df: pd.DataFrame, length: int = 20, kc_mult: float = 1.5, bb_mult: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    basis = df["close"].rolling(length).mean()
    dev = df["close"].rolling(length).std() * bb_mult
    upper_bb = basis + dev
    lower_bb = basis - dev

    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    range_ma = tr.rolling(length).mean()
    upper_kc = basis + (range_ma * kc_mult)
    lower_kc = basis - (range_ma * kc_mult)

    squeeze_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
    squeeze_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
    momentum_basis = df["close"] - (
        ((df["high"].rolling(length).max() + df["low"].rolling(length).min()) / 2)
        + df["close"].rolling(length).mean()
    ) / 2
    momentum = momentum_basis.rolling(length).apply(_linreg_last, raw=True)
    slope = momentum.diff()
    return squeeze_on.fillna(False), squeeze_off.fillna(False), momentum, slope


def _session_vwap_bands(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    if "timestamp" not in df.columns:
        fallback = _vwap(df)
        zeros = pd.Series(0.0, index=df.index, dtype=float)
        return fallback, zeros, zeros, pd.Series(0, index=df.index, dtype=int)

    session = pd.to_datetime(df["timestamp"], utc=True).dt.floor("D")
    hl2 = (df["high"] + df["low"]) / 2
    vwapsum = (hl2 * df["volume"]).groupby(session).cumsum()
    volumesum = df["volume"].groupby(session).cumsum().replace(0, pd.NA)
    v2sum = (df["volume"] * hl2 * hl2).groupby(session).cumsum()
    session_vwap = vwapsum / volumesum
    session_std = ((v2sum / volumesum) - (session_vwap * session_vwap)).clip(lower=0).pow(0.5)
    zscore = (df["close"] - session_vwap) / session_std.replace(0, pd.NA)

    abs_z = zscore.abs().fillna(0.0)
    band_rank = pd.Series(0, index=df.index, dtype=int)
    band_rank = band_rank.mask(abs_z >= 1.28, 1)
    band_rank = band_rank.mask(abs_z >= 2.01, 2)
    band_rank = band_rank.mask(abs_z >= 2.51, 3)
    band_rank = band_rank.mask(abs_z >= 3.09, 4)
    band_rank = band_rank.mask(abs_z >= 4.01, 5)
    return session_vwap, session_std, zscore, band_rank.astype(int)


def _smc_lite(df: pd.DataFrame, window: int = 10) -> dict[str, pd.Series]:
    structure_high = df["high"].shift(1).rolling(window).max()
    structure_low = df["low"].shift(1).rolling(window).min()
    atr = _atr(df, 14)
    tolerance = atr.fillna(0) * 0.12
    bullish_bos = df["close"] > structure_high
    bearish_bos = df["close"] < structure_low
    bullish_choch = bullish_bos & (df["close"].shift(1) <= df["ema_20"].shift(1))
    bearish_choch = bearish_bos & (df["close"].shift(1) >= df["ema_20"].shift(1))
    eqh_sweep = (df["high"] > structure_high) & (df["close"] <= (structure_high + tolerance))
    eql_sweep = (df["low"] < structure_low) & (df["close"] >= (structure_low - tolerance))

    bullish_fvg = df["low"] > df["high"].shift(2)
    bearish_fvg = df["high"] < df["low"].shift(2)
    bullish_fvg_mid = pd.Series(
        np.where(
            bullish_fvg,
            (df["low"] + df["high"].shift(2)) / 2,
            np.nan,
        ),
        index=df.index,
    ).ffill()
    bearish_fvg_mid = pd.Series(
        np.where(
            bearish_fvg,
            (df["high"] + df["low"].shift(2)) / 2,
            np.nan,
        ),
        index=df.index,
    ).ffill()
    recent_bullish_fvg = bullish_fvg.rolling(5).max().fillna(False).astype(bool)
    recent_bearish_fvg = bearish_fvg.rolling(5).max().fillna(False).astype(bool)

    return {
        "structure_high_ref": structure_high,
        "structure_low_ref": structure_low,
        "bullish_bos": bullish_bos.fillna(False),
        "bearish_bos": bearish_bos.fillna(False),
        "bullish_choch": bullish_choch.fillna(False),
        "bearish_choch": bearish_choch.fillna(False),
        "eqh_sweep": eqh_sweep.fillna(False),
        "eql_sweep": eql_sweep.fillna(False),
        "bullish_fvg": bullish_fvg.fillna(False),
        "bearish_fvg": bearish_fvg.fillna(False),
        "recent_bullish_fvg": recent_bullish_fvg,
        "recent_bearish_fvg": recent_bearish_fvg,
        "bullish_fvg_mid": bullish_fvg_mid,
        "bearish_fvg_mid": bearish_fvg_mid,
    }


def _enrich(df: pd.DataFrame, breakout_lookback: int) -> pd.DataFrame:
    enriched = df.copy()
    enriched["ema_20"] = _ema(enriched["close"], 20)
    enriched["ema_50"] = _ema(enriched["close"], 50)
    enriched["rsi_14"] = _rsi(enriched["close"], 14)
    enriched["atr_14"] = _atr(enriched, 14)
    enriched["vwap"] = _vwap(enriched)
    enriched["bb_mid"], enriched["bb_upper"], enriched["bb_lower"] = _bollinger(enriched["close"], 20, 2.0)
    enriched["macd"], enriched["macd_signal"], enriched["macd_hist"] = _macd(enriched["close"])
    enriched["stoch_k"], enriched["stoch_d"] = _stochastic(enriched, 14, 3)
    enriched["squeeze_on"], enriched["squeeze_off"], enriched["squeeze_momentum"], enriched["squeeze_slope"] = _squeeze_features(enriched)
    enriched["volume_sma_20"] = enriched["volume"].rolling(20).mean()
    enriched["atr_sma_20"] = enriched["atr_14"].rolling(20).mean()
    enriched["breakout_high"] = enriched["high"].shift(1).rolling(breakout_lookback).max()
    enriched["breakdown_low"] = enriched["low"].shift(1).rolling(breakout_lookback).min()
    (
        enriched["session_vwap"],
        enriched["session_vwap_std"],
        enriched["session_vwap_zscore"],
        enriched["session_vwap_band_rank"],
    ) = _session_vwap_bands(enriched)
    smc = _smc_lite(enriched, max(8, breakout_lookback + 3))
    for key, value in smc.items():
        enriched[key] = value
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
    previous_2 = low_df.iloc[-3]
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
        current["macd"],
        current["macd_signal"],
        current["macd_hist"],
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
    higher_rsi = float(higher["rsi_14"])
    higher_close = float(higher["close"])
    higher_vwap = float(higher["vwap"])
    bullish_bos = bool(current["bullish_bos"])
    bearish_bos = bool(current["bearish_bos"])
    bullish_choch = bool(current["bullish_choch"])
    bearish_choch = bool(current["bearish_choch"])
    eqh_sweep = bool(current["eqh_sweep"])
    eql_sweep = bool(current["eql_sweep"])
    recent_bullish_fvg = bool(current["recent_bullish_fvg"])
    recent_bearish_fvg = bool(current["recent_bearish_fvg"])
    higher_long_bias = higher_bullish or (
        float(higher["close"]) >= float(higher["ema_20"]) and higher_ema_rising
    ) or (
        higher_close >= float(higher["ema_50"]) * 0.995 and higher_rsi >= 46 and higher_close >= higher_vwap * 0.982
    ) or (
        bullish_choch or bullish_bos
    ) or (
        eql_sweep
        and bool(current["squeeze_off"])
        and (float(current["squeeze_momentum"]) if not pd.isna(current["squeeze_momentum"]) else 0.0) > 0
        and (float(current["session_vwap_zscore"]) if not pd.isna(current["session_vwap_zscore"]) else 0.0) >= -2.4
    )
    higher_short_bias = higher_bearish or (
        float(higher["close"]) <= float(higher["ema_20"]) and higher_ema_falling
    ) or (
        higher_close <= float(higher["ema_50"]) * 1.005 and higher_rsi <= 54 and higher_close <= higher_vwap * 1.018
    ) or (
        bearish_choch or bearish_bos
    ) or (
        eqh_sweep
        and bool(current["squeeze_off"])
        and (float(current["squeeze_momentum"]) if not pd.isna(current["squeeze_momentum"]) else 0.0) < 0
        and (float(current["session_vwap_zscore"]) if not pd.isna(current["session_vwap_zscore"]) else 0.0) <= 2.4
    )
    volume_ratio = current["volume"] / current["volume_sma_20"] if float(current["volume_sma_20"]) > 0 else 0.0
    atr_value = float(current["atr_14"]) if not math.isnan(float(current["atr_14"])) else 0.0
    close_now = float(current["close"])
    close_prev = float(previous["close"])
    close_prev_2 = float(previous_2["close"])
    open_now = float(current["open"])
    high_now = float(current["high"])
    low_now = float(current["low"])
    high_prev = float(previous["high"])
    low_prev = float(previous["low"])
    candle_is_bullish = float(current["close"]) > float(current["open"])
    candle_is_bearish = float(current["close"]) < float(current["open"])
    body_to_atr = abs(close_now - open_now) / atr_value if atr_value > 0 else 0.0
    close_3_back = float(low_df["close"].iloc[-4])
    three_bar_return_pct = ((close_now / close_3_back) - 1.0) * 100 if close_3_back > 0 else 0.0
    recent_resistance = float(low_df["high"].iloc[-13:-1].max())
    recent_support = float(low_df["low"].iloc[-13:-1].min())
    vwap_gap_pct = abs(float(current["close"]) - float(current["vwap"])) / float(current["vwap"]) if float(current["vwap"]) > 0 else 0.0
    session_vwap = float(current["session_vwap"]) if not pd.isna(current["session_vwap"]) else float(current["vwap"])
    session_vwap_std = float(current["session_vwap_std"]) if not pd.isna(current["session_vwap_std"]) else 0.0
    session_vwap_zscore = float(current["session_vwap_zscore"]) if not pd.isna(current["session_vwap_zscore"]) else 0.0
    session_band_rank = int(current["session_vwap_band_rank"]) if not pd.isna(current["session_vwap_band_rank"]) else 0
    session_vwap_reclaim_long = close_prev < float(previous["session_vwap"]) * 1.003 and close_now >= session_vwap * 0.997 if not pd.isna(previous["session_vwap"]) else False
    session_vwap_reject_short = close_prev > float(previous["session_vwap"]) * 0.997 and close_now <= session_vwap * 1.003 if not pd.isna(previous["session_vwap"]) else False
    squeeze_on = bool(current["squeeze_on"])
    squeeze_off = bool(current["squeeze_off"])
    squeeze_momentum = float(current["squeeze_momentum"]) if not pd.isna(current["squeeze_momentum"]) else 0.0
    squeeze_slope = float(current["squeeze_slope"]) if not pd.isna(current["squeeze_slope"]) else 0.0
    squeeze_long_bias = squeeze_off and squeeze_momentum > 0 and squeeze_slope >= 0
    squeeze_short_bias = squeeze_off and squeeze_momentum < 0 and squeeze_slope <= 0
    near_vwap_long = (
        float(current["close"]) >= float(current["vwap"]) * 0.982
        or (atr_value > 0 and float(current["close"]) >= float(current["vwap"]) - (atr_value * 2.2))
        or session_vwap_zscore >= -2.35
        or session_vwap_reclaim_long
    )
    near_vwap_short = (
        float(current["close"]) <= float(current["vwap"]) * 1.018
        or (atr_value > 0 and float(current["close"]) <= float(current["vwap"]) + (atr_value * 2.2))
        or session_vwap_zscore <= 2.35
        or session_vwap_reject_short
    )
    macd_now = float(current["macd"])
    macd_signal_now = float(current["macd_signal"])
    macd_hist_now = float(current["macd_hist"])
    macd_prev = float(previous["macd"])
    macd_signal_prev = float(previous["macd_signal"])
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
        "macd": round(macd_now, 6),
        "macd_signal": round(macd_signal_now, 6),
        "macd_hist": round(macd_hist_now, 6),
        "rsi_14": round(float(current["rsi_14"]), 2),
        "atr_14": round(float(atr_value), 6),
        "session_vwap": round(session_vwap, 6),
        "session_vwap_std": round(session_vwap_std, 6),
        "session_vwap_zscore": round(session_vwap_zscore, 2),
        "session_vwap_band_rank": session_band_rank,
        "session_vwap_reclaim_long": session_vwap_reclaim_long,
        "session_vwap_reject_short": session_vwap_reject_short,
        "atr_regime_ratio": round(
            float(current["atr_14"] / current["atr_sma_20"]) if float(current["atr_sma_20"]) > 0 else 0.0,
            2,
        ),
        "volume_ratio": round(float(volume_ratio), 2),
        "squeeze_on": squeeze_on,
        "squeeze_off": squeeze_off,
        "squeeze_momentum": round(squeeze_momentum, 6),
        "squeeze_slope": round(squeeze_slope, 6),
        "squeeze_long_bias": squeeze_long_bias,
        "squeeze_short_bias": squeeze_short_bias,
        "bullish_bos": bullish_bos,
        "bearish_bos": bearish_bos,
        "bullish_choch": bullish_choch,
        "bearish_choch": bearish_choch,
        "eqh_sweep": eqh_sweep,
        "eql_sweep": eql_sweep,
        "recent_bullish_fvg": recent_bullish_fvg,
        "recent_bearish_fvg": recent_bearish_fvg,
        "candle_is_bullish": candle_is_bullish,
        "candle_is_bearish": candle_is_bearish,
        "higher_ema_rising": higher_ema_rising,
        "higher_ema_falling": higher_ema_falling,
        "near_vwap_long": near_vwap_long,
        "near_vwap_short": near_vwap_short,
        "vwap_gap_pct": round(vwap_gap_pct * 100, 2),
        "body_to_atr": round(body_to_atr, 2),
        "three_bar_return_pct": round(three_bar_return_pct, 2),
        "recent_resistance": round(recent_resistance, 6),
        "recent_support": round(recent_support, 6),
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
    trend_reclaim_long = (
        close_prev < float(previous["ema_20"])
        and close_now > float(current["ema_20"])
        and candle_is_bullish
        and body_to_atr >= 0.22
    )
    smc_reversal_long = (
        (bullish_choch or eql_sweep)
        and candle_is_bullish
        and (close_now >= float(current["ema_20"]) * 0.992 or session_vwap_reclaim_long)
    )
    vwap_reclaim_long = (
        close_prev < float(previous["vwap"]) * 1.002
        and close_now >= float(current["vwap"]) * 0.995
        and candle_is_bullish
    )
    bollinger_reclaim_long = (
        close_prev <= float(previous["bb_mid"])
        and close_now > float(current["bb_mid"])
        and candle_is_bullish
    )
    long_resume_confirmed = (
        breakout_long
        or continuation_long
        or trend_reclaim_long
        or smc_reversal_long
        or vwap_reclaim_long
        or session_vwap_reclaim_long
        or (squeeze_long_bias and candle_is_bullish and recent_bullish_fvg)
        or (
            candle_is_bullish
            and close_now > close_prev
            and high_now >= high_prev
            and close_now >= high_prev * 0.996
        )
    )
    long_impulse_confirmed = (
        close_now > close_prev >= close_prev_2
        and three_bar_return_pct >= max((atr_value / close_now) * 100 * 0.55, 0.10)
        and body_to_atr >= 0.22
        and volume_ratio >= max(config.min_volume_ratio * 0.55, 0.22)
    )
    macd_bullish = (
        (macd_now >= macd_signal_now and macd_hist_now >= -0.000001)
        or (macd_prev <= macd_signal_prev and macd_now > macd_signal_now)
    )
    permissive_long_stoch = (
        float(current["stoch_k"]) > float(current["stoch_d"])
        and float(current["stoch_k"]) <= (config.long_stoch_max + 12)
    )
    strong_long_continuation = continuation_long and volume_ratio >= max(config.min_volume_ratio, 0.8)
    early_reversal_long = (
        higher_long_bias
        and (current["close"] > current["ema_20"] or smc_reversal_long or session_vwap_zscore <= -1.8)
        and (float(current["stoch_k"]) > float(current["stoch_d"]) or macd_bullish)
        and max(config.long_rsi_min - 8, 44) <= float(current["rsi_14"]) <= min(config.long_rsi_max + 8, 82)
    )
    long_stoch_ok = long_stoch_aligned or (higher_long_bias and permissive_long_stoch) or strong_long_continuation or macd_bullish
    long_rsi_ok = (
        config.long_rsi_min <= current["rsi_14"] <= config.long_rsi_max
        or early_reversal_long
        or ((trend_reclaim_long or vwap_reclaim_long or bollinger_reclaim_long) and 42 <= float(current["rsi_14"]) <= 82)
    )
    long_setup_ready = (
        pullback_recovery_long
        or breakout_long
        or continuation_long
        or early_reversal_long
        or trend_reclaim_long
        or smc_reversal_long
        or vwap_reclaim_long
        or bollinger_reclaim_long
        or (recent_bullish_fvg and squeeze_long_bias)
    )
    estimated_long_stop = min(float(low_df["low"].tail(5).min()), close_now - atr_value * config.atr_stop_multiplier) if atr_value > 0 else float(low_df["low"].tail(5).min())
    estimated_long_risk_pct = max(0.0, (close_now - estimated_long_stop) / close_now) if close_now > 0 else 0.0
    resistance_room_pct = max(0.0, (recent_resistance - close_now) / close_now) if close_now > 0 else 0.0
    long_resistance_room_ok = breakout_long or resistance_room_pct >= max(estimated_long_risk_pct * 0.55, 0.0022)
    if not higher_long_bias:
        reasons.append("Long rejected: higher timeframe bias is still too weak.")
    if not near_vwap_long:
        reasons.append("Long rejected: price is too far below VWAP.")
    if not long_rsi_ok:
        reasons.append("Long rejected: RSI is outside the long trend zone.")
    if not long_stoch_ok:
        reasons.append("Long rejected: stochastic is not aligned for a profitable long entry.")
    if config.require_signal_candle_confirmation and not candle_is_bullish:
        reasons.append("Long rejected: signal candle does not confirm bullish continuation.")
    if not long_setup_ready:
        reasons.append("Long rejected: no recovery, continuation, or breakout confirmation.")
    if not long_resume_confirmed:
        reasons.append("Long rejected: resume candle confirmation is still weak.")
    if not long_impulse_confirmed:
        reasons.append("Long rejected: individual momentum is not strong enough yet.")
    if not long_resistance_room_ok:
        reasons.append("Long rejected: nearby resistance is too close for a clean reward path.")
    if squeeze_on and not long_setup_ready:
        reasons.append("Long rejected: volatility is still compressed without a clean release.")
    if higher_long_bias and near_vwap_long and long_rsi_ok:
        if long_setup_ready:
            if not long_stoch_ok:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if config.require_signal_candle_confirmation and not candle_is_bullish:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if not long_resume_confirmed or not long_impulse_confirmed or not long_resistance_room_ok:
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
                base_score += 0.15 if long_stoch_aligned else 0.08 if long_stoch_ok else 0.0
                base_score += 0.10 if candle_is_bullish else 0.0
                base_score += 0.10 if breakout_long or pullback_recovery_long or continuation_long or trend_reclaim_long or vwap_reclaim_long or smc_reversal_long else 0.0
                base_score += 0.08 if long_resume_confirmed else 0.0
                base_score += 0.07 if long_impulse_confirmed else 0.0
                base_score += 0.05 if long_resistance_room_ok else 0.0
                base_score += 0.08 if bullish_bos else 0.05 if bullish_choch else 0.0
                base_score += 0.06 if eql_sweep else 0.0
                base_score += 0.05 if recent_bullish_fvg else 0.0
                base_score += 0.05 if squeeze_long_bias else 0.0
                base_score += 0.04 if session_vwap_reclaim_long else 0.0
                if session_vwap_zscore <= -2.0 and candle_is_bullish:
                    base_score += 0.04
                entry_profile = classify_entry_profile(base_score)
                if early_reversal_long and not bullish_trend:
                    setup_type = "early_reversal"
                elif continuation_long:
                    setup_type = "continuation"
                elif smc_reversal_long:
                    setup_type = "smc_reversal"
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
                        "resume_confirmed": long_resume_confirmed,
                        "impulse_confirmed": long_impulse_confirmed,
                        "smc_reversal_long": smc_reversal_long,
                        "session_vwap_zscore": round(session_vwap_zscore, 2),
                        "squeeze_long_bias": squeeze_long_bias,
                        "resistance_room_pct": round(resistance_room_pct * 100, 2),
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
    trend_reclaim_short = (
        close_prev > float(previous["ema_20"])
        and close_now < float(current["ema_20"])
        and candle_is_bearish
        and body_to_atr >= 0.22
    )
    smc_reversal_short = (
        (bearish_choch or eqh_sweep)
        and candle_is_bearish
        and (close_now <= float(current["ema_20"]) * 1.008 or session_vwap_reject_short)
    )
    vwap_reject_short = (
        close_prev > float(previous["vwap"]) * 0.998
        and close_now <= float(current["vwap"]) * 1.005
        and candle_is_bearish
    )
    bollinger_reject_short = (
        close_prev >= float(previous["bb_mid"])
        and close_now < float(current["bb_mid"])
        and candle_is_bearish
    )
    short_resume_confirmed = (
        breakdown_short
        or continuation_short
        or trend_reclaim_short
        or smc_reversal_short
        or vwap_reject_short
        or session_vwap_reject_short
        or (squeeze_short_bias and candle_is_bearish and recent_bearish_fvg)
        or (
            candle_is_bearish
            and close_now < close_prev
            and low_now <= low_prev
            and close_now <= low_prev * 1.004
        )
    )
    short_impulse_confirmed = (
        close_now < close_prev <= close_prev_2
        and three_bar_return_pct <= -max((atr_value / close_now) * 100 * 0.55, 0.10)
        and body_to_atr >= 0.22
        and volume_ratio >= max(config.min_volume_ratio * 0.55, 0.22)
    )
    macd_bearish = (
        (macd_now <= macd_signal_now and macd_hist_now <= 0.000001)
        or (macd_prev >= macd_signal_prev and macd_now < macd_signal_now)
    )
    permissive_short_stoch = (
        float(current["stoch_k"]) < float(current["stoch_d"])
        and float(current["stoch_k"]) >= max(config.short_stoch_min - 12, 5)
    )
    strong_short_continuation = continuation_short and volume_ratio >= max(config.min_volume_ratio, 0.8)
    early_reversal_short = (
        higher_short_bias
        and (current["close"] < current["ema_20"] or smc_reversal_short or session_vwap_zscore >= 1.8)
        and (float(current["stoch_k"]) < float(current["stoch_d"]) or macd_bearish)
        and max(config.short_rsi_min - 8, 24) <= float(current["rsi_14"]) <= min(config.short_rsi_max + 10, 74)
    )
    short_rsi_ok = (
        config.short_rsi_min <= current["rsi_14"] <= config.short_rsi_max
        or early_reversal_short
        or ((trend_reclaim_short or vwap_reject_short or bollinger_reject_short) and 24 <= float(current["rsi_14"]) <= 74)
    )
    short_setup_ready = (
        pullback_recovery_short
        or breakdown_short
        or continuation_short
        or early_reversal_short
        or trend_reclaim_short
        or smc_reversal_short
        or vwap_reject_short
        or bollinger_reject_short
        or (recent_bearish_fvg and squeeze_short_bias)
    )
    short_stoch_ok = short_stoch_aligned or (higher_short_bias and permissive_short_stoch) or strong_short_continuation or macd_bearish
    estimated_short_stop = max(float(low_df["high"].tail(5).max()), close_now + atr_value * config.atr_stop_multiplier) if atr_value > 0 else float(low_df["high"].tail(5).max())
    estimated_short_risk_pct = max(0.0, (estimated_short_stop - close_now) / close_now) if close_now > 0 else 0.0
    support_room_pct = max(0.0, (close_now - recent_support) / close_now) if close_now > 0 else 0.0

    if config.allow_short and higher_short_bias and near_vwap_short and short_rsi_ok:
        if not short_stoch_ok:
            reasons.append("Short rejected: stochastic is not aligned for a profitable short entry.")
        if config.require_signal_candle_confirmation and not candle_is_bearish:
            reasons.append("Short rejected: signal candle does not confirm bearish continuation.")
        if not short_resume_confirmed:
            reasons.append("Short rejected: resume candle confirmation is still weak.")
        if not short_impulse_confirmed:
            reasons.append("Short rejected: individual momentum is not strong enough yet.")
        if not (breakdown_short or support_room_pct >= max(estimated_short_risk_pct * 0.55, 0.0022)):
            reasons.append("Short rejected: nearby support is too close for a clean reward path.")
        if squeeze_on and not short_setup_ready:
            reasons.append("Short rejected: volatility is still compressed without a clean release.")
        if short_setup_ready:
            if not short_stoch_ok:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if config.require_signal_candle_confirmation and not candle_is_bearish:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if not short_resume_confirmed or not short_impulse_confirmed:
                return MarketScan(symbol=symbol, signal=None, reasons=reasons, metrics=metrics)
            if not (breakdown_short or support_room_pct >= max(estimated_short_risk_pct * 0.55, 0.0022)):
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
                base_score += 0.15 if short_stoch_aligned else 0.08 if short_stoch_ok else 0.0
                base_score += 0.10 if candle_is_bearish else 0.0
                base_score += 0.10 if breakdown_short or pullback_recovery_short or continuation_short or trend_reclaim_short or vwap_reject_short or smc_reversal_short else 0.0
                base_score += 0.08 if short_resume_confirmed else 0.0
                base_score += 0.07 if short_impulse_confirmed else 0.0
                base_score += 0.05 if (breakdown_short or support_room_pct >= max(estimated_short_risk_pct * 0.55, 0.0022)) else 0.0
                base_score += 0.08 if bearish_bos else 0.05 if bearish_choch else 0.0
                base_score += 0.06 if eqh_sweep else 0.0
                base_score += 0.05 if recent_bearish_fvg else 0.0
                base_score += 0.05 if squeeze_short_bias else 0.0
                base_score += 0.04 if session_vwap_reject_short else 0.0
                if session_vwap_zscore >= 2.0 and candle_is_bearish:
                    base_score += 0.04
                entry_profile = classify_entry_profile(base_score)
                if early_reversal_short and not bearish_trend:
                    setup_type = "early_reversal"
                elif continuation_short:
                    setup_type = "continuation"
                elif smc_reversal_short:
                    setup_type = "smc_reversal"
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
                        "resume_confirmed": short_resume_confirmed,
                        "impulse_confirmed": short_impulse_confirmed,
                        "smc_reversal_short": smc_reversal_short,
                        "session_vwap_zscore": round(session_vwap_zscore, 2),
                        "squeeze_short_bias": squeeze_short_bias,
                        "support_room_pct": round(support_room_pct * 100, 2),
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
