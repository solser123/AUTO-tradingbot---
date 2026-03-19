from __future__ import annotations

import math

from .config import BotConfig
from .models import MarketScan


DEFAULT_FUTURES_CANDIDATES = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "LINK/USDT",
    "AVAX/USDT",
    "LTC/USDT",
    "SUI/USDT",
    "TRX/USDT",
    "BCH/USDT",
    "DOT/USDT",
    "AAVE/USDT",
    "UNI/USDT",
]


def default_candidate_symbols(config: BotConfig) -> list[str]:
    if config.candidate_symbols:
        return config.candidate_symbols
    if config.main_symbols:
        return config.main_symbols
    if not config.is_futures:
        return config.symbols

    normalized = []
    for symbol in DEFAULT_FUTURES_CANDIDATES:
        base, quote = symbol.split("/", 1)
        normalized.append(f"{base}/{quote}:{quote}")
    return normalized


def build_exit_roadmap(entry_price: float, stop_price: float, target_price: float, max_hold_minutes: int) -> dict[str, float | int]:
    stop_pct = abs(entry_price - stop_price) / entry_price * 100 if entry_price else 0.0
    target_pct = abs(target_price - entry_price) / entry_price * 100 if entry_price else 0.0
    return {
        "stop_pct": round(stop_pct, 2),
        "target_pct": round(target_pct, 2),
        "max_hold_minutes": max_hold_minutes,
    }


def rank_scan(scan: MarketScan, quote_volume: float) -> tuple[str, float]:
    metrics = scan.metrics
    if scan.signal is not None:
        return "signal", 1000.0 + math.log10(max(quote_volume, 1.0))

    close = float(metrics.get("close", 0.0) or 0.0)
    ema_20 = float(metrics.get("ema_20", 0.0) or 0.0)
    ema_50 = float(metrics.get("ema_50", 0.0) or 0.0)
    higher_ema_20 = float(metrics.get("higher_ema_20", 0.0) or 0.0)
    higher_ema_50 = float(metrics.get("higher_ema_50", 0.0) or 0.0)
    vwap = float(metrics.get("vwap", 0.0) or 0.0)
    rsi = float(metrics.get("rsi_14", 0.0) or 0.0)
    volume_ratio = float(metrics.get("volume_ratio", 0.0) or 0.0)

    bullish = higher_ema_20 > higher_ema_50 and ema_20 > ema_50
    bearish = higher_ema_20 < higher_ema_50 and ema_20 < ema_50

    score = 0.0
    if bullish:
        score += 2.0
    if bearish:
        score += 2.0
    if bullish and close > vwap:
        score += 2.0
    if bearish and close < vwap:
        score += 2.0
    if 52 <= rsi <= 68:
        score += 2.0
    if 32 <= rsi <= 48:
        score += 2.0
    score += min(volume_ratio, 2.0)
    score += min(math.log10(max(quote_volume, 1.0)), 12.0) / 10.0

    if score >= 5.0:
        return "watch", score
    return "ignore", score
