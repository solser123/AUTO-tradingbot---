from __future__ import annotations

import math
from dataclasses import dataclass

from .exchange import BinanceExchange


@dataclass(frozen=True)
class HotMoverCandidate:
    symbol: str
    direction: str
    pct_change_24h: float
    quote_volume: float
    last_price: float
    score: float
    recent_listing: bool = False


def discover_hot_movers(
    exchange: BinanceExchange,
    *,
    limit: int,
    min_pct_change: float,
    min_quote_volume: float,
    allow_shorts: bool,
    exclude_symbols: set[str] | None = None,
    recent_listing_symbols: set[str] | None = None,
    allowed_symbols: set[str] | None = None,
) -> list[HotMoverCandidate]:
    if not exchange.config.is_futures:
        return []

    excluded = exclude_symbols or set()
    recent_listings = recent_listing_symbols or set()
    allowed = allowed_symbols or set()
    candidates: list[HotMoverCandidate] = []
    try:
        rows = exchange.client.fapiPublicGetTicker24hr()
    except Exception:
        return []

    for item in rows:
        symbol_id = str(item.get("symbol") or "")
        if not symbol_id.endswith("USDT"):
            continue
        market_symbol = f"{symbol_id[:-4]}/USDT:USDT"
        if market_symbol in excluded:
            continue
        if allowed and market_symbol not in allowed:
            continue

        try:
            pct_change = float(item.get("priceChangePercent") or 0.0)
            quote_volume = float(item.get("quoteVolume") or 0.0)
            last_price = float(item.get("lastPrice") or 0.0)
        except Exception:
            continue

        if last_price <= 0 or quote_volume < min_quote_volume or abs(pct_change) < min_pct_change:
            continue

        direction = "long" if pct_change > 0 else "short"
        if direction == "short" and not allow_shorts:
            continue

        recent_listing = market_symbol in recent_listings
        score = abs(pct_change) * (1.0 + min(math.log10(max(quote_volume, 1.0)), 9.0) / 10.0)
        if recent_listing:
            score += 6.0

        candidates.append(
            HotMoverCandidate(
                symbol=market_symbol,
                direction=direction,
                pct_change_24h=pct_change,
                quote_volume=quote_volume,
                last_price=last_price,
                score=score,
                recent_listing=recent_listing,
            )
        )

    candidates.sort(key=lambda item: (-item.score, -abs(item.pct_change_24h), -item.quote_volume, item.symbol))
    return candidates[: max(limit, 0)]
