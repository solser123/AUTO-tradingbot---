from __future__ import annotations


def estimate_slippage(
    side: str,
    qty: float,
    spread_bps: float,
    depth_usd: float,
    volatility_bps: float,
    aggressiveness: str,
) -> float:
    if qty <= 0 or depth_usd <= 0:
        return max(spread_bps, 0.0)
    aggressiveness_multiplier = {
        "aggressive": 1.25,
        "balanced": 1.0,
        "conservative": 0.8,
    }.get(aggressiveness, 1.0)
    impact_bps = min((qty / depth_usd) * 10_000, 80.0)
    volatility_addon = max(volatility_bps, 0.0) * 0.25
    return max(spread_bps, 0.0) + (impact_bps + volatility_addon) * aggressiveness_multiplier
