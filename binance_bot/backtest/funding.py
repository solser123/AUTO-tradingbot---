from __future__ import annotations


def estimate_funding_cost(notional: float, held_hours: float, funding_rate_per_8h: float = 0.0001) -> float:
    if notional <= 0 or held_hours <= 0:
        return 0.0
    return notional * funding_rate_per_8h * (held_hours / 8.0)
