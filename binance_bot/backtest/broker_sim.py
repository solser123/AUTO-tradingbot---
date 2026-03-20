from __future__ import annotations

from dataclasses import dataclass

from .fills import estimate_slippage
from .funding import estimate_funding_cost


@dataclass(frozen=True)
class SimOrder:
    symbol: str
    side: str
    qty: float
    order_type: str
    reference_price: float
    aggressiveness: str = "balanced"


@dataclass(frozen=True)
class MarketContext:
    spread_bps: float
    depth_usd: float
    volatility_bps: float


@dataclass(frozen=True)
class SimFillResult:
    fill_price: float
    filled_qty: float
    fee: float
    slippage_bps: float


class BrokerSimulator:
    def __init__(self, taker_fee_rate: float = 0.0004) -> None:
        self.taker_fee_rate = taker_fee_rate

    def place_order(self, order: SimOrder, market_ctx: MarketContext) -> SimFillResult:
        slippage_bps = estimate_slippage(
            side=order.side,
            qty=order.qty * order.reference_price,
            spread_bps=market_ctx.spread_bps,
            depth_usd=market_ctx.depth_usd,
            volatility_bps=market_ctx.volatility_bps,
            aggressiveness=order.aggressiveness,
        )
        if order.side == "buy":
            fill_price = order.reference_price * (1 + slippage_bps / 10_000)
        else:
            fill_price = order.reference_price * (1 - slippage_bps / 10_000)
        fee = fill_price * order.qty * self.taker_fee_rate
        return SimFillResult(
            fill_price=fill_price,
            filled_qty=order.qty,
            fee=fee,
            slippage_bps=slippage_bps,
        )

    def apply_funding(self, notional: float, held_hours: float) -> float:
        return estimate_funding_cost(notional, held_hours)
