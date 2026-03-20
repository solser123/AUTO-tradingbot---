from __future__ import annotations

from dataclasses import dataclass

from .exchange import BinanceExchange


@dataclass(frozen=True)
class MarketOrderPlan:
    symbol: str
    side: str
    reduce_only: bool
    requested_quantity: float
    normalized_quantity: float
    reference_price: float
    estimated_fill_price: float
    estimated_notional: float
    estimated_slippage_pct: float
    tick_size: float
    step_size: float
    min_amount: float
    min_notional: float
    reason: str


@dataclass(frozen=True)
class ExecutionResult:
    accepted: bool
    symbol: str
    side: str
    reduce_only: bool
    requested_quantity: float
    executed_quantity: float
    average_price: float
    filled_notional: float
    estimated_notional: float
    estimated_slippage_pct: float
    actual_slippage_pct: float
    order_id: str
    status: str
    reason: str
    raw_order: dict


class ExecutionRouter:
    def __init__(self, exchange: BinanceExchange) -> None:
        self.exchange = exchange

    def prepare_market_order(
        self,
        *,
        symbol: str,
        side: str,
        reference_price: float,
        requested_quantity: float,
        reduce_only: bool = False,
    ) -> MarketOrderPlan:
        rules = self.exchange.market_rules(symbol)
        allowed, normalized_quantity, reason = self.exchange.validate_order_quantity(
            symbol,
            requested_quantity,
            reference_price,
        )
        micro = self.exchange.fetch_microstructure(symbol)
        estimated_fill_price = self.exchange.estimate_market_fill_price(
            symbol,
            side,
            normalized_quantity,
            fallback_price=reference_price,
            microstructure=micro,
        )
        estimated_notional = estimated_fill_price * normalized_quantity
        estimated_slippage_pct = (
            abs(estimated_fill_price - reference_price) / reference_price if reference_price > 0 else 0.0
        )
        plan_reason = reason if allowed else f"Order rejected: {reason}"
        return MarketOrderPlan(
            symbol=symbol,
            side=side,
            reduce_only=reduce_only,
            requested_quantity=requested_quantity,
            normalized_quantity=normalized_quantity,
            reference_price=reference_price,
            estimated_fill_price=estimated_fill_price,
            estimated_notional=estimated_notional,
            estimated_slippage_pct=estimated_slippage_pct,
            tick_size=float(rules.get("tick_size") or 0.0),
            step_size=float(rules.get("step_size") or 0.0),
            min_amount=float(rules.get("min_amount") or 0.0),
            min_notional=float(rules.get("min_cost") or 0.0),
            reason=plan_reason,
        )

    def execute_market_order(self, plan: MarketOrderPlan) -> ExecutionResult:
        if plan.normalized_quantity <= 0:
            return ExecutionResult(
                accepted=False,
                symbol=plan.symbol,
                side=plan.side,
                reduce_only=plan.reduce_only,
                requested_quantity=plan.requested_quantity,
                executed_quantity=0.0,
                average_price=0.0,
                filled_notional=0.0,
                estimated_notional=plan.estimated_notional,
                estimated_slippage_pct=plan.estimated_slippage_pct,
                actual_slippage_pct=0.0,
                order_id="",
                status="rejected",
                reason=plan.reason,
                raw_order={},
            )

        order = self.exchange.create_market_order(
            plan.symbol,
            plan.side,
            plan.normalized_quantity,
            reduce_only=plan.reduce_only,
        )
        order_id = str(order.get("id") or "")
        resolved_order = self.exchange.fetch_order_snapshot(plan.symbol, order_id, fallback=order)
        average_price = self.exchange.resolve_fill_price(resolved_order, plan.estimated_fill_price)
        executed_quantity = self.exchange.resolve_filled_quantity(resolved_order, plan.normalized_quantity)
        filled_notional = average_price * executed_quantity
        actual_slippage_pct = (
            abs(average_price - plan.reference_price) / plan.reference_price if plan.reference_price > 0 else 0.0
        )
        status = str(resolved_order.get("status") or order.get("status") or "unknown")
        return ExecutionResult(
            accepted=True,
            symbol=plan.symbol,
            side=plan.side,
            reduce_only=plan.reduce_only,
            requested_quantity=plan.requested_quantity,
            executed_quantity=executed_quantity,
            average_price=average_price,
            filled_notional=filled_notional,
            estimated_notional=plan.estimated_notional,
            estimated_slippage_pct=plan.estimated_slippage_pct,
            actual_slippage_pct=actual_slippage_pct,
            order_id=order_id,
            status=status,
            reason="ok",
            raw_order=resolved_order,
        )
