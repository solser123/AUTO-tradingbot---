from __future__ import annotations

from datetime import datetime, timezone

from .order_registry import OrderRegistry
from .order_state import OrderLifecycle


class UserStreamConsumer:
    def __init__(self, registry: OrderRegistry) -> None:
        self.registry = registry

    def on_order_trade_update(self, payload: dict) -> int:
        order = payload.get("o", payload)
        lifecycle = OrderLifecycle(
            client_order_id=str(order.get("c") or order.get("clientOrderId") or ""),
            exchange_order_id=str(order.get("i") or order.get("orderId") or "") or None,
            symbol=str(order.get("s") or order.get("symbol") or ""),
            order_type=str(order.get("o") or order.get("type") or ""),
            side=str(order.get("S") or order.get("side") or ""),
            status=str(order.get("X") or order.get("status") or ""),
            requested_qty=float(order.get("q") or order.get("origQty") or 0.0),
            filled_qty=float(order.get("z") or order.get("executedQty") or 0.0),
            avg_price=float(order.get("ap") or order.get("avgPrice") or 0.0) or None,
            is_algo=bool(order.get("sp") or order.get("stopPrice") or order.get("wt")),
            is_reduce_only=bool(order.get("R") or order.get("reduceOnly")),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            raw_json=payload,
        )
        return self.registry.record(lifecycle)

    def on_algo_update(self, payload: dict) -> int:
        return self.on_order_trade_update(payload)

    def on_trade_lite(self, payload: dict) -> int:
        return self.on_order_trade_update(payload)
