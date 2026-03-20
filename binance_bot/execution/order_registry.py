from __future__ import annotations

from datetime import datetime, timezone

from .order_state import OrderLifecycle
from ..storage import StateStore


class OrderRegistry:
    def __init__(self, store: StateStore) -> None:
        self.store = store

    def record(self, lifecycle: OrderLifecycle) -> int:
        return self.store.upsert_order_lifecycle(
            {
                "client_order_id": lifecycle.client_order_id,
                "exchange_order_id": lifecycle.exchange_order_id or "",
                "symbol": lifecycle.symbol,
                "order_type": lifecycle.order_type,
                "side": lifecycle.side,
                "status": lifecycle.status,
                "requested_qty": lifecycle.requested_qty,
                "filled_qty": lifecycle.filled_qty,
                "avg_price": lifecycle.avg_price or 0.0,
                "is_algo": lifecycle.is_algo,
                "is_reduce_only": lifecycle.is_reduce_only,
                "raw_json": lifecycle.raw_json,
                "created_at": lifecycle.created_at.isoformat(),
                "updated_at": lifecycle.updated_at.isoformat(),
            }
        )

    def from_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        requested_qty: float,
        reduce_only: bool,
        order: dict,
        status: str,
    ) -> OrderLifecycle:
        now = datetime.now(timezone.utc)
        return OrderLifecycle(
            client_order_id=str(order.get("clientOrderId") or ""),
            exchange_order_id=str(order.get("id") or "") or None,
            symbol=symbol,
            order_type=order_type,
            side=side,
            status=status,
            requested_qty=requested_qty,
            filled_qty=float(order.get("filled") or order.get("amount") or 0.0),
            avg_price=float(order.get("average") or order.get("price") or 0.0) or None,
            is_algo=bool(order.get("stopPrice") or order.get("triggerPrice")),
            is_reduce_only=reduce_only,
            created_at=now,
            updated_at=now,
            raw_json=order,
        )
