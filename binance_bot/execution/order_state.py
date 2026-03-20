from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OrderLifecycle:
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    order_type: str
    side: str
    status: str
    requested_qty: float
    filled_qty: float
    avg_price: float | None
    is_algo: bool
    is_reduce_only: bool
    created_at: datetime
    updated_at: datetime
    raw_json: dict
