from __future__ import annotations


def reconcile_order_snapshot(local_status: str, exchange_status: str) -> str:
    if not exchange_status:
        return local_status
    if local_status == exchange_status:
        return local_status
    terminal = {"filled", "canceled", "expired", "rejected", "closed"}
    if exchange_status.lower() in terminal:
        return exchange_status
    return exchange_status
