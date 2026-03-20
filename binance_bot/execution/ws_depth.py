from __future__ import annotations


class LocalDepthBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.last_update_id = 0
        self.bids: list[list[float]] = []
        self.asks: list[list[float]] = []

    def load_snapshot(self, snapshot: dict) -> None:
        self.last_update_id = int(snapshot.get("lastUpdateId") or 0)
        self.bids = [[float(price), float(size)] for price, size in snapshot.get("bids", [])]
        self.asks = [[float(price), float(size)] for price, size in snapshot.get("asks", [])]

    def apply_diff(self, payload: dict) -> bool:
        first_update = int(payload.get("U") or 0)
        final_update = int(payload.get("u") or 0)
        if self.last_update_id and final_update <= self.last_update_id:
            return True
        if self.last_update_id and first_update > self.last_update_id + 1:
            return False
        self.last_update_id = final_update
        return True
