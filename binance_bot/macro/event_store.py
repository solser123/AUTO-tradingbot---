from __future__ import annotations

from ..storage import StateStore


def upsert_macro_events(store: StateStore, events: list[dict]) -> int:
    return store.upsert_macro_events(events)
