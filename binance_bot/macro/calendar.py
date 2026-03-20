from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..storage import StateStore


def seed_default_macro_events(store: StateStore) -> int:
    now = datetime.now(timezone.utc)
    current_month = now.strftime("%Y-%m")
    defaults = [
        {
            "event_key": f"us-cpi-{current_month}",
            "title": "US CPI (placeholder)",
            "country": "US",
            "importance": "A",
            "scheduled_at": (now + timedelta(days=7)).replace(hour=12, minute=30, second=0, microsecond=0).isoformat(),
            "source": "seed",
            "raw_json": {"placeholder": True},
        },
        {
            "event_key": f"us-fomc-{current_month}",
            "title": "FOMC Rate Decision (placeholder)",
            "country": "US",
            "importance": "A",
            "scheduled_at": (now + timedelta(days=14)).replace(hour=18, minute=0, second=0, microsecond=0).isoformat(),
            "source": "seed",
            "raw_json": {"placeholder": True},
        },
    ]
    return store.upsert_macro_events(defaults)


def get_upcoming_macro_events(store: StateStore, hours: int = 48) -> list[dict]:
    rows = store.get_upcoming_macro_events(hours=hours, limit=50)
    return [
        {
            "event_key": str(row["event_key"]),
            "title": str(row["title"]),
            "country": str(row["country"]),
            "importance": str(row["importance"]),
            "scheduled_at": str(row["scheduled_at"]),
            "source": str(row["source"]),
        }
        for row in rows
    ]
