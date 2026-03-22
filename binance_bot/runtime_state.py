from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .storage import StateStore


TRANSIENT_REASON_PREFIXES = (
    "Exchange/runtime failure streak",
    "AI validation failure streak",
    "Abnormal slippage detected",
)

FATAL_REASON_KEYWORDS = (
    "invalid api",
    "authentication",
    "permission",
    "insufficient",
    "daily percentage loss limit",
    "weekly percentage loss limit",
    "account equity floor",
    "hard floor",
    "symbol is blocked",
)

RUNTIME_KEYS = [
    "emergency_stop",
    "emergency_reason",
    "emergency_severity",
    "emergency_set_at",
    "emergency_cleared_at",
    "exchange_failure_streak",
    "ai_failure_streak",
    "last_exchange_ok_at",
    "last_exchange_error_at",
    "last_order_error_at",
    "last_startup_reset_at",
    "last_runtime_recovery_at",
    "service_pid",
    "service_started_at",
    "service_stopped_at",
]


def load_runtime_flags(store: StateStore) -> dict[str, str]:
    snapshot = store.get_runtime_snapshot(RUNTIME_KEYS)
    flags: dict[str, str] = {}
    for key, record in snapshot.items():
        flags[key] = "" if record is None else str(record["value"])
    return flags


def set_runtime_flag(store: StateStore, key: str, value: str | int | float | bool) -> None:
    store.set_state(key, str(value))


def clear_emergency_stop(store: StateStore, reason_prefix: str | None = None) -> bool:
    current_reason = store.get_state("emergency_reason") or ""
    if reason_prefix and not current_reason.startswith(reason_prefix):
        return False
    store.clear_emergency_stop()
    return True


def reset_failure_streaks(store: StateStore, on_startup: bool = False) -> None:
    store.reset_state_counter("exchange_failure_streak")
    store.reset_state_counter("ai_failure_streak")
    if on_startup:
        set_runtime_flag(store, "last_startup_reset_at", datetime.now(timezone.utc).isoformat())


def classify_emergency_reason(reason: str) -> str:
    normalized = (reason or "").strip().lower()
    if not normalized:
        return "none"
    if normalized.startswith(tuple(prefix.lower() for prefix in TRANSIENT_REASON_PREFIXES)):
        return "transient"
    if any(keyword in normalized for keyword in FATAL_REASON_KEYWORDS):
        return "fatal"
    return "fatal"


def recover_runtime_state(
    store: StateStore,
    *,
    exchange_ok: bool,
    exchange_message: str,
    now: datetime | None = None,
) -> tuple[bool, str]:
    reference = now or datetime.now(timezone.utc)
    flags = load_runtime_flags(store)
    active = flags.get("emergency_stop") == "1"
    reason = flags.get("emergency_reason", "")
    severity = flags.get("emergency_severity") or classify_emergency_reason(reason)
    recoverable, message = runtime_recovery_status(
        store,
        exchange_ok=exchange_ok,
        exchange_message=exchange_message,
        now=reference,
    )
    if not active:
        if exchange_ok:
            set_runtime_flag(store, "last_exchange_ok_at", reference.isoformat())
        return False, "No active emergency stop."
    set_runtime_flag(store, "emergency_severity", severity)
    if not recoverable:
        if exchange_ok:
            set_runtime_flag(store, "last_exchange_ok_at", reference.isoformat())
        else:
            set_runtime_flag(store, "last_exchange_error_at", reference.isoformat())
        return False, message

    reset_failure_streaks(store, on_startup=True)
    store.clear_emergency_stop()
    set_runtime_flag(store, "last_runtime_recovery_at", reference.isoformat())
    store.log_decision(
        symbol="SYSTEM",
        mode="system",
        stage="runtime_recovery",
        outcome="cleared",
        detail=f"Recovered transient emergency stop after healthcheck: {reason}",
        payload={"exchange_message": exchange_message},
    )
    return True, "Transient emergency stop was auto-cleared after startup healthcheck."


def runtime_recovery_status(
    store: StateStore,
    *,
    exchange_ok: bool,
    exchange_message: str,
    now: datetime | None = None,
) -> tuple[bool, str]:
    reference = now or datetime.now(timezone.utc)
    flags = load_runtime_flags(store)
    active = flags.get("emergency_stop") == "1"
    reason = flags.get("emergency_reason", "")
    severity = flags.get("emergency_severity") or classify_emergency_reason(reason)
    if not active:
        return True, "No active emergency stop."
    if not exchange_ok:
        return False, f"Exchange healthcheck failed: {exchange_message}"
    if severity != "transient":
        return False, "Emergency reason is fatal."

    emergency_set_at = flags.get("emergency_set_at") or ""
    if emergency_set_at:
        try:
            emergency_time = datetime.fromisoformat(emergency_set_at)
            if emergency_time.tzinfo is None:
                emergency_time = emergency_time.replace(tzinfo=timezone.utc)
            if reference - emergency_time < timedelta(seconds=60):
                return False, "Emergency stop is too recent to auto-recover."
        except Exception:
            pass

    last_order_error_at = flags.get("last_order_error_at") or ""
    if last_order_error_at:
        try:
            order_error_time = datetime.fromisoformat(last_order_error_at)
            if order_error_time.tzinfo is None:
                order_error_time = order_error_time.replace(tzinfo=timezone.utc)
            if reference - order_error_time < timedelta(minutes=5):
                return False, "Recent order error blocks auto-recovery."
        except Exception:
            pass

    return True, "Transient stop is recoverable after healthcheck."
