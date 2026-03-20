from __future__ import annotations


def classify_order_exception(exc: Exception) -> str:
    message = str(exc).lower()
    if any(token in message for token in ("timeout", "network", "temporarily unavailable", "recvwindow")):
        return "retryable"
    if any(token in message for token in ("min notional", "lot size", "precision", "insufficient")):
        return "exchange_rule_violation"
    if any(token in message for token in ("invalid api", "permission", "signature")):
        return "fatal"
    if "too many requests" in message or "rate limit" in message:
        return "throttling"
    return "unknown"
