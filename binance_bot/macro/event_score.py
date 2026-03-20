from __future__ import annotations


def macro_importance_penalty(importance: str, minutes_to_event: float) -> float:
    level = (importance or "").upper()
    if level == "A":
        if -30 <= minutes_to_event <= 60:
            return -100.0
        if -120 <= minutes_to_event <= 180:
            return -20.0
    if level == "B":
        if -30 <= minutes_to_event <= 60:
            return -12.0
        if -120 <= minutes_to_event <= 180:
            return -6.0
    if level == "C" and -30 <= minutes_to_event <= 60:
        return -3.0
    return 0.0
