from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd

from .config import BotConfig
from .exchange import BinanceExchange
from .storage import _json_safe
from .storage import StateStore


def timeframe_to_minutes(timeframe: str) -> int:
    raw = timeframe.strip().lower()
    if raw.endswith("m"):
        return int(raw[:-1])
    if raw.endswith("h"):
        return int(raw[:-1]) * 60
    if raw.endswith("d"):
        return int(raw[:-1]) * 1440
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def analyze_pending_opportunities(
    store: StateStore,
    exchange: BinanceExchange,
    config: BotConfig,
    *,
    batch_limit: int = 60,
) -> int:
    decisions = store.get_unreviewed_no_entry_decisions(
        mode=config.mode,
        min_age_minutes=config.opportunity_lookahead_minutes,
        limit=batch_limit,
    )
    if not decisions:
        return 0

    reviewed = 0
    grouped: dict[str, list] = {}
    for row in decisions:
        grouped.setdefault(str(row["symbol"]), []).append(row)

    tf_minutes = timeframe_to_minutes(config.timeframe)
    bars_needed = max(int(config.opportunity_lookahead_minutes / max(tf_minutes, 1)) + 6, 24)
    limit = max(250, bars_needed + 40)

    for symbol, rows in grouped.items():
        try:
            execution_df = exchange.fetch_ohlcv(symbol, config.timeframe, limit=limit)
        except Exception:
            continue
        if execution_df.empty:
            continue

        enriched = execution_df.copy()
        enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True)
        enriched = enriched.sort_values("timestamp").reset_index(drop=True)

        for row in rows:
            result = _analyze_decision_row(row, enriched, config, tf_minutes)
            if result is None:
                continue
            store.log_opportunity_review(result)
            reviewed += 1

    return reviewed


def _analyze_decision_row(row, execution_df: pd.DataFrame, config: BotConfig, tf_minutes: int) -> dict | None:
    decision_time = datetime.fromisoformat(str(row["created_at"]))
    if decision_time.tzinfo is None:
        decision_time = decision_time.replace(tzinfo=timezone.utc)
    else:
        decision_time = decision_time.astimezone(timezone.utc)

    lookahead_bars = max(int(config.opportunity_lookahead_minutes / max(tf_minutes, 1)), 4)
    future = execution_df[execution_df["timestamp"] >= pd.Timestamp(decision_time)]
    if len(future) < 3:
        return None

    first_index = int(future.index[0])
    window = execution_df.iloc[first_index : first_index + lookahead_bars + 1]
    if len(window) < 3:
        return None

    entry_close = float(window.iloc[0]["close"])
    peak_price = float(window["high"].max())
    trough_price = float(window["low"].min())
    close_price = float(window.iloc[-1]["close"])
    peak_time = pd.Timestamp(window.loc[window["high"].idxmax(), "timestamp"]).to_pydatetime()
    trough_time = pd.Timestamp(window.loc[window["low"].idxmin(), "timestamp"]).to_pydatetime()

    up_pct = ((peak_price - entry_close) / entry_close) * 100 if entry_close else 0.0
    down_pct = ((entry_close - trough_price) / entry_close) * 100 if entry_close else 0.0
    dominant_side = "long" if up_pct >= down_pct else "short"
    dominant_move_pct = up_pct if dominant_side == "long" else down_pct
    close_move_pct = ((close_price - entry_close) / entry_close) * 100 if entry_close else 0.0
    notional_cost = config.notional_per_trade * (dominant_move_pct / 100.0)
    detail = str(row["detail"])
    block_tags = _classify_blockers(detail)

    return {
        "decision_log_id": int(row["id"]),
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
        "symbol": str(row["symbol"]),
        "decision_time": decision_time.isoformat(),
        "timeframe": config.timeframe,
        "lookahead_minutes": config.opportunity_lookahead_minutes,
        "entry_price": entry_close,
        "peak_price": peak_price,
        "trough_price": trough_price,
        "close_price": close_price,
        "peak_time": peak_time.astimezone(timezone.utc).isoformat(),
        "trough_time": trough_time.astimezone(timezone.utc).isoformat(),
        "dominant_side": dominant_side,
        "dominant_move_pct": dominant_move_pct,
        "up_move_pct": up_pct,
        "down_move_pct": down_pct,
        "close_move_pct": close_move_pct,
        "missed_notional_pnl": notional_cost,
        "is_material": 1 if dominant_move_pct >= config.opportunity_min_move_pct else 0,
        "blockers_csv": ",".join(block_tags),
        "detail": detail,
        "payload_json": json.dumps(
            _json_safe({
                "blockers": block_tags,
                "decision_payload": _safe_json_load(row["payload_json"]),
            }),
            ensure_ascii=False,
        ),
    }


def _classify_blockers(detail: str) -> list[str]:
    lowered = detail.lower()
    blockers: list[str] = []
    if "vwap" in lowered:
        blockers.append("vwap")
    if "stochastic" in lowered:
        blockers.append("stochastic")
    if "rsi" in lowered:
        blockers.append("rsi")
    if "higher timeframe" in lowered or "bias" in lowered:
        blockers.append("higher_bias")
    if "breakout" in lowered or "breakdown" in lowered or "recovery" in lowered or "continuation" in lowered:
        blockers.append("setup_confirmation")
    if not blockers:
        blockers.append("other")
    return blockers


def _safe_json_load(text: str) -> dict:
    try:
        loaded = json.loads(text or "{}")
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}
