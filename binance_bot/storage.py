from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .models import Position, TradeSignal


KST = ZoneInfo("Asia/Seoul")


def _json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return str(value)


def trading_day_anchor(reference: datetime | None = None) -> datetime:
    local_now = (reference or datetime.now(KST)).astimezone(KST)
    anchor = local_now.replace(hour=8, minute=0, second=0, microsecond=0)
    if local_now < anchor:
        anchor -= timedelta(days=1)
    return anchor


def trading_week_anchor(reference: datetime | None = None) -> datetime:
    day_anchor = trading_day_anchor(reference)
    return day_anchor - timedelta(days=day_anchor.weekday())


class StateStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    entry_profile TEXT NOT NULL DEFAULT 'conservative',
                    profile_stage TEXT NOT NULL DEFAULT 'conservative',
                    half_defense_trigger REAL NOT NULL DEFAULT 0,
                    full_defense_trigger REAL NOT NULL DEFAULT 0,
                    opened_at TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    exit_price REAL,
                    closed_at TEXT,
                    exit_reason TEXT,
                    realized_pnl REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_price REAL NOT NULL,
                    target_price REAL NOT NULL,
                    rr REAL NOT NULL,
                    approved INTEGER NOT NULL,
                    ai_confidence REAL NOT NULL,
                    setup_type TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS external_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    published_at TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    sentiment_score REAL NOT NULL,
                    symbols_json TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS opportunity_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_log_id INTEGER NOT NULL UNIQUE,
                    reviewed_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    decision_time TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    lookahead_minutes INTEGER NOT NULL,
                    entry_price REAL NOT NULL,
                    peak_price REAL NOT NULL,
                    trough_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    peak_time TEXT NOT NULL,
                    trough_time TEXT NOT NULL,
                    dominant_side TEXT NOT NULL,
                    dominant_move_pct REAL NOT NULL,
                    up_move_pct REAL NOT NULL,
                    down_move_pct REAL NOT NULL,
                    close_move_pct REAL NOT NULL,
                    missed_notional_pnl REAL NOT NULL,
                    is_material INTEGER NOT NULL,
                    blockers_csv TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sector_flow_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_at TEXT NOT NULL,
                    sector TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    flow_score REAL NOT NULL,
                    avg_short_return_pct REAL NOT NULL,
                    avg_medium_return_pct REAL NOT NULL,
                    avg_volume_ratio REAL NOT NULL,
                    liquidity_usdt REAL NOT NULL,
                    symbol_count INTEGER NOT NULL,
                    leaders_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(snapshot_at, sector)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_tag TEXT,
                    started_at TEXT,
                    ended_at TEXT,
                    config_json TEXT,
                    symbols_json TEXT,
                    metrics_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_trade (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    entry_time TEXT,
                    exit_time TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    qty REAL,
                    fee REAL,
                    funding REAL,
                    slippage_bps REAL,
                    pnl REAL,
                    mae REAL,
                    mfe REAL,
                    reason_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_lifecycle (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT,
                    exchange_order_id TEXT,
                    symbol TEXT,
                    order_type TEXT,
                    side TEXT,
                    status TEXT,
                    requested_qty REAL,
                    filled_qty REAL,
                    avg_price REAL,
                    is_algo INTEGER,
                    is_reduce_only INTEGER,
                    raw_json TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS macro_event (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE,
                    title TEXT,
                    country TEXT,
                    importance TEXT,
                    scheduled_at TEXT,
                    source TEXT,
                    raw_json TEXT
                )
                """
            )
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(positions)").fetchall()}
            if columns and "realized_pnl" not in columns:
                conn.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL")
            migrations = {
                "entry_profile": "ALTER TABLE positions ADD COLUMN entry_profile TEXT NOT NULL DEFAULT 'conservative'",
                "profile_stage": "ALTER TABLE positions ADD COLUMN profile_stage TEXT NOT NULL DEFAULT 'conservative'",
                "half_defense_trigger": "ALTER TABLE positions ADD COLUMN half_defense_trigger REAL NOT NULL DEFAULT 0",
                "full_defense_trigger": "ALTER TABLE positions ADD COLUMN full_defense_trigger REAL NOT NULL DEFAULT 0",
            }
            for column, sql in migrations.items():
                if column not in columns:
                    conn.execute(sql)

    def log_signal(self, signal: TradeSignal, approved: bool, ai_confidence: float, reason: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO signals (
                    created_at, symbol, side, entry_price, stop_price, target_price,
                    rr, approved, ai_confidence, setup_type, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    signal.symbol,
                    signal.side,
                    signal.entry_price,
                    signal.stop_price,
                    signal.target_price,
                    signal.rr,
                    int(approved),
                    ai_confidence,
                    signal.setup_type,
                    reason,
                ),
            )

    def log_decision(
        self,
        symbol: str,
        mode: str,
        stage: str,
        outcome: str,
        detail: str,
        payload: dict | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO decision_log (
                    created_at, symbol, mode, stage, outcome, detail, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    symbol,
                    mode,
                    stage,
                    outcome,
                    detail,
                    json.dumps(_json_safe(payload or {}), ensure_ascii=False),
                ),
            )

    def has_recent_decision(
        self,
        *,
        symbol: str,
        mode: str,
        stage: str,
        outcome: str,
        detail: str,
        within_minutes: int,
    ) -> bool:
        anchor = datetime.now(timezone.utc) - timedelta(minutes=max(within_minutes, 1))
        query = """
                SELECT 1
                FROM decision_log
                WHERE symbol = ?
                  AND mode = ?
                  AND stage = ?
                  AND outcome = ?
                  AND detail = ?
                  AND created_at >= ?
                ORDER BY id DESC
                LIMIT 1
                """
        with self._connect() as conn:
            row = conn.execute(
                query,
                (symbol, mode, stage, outcome, detail, anchor.isoformat()),
            ).fetchone()
        return row is not None

    def get_open_position(self, symbol: str, mode: str | None = None) -> Position | None:
        query = """
                SELECT * FROM positions
                WHERE symbol = ? AND status = 'OPEN'
                """
        params: list[object] = [symbol]
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        query += """
                ORDER BY id DESC
                LIMIT 1
                """
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()

        if row is None:
            return None

        return Position(
            id=row["id"],
            symbol=row["symbol"],
            side=row["side"],
            quantity=row["quantity"],
            entry_price=row["entry_price"],
            stop_price=row["stop_price"],
            target_price=row["target_price"],
            entry_profile=row["entry_profile"],
            profile_stage=row["profile_stage"],
            half_defense_trigger=row["half_defense_trigger"],
            full_defense_trigger=row["full_defense_trigger"],
            opened_at=datetime.fromisoformat(row["opened_at"]),
            mode=row["mode"],
            status=row["status"],
        )

    def get_open_positions(self, mode: str | None = None) -> list[Position]:
        query = "SELECT * FROM positions WHERE status = 'OPEN'"
        params: list[object] = []
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        query += " ORDER BY opened_at ASC"
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [
            Position(
                id=row["id"],
                symbol=row["symbol"],
                side=row["side"],
                quantity=row["quantity"],
                entry_price=row["entry_price"],
                stop_price=row["stop_price"],
                target_price=row["target_price"],
                entry_profile=row["entry_profile"],
                profile_stage=row["profile_stage"],
                half_defense_trigger=row["half_defense_trigger"],
                full_defense_trigger=row["full_defense_trigger"],
                opened_at=datetime.fromisoformat(row["opened_at"]),
                mode=row["mode"],
                status=row["status"],
            )
            for row in rows
        ]

    def count_open_positions(self, mode: str | None = None) -> int:
        with self._connect() as conn:
            if mode is None:
                row = conn.execute("SELECT COUNT(*) AS count FROM positions WHERE status = 'OPEN'").fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) AS count FROM positions WHERE status = 'OPEN' AND mode = ?",
                    (mode,),
                ).fetchone()
        return int(row["count"])

    def get_open_symbols(self, mode: str | None = None) -> list[str]:
        with self._connect() as conn:
            if mode is None:
                rows = conn.execute("SELECT DISTINCT symbol FROM positions WHERE status = 'OPEN'").fetchall()
            else:
                rows = conn.execute(
                    "SELECT DISTINCT symbol FROM positions WHERE status = 'OPEN' AND mode = ?",
                    (mode,),
                ).fetchall()
        return [str(row["symbol"]) for row in rows]

    def open_position(self, position: Position) -> Position:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO positions (
                    symbol, side, quantity, entry_price, stop_price, target_price,
                    entry_profile, profile_stage, half_defense_trigger, full_defense_trigger,
                    opened_at, mode, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.symbol,
                    position.side,
                    position.quantity,
                    position.entry_price,
                    position.stop_price,
                    position.target_price,
                    position.entry_profile,
                    position.profile_stage,
                    position.half_defense_trigger,
                    position.full_defense_trigger,
                    position.opened_at.isoformat(),
                    position.mode,
                    position.status,
                ),
            )
            position.id = int(cursor.lastrowid)
        return position

    def update_position_stage(self, position_id: int, quantity: float, profile_stage: str) -> None:
        self.update_position_management(
            position_id,
            quantity=quantity,
            profile_stage=profile_stage,
        )

    def update_position_target(self, position_id: int, target_price: float) -> None:
        self.update_position_management(
            position_id,
            target_price=target_price,
        )

    def update_position_management(
        self,
        position_id: int,
        *,
        quantity: float | None = None,
        profile_stage: str | None = None,
        target_price: float | None = None,
    ) -> None:
        updates: list[str] = []
        params: list[object] = []
        if quantity is not None:
            updates.append("quantity = ?")
            params.append(quantity)
        if profile_stage is not None:
            updates.append("profile_stage = ?")
            params.append(profile_stage)
        if target_price is not None:
            updates.append("target_price = ?")
            params.append(target_price)
        if not updates:
            return
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE positions
                SET {', '.join(updates)}
                WHERE id = ?
                """,
                (*params, position_id),
            )

    def cleanup_zero_quantity_open_positions(
        self,
        mode: str,
        *,
        reason: str = "zero_quantity_cleanup",
        quantity_epsilon: float = 1e-9,
    ) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, symbol, entry_price, quantity
                FROM positions
                WHERE status = 'OPEN'
                  AND mode = ?
                  AND ABS(quantity) <= ?
                ORDER BY id
                """,
                (mode, quantity_epsilon),
            ).fetchall()

        cleaned: list[str] = []
        for row in rows:
            self.close_position(int(row["id"]), float(row["entry_price"]), reason)
            cleaned.append(str(row["symbol"]))
        return cleaned

    def close_position(self, position_id: int, exit_price: float, exit_reason: str) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT symbol, side, quantity, entry_price, mode FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()

        if row is None:
            return

        quantity = float(row["quantity"])
        entry_price = float(row["entry_price"])
        side = row["side"]
        symbol = str(row["symbol"])
        mode = str(row["mode"])
        if side == "long":
            realized_pnl = (exit_price - entry_price) * quantity
        else:
            realized_pnl = (entry_price - exit_price) * quantity

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE positions
                SET status = 'CLOSED',
                    exit_price = ?,
                    closed_at = ?,
                    exit_reason = ?,
                    realized_pnl = ?
                WHERE id = ?
                """,
                (exit_price, datetime.now(timezone.utc).isoformat(), exit_reason, realized_pnl, position_id),
            )

        self.log_decision(
            symbol=symbol,
            mode=mode,
            stage="position_close",
            outcome=exit_reason,
            detail=f"Closed {side} position at {exit_price:.6f} with pnl {realized_pnl:.6f}.",
            payload={
                "position_id": position_id,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "quantity": quantity,
                "realized_pnl": realized_pnl,
            },
        )

    def get_today_realized_pnl(self, mode: str | None = None, reference: datetime | None = None) -> float:
        anchor = trading_day_anchor(reference).astimezone(ZoneInfo("UTC"))
        return self._sum_realized_pnl_since(anchor, mode)

    def get_week_realized_pnl(self, mode: str | None = None, reference: datetime | None = None) -> float:
        anchor = trading_week_anchor(reference).astimezone(ZoneInfo("UTC"))
        return self._sum_realized_pnl_since(anchor, mode)

    def _sum_realized_pnl_since(self, anchor_utc: datetime, mode: str | None = None) -> float:
        query = """
                SELECT COALESCE(SUM(realized_pnl), 0) AS pnl
                FROM positions
                WHERE status = 'CLOSED'
                  AND closed_at IS NOT NULL
                  AND closed_at >= ?
                """
        params: list[object] = [anchor_utc.isoformat()]
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
        return float(row["pnl"])

    def get_open_exposure(self, mode: str | None = None) -> float:
        query = """
                SELECT COALESCE(SUM(entry_price * quantity), 0) AS exposure
                FROM positions
                WHERE status = 'OPEN'
                """
        params: list[object] = []
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
        return float(row["exposure"])

    def get_summary(self) -> dict[str, float | int]:
        with self._connect() as conn:
            total_signals = int(conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0])
            approved_signals = int(conn.execute("SELECT COUNT(*) FROM signals WHERE approved = 1").fetchone()[0])
            open_positions = int(conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'OPEN'").fetchone()[0])
            closed_positions = int(conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'CLOSED'").fetchone()[0])
            realized_pnl = float(
                conn.execute(
                    "SELECT COALESCE(SUM(realized_pnl), 0) FROM positions WHERE status = 'CLOSED'"
                ).fetchone()[0]
            )
            wins = int(
                conn.execute(
                    "SELECT COUNT(*) FROM positions WHERE status = 'CLOSED' AND realized_pnl > 0"
                ).fetchone()[0]
            )
            decision_rows = conn.execute("SELECT COUNT(*) FROM decision_log").fetchone()
        win_rate = (wins / closed_positions) * 100 if closed_positions else 0.0
        return {
            "total_signals": total_signals,
            "approved_signals": approved_signals,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "realized_pnl": realized_pnl,
            "win_rate": win_rate,
            "decision_events": int(decision_rows[0] if decision_rows else 0),
        }

    def get_closed_positions(self, mode: str | None = None) -> list[sqlite3.Row]:
        query = """
                SELECT *
                FROM positions
                WHERE status = 'CLOSED' AND closed_at IS NOT NULL
                """
        params: list[object] = []
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        query += " ORDER BY closed_at ASC, id ASC"
        with self._connect() as conn:
            return conn.execute(query, tuple(params)).fetchall()

    def get_trade_metrics(self, mode: str | None = None) -> dict[str, float | int]:
        rows = self.get_closed_positions(mode)
        pnls = [float(row["realized_pnl"] or 0.0) for row in rows]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
        expectancy = sum(pnls) / len(pnls) if pnls else 0.0
        cumulative = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for pnl in pnls:
            cumulative += pnl
            peak = max(peak, cumulative)
            max_drawdown = max(max_drawdown, peak - cumulative)
        return {
            "trades": len(pnls),
            "wins": len(wins),
            "losses": len(losses),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "realized_pnl": sum(pnls),
            "avg_win": (gross_profit / len(wins)) if wins else 0.0,
            "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
            "max_drawdown_abs": max_drawdown,
        }

    def count_decisions(
        self,
        mode: str | None = None,
        stage: str | None = None,
        outcome: str | None = None,
        detail_contains: str | None = None,
    ) -> int:
        query = "SELECT COUNT(*) AS count FROM decision_log WHERE 1=1"
        params: list[object] = []
        if mode is not None:
            query += " AND mode = ?"
            params.append(mode)
        if stage is not None:
            query += " AND stage = ?"
            params.append(stage)
        if outcome is not None:
            query += " AND outcome = ?"
            params.append(outcome)
        if detail_contains is not None:
            query += " AND detail LIKE ?"
            params.append(f"%{detail_contains}%")
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
        return int(row["count"] if row else 0)

    def upsert_external_items(self, items: list[dict]) -> int:
        inserted = 0
        if not items:
            return inserted
        with self._connect() as conn:
            for item in items:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO external_items (
                        fetched_at, source, source_type, title, summary, url,
                        published_at, direction, sentiment_score, symbols_json, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        datetime.now(timezone.utc).isoformat(),
                        str(item.get("source", "")),
                        str(item.get("source_type", "")),
                        str(item.get("title", "")),
                        str(item.get("summary", "")),
                        str(item.get("url", "")),
                        str(item.get("published_at", datetime.now(timezone.utc).isoformat())),
                        str(item.get("direction", "neutral")),
                        float(item.get("sentiment_score", 0.0) or 0.0),
                        json.dumps(_json_safe(item.get("symbols", [])), ensure_ascii=False),
                        json.dumps(_json_safe(item.get("raw_json", {})), ensure_ascii=False),
                    ),
                )
                inserted += int(cursor.rowcount > 0)
        return inserted

    def get_recent_external_items(self, limit: int = 10, symbol: str | None = None, hours: int = 24) -> list[sqlite3.Row]:
        anchor = datetime.now(timezone.utc) - timedelta(hours=max(hours, 1))
        query = """
                SELECT *
                FROM external_items
                WHERE published_at >= ?
                """
        params: list[object] = [anchor.isoformat()]
        if symbol is not None:
            query += " AND symbols_json LIKE ?"
            params.append(f"%{symbol}%")
        query += " ORDER BY published_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            return conn.execute(query, tuple(params)).fetchall()

    def get_external_alignment(self, symbol: str, side: str, hours: int = 24) -> dict[str, float | int]:
        rows = self.get_recent_external_items(limit=100, symbol=symbol, hours=hours)
        if not rows:
            return {
                "count": 0,
                "alignment_score": 0.0,
                "community_score": 0.0,
                "news_score": 0.0,
            }
        total = 0.0
        community = 0.0
        news = 0.0
        community_count = 0
        news_count = 0
        for row in rows:
            score = float(row["sentiment_score"] or 0.0)
            total += score
            if row["source"] == "tradingview":
                community += score
                community_count += 1
            elif row["source"] == "blockmedia":
                news += score
                news_count += 1
        avg = total / len(rows)
        side_adjusted = avg if side == "long" else -avg
        return {
            "count": len(rows),
            "alignment_score": side_adjusted,
            "community_score": (community / community_count) if community_count else 0.0,
            "news_score": (news / news_count) if news_count else 0.0,
        }

    def log_sector_flow_snapshot(self, snapshot: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sector_flow_snapshots (
                    snapshot_at, sector, direction, flow_score, avg_short_return_pct,
                    avg_medium_return_pct, avg_volume_ratio, liquidity_usdt, symbol_count,
                    leaders_json, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(snapshot["snapshot_at"]),
                    str(snapshot["sector"]),
                    str(snapshot["direction"]),
                    float(snapshot["flow_score"]),
                    float(snapshot["avg_short_return_pct"]),
                    float(snapshot["avg_medium_return_pct"]),
                    float(snapshot["avg_volume_ratio"]),
                    float(snapshot["liquidity_usdt"]),
                    int(snapshot["symbol_count"]),
                    json.dumps(_json_safe(snapshot.get("leaders", [])), ensure_ascii=False),
                    json.dumps(_json_safe(snapshot.get("payload", {})), ensure_ascii=False),
                ),
            )

    def get_latest_sector_flow(self, sector: str) -> dict[str, object]:
        query = """
                SELECT *
                FROM sector_flow_snapshots
                WHERE sector = ?
                ORDER BY snapshot_at DESC, id DESC
                LIMIT 1
                """
        with self._connect() as conn:
            row = conn.execute(query, (sector,)).fetchone()
        if row is None:
            return {
                "sector": sector,
                "direction": "neutral",
                "flow_score": 0.0,
                "avg_short_return_pct": 0.0,
                "avg_medium_return_pct": 0.0,
                "avg_volume_ratio": 0.0,
                "liquidity_usdt": 0.0,
                "symbol_count": 0,
                "leaders": [],
            }
        return {
            "sector": str(row["sector"]),
            "direction": str(row["direction"]),
            "flow_score": float(row["flow_score"]),
            "avg_short_return_pct": float(row["avg_short_return_pct"]),
            "avg_medium_return_pct": float(row["avg_medium_return_pct"]),
            "avg_volume_ratio": float(row["avg_volume_ratio"]),
            "liquidity_usdt": float(row["liquidity_usdt"]),
            "symbol_count": int(row["symbol_count"]),
            "leaders": json.loads(row["leaders_json"] or "[]"),
            "snapshot_at": str(row["snapshot_at"]),
        }

    def get_latest_sector_flows(self, limit: int = 8) -> list[dict[str, object]]:
        query = """
                SELECT s.*
                FROM sector_flow_snapshots s
                INNER JOIN (
                    SELECT sector, MAX(id) AS max_id
                    FROM sector_flow_snapshots
                    GROUP BY sector
                ) latest
                    ON latest.max_id = s.id
                ORDER BY ABS(s.flow_score) DESC, s.liquidity_usdt DESC
                LIMIT ?
                """
        with self._connect() as conn:
            rows = conn.execute(query, (limit,)).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            result.append(
                {
                    "sector": str(row["sector"]),
                    "direction": str(row["direction"]),
                    "flow_score": float(row["flow_score"]),
                    "avg_short_return_pct": float(row["avg_short_return_pct"]),
                    "avg_medium_return_pct": float(row["avg_medium_return_pct"]),
                    "avg_volume_ratio": float(row["avg_volume_ratio"]),
                    "liquidity_usdt": float(row["liquidity_usdt"]),
                    "symbol_count": int(row["symbol_count"]),
                    "leaders": json.loads(row["leaders_json"] or "[]"),
                    "snapshot_at": str(row["snapshot_at"]),
                }
            )
        return result

    def get_unreviewed_no_entry_decisions(
        self,
        *,
        mode: str,
        min_age_minutes: int,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        anchor = datetime.now(timezone.utc) - timedelta(minutes=max(min_age_minutes, 1))
        query = """
                SELECT dl.*
                FROM decision_log dl
                LEFT JOIN opportunity_reviews opr ON opr.decision_log_id = dl.id
                WHERE dl.mode = ?
                  AND dl.outcome = 'no_entry'
                  AND dl.stage = 'scan'
                  AND dl.created_at <= ?
                  AND opr.decision_log_id IS NULL
                ORDER BY dl.created_at ASC, dl.id ASC
                LIMIT ?
                """
        with self._connect() as conn:
            return conn.execute(query, (mode, anchor.isoformat(), limit)).fetchall()

    def log_opportunity_review(self, review: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO opportunity_reviews (
                    decision_log_id, reviewed_at, symbol, decision_time, timeframe, lookahead_minutes,
                    entry_price, peak_price, trough_price, close_price, peak_time, trough_time,
                    dominant_side, dominant_move_pct, up_move_pct, down_move_pct, close_move_pct,
                    missed_notional_pnl, is_material, blockers_csv, detail, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(review["decision_log_id"]),
                    str(review["reviewed_at"]),
                    str(review["symbol"]),
                    str(review["decision_time"]),
                    str(review["timeframe"]),
                    int(review["lookahead_minutes"]),
                    float(review["entry_price"]),
                    float(review["peak_price"]),
                    float(review["trough_price"]),
                    float(review["close_price"]),
                    str(review["peak_time"]),
                    str(review["trough_time"]),
                    str(review["dominant_side"]),
                    float(review["dominant_move_pct"]),
                    float(review["up_move_pct"]),
                    float(review["down_move_pct"]),
                    float(review["close_move_pct"]),
                    float(review["missed_notional_pnl"]),
                    int(review["is_material"]),
                    str(review["blockers_csv"]),
                    str(review["detail"]),
                    str(review["payload_json"]),
                ),
            )

    def get_opportunity_reviews(
        self,
        *,
        symbol: str | None = None,
        hours: int = 48,
        only_material: bool = False,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        anchor = datetime.now(timezone.utc) - timedelta(hours=max(hours, 1))
        query = """
                SELECT *
                FROM opportunity_reviews
                WHERE decision_time >= ?
                """
        params: list[object] = [anchor.isoformat()]
        if symbol is not None:
            query += " AND symbol = ?"
            params.append(symbol)
        if only_material:
            query += " AND is_material = 1"
        query += " ORDER BY dominant_move_pct DESC, decision_time DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            return conn.execute(query, tuple(params)).fetchall()

    def get_opportunity_summary(self, *, symbol: str | None = None, hours: int = 48) -> dict[str, float | int]:
        rows = self.get_opportunity_reviews(symbol=symbol, hours=hours, only_material=False, limit=500)
        if not rows:
            return {
                "reviews": 0,
                "material_reviews": 0,
                "avg_move_pct": 0.0,
                "best_move_pct": 0.0,
                "missed_notional_pnl": 0.0,
            }
        material = [row for row in rows if int(row["is_material"]) == 1]
        return {
            "reviews": len(rows),
            "material_reviews": len(material),
            "avg_move_pct": sum(float(row["dominant_move_pct"]) for row in rows) / len(rows),
            "best_move_pct": max(float(row["dominant_move_pct"]) for row in rows),
            "missed_notional_pnl": sum(float(row["missed_notional_pnl"]) for row in material),
        }

    def get_symbol_stoploss_streak(self, symbol: str, mode: str) -> int:
        query = """
                SELECT exit_reason
                FROM positions
                WHERE status = 'CLOSED' AND mode = ? AND symbol = ?
                ORDER BY closed_at DESC
                LIMIT 20
                """
        with self._connect() as conn:
            rows = conn.execute(query, (mode, symbol)).fetchall()
        streak = 0
        for row in rows:
            if row["exit_reason"] == "stop_loss":
                streak += 1
                continue
            break
        return streak

    def get_global_stoploss_streak(self, mode: str) -> int:
        query = """
                SELECT exit_reason
                FROM positions
                WHERE status = 'CLOSED' AND mode = ?
                ORDER BY closed_at DESC
                LIMIT 50
                """
        with self._connect() as conn:
            rows = conn.execute(query, (mode,)).fetchall()
        streak = 0
        for row in rows:
            if row["exit_reason"] == "stop_loss":
                streak += 1
                continue
            break
        return streak

    def get_last_stoploss_closed_at(self, symbol: str, mode: str) -> datetime | None:
        query = """
                SELECT closed_at
                FROM positions
                WHERE status = 'CLOSED' AND mode = ? AND symbol = ? AND exit_reason = 'stop_loss'
                ORDER BY closed_at DESC
                LIMIT 1
                """
        with self._connect() as conn:
            row = conn.execute(query, (mode, symbol)).fetchone()
        if row is None or not row["closed_at"]:
            return None
        parsed = datetime.fromisoformat(str(row["closed_at"]))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    def get_state(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM runtime_state WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def get_state_record(self, key: str) -> dict[str, str] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, value, updated_at FROM runtime_state WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "key": str(row["key"]),
            "value": str(row["value"]),
            "updated_at": str(row["updated_at"]),
        }

    def get_runtime_snapshot(self, keys: list[str]) -> dict[str, dict[str, str] | None]:
        snapshot: dict[str, dict[str, str] | None] = {}
        for key in keys:
            snapshot[key] = self.get_state_record(key)
        return snapshot

    def set_state(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runtime_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (key, value, datetime.now(timezone.utc).isoformat()),
            )

    def increment_state_counter(self, key: str) -> int:
        current = int(self.get_state(key) or "0") + 1
        self.set_state(key, str(current))
        return current

    def reset_state_counter(self, key: str) -> None:
        self.set_state(key, "0")

    def clear_emergency_stop(self) -> None:
        self.set_state("emergency_stop", "0")
        self.set_state("emergency_reason", "")
        self.set_state("emergency_severity", "none")
        self.set_state("emergency_set_at", "")
        self.set_state("emergency_cleared_at", datetime.now(timezone.utc).isoformat())

    def set_emergency_stop(self, reason: str, severity: str = "fatal") -> None:
        self.set_state("emergency_stop", "1")
        self.set_state("emergency_reason", reason)
        self.set_state("emergency_severity", severity)
        self.set_state("emergency_set_at", datetime.now(timezone.utc).isoformat())
        self.log_decision(
            symbol="SYSTEM",
            mode="system",
            stage="emergency_stop",
            outcome="triggered",
            detail=reason,
            payload={"severity": severity},
        )

    def is_emergency_stop(self) -> tuple[bool, str]:
        active = self.get_state("emergency_stop") == "1"
        reason = self.get_state("emergency_reason") or ""
        return active, reason

    def create_backtest_run(
        self,
        *,
        run_tag: str,
        started_at: str,
        ended_at: str,
        config_json: dict,
        symbols_json: list[str],
        metrics_json: dict,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_run (
                    run_tag, started_at, ended_at, config_json, symbols_json, metrics_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_tag,
                    started_at,
                    ended_at,
                    json.dumps(_json_safe(config_json), ensure_ascii=False),
                    json.dumps(_json_safe(symbols_json), ensure_ascii=False),
                    json.dumps(_json_safe(metrics_json), ensure_ascii=False),
                ),
            )
        return int(cursor.lastrowid)

    def insert_backtest_trades(self, run_id: int, trades: list[dict]) -> None:
        if not trades:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO backtest_trade (
                    run_id, symbol, side, entry_time, exit_time, entry_price, exit_price,
                    qty, fee, funding, slippage_bps, pnl, mae, mfe, reason_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        str(item["symbol"]),
                        str(item["side"]),
                        str(item["entry_time"]),
                        str(item["exit_time"]),
                        float(item["entry_price"]),
                        float(item["exit_price"]),
                        float(item["qty"]),
                        float(item["fee"]),
                        float(item["funding"]),
                        float(item["slippage_bps"]),
                        float(item["pnl"]),
                        float(item["mae"]),
                        float(item["mfe"]),
                        json.dumps(_json_safe(item["reason_json"]), ensure_ascii=False),
                    )
                    for item in trades
                ],
            )

    def upsert_order_lifecycle(self, lifecycle: dict) -> int:
        exchange_order_id = str(lifecycle.get("exchange_order_id") or "")
        client_order_id = str(lifecycle.get("client_order_id") or "")
        with self._connect() as conn:
            existing = None
            if exchange_order_id:
                existing = conn.execute(
                    "SELECT id FROM order_lifecycle WHERE exchange_order_id = ? ORDER BY id DESC LIMIT 1",
                    (exchange_order_id,),
                ).fetchone()
            if existing is None and client_order_id:
                existing = conn.execute(
                    "SELECT id FROM order_lifecycle WHERE client_order_id = ? ORDER BY id DESC LIMIT 1",
                    (client_order_id,),
                ).fetchone()
            payload = (
                client_order_id,
                exchange_order_id,
                str(lifecycle.get("symbol") or ""),
                str(lifecycle.get("order_type") or ""),
                str(lifecycle.get("side") or ""),
                str(lifecycle.get("status") or ""),
                float(lifecycle.get("requested_qty") or 0.0),
                float(lifecycle.get("filled_qty") or 0.0),
                float(lifecycle.get("avg_price") or 0.0),
                int(bool(lifecycle.get("is_algo"))),
                int(bool(lifecycle.get("is_reduce_only"))),
                json.dumps(_json_safe(lifecycle.get("raw_json") or {}), ensure_ascii=False),
                str(lifecycle.get("created_at") or datetime.now(timezone.utc).isoformat()),
                str(lifecycle.get("updated_at") or datetime.now(timezone.utc).isoformat()),
            )
            if existing is None:
                cursor = conn.execute(
                    """
                    INSERT INTO order_lifecycle (
                        client_order_id, exchange_order_id, symbol, order_type, side, status,
                        requested_qty, filled_qty, avg_price, is_algo, is_reduce_only, raw_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                return int(cursor.lastrowid)
            conn.execute(
                """
                UPDATE order_lifecycle
                SET client_order_id = ?, exchange_order_id = ?, symbol = ?, order_type = ?, side = ?, status = ?,
                    requested_qty = ?, filled_qty = ?, avg_price = ?, is_algo = ?, is_reduce_only = ?,
                    raw_json = ?, created_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (*payload, int(existing["id"])),
            )
            return int(existing["id"])

    def get_recent_order_lifecycle(self, limit: int = 20) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM order_lifecycle ORDER BY updated_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def upsert_macro_events(self, events: list[dict]) -> int:
        inserted = 0
        if not events:
            return inserted
        with self._connect() as conn:
            for event in events:
                cursor = conn.execute(
                    """
                    INSERT INTO macro_event (
                        event_key, title, country, importance, scheduled_at, source, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_key) DO UPDATE SET
                        title = excluded.title,
                        country = excluded.country,
                        importance = excluded.importance,
                        scheduled_at = excluded.scheduled_at,
                        source = excluded.source,
                        raw_json = excluded.raw_json
                    """,
                    (
                        str(event.get("event_key") or ""),
                        str(event.get("title") or ""),
                        str(event.get("country") or ""),
                        str(event.get("importance") or ""),
                        str(event.get("scheduled_at") or ""),
                        str(event.get("source") or ""),
                        json.dumps(_json_safe(event.get("raw_json") or {}), ensure_ascii=False),
                    ),
                )
                inserted += int(cursor.rowcount > 0)
        return inserted

    def get_upcoming_macro_events(self, hours: int = 48, limit: int = 20) -> list[sqlite3.Row]:
        anchor = datetime.now(timezone.utc)
        horizon = anchor + timedelta(hours=max(hours, 1))
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM macro_event
                WHERE scheduled_at >= ? AND scheduled_at <= ?
                ORDER BY scheduled_at ASC, id ASC
                LIMIT ?
                """,
                (anchor.isoformat(), horizon.isoformat(), limit),
            ).fetchall()
