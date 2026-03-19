from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .models import Position, TradeSignal


KST = ZoneInfo("Asia/Seoul")


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
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(positions)").fetchall()}
            if columns and "realized_pnl" not in columns:
                conn.execute("ALTER TABLE positions ADD COLUMN realized_pnl REAL")

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
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )

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
                    opened_at, mode, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.symbol,
                    position.side,
                    position.quantity,
                    position.entry_price,
                    position.stop_price,
                    position.target_price,
                    position.opened_at.isoformat(),
                    position.mode,
                    position.status,
                ),
            )
            position.id = int(cursor.lastrowid)
        return position

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

    def set_emergency_stop(self, reason: str) -> None:
        self.set_state("emergency_stop", "1")
        self.set_state("emergency_reason", reason)
        self.log_decision(
            symbol="SYSTEM",
            mode="system",
            stage="emergency_stop",
            outcome="triggered",
            detail=reason,
            payload={},
        )

    def is_emergency_stop(self) -> tuple[bool, str]:
        active = self.get_state("emergency_stop") == "1"
        reason = self.get_state("emergency_reason") or ""
        return active, reason
