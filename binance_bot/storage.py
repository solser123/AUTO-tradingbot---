from __future__ import annotations

import sqlite3
from datetime import datetime

from .models import Position, TradeSignal


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
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(positions)").fetchall()
            }
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
                    datetime.utcnow().isoformat(),
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
                "SELECT side, quantity, entry_price FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()

        if row is None:
            return

        quantity = float(row["quantity"])
        entry_price = float(row["entry_price"])
        side = row["side"]
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
                (exit_price, datetime.utcnow().isoformat(), exit_reason, realized_pnl, position_id),
            )

    def get_today_realized_pnl(self, mode: str | None = None) -> float:
        today = datetime.utcnow().date().isoformat()
        query = """
                SELECT COALESCE(SUM(realized_pnl), 0) AS pnl
                FROM positions
                WHERE status = 'CLOSED'
                  AND closed_at IS NOT NULL
                  AND substr(closed_at, 1, 10) = ?
                """
        params: list[object] = [today]
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
        win_rate = (wins / closed_positions) * 100 if closed_positions else 0.0
        return {
            "total_signals": total_signals,
            "approved_signals": approved_signals,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "realized_pnl": realized_pnl,
            "win_rate": win_rate,
        }
