from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .backtest import run_backtest_for_symbol
from .config import BotConfig
from .exchange import BinanceExchange


def _safe_symbol(symbol: str) -> str:
    try:
        cleaned = symbol.strip()
        cleaned.encode("ascii")
        if not cleaned or cleaned.startswith("/") or cleaned.startswith(":"):
            return ""
        return cleaned
    except UnicodeEncodeError:
        normalized = symbol.encode("ascii", "ignore").decode("ascii").strip()
        if not normalized or normalized.startswith("/") or normalized.startswith(":"):
            return ""
        return normalized


def run_universe_backtest(
    config: BotConfig,
    exchange: BinanceExchange,
    output_dir: Path,
) -> tuple[Path, Path, dict[str, float | int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"universe_backtest_{timestamp}.csv"
    report_path = output_dir / f"universe_backtest_{timestamp}.md"

    symbols = exchange.resolve_symbols(["ALL"]) if config.is_futures else exchange.resolve_symbols(config.symbols)
    rows: list[dict[str, float | int | str]] = []
    aggregate_trades = 0
    aggregate_wins = 0
    aggregate_pnl = 0.0

    for symbol in symbols:
        try:
            result = run_backtest_for_symbol(symbol, exchange, config)
        except Exception:
            continue
        row = asdict(result)
        rows.append(row)
        aggregate_trades += int(result.trades)
        aggregate_wins += int(result.wins)
        aggregate_pnl += float(result.realized_pnl)

    rows.sort(key=lambda item: (float(item["realized_pnl"]), float(item["win_rate"])), reverse=True)

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "trades", "wins", "losses", "win_rate", "realized_pnl"],
        )
        writer.writeheader()
        writer.writerows(rows)

    symbols_with_trades = [row for row in rows if int(row["trades"]) > 0]
    aggregate_win_rate = (aggregate_wins / aggregate_trades * 100) if aggregate_trades else 0.0

    best_rows = symbols_with_trades[:15]
    worst_rows = sorted(symbols_with_trades, key=lambda item: float(item["realized_pnl"]))[:15]

    lines = [
        "# Universe Backtest Report",
        "",
        f"- generated_at: {datetime.now().isoformat()}",
        f"- symbols_scanned: {len(rows)}",
        f"- symbols_with_trades: {len(symbols_with_trades)}",
        f"- aggregate_trades: {aggregate_trades}",
        f"- aggregate_wins: {aggregate_wins}",
        f"- aggregate_win_rate: {aggregate_win_rate:.2f}",
        f"- aggregate_realized_pnl: {aggregate_pnl:.6f}",
        "",
        "## Top Symbols",
        "",
        "| symbol | trades | win_rate | realized_pnl |",
        "|---|---:|---:|---:|",
    ]
    for row in best_rows:
        lines.append(
            f"| {row['symbol']} | {row['trades']} | {float(row['win_rate']):.2f} | {float(row['realized_pnl']):.6f} |"
        )

    lines.extend(
        [
            "",
            "## Worst Symbols",
            "",
            "| symbol | trades | win_rate | realized_pnl |",
            "|---|---:|---:|---:|",
        ]
    )
    for row in worst_rows:
        lines.append(
            f"| {row['symbol']} | {row['trades']} | {float(row['win_rate']):.2f} | {float(row['realized_pnl']):.6f} |"
        )

    report_path.write_text("\n".join(lines), encoding="utf-8")

    summary = {
        "symbols_scanned": len(rows),
        "symbols_with_trades": len(symbols_with_trades),
        "aggregate_trades": aggregate_trades,
        "aggregate_wins": aggregate_wins,
        "aggregate_win_rate": round(aggregate_win_rate, 2),
        "aggregate_realized_pnl": round(aggregate_pnl, 6),
    }
    return csv_path, report_path, summary


def latest_universe_candidates(log_dir: Path, limit: int = 15, min_trades: int = 2) -> list[str]:
    latest = sorted(log_dir.glob("universe_backtest_*.csv"), key=lambda item: item.stat().st_mtime, reverse=True)
    if not latest:
        return []
    path = latest[0]
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                trades = int(float(row.get("trades", 0) or 0))
                pnl = float(row.get("realized_pnl", 0) or 0.0)
                win_rate = float(row.get("win_rate", 0) or 0.0)
            except Exception:
                continue
            if trades < min_trades:
                continue
            if pnl <= 0:
                continue
            rows.append(
                {
                    "symbol": row.get("symbol", ""),
                    "trades": str(trades),
                    "realized_pnl": str(pnl),
                    "win_rate": str(win_rate),
                }
            )
    rows.sort(
        key=lambda item: (
            -float(item["realized_pnl"]),
            -float(item["win_rate"]),
            -int(item["trades"]),
        )
    )
    return [_safe_symbol(row["symbol"]) for row in rows[:limit] if _safe_symbol(row["symbol"])]


def recent_listing_candidates(
    exchange: BinanceExchange,
    limit: int = 12,
    lookback_days: int = 180,
) -> list[str]:
    if not exchange.config.is_futures:
        return []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp() * 1000)
    candidates: list[tuple[str, int]] = []
    for symbol, market in exchange.client.markets.items():
        if not market.get("swap") or market.get("quote") != "USDT":
            continue
        info = market.get("info", {}) or {}
        onboard_raw = info.get("onboardDate") or info.get("listingDate") or info.get("launchTime")
        try:
            onboard_ms = int(onboard_raw)
        except Exception:
            continue
        if onboard_ms < cutoff_ms or onboard_ms > now_ms:
            continue
        candidates.append((symbol, onboard_ms))
    candidates.sort(key=lambda item: item[1], reverse=True)
    return [_safe_symbol(symbol) for symbol, _ in candidates[:limit] if _safe_symbol(symbol)]
