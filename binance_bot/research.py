from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from .backtest import run_backtest_for_symbol
from .config import BotConfig
from .exchange import BinanceExchange


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
