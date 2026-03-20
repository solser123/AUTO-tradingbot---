from __future__ import annotations

from html import escape
from pathlib import Path

from ..backtest_engine import BacktestResult
from ..reporting import format_report_lines, summarize_backtest_results


def build_backtest_report(results: list[BacktestResult], initial_equity: float) -> dict:
    report = summarize_backtest_results(results, initial_equity)
    return {
        "aggregate": report.metrics.__dict__,
        "started_at": report.started_at.isoformat() if report.started_at else None,
        "ended_at": report.ended_at.isoformat() if report.ended_at else None,
        "by_symbol": report.by_symbol,
        "by_sector": report.by_sector,
        "by_hour": report.by_hour,
        "by_side": report.by_side,
    }


def export_backtest_html(results: list[BacktestResult], out_path: str, initial_equity: float) -> str:
    report = summarize_backtest_results(results, initial_equity)
    lines = "<br/>".join(escape(line) for line in format_report_lines(report))
    html = f"<html><body><h1>Backtest Report</h1><pre>{lines}</pre></body></html>"
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return str(path)


def export_backtest_csv(results: list[BacktestResult], out_dir: str) -> list[str]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for result in results:
        file_path = output_dir / f"{result.symbol.replace('/', '_').replace(':', '_')}_trades.csv"
        rows = [
            "symbol,side,entry_time,exit_time,entry_price,exit_price,qty,fee,funding,slippage_bps,pnl,mae,mfe,exit_reason"
        ]
        for trade in result.trades_data:
            rows.append(
                f"{trade.symbol},{trade.side},{trade.entry_time.isoformat()},{trade.exit_time.isoformat()},"
                f"{trade.entry_price:.8f},{trade.exit_price:.8f},{trade.quantity:.8f},{trade.fees:.8f},"
                f"{trade.funding_fee:.8f},{(trade.slippage_cost / max(trade.notional, 1e-9)) * 10000:.4f},"
                f"{trade.net_pnl:.8f},{trade.mae_pct:.4f},{trade.mfe_pct:.4f},{trade.exit_reason}"
            )
        file_path.write_text("\n".join(rows), encoding="utf-8")
        written.append(str(file_path))
    return written
