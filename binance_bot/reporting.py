from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from .backtest_engine import BacktestMetrics, BacktestResult, BacktestTrade


@dataclass(frozen=True)
class AggregateBacktestReport:
    metrics: BacktestMetrics
    started_at: datetime | None
    ended_at: datetime | None
    by_symbol: dict[str, dict[str, float]]
    by_sector: dict[str, dict[str, float]]
    by_hour: dict[int, dict[str, float]]
    by_side: dict[str, dict[str, float]]


def _build_metrics_from_trades(
    trades: list[BacktestTrade],
    equity_curve: list[tuple[datetime, float]],
    initial_equity: float,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> BacktestMetrics:
    from .backtest_engine import _build_metrics  # local import to avoid circular import exposure

    return _build_metrics(trades, equity_curve, initial_equity, started_at, ended_at)


def _bucket_stats(trades: list[BacktestTrade]) -> dict[str, float]:
    pnls = [trade.net_pnl for trade in trades]
    wins = [pnl for pnl in pnls if pnl > 0]
    trade_count = len(trades)
    win_rate = (len(wins) / trade_count) * 100 if trade_count else 0.0
    return {
        "trades": float(trade_count),
        "win_rate": win_rate,
        "realized_pnl": sum(pnls),
        "avg_pnl": (sum(pnls) / trade_count) if trade_count else 0.0,
    }


def summarize_backtest_results(results: list[BacktestResult], initial_equity: float) -> AggregateBacktestReport:
    all_trades: list[BacktestTrade] = []
    all_equity_points: list[tuple[datetime, float]] = []
    by_symbol_source: dict[str, list[BacktestTrade]] = defaultdict(list)
    by_sector_source: dict[str, list[BacktestTrade]] = defaultdict(list)
    by_hour_source: dict[int, list[BacktestTrade]] = defaultdict(list)
    by_side_source: dict[str, list[BacktestTrade]] = defaultdict(list)
    started_at: datetime | None = None
    ended_at: datetime | None = None

    cumulative_equity = initial_equity
    for result in results:
        if result.started_at and (started_at is None or result.started_at < started_at):
            started_at = result.started_at
        if result.ended_at and (ended_at is None or result.ended_at > ended_at):
            ended_at = result.ended_at
        for trade in result.trades_data:
            all_trades.append(trade)
            by_symbol_source[trade.symbol].append(trade)
            by_sector_source[trade.sector].append(trade)
            by_hour_source[trade.entry_time.hour].append(trade)
            by_side_source[trade.side].append(trade)
            cumulative_equity += trade.net_pnl
            all_equity_points.append((trade.exit_time, cumulative_equity))

    metrics = _build_metrics_from_trades(all_trades, all_equity_points, initial_equity, started_at, ended_at)
    by_symbol = {symbol: _bucket_stats(trades) for symbol, trades in by_symbol_source.items()}
    by_sector = {sector: _bucket_stats(trades) for sector, trades in by_sector_source.items()}
    by_hour = {hour: _bucket_stats(trades) for hour, trades in by_hour_source.items()}
    by_side = {side: _bucket_stats(trades) for side, trades in by_side_source.items()}

    return AggregateBacktestReport(
        metrics=metrics,
        started_at=started_at,
        ended_at=ended_at,
        by_symbol=by_symbol,
        by_sector=by_sector,
        by_hour=by_hour,
        by_side=by_side,
    )


def format_report_lines(report: AggregateBacktestReport) -> list[str]:
    metrics = report.metrics
    lines = [
        "aggregate:",
        f"  trades: {metrics.trades}",
        f"  wins: {metrics.wins}",
        f"  losses: {metrics.losses}",
        f"  win_rate: {metrics.win_rate:.2f}",
        f"  realized_pnl: {metrics.realized_pnl:.4f}",
        f"  profit_factor: {metrics.profit_factor:.4f}",
        f"  expectancy: {metrics.expectancy:.4f}",
        f"  avg_win: {metrics.avg_win:.4f}",
        f"  avg_loss: {metrics.avg_loss:.4f}",
        f"  max_drawdown_abs: {metrics.max_drawdown_abs:.4f}",
        f"  max_drawdown_pct: {metrics.max_drawdown_pct * 100:.2f}",
        f"  cagr: {metrics.cagr * 100:.2f}",
        f"  sharpe: {metrics.sharpe:.4f}",
        f"  sortino: {metrics.sortino:.4f}",
        f"  avg_mae_pct: {metrics.avg_mae_pct:.2f}",
        f"  avg_mfe_pct: {metrics.avg_mfe_pct:.2f}",
    ]

    if report.started_at is not None and report.ended_at is not None:
        lines.append(f"  started_at: {report.started_at.isoformat()}")
        lines.append(f"  ended_at: {report.ended_at.isoformat()}")

    if report.by_sector:
        lines.append("by_sector:")
        for sector, stats in sorted(report.by_sector.items(), key=lambda item: item[1]["realized_pnl"], reverse=True)[:8]:
            lines.append(
                f"  {sector}: trades={int(stats['trades'])} win_rate={stats['win_rate']:.2f} pnl={stats['realized_pnl']:.4f}"
            )

    if report.by_hour:
        lines.append("by_hour:")
        for hour, stats in sorted(report.by_hour.items(), key=lambda item: item[1]["realized_pnl"], reverse=True)[:8]:
            lines.append(
                f"  {hour:02d}: trades={int(stats['trades'])} win_rate={stats['win_rate']:.2f} pnl={stats['realized_pnl']:.4f}"
            )

    if report.by_side:
        lines.append("by_side:")
        for side, stats in sorted(report.by_side.items()):
            lines.append(
                f"  {side}: trades={int(stats['trades'])} win_rate={stats['win_rate']:.2f} pnl={stats['realized_pnl']:.4f}"
            )

    return lines
