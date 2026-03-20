from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..backtest_engine import BacktestResult, run_backtest_for_symbol
from ..config import BotConfig
from ..storage import StateStore
from .report import build_backtest_report, export_backtest_csv, export_backtest_html


@dataclass(frozen=True)
class BacktestBatchResult:
    results: list[BacktestResult]
    report: dict
    run_id: int | None
    html_path: str | None
    csv_paths: list[str]


class BacktestEngine:
    def __init__(self, exchange, store: StateStore | None = None) -> None:
        self.exchange = exchange
        self.store = store

    def run(
        self,
        symbols: list[str],
        config: BotConfig,
        *,
        export_dir: str | None = None,
        run_tag: str | None = None,
    ) -> BacktestBatchResult:
        results = [run_backtest_for_symbol(symbol, self.exchange, config) for symbol in symbols]
        report = build_backtest_report(results, config.paper_start_balance)
        csv_paths: list[str] = []
        html_path: str | None = None
        if export_dir:
            csv_paths = export_backtest_csv(results, export_dir)
            html_path = export_backtest_html(results, f"{export_dir}\\backtest_report.html", config.paper_start_balance)

        run_id: int | None = None
        if self.store is not None:
            started = [item.started_at for item in results if item.started_at is not None]
            ended = [item.ended_at for item in results if item.ended_at is not None]
            run_id = self.store.create_backtest_run(
                run_tag=run_tag or datetime.utcnow().strftime("bt_%Y%m%d_%H%M%S"),
                started_at=min(started).isoformat() if started else "",
                ended_at=max(ended).isoformat() if ended else "",
                config_json={
                    "timeframe": config.timeframe,
                    "higher_timeframe": config.higher_timeframe,
                    "min_rr": config.min_rr,
                    "max_stop_pct": config.max_stop_pct,
                    "backtest_limit": config.backtest_limit,
                },
                symbols_json=symbols,
                metrics_json=report,
            )
            all_trades: list[dict] = []
            for result in results:
                for trade in result.trades_data:
                    all_trades.append(
                        {
                            "symbol": trade.symbol,
                            "side": trade.side,
                            "entry_time": trade.entry_time.isoformat(),
                            "exit_time": trade.exit_time.isoformat(),
                            "entry_price": trade.entry_price,
                            "exit_price": trade.exit_price,
                            "qty": trade.quantity,
                            "fee": trade.fees,
                            "funding": trade.funding_fee,
                            "slippage_bps": (trade.slippage_cost / max(trade.notional, 1e-9)) * 10000,
                            "pnl": trade.net_pnl,
                            "mae": trade.mae_pct,
                            "mfe": trade.mfe_pct,
                            "reason_json": {
                                "exit_reason": trade.exit_reason,
                                "setup_type": trade.setup_type,
                                "entry_profile": trade.entry_profile,
                                "sector": trade.sector,
                            },
                        }
                    )
            self.store.insert_backtest_trades(run_id, all_trades)

        return BacktestBatchResult(
            results=results,
            report=report,
            run_id=run_id,
            html_path=html_path,
            csv_paths=csv_paths,
        )
