from __future__ import annotations

from dataclasses import replace

from .backtest import BacktestResult, run_backtest_for_symbol
from .config import BotConfig
from .exchange import BinanceExchange


def run_optimization(exchange: BinanceExchange, base_config: BotConfig) -> list[tuple[BotConfig, list[BacktestResult], float]]:
    candidates: list[BotConfig] = []
    for rr in [1.6, 1.8, 2.0]:
        for volume_ratio in [1.0, 1.1]:
            for max_stop in [0.02, 0.025]:
                for long_band in [(50.0, 70.0), (52.0, 68.0)]:
                    candidates.append(
                        replace(
                            base_config,
                            min_rr=rr,
                            min_volume_ratio=volume_ratio,
                            max_stop_pct=max_stop,
                            long_rsi_min=long_band[0],
                            long_rsi_max=long_band[1],
                        )
                    )

    scored: list[tuple[BotConfig, list[BacktestResult], float]] = []
    for config in candidates:
        results = [run_backtest_for_symbol(symbol, exchange, config) for symbol in config.symbols]
        total_pnl = sum(item.realized_pnl for item in results)
        total_trades = sum(item.trades for item in results)
        total_profit_factor = sum(item.metrics.profit_factor for item in results if item.trades > 0)
        total_expectancy = sum(item.metrics.expectancy for item in results if item.trades > 0)
        if total_trades == 0:
            continue
        score = total_pnl + (total_trades * 0.05) + (total_profit_factor * 0.3) + total_expectancy
        scored.append((config, results, score))

    scored.sort(key=lambda item: item[2], reverse=True)
    return scored[:5]
