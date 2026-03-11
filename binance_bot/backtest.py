from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone

from .config import BotConfig
from .exchange import BinanceExchange
from .models import Position
from .strategy import scan_market, should_exit


@dataclass(frozen=True)
class BacktestResult:
    symbol: str
    trades: int
    wins: int
    losses: int
    win_rate: float
    realized_pnl: float
def run_backtest_for_symbol(
    symbol: str,
    exchange: BinanceExchange,
    config: BotConfig,
) -> BacktestResult:
    execution_df = exchange.fetch_ohlcv(symbol, config.timeframe, limit=max(config.backtest_limit, 120))
    higher_df = exchange.fetch_ohlcv(symbol, config.higher_timeframe, limit=max(config.backtest_limit, 120))

    execution_df = execution_df.reset_index(drop=True)
    higher_df = higher_df.reset_index(drop=True)

    trades = 0
    wins = 0
    losses = 0
    realized_pnl = 0.0
    open_position: Position | None = None

    for idx in range(60, len(execution_df)):
        current_slice = execution_df.iloc[: idx + 1].copy()
        current_time = current_slice.iloc[-1]["timestamp"]
        higher_slice = higher_df[higher_df["timestamp"] <= current_time].copy()
        if len(higher_slice) < 60:
            continue

        close_price = float(current_slice.iloc[-1]["close"])

        if open_position is not None:
            candle_time = current_time.to_pydatetime() if hasattr(current_time, "to_pydatetime") else current_time
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            exit_reason = should_exit(open_position, close_price, config.max_hold_minutes, candle_time)
            if exit_reason is not None:
                if open_position.side == "long":
                    pnl = (close_price - open_position.entry_price) * open_position.quantity
                else:
                    pnl = (open_position.entry_price - close_price) * open_position.quantity
                realized_pnl += pnl
                trades += 1
                if pnl > 0:
                    wins += 1
                else:
                    losses += 1
                open_position = None
            continue

        scan = scan_market(symbol, current_slice, higher_slice, config)
        signal = scan.signal
        if signal is None:
            continue

        quantity = config.notional_per_trade / signal.entry_price
        open_position = Position(
            symbol=symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=signal.entry_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            opened_at=(current_time.to_pydatetime() if hasattr(current_time, "to_pydatetime") else current_time),
            mode="backtest",
        )
        if open_position.opened_at.tzinfo is None:
            open_position.opened_at = open_position.opened_at.replace(tzinfo=timezone.utc)

    if open_position is not None:
        final_close = float(execution_df.iloc[-1]["close"])
        if open_position.side == "long":
            pnl = (final_close - open_position.entry_price) * open_position.quantity
        else:
            pnl = (open_position.entry_price - final_close) * open_position.quantity
        realized_pnl += pnl
        trades += 1
        if pnl > 0:
            wins += 1
        else:
            losses += 1

    win_rate = (wins / trades) * 100 if trades else 0.0
    return BacktestResult(
        symbol=symbol,
        trades=trades,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        realized_pnl=realized_pnl,
    )
