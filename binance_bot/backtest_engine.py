from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math

import pandas as pd

from .config import BotConfig
from .models import Position, TradeSignal
from .sectors import sector_for_symbol
from .strategy import scan_market


BACKTEST_WARMUP_CANDLES = 60
BACKTEST_ENTRY_LATENCY_BARS = 1
BACKTEST_FEE_RATE = 0.0004
BACKTEST_SLIPPAGE_PCT = 0.0006
BACKTEST_FUNDING_RATE_PER_8H = 0.0001
BACKTEST_LIQUIDITY_FILL_SHARE = 0.02


@dataclass(frozen=True)
class BacktestTrade:
    symbol: str
    sector: str
    side: str
    setup_type: str
    entry_profile: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    quantity: float
    notional: float
    gross_pnl: float
    net_pnl: float
    fees: float
    funding_fee: float
    slippage_cost: float
    mae_pct: float
    mfe_pct: float
    bars_held: int
    exit_reason: str


@dataclass(frozen=True)
class BacktestMetrics:
    trades: int
    wins: int
    losses: int
    win_rate: float
    realized_pnl: float
    gross_profit: float
    gross_loss: float
    profit_factor: float
    expectancy: float
    avg_win: float
    avg_loss: float
    max_drawdown_abs: float
    max_drawdown_pct: float
    cagr: float
    sharpe: float
    sortino: float
    avg_mae_pct: float
    avg_mfe_pct: float


@dataclass(frozen=True)
class BacktestResult:
    symbol: str
    trades_data: list[BacktestTrade]
    equity_curve: list[tuple[datetime, float]]
    metrics: BacktestMetrics
    started_at: datetime | None
    ended_at: datetime | None

    @property
    def trades(self) -> int:
        return self.metrics.trades

    @property
    def wins(self) -> int:
        return self.metrics.wins

    @property
    def losses(self) -> int:
        return self.metrics.losses

    @property
    def win_rate(self) -> float:
        return self.metrics.win_rate

    @property
    def realized_pnl(self) -> float:
        return self.metrics.realized_pnl


@dataclass
class _OpenBacktestPosition:
    position: Position
    setup_type: str
    sector: str
    entry_time: datetime
    entry_index: int
    initial_quantity: float
    initial_notional: float
    entry_fee: float
    entry_slippage_cost: float
    partial_realized_pnl: float = 0.0
    partial_fees: float = 0.0
    partial_slippage_cost: float = 0.0
    funding_fee: float = 0.0
    mfe_pct: float = 0.0
    mae_pct: float = 0.0


def _safe_datetime(value) -> datetime:
    if hasattr(value, "to_pydatetime"):
        parsed = value.to_pydatetime()
    else:
        parsed = value
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _apply_slippage(side: str, raw_price: float) -> float:
    if side == "long":
        return raw_price * (1.0 + BACKTEST_SLIPPAGE_PCT)
    return raw_price * (1.0 - BACKTEST_SLIPPAGE_PCT)


def _close_slippage(side: str, raw_price: float) -> float:
    if side == "long":
        return raw_price * (1.0 - BACKTEST_SLIPPAGE_PCT)
    return raw_price * (1.0 + BACKTEST_SLIPPAGE_PCT)


def _fill_ratio(bar: pd.Series, desired_notional: float) -> float:
    close_price = float(bar["close"] or 0.0)
    volume = float(bar["volume"] or 0.0)
    available_notional = close_price * volume * BACKTEST_LIQUIDITY_FILL_SHARE
    if desired_notional <= 0 or available_notional <= 0:
        return 0.0
    return max(0.0, min(1.0, available_notional / desired_notional))


def _trade_pnl(side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    if side == "long":
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity


def _trade_returns(equity_curve: list[tuple[datetime, float]]) -> list[float]:
    returns: list[float] = []
    previous = None
    for _, equity in equity_curve:
        if previous is not None and previous > 0:
            returns.append((equity - previous) / previous)
        previous = equity
    return returns


def _annualize_ratio(values: list[float], downside_only: bool = False) -> float:
    if len(values) < 2:
        return 0.0
    target = [value for value in values if value < 0] if downside_only else values
    if len(target) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((item - mean) ** 2 for item in target) / max(len(target) - 1, 1)
    std = math.sqrt(variance)
    if std <= 0:
        return 0.0
    return (mean / std) * math.sqrt(len(values))


def _build_metrics(
    trades: list[BacktestTrade],
    equity_curve: list[tuple[datetime, float]],
    initial_equity: float,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> BacktestMetrics:
    pnls = [trade.net_pnl for trade in trades]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    trade_count = len(trades)
    win_count = len(wins)
    loss_count = len([pnl for pnl in pnls if pnl < 0])
    win_rate = (win_count / trade_count) * 100 if trade_count else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    expectancy = sum(pnls) / trade_count if trade_count else 0.0
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0

    peak = initial_equity
    max_drawdown_abs = 0.0
    max_drawdown_pct = 0.0
    for _, equity in equity_curve:
        peak = max(peak, equity)
        drawdown_abs = peak - equity
        drawdown_pct = (drawdown_abs / peak) if peak > 0 else 0.0
        max_drawdown_abs = max(max_drawdown_abs, drawdown_abs)
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)

    cagr = 0.0
    if started_at is not None and ended_at is not None and initial_equity > 0 and equity_curve:
        ending_equity = equity_curve[-1][1]
        total_days = max((ended_at - started_at).total_seconds() / 86400.0, 1e-9)
        years = total_days / 365.25
        if years > 0 and ending_equity > 0:
            cagr = (ending_equity / initial_equity) ** (1 / years) - 1

    returns = _trade_returns(equity_curve)
    sharpe = _annualize_ratio(returns, downside_only=False)
    sortino = _annualize_ratio(returns, downside_only=True)
    avg_mae = sum(trade.mae_pct for trade in trades) / trade_count if trade_count else 0.0
    avg_mfe = sum(trade.mfe_pct for trade in trades) / trade_count if trade_count else 0.0

    return BacktestMetrics(
        trades=trade_count,
        wins=win_count,
        losses=loss_count,
        win_rate=win_rate,
        realized_pnl=sum(pnls),
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        expectancy=expectancy,
        avg_win=avg_win,
        avg_loss=avg_loss,
        max_drawdown_abs=max_drawdown_abs,
        max_drawdown_pct=max_drawdown_pct,
        cagr=cagr,
        sharpe=sharpe,
        sortino=sortino,
        avg_mae_pct=avg_mae,
        avg_mfe_pct=avg_mfe,
    )


def _intrabar_exit(open_position: _OpenBacktestPosition, bar: pd.Series) -> tuple[str, float] | None:
    high_price = float(bar["high"])
    low_price = float(bar["low"])
    close_price = float(bar["close"])
    position = open_position.position
    if position.side == "long":
        if low_price <= position.stop_price:
            return "stop_loss", position.stop_price
        if high_price >= position.target_price:
            return "take_profit", position.target_price
        if position.profile_stage == "aggressive" and low_price <= position.half_defense_trigger:
            return "rebalance_to_balanced", position.half_defense_trigger
        if position.profile_stage in {"aggressive", "balanced"} and low_price <= position.full_defense_trigger:
            return "rebalance_to_conservative", position.full_defense_trigger
    else:
        if high_price >= position.stop_price:
            return "stop_loss", position.stop_price
        if low_price <= position.target_price:
            return "take_profit", position.target_price
        if position.profile_stage == "aggressive" and high_price >= position.half_defense_trigger:
            return "rebalance_to_balanced", position.half_defense_trigger
        if position.profile_stage in {"aggressive", "balanced"} and high_price >= position.full_defense_trigger:
            return "rebalance_to_conservative", position.full_defense_trigger

    return None


def _rebalance_position(open_position: _OpenBacktestPosition, trigger_price: float, next_stage: str) -> None:
    stage_fraction = {"aggressive": 1.0, "balanced": 0.5, "conservative": 0.25}
    current_fraction = stage_fraction.get(open_position.position.profile_stage, 0.25)
    target_fraction = stage_fraction.get(next_stage, 0.25)
    if target_fraction >= current_fraction:
        return
    reduction_ratio = 1.0 - (target_fraction / current_fraction)
    reduce_qty = round(open_position.position.quantity * reduction_ratio, 12)
    if reduce_qty <= 0:
        return
    fill_price = _close_slippage(open_position.position.side, trigger_price)
    gross_pnl = _trade_pnl(open_position.position.side, open_position.position.entry_price, fill_price, reduce_qty)
    fee = (fill_price * reduce_qty) * BACKTEST_FEE_RATE
    slippage_cost = abs(fill_price - trigger_price) * reduce_qty
    open_position.partial_realized_pnl += gross_pnl - fee
    open_position.partial_fees += fee
    open_position.partial_slippage_cost += slippage_cost
    open_position.position.quantity = max(open_position.position.quantity - reduce_qty, 0.0)
    open_position.position.profile_stage = next_stage


def _update_excursions(open_position: _OpenBacktestPosition, bar: pd.Series) -> None:
    position = open_position.position
    high_price = float(bar["high"])
    low_price = float(bar["low"])
    if position.entry_price <= 0:
        return
    if position.side == "long":
        mfe_pct = ((high_price / position.entry_price) - 1.0) * 100.0
        mae_pct = ((low_price / position.entry_price) - 1.0) * 100.0
    else:
        mfe_pct = ((position.entry_price / low_price) - 1.0) * 100.0 if low_price > 0 else 0.0
        mae_pct = ((position.entry_price / high_price) - 1.0) * 100.0 if high_price > 0 else 0.0
        mae_pct *= -1.0
    open_position.mfe_pct = max(open_position.mfe_pct, mfe_pct)
    open_position.mae_pct = min(open_position.mae_pct, mae_pct)


def _apply_funding(open_position: _OpenBacktestPosition, current_time: datetime) -> None:
    held_hours = max((current_time - open_position.entry_time).total_seconds() / 3600.0, 0.0)
    accrued = open_position.position.entry_price * open_position.position.quantity * BACKTEST_FUNDING_RATE_PER_8H * (held_hours / 8.0)
    open_position.funding_fee = accrued


def run_backtest_for_symbol(
    symbol: str,
    exchange,
    config: BotConfig,
) -> BacktestResult:
    execution_df = exchange.fetch_ohlcv(symbol, config.timeframe, limit=max(config.backtest_limit, 240)).reset_index(drop=True)
    higher_df = exchange.fetch_ohlcv(symbol, config.higher_timeframe, limit=max(config.backtest_limit, 240)).reset_index(drop=True)

    if len(execution_df) <= BACKTEST_WARMUP_CANDLES + BACKTEST_ENTRY_LATENCY_BARS or len(higher_df) <= BACKTEST_WARMUP_CANDLES:
        metrics = _build_metrics([], [], config.paper_start_balance, None, None)
        return BacktestResult(symbol=symbol, trades_data=[], equity_curve=[], metrics=metrics, started_at=None, ended_at=None)

    started_at = _safe_datetime(execution_df.iloc[BACKTEST_WARMUP_CANDLES]["timestamp"])
    ended_at = _safe_datetime(execution_df.iloc[-1]["timestamp"])
    equity = config.paper_start_balance
    equity_curve: list[tuple[datetime, float]] = [(started_at, equity)]
    trades: list[BacktestTrade] = []
    open_position: _OpenBacktestPosition | None = None
    pending_signal: tuple[TradeSignal, int] | None = None

    for idx in range(BACKTEST_WARMUP_CANDLES, len(execution_df)):
        bar = execution_df.iloc[idx]
        bar_time = _safe_datetime(bar["timestamp"])

        if pending_signal is not None and pending_signal[1] == idx and open_position is None:
            signal = pending_signal[0]
            raw_entry = float(bar["open"])
            entry_price = _apply_slippage(signal.side, raw_entry)
            desired_notional = max(config.stage_notional(symbol), config.notional_per_trade)
            fill_ratio = _fill_ratio(bar, desired_notional)
            if fill_ratio >= 0.25:
                notional = desired_notional * fill_ratio
                quantity = notional / entry_price if entry_price > 0 else 0.0
                entry_fee = notional * BACKTEST_FEE_RATE
                open_position = _OpenBacktestPosition(
                    position=Position(
                        symbol=symbol,
                        side=signal.side,
                        quantity=quantity,
                        entry_price=entry_price,
                        stop_price=signal.stop_price,
                        target_price=signal.target_price,
                        entry_profile=signal.entry_profile,
                        profile_stage=signal.entry_profile,
                        half_defense_trigger=signal.entry_price - abs(signal.entry_price - signal.stop_price) * config.balanced_defense_r_multiple if signal.side == "long" else signal.entry_price + abs(signal.entry_price - signal.stop_price) * config.balanced_defense_r_multiple,
                        full_defense_trigger=signal.entry_price - abs(signal.entry_price - signal.stop_price) * config.conservative_defense_r_multiple if signal.side == "long" else signal.entry_price + abs(signal.entry_price - signal.stop_price) * config.conservative_defense_r_multiple,
                        opened_at=bar_time,
                        mode="backtest",
                    ),
                    setup_type=signal.setup_type,
                    sector=sector_for_symbol(symbol),
                    entry_time=bar_time,
                    entry_index=idx,
                    initial_quantity=quantity,
                    initial_notional=notional,
                    entry_fee=entry_fee,
                    entry_slippage_cost=abs(entry_price - raw_entry) * quantity,
                )
            pending_signal = None

        if open_position is not None:
            _update_excursions(open_position, bar)
            _apply_funding(open_position, bar_time)
            intrabar = _intrabar_exit(open_position, bar)
            exit_reason: str | None = None
            exit_raw_price: float | None = None
            if intrabar is not None:
                exit_reason, exit_raw_price = intrabar
                if exit_reason in {"rebalance_to_balanced", "rebalance_to_conservative"}:
                    next_stage = "balanced" if exit_reason == "rebalance_to_balanced" else "conservative"
                    _rebalance_position(open_position, exit_raw_price, next_stage)
                    if open_position.position.quantity <= 0:
                        exit_reason = "rebalance_exhausted"
                        exit_raw_price = float(bar["close"])
                    else:
                        exit_reason = None
                        exit_raw_price = None
            if exit_reason is None:
                age_minutes = (bar_time - open_position.position.opened_at).total_seconds() / 60.0
                if age_minutes >= config.max_hold_minutes:
                    exit_reason = "time_exit"
                    exit_raw_price = float(bar["close"])
            if exit_reason is not None and exit_raw_price is not None:
                fill_price = _close_slippage(open_position.position.side, exit_raw_price)
                gross_pnl = _trade_pnl(
                    open_position.position.side,
                    open_position.position.entry_price,
                    fill_price,
                    open_position.position.quantity,
                )
                exit_fee = (fill_price * open_position.position.quantity) * BACKTEST_FEE_RATE
                slippage_cost = abs(fill_price - exit_raw_price) * open_position.position.quantity
                net_pnl = (
                    open_position.partial_realized_pnl
                    + gross_pnl
                    - open_position.entry_fee
                    - open_position.partial_fees
                    - exit_fee
                    - open_position.funding_fee
                )
                equity += net_pnl
                trade = BacktestTrade(
                    symbol=symbol,
                    sector=open_position.sector,
                    side=open_position.position.side,
                    setup_type=open_position.setup_type,
                    entry_profile=open_position.position.entry_profile,
                    entry_time=open_position.entry_time,
                    exit_time=bar_time,
                    entry_price=open_position.position.entry_price,
                    exit_price=fill_price,
                    quantity=open_position.initial_quantity,
                    notional=open_position.initial_notional,
                    gross_pnl=open_position.partial_realized_pnl + gross_pnl,
                    net_pnl=net_pnl,
                    fees=open_position.entry_fee + open_position.partial_fees + exit_fee,
                    funding_fee=open_position.funding_fee,
                    slippage_cost=open_position.entry_slippage_cost + open_position.partial_slippage_cost + slippage_cost,
                    mae_pct=abs(open_position.mae_pct),
                    mfe_pct=open_position.mfe_pct,
                    bars_held=max(idx - open_position.entry_index + 1, 1),
                    exit_reason=exit_reason,
                )
                trades.append(trade)
                equity_curve.append((bar_time, equity))
                open_position = None

        if idx >= len(execution_df) - BACKTEST_ENTRY_LATENCY_BARS:
            continue
        if open_position is not None or pending_signal is not None:
            continue

        current_slice = execution_df.iloc[: idx + 1].copy()
        higher_slice = higher_df[higher_df["timestamp"] <= bar_time].copy()
        if len(higher_slice) < BACKTEST_WARMUP_CANDLES:
            continue
        scan = scan_market(symbol, current_slice, higher_slice, config)
        if scan.signal is not None:
            pending_signal = (scan.signal, idx + BACKTEST_ENTRY_LATENCY_BARS)

    if open_position is not None:
        final_bar = execution_df.iloc[-1]
        final_time = _safe_datetime(final_bar["timestamp"])
        _update_excursions(open_position, final_bar)
        _apply_funding(open_position, final_time)
        exit_raw_price = float(final_bar["close"])
        fill_price = _close_slippage(open_position.position.side, exit_raw_price)
        gross_pnl = _trade_pnl(
            open_position.position.side,
            open_position.position.entry_price,
            fill_price,
            open_position.position.quantity,
        )
        exit_fee = (fill_price * open_position.position.quantity) * BACKTEST_FEE_RATE
        slippage_cost = abs(fill_price - exit_raw_price) * open_position.position.quantity
        net_pnl = (
            open_position.partial_realized_pnl
            + gross_pnl
            - open_position.entry_fee
            - open_position.partial_fees
            - exit_fee
            - open_position.funding_fee
        )
        equity += net_pnl
        trades.append(
            BacktestTrade(
                symbol=symbol,
                sector=open_position.sector,
                side=open_position.position.side,
                setup_type=open_position.setup_type,
                entry_profile=open_position.position.entry_profile,
                entry_time=open_position.entry_time,
                exit_time=final_time,
                entry_price=open_position.position.entry_price,
                exit_price=fill_price,
                quantity=open_position.initial_quantity,
                notional=open_position.initial_notional,
                gross_pnl=open_position.partial_realized_pnl + gross_pnl,
                net_pnl=net_pnl,
                fees=open_position.entry_fee + open_position.partial_fees + exit_fee,
                funding_fee=open_position.funding_fee,
                slippage_cost=open_position.entry_slippage_cost + open_position.partial_slippage_cost + slippage_cost,
                mae_pct=abs(open_position.mae_pct),
                mfe_pct=open_position.mfe_pct,
                bars_held=max(len(execution_df) - open_position.entry_index, 1),
                exit_reason="final_close",
            )
        )
        equity_curve.append((final_time, equity))

    metrics = _build_metrics(trades, equity_curve, config.paper_start_balance, started_at, ended_at)
    return BacktestResult(
        symbol=symbol,
        trades_data=trades,
        equity_curve=equity_curve,
        metrics=metrics,
        started_at=started_at,
        ended_at=ended_at,
    )
