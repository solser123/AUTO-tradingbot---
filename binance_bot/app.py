from __future__ import annotations

import argparse
import importlib.util
import logging

from .ai_validator import AIValidator
from .config import BotConfig
from .notifier import TelegramNotifier
from .risk import RiskManager
from .storage import StateStore


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def build_engine():
    from .engine import TradingEngine
    from .exchange import BinanceExchange

    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    store = StateStore(config.database_path)
    notifier = TelegramNotifier(config.telegram_token, config.telegram_chat_id)
    ai_validator = AIValidator(config)
    risk_manager = RiskManager(config, store)
    return TradingEngine(config, exchange, store, notifier, ai_validator, risk_manager)


def run_doctor() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    checks: list[tuple[str, bool, str]] = []

    checks.append(("mode", True, f"BOT_MODE={config.mode}"))
    checks.append(("symbols", bool(config.symbols), f"symbols={', '.join(config.symbols)}"))
    checks.append(("database", True, f"db={config.database_path}"))
    checks.append(("ccxt", importlib.util.find_spec("ccxt") is not None, "required exchange library"))
    checks.append(("python-dotenv", importlib.util.find_spec("dotenv") is not None, "env loader"))
    checks.append(("pandas", importlib.util.find_spec("pandas") is not None, "indicator calculations"))
    checks.append(("requests", importlib.util.find_spec("requests") is not None, "telegram notifications"))
    if config.ai_validation:
        checks.append(("openai", importlib.util.find_spec("openai") is not None, "AI validation enabled"))
        checks.append(("openai-key", bool(config.openai_api_key), "OPENAI_API_KEY required when AI validation is enabled"))
    if config.mode == "live":
        checks.append(("binance-key", bool(config.api_key), "BINANCE_API_KEY"))
        checks.append(("binance-secret", bool(config.secret_key), "BINANCE_SECRET_KEY"))

    failed = False
    for name, ok, detail in checks:
        state = "OK" if ok else "FAIL"
        print(f"{state:<4} | {name:<14} | {detail}")
        failed = failed or (not ok)

    return 1 if failed else 0


def run_summary() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    summary = store.get_summary()
    if config.mode == "paper":
        open_exposure = store.get_open_exposure()
        paper_balance = config.paper_start_balance + float(summary["realized_pnl"]) - open_exposure
        paper_equity = config.paper_start_balance + float(summary["realized_pnl"])
        summary["paper_start_balance"] = config.paper_start_balance
        summary["open_exposure"] = open_exposure
        summary["paper_balance"] = paper_balance
        summary["paper_equity"] = paper_equity
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


def run_scan() -> int:
    from .exchange import BinanceExchange
    from .strategy import scan_market

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    for symbol in config.symbols:
        execution_df = exchange.fetch_ohlcv(symbol, config.timeframe)
        higher_df = exchange.fetch_ohlcv(symbol, config.higher_timeframe)
        scan = scan_market(symbol, execution_df, higher_df, config)
        print(f"symbol: {symbol}")
        print(f"signal_found: {scan.signal is not None}")
        for key, value in scan.metrics.items():
            print(f"  {key}: {value}")
        for reason in scan.reasons:
            print(f"  reason: {reason}")
        print("")
    return 0


def run_backtest() -> int:
    from .backtest import run_backtest_for_symbol
    from .exchange import BinanceExchange

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    total_trades = 0
    total_wins = 0
    total_pnl = 0.0
    for symbol in config.symbols:
        result = run_backtest_for_symbol(symbol, exchange, config)
        total_trades += result.trades
        total_wins += result.wins
        total_pnl += result.realized_pnl
        print(f"symbol: {result.symbol}")
        print(f"  trades: {result.trades}")
        print(f"  wins: {result.wins}")
        print(f"  losses: {result.losses}")
        print(f"  win_rate: {result.win_rate:.2f}")
        print(f"  realized_pnl: {result.realized_pnl:.2f}")
        print("")

    aggregate_win_rate = (total_wins / total_trades) * 100 if total_trades else 0.0
    print("aggregate:")
    print(f"  trades: {total_trades}")
    print(f"  wins: {total_wins}")
    print(f"  win_rate: {aggregate_win_rate:.2f}")
    print(f"  realized_pnl: {total_pnl:.2f}")
    return 0


def run_optimize() -> int:
    from .exchange import BinanceExchange
    from .optimize import run_optimization

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    best = run_optimization(exchange, config)
    for index, (candidate, results, score) in enumerate(best, start=1):
        print(f"rank: {index}")
        print(
            "  params: "
            f"min_rr={candidate.min_rr}, min_volume_ratio={candidate.min_volume_ratio}, "
            f"max_stop_pct={candidate.max_stop_pct}, long_rsi=({candidate.long_rsi_min}, {candidate.long_rsi_max})"
        )
        total_trades = sum(item.trades for item in results)
        total_wins = sum(item.wins for item in results)
        total_pnl = sum(item.realized_pnl for item in results)
        win_rate = (total_wins / total_trades) * 100 if total_trades else 0.0
        print(f"  aggregate_trades: {total_trades}")
        print(f"  aggregate_win_rate: {win_rate:.2f}")
        print(f"  aggregate_pnl: {total_pnl:.2f}")
        print(f"  score: {score:.2f}")
        print("")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Bot V2 foundation")
    parser.add_argument("--once", action="store_true", help="Run one bot cycle and exit")
    parser.add_argument("--doctor", action="store_true", help="Validate config and dependency readiness")
    parser.add_argument("--summary", action="store_true", help="Print stored bot statistics")
    parser.add_argument("--scan", action="store_true", help="Scan current markets and explain signal decisions")
    parser.add_argument("--backtest", action="store_true", help="Run a simple historical strategy check")
    parser.add_argument("--optimize", action="store_true", help="Search a few strategy parameter combinations")
    args = parser.parse_args()

    if args.doctor:
        return run_doctor()

    if args.summary:
        return run_summary()

    if args.scan:
        return run_scan()

    if args.backtest:
        return run_backtest()

    if args.optimize:
        return run_optimize()

    _configure_logging()
    engine = build_engine()

    if args.once:
        engine.run_once()
        return 0

    engine.run_forever()
    return 0
