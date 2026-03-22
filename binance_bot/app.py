from __future__ import annotations

import argparse
import importlib.util
import logging
from datetime import datetime, timezone
from pathlib import Path

from .ai_validator import AIValidator
from .config import BotConfig
from .notifier import TelegramNotifier
from .risk import RiskManager
from .runtime_state import load_runtime_flags, recover_runtime_state, runtime_recovery_status
from .selector import build_exit_roadmap, default_candidate_symbols, rank_scan
from .storage import StateStore


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def build_engine():
    from .engine import TradingEngine
    from .execution import ExecutionRouter, OrderRegistry
    from .exchange import BinanceExchange
    from .macro import seed_default_macro_events

    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    store = StateStore(config.database_path)
    exchange_ok, exchange_message = exchange.validate_connection()
    recover_runtime_state(store, exchange_ok=exchange_ok, exchange_message=exchange_message)
    seed_default_macro_events(store)
    notifier = TelegramNotifier(config.telegram_token, config.telegram_chat_id)
    ai_validator = AIValidator(config)
    risk_manager = RiskManager(config, store)
    order_registry = OrderRegistry(store)
    execution_router = ExecutionRouter(exchange, registry=order_registry)
    return TradingEngine(config, exchange, store, notifier, ai_validator, risk_manager, execution_router=execution_router)


def run_preflight() -> int:
    from .exchange import BinanceExchange

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    store = StateStore(config.database_path)
    notifier = TelegramNotifier(config.telegram_token, config.telegram_chat_id)
    ai_validator = AIValidator(config)

    checks: list[tuple[str, bool, str]] = []
    exchange_ok, exchange_message = exchange.validate_connection()
    if exchange_ok:
        store.set_state("last_exchange_ok_at", datetime.now(timezone.utc).isoformat())
    else:
        store.set_state("last_exchange_error_at", datetime.now(timezone.utc).isoformat())
    checks.append(("binance", exchange_ok, exchange_message))

    if config.ai_validation:
        ai_ok, ai_message = ai_validator.healthcheck()
        checks.append(("openai", ai_ok, ai_message))

    if config.telegram_token and config.telegram_chat_id:
        telegram_ok, telegram_message = notifier.validate_chat()
        checks.append(("telegram", telegram_ok, telegram_message))

    failed = False
    for name, ok, detail in checks:
        state = "OK" if ok else "FAIL"
        print(f"{state:<4} | {name:<10} | {detail}")
        failed = failed or (not ok)

    return 1 if failed else 0


def run_doctor() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    runtime_flags = load_runtime_flags(store)
    runtime_recoverable = "unknown"
    runtime_recovery_message = "doctor skipped exchange healthcheck"
    if config.mode == "live":
        from .exchange import BinanceExchange

        exchange = BinanceExchange(config)
        exchange_ok, exchange_message = exchange.validate_connection()
        can_recover, recover_message = runtime_recovery_status(
            store,
            exchange_ok=exchange_ok,
            exchange_message=exchange_message,
        )
        runtime_recoverable = "yes" if can_recover else "no"
        runtime_recovery_message = recover_message
    checks: list[tuple[str, bool, str]] = []

    checks.append(("mode", True, f"BOT_MODE={config.mode}"))
    checks.append(("market", True, f"BOT_MARKET_TYPE={config.market_type}"))
    if config.is_futures:
        checks.append(("futures-risk", True, f"margin={config.futures_margin_mode}, leverage={config.futures_leverage}x"))
        checks.append(("core-tier", True, f"core={config.core_leverage}x liquid={config.liquid_leverage}x"))
        checks.append(("overflow-review", True, f"enabled={config.enable_overflow_review} limit={config.overflow_scan_limit}"))
        checks.append(("hot-mover-scout", True, f"enabled={config.enable_hot_mover_scout} limit={config.hot_mover_scan_limit} min24h={config.hot_mover_min_24h_pct:.1f}% lev={config.hot_mover_leverage}x"))
        checks.append(("entry-windows", True, ",".join(config.allowed_entry_windows) or "always"))
        checks.append(("cooldown", True, f"{config.symbol_cooldown_minutes}m"))
        checks.append(("loss-guard", True, f"daily={config.max_daily_loss_pct:.0%} weekly={config.max_weekly_loss_pct:.0%}"))
    checks.append(("symbols", bool(config.symbols), f"symbols={', '.join(config.symbols)}"))
    if config.main_symbols:
        checks.append(("main-symbols", True, f"main={', '.join(config.main_symbols)}"))
    if config.live_symbols():
        checks.append(("live-symbols", True, f"live={', '.join(config.live_symbols())}"))
    checks.append(("stage1", True, f"s1={','.join(config.stage1_symbols) or 'none'} notional={config.stage1_notional:.2f} ai={config.stage1_min_ai_confidence:.2f}"))
    checks.append(("stage2", True, f"s2={','.join(config.stage2_symbols) or 'none'} notional={config.stage2_notional:.2f} ai={config.stage2_min_ai_confidence:.2f}"))
    checks.append(("stage3", True, f"s3={','.join(config.stage3_symbols) or 'none'} notional={config.stage3_notional:.2f} ai={config.stage3_min_ai_confidence:.2f}"))
    checks.append(("stage4", True, f"s4={','.join(config.stage4_symbols) or 'none'} notional={config.stage4_notional:.2f} ai={config.stage4_min_ai_confidence:.2f}"))
    checks.append(("sector-flow", True, f"enabled={config.enable_sector_flow} sync={config.sector_sync_interval_minutes}m boost={config.sector_alignment_notional_boost_pct:.0%}"))
    checks.append(("micro-filter", True, f"enabled={config.enable_microstructure_filter} depth={config.microstructure_orderbook_depth} spread={config.microstructure_max_spread_pct:.3%}"))
    checks.append(("sizing", True, f"risk_full={config.sizing_risk_pct_full:.2%} total_open={config.sizing_max_total_open_risk_pct:.2%} sector_cap={config.sizing_max_same_sector_open_risk_pct:.2%}"))
    checks.append(("execution-router", True, "market-order planning with exchange rules and fill estimation"))
    checks.append(("order-lifecycle", True, "order_lifecycle table + registry enabled"))
    checks.append(("macro-layer", True, f"upcoming={len(store.get_upcoming_macro_events(hours=48, limit=10))}"))
    checks.append(("emergency-stop", True, runtime_flags.get("emergency_stop", "0") or "0"))
    checks.append(("emergency-severity", True, runtime_flags.get("emergency_severity", "") or "none"))
    checks.append(("emergency-reason", True, runtime_flags.get("emergency_reason", "") or "none"))
    checks.append(("exchange-streak", True, runtime_flags.get("exchange_failure_streak", "0") or "0"))
    checks.append(("ai-streak", True, runtime_flags.get("ai_failure_streak", "0") or "0"))
    checks.append(("last-exchange-ok", True, runtime_flags.get("last_exchange_ok_at", "") or "none"))
    checks.append(("last-order-error", True, runtime_flags.get("last_order_error_at", "") or "none"))
    checks.append(("service-pid", True, runtime_flags.get("service_pid", "") or "none"))
    checks.append(("service-started", True, runtime_flags.get("service_started_at", "") or "none"))
    checks.append(("service-stopped", True, runtime_flags.get("service_stopped_at", "") or "none"))
    checks.append(("runtime-recoverable", True, runtime_recoverable))
    checks.append(("runtime-recovery-note", True, runtime_recovery_message))
    if config.research_symbols:
        checks.append(("research-symbols", True, f"research={', '.join(config.research_symbols)}"))
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


def run_balance() -> int:
    from .exchange import BinanceExchange

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    balance = exchange.fetch_balance()
    info = balance.get("info", {}) or {}
    account_type = "usdt-m-futures" if config.is_futures else "spot"
    print(f"account_type: {account_type}")

    if config.is_futures:
        usdt_total = float(balance.get("total", {}).get("USDT", 0.0) or 0.0)
        usdt_free = float(balance.get("free", {}).get("USDT", 0.0) or 0.0)
        usdt_used = float(balance.get("used", {}).get("USDT", 0.0) or 0.0)
        print(f"usdt_wallet_balance: {usdt_total}")
        print(f"usdt_available_balance: {usdt_free}")
        print(f"usdt_used_balance: {usdt_used}")
        if info:
            print(f"total_wallet_balance: {info.get('totalWalletBalance', '0')}")
            print(f"available_balance: {info.get('availableBalance', '0')}")
            print(f"total_unrealized_profit: {info.get('totalUnrealizedProfit', '0')}")
            print(f"total_margin_balance: {info.get('totalMarginBalance', '0')}")
    else:
        nonzero_assets = []
        for asset, total in balance.get("total", {}).items():
            total_value = float(total or 0.0)
            if total_value > 0:
                free_value = float(balance.get("free", {}).get(asset, 0.0) or 0.0)
                used_value = float(balance.get("used", {}).get(asset, 0.0) or 0.0)
                nonzero_assets.append((asset, free_value, used_value, total_value))
        nonzero_assets.sort(key=lambda item: item[0])
        print(f"nonzero_assets: {len(nonzero_assets)}")
        for asset, free_value, used_value, total_value in nonzero_assets:
            print(f"{asset}: free={free_value} used={used_value} total={total_value}")

    return 0


def run_demo() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    notifier = TelegramNotifier(config.telegram_token, config.telegram_chat_id)
    today = datetime.now().date().isoformat()

    watchlist = ", ".join(config.symbols)
    message = (
        f"[DEMO START] {config.mode.upper()} {('USDT-M FUTURES' if config.is_futures else 'SPOT')}\n"
        f"date={today}\n"
        f"paper_balance={config.paper_start_balance:.2f} USDT\n"
        f"notional_per_trade={config.notional_per_trade:.2f} USDT\n"
        f"max_open_positions={config.max_open_positions}\n"
        f"watchlist={watchlist}"
    )
    notifier.send(message)
    print("demo_notification_sent: true")
    print(message)
    return 0


def run_summary() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    summary = store.get_summary()
    emergency_active, emergency_reason = store.is_emergency_stop()
    if config.mode == "paper":
        open_exposure = store.get_open_exposure(config.mode)
        paper_balance = config.paper_start_balance + float(summary["realized_pnl"]) - open_exposure
        paper_equity = config.paper_start_balance + float(summary["realized_pnl"])
        summary["paper_start_balance"] = config.paper_start_balance
        summary["open_exposure"] = open_exposure
        summary["paper_balance"] = paper_balance
        summary["paper_equity"] = paper_equity
    summary["emergency_stop"] = emergency_active
    if emergency_reason:
        summary["emergency_reason"] = emergency_reason
    recent_orders = store.get_recent_order_lifecycle(limit=3)
    summary["recent_order_events"] = len(recent_orders)
    for key, value in summary.items():
        print(f"{key}: {value}")
    if recent_orders:
        print("recent_orders:")
        for row in recent_orders:
            print(
                f"  {row['symbol']} {row['side']} status={row['status']} "
                f"filled={float(row['filled_qty'] or 0.0):.6f} avg={float(row['avg_price'] or 0.0):.6f}"
            )
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
        if scan.signal is not None:
            signal = scan.signal
            roadmap = build_exit_roadmap(
                signal.entry_price,
                signal.stop_price,
                signal.target_price,
                config.max_hold_minutes,
            )
            print(f"  side: {signal.side}")
            print(f"  setup_type: {signal.setup_type}")
            print(f"  entry_profile: {signal.entry_profile}")
            print(f"  entry_price: {signal.entry_price:.6f}")
            print(f"  stop_price: {signal.stop_price:.6f}")
            print(f"  target_price: {signal.target_price:.6f}")
            print(f"  rr: {signal.rr:.2f}")
            print(f"  exit_stop_pct: {roadmap['stop_pct']}")
            print(f"  exit_target_pct: {roadmap['target_pct']}")
            print(f"  exit_max_hold_minutes: {roadmap['max_hold_minutes']}")
        for key, value in scan.metrics.items():
            print(f"  {key}: {value}")
        for reason in scan.reasons:
            print(f"  reason: {reason}")
        print("")
    return 0


def run_rank() -> int:
    from .exchange import BinanceExchange
    from .strategy import scan_market

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    today = datetime.now().date().isoformat()

    volume_map: dict[str, float] = {}
    if config.is_futures:
        for item in exchange.client.fapiPublicGetTicker24hr():
            symbol_id = item.get("symbol", "")
            if not symbol_id.endswith("USDT"):
                continue
            futures_symbol = f"{symbol_id[:-4]}/USDT:USDT"
            volume_map[futures_symbol] = float(item.get("quoteVolume") or 0.0)

    ranked_rows: list[dict[str, object]] = []
    for symbol in default_candidate_symbols(config):
        execution_df = exchange.fetch_ohlcv(symbol, config.timeframe)
        higher_df = exchange.fetch_ohlcv(symbol, config.higher_timeframe)
        scan = scan_market(symbol, execution_df, higher_df, config)
        status, score = rank_scan(scan, volume_map.get(symbol, 0.0))
        if status == "ignore":
            continue

        row: dict[str, object] = {
            "symbol": symbol,
            "status": status,
            "score": round(score, 2),
            "quote_volume": round(volume_map.get(symbol, 0.0), 2),
            "scan": scan,
        }
        ranked_rows.append(row)

    ranked_rows.sort(key=lambda item: (item["status"] != "signal", -float(item["score"])))

    print(f"today_date: {today}")
    print(f"market_type: {'usdt-m-futures' if config.is_futures else 'spot'}")
    print("candidates:")
    for row in ranked_rows[:10]:
        scan = row["scan"]
        symbol = row["symbol"]
        status = row["status"]
        score = row["score"]
        quote_volume = row["quote_volume"]
        print(f"  {symbol} | status={status} | score={score} | quote_volume={quote_volume}")
        if scan.signal is not None:
            signal = scan.signal
            roadmap = build_exit_roadmap(
                signal.entry_price,
                signal.stop_price,
                signal.target_price,
                config.max_hold_minutes,
            )
            print(
                f"    side={signal.side} entry={signal.entry_price:.6f} "
                f"stop={signal.stop_price:.6f} target={signal.target_price:.6f} "
                f"stop_pct={roadmap['stop_pct']} target_pct={roadmap['target_pct']}"
            )
        else:
            metrics = scan.metrics
            print(
                f"    close={metrics.get('close')} rsi={metrics.get('rsi_14')} "
                f"volume_ratio={metrics.get('volume_ratio')}"
            )
            print(f"    note={' | '.join(scan.reasons[:2])}")

    recommended = [str(row["symbol"]) for row in ranked_rows[:5]]
    print("")
    print("recommended_watchlist:")
    print(",".join(recommended))
    return 0


def run_backtest() -> int:
    from .backtest.engine import BacktestEngine
    from .exchange import BinanceExchange
    from .reporting import format_report_lines, summarize_backtest_results

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    store = StateStore(config.database_path)
    engine = BacktestEngine(exchange, store=store)
    batch = engine.run(config.symbols, config, export_dir="logs")
    results = batch.results
    for result in results:
        print(f"symbol: {result.symbol}")
        print(f"  trades: {result.trades}")
        print(f"  wins: {result.wins}")
        print(f"  losses: {result.losses}")
        print(f"  win_rate: {result.win_rate:.2f}")
        print(f"  realized_pnl: {result.realized_pnl:.4f}")
        print(f"  profit_factor: {result.metrics.profit_factor:.4f}")
        print(f"  expectancy: {result.metrics.expectancy:.4f}")
        print(f"  max_drawdown_pct: {result.metrics.max_drawdown_pct * 100:.2f}")
        print("")

    report = summarize_backtest_results(results, config.paper_start_balance)
    for line in format_report_lines(report):
        print(line)
    print(f"run_id: {batch.run_id}")
    if batch.html_path:
        print(f"html_path: {batch.html_path}")
    if batch.csv_paths:
        print(f"csv_count: {len(batch.csv_paths)}")
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
        profit_factor = sum(item.metrics.profit_factor for item in results if item.trades > 0) / max(len([item for item in results if item.trades > 0]), 1)
        print(f"  aggregate_trades: {total_trades}")
        print(f"  aggregate_win_rate: {win_rate:.2f}")
        print(f"  aggregate_pnl: {total_pnl:.4f}")
        print(f"  aggregate_profit_factor: {profit_factor:.4f}")
        print(f"  score: {score:.2f}")
        print("")
    return 0


def run_universe_backtest() -> int:
    from pathlib import Path

    from .exchange import BinanceExchange
    from .research import run_universe_backtest as execute_universe_backtest

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    csv_path, report_path, summary = execute_universe_backtest(config, exchange, Path("logs"))
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(f"csv_path: {csv_path}")
    print(f"report_path: {report_path}")
    return 0


def run_stage_report() -> int:
    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    summary = store.get_summary()
    metrics = store.get_trade_metrics(config.mode)
    equity_text = store.get_state("last_known_equity") or "0"
    try:
        equity = float(equity_text)
    except ValueError:
        equity = 0.0
    drawdown_pct = (float(metrics["max_drawdown_abs"]) / equity * 100) if equity > 0 else 0.0
    slippage_events = store.count_decisions(config.mode, "emergency_stop", "triggered", "slippage")
    emergency_events = store.count_decisions("system", "emergency_stop", "triggered", None)
    stage = 1
    recommendation = "Stay on stage 1."
    if (
        int(metrics["trades"]) >= 30
        and float(metrics["profit_factor"]) >= 1.30
        and drawdown_pct <= 8.0
        and slippage_events == 0
        and emergency_events == 0
    ):
        stage = 2
        recommendation = "Eligible to review promotion to stage 2."
    if (
        int(metrics["trades"]) >= 60
        and float(metrics["profit_factor"]) >= 1.45
        and drawdown_pct <= 7.0
        and slippage_events == 0
        and emergency_events == 0
    ):
        stage = 3
        recommendation = "Eligible to review promotion to stage 3."
    if (
        int(metrics["trades"]) >= 100
        and float(metrics["profit_factor"]) >= 1.60
        and drawdown_pct <= 6.0
        and slippage_events == 0
        and emergency_events == 0
    ):
        stage = 4
        recommendation = "Eligible to review promotion to stage 4."

    print(f"current_review_stage: {stage}")
    print(f"recommendation: {recommendation}")
    print(f"mode: {config.mode}")
    print(f"equity: {equity}")
    print(f"total_signals: {summary['total_signals']}")
    print(f"approved_signals: {summary['approved_signals']}")
    print(f"trades: {metrics['trades']}")
    print(f"wins: {metrics['wins']}")
    print(f"losses: {metrics['losses']}")
    print(f"realized_pnl: {metrics['realized_pnl']:.6f}")
    print(f"profit_factor: {metrics['profit_factor']:.4f}")
    print(f"expectancy: {metrics['expectancy']:.6f}")
    print(f"max_drawdown_abs: {metrics['max_drawdown_abs']:.6f}")
    print(f"max_drawdown_pct_of_equity: {drawdown_pct:.2f}")
    print(f"slippage_events: {slippage_events}")
    print(f"emergency_events: {emergency_events}")
    print("stage_rules:")
    print(f"  symbol_stage1={','.join(config.stage1_symbols) or 'none'} notional={config.stage1_notional:.2f} ai={config.stage1_min_ai_confidence:.2f}")
    print(f"  symbol_stage2={','.join(config.stage2_symbols) or 'none'} notional={config.stage2_notional:.2f} ai={config.stage2_min_ai_confidence:.2f}")
    print(f"  symbol_stage3={','.join(config.stage3_symbols) or 'none'} notional={config.stage3_notional:.2f} ai={config.stage3_min_ai_confidence:.2f}")
    print(f"  symbol_stage4={','.join(config.stage4_symbols) or 'none'} notional={config.stage4_notional:.2f} ai={config.stage4_min_ai_confidence:.2f}")
    return 0


def run_research_snapshot() -> int:
    from .exchange import BinanceExchange
    from .research import latest_universe_candidates, recent_listing_candidates

    _configure_logging()
    config = BotConfig.from_env()
    exchange = BinanceExchange(config)
    latest_backtest = latest_universe_candidates(Path("logs"), limit=15, min_trades=2)
    recent_listings = recent_listing_candidates(exchange, limit=15, lookback_days=180)
    print("research_snapshot:")
    print(f"backtest_winners: {','.join(latest_backtest)}")
    print(f"recent_listings: {','.join(recent_listings)}")
    return 0


def run_research_news() -> int:
    from .external_sources import fetch_blockmedia_news, fetch_tradingview_ideas

    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    inserted = 0
    inserted += store.upsert_external_items(fetch_tradingview_ideas(limit=12))
    inserted += store.upsert_external_items(fetch_blockmedia_news(limit=12))
    print(f"external_inserted: {inserted}")
    for row in store.get_recent_external_items(limit=10, hours=48):
        print(
            f"{row['source']} | {row['direction']} | {row['published_at']} | "
            f"{row['title'][:90]} | {row['url']}"
        )
    return 0


def run_opportunity_report() -> int:
    from .exchange import BinanceExchange
    from .opportunity import analyze_pending_opportunities

    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    exchange = BinanceExchange(config)
    inserted = analyze_pending_opportunities(store, exchange, config, batch_limit=120)
    print(f"reviews_inserted: {inserted}")
    print("summary:")
    for key, value in store.get_opportunity_summary(hours=48).items():
        print(f"  {key}: {value}")
    print("top_material:")
    for row in store.get_opportunity_reviews(hours=48, only_material=True, limit=10):
        print(
            f"  {row['symbol']} | side={row['dominant_side']} | move={float(row['dominant_move_pct']):.2f}% | "
            f"missed_pnl={float(row['missed_notional_pnl']):.4f} | blockers={row['blockers_csv']}"
        )
    return 0


def run_macro() -> int:
    from .macro import get_upcoming_macro_events, seed_default_macro_events

    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    seeded = seed_default_macro_events(store)
    print(f"seeded: {seeded}")
    events = get_upcoming_macro_events(store, hours=168)
    for event in events[:20]:
        print(
            f"{event['importance']} | {event['scheduled_at']} | "
            f"{event['country']} | {event['title']} | {event['source']}"
        )
    return 0


def run_live_report() -> int:
    from .live_report import build_live_report, render_live_report, write_live_report

    _configure_logging()
    config = BotConfig.from_env()
    store = StateStore(config.database_path)
    report = build_live_report(store, lookback_hours=48)
    rendered = render_live_report(report)
    print(rendered)
    output_path = write_live_report(report, Path("logs"))
    print(f"\nreport_path: {output_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Bot V2 foundation")
    parser.add_argument("--once", action="store_true", help="Run one bot cycle and exit")
    parser.add_argument("--duration-minutes", type=int, default=0, help="Run the bot for a fixed number of minutes")
    parser.add_argument("--doctor", action="store_true", help="Validate config and dependency readiness")
    parser.add_argument("--preflight", action="store_true", help="Run live readiness checks against external services")
    parser.add_argument("--balance", action="store_true", help="Show exchange balance for the configured market")
    parser.add_argument("--demo", action="store_true", help="Send a Telegram demo startup message for the current configuration")
    parser.add_argument("--rank", action="store_true", help="Rank today's candidate symbols for the configured strategy")
    parser.add_argument("--summary", action="store_true", help="Print stored bot statistics")
    parser.add_argument("--scan", action="store_true", help="Scan current markets and explain signal decisions")
    parser.add_argument("--backtest", action="store_true", help="Run a simple historical strategy check")
    parser.add_argument("--optimize", action="store_true", help="Search a few strategy parameter combinations")
    parser.add_argument("--universe-backtest", action="store_true", help="Run a large backtest across the futures universe")
    parser.add_argument("--stage-report", action="store_true", help="Summarize readiness for stage-based leverage promotion")
    parser.add_argument("--research-snapshot", action="store_true", help="Show latest risky/new listing research candidates")
    parser.add_argument("--research-news", action="store_true", help="Fetch and print recent TradingView ideas and Blockmedia news")
    parser.add_argument("--opportunity-report", action="store_true", help="Backfill and print missed opportunity analysis")
    parser.add_argument("--macro", action="store_true", help="Show upcoming stored macro events")
    parser.add_argument("--live-report", action="store_true", help="Summarize recent live trading strengths and weaknesses")
    args = parser.parse_args()

    if args.doctor:
        return run_doctor()

    if args.preflight:
        return run_preflight()

    if args.balance:
        return run_balance()

    if args.demo:
        return run_demo()

    if args.summary:
        return run_summary()

    if args.rank:
        return run_rank()

    if args.scan:
        return run_scan()

    if args.backtest:
        return run_backtest()

    if args.optimize:
        return run_optimize()

    if args.universe_backtest:
        return run_universe_backtest()

    if args.stage_report:
        return run_stage_report()

    if args.research_snapshot:
        return run_research_snapshot()

    if args.research_news:
        return run_research_news()

    if args.opportunity_report:
        return run_opportunity_report()

    if args.macro:
        return run_macro()

    if args.live_report:
        return run_live_report()

    _configure_logging()
    engine = build_engine()
    if engine.config.mode == "live":
        preflight_status = run_preflight()
        if preflight_status != 0:
            logging.error("Live mode blocked because preflight checks failed.")
            return preflight_status

    if args.duration_minutes and args.duration_minutes > 0:
        engine.run_for_duration(args.duration_minutes * 60)
        return 0

    if args.once:
        engine.run_once()
        return 0

    engine.run_forever()
    return 0
