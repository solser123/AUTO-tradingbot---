from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from .ai_validator import AIValidator
from .config import BotConfig
from .exchange import BinanceExchange
from .models import Position
from .notifier import TelegramNotifier
from .risk import RiskManager
from .storage import StateStore
from .strategy import scan_market, should_exit


class TradingEngine:
    def __init__(
        self,
        config: BotConfig,
        exchange: BinanceExchange,
        store: StateStore,
        notifier: TelegramNotifier,
        ai_validator: AIValidator,
        risk_manager: RiskManager,
    ) -> None:
        self.config = config
        self.exchange = exchange
        self.store = store
        self.notifier = notifier
        self.ai_validator = ai_validator
        self.risk_manager = risk_manager
        self._scan_symbols_cache: list[str] | None = None

    def _scan_symbols(self) -> list[str]:
        if self._scan_symbols_cache is None:
            self._scan_symbols_cache = self.exchange.resolve_symbols(self.config.symbols)
        return self._scan_symbols_cache

    def run_forever(self) -> None:
        symbols = self._scan_symbols()
        preview = ", ".join(symbols[:5])
        if len(symbols) > 5:
            preview = f"{preview} ... (+{len(symbols) - 5} more)"
        logging.info("Starting bot loop in %s mode", self.config.mode)
        self.notifier.send(
            f"[BOT START] mode={self.config.mode} "
            f"market={'USDT-M futures' if self.config.is_futures else 'spot'} "
            f"symbols={preview}"
        )
        while True:
            self.run_once()
            time.sleep(self.config.loop_seconds)

    def run_once(self) -> None:
        for symbol in self._scan_symbols():
            try:
                self._process_symbol(symbol)
            except Exception as exc:
                logging.exception("Symbol loop failed for %s: %s", symbol, exc)
                self.notifier.send(f"[{symbol}] loop failed: {exc}")

    def _process_symbol(self, symbol: str) -> None:
        position = self.store.get_open_position(symbol)
        if position is not None:
            self._manage_position(position)
            return

        execution_df = self.exchange.fetch_ohlcv(symbol, self.config.timeframe)
        higher_df = self.exchange.fetch_ohlcv(symbol, self.config.higher_timeframe)
        scan = scan_market(symbol, execution_df, higher_df, self.config)
        signal = scan.signal
        if signal is None:
            logging.info("%s: no rule-based setup. %s", symbol, " | ".join(scan.reasons[:3]))
            return

        review = self.ai_validator.review(signal)
        self.store.log_signal(signal, review.approved, review.confidence, review.reason)
        if not review.approved:
            logging.info("%s: AI rejected signal. %s", symbol, review.reason)
            return

        decision = self.risk_manager.can_open_trade(signal, review)
        if not decision.allowed:
            logging.info("%s: risk manager rejected signal. %s", symbol, decision.reason)
            return

        quantity = self.exchange.amount_to_precision(symbol, self.config.notional_per_trade / signal.entry_price)
        if quantity <= 0:
            logging.info("%s: quantity below exchange minimum.", symbol)
            return

        position = Position(
            symbol=symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=signal.entry_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            opened_at=datetime.now(timezone.utc),
            mode=self.config.mode,
        )

        if self.config.mode == "live":
            live_side = "buy" if signal.side == "long" else "sell"
            self.exchange.create_market_order(symbol, live_side, quantity)

        self.store.open_position(position)
        logging.info("%s: opened %s position at %.4f", symbol, signal.side, signal.entry_price)
        self.notifier.send(
            f"[OPEN] {symbol} {signal.side} entry={signal.entry_price:.4f} "
            f"stop={signal.stop_price:.4f} target={signal.target_price:.4f} ai={review.confidence:.2f}"
        )

    def _manage_position(self, position: Position) -> None:
        current_price = self.exchange.fetch_last_price(position.symbol)
        exit_reason = should_exit(position, current_price, self.config.max_hold_minutes)
        if exit_reason is None:
            logging.info("%s: position open, no exit. price=%.4f", position.symbol, current_price)
            return

        if self.config.mode == "live":
            live_side = "sell" if position.side == "long" else "buy"
            self.exchange.create_market_order(position.symbol, live_side, position.quantity, reduce_only=self.config.is_futures)

        self.store.close_position(position.id or 0, current_price, exit_reason)
        logging.info("%s: closed position at %.4f (%s)", position.symbol, current_price, exit_reason)
        self.notifier.send(
            f"[CLOSE] {position.symbol} {position.side} exit={current_price:.4f} reason={exit_reason}"
        )
