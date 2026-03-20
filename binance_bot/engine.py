from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .ai_validator import AIValidator
from .config import BotConfig
from .exchange import BinanceExchange
from .models import Position
from .notifier import TelegramNotifier
from .research import latest_universe_candidates, recent_listing_candidates
from .risk import RiskManager
from .selector import build_exit_roadmap, default_candidate_symbols, rank_scan
from .storage import StateStore, trading_day_anchor, trading_week_anchor
from .strategy import scan_market, should_exit


KST = ZoneInfo("Asia/Seoul")


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
            configured_symbols = self.exchange.resolve_symbols(self.config.active_symbols())
            managed_symbols = self.store.get_open_symbols(self.config.mode)
            merged = configured_symbols[:]
            for symbol in managed_symbols:
                if symbol not in merged:
                    merged.append(symbol)
            self._scan_symbols_cache = merged
        return self._scan_symbols_cache

    def run_forever(self) -> None:
        self.store.set_state("runtime_stop_requested", "0")
        self._prime_telegram_offset()
        symbols = self._scan_symbols()
        preview = ", ".join(symbols[:5])
        if len(symbols) > 5:
            preview = f"{preview} ... (+{len(symbols) - 5} more)"
        logging.info("Starting bot loop in %s mode", self.config.mode)
        self.notifier.send(
            f"[BOT START] mode={self.config.mode} "
            f"market={'USDT-M futures' if self.config.is_futures else 'spot'} "
            f"symbols={preview}\n"
            f"cmd=/help /status /positions /pause /resume /rank /stage /research /scan BTC /summary /closeall /stopbot"
        )
        while True:
            if self._process_telegram_commands():
                break
            self.run_once()
            if self._stop_requested():
                break
            time.sleep(self.config.loop_seconds)
        logging.info("Bot loop finished.")
        self.notifier.send(f"[BOT STOP] mode={self.config.mode} reason=telegram_or_runtime_stop")

    def run_for_duration(self, duration_seconds: int) -> None:
        self.store.set_state("runtime_stop_requested", "0")
        self._prime_telegram_offset()
        end_time = time.time() + max(duration_seconds, 0)
        symbols = self._scan_symbols()
        preview = ", ".join(symbols[:5])
        if len(symbols) > 5:
            preview = f"{preview} ... (+{len(symbols) - 5} more)"
        logging.info(
            "Starting bounded bot loop in %s mode for %s seconds",
            self.config.mode,
            duration_seconds,
        )
        self.notifier.send(
            f"[BOT START] mode={self.config.mode} "
            f"market={'USDT-M futures' if self.config.is_futures else 'spot'} "
            f"duration_seconds={duration_seconds} symbols={preview}\n"
            f"cmd=/help /status /positions /pause /resume /rank /stage /research /scan BTC /summary /closeall /stopbot"
        )
        while time.time() < end_time:
            if self._process_telegram_commands():
                break
            self.run_once()
            if time.time() >= end_time:
                break
            if self._stop_requested():
                break
            time.sleep(self.config.loop_seconds)
        logging.info("Bounded bot loop finished.")
        self.notifier.send(f"[BOT STOP] mode={self.config.mode} duration_seconds={duration_seconds}")

    def run_once(self) -> None:
        reference_time = datetime.now(KST)
        account_equity = self._account_equity(reference_time)
        self._refresh_reference_equity(account_equity, reference_time)
        self._reconcile_live_positions()
        emergency_active, emergency_reason = self.store.is_emergency_stop()
        if emergency_active:
            logging.warning("Emergency stop active: %s", emergency_reason)
            return

        for symbol in self._scan_symbols():
            try:
                self._process_symbol(symbol, account_equity, reference_time)
            except Exception as exc:
                logging.exception("Symbol loop failed for %s: %s", symbol, exc)
                self.store.log_decision(
                    symbol=symbol,
                    mode=self.config.mode,
                    stage="runtime_exception",
                    outcome="error",
                    detail=str(exc),
                    payload={},
                )
                streak = self.store.increment_state_counter("exchange_failure_streak")
                if streak >= self.config.exchange_failure_limit:
                    self.store.set_emergency_stop(f"Exchange/runtime failure streak reached {streak}.")
                    self.notifier.send(f"[EMERGENCY STOP] Exchange/runtime failure streak reached {streak}.")
                self.notifier.send(f"[{symbol}] loop failed: {exc}")
        self._review_overflow_candidates(reference_time)

    def _process_symbol(self, symbol: str, account_equity: float, reference_time: datetime) -> None:
        position = self.store.get_open_position(symbol, self.config.mode)
        if position is not None:
            self._manage_position(position, reference_time)
            return

        if self._entries_paused():
            return

        emergency_active, _ = self.store.is_emergency_stop()
        if emergency_active:
            return

        execution_df = self.exchange.fetch_ohlcv(symbol, self.config.timeframe)
        higher_df = self.exchange.fetch_ohlcv(symbol, self.config.higher_timeframe)
        scan = scan_market(symbol, execution_df, higher_df, self.config)
        signal = scan.signal
        if signal is None:
            detail = " | ".join(scan.reasons[:3]) if scan.reasons else "No rule-based setup."
            logging.info("%s: no rule-based setup. %s", symbol, detail)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="scan",
                outcome="no_entry",
                detail=detail,
                payload={"metrics": scan.metrics, "reasons": scan.reasons[:8]},
            )
            return

        horizon_context = self._build_horizon_context(symbol, execution_df, higher_df, scan)
        signal.strategy_data["multi_horizon"] = horizon_context
        same_side_horizons = int(horizon_context.get("same_side_count", 0))
        opposite_horizons = int(horizon_context.get("opposite_side_count", 0))
        if opposite_horizons >= 2 and same_side_horizons == 0:
            detail = "Multi-horizon context is materially against the short-term entry."
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="horizon_gate",
                outcome="rejected",
                detail=detail,
                payload={"multi_horizon": horizon_context, "signal": signal.strategy_data},
            )
            return

        review = self.ai_validator.review(signal)
        self.store.log_signal(signal, review.approved, review.confidence, review.reason)
        if review.reason.startswith("AI validation failed"):
            streak = self.store.increment_state_counter("ai_failure_streak")
            if streak >= self.config.ai_failure_limit:
                self.store.set_emergency_stop(f"AI validation failure streak reached {streak}.")
                self.notifier.send(f"[EMERGENCY STOP] AI validation failure streak reached {streak}.")
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="ai_review",
                outcome="error",
                detail=review.reason,
                payload={"signal": signal.strategy_data, "committee": review.committee},
            )
            return
        self.store.reset_state_counter("ai_failure_streak")

        if not review.approved:
            logging.info("%s: AI rejected signal. %s", symbol, review.reason)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="ai_review",
                outcome="rejected",
                detail=review.reason,
                payload={"signal": signal.strategy_data, "confidence": review.confidence, "committee": review.committee},
            )
            return

        decision = self.risk_manager.can_open_trade(signal, review, account_equity, reference_time)
        if not decision.allowed:
            logging.info("%s: risk manager rejected signal. %s", symbol, decision.reason)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="risk_gate",
                outcome="rejected",
                detail=decision.reason,
                payload={"signal": signal.strategy_data, "confidence": review.confidence, "committee": review.committee},
            )
            return

        if self.config.mode == "live" and self.config.is_experimental_symbol(symbol) and not self.config.enable_experimental_live:
            logging.info("%s: live execution blocked for experimental x10/x20 tier.", symbol)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="tier_gate",
                outcome="blocked",
                detail="Experimental tier is blocked in live mode.",
                payload={"signal": signal.strategy_data},
            )
            return

        initial_notional = self._notional_for_profile(signal.entry_profile)
        quantity = self.exchange.amount_to_precision(symbol, initial_notional / signal.entry_price)
        if quantity <= 0:
            logging.info("%s: quantity below exchange minimum.", symbol)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="sizing",
                outcome="rejected",
                detail="Quantity below exchange minimum.",
                payload={"entry_price": signal.entry_price},
            )
            return

        entry_price = signal.entry_price
        if self.config.mode == "live":
            live_side = "buy" if signal.side == "long" else "sell"
            order = self.exchange.create_market_order(symbol, live_side, quantity)
            self.store.reset_state_counter("exchange_failure_streak")
            entry_price = self._resolved_fill_price(order, signal.entry_price)
            slippage_pct = abs(entry_price - signal.entry_price) / signal.entry_price if signal.entry_price else 0.0
            if slippage_pct > self.config.max_slippage_pct:
                self.store.set_emergency_stop(
                    f"Abnormal slippage detected on {symbol}: {slippage_pct * 100:.2f}%."
                )
                self.notifier.send(
                    f"[EMERGENCY STOP] {symbol} slippage {slippage_pct * 100:.2f}% exceeded limit."
                )

        risk_distance = abs(signal.entry_price - signal.stop_price)
        half_defense_trigger, full_defense_trigger = self._defense_triggers(
            signal.side,
            signal.entry_price,
            risk_distance,
        )
        position = Position(
            symbol=symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=entry_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            entry_profile=signal.entry_profile,
            profile_stage=signal.entry_profile,
            half_defense_trigger=half_defense_trigger,
            full_defense_trigger=full_defense_trigger,
            opened_at=datetime.now(timezone.utc),
            mode=self.config.mode,
        )

        self.store.open_position(position)
        self.store.log_decision(
            symbol=symbol,
            mode=self.config.mode,
            stage="entry",
            outcome="opened",
            detail=f"Opened {signal.side} position.",
            payload={
                "entry_price": entry_price,
                "stop_price": signal.stop_price,
                "target_price": signal.target_price,
                "quantity": quantity,
                "entry_profile": signal.entry_profile,
                "ai_confidence": review.confidence,
                "committee": review.committee,
                "signal": signal.strategy_data,
            },
        )
        logging.info("%s: opened %s position at %.4f", symbol, signal.side, entry_price)
        self.notifier.send(
            f"[OPEN] {symbol} {signal.side} entry={entry_price:.4f} "
            f"stop={signal.stop_price:.4f} target={signal.target_price:.4f} ai={review.confidence:.2f}"
        )

    def _manage_position(self, position: Position, reference_time: datetime) -> None:
        current_price = self.exchange.fetch_last_price(position.symbol)
        exit_reason = should_exit(position, current_price, self.config.max_hold_minutes, reference_time.astimezone(timezone.utc))
        if exit_reason is None:
            logging.info("%s: position open, no exit. price=%.4f", position.symbol, current_price)
            self.store.log_decision(
                symbol=position.symbol,
                mode=self.config.mode,
                stage="position_manage",
                outcome="hold",
                detail="Position remains open.",
                payload={"current_price": current_price},
            )
            return

        if exit_reason in {"rebalance_to_balanced", "rebalance_to_conservative"}:
            next_stage = "balanced" if exit_reason == "rebalance_to_balanced" else "conservative"
            self._rebalance_position(position, current_price, next_stage)
            return

        if self.config.mode == "live":
            live_side = "sell" if position.side == "long" else "buy"
            self.exchange.create_market_order(position.symbol, live_side, position.quantity, reduce_only=self.config.is_futures)
            self.store.reset_state_counter("exchange_failure_streak")

        self.store.close_position(position.id or 0, current_price, exit_reason)
        logging.info("%s: closed position at %.4f (%s)", position.symbol, current_price, exit_reason)
        self.notifier.send(
            f"[CLOSE] {position.symbol} {position.side} exit={current_price:.4f} reason={exit_reason}"
        )

        if exit_reason == "stop_loss":
            symbol_streak = self.store.get_symbol_stoploss_streak(position.symbol, self.config.mode)
            global_streak = self.store.get_global_stoploss_streak(self.config.mode)
            if symbol_streak >= self.config.same_symbol_stoploss_limit:
                self.notifier.send(f"[SYMBOL STOP] {position.symbol} stop-loss streak={symbol_streak}")
            if global_streak >= self.config.global_stoploss_limit:
                self.notifier.send(f"[REVIEW MODE] global stop-loss streak={global_streak}")

    def _notional_for_profile(self, profile: str) -> float:
        if profile == "aggressive":
            return self.config.notional_per_trade
        if profile == "balanced":
            return self.config.notional_per_trade * 0.75
        return self.config.notional_per_trade * 0.5

    def _defense_triggers(self, side: str, entry_price: float, risk_distance: float) -> tuple[float, float]:
        if side == "long":
            return (
                entry_price - (risk_distance * self.config.balanced_defense_r_multiple),
                entry_price - (risk_distance * self.config.conservative_defense_r_multiple),
            )
        return (
            entry_price + (risk_distance * self.config.balanced_defense_r_multiple),
            entry_price + (risk_distance * self.config.conservative_defense_r_multiple),
        )

    def _rebalance_position(self, position: Position, current_price: float, next_stage: str) -> None:
        if position.profile_stage == next_stage:
            return
        stage_fraction = {"aggressive": 1.0, "balanced": 0.5, "conservative": 0.25}
        current_fraction = stage_fraction.get(position.profile_stage, 0.25)
        target_fraction = stage_fraction.get(next_stage, 0.25)
        if target_fraction >= current_fraction:
            return
        reduction_ratio = 1.0 - (target_fraction / current_fraction)
        reduce_qty = round(position.quantity * reduction_ratio, 12)
        if reduce_qty <= 0:
            return

        if self.config.mode == "live":
            live_side = "sell" if position.side == "long" else "buy"
            self.exchange.create_market_order(position.symbol, live_side, reduce_qty, reduce_only=self.config.is_futures)
            self.store.reset_state_counter("exchange_failure_streak")

        remaining_qty = max(position.quantity - reduce_qty, 0.0)
        self.store.update_position_stage(position.id or 0, remaining_qty, next_stage)
        self.store.log_decision(
            symbol=position.symbol,
            mode=self.config.mode,
            stage="position_rebalance",
            outcome=next_stage,
            detail=f"Position rebalanced from {position.profile_stage} to {next_stage}.",
            payload={"current_price": current_price, "reduced_qty": reduce_qty, "remaining_qty": remaining_qty},
        )
        self.notifier.send(
            f"[REBALANCE] {position.symbol} {position.profile_stage}->{next_stage} "
            f"reduced={reduce_qty:.6f} remaining={remaining_qty:.6f}"
        )

    def _account_equity(self, reference_time: datetime) -> float:
        if self.config.mode == "paper":
            realized = float(self.store.get_summary()["realized_pnl"])
            return self.config.paper_start_balance + realized

        try:
            equity = self.exchange.fetch_account_equity()
            self.store.reset_state_counter("exchange_failure_streak")
            self.store.set_state("last_known_equity", f"{equity}")
            return equity
        except Exception as exc:
            streak = self.store.increment_state_counter("exchange_failure_streak")
            self.store.log_decision(
                symbol="SYSTEM",
                mode=self.config.mode,
                stage="balance_check",
                outcome="error",
                detail=str(exc),
                payload={"streak": streak},
            )
            if streak >= self.config.exchange_failure_limit:
                self.store.set_emergency_stop(f"Balance check failure streak reached {streak}.")
            fallback = self.store.get_state("last_known_equity")
            if fallback is not None:
                return float(fallback)
            return 0.0

    def _refresh_reference_equity(self, account_equity: float, reference_time: datetime) -> None:
        self.store.set_state("last_known_equity", f"{account_equity}")
        daily_anchor = trading_day_anchor(reference_time)
        weekly_anchor = trading_week_anchor(reference_time)
        self._ensure_reference_state(f"daily_reference:{daily_anchor.date().isoformat()}", account_equity)
        self._ensure_reference_state(f"weekly_reference:{weekly_anchor.date().isoformat()}", account_equity)

    def _reconcile_live_positions(self) -> None:
        if self.config.mode != "live" or not self.config.is_futures:
            return
        emergency_active, _ = self.store.is_emergency_stop()
        if emergency_active:
            return
        try:
            db_symbols = sorted(self.store.get_open_symbols(self.config.mode))
            exchange_symbols = self.exchange.fetch_open_position_symbols()
        except Exception as exc:
            self.store.log_decision(
                symbol="SYSTEM",
                mode=self.config.mode,
                stage="position_reconcile",
                outcome="error",
                detail=str(exc),
                payload={},
            )
            return
        if db_symbols != exchange_symbols:
            reason = (
                "Live/open position mismatch detected. "
                f"db={','.join(db_symbols) or 'none'} exchange={','.join(exchange_symbols) or 'none'}"
            )
            self.store.set_emergency_stop(reason)
            self.notifier.send(f"[EMERGENCY STOP] {reason}")

    def _entries_paused(self) -> bool:
        return self.store.get_state("entry_pause") == "1"

    def _stop_requested(self) -> bool:
        return self.store.get_state("runtime_stop_requested") == "1"

    def _process_telegram_commands(self) -> bool:
        if not self.config.telegram_token or not self.config.telegram_chat_id:
            return False

        offset_text = self.store.get_state("telegram_update_offset") or "0"
        try:
            offset = int(offset_text)
        except ValueError:
            offset = 0

        stop_requested = False
        for update in self.notifier.fetch_updates(offset=offset, timeout_seconds=0):
            update_id = int(update.get("update_id") or 0)
            self.store.set_state("telegram_update_offset", str(update_id + 1))
            message = update.get("message") or update.get("edited_message")
            if not isinstance(message, dict):
                continue
            chat = message.get("chat") or {}
            if not self._authorized_chat(chat):
                continue
            text = str(message.get("text") or "").strip()
            if not text.startswith("/"):
                continue
            response, requested_stop = self._handle_telegram_command(text)
            if response:
                self.notifier.send(response)
            stop_requested = stop_requested or requested_stop
        return stop_requested

    def _prime_telegram_offset(self) -> None:
        if not self.config.telegram_token or not self.config.telegram_chat_id:
            return
        if self.store.get_state("telegram_update_offset") is not None:
            return
        updates = self.notifier.fetch_updates(offset=None, timeout_seconds=0)
        if not updates:
            return
        last_update_id = max(int(item.get("update_id") or 0) for item in updates)
        self.store.set_state("telegram_update_offset", str(last_update_id + 1))

    def _authorized_chat(self, chat: dict) -> bool:
        configured = self.config.telegram_chat_id.strip()
        chat_id = str(chat.get("id") or "").strip()
        username = str(chat.get("username") or "").strip().lower()
        if configured == chat_id:
            return True
        if configured.startswith("@") and username and configured.lower() == f"@{username}":
            return True
        return False

    def _handle_telegram_command(self, raw_text: str) -> tuple[str, bool]:
        text = raw_text.strip()
        command_text = text.split()[0]
        command = command_text.split("@", 1)[0].lower()
        args = text.split()[1:]
        now_text = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")

        if command == "/help":
            return (
                "명령어\n"
                "/status 현재 상태\n"
                "/summary 누적 요약\n"
                "/positions 열린 포지션\n"
                "/rank 후보 상위 5개\n"
                "/stage 레버리지 단계 보고\n"
                "/research 연구 후보 스냅샷\n"
                "/scan BTC 특정 심볼 스캔\n"
                "/pause 신규 진입 정지\n"
                "/resume 신규 진입 재개\n"
                "/emergency 긴급정지\n"
                "/clearstop 긴급정지 해제\n"
                "/closeall 전체 포지션 정리\n"
                "/stopbot 프로세스 종료\n"
                f"updated={now_text}",
                False,
            )

        if command == "/ping":
            return f"pong {now_text}", False

        if command == "/status":
            return self._format_status(), False

        if command == "/summary":
            return self._format_summary(), False

        if command == "/positions":
            return self._format_positions(), False

        if command == "/rank":
            return self._format_rank(), False

        if command == "/stage":
            return self._format_stage_report(), False

        if command == "/research":
            return self._format_research_snapshot(), False

        if command == "/scan":
            if not args:
                return "사용법: /scan BTC 또는 /scan AVAX", False
            return self._format_scan(args[0]), False

        if command == "/pause":
            self.store.set_state("entry_pause", "1")
            self.store.log_decision("SYSTEM", self.config.mode, "telegram", "pause", "New entries paused by Telegram.", {})
            return f"신규 진입 정지됨 mode={self.config.mode}", False

        if command == "/resume":
            self.store.set_state("entry_pause", "0")
            self.store.log_decision("SYSTEM", self.config.mode, "telegram", "resume", "New entries resumed by Telegram.", {})
            return f"신규 진입 재개됨 mode={self.config.mode}", False

        if command == "/emergency":
            self.store.set_emergency_stop("Manual emergency stop from Telegram.")
            return "긴급정지 활성화됨. 기존 포지션 관리는 유지되고 신규 진입은 막힙니다.", False

        if command == "/clearstop":
            self.store.clear_emergency_stop()
            return "긴급정지 해제됨.", False

        if command == "/closeall":
            return self._close_all_positions(), False

        if command == "/stopbot":
            self.store.set_state("runtime_stop_requested", "1")
            return "봇 종료 요청을 받았습니다. 현재 사이클 후 종료합니다.", True

        return f"알 수 없는 명령입니다: {command}. /help 를 사용하세요.", False

    def _format_status(self) -> str:
        summary = self.store.get_summary()
        emergency_active, emergency_reason = self.store.is_emergency_stop()
        equity = self.store.get_state("last_known_equity") or "0"
        paused = "on" if self._entries_paused() else "off"
        return (
            f"status mode={self.config.mode}\n"
            f"paused={paused} emergency={emergency_active}\n"
            f"equity={equity}\n"
            f"open={summary['open_positions']} closed={summary['closed_positions']}\n"
            f"signals={summary['total_signals']} approved={summary['approved_signals']}\n"
            f"realized_pnl={summary['realized_pnl']:.4f} win_rate={summary['win_rate']:.2f}%\n"
            f"reason={emergency_reason or 'none'}"
        )

    def _format_summary(self) -> str:
        summary = self.store.get_summary()
        return (
            f"summary\n"
            f"signals={summary['total_signals']} approved={summary['approved_signals']}\n"
            f"open={summary['open_positions']} closed={summary['closed_positions']}\n"
            f"realized_pnl={summary['realized_pnl']:.4f}\n"
            f"win_rate={summary['win_rate']:.2f}%\n"
            f"decision_events={summary['decision_events']}"
        )

    def _format_positions(self) -> str:
        positions = self.store.get_open_positions(self.config.mode)
        if not positions:
            return "열린 포지션이 없습니다."

        lines = ["open positions"]
        for position in positions[:5]:
            try:
                current_price = self.exchange.fetch_last_price(position.symbol)
                if position.side == "long":
                    pnl = (current_price - position.entry_price) * position.quantity
                else:
                    pnl = (position.entry_price - current_price) * position.quantity
                lines.append(
                    f"{position.symbol} {position.side} qty={position.quantity:.6f} "
                    f"entry={position.entry_price:.4f} now={current_price:.4f} pnl={pnl:.4f} "
                    f"stage={position.profile_stage}"
                )
            except Exception as exc:
                lines.append(
                    f"{position.symbol} {position.side} qty={position.quantity:.6f} "
                    f"entry={position.entry_price:.4f} stage={position.profile_stage} err={exc}"
                )
        return "\n".join(lines)

    def _format_rank(self) -> str:
        rows = self._rank_rows()[:5]
        if not rows:
            return "후보가 없습니다."
        lines = ["top candidates"]
        for row in rows:
            scan = row["scan"]
            if scan.signal is not None:
                signal = scan.signal
                roadmap = build_exit_roadmap(signal.entry_price, signal.stop_price, signal.target_price, self.config.max_hold_minutes)
                lines.append(
                    f"{row['symbol']} signal {signal.side} {signal.entry_profile} "
                    f"entry={signal.entry_price:.4f} stop={roadmap['stop_pct']}% target={roadmap['target_pct']}%"
                )
            else:
                lines.append(
                    f"{row['symbol']} watch score={row['score']:.2f} "
                    f"rsi={scan.metrics.get('rsi_14')} vol={scan.metrics.get('volume_ratio')}"
                )
        return "\n".join(lines)

    def _format_stage_report(self) -> str:
        summary = self.store.get_summary()
        metrics = self.store.get_trade_metrics(self.config.mode)
        equity_text = self.store.get_state("last_known_equity") or "0"
        try:
            equity = float(equity_text)
        except ValueError:
            equity = 0.0
        drawdown_pct = (float(metrics["max_drawdown_abs"]) / equity * 100) if equity > 0 else 0.0
        slippage_events = self.store.count_decisions(self.config.mode, "emergency_stop", "triggered", "slippage")
        emergency_events = self.store.count_decisions("system", "emergency_stop", "triggered", None)
        stage = 1
        recommendation = "stage1 유지"
        if int(metrics["trades"]) >= 30 and float(metrics["profit_factor"]) >= 1.30 and drawdown_pct <= 8.0 and slippage_events == 0 and emergency_events == 0:
            stage = 2
            recommendation = "stage2 승격 검토 가능"
        if int(metrics["trades"]) >= 60 and float(metrics["profit_factor"]) >= 1.45 and drawdown_pct <= 7.0 and slippage_events == 0 and emergency_events == 0:
            stage = 3
            recommendation = "stage3 승격 검토 가능"
        if int(metrics["trades"]) >= 100 and float(metrics["profit_factor"]) >= 1.60 and drawdown_pct <= 6.0 and slippage_events == 0 and emergency_events == 0:
            stage = 4
            recommendation = "stage4 승격 검토 가능"
        return (
            f"stage report\n"
            f"review_stage={stage} {recommendation}\n"
            f"signals={summary['total_signals']} approved={summary['approved_signals']}\n"
            f"trades={metrics['trades']} pf={float(metrics['profit_factor']):.2f} pnl={float(metrics['realized_pnl']):.4f}\n"
            f"dd={drawdown_pct:.2f}% slippage={slippage_events} emergency={emergency_events}"
        )

    def _format_research_snapshot(self) -> str:
        backtest = latest_universe_candidates(Path("logs"), limit=8, min_trades=2)
        recent = recent_listing_candidates(self.exchange, limit=8, lookback_days=180)
        return (
            "research snapshot\n"
            f"backtest={','.join(backtest) or 'none'}\n"
            f"recent={','.join(recent) or 'none'}"
        )

    def _format_scan(self, symbol_text: str) -> str:
        target = symbol_text.strip().upper()
        if "/" not in target:
            target = f"{target}/USDT"
        resolved = self.exchange.resolve_symbols([target])[0]
        execution_df = self.exchange.fetch_ohlcv(resolved, self.config.timeframe)
        higher_df = self.exchange.fetch_ohlcv(resolved, self.config.higher_timeframe)
        scan = scan_market(resolved, execution_df, higher_df, self.config)
        horizons = self._build_horizon_context(resolved, execution_df, higher_df, scan)
        if scan.signal is None:
            return (
                f"{resolved}\nno signal\n"
                + "\n".join(scan.reasons[:5])
                + "\n"
                + f"bias short={horizons['short']['bias']} mid={horizons['medium']['bias']} long={horizons['long']['bias']}"
            )
        signal = scan.signal
        roadmap = build_exit_roadmap(signal.entry_price, signal.stop_price, signal.target_price, self.config.max_hold_minutes)
        return (
            f"{resolved}\n"
            f"signal={signal.side} profile={signal.entry_profile} setup={signal.setup_type}\n"
            f"entry={signal.entry_price:.6f} stop={signal.stop_price:.6f} target={signal.target_price:.6f}\n"
            f"stop_pct={roadmap['stop_pct']} target_pct={roadmap['target_pct']} rr={signal.rr:.2f}\n"
            f"bias short={horizons['short']['bias']} mid={horizons['medium']['bias']} long={horizons['long']['bias']}"
        )

    def _rank_rows(self) -> list[dict[str, object]]:
        volume_map: dict[str, float] = {}
        if self.config.is_futures:
            try:
                for item in self.exchange.client.fapiPublicGetTicker24hr():
                    symbol_id = item.get("symbol", "")
                    if not symbol_id.endswith("USDT"):
                        continue
                    volume_map[f"{symbol_id[:-4]}/USDT:USDT"] = float(item.get("quoteVolume") or 0.0)
            except Exception:
                volume_map = {}

        ranked_rows: list[dict[str, object]] = []
        for symbol in default_candidate_symbols(self.config):
            execution_df = self.exchange.fetch_ohlcv(symbol, self.config.timeframe)
            higher_df = self.exchange.fetch_ohlcv(symbol, self.config.higher_timeframe)
            scan = scan_market(symbol, execution_df, higher_df, self.config)
            status, score = rank_scan(scan, volume_map.get(symbol, 0.0))
            if status == "ignore":
                continue
            ranked_rows.append(
                {
                    "symbol": symbol,
                    "status": status,
                    "score": float(score),
                    "scan": scan,
                }
            )
        ranked_rows.sort(key=lambda item: (item["status"] != "signal", -float(item["score"])))
        return ranked_rows

    def _build_horizon_context(self, symbol: str, execution_df, higher_df, short_scan) -> dict[str, object]:
        medium_higher_df = self.exchange.fetch_ohlcv(symbol, self.config.medium_higher_timeframe)
        long_higher_df = self.exchange.fetch_ohlcv(symbol, self.config.long_higher_timeframe)
        medium_scan = scan_market(symbol, higher_df, medium_higher_df, self.config)
        long_scan = scan_market(symbol, medium_higher_df, long_higher_df, self.config)

        short_bias = self._horizon_bias(short_scan)
        medium_bias = self._horizon_bias(medium_scan)
        long_bias = self._horizon_bias(long_scan)
        target_side = short_scan.signal.side if short_scan.signal is not None else "none"
        expected_bias = "bullish" if target_side == "long" else "bearish" if target_side == "short" else "neutral"
        same_side_count = sum(1 for bias in (medium_bias, long_bias) if bias == expected_bias)
        opposite_side_count = sum(
            1 for bias in (medium_bias, long_bias) if bias not in {"neutral", expected_bias}
        )
        return {
            "short": {
                "timeframes": f"{self.config.timeframe}/{self.config.higher_timeframe}",
                "bias": short_bias,
                "has_signal": short_scan.signal is not None,
            },
            "medium": {
                "timeframes": f"{self.config.medium_timeframe}/{self.config.medium_higher_timeframe}",
                "bias": medium_bias,
                "has_signal": medium_scan.signal is not None,
                "top_reasons": medium_scan.reasons[:3],
            },
            "long": {
                "timeframes": f"{self.config.long_timeframe}/{self.config.long_higher_timeframe}",
                "bias": long_bias,
                "has_signal": long_scan.signal is not None,
                "top_reasons": long_scan.reasons[:3],
            },
            "same_side_count": same_side_count,
            "opposite_side_count": opposite_side_count,
        }

    def _horizon_bias(self, scan) -> str:
        metrics = scan.metrics
        close = float(metrics.get("close", 0.0) or 0.0)
        ema_20 = float(metrics.get("ema_20", 0.0) or 0.0)
        ema_50 = float(metrics.get("ema_50", 0.0) or 0.0)
        higher_ema_20 = float(metrics.get("higher_ema_20", 0.0) or 0.0)
        higher_ema_50 = float(metrics.get("higher_ema_50", 0.0) or 0.0)
        vwap = float(metrics.get("vwap", 0.0) or 0.0)
        if ema_20 >= ema_50 and higher_ema_20 >= higher_ema_50 and close >= vwap * 0.995:
            return "bullish"
        if ema_20 <= ema_50 and higher_ema_20 <= higher_ema_50 and close <= vwap * 1.005:
            return "bearish"
        return "neutral"

    def _close_all_positions(self) -> str:
        positions = self.store.get_open_positions(self.config.mode)
        if not positions:
            return "정리할 열린 포지션이 없습니다."

        closed = 0
        errors: list[str] = []
        for position in positions:
            try:
                current_price = self.exchange.fetch_last_price(position.symbol)
                if self.config.mode == "live":
                    live_side = "sell" if position.side == "long" else "buy"
                    self.exchange.create_market_order(position.symbol, live_side, position.quantity, reduce_only=self.config.is_futures)
                    self.store.reset_state_counter("exchange_failure_streak")
                self.store.close_position(position.id or 0, current_price, "telegram_closeall")
                closed += 1
            except Exception as exc:
                errors.append(f"{position.symbol}:{exc}")
        message = f"closeall 완료 closed={closed}"
        if errors:
            message += f"\nerrors={' | '.join(errors[:3])}"
        return message

    def _ensure_reference_state(self, key: str, account_equity: float) -> None:
        if self.store.get_state(key) is None:
            self.store.set_state(key, f"{account_equity}")

    def _review_overflow_candidates(self, reference_time: datetime) -> None:
        if not self.config.enable_overflow_review or not self.config.overflow_symbols:
            return
        emergency_active, _ = self.store.is_emergency_stop()
        if emergency_active:
            return

        active_set = set(self._scan_symbols())
        dynamic_recent = recent_listing_candidates(self.exchange, limit=10, lookback_days=180)
        dynamic_backtest = latest_universe_candidates(Path("logs"), limit=10, min_trades=2)
        merged_overflow = list(dict.fromkeys(self.config.overflow_symbols + dynamic_recent + dynamic_backtest))
        overflow_symbols = [symbol for symbol in self.exchange.resolve_symbols(merged_overflow) if symbol not in active_set]
        if not overflow_symbols:
            return

        reviewed = 0
        for symbol in overflow_symbols:
            if reviewed >= self.config.overflow_scan_limit:
                break
            try:
                execution_df = self.exchange.fetch_ohlcv(symbol, self.config.timeframe)
                higher_df = self.exchange.fetch_ohlcv(symbol, self.config.higher_timeframe)
                scan = scan_market(symbol, execution_df, higher_df, self.config)
                status, score = rank_scan(scan, 0.0)
                if score < self.config.overflow_min_score:
                    continue
                reviewed += 1
                if scan.signal is None:
                    self.store.log_decision(
                        symbol=symbol,
                        mode=self.config.mode,
                        stage="overflow_review",
                        outcome="watch_only",
                        detail=f"Overflow candidate scored {score:.2f} but no valid entry signal.",
                        payload={"metrics": scan.metrics, "reasons": scan.reasons[:8], "score": score, "status": status},
                    )
                    continue

                review = self.ai_validator.review(scan.signal, advisory=True)
                self.store.log_decision(
                    symbol=symbol,
                    mode=self.config.mode,
                    stage="overflow_committee",
                    outcome="promotion_candidate" if review.approved else "rejected",
                    detail=review.reason,
                    payload={
                        "score": score,
                        "status": status,
                        "signal": scan.signal.strategy_data,
                        "committee": review.committee,
                        "confidence": review.confidence,
                    },
                )
                if review.approved:
                    self.notifier.send(
                        f"[OVERFLOW CANDIDATE] {symbol} looks promotable "
                        f"score={score:.2f} ai={review.confidence:.2f} reason={review.reason}"
                    )
            except Exception as exc:
                self.store.log_decision(
                    symbol=symbol,
                    mode=self.config.mode,
                    stage="overflow_review",
                    outcome="error",
                    detail=str(exc),
                    payload={},
                )

    def _resolved_fill_price(self, order: dict, fallback_price: float) -> float:
        average = order.get("average")
        price = order.get("price")
        if average is not None and float(average or 0.0) > 0:
            return float(average)
        if price is not None and float(price or 0.0) > 0:
            return float(price)
        return fallback_price
