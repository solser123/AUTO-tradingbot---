from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .ai_validator import AIValidator
from .config import BotConfig
from .execution_router import ExecutionRouter
from .exchange import BinanceExchange
from .external_sources import fetch_blockmedia_news, fetch_tradingview_ideas
from .models import Position, TradeSignal
from .notifier import TelegramNotifier
from .opportunity import analyze_pending_opportunities
from .research import latest_universe_candidates, recent_listing_candidates
from .risk import RiskManager
from .runtime_state import set_runtime_flag
from .sectors import sector_for_symbol, sector_label
from .selector import build_exit_roadmap, default_candidate_symbols, rank_scan
from .sizing import build_sizing_decision
from .storage import StateStore, trading_day_anchor, trading_week_anchor
from .strategy import scan_market, should_exit


KST = ZoneInfo("Asia/Seoul")


def _timeframe_to_minutes(timeframe: str) -> int:
    raw = timeframe.strip().lower()
    if raw.endswith("m"):
        return int(raw[:-1])
    if raw.endswith("h"):
        return int(raw[:-1]) * 60
    if raw.endswith("d"):
        return int(raw[:-1]) * 1440
    return 15


class TradingEngine:
    def __init__(
        self,
        config: BotConfig,
        exchange: BinanceExchange,
        store: StateStore,
        notifier: TelegramNotifier,
        ai_validator: AIValidator,
        risk_manager: RiskManager,
        execution_router: ExecutionRouter | None = None,
    ) -> None:
        self.config = config
        self.exchange = exchange
        self.store = store
        self.notifier = notifier
        self.ai_validator = ai_validator
        self.risk_manager = risk_manager
        self.execution_router = execution_router or ExecutionRouter(exchange)
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
            f"cmd=/help /status /positions /pause /resume /rank /stage /research /sectors /research-news /opportunity BTC /scan BTC /summary /closeall /stopbot"
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
            f"cmd=/help /status /positions /pause /resume /rank /stage /research /sectors /research-news /opportunity BTC /scan BTC /summary /closeall /stopbot"
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
        self._sync_external_research(reference_time)
        self._sync_sector_flows(reference_time)
        self._sync_opportunity_reviews(reference_time)
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
                set_runtime_flag(self.store, "last_exchange_error_at", datetime.now(timezone.utc).isoformat())
                streak = self.store.increment_state_counter("exchange_failure_streak")
                if streak >= self.config.exchange_failure_limit:
                    self.store.set_emergency_stop(
                        f"Exchange/runtime failure streak reached {streak}.",
                        severity="transient",
                    )
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
        horizon_context = self._build_horizon_context(symbol, execution_df, higher_df, scan)
        if signal is None:
            recovered_signal = None
            if self.config.enable_context_recovery:
                recovered_signal = self._build_context_recovery_signal(symbol, scan, horizon_context)
            if recovered_signal is not None:
                signal = recovered_signal
                self.store.log_decision(
                    symbol=symbol,
                    mode=self.config.mode,
                    stage="context_recovery",
                    outcome="triggered",
                    detail=f"Context recovery promoted {signal.side} entry candidate.",
                    payload={"signal": signal.strategy_data, "reasons": scan.reasons[:8]},
                )
            else:
                detail = " | ".join(scan.reasons[:3]) if scan.reasons else "No rule-based setup."
                logging.info("%s: no rule-based setup. %s", symbol, detail)
                if not self.store.has_recent_decision(
                    symbol=symbol,
                    mode=self.config.mode,
                    stage="scan",
                    outcome="no_entry",
                    detail=detail,
                    within_minutes=_timeframe_to_minutes(self.config.timeframe),
                ):
                    self.store.log_decision(
                        symbol=symbol,
                        mode=self.config.mode,
                        stage="scan",
                        outcome="no_entry",
                        detail=detail,
                        payload={"metrics": scan.metrics, "reasons": scan.reasons[:8]},
                    )
                return

        signal.strategy_data["multi_horizon"] = horizon_context
        external_alignment = self.store.get_external_alignment(symbol, signal.side, hours=36)
        signal.strategy_data["external_alignment"] = external_alignment
        sector_context = self._sector_context(symbol)
        signal.strategy_data["sector"] = sector_context["sector"]
        signal.strategy_data["sector_label"] = sector_context["label"]
        signal.strategy_data["sector_context"] = sector_context
        microstructure = self.exchange.fetch_microstructure(
            symbol,
            depth=self.config.microstructure_orderbook_depth,
            trade_limit=self.config.microstructure_trade_limit,
        )
        signal.strategy_data["microstructure"] = microstructure
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
        if self._sector_blocks_signal(signal.side, sector_context):
            detail = "Sector flow is materially against this trade."
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="sector_gate",
                outcome="rejected",
                detail=detail,
                payload={"sector_context": sector_context, "signal": signal.strategy_data},
            )
            return
        micro_rejection = self._microstructure_rejection(signal.side, microstructure)
        if micro_rejection:
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="micro_gate",
                outcome="rejected",
                detail=micro_rejection,
                payload={"microstructure": microstructure, "signal": signal.strategy_data},
            )
            return
        if int(external_alignment.get("count", 0)) >= 4 and float(external_alignment.get("alignment_score", 0.0)) <= -0.25:
            detail = "External news/community alignment is materially against this trade."
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="external_gate",
                outcome="rejected",
                detail=detail,
                payload={"external_alignment": external_alignment, "signal": signal.strategy_data},
            )
            return

        sizing = build_sizing_decision(
            signal=signal,
            config=self.config,
            account_equity=account_equity,
            open_positions=self.store.get_open_positions(self.config.mode),
            horizon_context=horizon_context,
            sector_context=sector_context,
            external_alignment=external_alignment,
            microstructure=microstructure,
        )
        signal.strategy_data["sizing"] = {
            "score": sizing.score,
            "bucket": sizing.bucket,
            "risk_pct": sizing.risk_pct,
            "risk_multiple": sizing.risk_multiple,
            "notional": round(sizing.notional, 4),
            "risk_notional_cap": round(sizing.risk_notional_cap, 4),
            "stage_cap_notional": round(sizing.stage_cap_notional, 4),
            "components": sizing.components,
        }
        if not sizing.allowed:
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="sizing_model",
                outcome="rejected",
                detail=sizing.reason,
                payload={"sizing": signal.strategy_data["sizing"], "signal": signal.strategy_data},
            )
            return

        review = self.ai_validator.review(signal)
        self.store.log_signal(signal, review.approved, review.confidence, review.reason)
        if review.reason.startswith("AI validation failed"):
            set_runtime_flag(self.store, "last_ai_error_at", datetime.now(timezone.utc).isoformat())
            streak = self.store.increment_state_counter("ai_failure_streak")
            if streak >= self.config.ai_failure_limit:
                self.store.set_emergency_stop(
                    f"AI validation failure streak reached {streak}.",
                    severity="transient",
                )
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

        initial_notional = sizing.notional
        quantity_estimate = initial_notional / signal.entry_price
        order_plan = self.execution_router.prepare_market_order(
            symbol=symbol,
            side="buy" if signal.side == "long" else "sell",
            reference_price=signal.entry_price,
            requested_quantity=quantity_estimate,
            reduce_only=False,
        )
        quantity = order_plan.normalized_quantity
        if quantity <= 0 or order_plan.reason.startswith("Order rejected:"):
            logging.info("%s: order requirement rejected signal. %s", symbol, order_plan.reason)
            self.store.log_decision(
                symbol=symbol,
                mode=self.config.mode,
                stage="sizing",
                outcome="rejected",
                detail=order_plan.reason,
                payload={
                    "entry_price": signal.entry_price,
                    "requested_notional": initial_notional,
                    "requested_quantity": quantity_estimate,
                    "normalized_quantity": quantity,
                    "execution_plan": {
                        "estimated_fill_price": order_plan.estimated_fill_price,
                        "estimated_notional": order_plan.estimated_notional,
                        "estimated_slippage_pct": order_plan.estimated_slippage_pct,
                        "tick_size": order_plan.tick_size,
                        "step_size": order_plan.step_size,
                        "min_amount": order_plan.min_amount,
                        "min_notional": order_plan.min_notional,
                    },
                },
            )
            return

        entry_price = signal.entry_price
        if self.config.mode == "live":
            execution = self._execute_order_plan(order_plan)
            entry_price = execution.average_price or signal.entry_price
            slippage_pct = abs(entry_price - signal.entry_price) / signal.entry_price if signal.entry_price else 0.0
            if slippage_pct > self.config.max_slippage_pct:
                self.store.set_emergency_stop(
                    f"Abnormal slippage detected on {symbol}: {slippage_pct * 100:.2f}%.",
                    severity="transient",
                )
                self.notifier.send(
                    f"[EMERGENCY STOP] {symbol} slippage {slippage_pct * 100:.2f}% exceeded limit."
                )
        else:
            execution = None

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
                "symbol_stage": self.config.stage_for_symbol(symbol),
                "base_notional": initial_notional,
                "execution_plan": {
                    "estimated_fill_price": order_plan.estimated_fill_price,
                    "estimated_notional": order_plan.estimated_notional,
                    "estimated_slippage_pct": order_plan.estimated_slippage_pct,
                    "normalized_quantity": order_plan.normalized_quantity,
                },
                "execution_result": (
                    {
                        "order_id": execution.order_id,
                        "status": execution.status,
                        "filled_notional": execution.filled_notional,
                        "actual_slippage_pct": execution.actual_slippage_pct,
                    }
                    if execution is not None
                    else {}
                ),
                "sizing": signal.strategy_data.get("sizing", {}),
                "sector_context": sector_context,
                "ai_confidence": review.confidence,
                "committee": review.committee,
                "signal": signal.strategy_data,
            },
        )
        logging.info("%s: opened %s position at %.4f", symbol, signal.side, entry_price)
        self.notifier.send(
            f"[OPEN] {symbol} s{self.config.stage_for_symbol(symbol)} {signal.side} entry={entry_price:.4f} "
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
            order_plan = self.execution_router.prepare_market_order(
                symbol=position.symbol,
                side="sell" if position.side == "long" else "buy",
                reference_price=current_price,
                requested_quantity=position.quantity,
                reduce_only=self.config.is_futures,
            )
            execution = self._execute_order_plan(order_plan)
            current_price = execution.average_price or current_price

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

    def _notional_for_profile(
        self,
        symbol: str,
        profile: str,
        side: str,
        sector_context: dict[str, object] | None = None,
    ) -> float:
        base_notional = self.config.stage_notional(symbol)
        if profile == "aggressive":
            notional = base_notional
        elif profile == "balanced":
            notional = base_notional * 0.75
        else:
            notional = base_notional * 0.5
        if self.config.stage_for_symbol(symbol) >= 2 and self._sector_supports_side(side, sector_context):
            notional *= 1.0 + self.config.sector_alignment_notional_boost_pct
        return notional

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
            order_plan = self.execution_router.prepare_market_order(
                symbol=position.symbol,
                side="sell" if position.side == "long" else "buy",
                reference_price=current_price,
                requested_quantity=reduce_qty,
                reduce_only=self.config.is_futures,
            )
            execution = self._execute_order_plan(order_plan)
            reduce_qty = execution.executed_quantity or reduce_qty

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

    def _execute_order_plan(self, order_plan):
        try:
            execution = self.execution_router.execute_market_order(order_plan)
            self.store.reset_state_counter("exchange_failure_streak")
            set_runtime_flag(self.store, "last_exchange_ok_at", datetime.now(timezone.utc).isoformat())
            return execution
        except Exception:
            set_runtime_flag(self.store, "last_order_error_at", datetime.now(timezone.utc).isoformat())
            raise

    def _account_equity(self, reference_time: datetime) -> float:
        if self.config.mode == "paper":
            realized = float(self.store.get_summary()["realized_pnl"])
            return self.config.paper_start_balance + realized

        try:
            equity = self.exchange.fetch_account_equity()
            self.store.reset_state_counter("exchange_failure_streak")
            set_runtime_flag(self.store, "last_exchange_ok_at", datetime.now(timezone.utc).isoformat())
            self.store.set_state("last_known_equity", f"{equity}")
            return equity
        except Exception as exc:
            set_runtime_flag(self.store, "last_exchange_error_at", datetime.now(timezone.utc).isoformat())
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
                self.store.set_emergency_stop(
                    f"Balance check failure streak reached {streak}.",
                    severity="transient",
                )
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
            self.store.set_emergency_stop(reason, severity="fatal")
            self.notifier.send(f"[EMERGENCY STOP] {reason}")

    def _sync_opportunity_reviews(self, reference_time: datetime) -> None:
        last_sync_text = self.store.get_state("opportunity_sync_at")
        if last_sync_text:
            try:
                last_sync = datetime.fromisoformat(last_sync_text)
                if last_sync.tzinfo is None:
                    last_sync = last_sync.replace(tzinfo=timezone.utc)
                else:
                    last_sync = last_sync.astimezone(timezone.utc)
                elapsed = (reference_time.astimezone(timezone.utc) - last_sync).total_seconds()
                if elapsed < self.config.opportunity_sync_interval_minutes * 60:
                    return
            except ValueError:
                pass
        try:
            inserted = analyze_pending_opportunities(self.store, self.exchange, self.config, batch_limit=60)
            self.store.set_state("opportunity_sync_at", reference_time.astimezone(timezone.utc).isoformat())
            if inserted:
                self.store.log_decision(
                    symbol="SYSTEM",
                    mode=self.config.mode,
                    stage="opportunity_sync",
                    outcome="updated",
                    detail=f"Opportunity reviews inserted: {inserted}",
                    payload={"inserted": inserted},
                )
        except Exception as exc:
            self.store.log_decision(
                symbol="SYSTEM",
                mode=self.config.mode,
                stage="opportunity_sync",
                outcome="error",
                detail=str(exc),
                payload={},
            )

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
                "/sectors 섹터 자금 흐름\n"
                "/research-news 외부 뉴스/아이디어 요약\n"
                "/opportunity BTC 놓친 자리 분석\n"
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

        if command == "/sectors":
            return self._format_sector_flows(), False

        if command == "/research-news":
            return self._format_research_news(), False

        if command == "/opportunity":
            symbol = args[0] if args else None
            return self._format_opportunity(symbol), False

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
            self.store.set_emergency_stop("Manual emergency stop from Telegram.", severity="fatal")
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
        sectors = self.store.get_latest_sector_flows(limit=3)
        sector_text = ", ".join(
            f"{sector_label(str(item['sector']))}:{float(item['flow_score']):.2f}"
            for item in sectors
        ) or "none"
        return (
            f"status mode={self.config.mode}\n"
            f"paused={paused} emergency={emergency_active}\n"
            f"equity={equity}\n"
            f"open={summary['open_positions']} closed={summary['closed_positions']}\n"
            f"signals={summary['total_signals']} approved={summary['approved_signals']}\n"
            f"realized_pnl={summary['realized_pnl']:.4f} win_rate={summary['win_rate']:.2f}%\n"
            f"sectors={sector_text}\n"
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
            sector = sector_for_symbol(str(row["symbol"]))
            sector_ctx = self.store.get_latest_sector_flow(sector)
            if scan.signal is not None:
                signal = scan.signal
                roadmap = build_exit_roadmap(signal.entry_price, signal.stop_price, signal.target_price, self.config.max_hold_minutes)
                lines.append(
                    f"{row['symbol']} signal {signal.side} {signal.entry_profile} "
                    f"entry={signal.entry_price:.4f} stop={roadmap['stop_pct']}% target={roadmap['target_pct']}% "
                    f"sector={sector_label(sector)}:{float(sector_ctx['flow_score']):.2f}"
                )
            else:
                lines.append(
                    f"{row['symbol']} watch score={row['score']:.2f} "
                    f"rsi={scan.metrics.get('rsi_14')} vol={scan.metrics.get('volume_ratio')} "
                    f"sector={sector_label(sector)}:{float(sector_ctx['flow_score']):.2f}"
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
        sectors = self.store.get_latest_sector_flows(limit=5)
        sector_lines = ",".join(
            f"{sector_label(str(item['sector']))}:{str(item['direction'])}:{float(item['flow_score']):.2f}"
            for item in sectors
        ) or "none"
        return (
            "research snapshot\n"
            f"backtest={','.join(backtest) or 'none'}\n"
            f"recent={','.join(recent) or 'none'}\n"
            f"sectors={sector_lines}"
        )

    def _format_research_news(self) -> str:
        rows = self.store.get_recent_external_items(limit=8, hours=36)
        if not rows:
            return "research news\nno external items yet"
        sectors = self.store.get_latest_sector_flows(limit=3)
        sector_text = ", ".join(
            f"{sector_label(str(item['sector']))}:{str(item['direction'])}:{float(item['flow_score']):.2f}"
            for item in sectors
        ) or "none"
        lines = ["research news", f"top_sectors={sector_text}"]
        for row in rows:
            lines.append(
                f"{row['source']} {row['direction']} {row['title'][:70]}"
            )
        return "\n".join(lines)

    def _format_sector_flows(self) -> str:
        rows = self.store.get_latest_sector_flows(limit=8)
        if not rows:
            return "sector flows\nno sector data yet"
        lines = ["sector flows"]
        for row in rows:
            leaders = [str(item.get("symbol", "")) for item in list(row.get("leaders", []))[:2] if item.get("symbol")]
            leader_text = ",".join(leaders) or "none"
            lines.append(
                f"{sector_label(str(row['sector']))} {row['direction']} score={float(row['flow_score']):.2f} "
                f"liq={float(row['liquidity_usdt']):.0f} count={int(row['symbol_count'])} leaders={leader_text}"
            )
        return "\n".join(lines)

    def _format_opportunity(self, raw_symbol: str | None = None) -> str:
        symbol = None
        if raw_symbol:
            token = raw_symbol.strip().upper()
            symbol = token if "/" in token else f"{token}/USDT:USDT"
        summary = self.store.get_opportunity_summary(symbol=symbol, hours=48)
        rows = self.store.get_opportunity_reviews(symbol=symbol, hours=48, only_material=True, limit=5)
        label = symbol or "ALL"
        if not rows:
            return (
                f"opportunity {label}\n"
                f"reviews={summary['reviews']} material={summary['material_reviews']}\n"
                "아직 유의미한 놓친 자리 데이터가 없습니다."
            )
        lines = [
            f"opportunity {label}",
            (
                f"reviews={summary['reviews']} material={summary['material_reviews']} "
                f"avg_move={float(summary['avg_move_pct']):.2f}% "
                f"best={float(summary['best_move_pct']):.2f}% "
                f"missed_notional={float(summary['missed_notional_pnl']):.4f}"
            ),
        ]
        for row in rows:
            lines.append(
                f"{row['symbol']} {row['dominant_side']} move={float(row['dominant_move_pct']):.2f}% "
                f"blockers={row['blockers_csv']}"
            )
        return "\n".join(lines)

    def _format_scan(self, symbol_text: str) -> str:
        target = symbol_text.strip().upper()
        if "/" not in target:
            target = f"{target}/USDT"
        resolved = self.exchange.resolve_symbols([target])[0]
        execution_df = self.exchange.fetch_ohlcv(resolved, self.config.timeframe)
        higher_df = self.exchange.fetch_ohlcv(resolved, self.config.higher_timeframe)
        scan = scan_market(resolved, execution_df, higher_df, self.config)
        horizons = self._build_horizon_context(resolved, execution_df, higher_df, scan)
        side = scan.signal.side if scan.signal is not None else "long"
        external = self.store.get_external_alignment(resolved, side, hours=36)
        sector_ctx = self._sector_context(resolved)
        micro = self.exchange.fetch_microstructure(
            resolved,
            depth=self.config.microstructure_orderbook_depth,
            trade_limit=self.config.microstructure_trade_limit,
        )
        if scan.signal is None:
            return (
                f"{resolved}\nno signal\n"
                + "\n".join(scan.reasons[:5])
                + "\n"
                + f"bias short={horizons['short']['bias']} mid={horizons['medium']['bias']} long={horizons['long']['bias']}\n"
                + f"external count={external['count']} align={float(external['alignment_score']):.2f}\n"
                + f"sector={sector_ctx['label']} flow={float(sector_ctx['flow_score']):.2f} direction={sector_ctx['direction']}\n"
                + f"micro spread={float(micro['spread_pct'])*100:.3f}% depth={float(micro['total_depth_usdt']):.0f} flow={float(micro['trade_flow_score']):.2f} imbalance={float(micro['depth_imbalance']):.2f}"
            )
        signal = scan.signal
        roadmap = build_exit_roadmap(signal.entry_price, signal.stop_price, signal.target_price, self.config.max_hold_minutes)
        return (
            f"{resolved}\n"
            f"signal={signal.side} profile={signal.entry_profile} setup={signal.setup_type}\n"
            f"entry={signal.entry_price:.6f} stop={signal.stop_price:.6f} target={signal.target_price:.6f}\n"
            f"stop_pct={roadmap['stop_pct']} target_pct={roadmap['target_pct']} rr={signal.rr:.2f}\n"
            f"bias short={horizons['short']['bias']} mid={horizons['medium']['bias']} long={horizons['long']['bias']}\n"
            f"external count={external['count']} align={float(external['alignment_score']):.2f} "
            f"community={float(external['community_score']):.2f} news={float(external['news_score']):.2f}\n"
            f"sector={sector_ctx['label']} flow={float(sector_ctx['flow_score']):.2f} direction={sector_ctx['direction']}\n"
            f"micro spread={float(micro['spread_pct'])*100:.3f}% depth={float(micro['total_depth_usdt']):.0f} flow={float(micro['trade_flow_score']):.2f} imbalance={float(micro['depth_imbalance']):.2f}"
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

    def _build_context_recovery_signal(self, symbol: str, scan, horizon_context: dict[str, object]) -> TradeSignal | None:
        metrics = scan.metrics
        reasons = scan.reasons or []
        if not metrics:
            return None

        close = float(metrics.get("close", 0.0) or 0.0)
        ema_20 = float(metrics.get("ema_20", 0.0) or 0.0)
        ema_50 = float(metrics.get("ema_50", 0.0) or 0.0)
        atr = float(metrics.get("atr_14", 0.0) or 0.0)
        rsi = float(metrics.get("rsi_14", 0.0) or 0.0)
        stoch_k = float(metrics.get("stoch_k", 0.0) or 0.0)
        stoch_d = float(metrics.get("stoch_d", 0.0) or 0.0)
        volume_ratio = float(metrics.get("volume_ratio", 0.0) or 0.0)
        vwap = float(metrics.get("vwap", 0.0) or 0.0)
        higher_ema_rising = bool(metrics.get("higher_ema_rising", False))
        higher_ema_falling = bool(metrics.get("higher_ema_falling", False))
        if min(close, ema_20, ema_50, atr) <= 0:
            return None

        detail_text = " ".join(reasons).lower()
        blocked_by_transition = any(token in detail_text for token in ["vwap", "rsi", "higher timeframe bias", "stochastic"])
        if not blocked_by_transition:
            return None

        short_bias = str((horizon_context.get("short") or {}).get("bias", "neutral"))
        medium_bias = str((horizon_context.get("medium") or {}).get("bias", "neutral"))
        long_bias = str((horizon_context.get("long") or {}).get("bias", "neutral"))

        long_alignment = self.store.get_external_alignment(symbol, "long", hours=36)
        short_alignment = self.store.get_external_alignment(symbol, "short", hours=36)
        long_external_good = (
            int(long_alignment.get("count", 0)) >= self.config.context_recovery_external_count_min
            and float(long_alignment.get("alignment_score", 0.0)) >= self.config.context_recovery_external_min
        )
        short_external_good = (
            int(short_alignment.get("count", 0)) >= self.config.context_recovery_external_count_min
            and float(short_alignment.get("alignment_score", 0.0)) >= self.config.context_recovery_external_min
        )

        long_transition = (
            close >= ema_20
            and ema_20 >= ema_50 * 0.995
            and rsi >= 50
            and stoch_k >= stoch_d
            and volume_ratio >= 0.10
            and higher_ema_rising
            and (medium_bias == "bullish" or long_bias == "bullish" or short_bias == "bullish" or long_external_good)
        )
        if long_transition:
            stop = min(close - (atr * 1.6), close * (1 - self.config.max_stop_pct))
            risk = close - stop
            if risk > 0:
                target = close + (risk * max(self.config.min_rr, 1.4))
                return TradeSignal(
                    symbol=symbol,
                    side="long",
                    entry_price=close,
                    stop_price=stop,
                    target_price=target,
                    rr=max(self.config.min_rr, 1.4),
                    setup_type="context_recovery_long",
                    entry_profile="conservative",
                    reason="Context recovery long: trend transition + sentiment/multi-horizon support.",
                    strategy_data={
                        **metrics,
                        "entry_profile_score": 0.44,
                        "entry_profile": "conservative",
                        "context_recovery": True,
                        "context_side": "long",
                        "external_alignment": long_alignment,
                        "horizon": {"short": short_bias, "medium": medium_bias, "long": long_bias},
                    },
                )

        short_transition = (
            close <= ema_20
            and ema_20 <= ema_50 * 1.005
            and rsi <= 50
            and stoch_k <= stoch_d
            and volume_ratio >= 0.10
            and higher_ema_falling
            and (medium_bias == "bearish" or long_bias == "bearish" or short_bias == "bearish" or short_external_good)
        )
        if short_transition:
            stop = max(close + (atr * 1.6), close * (1 + self.config.max_stop_pct))
            risk = stop - close
            if risk > 0:
                target = close - (risk * max(self.config.min_rr, 1.4))
                return TradeSignal(
                    symbol=symbol,
                    side="short",
                    entry_price=close,
                    stop_price=stop,
                    target_price=target,
                    rr=max(self.config.min_rr, 1.4),
                    setup_type="context_recovery_short",
                    entry_profile="conservative",
                    reason="Context recovery short: trend transition + sentiment/multi-horizon support.",
                    strategy_data={
                        **metrics,
                        "entry_profile_score": 0.44,
                        "entry_profile": "conservative",
                        "context_recovery": True,
                        "context_side": "short",
                        "external_alignment": short_alignment,
                        "horizon": {"short": short_bias, "medium": medium_bias, "long": long_bias},
                    },
                )
        return None

    def _quote_volume_map(self) -> dict[str, float]:
        volume_map: dict[str, float] = {}
        if not self.config.is_futures:
            return volume_map
        try:
            for item in self.exchange.client.fapiPublicGetTicker24hr():
                symbol_id = item.get("symbol", "")
                if not symbol_id.endswith("USDT"):
                    continue
                volume_map[f"{symbol_id[:-4]}/USDT:USDT"] = float(item.get("quoteVolume") or 0.0)
        except Exception:
            return {}
        return volume_map

    def _sector_context(self, symbol: str) -> dict[str, object]:
        sector = sector_for_symbol(symbol)
        context = self.store.get_latest_sector_flow(sector)
        context["sector"] = sector
        context["label"] = sector_label(sector)
        return context

    def _sector_supports_side(self, side: str, sector_context: dict[str, object] | None) -> bool:
        if not self.config.enable_sector_flow or not sector_context:
            return False
        flow_score = float(sector_context.get("flow_score", 0.0) or 0.0)
        liquidity = float(sector_context.get("liquidity_usdt", 0.0) or 0.0)
        if liquidity < self.config.sector_min_liquidity_usdt:
            return False
        if side == "long":
            return flow_score >= self.config.sector_flow_positive_threshold
        return flow_score <= self.config.sector_flow_negative_threshold

    def _sector_blocks_signal(self, side: str, sector_context: dict[str, object] | None) -> bool:
        if not self.config.enable_sector_flow or not sector_context:
            return False
        flow_score = float(sector_context.get("flow_score", 0.0) or 0.0)
        symbol_count = int(sector_context.get("symbol_count", 0) or 0)
        if symbol_count < 2:
            return False
        if side == "long":
            return flow_score <= (-1.0 * self.config.sector_opposition_gate_threshold)
        return flow_score >= self.config.sector_opposition_gate_threshold

    def _microstructure_rejection(self, side: str, micro: dict[str, object] | None) -> str | None:
        if not self.config.enable_microstructure_filter or not micro:
            return None
        spread_pct = float(micro.get("spread_pct", 0.0) or 0.0)
        total_depth = float(micro.get("total_depth_usdt", 0.0) or 0.0)
        trade_flow = float(micro.get("trade_flow_score", 0.0) or 0.0)
        depth_imbalance = float(micro.get("depth_imbalance", 0.0) or 0.0)

        if spread_pct > self.config.microstructure_max_spread_pct:
            return "Microstructure rejected: spread is too wide."
        if total_depth < self.config.microstructure_min_total_depth_usdt:
            return "Microstructure rejected: order book depth is too thin."
        if side == "long":
            if trade_flow <= (-1.0 * self.config.microstructure_flow_gate_threshold):
                return "Microstructure rejected: recent trade flow is leaning too bearish."
            if depth_imbalance <= (-1.0 * self.config.microstructure_imbalance_gate_threshold):
                return "Microstructure rejected: book imbalance is leaning too bearish."
        else:
            if trade_flow >= self.config.microstructure_flow_gate_threshold:
                return "Microstructure rejected: recent trade flow is leaning too bullish."
            if depth_imbalance >= self.config.microstructure_imbalance_gate_threshold:
                return "Microstructure rejected: book imbalance is leaning too bullish."
        return None

    def _sync_sector_flows(self, reference_time: datetime) -> None:
        if not self.config.enable_sector_flow:
            return
        last_sync = self.store.get_state("sector_flow_sync_at")
        if last_sync:
            try:
                parsed = datetime.fromisoformat(last_sync)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                elapsed = (reference_time.astimezone(timezone.utc) - parsed).total_seconds()
                if elapsed < max(self.config.sector_sync_interval_minutes, 1) * 60:
                    return
            except Exception:
                pass

        universe = list(
            dict.fromkeys(
                self.config.live_symbols()
                + self.config.candidate_symbols
                + self.config.overflow_symbols
            )
        )
        if not universe:
            return

        volume_map = self._quote_volume_map()
        grouped: dict[str, list[dict[str, float | str]]] = {}
        for symbol in self.exchange.resolve_symbols(universe):
            sector = sector_for_symbol(symbol)
            try:
                execution_df = self.exchange.fetch_ohlcv(symbol, self.config.timeframe)
                higher_df = self.exchange.fetch_ohlcv(symbol, self.config.higher_timeframe)
            except Exception:
                continue
            if execution_df is None or higher_df is None or len(execution_df) < 20 or len(higher_df) < 6:
                continue
            try:
                close_now = float(execution_df["close"].iloc[-1])
                close_then = float(execution_df["close"].iloc[-5])
                high_close_now = float(higher_df["close"].iloc[-1])
                high_close_then = float(higher_df["close"].iloc[-4])
                last_volume = float(execution_df["volume"].iloc[-1])
                avg_volume = float(execution_df["volume"].tail(20).mean())
            except Exception:
                continue
            if min(close_now, close_then, high_close_now, high_close_then) <= 0:
                continue
            grouped.setdefault(sector, []).append(
                {
                    "symbol": symbol,
                    "short_return_pct": ((close_now / close_then) - 1.0) * 100.0,
                    "medium_return_pct": ((high_close_now / high_close_then) - 1.0) * 100.0,
                    "volume_ratio": (last_volume / avg_volume) if avg_volume > 0 else 0.0,
                    "liquidity_usdt": float(volume_map.get(symbol, 0.0) or 0.0),
                }
            )

        if not grouped:
            return

        snapshot_at = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
        logged = 0
        for sector, rows in grouped.items():
            avg_short = sum(float(item["short_return_pct"]) for item in rows) / len(rows)
            avg_medium = sum(float(item["medium_return_pct"]) for item in rows) / len(rows)
            avg_volume_ratio = sum(float(item["volume_ratio"]) for item in rows) / len(rows)
            liquidity = sum(float(item["liquidity_usdt"]) for item in rows)
            positive = sum(
                1 for item in rows
                if float(item["short_return_pct"]) > 0 and float(item["medium_return_pct"]) > 0
            )
            negative = sum(
                1 for item in rows
                if float(item["short_return_pct"]) < 0 and float(item["medium_return_pct"]) < 0
            )
            breadth = (positive - negative) / max(len(rows), 1)
            flow_score = (
                max(-0.60, min(0.60, avg_short / 3.0))
                + max(-0.45, min(0.45, avg_medium / 4.0))
                + max(-0.30, min(0.30, (avg_volume_ratio - 1.0) * 0.30))
                + max(-0.25, min(0.25, breadth * 0.25))
            )
            direction = "neutral"
            if flow_score >= self.config.sector_flow_positive_threshold:
                direction = "bullish"
            elif flow_score <= self.config.sector_flow_negative_threshold:
                direction = "bearish"
            leaders = sorted(
                rows,
                key=lambda item: abs(float(item["short_return_pct"])) + abs(float(item["medium_return_pct"])),
                reverse=True,
            )[:3]
            self.store.log_sector_flow_snapshot(
                {
                    "snapshot_at": snapshot_at,
                    "sector": sector,
                    "direction": direction,
                    "flow_score": flow_score,
                    "avg_short_return_pct": avg_short,
                    "avg_medium_return_pct": avg_medium,
                    "avg_volume_ratio": avg_volume_ratio,
                    "liquidity_usdt": liquidity,
                    "symbol_count": len(rows),
                    "leaders": leaders,
                    "payload": {"breadth": breadth},
                }
            )
            logged += 1
        self.store.set_state("sector_flow_sync_at", datetime.now(timezone.utc).isoformat())
        self.store.log_decision(
            symbol="SYSTEM",
            mode=self.config.mode,
            stage="sector_flow_sync",
            outcome="updated",
            detail=f"Sector flow sync completed for {logged} sectors.",
            payload={"sectors": logged},
        )

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
                    order_plan = self.execution_router.prepare_market_order(
                        symbol=position.symbol,
                        side="sell" if position.side == "long" else "buy",
                        reference_price=current_price,
                        requested_quantity=position.quantity,
                        reduce_only=self.config.is_futures,
                    )
                    execution = self._execute_order_plan(order_plan)
                    current_price = execution.average_price or current_price
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

    def _sync_external_research(self, reference_time: datetime) -> None:
        last_sync = self.store.get_state("external_sync_at")
        if last_sync:
            try:
                parsed = datetime.fromisoformat(last_sync)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                if (reference_time.astimezone(timezone.utc) - parsed).total_seconds() < 900:
                    return
            except Exception:
                pass
        try:
            inserted = 0
            inserted += self.store.upsert_external_items(fetch_tradingview_ideas(limit=15))
            inserted += self.store.upsert_external_items(fetch_blockmedia_news(limit=15))
            self.store.set_state("external_sync_at", datetime.now(timezone.utc).isoformat())
            self.store.log_decision(
                symbol="SYSTEM",
                mode=self.config.mode,
                stage="external_sync",
                outcome="updated",
                detail=f"External sync completed with {inserted} new items.",
                payload={"inserted": inserted},
            )
        except Exception as exc:
            self.store.log_decision(
                symbol="SYSTEM",
                mode=self.config.mode,
                stage="external_sync",
                outcome="error",
                detail=str(exc),
                payload={},
            )
