from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .config import BotConfig
from .models import AIReview, Position, RiskDecision, TradeSignal
from .storage import StateStore, trading_day_anchor, trading_week_anchor


KST = ZoneInfo("Asia/Seoul")
CORRELATION_CLUSTERS = [
    {"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "DOGE/USDT:USDT"},
    {"BNB/USDT:USDT", "LINK/USDT:USDT", "AVAX/USDT:USDT", "ADA/USDT:USDT", "XRP/USDT:USDT"},
]


class RiskManager:
    def __init__(self, config: BotConfig, store: StateStore) -> None:
        self.config = config
        self.store = store

    def can_open_trade(
        self,
        signal: TradeSignal,
        review: AIReview,
        account_equity: float,
        now_time: datetime | None = None,
    ) -> RiskDecision:
        reference_time = (now_time or datetime.now(KST)).astimezone(KST)
        emergency_active, emergency_reason = self.store.is_emergency_stop()
        if emergency_active:
            return RiskDecision(False, f"Emergency stop is active: {emergency_reason}")

        if self.store.get_open_position(signal.symbol, self.config.mode) is not None:
            return RiskDecision(False, "There is already an open position for this symbol.")

        if self.store.count_open_positions(self.config.mode) >= self.config.max_open_positions:
            return RiskDecision(False, "Maximum number of open positions reached.")

        if self.config.mode == "live" and signal.symbol not in self.config.live_symbols():
            return RiskDecision(False, "Live trading is restricted to configured stage symbols only.")

        if self.config.mode == "live" and not self._is_allowed_entry_time(reference_time):
            return RiskDecision(False, "New entries are disabled outside the configured main session windows.")

        cooldown_deadline = self._cooldown_deadline(signal.symbol)
        if cooldown_deadline is not None and reference_time.astimezone(ZoneInfo("UTC")) < cooldown_deadline:
            return RiskDecision(False, "Symbol cooldown is active after a stop-loss.")

        stop_pct = abs(signal.entry_price - signal.stop_price) / signal.entry_price
        if stop_pct > self.config.max_stop_pct:
            return RiskDecision(False, "Stop distance exceeds configured maximum.")

        trade_risk_pct = self._trade_risk_pct(signal, account_equity)
        if trade_risk_pct > self.config.max_trade_risk_pct:
            return RiskDecision(False, "Per-trade account risk exceeds the configured cap.")

        if signal.rr < self.config.min_rr:
            return RiskDecision(False, "Reward/risk is below the configured minimum.")

        atr_regime_ratio = float(signal.strategy_data.get("atr_regime_ratio", 0.0) or 0.0)
        if atr_regime_ratio >= self.config.atr_overheat_multiplier:
            return RiskDecision(False, "ATR regime is overheated for a safe entry.")

        if self.config.ai_validation and review.confidence < self.config.min_ai_confidence_for_symbol(signal.symbol):
            return RiskDecision(False, "AI confidence is below the configured minimum.")

        if self.store.get_symbol_stoploss_streak(signal.symbol, self.config.mode) >= self.config.same_symbol_stoploss_limit:
            return RiskDecision(False, "Symbol is blocked after repeated stop-losses.")

        if (
            self.store.get_global_stoploss_streak(self.config.mode) >= self.config.global_stoploss_limit
            and self._is_drawdown_warning(account_equity, reference_time)
        ):
            return RiskDecision(False, "Global stop-loss streak and drawdown review mode are active.")

        if self._correlated_position_limit_reached(signal):
            return RiskDecision(False, "Correlated symbol exposure limit reached.")

        daily_pnl = self.store.get_today_realized_pnl(self.config.mode, reference_time)
        if daily_pnl <= (-1 * self.config.max_daily_loss):
            return RiskDecision(False, "Daily absolute loss limit reached.")
        if self._loss_pct_reached("daily", account_equity, reference_time):
            return RiskDecision(False, "Daily percentage loss limit reached.")
        if self._loss_pct_reached("weekly", account_equity, reference_time):
            return RiskDecision(False, "Weekly percentage loss limit reached.")
        if self._hard_floor_breached(account_equity, reference_time):
            return RiskDecision(False, "Account equity floor was breached.")

        if self.config.mode == "paper":
            open_exposure = self.store.get_open_exposure(self.config.mode)
            free_capital = self.config.paper_start_balance + self.store.get_summary()["realized_pnl"] - open_exposure
            if free_capital < self.config.stage_notional(signal.symbol):
                return RiskDecision(False, "Paper balance is not large enough for another trade.")

        return RiskDecision(True, "Trade allowed.")

    def _trade_risk_pct(self, signal: TradeSignal, account_equity: float) -> float:
        if account_equity <= 0:
            return 1.0
        notional = self.config.stage_notional(signal.symbol)
        if signal.entry_price <= 0:
            return 1.0
        stop_pct = abs(signal.entry_price - signal.stop_price) / signal.entry_price
        return (notional * stop_pct) / account_equity

    def _is_allowed_entry_time(self, reference_time: datetime) -> bool:
        if not self.config.allowed_entry_windows:
            return True
        current_minutes = reference_time.hour * 60 + reference_time.minute
        for window in self.config.allowed_entry_windows:
            try:
                start_text, end_text = [part.strip() for part in window.split("-", 1)]
                start_minutes = self._parse_minutes(start_text)
                end_minutes = self._parse_minutes(end_text)
            except Exception:
                continue
            if start_minutes <= end_minutes and start_minutes <= current_minutes <= end_minutes:
                return True
            if start_minutes > end_minutes and (current_minutes >= start_minutes or current_minutes <= end_minutes):
                return True
        return False

    def _parse_minutes(self, value: str) -> int:
        hour, minute = value.split(":", 1)
        return int(hour) * 60 + int(minute)

    def _cooldown_deadline(self, symbol: str) -> datetime | None:
        closed_at = self.store.get_last_stoploss_closed_at(symbol, self.config.mode)
        if closed_at is None:
            return None
        return closed_at + timedelta(minutes=self.config.symbol_cooldown_minutes)

    def _correlated_position_limit_reached(self, signal: TradeSignal) -> bool:
        open_positions = self.store.get_open_positions(self.config.mode)
        same_side_positions = [position for position in open_positions if position.side == signal.side]
        cluster = self._find_cluster(signal.symbol)
        if cluster is None:
            return False
        correlated_count = sum(1 for position in same_side_positions if position.symbol in cluster and position.symbol != signal.symbol)
        return correlated_count >= self.config.max_correlated_positions

    def _find_cluster(self, symbol: str) -> set[str] | None:
        for cluster in CORRELATION_CLUSTERS:
            if symbol in cluster:
                return cluster
        return None

    def _reference_equity(self, scope: str, account_equity: float, reference_time: datetime) -> float:
        if scope == "daily":
            anchor = trading_day_anchor(reference_time)
            key_name = "daily_reference"
        else:
            anchor = trading_week_anchor(reference_time)
            key_name = "weekly_reference"
        key = f"{key_name}:{anchor.date().isoformat()}"
        stored = self.store.get_state(key)
        if stored is None:
            self.store.set_state(key, f"{account_equity}")
            return account_equity
        return float(stored)

    def _loss_pct_reached(self, scope: str, account_equity: float, reference_time: datetime) -> bool:
        baseline = self._reference_equity(scope, account_equity, reference_time)
        if baseline <= 0:
            return False
        drawdown = max(0.0, (baseline - account_equity) / baseline)
        limit = self.config.max_daily_loss_pct if scope == "daily" else self.config.max_weekly_loss_pct
        return drawdown >= limit

    def _hard_floor_breached(self, account_equity: float, reference_time: datetime) -> bool:
        baseline = self._reference_equity("daily", account_equity, reference_time)
        if baseline <= 0:
            return False
        return account_equity <= baseline * self.config.hard_stop_equity_floor_pct

    def _is_drawdown_warning(self, account_equity: float, reference_time: datetime) -> bool:
        baseline = self._reference_equity("daily", account_equity, reference_time)
        if baseline <= 0:
            return False
        drawdown = max(0.0, (baseline - account_equity) / baseline)
        return drawdown >= min(self.config.max_daily_loss_pct, 0.10)
