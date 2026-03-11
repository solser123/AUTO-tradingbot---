from __future__ import annotations

from .config import BotConfig
from .models import AIReview, RiskDecision, TradeSignal
from .storage import StateStore


class RiskManager:
    def __init__(self, config: BotConfig, store: StateStore) -> None:
        self.config = config
        self.store = store

    def can_open_trade(self, signal: TradeSignal, review: AIReview) -> RiskDecision:
        if self.store.get_open_position(signal.symbol) is not None:
            return RiskDecision(False, "There is already an open position for this symbol.")

        if self.store.count_open_positions() >= self.config.max_open_positions:
            return RiskDecision(False, "Maximum number of open positions reached.")

        stop_pct = abs(signal.entry_price - signal.stop_price) / signal.entry_price
        if stop_pct > self.config.max_stop_pct:
            return RiskDecision(False, "Stop distance exceeds configured maximum.")

        if signal.rr < self.config.min_rr:
            return RiskDecision(False, "Reward/risk is below the configured minimum.")

        if self.config.ai_validation and review.confidence < self.config.min_ai_confidence:
            return RiskDecision(False, "AI confidence is below the configured minimum.")

        daily_pnl = self.store.get_today_realized_pnl()
        if daily_pnl <= (-1 * self.config.max_daily_loss):
            return RiskDecision(False, "Daily loss limit reached.")

        if self.config.mode == "paper":
            open_exposure = self.store.get_open_exposure()
            free_capital = self.config.paper_start_balance + self.store.get_summary()["realized_pnl"] - open_exposure
            if free_capital < self.config.notional_per_trade:
                return RiskDecision(False, "Paper balance is not large enough for another trade.")

        return RiskDecision(True, "Trade allowed.")
