from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:
        return None


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def _as_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _as_list(name: str) -> list[str]:
    value = os.getenv(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_market_type(value: str | None) -> str:
    raw = (value or "spot").strip().lower()
    aliases = {
        "spot": "spot",
        "swap": "swap",
        "future": "swap",
        "futures": "swap",
        "usdm": "swap",
        "usdt-m": "swap",
        "usdtm": "swap",
        "usdm_futures": "swap",
        "usdtm_futures": "swap",
    }
    normalized = aliases.get(raw)
    if normalized is None:
        raise ValueError("BOT_MARKET_TYPE must be one of: spot, swap, future, futures, usdm, usdt-m.")
    return normalized


def _normalize_symbol(symbol: str, market_type: str) -> str:
    cleaned = symbol.strip()
    if market_type != "swap" or ":" in cleaned or "/" not in cleaned:
        return cleaned

    base, quote = cleaned.split("/", 1)
    return f"{base}/{quote}:{quote}"


@dataclass(frozen=True)
class BotConfig:
    api_key: str
    secret_key: str
    openai_api_key: str
    coinglass_api_key: str
    telegram_token: str
    telegram_chat_id: str
    mode: str
    market_type: str
    symbols: list[str]
    main_symbols: list[str]
    research_symbols: list[str]
    overflow_symbols: list[str]
    candidate_symbols: list[str]
    stage1_symbols: list[str]
    stage2_symbols: list[str]
    stage3_symbols: list[str]
    stage4_symbols: list[str]
    timeframe: str
    higher_timeframe: str
    medium_timeframe: str
    medium_higher_timeframe: str
    long_timeframe: str
    long_higher_timeframe: str
    loop_seconds: int
    notional_per_trade: float
    stage1_notional: float
    stage2_notional: float
    stage3_notional: float
    stage4_notional: float
    max_open_positions: int
    futures_leverage: int
    core_symbols: list[str]
    core_leverage: int
    liquid_leverage: int
    experimental_x10_symbols: list[str]
    experimental_x20_symbols: list[str]
    enable_overflow_review: bool
    overflow_scan_limit: int
    overflow_min_score: float
    futures_margin_mode: str
    allow_short: bool
    min_rr: float
    max_stop_pct: float
    max_hold_minutes: int
    allowed_entry_windows: list[str]
    symbol_cooldown_minutes: int
    ai_validation: bool
    ai_scan_assist: bool
    ai_scan_hourly_budget_total: int
    ai_scan_hourly_budget_per_symbol: int
    ai_scan_min_confidence: float
    ai_scan_trigger_score: float
    min_ai_confidence: float
    stage1_min_ai_confidence: float
    stage2_min_ai_confidence: float
    stage3_min_ai_confidence: float
    stage4_min_ai_confidence: float
    ai_review_hourly_budget_total: int
    ai_review_hourly_budget_per_symbol: int
    max_daily_loss: float
    max_daily_loss_pct: float
    max_weekly_loss_pct: float
    hard_stop_equity_floor_pct: float
    max_trade_risk_pct: float
    max_correlated_positions: int
    same_symbol_stoploss_limit: int
    global_stoploss_limit: int
    exchange_failure_limit: int
    ai_failure_limit: int
    enable_ai_position_manager: bool
    ai_position_manage_interval_minutes: int
    ai_position_manage_min_age_minutes: int
    ai_position_min_confidence: float
    ai_position_exploratory_min_confidence: float
    ai_position_daily_profit_target_pct: float
    ai_position_target_raise_step_r: float
    ai_position_target_raise_cap_r: float
    monthly_living_cost_krw: float
    usdkrw_reference_rate: float
    enable_exploratory_live: bool
    exploratory_ai_min_confidence: float
    exploratory_ai_scan_min_confidence: float
    exploratory_followthrough_bars: int
    exploratory_min_progress_r: float
    signal_max_age_aggressive_seconds: int
    signal_max_age_exploratory_seconds: int
    enable_hot_mover_scout: bool
    hot_mover_scan_limit: int
    hot_mover_min_24h_pct: float
    hot_mover_min_quote_volume: float
    hot_mover_notional: float
    hot_mover_leverage: int
    hot_mover_max_positions: int
    hot_mover_allow_shorts: bool
    max_slippage_pct: float
    atr_overheat_multiplier: float
    aggressive_entry_score: float
    balanced_entry_score: float
    conservative_entry_score: float
    balanced_defense_r_multiple: float
    conservative_defense_r_multiple: float
    enable_context_recovery: bool
    context_recovery_external_min: float
    context_recovery_external_count_min: int
    enable_sector_flow: bool
    sector_sync_interval_minutes: int
    sector_flow_positive_threshold: float
    sector_flow_negative_threshold: float
    sector_opposition_gate_threshold: float
    sector_alignment_notional_boost_pct: float
    sector_alignment_ai_relief: float
    sector_min_liquidity_usdt: float
    enable_microstructure_filter: bool
    microstructure_orderbook_depth: int
    microstructure_max_spread_pct: float
    microstructure_min_total_depth_usdt: float
    microstructure_flow_gate_threshold: float
    microstructure_imbalance_gate_threshold: float
    microstructure_trade_limit: int
    sizing_risk_pct_full: float
    sizing_risk_pct_high: float
    sizing_risk_pct_medium: float
    sizing_risk_pct_low: float
    sizing_max_total_open_risk_pct: float
    sizing_max_same_sector_open_risk_pct: float
    paper_start_balance: float
    backtest_limit: int
    long_rsi_min: float
    long_rsi_max: float
    short_rsi_min: float
    short_rsi_max: float
    min_volume_ratio: float
    breakout_lookback: int
    pullback_tolerance: float
    atr_stop_multiplier: float
    short_stoch_min: float
    short_stoch_max: float
    long_stoch_min: float
    long_stoch_max: float
    require_signal_candle_confirmation: bool
    opportunity_lookahead_minutes: int
    opportunity_min_move_pct: float
    opportunity_sync_interval_minutes: int
    database_path: str
    ai_model: str

    @property
    def is_futures(self) -> bool:
        return self.market_type == "swap"

    def active_symbols(self) -> list[str]:
        if self.mode == "live":
            live_symbols = self.live_symbols()
            if live_symbols:
                return live_symbols
            if self.main_symbols:
                return self.main_symbols
        if self.mode == "paper" and self.research_symbols:
            return self.research_symbols
        return self.symbols

    def live_symbols(self) -> list[str]:
        ordered = self.stage1_symbols + self.stage2_symbols + self.stage3_symbols + self.stage4_symbols
        if ordered:
            return list(dict.fromkeys(ordered))
        return self.main_symbols

    def stage_for_symbol(self, symbol: str) -> int:
        if symbol in self.stage1_symbols:
            return 1
        if symbol in self.stage2_symbols:
            return 2
        if symbol in self.stage3_symbols:
            return 3
        if symbol in self.stage4_symbols:
            return 4
        if symbol in self.core_symbols:
            return 1
        if symbol in self.experimental_x10_symbols or symbol in self.experimental_x20_symbols:
            return 4
        if symbol in self.main_symbols:
            return 2
        return 3

    def stage_notional(self, symbol: str) -> float:
        stage = self.stage_for_symbol(symbol)
        if stage == 1:
            return self.stage1_notional
        if stage == 2:
            return self.stage2_notional
        if stage == 3:
            return self.stage3_notional
        return self.stage4_notional

    def min_ai_confidence_for_symbol(self, symbol: str) -> float:
        stage = self.stage_for_symbol(symbol)
        if stage == 1:
            return self.stage1_min_ai_confidence
        if stage == 2:
            return self.stage2_min_ai_confidence
        if stage == 3:
            return self.stage3_min_ai_confidence
        return self.stage4_min_ai_confidence

    def leverage_for_symbol(self, symbol: str) -> int:
        if symbol in self.experimental_x20_symbols:
            return 20
        if symbol in self.experimental_x10_symbols:
            return 10
        if symbol in self.core_symbols:
            return self.core_leverage
        if self.stage_for_symbol(symbol) == 3 and self.is_futures:
            return max(self.liquid_leverage, 3)
        if self.stage_for_symbol(symbol) == 4 and self.is_futures:
            return max(self.liquid_leverage, 5)
        return self.liquid_leverage if self.is_futures else 1

    def is_experimental_symbol(self, symbol: str) -> bool:
        return symbol in self.experimental_x10_symbols or symbol in self.experimental_x20_symbols

    @classmethod
    def from_env(cls) -> "BotConfig":
        try:
            load_dotenv(override=True)
        except TypeError:
            load_dotenv()
        mode = os.getenv("BOT_MODE", "paper").strip().lower()
        if mode not in {"paper", "live"}:
            raise ValueError("BOT_MODE must be either 'paper' or 'live'.")

        market_type = _normalize_market_type(os.getenv("BOT_MARKET_TYPE", "spot"))
        symbols = [
            _normalize_symbol(item, market_type)
            for item in os.getenv("BOT_SYMBOLS", "BTC/USDT").split(",")
            if item.strip()
        ]
        main_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_MAIN_SYMBOLS")
        ]
        research_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_RESEARCH_SYMBOLS")
        ]
        stage1_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_STAGE1_SYMBOLS")
        ]
        stage2_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_STAGE2_SYMBOLS")
        ]
        stage3_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_STAGE3_SYMBOLS")
        ]
        stage4_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_STAGE4_SYMBOLS")
        ]
        overflow_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_OVERFLOW_SYMBOLS")
        ]
        candidate_symbols = [
            _normalize_symbol(item, market_type)
            for item in os.getenv("BOT_CANDIDATE_SYMBOLS", "").split(",")
            if item.strip()
        ]
        core_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_CORE_SYMBOLS")
        ]
        experimental_x10_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_EXPERIMENTAL_X10_SYMBOLS")
        ]
        experimental_x20_symbols = [
            _normalize_symbol(item, market_type)
            for item in _as_list("BOT_EXPERIMENTAL_X20_SYMBOLS")
        ]
        if not symbols:
            raise ValueError("BOT_SYMBOLS must include at least one symbol.")

        if not stage1_symbols and core_symbols:
            stage1_symbols = core_symbols[:]
        if not stage4_symbols and (experimental_x10_symbols or experimental_x20_symbols):
            stage4_symbols = list(dict.fromkeys(experimental_x10_symbols + experimental_x20_symbols))
        if not stage2_symbols and main_symbols:
            stage2_symbols = [
                symbol
                for symbol in main_symbols
                if symbol not in stage1_symbols and symbol not in stage4_symbols
            ]
        if not stage3_symbols:
            stage3_symbols = [
                symbol
                for symbol in candidate_symbols + overflow_symbols
                if symbol not in stage1_symbols and symbol not in stage2_symbols and symbol not in stage4_symbols
            ]

        config = cls(
            api_key=os.getenv("BINANCE_API_KEY", "").strip(),
            secret_key=os.getenv("BINANCE_SECRET_KEY", "").strip(),
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            coinglass_api_key=os.getenv("COINGLASS_API_KEY", "").strip(),
            telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            mode=mode,
            market_type=market_type,
            symbols=symbols,
            main_symbols=main_symbols,
            research_symbols=research_symbols,
            overflow_symbols=overflow_symbols,
            candidate_symbols=candidate_symbols,
            stage1_symbols=list(dict.fromkeys(stage1_symbols)),
            stage2_symbols=list(dict.fromkeys(stage2_symbols)),
            stage3_symbols=list(dict.fromkeys(stage3_symbols)),
            stage4_symbols=list(dict.fromkeys(stage4_symbols)),
            timeframe=os.getenv("BOT_TIMEFRAME", "15m").strip(),
            higher_timeframe=os.getenv("BOT_HIGHER_TIMEFRAME", "1h").strip(),
            medium_timeframe=os.getenv("BOT_MEDIUM_TIMEFRAME", "1h").strip(),
            medium_higher_timeframe=os.getenv("BOT_MEDIUM_HIGHER_TIMEFRAME", "4h").strip(),
            long_timeframe=os.getenv("BOT_LONG_TIMEFRAME", "4h").strip(),
            long_higher_timeframe=os.getenv("BOT_LONG_HIGHER_TIMEFRAME", "1d").strip(),
            loop_seconds=_as_int("BOT_LOOP_SECONDS", 60),
            notional_per_trade=_as_float("BOT_NOTIONAL_PER_TRADE", 100.0),
            stage1_notional=_as_float("BOT_STAGE1_NOTIONAL", 100.0),
            stage2_notional=_as_float("BOT_STAGE2_NOTIONAL", 20.0),
            stage3_notional=_as_float("BOT_STAGE3_NOTIONAL", 10.0),
            stage4_notional=_as_float("BOT_STAGE4_NOTIONAL", 5.0),
            max_open_positions=_as_int("BOT_MAX_OPEN_POSITIONS", 2),
            futures_leverage=_as_int("BOT_FUTURES_LEVERAGE", 1),
            core_symbols=core_symbols,
            core_leverage=_as_int("BOT_CORE_LEVERAGE", 3),
            liquid_leverage=_as_int("BOT_LIQUID_LEVERAGE", 2),
            experimental_x10_symbols=experimental_x10_symbols,
            experimental_x20_symbols=experimental_x20_symbols,
            enable_overflow_review=_as_bool(os.getenv("BOT_ENABLE_OVERFLOW_REVIEW"), True),
            overflow_scan_limit=_as_int("BOT_OVERFLOW_SCAN_LIMIT", 5),
            overflow_min_score=_as_float("BOT_OVERFLOW_MIN_SCORE", 6.0),
            futures_margin_mode=os.getenv("BOT_FUTURES_MARGIN_MODE", "isolated").strip().lower(),
            allow_short=_as_bool(os.getenv("BOT_ALLOW_SHORT"), False),
            min_rr=_as_float("BOT_MIN_RR", 1.5),
            max_stop_pct=_as_float("BOT_MAX_STOP_PCT", 0.025),
            max_hold_minutes=_as_int("BOT_MAX_HOLD_MINUTES", 720),
            allowed_entry_windows=_as_list("BOT_ALLOWED_ENTRY_WINDOWS"),
            symbol_cooldown_minutes=_as_int("BOT_SYMBOL_COOLDOWN_MINUTES", 240),
            ai_validation=_as_bool(os.getenv("BOT_AI_VALIDATION"), True),
            ai_scan_assist=_as_bool(os.getenv("BOT_AI_SCAN_ASSIST"), True),
            ai_scan_hourly_budget_total=_as_int("BOT_AI_SCAN_HOURLY_BUDGET_TOTAL", 240),
            ai_scan_hourly_budget_per_symbol=_as_int("BOT_AI_SCAN_HOURLY_BUDGET_PER_SYMBOL", 2),
            ai_scan_min_confidence=_as_float("BOT_AI_SCAN_MIN_CONFIDENCE", 0.58),
            ai_scan_trigger_score=_as_float("BOT_AI_SCAN_TRIGGER_SCORE", 4.0),
            min_ai_confidence=_as_float("BOT_MIN_AI_CONFIDENCE", 0.55),
            stage1_min_ai_confidence=_as_float("BOT_STAGE1_MIN_AI_CONFIDENCE", 0.60),
            stage2_min_ai_confidence=_as_float("BOT_STAGE2_MIN_AI_CONFIDENCE", 0.55),
            stage3_min_ai_confidence=_as_float("BOT_STAGE3_MIN_AI_CONFIDENCE", 0.50),
            stage4_min_ai_confidence=_as_float("BOT_STAGE4_MIN_AI_CONFIDENCE", 0.46),
            ai_review_hourly_budget_total=_as_int("BOT_AI_REVIEW_HOURLY_BUDGET_TOTAL", 120),
            ai_review_hourly_budget_per_symbol=_as_int("BOT_AI_REVIEW_HOURLY_BUDGET_PER_SYMBOL", 2),
            max_daily_loss=_as_float("BOT_MAX_DAILY_LOSS", 50.0),
            max_daily_loss_pct=_as_float("BOT_MAX_DAILY_LOSS_PCT", 0.10),
            max_weekly_loss_pct=_as_float("BOT_MAX_WEEKLY_LOSS_PCT", 0.10),
            hard_stop_equity_floor_pct=_as_float("BOT_HARD_STOP_EQUITY_FLOOR_PCT", 0.30),
            max_trade_risk_pct=_as_float("BOT_MAX_TRADE_RISK_PCT", 0.02),
            max_correlated_positions=_as_int("BOT_MAX_CORRELATED_POSITIONS", 1),
            same_symbol_stoploss_limit=_as_int("BOT_SAME_SYMBOL_STOPLOSS_LIMIT", 5),
            global_stoploss_limit=_as_int("BOT_GLOBAL_STOPLOSS_LIMIT", 5),
            exchange_failure_limit=_as_int("BOT_EXCHANGE_FAILURE_LIMIT", 3),
            ai_failure_limit=_as_int("BOT_AI_FAILURE_LIMIT", 3),
            enable_ai_position_manager=_as_bool(os.getenv("BOT_ENABLE_AI_POSITION_MANAGER"), True),
            ai_position_manage_interval_minutes=_as_int("BOT_AI_POSITION_MANAGE_INTERVAL_MINUTES", 15),
            ai_position_manage_min_age_minutes=_as_int("BOT_AI_POSITION_MANAGE_MIN_AGE_MINUTES", 15),
            ai_position_min_confidence=_as_float("BOT_AI_POSITION_MIN_CONFIDENCE", 0.52),
            ai_position_exploratory_min_confidence=_as_float("BOT_AI_POSITION_EXPLORATORY_MIN_CONFIDENCE", 0.44),
            ai_position_daily_profit_target_pct=_as_float("BOT_AI_POSITION_DAILY_PROFIT_TARGET_PCT", 0.01),
            ai_position_target_raise_step_r=_as_float("BOT_AI_POSITION_TARGET_RAISE_STEP_R", 0.25),
            ai_position_target_raise_cap_r=_as_float("BOT_AI_POSITION_TARGET_RAISE_CAP_R", 0.75),
            monthly_living_cost_krw=_as_float("BOT_MONTHLY_LIVING_COST_KRW", 0.0),
            usdkrw_reference_rate=_as_float("BOT_USDKRW_REFERENCE_RATE", 1480.0),
            enable_exploratory_live=_as_bool(os.getenv("BOT_ENABLE_EXPLORATORY_LIVE"), True),
            exploratory_ai_min_confidence=_as_float("BOT_EXPLORATORY_AI_MIN_CONFIDENCE", 0.42),
            exploratory_ai_scan_min_confidence=_as_float("BOT_EXPLORATORY_AI_SCAN_MIN_CONFIDENCE", 0.52),
            exploratory_followthrough_bars=_as_int("BOT_EXPLORATORY_FOLLOWTHROUGH_BARS", 3),
            exploratory_min_progress_r=_as_float("BOT_EXPLORATORY_MIN_PROGRESS_R", 0.15),
            signal_max_age_aggressive_seconds=_as_int("BOT_SIGNAL_MAX_AGE_AGGRESSIVE_SECONDS", 180),
            signal_max_age_exploratory_seconds=_as_int("BOT_SIGNAL_MAX_AGE_EXPLORATORY_SECONDS", 420),
            enable_hot_mover_scout=_as_bool(os.getenv("BOT_ENABLE_HOT_MOVER_SCOUT"), True),
            hot_mover_scan_limit=_as_int("BOT_HOT_MOVER_SCAN_LIMIT", 4),
            hot_mover_min_24h_pct=_as_float("BOT_HOT_MOVER_MIN_24H_PCT", 18.0),
            hot_mover_min_quote_volume=_as_float("BOT_HOT_MOVER_MIN_QUOTE_VOLUME", 3000000.0),
            hot_mover_notional=_as_float("BOT_HOT_MOVER_NOTIONAL", 5.0),
            hot_mover_leverage=_as_int("BOT_HOT_MOVER_LEVERAGE", 10),
            hot_mover_max_positions=_as_int("BOT_HOT_MOVER_MAX_POSITIONS", 1),
            hot_mover_allow_shorts=_as_bool(os.getenv("BOT_HOT_MOVER_ALLOW_SHORTS"), True),
            max_slippage_pct=_as_float("BOT_MAX_SLIPPAGE_PCT", 0.0025),
            atr_overheat_multiplier=_as_float("BOT_ATR_OVERHEAT_MULTIPLIER", 2.5),
            aggressive_entry_score=_as_float("BOT_AGGRESSIVE_ENTRY_SCORE", 0.68),
            balanced_entry_score=_as_float("BOT_BALANCED_ENTRY_SCORE", 0.54),
            conservative_entry_score=_as_float("BOT_CONSERVATIVE_ENTRY_SCORE", 0.42),
            balanced_defense_r_multiple=_as_float("BOT_BALANCED_DEFENSE_R_MULTIPLE", 0.50),
            conservative_defense_r_multiple=_as_float("BOT_CONSERVATIVE_DEFENSE_R_MULTIPLE", 0.85),
            enable_context_recovery=_as_bool(os.getenv("BOT_ENABLE_CONTEXT_RECOVERY"), True),
            context_recovery_external_min=_as_float("BOT_CONTEXT_RECOVERY_EXTERNAL_MIN", 0.10),
            context_recovery_external_count_min=_as_int("BOT_CONTEXT_RECOVERY_EXTERNAL_COUNT_MIN", 4),
            enable_sector_flow=_as_bool(os.getenv("BOT_ENABLE_SECTOR_FLOW"), True),
            sector_sync_interval_minutes=_as_int("BOT_SECTOR_SYNC_INTERVAL_MINUTES", 15),
            sector_flow_positive_threshold=_as_float("BOT_SECTOR_FLOW_POSITIVE_THRESHOLD", 0.18),
            sector_flow_negative_threshold=_as_float("BOT_SECTOR_FLOW_NEGATIVE_THRESHOLD", -0.18),
            sector_opposition_gate_threshold=_as_float("BOT_SECTOR_OPPOSITION_GATE_THRESHOLD", 0.30),
            sector_alignment_notional_boost_pct=_as_float("BOT_SECTOR_ALIGNMENT_NOTIONAL_BOOST_PCT", 0.15),
            sector_alignment_ai_relief=_as_float("BOT_SECTOR_ALIGNMENT_AI_RELIEF", 0.03),
            sector_min_liquidity_usdt=_as_float("BOT_SECTOR_MIN_LIQUIDITY_USDT", 10000000.0),
            enable_microstructure_filter=_as_bool(os.getenv("BOT_ENABLE_MICROSTRUCTURE_FILTER"), True),
            microstructure_orderbook_depth=_as_int("BOT_MICROSTRUCTURE_ORDERBOOK_DEPTH", 15),
            microstructure_max_spread_pct=_as_float("BOT_MICROSTRUCTURE_MAX_SPREAD_PCT", 0.0015),
            microstructure_min_total_depth_usdt=_as_float("BOT_MICROSTRUCTURE_MIN_TOTAL_DEPTH_USDT", 15000.0),
            microstructure_flow_gate_threshold=_as_float("BOT_MICROSTRUCTURE_FLOW_GATE_THRESHOLD", 0.18),
            microstructure_imbalance_gate_threshold=_as_float("BOT_MICROSTRUCTURE_IMBALANCE_GATE_THRESHOLD", 0.18),
            microstructure_trade_limit=_as_int("BOT_MICROSTRUCTURE_TRADE_LIMIT", 40),
            sizing_risk_pct_full=_as_float("BOT_SIZING_RISK_PCT_FULL", 0.0075),
            sizing_risk_pct_high=_as_float("BOT_SIZING_RISK_PCT_HIGH", 0.0060),
            sizing_risk_pct_medium=_as_float("BOT_SIZING_RISK_PCT_MEDIUM", 0.0045),
            sizing_risk_pct_low=_as_float("BOT_SIZING_RISK_PCT_LOW", 0.0035),
            sizing_max_total_open_risk_pct=_as_float("BOT_SIZING_MAX_TOTAL_OPEN_RISK_PCT", 0.018),
            sizing_max_same_sector_open_risk_pct=_as_float("BOT_SIZING_MAX_SAME_SECTOR_OPEN_RISK_PCT", 0.009),
            paper_start_balance=_as_float("BOT_PAPER_START_BALANCE", 1000.0),
            backtest_limit=_as_int("BOT_BACKTEST_LIMIT", 300),
            long_rsi_min=_as_float("BOT_LONG_RSI_MIN", 46.0),
            long_rsi_max=_as_float("BOT_LONG_RSI_MAX", 78.0),
            short_rsi_min=_as_float("BOT_SHORT_RSI_MIN", 34.0),
            short_rsi_max=_as_float("BOT_SHORT_RSI_MAX", 68.0),
            min_volume_ratio=_as_float("BOT_MIN_VOLUME_RATIO", 0.45),
            breakout_lookback=_as_int("BOT_BREAKOUT_LOOKBACK", 20),
            pullback_tolerance=_as_float("BOT_PULLBACK_TOLERANCE", 0.002),
            atr_stop_multiplier=_as_float("BOT_ATR_STOP_MULTIPLIER", 1.2),
            short_stoch_min=_as_float("BOT_SHORT_STOCH_MIN", 25.0),
            short_stoch_max=_as_float("BOT_SHORT_STOCH_MAX", 85.0),
            long_stoch_min=_as_float("BOT_LONG_STOCH_MIN", 15.0),
            long_stoch_max=_as_float("BOT_LONG_STOCH_MAX", 75.0),
            require_signal_candle_confirmation=_as_bool(os.getenv("BOT_REQUIRE_SIGNAL_CANDLE_CONFIRMATION"), False),
            opportunity_lookahead_minutes=_as_int("BOT_OPPORTUNITY_LOOKAHEAD_MINUTES", 240),
            opportunity_min_move_pct=_as_float("BOT_OPPORTUNITY_MIN_MOVE_PCT", 1.0),
            opportunity_sync_interval_minutes=_as_int("BOT_OPPORTUNITY_SYNC_INTERVAL_MINUTES", 60),
            database_path=os.getenv("BOT_DATABASE_PATH", "bot_state.db").strip(),
            ai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip(),
        )

        if config.mode == "live" and (not config.api_key or not config.secret_key):
            raise ValueError("Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY.")
        if config.mode == "live" and config.market_type == "spot" and config.allow_short:
            raise ValueError("Spot live mode does not support BOT_ALLOW_SHORT=true in this foundation.")
        if config.mode == "live" and not config.live_symbols():
            raise ValueError("Live mode requires at least one live stage symbol to be configured.")
        if config.is_futures and config.futures_margin_mode not in {"isolated", "cross"}:
            raise ValueError("BOT_FUTURES_MARGIN_MODE must be either 'isolated' or 'cross'.")
        if config.is_futures and config.futures_leverage < 1:
            raise ValueError("BOT_FUTURES_LEVERAGE must be at least 1.")
        if config.core_leverage < 1 or config.liquid_leverage < 1:
            raise ValueError("Leverage values must be at least 1.")
        if config.hot_mover_leverage < 1:
            raise ValueError("BOT_HOT_MOVER_LEVERAGE must be at least 1.")
        if config.hot_mover_scan_limit < 0 or config.hot_mover_max_positions < 0:
            raise ValueError("Hot mover scout limits must not be negative.")
        if config.max_open_positions < 1 or config.max_open_positions > 6:
            raise ValueError("BOT_MAX_OPEN_POSITIONS must be between 1 and 6.")
        if config.overflow_scan_limit < 0:
            raise ValueError("BOT_OVERFLOW_SCAN_LIMIT must not be negative.")
        if (
            config.ai_scan_hourly_budget_total < 1
            or config.ai_scan_hourly_budget_per_symbol < 1
            or config.ai_review_hourly_budget_total < 1
            or config.ai_review_hourly_budget_per_symbol < 1
        ):
            raise ValueError("AI hourly budget values must be at least 1.")
        if config.signal_max_age_aggressive_seconds < 30 or config.signal_max_age_exploratory_seconds < 30:
            raise ValueError("Signal freshness limits must be at least 30 seconds.")
        if config.max_daily_loss_pct <= 0 or config.max_weekly_loss_pct <= 0:
            raise ValueError("Loss percentage limits must be positive.")
        if not (
            config.aggressive_entry_score >= config.balanced_entry_score >= config.conservative_entry_score >= 0
        ):
            raise ValueError("Entry profile scores must descend: aggressive >= balanced >= conservative >= 0.")
        return config
