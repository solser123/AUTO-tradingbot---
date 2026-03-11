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


@dataclass(frozen=True)
class BotConfig:
    api_key: str
    secret_key: str
    openai_api_key: str
    telegram_token: str
    telegram_chat_id: str
    mode: str
    market_type: str
    symbols: list[str]
    timeframe: str
    higher_timeframe: str
    loop_seconds: int
    notional_per_trade: float
    max_open_positions: int
    allow_short: bool
    min_rr: float
    max_stop_pct: float
    max_hold_minutes: int
    ai_validation: bool
    min_ai_confidence: float
    max_daily_loss: float
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
    database_path: str
    ai_model: str

    @classmethod
    def from_env(cls) -> "BotConfig":
        load_dotenv()
        mode = os.getenv("BOT_MODE", "paper").strip().lower()
        if mode not in {"paper", "live"}:
            raise ValueError("BOT_MODE must be either 'paper' or 'live'.")

        symbols = [item.strip() for item in os.getenv("BOT_SYMBOLS", "BTC/USDT").split(",") if item.strip()]
        if not symbols:
            raise ValueError("BOT_SYMBOLS must include at least one symbol.")

        config = cls(
            api_key=os.getenv("BINANCE_API_KEY", "").strip(),
            secret_key=os.getenv("BINANCE_SECRET_KEY", "").strip(),
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            mode=mode,
            market_type=os.getenv("BOT_MARKET_TYPE", "spot").strip().lower(),
            symbols=symbols,
            timeframe=os.getenv("BOT_TIMEFRAME", "15m").strip(),
            higher_timeframe=os.getenv("BOT_HIGHER_TIMEFRAME", "1h").strip(),
            loop_seconds=_as_int("BOT_LOOP_SECONDS", 60),
            notional_per_trade=_as_float("BOT_NOTIONAL_PER_TRADE", 100.0),
            max_open_positions=_as_int("BOT_MAX_OPEN_POSITIONS", 2),
            allow_short=_as_bool(os.getenv("BOT_ALLOW_SHORT"), False),
            min_rr=_as_float("BOT_MIN_RR", 1.8),
            max_stop_pct=_as_float("BOT_MAX_STOP_PCT", 0.025),
            max_hold_minutes=_as_int("BOT_MAX_HOLD_MINUTES", 720),
            ai_validation=_as_bool(os.getenv("BOT_AI_VALIDATION"), True),
            min_ai_confidence=_as_float("BOT_MIN_AI_CONFIDENCE", 0.55),
            max_daily_loss=_as_float("BOT_MAX_DAILY_LOSS", 50.0),
            paper_start_balance=_as_float("BOT_PAPER_START_BALANCE", 1000.0),
            backtest_limit=_as_int("BOT_BACKTEST_LIMIT", 300),
            long_rsi_min=_as_float("BOT_LONG_RSI_MIN", 52.0),
            long_rsi_max=_as_float("BOT_LONG_RSI_MAX", 68.0),
            short_rsi_min=_as_float("BOT_SHORT_RSI_MIN", 32.0),
            short_rsi_max=_as_float("BOT_SHORT_RSI_MAX", 48.0),
            min_volume_ratio=_as_float("BOT_MIN_VOLUME_RATIO", 1.1),
            breakout_lookback=_as_int("BOT_BREAKOUT_LOOKBACK", 20),
            pullback_tolerance=_as_float("BOT_PULLBACK_TOLERANCE", 0.002),
            atr_stop_multiplier=_as_float("BOT_ATR_STOP_MULTIPLIER", 1.2),
            database_path=os.getenv("BOT_DATABASE_PATH", "bot_state.db").strip(),
            ai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip(),
        )

        if config.mode == "live" and (not config.api_key or not config.secret_key):
            raise ValueError("Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY.")
        if config.mode == "live" and config.market_type == "spot" and config.allow_short:
            raise ValueError("Spot live mode does not support BOT_ALLOW_SHORT=true in this foundation.")

        return config
