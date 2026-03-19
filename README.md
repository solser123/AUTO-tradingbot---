# Binance Bot V2 Foundation

This repository now holds the basic framework for a simpler Binance trading bot.

## Core principle

The overall flow should stay stable even if we later change the entry rules.

1. Load config
2. Fetch market data
3. Build a rule-based candidate signal
4. Ask AI to validate only that candidate
5. Apply risk rules
6. Execute in `paper` or `live` mode
7. Store signals and positions in SQLite

## Baseline entry and exit rules

The first version uses a clear trend-following structure:

- Higher timeframe trend must agree with the execution timeframe
- Price must be above VWAP for longs
- EMA20 must be above EMA50 for longs
- RSI must stay in a healthy trend range
- Entry comes only after pullback recovery or a breakout with volume
- Stop loss is based on recent swing plus ATR protection
- Target uses a fixed reward/risk multiple
- Exit happens on stop loss, target hit, or max holding time

This keeps the framework consistent while allowing us to improve only the strategy module later.

## Files

- `main.py`: CLI entrypoint
- `binance_bot/config.py`: environment-based configuration
- `binance_bot/exchange.py`: Binance market access
- `binance_bot/strategy.py`: baseline entry/exit rules
- `binance_bot/ai_validator.py`: AI review layer
- `binance_bot/storage.py`: SQLite persistence
- `binance_bot/engine.py`: bot runtime loop

## Run

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
copy .env.example .env
python main.py --doctor
python main.py --once
```

`BOT_MODE=paper` is the default and is the recommended starting mode.

For Binance USDT-M perpetuals, use `BOT_MARKET_TYPE=usdm`. Symbols can stay in the simple form like `BTC/USDT`; the bot will normalize them to the futures symbol format internally.
For safer live tests, you can pin futures risk with `BOT_FUTURES_MARGIN_MODE=isolated` and `BOT_FUTURES_LEVERAGE=1`.

## Commands

- `python main.py --doctor`: validate config and dependency readiness
- `python main.py --balance`: show the configured exchange balance (`spot` or `USDT-M futures`)
- `python main.py --demo`: send a Telegram startup message for the current paper/live configuration
- `python main.py --rank`: rank today's candidate symbols and print a recommended watchlist
- `python main.py --once`: run one cycle
- `python main.py --summary`: show stored signal and position stats
- `python main.py --scan`: inspect current market metrics and why a signal was rejected
- `python main.py --backtest`: run a lightweight historical check of the current strategy
- `python main.py --optimize`: test a few nearby parameter combinations and rank them

In `paper` mode the bot also tracks:

- starting balance
- capital currently tied in open positions
- realized PnL
- estimated paper equity
