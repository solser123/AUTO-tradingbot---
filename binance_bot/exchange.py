from __future__ import annotations

import ccxt
import pandas as pd

from .config import BotConfig


class BinanceExchange:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        options = {"defaultType": config.market_type}
        self.client = ccxt.binance(
            {
                "apiKey": config.api_key,
                "secret": config.secret_key,
                "enableRateLimit": True,
                "options": options,
            }
        )
        self.client.load_markets()

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 250) -> pd.DataFrame:
        candles = self.client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def fetch_last_price(self, symbol: str) -> float:
        return float(self.client.fetch_ticker(symbol)["last"])

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        return float(self.client.amount_to_precision(symbol, amount))

    def create_market_order(self, symbol: str, side: str, amount: float) -> dict:
        return self.client.create_order(symbol=symbol, type="market", side=side, amount=amount)
