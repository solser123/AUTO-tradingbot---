from __future__ import annotations

import ccxt
import pandas as pd

from .config import BotConfig


class BinanceExchange:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        options = {
            "defaultType": config.market_type,
            "adjustForTimeDifference": True,
            "recvWindow": 10000,
        }
        self.client = ccxt.binance(
            {
                "apiKey": config.api_key,
                "secret": config.secret_key,
                "enableRateLimit": True,
                "options": options,
            }
        )
        self.client.load_time_difference()
        self.client.load_markets()
        if self.config.is_futures:
            self._configure_futures_risk_profile()

    def resolve_symbols(self, symbols: list[str]) -> list[str]:
        if not self.config.is_futures:
            return symbols

        if len(symbols) == 1 and symbols[0].strip().upper() == "ALL":
            volumes: dict[str, float] = {}
            try:
                for item in self.client.fapiPublicGetTicker24hr():
                    symbol_id = item.get("symbol", "")
                    if not symbol_id.endswith("USDT"):
                        continue
                    market_symbol = f"{symbol_id[:-4]}/USDT:USDT"
                    volumes[market_symbol] = float(item.get("quoteVolume") or 0.0)
            except Exception:
                volumes = {}

            markets = [
                market["symbol"]
                for market in self.client.markets.values()
                if market.get("swap")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            ]
            return sorted(set(markets), key=lambda item: (-volumes.get(item, 0.0), item))

        resolved: list[str] = []
        for symbol in symbols:
            if symbol in self.client.markets:
                resolved.append(symbol)
                continue
            if ":" not in symbol and "/" in symbol:
                base, quote = symbol.split("/", 1)
                normalized = f"{base}/{quote}:{quote}"
                if normalized in self.client.markets:
                    resolved.append(normalized)
                    continue
            resolved.append(symbol)
        return resolved

    def _configure_futures_risk_profile(self) -> None:
        if len(self.config.symbols) == 1 and self.config.symbols[0].strip().upper() == "ALL":
            return

        for symbol in self.resolve_symbols(self.config.symbols):
            self._configure_symbol_risk_profile(symbol)

    def _configure_symbol_risk_profile(self, symbol: str) -> None:
        try:
            self.client.set_margin_mode(
                self.config.futures_margin_mode,
                symbol,
                {"leverage": self.config.futures_leverage},
            )
        except Exception:
            # Binance returns an error if the margin mode is already set.
            pass
        try:
            self.client.set_leverage(self.config.futures_leverage, symbol)
        except Exception:
            # Some symbols or account states can reject leverage updates; the order path will surface issues later.
            pass

    def fetch_balance(self) -> dict:
        if self.config.is_futures:
            return self.client.fetch_balance({"type": "swap"})
        return self.client.fetch_balance()

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 250) -> pd.DataFrame:
        candles = self.client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def fetch_last_price(self, symbol: str) -> float:
        return float(self.client.fetch_ticker(symbol)["last"])

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        return float(self.client.amount_to_precision(symbol, amount))

    def create_market_order(self, symbol: str, side: str, amount: float, reduce_only: bool = False) -> dict:
        params = {}
        if self.config.is_futures:
            self._configure_symbol_risk_profile(symbol)
        if self.config.is_futures and reduce_only:
            params["reduceOnly"] = True
        return self.client.create_order(symbol=symbol, type="market", side=side, amount=amount, params=params)
