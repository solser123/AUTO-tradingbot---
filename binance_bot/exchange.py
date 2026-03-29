from __future__ import annotations

import ccxt
import math
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

    def normalize_symbol(self, symbol: str) -> str | None:
        cleaned = str(symbol or "").strip()
        if not cleaned:
            return None
        if cleaned in self.client.markets:
            return cleaned
        if ":" not in cleaned and "/" in cleaned:
            base, quote = cleaned.split("/", 1)
            normalized = f"{base}/{quote}:{quote}"
            if normalized in self.client.markets:
                return normalized
        return None

    def is_known_symbol(self, symbol: str) -> bool:
        return self.normalize_symbol(symbol) is not None

    def filter_known_symbols(self, symbols: list[str]) -> list[str]:
        filtered: list[str] = []
        for symbol in symbols:
            normalized = self.normalize_symbol(symbol)
            if normalized and normalized not in filtered:
                filtered.append(normalized)
        return filtered

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
            normalized = self.normalize_symbol(symbol)
            if normalized and normalized not in resolved:
                resolved.append(normalized)
        return resolved

    def _configure_futures_risk_profile(self) -> None:
        if len(self.config.symbols) == 1 and self.config.symbols[0].strip().upper() == "ALL":
            return

        for symbol in self.resolve_symbols(self.config.symbols):
            self._configure_symbol_risk_profile(symbol)

    def _configure_symbol_risk_profile(self, symbol: str, leverage_override: int | None = None) -> None:
        leverage = leverage_override or self.config.leverage_for_symbol(symbol)
        try:
            self.client.set_margin_mode(
                self.config.futures_margin_mode,
                symbol,
                {"leverage": leverage},
            )
        except Exception:
            # Binance returns an error if the margin mode is already set.
            pass
        try:
            self.client.set_leverage(leverage, symbol)
        except Exception:
            # Some symbols or account states can reject leverage updates; the order path will surface issues later.
            pass

    def configure_symbol_risk_profile(self, symbol: str, leverage_override: int | None = None) -> None:
        if not self.config.is_futures:
            return
        self._configure_symbol_risk_profile(symbol, leverage_override=leverage_override)

    def fetch_balance(self) -> dict:
        if self.config.is_futures:
            return self.client.fetch_balance({"type": "swap"})
        return self.client.fetch_balance()

    def fetch_account_equity(self) -> float:
        balance = self.fetch_balance()
        if self.config.is_futures:
            info = balance.get("info", {}) or {}
            if info.get("totalMarginBalance") is not None:
                return float(info.get("totalMarginBalance") or 0.0)
            return float(balance.get("total", {}).get("USDT", 0.0) or 0.0)
        totals = balance.get("total", {}) or {}
        usdt_total = float(totals.get("USDT", 0.0) or 0.0)
        return usdt_total

    def validate_connection(self) -> tuple[bool, str]:
        try:
            balance = self.fetch_balance()
            if self.config.is_futures:
                available = float(balance.get("free", {}).get("USDT", 0.0) or 0.0)
                return True, f"Binance futures reachable. Available USDT: {available}"
            assets = sum(1 for _, value in balance.get("total", {}).items() if float(value or 0.0) > 0.0)
            return True, f"Binance spot reachable. Nonzero assets: {assets}"
        except Exception as exc:
            return False, f"Binance validation failed: {exc}"

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 250) -> pd.DataFrame:
        candles = self.client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def fetch_last_price(self, symbol: str) -> float:
        return float(self.client.fetch_ticker(symbol)["last"])

    def fetch_microstructure(self, symbol: str, depth: int = 15, trade_limit: int = 40) -> dict[str, float | int | str]:
        try:
            order_book = self.client.fetch_order_book(symbol, limit=depth)
        except Exception:
            order_book = {"bids": [], "asks": []}
        try:
            trades = self.client.fetch_trades(symbol, limit=trade_limit)
        except Exception:
            trades = []

        bids = order_book.get("bids") or []
        asks = order_book.get("asks") or []
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        mid_price = ((best_bid + best_ask) / 2.0) if best_bid > 0 and best_ask > 0 else 0.0
        spread_pct = ((best_ask - best_bid) / mid_price) if mid_price > 0 else 0.0

        bid_notional = sum(float(price) * float(amount) for price, amount in bids[:depth])
        ask_notional = sum(float(price) * float(amount) for price, amount in asks[:depth])
        total_depth = bid_notional + ask_notional
        depth_imbalance = ((bid_notional - ask_notional) / total_depth) if total_depth > 0 else 0.0

        buy_notional = 0.0
        sell_notional = 0.0
        for trade in trades[:trade_limit]:
            price = float(trade.get("price") or 0.0)
            amount = float(trade.get("amount") or 0.0)
            notional = price * amount
            side = str(trade.get("side") or "").lower()
            if side not in {"buy", "sell"}:
                is_buyer_maker = bool((trade.get("info") or {}).get("isBuyerMaker"))
                side = "sell" if is_buyer_maker else "buy"
            if side == "buy":
                buy_notional += notional
            elif side == "sell":
                sell_notional += notional
        total_trade_notional = buy_notional + sell_notional
        trade_flow = ((buy_notional - sell_notional) / total_trade_notional) if total_trade_notional > 0 else 0.0

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread_pct": spread_pct,
            "bid_depth_usdt": bid_notional,
            "ask_depth_usdt": ask_notional,
            "total_depth_usdt": total_depth,
            "depth_imbalance": depth_imbalance,
            "buy_trade_usdt": buy_notional,
            "sell_trade_usdt": sell_notional,
            "trade_flow_score": trade_flow,
            "trade_count": len(trades[:trade_limit]),
        }

    def fetch_open_position_symbols(self) -> list[str]:
        if not self.config.is_futures:
            return []
        positions = self.client.fetch_positions()
        open_symbols: list[str] = []
        for position in positions:
            contracts = float(position.get("contracts") or 0.0)
            if abs(contracts) <= 0:
                continue
            symbol = position.get("symbol")
            if symbol:
                open_symbols.append(str(symbol))
        return sorted(set(open_symbols))

    def amount_to_precision(self, symbol: str, amount: float) -> float:
        return float(self.client.amount_to_precision(symbol, amount))

    def price_to_precision(self, symbol: str, price: float) -> float:
        return float(self.client.price_to_precision(symbol, price))

    def market_rules(self, symbol: str) -> dict[str, float]:
        market = self.client.market(symbol)
        requirements = self.order_requirements(symbol)
        return {
            **requirements,
            "step_size": self._amount_step(symbol),
            "tick_size": self._price_step(symbol),
        }

    def order_requirements(self, symbol: str) -> dict[str, float]:
        market = self.client.market(symbol)
        limits = market.get("limits", {}) or {}
        amount_limits = limits.get("amount", {}) or {}
        market_limits = limits.get("market", {}) or {}
        cost_limits = limits.get("cost", {}) or {}
        return {
            "min_amount": float(amount_limits.get("min") or 0.0),
            "max_amount": float(amount_limits.get("max") or 0.0),
            "min_market_amount": float(market_limits.get("min") or 0.0),
            "max_market_amount": float(market_limits.get("max") or 0.0),
            "min_cost": float(cost_limits.get("min") or 0.0),
        }

    def _amount_step(self, symbol: str) -> float:
        market = self.client.market(symbol)
        filters = (market.get("info") or {}).get("filters") or []
        candidates: list[float] = []
        for item in filters:
            if item.get("filterType") not in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
                continue
            try:
                step = float(item.get("stepSize") or 0.0)
            except Exception:
                step = 0.0
            if step > 0:
                candidates.append(step)
        if candidates:
            return min(candidates)
        precision = float((market.get("precision") or {}).get("amount") or 0.0)
        return precision if precision > 0 else 0.0

    def _price_step(self, symbol: str) -> float:
        market = self.client.market(symbol)
        filters = (market.get("info") or {}).get("filters") or []
        for item in filters:
            if item.get("filterType") != "PRICE_FILTER":
                continue
            try:
                tick = float(item.get("tickSize") or 0.0)
            except Exception:
                tick = 0.0
            if tick > 0:
                return tick
        precision = float((market.get("precision") or {}).get("price") or 0.0)
        return precision if precision > 0 else 0.0

    def _round_up_amount(self, symbol: str, amount: float) -> float:
        step = self._amount_step(symbol)
        if step <= 0:
            return self.amount_to_precision(symbol, amount)
        rounded = math.ceil(max(amount, 0.0) / step - 1e-12) * step
        return float(self.client.amount_to_precision(symbol, rounded))

    def validate_order_quantity(self, symbol: str, amount: float, reference_price: float) -> tuple[bool, float, str]:
        requirements = self.order_requirements(symbol)
        normalized_amount = self._round_up_amount(symbol, amount)

        min_amount = max(requirements["min_amount"], requirements["min_market_amount"])
        max_amount_candidates = [value for value in (requirements["max_market_amount"], requirements["max_amount"]) if value > 0]
        max_amount = min(max_amount_candidates) if max_amount_candidates else 0.0

        if normalized_amount <= 0:
            return False, normalized_amount, "Quantity rounds down to zero for exchange precision."
        if min_amount > 0 and normalized_amount < min_amount:
            normalized_amount = self._round_up_amount(symbol, min_amount)
        notional = normalized_amount * reference_price
        if requirements["min_cost"] > 0 and notional < requirements["min_cost"]:
            required_amount = requirements["min_cost"] / reference_price if reference_price > 0 else normalized_amount
            normalized_amount = self._round_up_amount(symbol, max(normalized_amount, required_amount))
            notional = normalized_amount * reference_price
        if max_amount > 0 and normalized_amount > max_amount:
            return False, normalized_amount, f"Quantity {normalized_amount} is above maximum amount {max_amount}."
        if requirements["min_cost"] > 0 and notional < requirements["min_cost"]:
            return False, normalized_amount, f"Notional {notional:.4f} is below minimum notional {requirements['min_cost']:.4f}."
        if normalized_amount > amount:
            return True, normalized_amount, f"Adjusted quantity upward to meet exchange minimums. notional={notional:.4f}"
        return True, normalized_amount, "ok"

    def create_market_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        reduce_only: bool = False,
        leverage_override: int | None = None,
    ) -> dict:
        params = {}
        if self.config.is_futures:
            self._configure_symbol_risk_profile(symbol, leverage_override=leverage_override)
        if self.config.is_futures and reduce_only:
            params["reduceOnly"] = True
        return self.client.create_order(symbol=symbol, type="market", side=side, amount=amount, params=params)

    def fetch_order_snapshot(self, symbol: str, order_id: str, fallback: dict | None = None) -> dict:
        if not order_id:
            return fallback or {}
        try:
            return self.client.fetch_order(order_id, symbol)
        except Exception:
            return fallback or {}

    def resolve_fill_price(self, order: dict, fallback_price: float) -> float:
        average = order.get("average")
        price = order.get("price")
        if average is not None and float(average or 0.0) > 0:
            return float(average)
        if price is not None and float(price or 0.0) > 0:
            return float(price)
        fills = order.get("trades") or order.get("fills") or []
        if fills:
            total_qty = 0.0
            total_value = 0.0
            for fill in fills:
                qty = float(fill.get("amount") or fill.get("qty") or 0.0)
                fill_price = float(fill.get("price") or 0.0)
                total_qty += qty
                total_value += qty * fill_price
            if total_qty > 0:
                return total_value / total_qty
        return fallback_price

    def resolve_filled_quantity(self, order: dict, fallback_quantity: float) -> float:
        filled = order.get("filled")
        amount = order.get("amount")
        if filled is not None and float(filled or 0.0) > 0:
            return float(filled)
        if amount is not None and float(amount or 0.0) > 0:
            return float(amount)
        trades = order.get("trades") or order.get("fills") or []
        if trades:
            total_qty = 0.0
            for trade in trades:
                total_qty += float(trade.get("amount") or trade.get("qty") or 0.0)
            if total_qty > 0:
                return total_qty
        return fallback_quantity

    def estimate_market_fill_price(
        self,
        symbol: str,
        side: str,
        quantity: float,
        *,
        fallback_price: float,
        microstructure: dict[str, float | int | str] | None = None,
    ) -> float:
        micro = microstructure or self.fetch_microstructure(symbol)
        best_bid = float(micro.get("best_bid", 0.0) or 0.0)
        best_ask = float(micro.get("best_ask", 0.0) or 0.0)
        mid_price = float(micro.get("mid_price", 0.0) or 0.0)
        total_depth = float(micro.get("total_depth_usdt", 0.0) or 0.0)
        reference = best_ask if side == "buy" else best_bid
        if reference <= 0:
            reference = mid_price if mid_price > 0 else fallback_price
        if reference <= 0:
            return 0.0
        notional = quantity * reference
        if total_depth <= 0 or notional <= 0:
            return reference
        impact_share = min(notional / total_depth, 1.0)
        impact_pct = impact_share * 0.003
        if side == "buy":
            return reference * (1.0 + impact_pct)
        return reference * (1.0 - impact_pct)
