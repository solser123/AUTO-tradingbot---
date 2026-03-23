from __future__ import annotations

from dataclasses import dataclass

import requests

from .config import BotConfig


COINGLASS_BASE_URL = "https://open-api-v4.coinglass.com/api"


@dataclass(frozen=True)
class CoinGlassProbe:
    enabled: bool
    supported_ok: bool
    supported_count: int
    plan_status: str
    detail: str


class CoinGlassClient:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    @property
    def enabled(self) -> bool:
        return bool(self.config.coinglass_api_key)

    def _headers(self) -> dict[str, str]:
        return {
            "accept": "application/json",
            "CG-API-KEY": self.config.coinglass_api_key,
        }

    def _get(self, path: str) -> dict:
        response = requests.get(
            f"{COINGLASS_BASE_URL}/{path.lstrip('/')}",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code", "")) != "0":
            raise RuntimeError(payload.get("msg") or "CoinGlass request failed")
        return payload

    def fetch_supported_futures_symbols(self) -> set[str]:
        if not self.enabled:
            return set()
        payload = self._get("futures/supported-coins")
        data = payload.get("data") or []
        symbols: set[str] = set()
        for item in data:
            base = str(item or "").strip().upper()
            if not base:
                continue
            symbols.add(f"{base}/USDT:USDT")
        return symbols

    def probe(self) -> CoinGlassProbe:
        if not self.enabled:
            return CoinGlassProbe(
                enabled=False,
                supported_ok=False,
                supported_count=0,
                plan_status="disabled",
                detail="COINGLASS_API_KEY is not configured.",
            )
        try:
            supported = self.fetch_supported_futures_symbols()
        except Exception as exc:
            return CoinGlassProbe(
                enabled=True,
                supported_ok=False,
                supported_count=0,
                plan_status="error",
                detail=str(exc),
            )

        plan_status = "supported_only"
        detail = "CoinGlass key is valid. Supported futures coin endpoint is available."
        try:
            response = requests.get(
                f"{COINGLASS_BASE_URL}/futures/coins-price-change",
                headers=self._headers(),
                timeout=20,
            )
            payload = response.json()
            if str(payload.get("code", "")) == "401":
                plan_status = "upgrade_required"
                detail = payload.get("msg") or "Higher-tier CoinGlass endpoints require an upgraded plan."
            elif str(payload.get("code", "")) == "0":
                plan_status = "market_data_enabled"
                detail = "CoinGlass market data endpoints are available."
        except Exception:
            pass

        return CoinGlassProbe(
            enabled=True,
            supported_ok=True,
            supported_count=len(supported),
            plan_status=plan_status,
            detail=detail,
        )
