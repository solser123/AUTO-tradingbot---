from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

TRADINGVIEW_CRYPTO_IDEAS_URL = "https://in.tradingview.com/ideas/cryptocurrencies/"
BLOCKMEDIA_FEED_URL = "https://www.blockmedia.co.kr/feed"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"

POSITIVE_KEYWORDS = [
    "bullish",
    "long",
    "breakout",
    "support",
    "accumulation",
    "uptrend",
    "rally",
    "buy",
    "surge",
    "rebound",
    "recovery",
]
NEGATIVE_KEYWORDS = [
    "bearish",
    "short",
    "breakdown",
    "resistance",
    "distribution",
    "downtrend",
    "sell",
    "drop",
    "crash",
    "risk-off",
    "liquidation",
]

SYMBOL_HINTS = {
    "BTC": "BTC/USDT:USDT",
    "BITCOIN": "BTC/USDT:USDT",
    "ETH": "ETH/USDT:USDT",
    "ETHEREUM": "ETH/USDT:USDT",
    "BNB": "BNB/USDT:USDT",
    "SOL": "SOL/USDT:USDT",
    "SOLANA": "SOL/USDT:USDT",
    "XRP": "XRP/USDT:USDT",
    "ADA": "ADA/USDT:USDT",
    "DOGE": "DOGE/USDT:USDT",
    "TRX": "TRX/USDT:USDT",
    "LINK": "LINK/USDT:USDT",
    "AVAX": "AVAX/USDT:USDT",
    "LTC": "LTC/USDT:USDT",
    "DOT": "DOT/USDT:USDT",
    "AAVE": "AAVE/USDT:USDT",
    "UNI": "UNI/USDT:USDT",
    "BCH": "BCH/USDT:USDT",
    "SUI": "SUI/USDT:USDT",
    "NEAR": "NEAR/USDT:USDT",
    "APT": "APT/USDT:USDT",
    "ATOM": "ATOM/USDT:USDT",
    "ARB": "ARB/USDT:USDT",
    "OP": "OP/USDT:USDT",
    "ETC": "ETC/USDT:USDT",
    "FIL": "FIL/USDT:USDT",
    "KAS": "KAS/USDT:USDT",
}


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _safe_ascii(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    try:
        cleaned.encode("ascii")
        return cleaned
    except UnicodeEncodeError:
        return cleaned.encode("ascii", "ignore").decode("ascii").strip()


def _sentiment_score(text: str) -> tuple[str, float]:
    normalized = _normalize_text(text).lower()
    positive = sum(1 for keyword in POSITIVE_KEYWORDS if keyword in normalized)
    negative = sum(1 for keyword in NEGATIVE_KEYWORDS if keyword in normalized)
    if positive == negative == 0:
        return "neutral", 0.0
    if positive > negative:
        return "bullish", min(1.0, 0.2 + (positive - negative) * 0.2)
    if negative > positive:
        return "bearish", max(-1.0, -0.2 - (negative - positive) * 0.2)
    return "neutral", 0.0


def _extract_symbols(text: str) -> list[str]:
    upper = _normalize_text(text).upper()
    found: list[str] = []
    for token, normalized in SYMBOL_HINTS.items():
        if token in upper and normalized not in found:
            found.append(normalized)
    return found


def _parse_pub_date(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).isoformat()
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def fetch_tradingview_ideas(limit: int = 20) -> list[dict]:
    response = requests.get(TRADINGVIEW_CRYPTO_IDEAS_URL, headers={"User-Agent": USER_AGENT}, timeout=20)
    response.raise_for_status()
    text = response.text
    pattern = re.compile(
        r'<article[^>]*>.*?<a href="(?P<link>https://in\.tradingview\.com/chart/[^"]+)"[^>]*class="title-[^"]*"[^>]*>(?P<title>.*?)</a>'
        r'.*?<a href="https://in\.tradingview\.com/chart/[^"]+"[^>]*class="paragraph-[^"]*"[^>]*>.*?<span class="line-clamp-content-[^"]*">(?P<summary>.*?)</span>',
        re.DOTALL,
    )
    items: list[dict] = []
    seen_urls: set[str] = set()
    for match in pattern.finditer(text):
        link = _normalize_text(match.group("link"))
        if link in seen_urls:
            continue
        seen_urls.add(link)
        title = _normalize_text(re.sub(r"<[^>]+>", " ", match.group("title")))
        summary = _normalize_text(re.sub(r"<[^>]+>", " ", match.group("summary")))
        symbol_match = re.search(r"/chart/([^/]+)/", link)
        symbols = _extract_symbols(" ".join([title, summary]))
        if symbol_match:
            symbol_token = _safe_ascii(symbol_match.group(1)).upper()
            if symbol_token.endswith("USDT"):
                normalized = f"{symbol_token[:-4]}/USDT:USDT"
                if normalized not in symbols:
                    symbols.insert(0, normalized)
            elif symbol_token in SYMBOL_HINTS:
                normalized = SYMBOL_HINTS[symbol_token]
                if normalized not in symbols:
                    symbols.insert(0, normalized)
        direction, score = _sentiment_score(f"{title} {summary}")
        items.append(
            {
                "source": "tradingview",
                "source_type": "idea",
                "title": title,
                "summary": summary[:500],
                "url": link,
                "published_at": datetime.now(timezone.utc).isoformat(),
                "direction": direction,
                "sentiment_score": score,
                "symbols": symbols,
                "raw_json": {"link": link, "title": title},
            }
        )
        if len(items) >= limit:
            break
    return items


def fetch_blockmedia_news(limit: int = 20) -> list[dict]:
    response = requests.get(BLOCKMEDIA_FEED_URL, headers={"User-Agent": USER_AGENT}, timeout=20)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    items: list[dict] = []
    for item in root.findall("./channel/item"):
        title = _normalize_text(item.findtext("title", default=""))
        link = _normalize_text(item.findtext("link", default=""))
        description = _normalize_text(item.findtext("description", default=""))
        pub_date = _parse_pub_date(item.findtext("pubDate"))
        categories = [(_normalize_text(cat.text or "")) for cat in item.findall("category") if _normalize_text(cat.text or "")]
        combined = " ".join([title, description, " ".join(categories)])
        symbols = _extract_symbols(combined)
        direction, score = _sentiment_score(combined)
        items.append(
            {
                "source": "blockmedia",
                "source_type": "news",
                "title": title,
                "summary": description[:500],
                "url": link,
                "published_at": pub_date,
                "direction": direction,
                "sentiment_score": score,
                "symbols": symbols,
                "raw_json": {"categories": categories},
            }
        )
        if len(items) >= limit:
            break
    return items
