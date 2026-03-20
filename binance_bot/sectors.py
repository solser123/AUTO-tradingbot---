from __future__ import annotations


SECTOR_LABELS = {
    "LAYER1": "Layer1",
    "LAYER2": "Layer2",
    "DEFI": "DeFi",
    "AI": "AI",
    "MEME": "Meme",
    "NFT_GAMING": "NFT/Gaming",
    "INFRA": "Infra",
    "OTHER": "Other",
}


SECTOR_MAP = {
    "BTC/USDT:USDT": "LAYER1",
    "ETH/USDT:USDT": "LAYER1",
    "BNB/USDT:USDT": "LAYER1",
    "SOL/USDT:USDT": "LAYER1",
    "XRP/USDT:USDT": "LAYER1",
    "ADA/USDT:USDT": "LAYER1",
    "TRX/USDT:USDT": "LAYER1",
    "AVAX/USDT:USDT": "LAYER1",
    "LTC/USDT:USDT": "LAYER1",
    "BCH/USDT:USDT": "LAYER1",
    "SUI/USDT:USDT": "LAYER1",
    "NEAR/USDT:USDT": "LAYER1",
    "APT/USDT:USDT": "LAYER1",
    "ATOM/USDT:USDT": "LAYER1",
    "ETC/USDT:USDT": "LAYER1",
    "FIL/USDT:USDT": "LAYER1",
    "KAS/USDT:USDT": "LAYER1",
    "ONE/USDT:USDT": "LAYER1",
    "ARB/USDT:USDT": "LAYER2",
    "OP/USDT:USDT": "LAYER2",
    "ZRO/USDT:USDT": "LAYER2",
    "AAVE/USDT:USDT": "DEFI",
    "UNI/USDT:USDT": "DEFI",
    "LINK/USDT:USDT": "DEFI",
    "QNT/USDT:USDT": "DEFI",
    "KAVA/USDT:USDT": "DEFI",
    "AIOT/USDT:USDT": "AI",
    "ARC/USDT:USDT": "AI",
    "SAHARA/USDT:USDT": "AI",
    "DOGE/USDT:USDT": "MEME",
    "IMX/USDT:USDT": "NFT_GAMING",
    "ENJ/USDT:USDT": "NFT_GAMING",
    "RARE/USDT:USDT": "NFT_GAMING",
    "ANKR/USDT:USDT": "INFRA",
    "INIT/USDT:USDT": "INFRA",
    "NIGHT/USDT:USDT": "INFRA",
    "DEEP/USDT:USDT": "INFRA",
    "HYPER/USDT:USDT": "INFRA",
}


def _canonical_symbol(symbol: str) -> str:
    cleaned = (symbol or "").strip().upper()
    if ":" in cleaned:
        return cleaned
    if "/" in cleaned:
        base, quote = cleaned.split("/", 1)
        return f"{base}/{quote}:{quote}"
    return cleaned


def sector_for_symbol(symbol: str) -> str:
    canonical = _canonical_symbol(symbol)
    if canonical in SECTOR_MAP:
        return SECTOR_MAP[canonical]
    return SECTOR_MAP.get((symbol or "").strip().upper(), "OTHER")


def sector_label(sector: str) -> str:
    return SECTOR_LABELS.get(sector, sector)


def sector_symbols(symbols: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for symbol in symbols:
        sector = sector_for_symbol(symbol)
        grouped.setdefault(sector, []).append(symbol)
    return grouped
