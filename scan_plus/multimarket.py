"""Multi-market routing layer.

This layer intentionally knows nothing about Elliott/Fibo/order flow internals.
It only resolves user intent and invokes independent market adapters.
"""
import re

from scan_plus.market_registry import MarketRegistry
from scan_plus.instrument_resolver import normalize_symbol, resolve_market

ALIASES = {
    "crypto": "crypto", "крипта": "crypto",
    "stocks": "stocks", "stock": "stocks", "акции": "stocks",
    "forex": "forex", "fx": "forex", "форекс": "forex",
    "commodities": "commodities", "commodity": "commodities", "сырье": "commodities", "сырьё": "commodities",
    "metals": "commodities", "металлы": "commodities",
    "oil": "commodities", "нефть": "commodities",
    "cocoa": "commodities", "какао": "commodities",
}

DEFAULT_MARKETS = ("crypto", "stocks", "forex", "commodities")


def parse_scan_command(command):
    text = " ".join(str(command or "").strip().split())
    lowered = text.lower()
    if lowered in ("scan", "скан"):
        return {"markets": DEFAULT_MARKETS, "symbols": []}

    tokens = text.split()
    if tokens and tokens[0].lower() in ("scan", "скан"):
        tokens = tokens[1:]

    markets = []
    symbols = []
    for token in tokens:
        key = token.lower().strip(",")
        market = ALIASES.get(key)
        if market:
            if market not in markets:
                markets.append(market)
            continue
        clean = normalize_symbol(token)
        if clean:
            symbols.append(clean)

    if symbols and not markets:
        resolved=[resolve_market(s) for s in symbols]
        known=tuple(dict.fromkeys(m for m in resolved if m))
        return {"markets": known, "symbols": symbols}
    return {"markets": tuple(markets) if markets else (), "symbols": symbols}


class MultiMarketOrchestrator:
    def __init__(self, registry=None):
        self.registry = registry or MarketRegistry()

    def scan_many(self, requests):
        """Run independent requests; one adapter error never aborts the batch."""
        results = []
        for market, symbol in requests:
            results.append(self.registry.safe_scan(market, symbol))
        return results
