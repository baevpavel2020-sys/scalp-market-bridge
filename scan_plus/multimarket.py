"""Multi-market routing layer.

This layer intentionally knows nothing about Elliott/Fibo/order flow internals.
It only resolves user intent and invokes independent market adapters.
"""
import re

from scan_plus.market_registry import MarketRegistry
from scan_plus.instrument_resolver import normalize_symbol, resolve_market
from scan_plus.decision_aggregator import aggregate_results

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
DEFAULT_UNIVERSE_LIMITS = {"crypto":30,"stocks":15,"forex":9,"commodities":5}


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

    @staticmethod
    def build_requests(parsed):
        markets=tuple(parsed.get("markets") or ())
        symbols=tuple(parsed.get("symbols") or ())
        if not symbols:
            return [(market,None) for market in markets]
        requests=[]
        for symbol in symbols:
            resolved=resolve_market(symbol)
            if resolved:
                if not markets or resolved in markets:
                    requests.append((resolved,symbol))
            elif len(markets)==1:
                requests.append((markets[0],symbol))
        return requests

    def scan_many(self, requests):
        """Expand market-native universes and run isolated full scans."""
        results = []
        for market, symbol in requests:
            key=str(market).lower()
            if symbol is None:
                try:
                    adapter=self.registry.get(key)
                    symbols=adapter.default_symbols(DEFAULT_UNIVERSE_LIMITS.get(key,15))
                except Exception as exc:
                    results.append({"status":"DATA_ERROR","market":key,
                                    "error":f"{type(exc).__name__}: {exc}"})
                    continue
                if not symbols:
                    results.append({"status":"NO_UNIVERSE","market":key,
                                    "reason":"market_native_universe_unavailable"})
                    continue
                for item in symbols:
                    results.append(self.registry.safe_scan(key,item))
            else:
                results.append(self.registry.safe_scan(key,symbol))
        return results

    def scan(self, command):
        parsed=parse_scan_command(command)
        if not parsed.get("markets") and not parsed.get("symbols"):
            return {"status":"INVALID_COMMAND","reason":"no_market_or_symbol"}
        requests=self.build_requests(parsed)
        return {
            "status":"OK",
            "command":str(command),
            "parsed":parsed,
            "requests":requests,
            "results":self.scan_many(requests),
            "decision":aggregate_results(self.scan_many(requests)),
        }
