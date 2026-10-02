"""Multi-market routing layer.

This layer intentionally knows nothing about Elliott/Fibo/order flow internals.
It only resolves user intent and invokes independent market adapters.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from scan_plus.market_registry import MarketRegistry
from scan_plus.instrument_resolver import normalize_symbol, resolve_market
from scan_plus.decision_aggregator import aggregate_results
from scan_plus.operator_view import build_operator_view

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

    routing_errors=[]
    if symbols and not markets:
        resolved=[resolve_market(s) for s in symbols]
        unknown=[s for s,m in zip(symbols,resolved) if m is None]
        known=tuple(dict.fromkeys(m for m in resolved if m))
        if unknown:
            routing_errors.extend({"symbol":s,"reason":"market_unresolved"} for s in unknown)
        return {"markets": known, "symbols": symbols, "routing_errors": routing_errors}
    if markets:
        for symbol in symbols:
            resolved=resolve_market(symbol)
            if resolved is not None and resolved not in markets:
                routing_errors.append({
                    "symbol":symbol,
                    "resolved_market":resolved,
                    "requested_markets":list(markets),
                    "reason":"market_symbol_mismatch",
                })
            elif resolved is None and len(markets)>1:
                routing_errors.append({
                    "symbol":symbol,
                    "requested_markets":list(markets),
                    "reason":"ambiguous_symbol_for_multiple_markets",
                })
    return {"markets": tuple(markets) if markets else (), "symbols": symbols, "routing_errors": routing_errors}


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
        seen=set()
        for symbol in symbols:
            resolved=resolve_market(symbol)
            if resolved:
                if not markets or resolved in markets:
                    key=(resolved,str(symbol).upper())
                    if key not in seen:
                        requests.append(key); seen.add(key)
            elif len(markets)==1:
                key=(markets[0],str(symbol).upper())
                if key not in seen:
                    requests.append(key); seen.add(key)
        return requests

    def scan_many(self, requests):
        """Expand market-native universes and run isolated full scans."""
        expanded=[]
        results=[]
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
                local_seen=set()
                for item in symbols:
                    normalized=str(item).upper()
                    if normalized not in local_seen:
                        expanded.append((key,normalized)); local_seen.add(normalized)
            else:
                expanded.append((key,symbol))

        if not expanded:
            return results
        if len(expanded) == 1:
            market, symbol = expanded[0]
            return [self.registry.safe_scan(market, symbol)]

        # Provider calls are independent per instrument. Parallelize them with a
        # bounded pool to reduce wall-clock time without creating unbounded load.
        workers=min(8,len(expanded))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures={pool.submit(self.registry.safe_scan,market,symbol):(market,symbol)
                      for market,symbol in expanded}
            completed={}
            for future in as_completed(futures):
                market,symbol=futures[future]
                key=(market,symbol)
                try:
                    completed[key]=future.result()
                except Exception as exc:
                    completed[key]={"status":"DATA_ERROR","market":market,"symbol":symbol,
                                    "error":f"{type(exc).__name__}: {exc}"}
        # Preserve request order after parallel execution. This makes logs,
        # snapshots and regression tests deterministic without sacrificing latency.
        return [completed[(market,symbol)] for market,symbol in expanded if (market,symbol) in completed]

    def scan(self, command):
        parsed=parse_scan_command(command)
        if not parsed.get("markets") and not parsed.get("symbols"):
            return {"status":"INVALID_COMMAND","reason":"no_market_or_symbol"}
        requests=self.build_requests(parsed)
        results=self.scan_many(requests)
        for error in parsed.get("routing_errors") or []:
            results.append({
                "status":"DATA_ERROR",
                "market":(error.get("resolved_market") or (
                    error.get("requested_markets") or [None]
                )[0]),
                "symbol":error.get("symbol"),
                "error":error.get("reason"),
                "routing_error":dict(error),
            })
        decision=aggregate_results(results)
        return {
            "status":"OK",
            "command":str(command),
            "parsed":parsed,
            "requests":requests,
            "results":results,
            "decision":decision,
            "operator":build_operator_view(decision),
        }
