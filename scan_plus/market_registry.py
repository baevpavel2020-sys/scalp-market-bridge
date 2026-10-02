"""Failure-isolated registry for Scan+ market adapters."""
from typing import Dict, Mapping

from scan_plus.contracts import MarketAdapter


class MarketRegistry:
    def __init__(self):
        self._adapters: Dict[str, MarketAdapter] = {}

    def register(self, adapter: MarketAdapter):
        market = str(adapter.market).lower()
        if not market:
            raise ValueError("adapter market is required")
        self._adapters[market] = adapter
        return adapter

    def get(self, market: str) -> MarketAdapter:
        key = str(market).lower()
        if key not in self._adapters:
            raise KeyError(f"market adapter not registered: {key}")
        return self._adapters[key]

    def markets(self):
        return tuple(sorted(self._adapters))

    def safe_scan(self, market: str, symbol: str):
        key=str(market).lower()
        normalized_symbol=str(symbol).upper()
        try:
            result=self.get(key).scan(symbol)
            if not isinstance(result, Mapping):
                raise TypeError("adapter scan must return a mapping")
            returned_market=str(result.get("market") or "").lower()
            if returned_market != key:
                raise ValueError(
                    f"market contract violation: requested={key}, returned={returned_market or 'missing'}"
                )
            returned_symbol=str(result.get("symbol") or "").upper()
            if returned_symbol and returned_symbol != normalized_symbol:
                raise ValueError(
                    f"symbol contract violation: requested={normalized_symbol}, returned={returned_symbol}"
                )
            return {"status": "OK", "market": key, "symbol": normalized_symbol, "result": dict(result)}
        except Exception as exc:
            return {
                "status": "DATA_ERROR",
                "market": str(market).lower(),
                "symbol": symbol,
                "error": f"{type(exc).__name__}: {exc}",
            }
