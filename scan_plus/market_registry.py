"""Failure-isolated registry for Scan+ market adapters."""
from typing import Dict

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
        try:
            return {"status": "OK", "market": market, "result": self.get(market).scan(symbol)}
        except Exception as exc:
            return {
                "status": "DATA_ERROR",
                "market": str(market).lower(),
                "symbol": symbol,
                "error": f"{type(exc).__name__}: {exc}",
            }
