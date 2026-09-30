"""Compatibility adapter for the existing Crypto engine.

During extraction the legacy engine remains the source of truth. This wrapper gives
the multi-market orchestrator a stable boundary without changing Crypto decisions.
"""
from typing import Any, Mapping

from dynamic_collector import dynamic_manager, run_prescan
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile


class CryptoMarketAdapter(MarketAdapter):
    market = "crypto"

    def __init__(self, manager=None, prescan_runner=None):
        self._manager = manager or dynamic_manager
        self._prescan_runner = prescan_runner or run_prescan

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    def prescan(self, **kwargs) -> Mapping[str, Any]:
        return self._prescan_runner(**kwargs)

    def scan(self, symbol: str) -> Mapping[str, Any]:
        return self._manager.scan(symbol)

    def diagnostics(self, symbol: str) -> Mapping[str, Any]:
        return self._manager.diagnostics(symbol)
