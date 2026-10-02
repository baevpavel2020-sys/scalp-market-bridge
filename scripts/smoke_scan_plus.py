"""Offline smoke harness for Scan+.

It deliberately does not call external providers. It exercises the complete
routing -> registry -> aggregation -> Operator View boundary with deterministic
fake adapters, plus the adversarial cases that must remain blocked.
"""
from __future__ import annotations

from scan_plus.market_registry import MarketRegistry
from scan_plus.multimarket import MultiMarketOrchestrator
from scan_plus.decision_aggregator import aggregate_results


class FakeAdapter:
    def __init__(self, market):
        self.market = market

    def default_symbols(self, limit=30):
        return [f"{self.market.upper()}_TEST"]

    def scan(self, symbol):
        return {
            "market": self.market,
            "symbol": symbol,
            "status": "OK",
            "candidate": {
                "status": "WATCH",
                "direction": "unknown",
                "situation": "test",
                "reason": "offline smoke",
            },
        }


def build_registry():
    registry = MarketRegistry()
    for market in ("crypto", "stocks", "forex", "commodities"):
        registry.register(FakeAdapter(market))
    return registry


def run():
    registry = build_registry()
    orchestrator = MultiMarketOrchestrator(registry)

    result = orchestrator.scan("скан")
    decision = result["decision"]

    assert decision["count"] == 4, decision
    assert decision["error_count"] == 0, decision
    assert {x["market"] for x in decision["results"]} == {
        "crypto", "stocks", "forex", "commodities"
    }

    bad_market = aggregate_results([{
        "status": "OK",
        "market": "mars",
        "symbol": "ABC",
        "candidate": {"status": "CANDIDATE", "direction": "bullish"},
    }])
    assert bad_market["count"] == 0
    assert bad_market["error_count"] == 1

    print("SCAN_PLUS_OFFLINE_SMOKE: PASS")
    print("markets=crypto,stocks,forex,commodities")
    print("decision_count=4")
    print("error_count=0")


if __name__ == "__main__":
    run()
