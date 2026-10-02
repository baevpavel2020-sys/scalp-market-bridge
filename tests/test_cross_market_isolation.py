import unittest
from scan_plus.contracts import MarketAdapter
from scan_plus.market_registry import MarketRegistry
from scan_plus.multimarket import MultiMarketOrchestrator, parse_scan_command
from scan_plus.decision_aggregator import aggregate_results


class FakeAdapter(MarketAdapter):
    def __init__(self, market, wrong_market=None):
        self.market=market
        self.wrong_market=wrong_market

    def prescan(self, **kwargs):
        return {"market":self.market}

    def default_symbols(self, limit=30):
        return [self.market.upper()+"1"]

    def scan(self, symbol):
        return {
            "market":self.wrong_market or self.market,
            "symbol":symbol.upper(),
            "status":"OK",
            "candidate":{"status":"WATCH","direction":"unknown","situation":"continuation","reason":"test"},
        }

    def diagnostics(self, symbol):
        return {"market":self.market,"symbol":symbol}


class MarketIsolationAuditTests(unittest.TestCase):
    def test_default_scan_routes_only_to_four_market_namespaces(self):
        parsed=parse_scan_command("скан")
        self.assertEqual(parsed["markets"],("crypto","stocks","forex","commodities"))

    def test_explicit_market_mismatch_is_not_silently_dropped(self):
        parsed=parse_scan_command("скан stocks BTCUSDT")
        self.assertEqual(parsed["routing_errors"][0]["reason"],"market_symbol_mismatch")

    def test_ambiguous_symbol_with_multiple_markets_is_reported(self):
        parsed=parse_scan_command("скан crypto stocks ABC")
        self.assertEqual(parsed["routing_errors"][0]["reason"],"ambiguous_symbol_for_multiple_markets")

    def test_registry_rejects_cross_market_payload(self):
        registry=MarketRegistry()
        registry.register(FakeAdapter("stocks",wrong_market="forex"))
        out=registry.safe_scan("stocks","NVDA")
        self.assertEqual(out["status"],"DATA_ERROR")
        self.assertIn("market contract violation",out["error"])

    def test_registry_rejects_cross_symbol_payload(self):
        class BadSymbol(FakeAdapter):
            def scan(self,symbol):
                out=super().scan(symbol)
                out["symbol"]="AAPL"
                return out
        registry=MarketRegistry()
        registry.register(BadSymbol("stocks"))
        out=registry.safe_scan("stocks","NVDA")
        self.assertEqual(out["status"],"DATA_ERROR")
        self.assertIn("symbol contract violation",out["error"])

    def test_no_universe_is_an_error_not_a_watch_candidate(self):
        decision=aggregate_results([{
            "status":"NO_UNIVERSE",
            "market":"forex",
            "reason":"market_native_universe_unavailable",
        }])
        self.assertEqual(decision["count"],0)
        self.assertEqual(decision["error_count"],1)
        self.assertEqual(decision["errors"][0]["status"],"DATA_ERROR")

    def test_market_provenance_mismatch_cannot_reach_operator_results(self):
        decision=aggregate_results([{
            "status":"OK",
            "market":"crypto",
            "result":{
                "market":"forex","symbol":"EURUSD",
                "candidate":{"status":"WATCH","direction":"unknown"}
            }
        }])
        self.assertEqual(decision["count"],0)
        self.assertEqual(decision["error_count"],1)

    def test_four_markets_remain_independent(self):
        registry=MarketRegistry()
        for market in ("crypto","stocks","forex","commodities"):
            registry.register(FakeAdapter(market))
        out=MultiMarketOrchestrator(registry).scan("скан")
        self.assertEqual(out["decision"]["count"],4)
        self.assertEqual(
            {x["market"] for x in out["decision"]["results"]},
            {"crypto","stocks","forex","commodities"},
        )
        self.assertEqual(
            {x["market"] for x in out["operator"]["watchlist"]},
            {"crypto","stocks","forex","commodities"},
        )


if __name__=="__main__":
    unittest.main()
