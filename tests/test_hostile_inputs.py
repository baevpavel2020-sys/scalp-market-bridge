import unittest
from scan_plus.multimarket import MultiMarketOrchestrator
from scan_plus.market_registry import MarketRegistry
from scan_plus.markets.forex.candidate import build_forex_candidate
from scan_plus.markets.commodities.candidate import build_commodity_candidate
from scan_plus.markets.stocks.candidate import build_stock_candidate


class HostileInputAuditTests(unittest.TestCase):
    def test_duplicate_symbol_is_scanned_once(self):
        parsed={"markets":("crypto",),"symbols":["BTCUSDT","BTCUSDT"],"routing_errors":[]}
        self.assertEqual(MultiMarketOrchestrator.build_requests(parsed),[("crypto","BTCUSDT")])

    def test_empty_forex_data_cannot_create_candidate(self):
        out=build_forex_candidate(frames={},active_sessions=[],overlap=False)
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_empty_commodity_data_cannot_create_candidate(self):
        out=build_commodity_candidate(symbol="XAUUSD",group="metals",frames={})
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_empty_stock_data_cannot_create_candidate(self):
        out=build_stock_candidate(symbol="NVDAXUSDT",frames={},gap={},underlying=None,ticker={})
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_cross_market_fake_candidate_is_rejected(self):
        registry=MarketRegistry()
        class Fake:
            market="forex"
            def scan(self,symbol):
                return {"market":"forex","symbol":"BTCUSDT",
                        "candidate":{"status":"CANDIDATE","direction":"bullish"}}
        registry.register(Fake())
        out=registry.safe_scan("forex","EURUSD")
        self.assertEqual(out["status"],"DATA_ERROR")

if __name__=="__main__":
    unittest.main()
