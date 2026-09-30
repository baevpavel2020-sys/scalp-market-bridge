import unittest

from scan_plus.market_registry import MarketRegistry
from scan_plus.markets.crypto import CryptoMarketAdapter


class FakeManager:
    def scan(self, symbol):
        return {"symbol": symbol, "trade_state": "WAIT_TRIGGER"}
    def diagnostics(self, symbol):
        return {"symbol": symbol, "ok": True}


class BrokenManager:
    def scan(self, symbol):
        raise RuntimeError("provider unavailable")
    def diagnostics(self, symbol):
        return {}


class CryptoAdapterTests(unittest.TestCase):
    def test_crypto_adapter_preserves_legacy_result(self):
        adapter = CryptoMarketAdapter(manager=FakeManager(), prescan_runner=lambda **kw: {"status": "PASS"})
        result = adapter.scan("BTCUSDT")
        self.assertEqual(result["symbol"], "BTCUSDT")
        self.assertEqual(result["trade_state"], "WAIT_TRIGGER")
        self.assertIn("event_engine", result)
        self.assertFalse(result["event_engine"]["affects_trade_decision"])
        self.assertIn("adaptive_abnormal_pump", result["event_engine"])
        self.assertIsNone(result["event_engine"]["adaptive_abnormal_pump"]["confirmed"])
        self.assertEqual(adapter.prescan()["status"], "PASS")

    def test_registry_isolates_adapter_failure(self):
        registry = MarketRegistry()
        registry.register(CryptoMarketAdapter(manager=BrokenManager(), prescan_runner=lambda **kw: {}))
        result = registry.safe_scan("crypto", "BTCUSDT")
        self.assertEqual(result["status"], "DATA_ERROR")
        self.assertIn("provider unavailable", result["error"])


if __name__ == "__main__":
    unittest.main()
