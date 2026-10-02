import unittest
from datetime import datetime, timezone

from scan_plus.core.data_contract import normalize_candles
from scan_plus.markets.stocks.underlying import UnderlyingProvider


class DataContractAuditTests(unittest.TestCase):
    def test_normalizes_seconds_and_orders_without_cross_symbol_rows(self):
        rows=[
            {"timestamp":2,"open":2,"high":3,"low":1,"close":2.5},
            {"timestamp":1,"open":1,"high":2,"low":0.5,"close":1.5},
            {"timestamp":2,"open":99,"high":100,"low":98,"close":99},
            {"timestamp":3,"open":3,"high":2,"low":1,"close":2},
        ]
        clean, quality=normalize_candles(rows,symbol="EURUSD",interval="60")
        self.assertEqual([x["timestamp_ms"] for x in clean],[1000,2000])
        self.assertEqual(quality["rejected_bars"],2)
        self.assertEqual(clean[0]["_market_contract"],{"symbol":"EURUSD","interval":"60"})

    def test_underlying_symbol_cannot_bleed_into_requested_stock(self):
        provider=UnderlyingProvider(lambda symbol: {
            "symbol":"AAPL","price":100,"session_open":100,"previous_close":99
        })
        with self.assertRaises(ValueError):
            provider.quote("NVDA")

    def test_underlying_matching_symbol_is_accepted(self):
        provider=UnderlyingProvider(lambda symbol: {
            "symbol":"NVDA","price":100,"session_open":100,"previous_close":99
        })
        self.assertEqual(provider.quote("NVDA")["symbol"],"NVDA")


if __name__=="__main__":
    unittest.main()
