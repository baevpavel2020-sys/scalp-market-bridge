import unittest
from scan_plus.markets.stocks.adapter import StocksMarketAdapter
from scan_plus.markets.stocks.underlying import UnderlyingProvider


class FakeLoader:
    def ticker(self, symbol):
        return {"product":"xstock_spot","symbol":"NVDAXUSDT",
                "underlying":symbol,"last_price":180.0,"volume_24h":1000.0}
    def klines(self, symbol, interval="5", limit=240):
        rows=[]
        for i in range(30):
            p=175+i*0.2
            rows.append({"timestamp_ms":i*300000,"open":p,"high":p+1,
                         "low":p-1,"close":p+0.5,"volume":100+i,"turnover":1000+i})
        return {"candles":rows}


class FakeEngine:
    def analyze_profiled(self, rows, priorities):
        return {"requested":list(priorities),"bars":len(rows)}


class FakeUnderlying:
    def quote(self, symbol):
        return {"session_open":181.0,"previous_close":175.0,"price":180.5}


class StocksAdapterTests(unittest.TestCase):
    def setUp(self):
        self.adapter=StocksMarketAdapter(
            loader=FakeLoader(),
            underlying_provider=FakeUnderlying(),
            engine=FakeEngine()
        )

    def test_prescan_keeps_underlying_and_gap_separate(self):
        out=self.adapter.prescan("NVDA")
        self.assertEqual(out["token_symbol"],"NVDAXUSDT")
        self.assertEqual(out["underlying"]["previous_close"],175.0)
        self.assertAlmostEqual(out["gap"]["pct"],100*6/175)
        self.assertEqual(out["gap_classification"]["state"],"gap_up")

    def test_scan_has_independent_timeframes(self):
        out=self.adapter.scan("NVDA")
        self.assertEqual(set(out["frames"]),{"1d","5m","15m","1h","4h"})
        self.assertEqual(out["execution_context"]["product"],"xstock_spot")
        self.assertIn("candidate",out)
        self.assertEqual(out["priority"]["situation"],"gap")

    def test_underlying_payload_is_normalized(self):
        provider=UnderlyingProvider(lambda symbol: {
            "last_price":"180.5","open":"181","prev_close":"175"})
        out=provider.normalize("NVDA",provider.quote("NVDA"))
        self.assertTrue(out["ready"])
        self.assertEqual(out["symbol"],"NVDA")
        self.assertEqual(out["previous_close"],175.0)
        self.assertEqual(out["session_open"],181.0)

    def test_invalid_underlying_payload_stays_not_ready(self):
        provider=UnderlyingProvider(lambda symbol: {"volume":"100"})
        out=provider.normalize("NVDA",provider.quote("NVDA"))
        self.assertFalse(out["ready"])

    def test_missing_underlying_does_not_fake_gap(self):
        adapter=StocksMarketAdapter(loader=FakeLoader(),engine=FakeEngine())
        self.assertFalse(adapter.prescan("NVDA")["gap"]["ready"])


if __name__=="__main__":
    unittest.main()
