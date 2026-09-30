import unittest
from scan_plus.markets.forex.adapter import ForexMarketAdapter


def rows():
    return [{"timestamp_ms":1704103200000+i*3600000,
             "open":1.10+i*0.001,"high":1.102+i*0.001,
             "low":1.098+i*0.001,"close":1.101+i*0.001,"volume":100+i}
            for i in range(48)]


class FakeLoader:
    def candles(self,symbol="EURUSD",interval="60"):
        return {"source":"fake","product":"fx","symbol":symbol,
                "interval":interval,"candles":rows()}


class FakeEngine:
    def analyze_profiled(self,rows,priorities):
        return {"requested":list(priorities),"bars":len(rows)}


class ForexAdapterTests(unittest.TestCase):
    def setUp(self):
        self.adapter=ForexMarketAdapter(loader=FakeLoader(),engine=FakeEngine())

    def test_prescan_is_session_aware(self):
        out=self.adapter.prescan("EURUSD")
        self.assertEqual(out["market"],"forex")
        self.assertEqual(out["symbol"],"EURUSD")
        self.assertIn("london",out["session"])

    def test_scan_keeps_timeframes_and_fx_context_separate(self):
        out=self.adapter.scan("EURUSD")
        self.assertEqual(set(out["frames"]),{"5m","15m","1h","4h"})
        self.assertTrue(out["execution_context"]["session_sensitive"])
        self.assertTrue(out["execution_context"]["no_crypto_oi_funding_liquidations"])

    def test_diagnostics_identifies_loader(self):
        self.assertEqual(self.adapter.diagnostics("EURUSD")["loader"],"FakeLoader")


if __name__=="__main__":
    unittest.main()
