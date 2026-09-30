import unittest
from scan_plus.markets.forex.adapter import ForexMarketAdapter
from scan_plus.markets.forex.candidate import build_forex_candidate


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


class ForexCandidateAuditTests(unittest.TestCase):
    def test_sweep_without_structure_confirmation_never_creates_candidate(self):
        frames={"15m":{
            "analysis":{
                "structure":{"state":"unknown"},
                "fibonacci":{"ready":True},
                "elliott":{"ready":True}
            },
            "session_liquidity":{
                "latest_candle_sweeps":{"asia":{"state":"low_sweep"}}
            }
        }}
        out=build_forex_candidate(frames=frames,active_sessions=["london"])
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_bullish_candidate_requires_low_sweep_and_shared_confirmation(self):
        frames={"15m":{
            "analysis":{
                "structure":{"state":"bullish"},
                "fibonacci":{"ready":True},
                "elliott":{"ready":True}
            },
            "session_liquidity":{
                "latest_candle_sweeps":{"asia":{"state":"low_sweep"}}
            }
        }}
        out=build_forex_candidate(frames=frames,active_sessions=["london"])
        self.assertEqual(out["status"],"CANDIDATE")
        self.assertEqual(out["direction"],"bullish")

    def test_high_sweep_does_not_create_bullish_candidate(self):
        frames={"15m":{
            "analysis":{
                "structure":{"state":"bullish"},
                "fibonacci":{"ready":True},
                "elliott":{"ready":True}
            },
            "session_liquidity":{
                "latest_candle_sweeps":{"asia":{"state":"high_sweep"}}
            }
        }}
        out=build_forex_candidate(frames=frames,active_sessions=["london"])
        self.assertEqual(out["status"],"WATCH")
