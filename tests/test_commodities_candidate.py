import unittest
from scan_plus.markets.commodities.candidate import build_commodity_candidate


def bullish_frames():
    return {
        "15m":{"analysis":{
            "structure":{"state":"bullish"},
            "fibonacci":{"ready":True},
            "elliott":{"ready":True},
        },"commodity_context":{
            "available":["usd_yields_context"],"unknown":[]}},
    }


class CommodityCandidateAuditTests(unittest.TestCase):
    def test_structure_and_shared_confirmation_create_candidate(self):
        out=build_commodity_candidate(
            symbol="XAUUSD",group="metals",frames=bullish_frames())
        self.assertEqual(out["status"],"CANDIDATE")
        self.assertEqual(out["direction"],"bullish")
        self.assertIn("usd_yields_context",out["context_available"])

    def test_missing_context_does_not_create_direction(self):
        frames=bullish_frames()
        frames["15m"]["analysis"]["structure"]["state"]="unknown"
        out=build_commodity_candidate(symbol="WTI",group="energy",frames=frames)
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_confirmation_cannot_leak_from_other_timeframe(self):
        frames={
            "15m":{"analysis":{"structure":{"state":"bullish"},
                                "fibonacci":{"ready":False},"elliott":{"ready":False}}},
            "1h":{"analysis":{"structure":{"state":"unknown"},
                              "fibonacci":{"ready":True},"elliott":{"ready":True}}},
        }
        out=build_commodity_candidate(symbol="COCOA",group="softs",frames=frames)
        self.assertEqual(out["status"],"WATCH")
        self.assertIsNone(out["confirmation_timeframe"])


if __name__=="__main__":
    unittest.main()
