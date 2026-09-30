import unittest
from scan_plus.markets.forex.execution import build_forex_execution_plan


def frames(direction="bullish"):
    return {"15m":{"analysis":{
        "structure":{
            "last_swing_low":{"price":1.098},
            "last_swing_high":{"price":1.104},
        },
        "fibonacci":{"ready":True,"extensions":{"1.618":1.11 if direction=="bullish" else 1.092}}
    }}}


class ForexExecutionTests(unittest.TestCase):
    def test_bullish_plan_uses_cash_risk(self):
        out=build_forex_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish"},
            frames=frames(),price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"PLAN")
        self.assertEqual(out["side"],"BUY")
        self.assertAlmostEqual(out["risk"]["risk_cash"],100)
        self.assertFalse(out["execution_policy"]["submit"])

    def test_watch_never_creates_plan(self):
        out=build_forex_execution_plan(
            candidate={"status":"WATCH","direction":"bullish"},
            frames=frames(),price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")

    def test_invalid_stop_side_blocks(self):
        bad={"15m":{"analysis":{
            "structure":{"last_swing_low":{"price":1.101}},
            "fibonacci":{"ready":True,"extensions":{"1.618":1.11}}
        }}}
        out=build_forex_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish"},
            frames=bad,price=1.1,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")


if __name__=="__main__":
    unittest.main()
