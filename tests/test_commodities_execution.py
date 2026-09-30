import unittest
from scan_plus.markets.commodities.execution import build_commodity_execution_plan


def frames():
    return {"15m":{"analysis":{
        "structure":{"last_swing_low":{"price":98},"last_swing_high":{"price":104}},
        "fibonacci":{"ready":True,"extensions":{"1.618":106}}
    }}}


class CommodityExecutionTests(unittest.TestCase):
    def test_cash_risk_plan(self):
        out=build_commodity_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish",
                       "group":"metals","confirmation_timeframe":"15m"},
            frames=frames(),price=100,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"PLAN")
        self.assertEqual(out["side"],"BUY")
        self.assertAlmostEqual(out["risk"]["risk_cash"],100)
        self.assertFalse(out["execution_policy"]["submit"])

    def test_watch_cannot_execute(self):
        out=build_commodity_execution_plan(
            candidate={"status":"WATCH","direction":"bullish",
                       "group":"energy","confirmation_timeframe":"15m"},
            frames=frames(),price=100,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")

    def test_confirmation_timeframe_is_required(self):
        out=build_commodity_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish",
                       "group":"softs","confirmation_timeframe":"1h"},
            frames=frames(),price=100,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")
        self.assertEqual(out["reason"],"confirmation_timeframe_unavailable")

if __name__=="__main__":
    unittest.main()
