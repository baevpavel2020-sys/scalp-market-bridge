import unittest
from scan_plus.markets.stocks.execution import build_stock_execution_plan


def frames(direction="bullish"):
    return {
        "15m":{"analysis":{
            "structure":{
                "last_swing_low":{"price":98},
                "last_swing_high":{"price":102},
            },
            "fibonacci":{"extensions":{"1.618":106 if direction=="bullish" else 94}}
        }}
    }


class StockExecutionTests(unittest.TestCase):
    def candidate(self,direction="bullish"):
        return {"status":"CANDIDATE","direction":direction}

    def test_bullish_plan_uses_structural_invalidation_and_cash_risk(self):
        out=build_stock_execution_plan(
            candidate=self.candidate(),frames=frames(),price=100,equity=10000,
            risk_fraction=.01,min_rr=1.5)
        self.assertEqual(out["status"],"PLAN")
        self.assertEqual(out["side"],"BUY")
        self.assertEqual(out["stop"]["price"],98)
        self.assertEqual(out["target"]["price"],106)
        self.assertAlmostEqual(out["risk"]["risk_cash"],100)
        self.assertAlmostEqual(out["risk"]["quantity"],50)

    def test_unconfirmed_candidate_never_creates_plan(self):
        out=build_stock_execution_plan(
            candidate={"status":"WATCH","direction":"bullish"},
            frames=frames(),price=100,equity=10000)
        self.assertEqual(out["status"],"WAIT")

    def test_invalid_stop_side_blocks_plan(self):
        out=build_stock_execution_plan(
            candidate=self.candidate(),frames={"15m":{"analysis":{
                "structure":{"last_swing_low":{"price":101}},
                "fibonacci":{"extensions":{"1.618":106}}
            }}},price=100,equity=10000,risk_fraction=.01)
        self.assertEqual(out["status"],"WAIT")


if __name__=="__main__":
    unittest.main()
