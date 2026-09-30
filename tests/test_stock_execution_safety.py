import unittest
from scan_plus.markets.stocks.execution import build_stock_execution_plan


class StockExecutionSafetyTests(unittest.TestCase):
    def test_no_implicit_risk_budget(self):
        with self.assertRaises(TypeError):
            build_stock_execution_plan(
                candidate={"status":"CANDIDATE","direction":"bullish"},
                frames={},price=100,equity=10000)

    def test_plan_never_submits(self):
        frames={"15m":{"analysis":{
            "structure":{"last_swing_low":{"price":98}},
            "fibonacci":{"extensions":{"1.618":106}}
        }}}
        out=build_stock_execution_plan(
            candidate={"status":"CANDIDATE","direction":"bullish"},
            frames=frames,price=100,equity=10000,risk_fraction=.01)
        self.assertFalse(out["execution_policy"]["submit"])


if __name__=="__main__":
    unittest.main()
