import unittest
from scan_plus.markets.stocks.candidate import build_stock_candidate


def frame(state="bullish", fib=True):
    return {
        "analysis":{
            "structure":{"state":state},
            "fibonacci":{"ready":fib}
        }
    }


class StockCandidateTests(unittest.TestCase):
    def test_gap_alone_does_not_create_direction(self):
        out=build_stock_candidate(
            frames={"15m":frame("unknown")},
            gap={"ready":True,"pct":4.0},
            underlying={"ready":True,"price":100},
            ticker={"last_price":104},
            session=["regular"],
        )
        self.assertEqual(out["status"],"WATCH")
        self.assertEqual(out["direction"],"unknown")

    def test_confirmed_structure_can_create_candidate(self):
        out=build_stock_candidate(
            frames={"15m":frame("bullish")},
            gap={"ready":True,"pct":2.0},
            underlying={"ready":True,"price":100},
            ticker={"last_price":102},
            session=["regular"],
        )
        self.assertEqual(out["status"],"CANDIDATE")
        self.assertEqual(out["direction"],"bullish")

    def test_missing_underlying_blocks_candidate(self):
        out=build_stock_candidate(
            frames={"15m":frame("bullish")},
            gap={"ready":True,"pct":2.0},
            underlying=None,
            ticker={"last_price":102},
            session=["regular"],
        )
        self.assertEqual(out["status"],"WATCH")
        self.assertIn("underlying_unavailable",out["reason"])


if __name__=="__main__":
    unittest.main()
