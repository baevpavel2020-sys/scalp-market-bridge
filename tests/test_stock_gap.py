import unittest
from scan_plus.markets.stocks.session_gap import classify_gap, gap_fill_progress


class StockGapTests(unittest.TestCase):
    def test_gap_classification(self):
        self.assertEqual(classify_gap({"ready":True,"pct":1.2})["state"],"gap_up")
        self.assertEqual(classify_gap({"ready":True,"pct":-0.8})["state"],"gap_down")
        self.assertEqual(classify_gap({"ready":True,"pct":0.05})["state"],"flat")

    def test_gap_fill_progress(self):
        out=gap_fill_progress(110,100,105)
        self.assertAlmostEqual(out["progress"],0.5)
        self.assertFalse(out["filled"])

    def test_unknown_gap_never_becomes_signal(self):
        self.assertEqual(classify_gap({"ready":False})["state"],"unknown")


if __name__=="__main__":
    unittest.main()
