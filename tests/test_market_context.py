import unittest
from datetime import datetime, timezone
from scan_plus.core.sessions import active_sessions, session_overlap
from scan_plus.core.gaps import gap_from_previous


class MarketContextTests(unittest.TestCase):
    def test_london_new_york_overlap(self):
        ts=datetime(2026,1,5,14,0,tzinfo=timezone.utc).timestamp()
        self.assertIn("london",active_sessions("forex",ts))
        self.assertIn("new_york",active_sessions("forex",ts))
        self.assertTrue(session_overlap("forex",ts))

    def test_us_regular_session(self):
        ts=datetime(2026,1,5,16,0,tzinfo=timezone.utc).timestamp()
        self.assertIn("regular",active_sessions("stocks_us",ts))

    def test_session_high_low_uses_local_session(self):
        from scan_plus.core.session_levels import session_high_low
        # 08:00 London = 08:00 UTC in January.
        candles=[{"timestamp_ms":int(datetime(2026,1,5,8,0,tzinfo=timezone.utc).timestamp()*1000),
                  "high":101,"low":99}]
        out=session_high_low(candles,"forex","london",before_timestamp_ms=int(datetime(2026,1,5,17,0,tzinfo=timezone.utc).timestamp()*1000))
        self.assertTrue(out["ready"])
        self.assertEqual(out["high"],101)

    def test_gap_is_explicit(self):
        out=gap_from_previous(105,100)
        self.assertEqual(out["direction"],"up")
        self.assertAlmostEqual(out["pct"],5.0)


if __name__=="__main__":
    unittest.main()
