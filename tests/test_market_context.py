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

    def test_gap_is_explicit(self):
        out=gap_from_previous(105,100)
        self.assertEqual(out["direction"],"up")
        self.assertAlmostEqual(out["pct"],5.0)


if __name__=="__main__":
    unittest.main()
