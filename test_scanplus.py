import unittest
from market_event_engine import _dedupe_events, detect_events
class TestScanPlus(unittest.TestCase):
    def test_context_events_survive(self):
        out=_dedupe_events([{"event":"session_context"},{"event":"inventory_event"}])
        self.assertEqual(len(out),2)
    def test_history_gate(self):
        self.assertFalse(detect_events("forex","EUR/USD",[])["ready"])
if __name__=="__main__":
    unittest.main()
