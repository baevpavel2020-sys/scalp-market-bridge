import unittest
from scan_plus.priority_engine import resolve_priorities

class PriorityTests(unittest.TestCase):
    def test_stock_gap_priorities_are_ordered_and_weighted(self):
        out=resolve_priorities("stocks","NVDA","gap")
        self.assertEqual(out["ordered"][0],"gaps")
        self.assertGreater(out["weights"]["gaps"],out["weights"]["harmonics"])

    def test_lower_priority_is_not_removed(self):
        out=resolve_priorities("forex","EURUSD","reversal")
        self.assertIn("harmonics",out["ordered"])
        self.assertIn("harmonics",out["weights"])

if __name__=="__main__":
    unittest.main()
