import unittest
from scan_plus.core.analytical_engine import AnalyticalEngine, LegacyCryptoBackend
from scan_plus.priority_engine import resolve_priorities


class CoreApiTests(unittest.TestCase):
    def test_unknown_priority_block_is_ignored_not_executed(self):
        engine=AnalyticalEngine(LegacyCryptoBackend())
        out=engine.analyze_profiled([],["structure_mtf","not_a_real_block"])
        self.assertEqual(set(out),{"structure"})

    def test_empty_profile_selection_does_not_run_optional_blocks(self):
        out=AnalyticalEngine(LegacyCryptoBackend()).analyze_profiled([],["gaps"])
        self.assertEqual(set(out),{"structure"})

    def test_profile_does_not_mutate_global_configuration(self):
        first=resolve_priorities("stocks","NVDA","gap")
        first["profile"]["scan"].append("corrupted")
        second=resolve_priorities("stocks","NVDA","gap")
        self.assertNotIn("corrupted",second["profile"]["scan"])

    def test_market_profile_rejects_cross_market_symbol(self):
        with self.assertRaises(ValueError):
            resolve_priorities("forex","NVDA")

if __name__=="__main__":
    unittest.main()
