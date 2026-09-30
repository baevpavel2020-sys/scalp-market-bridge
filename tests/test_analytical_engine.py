import unittest
from scan_plus.core.analytical_engine import AnalyticalEngine, LegacyCryptoBackend


class FakeBackend:
    def structure(self, rows): return {"ready":True,"state":"uptrend"}
    def fibonacci(self, rows, structure): return {"ready":True}
    def elliott(self, rows, structure): return {"ready":True}
    def harmonics(self, structure): return {"ready":True}


class AnalyticalEngineTests(unittest.TestCase):
    def test_requested_blocks_are_selective(self):
        out=AnalyticalEngine(FakeBackend()).analyze([],requested=["structure","fibonacci"])
        self.assertEqual(set(out),{"structure","fibonacci"})
        self.assertNotIn("elliott",out)
        self.assertNotIn("harmonics",out)

    def test_profiled_analysis_maps_priority_names(self):
        out=AnalyticalEngine(FakeBackend()).analyze_profiled(
            [],["structure_mtf","fibonacci"])
        self.assertEqual(set(out),{"structure","fibonacci"})

    def test_legacy_backend_has_all_shared_tools(self):
        backend=LegacyCryptoBackend()
        self.assertTrue(all(callable(getattr(backend,name)) for name in
                            ("structure","fibonacci","elliott","harmonics")))


if __name__=="__main__":
    unittest.main()
