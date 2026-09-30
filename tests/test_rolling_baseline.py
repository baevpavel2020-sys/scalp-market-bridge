import unittest
from scan_plus.core.rolling_baseline import RollingMoveBaseline


class RollingBaselineTests(unittest.TestCase):
    def test_not_ready_is_unknown_not_false(self):
        b=RollingMoveBaseline(min_samples=3)
        b.add(.1); b.add(.2)
        self.assertIsNone(b.is_abnormal(2.0))

    def test_outlier_detected_after_baseline(self):
        b=RollingMoveBaseline(min_samples=5)
        for x in (.10,.11,.09,.12,.10,.11,.09,.10): b.add(x)
        self.assertTrue(b.is_abnormal(1.0))
        self.assertFalse(b.is_abnormal(.11))


if __name__=="__main__":
    unittest.main()
