import json, os, tempfile, unittest
from unittest.mock import patch
from scan_plus.markets.crypto.shadow_report import evaluate_recorded_symbol


class RecordedReportTests(unittest.TestCase):
    def test_empty_symbol_report_is_explicit(self):
        with tempfile.TemporaryDirectory() as d:
            report=evaluate_recorded_symbol("BTCUSDT",root=d)
        self.assertEqual(report["sample"]["snapshots"],0)
        self.assertEqual(report["status"],"INSUFFICIENT_SAMPLE")
        self.assertEqual(report["symbol"],"BTCUSDT")


if __name__=="__main__":
    unittest.main()
