import unittest
import tempfile
import os
from scan_plus.core.data_contract import normalize_candles
from scan_plus.markets.crypto.shadow_recorder import ShadowRecorder


class FinalRegressionAuditTests(unittest.TestCase):
    def test_end_timestamp_seconds_are_normalized(self):
        clean, quality=normalize_candles([
            {"timestamp":1000,"end":1060,"open":1,"high":2,"low":1,"close":1.5}
        ],symbol="EURUSD",interval="1m",as_of_ms=2_000_000,closed_only=True)
        self.assertEqual(len(clean),1)
        self.assertEqual(clean[0]["end_timestamp_ms"],1_060_000)
        self.assertTrue(clean[0]["closed"])

    def test_shadow_records_same_millisecond_without_overwrite(self):
        with tempfile.TemporaryDirectory() as root:
            recorder=ShadowRecorder(root=root,max_files_per_symbol=10)
            scan={"symbol":"BTCUSDT","generated_at":1000}
            event={}
            p1=recorder.record(scan,event)
            p2=recorder.record(scan,event)
            self.assertNotEqual(p1,p2)
            directory=os.path.dirname(p1)
            self.assertEqual(len([x for x in os.listdir(directory) if x.endswith(".json")]),2)

if __name__=="__main__":
    unittest.main()
