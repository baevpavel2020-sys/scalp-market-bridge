import os, tempfile, unittest
from scan_plus.markets.crypto.shadow_recorder import ShadowRecorder, load_recorded_snapshots


class ShadowRecorderTests(unittest.TestCase):
    def test_records_and_prunes_point_in_time_snapshots(self):
        with tempfile.TemporaryDirectory() as d:
            r=ShadowRecorder(root=d,max_files_per_symbol=2)
            for ts in (1,2,3):
                r.record({"symbol":"BTCUSDT","generated_at":ts,"execution":{"price":100+ts}},{"mode":"observer"})
            path=os.path.join(d,"BTCUSDT")
            self.assertEqual(len([x for x in os.listdir(path) if x.endswith(".json")]),2)
            snaps=load_recorded_snapshots(path)
            self.assertEqual([x["generated_at"] for x in snaps],[2,3])


if __name__=="__main__":
    unittest.main()
