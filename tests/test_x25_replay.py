import unittest
from unittest.mock import patch
from scan_plus.markets.crypto.replay import replay_snapshots


class ReplayTests(unittest.TestCase):
    def test_future_prices_do_not_create_past_trigger(self):
        snaps=[{"execution":{"price":100}},{"execution":{"price":99}},{"execution":{"price":90}}]
        states=[
          {"manipulation_x25":{"state":"WATCH","action":"NO SHORT"}},
          {"manipulation_x25":{"state":"TRIGGERED","action":"SHORT_CANDIDATE"}},
          {"manipulation_x25":{"state":"TRIGGERED","action":"SHORT_CANDIDATE"}},
        ]
        with patch("scan_plus.markets.crypto.replay.observe_crypto_events",side_effect=states):
            out=replay_snapshots(snaps,forward_bars=2,stop_pct=.01)
        self.assertEqual(out["trigger_count"],1)
        self.assertEqual(out["outcomes"][0]["index"],1)
        self.assertTrue(out["outcomes"][0]["hit_2r"])

    def test_contiguous_trigger_is_one_event(self):
        snaps=[{"execution":{"price":100}} for _ in range(4)]
        states=[
          {"manipulation_x25":{"state":"WATCH","action":"NO SHORT"}},
          {"manipulation_x25":{"state":"TRIGGERED","action":"SHORT_CANDIDATE"}},
          {"manipulation_x25":{"state":"TRIGGERED","action":"SHORT_CANDIDATE"}},
          {"manipulation_x25":{"state":"WATCH","action":"NO SHORT"}},
        ]
        with patch("scan_plus.markets.crypto.replay.observe_crypto_events",side_effect=states):
            out=replay_snapshots(snaps)
        self.assertEqual(out["trigger_count"],1)


if __name__=="__main__":
    unittest.main()
