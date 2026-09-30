import unittest
from scan_plus.markets.crypto.event_adapter import observe_crypto_events


class CryptoEventAdapterTests(unittest.TestCase):
    def test_observer_never_changes_trade_decision(self):
        scan={"trade_state":"WAIT_TRIGGER","execution":{"windows":{}},"timeframes":{}}
        out=observe_crypto_events(scan)
        self.assertFalse(out["affects_trade_decision"])
        self.assertEqual(out["manipulation_x25"]["action"],"NO SHORT")

    def test_missing_liquidation_feed_is_not_invented(self):
        scan={"execution":{"windows":{"5m":{"perp_delta_ratio":1,"spot_delta_ratio":-1,"oi_change_pct":2}},
                           "funding_rate":0.01,
                           "flow_divergences":[{"type":"price_up_perp_led_spot_not_confirming"}]},
              "timeframes":{"5":{"liquidity":{"sweep":"buy_side_swept"}}},
              "setup":{"trigger_direction_aligned":False},"trade_state":"WAIT_TRIGGER"}
        out=observe_crypto_events(scan)
        families={e["family"] for e in out["events"]}
        self.assertNotIn("long_liquidation_cascade",families)
        self.assertNotIn("short_squeeze",families)

    def test_observer_cannot_trigger_x25_without_explicit_structure_trigger(self):
        scan={"execution":{"windows":{"5m":{"perp_delta_ratio":1,"spot_delta_ratio":-1,"oi_change_pct":2}},
                           "funding_rate":0.01,
                           "flow_divergences":[{"type":"price_up_perp_led_spot_not_confirming"}]},
              "timeframes":{"5":{"liquidity":{"sweep":"buy_side_swept"}}},
              "setup":{"trigger_direction_aligned":False},"trade_state":"WAIT_TRIGGER"}
        out=observe_crypto_events(scan)
        self.assertIn(out["manipulation_x25"]["state"],("WATCH","ARMED"))
        self.assertEqual(out["manipulation_x25"]["action"],"NO SHORT")


if __name__=="__main__":
    unittest.main()
