import unittest

from dynamic_collector import DynamicMarketManager
from scan_plus.market_profiles import get_profile


class DirectionRegressionTests(unittest.TestCase):
    def test_price_discovery_does_not_rewrite_confirmed_downtrend(self):
        frame = {
            "structure": {
                "state": "downtrend",
                "last_event": {"type": "BOS", "direction": "bearish", "confirmed_by_close": True},
                "event_lifecycle": {"fresh": True},
            },
            "confluence": {"direction": "bullish", "bullish": 9, "bearish": 1},
        }
        self.assertEqual(DynamicMarketManager._direction_from_frame_v37(frame), "bearish")

    def test_fresh_choch_is_transition_not_new_trend(self):
        frame = {
            "structure": {
                "state": "downtrend",
                "last_event": {"type": "CHOCH", "direction": "bullish", "confirmed_by_close": True},
                "event_lifecycle": {"fresh": True},
            }
        }
        self.assertEqual(DynamicMarketManager._direction_from_frame_v37(frame), "neutral")


class MarketProfileTests(unittest.TestCase):
    def test_profile_override_does_not_mutate_parent(self):
        gold = get_profile("commodities", "XAUUSD")
        base = get_profile("commodities")
        self.assertEqual(gold["group"], "metals")
        self.assertNotEqual(gold["scan"], base["scan"])

    def test_unknown_market_fails_closed(self):
        with self.assertRaises(ValueError):
            get_profile("unknown")


if __name__ == "__main__":
    unittest.main()
