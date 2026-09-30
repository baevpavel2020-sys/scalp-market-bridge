import unittest
from scan_plus.markets.crypto.reversal_trigger import detect_bearish_reversal_trigger


def scan(stage="WAIT_RETRACE", trigger="waiting"):
    return {"direction":{"bias":"bearish"},"trigger_state":trigger,
      "setup":{"trigger_direction_aligned":trigger=="aligned"},
      "timeframes":{"5":{"structure":{"event":{"type":"BOS","direction":"bearish","confirmed_by_close":True,"start":30}},
      "smc":{"mss":{"confirmed":True,"event":{"direction":"bearish","confirmed_by_close":True,"start":30}},
             "displacement":{"direction":"bearish","start":20,"end":25,"body_atr":1.2,"efficiency":0.7},
             "scenario":{"stage":stage},"poi":[{"low":99,"high":100}]}}}}


class ReversalTriggerTests(unittest.TestCase):
    def test_break_and_displacement_are_not_enough(self):
        out=detect_bearish_reversal_trigger(scan())
        self.assertTrue(out["best"]["structure_break"]["confirmed"])
        self.assertTrue(out["best"]["bearish_displacement"]["confirmed"])
        self.assertFalse(out["triggered"])

    def test_touch_without_rejection_is_not_failed_retest(self):
        out=detect_bearish_reversal_trigger(scan(stage="IN_POI",trigger="waiting"))
        self.assertTrue(out["best"]["failed_retest"]["observed"])
        self.assertFalse(out["best"]["failed_retest"]["confirmed"])
        self.assertFalse(out["triggered"])

    def test_full_chain_can_trigger_shadow_signal(self):
        out=detect_bearish_reversal_trigger(scan(stage="IN_POI",trigger="aligned"))
        self.assertTrue(out["triggered"])


if __name__=="__main__":
    unittest.main()
