import unittest
from scan_plus.core.mtf_state import build_mtf_state


class MTFStateTests(unittest.TestCase):
    def test_bullish_context_with_bearish_ltf_is_correction_not_reversal(self):
        out=build_mtf_state({
            "D":{"structure":"uptrend","direction":"bullish","raw_confluence":"bullish"},
            "240":{"structure":"uptrend","direction":"bullish","raw_confluence":"bullish"},
            "60":{"structure":"uptrend","direction":"bullish","raw_confluence":"bullish"},
            "15":{"structure":"range_or_transition","direction":"neutral","raw_confluence":"bearish"},
        })
        self.assertEqual(out["context_direction"],"bullish")
        self.assertEqual(out["execution_direction"],None)
        self.assertEqual(out["regime"],"context_only")
        self.assertFalse(out["reversal_confirmed"])

    def test_aligned_bullish_context_and_execution(self):
        out=build_mtf_state({
            "D":{"structure":"uptrend"},
            "4h":{"structure":"uptrend"},
            "1h":{"structure":"uptrend"},
            "15m":{"structure":"uptrend"},
        })
        self.assertEqual(out["regime"],"aligned")
        self.assertEqual(out["execution_direction"],"bullish")

    def test_medium_tf_flip_is_reversal_confirmation(self):
        out=build_mtf_state({
            "D":{"structure":"uptrend"},
            "4h":{"structure":"downtrend"},
            "1h":{"structure":"downtrend"},
            "15m":{"structure":"downtrend"},
        })
        self.assertEqual(out["context_direction"],"bullish")
        self.assertTrue(out["reversal_confirmed"])

    def test_transition_does_not_become_direction(self):
        out=build_mtf_state({
            "4h":{"structure":"range_or_transition","direction":"bullish"},
            "1h":{"structure":"range_or_transition","direction":"bearish"},
            "15m":{"structure":"range_or_transition"},
        })
        self.assertEqual(out["context_direction"],None)
        self.assertEqual(out["regime"],"unresolved")
        self.assertTrue(out["transition"])

if __name__=="__main__":
    unittest.main()
