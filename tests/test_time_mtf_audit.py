import unittest
from scan_plus.core.data_contract import normalize_candles
from scan_plus.core.mtf_state import build_mtf_state


class TimeMTFAuditTests(unittest.TestCase):
    def test_forming_bar_is_excluded(self):
        rows=[{"timestamp_ms":9999999999990,"open":1,"high":2,"low":1,"close":1.5}]
        clean, quality=normalize_candles(rows,symbol="EURUSD",interval="1m",as_of_ms=9999999999991,closed_only=True)
        self.assertEqual(clean,[])
        self.assertEqual(quality["closed_rejected_bars"],1)

    def test_mtf_disagreement_is_not_reversal_without_choch(self):
        frames={
            "4h":{"analysis":{"structure":{"state":"bullish","last_event":{"type":"BOS","direction":"bullish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "1h":{"analysis":{"structure":{"state":"bearish","last_event":{"type":"BOS","direction":"bearish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "15m":{"analysis":{"structure":{"state":"bearish"}},"data_quality":{"closed_only":True,"last_closed":True}},
        }
        out=build_mtf_state(frames)
        self.assertEqual(out["context_direction"],"bullish")
        self.assertFalse(out["reversal_confirmed"])

    def test_mtf_choch_confirms_transition(self):
        frames={
            "4h":{"analysis":{"structure":{"state":"bullish"}},"data_quality":{"closed_only":True,"last_closed":True}},
            "1h":{"analysis":{"structure":{"state":"bearish","last_event":{"type":"CHOCH","direction":"bearish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "15m":{"analysis":{"structure":{"state":"bearish"}},"data_quality":{"closed_only":True,"last_closed":True}},
        }
        out=build_mtf_state(frames)
        self.assertTrue(out["reversal_confirmed"])

    def test_transition_is_not_overwritten_by_direction(self):
        frames={"4h":{"analysis":{"structure":{"state":"transition"},"direction":"bullish"},"data_quality":{"closed_only":True,"last_closed":True}}}
        out=build_mtf_state(frames)
        self.assertEqual(out["states"]["4h"],"transition")


if __name__=="__main__":
    unittest.main()
