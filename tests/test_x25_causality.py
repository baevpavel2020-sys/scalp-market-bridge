import unittest
from scan_plus.markets.crypto.causality import validate_reversal_causality


class CausalityTests(unittest.TestCase):
    def test_contradictory_structural_timestamps_block_trigger(self):
        p={"abnormal_pump":{"confirmed":True},"failed_acceptance":{"confirmed":True}}
        r={"triggered":True,"best":{
            "bearish_displacement":{"event":{"end":200}},
            "structure_break":{"event":{"start":100}},
        }}
        out=validate_reversal_causality(p,r)
        self.assertFalse(out["structural_order_confirmed"])
        self.assertFalse(out["eligible_for_trigger"])

    def test_valid_structural_order_keeps_trigger(self):
        p={"abnormal_pump":{"confirmed":True},"failed_acceptance":{"confirmed":True}}
        r={"triggered":True,"best":{
            "bearish_displacement":{"event":{"end":100}},
            "structure_break":{"event":{"start":200}},
        }}
        out=validate_reversal_causality(p,r)
        self.assertTrue(out["structural_order_confirmed"])
        self.assertTrue(out["eligible_for_trigger"])

    def test_missing_time_is_unknown_not_fake_confirmation(self):
        out=validate_reversal_causality({},{"triggered":False,"best":{}})
        self.assertIsNone(out["structural_order_confirmed"])
        self.assertIsNone(out["full_temporal_order_confirmed"])


if __name__=="__main__":
    unittest.main()
