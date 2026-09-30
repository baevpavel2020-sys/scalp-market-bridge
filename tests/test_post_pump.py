import unittest
from scan_plus.markets.crypto.post_pump import detect_post_pump_pipeline


class PostPumpDetectorTests(unittest.TestCase):
    def test_unknown_inputs_remain_unobserved(self):
        out=detect_post_pump_pipeline({"execution":{"windows":{}},"timeframes":{}})
        self.assertFalse(out["abnormal_pump"]["observed"])
        self.assertFalse(out["leverage_fragility"]["observed"])

    def test_perp_spot_divergence_detected(self):
        scan={"execution":{"funding_rate":0.001,"book_imbalance":-0.2,
              "windows":{"5m":{"perp_flow_usable":True,"perp_price_change_pct":1.5,
              "perp_delta_ratio":0.6,"spot_delta_ratio":-0.1,"oi_change_pct":1.2,"sample_quality":0.8}}},
              "timeframes":{}}
        out=detect_post_pump_pipeline(scan)
        self.assertTrue(out["abnormal_pump"]["confirmed"])
        self.assertTrue(out["spot_perp_divergence"]["confirmed"])
        self.assertTrue(out["absorption"]["confirmed"])
        self.assertTrue(out["leverage_fragility"]["confirmed"])

    def test_aggression_inefficiency_needs_weak_price_progress(self):
        base={"execution":{"windows":{"5m":{"perp_flow_usable":True,"perp_delta_ratio":0.7,
              "perp_price_change_pct":0.2,"sample_quality":0.8}}},"timeframes":{}}
        self.assertTrue(detect_post_pump_pipeline(base)["aggression_inefficiency"]["confirmed"])
        base["execution"]["windows"]["5m"]["perp_price_change_pct"]=2.0
        self.assertFalse(detect_post_pump_pipeline(base)["aggression_inefficiency"]["confirmed"])


if __name__=="__main__":
    unittest.main()
