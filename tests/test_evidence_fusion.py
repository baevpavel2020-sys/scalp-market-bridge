import unittest
from scan_plus.markets.crypto.evidence_fusion import fuse_exhaustion, healthy_continuation_veto


class EvidenceFusionTests(unittest.TestCase):
    def test_one_channel_is_context_not_confirmation(self):
        p={"aggression_inefficiency":{"observed":True,"confirmed":True,"score":.9},
           "absorption":{"observed":True,"confirmed":False,"score":0},
           "spot_perp_divergence":{"observed":False,"confirmed":False,"score":0}}
        out=fuse_exhaustion(p)
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["independent_channel_count"],1)

    def test_two_independent_channels_confirm_exhaustion(self):
        p={"aggression_inefficiency":{"observed":True,"confirmed":True,"score":.7},
           "absorption":{"observed":True,"confirmed":False,"score":0},
           "spot_perp_divergence":{"observed":True,"confirmed":True,"score":.8}}
        out=fuse_exhaustion(p)
        self.assertTrue(out["confirmed"])
        self.assertEqual(set(out["independent_channels_confirmed"]),{"price_aggression","spot_perp"})

    def test_healthy_continuation_veto_requires_no_failed_acceptance(self):
        o={"spot_delta_ratio":.35,"perp_delta_ratio":.5,"oi_change_pct":1.2,
           "price_change_pct":1.1,"spot_not_confirming_up":False}
        self.assertTrue(healthy_continuation_veto(o,{"failed_acceptance":{"confirmed":False}})["active"])
        self.assertFalse(healthy_continuation_veto(o,{"failed_acceptance":{"confirmed":True}})["active"])


if __name__=="__main__":
    unittest.main()
