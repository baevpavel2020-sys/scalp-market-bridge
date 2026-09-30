import unittest
from unittest.mock import patch
from scan_plus.markets.crypto.evaluator import evaluate_shadow


def obs(state,reason,confirmed=()):
    stages={k:{"observed":True,"confirmed":k in confirmed} for k in
      ("abnormal_pump","aggression_inefficiency","absorption","spot_perp_divergence","leverage_fragility","failed_acceptance")}
    return {"manipulation_x25":{"state":state,"reason":reason},
            "post_pump_pipeline":stages,
            "reversal_trigger":{"best":{"structure_break":{"confirmed":False},
                                      "bearish_displacement":{"confirmed":False},
                                      "failed_retest":{"confirmed":False}}}}


class EvaluatorTests(unittest.TestCase):
    def test_small_sample_is_not_promotable(self):
        snaps=[{"execution":{"price":100}},{"execution":{"price":99}}]
        observations=[obs("WATCH","exhaustion_not_confirmed"),obs("ARMED","execution_trigger_missing",("abnormal_pump",))]
        replay={"trigger_count":0,"outcomes":[]}
        with patch("scan_plus.markets.crypto.evaluator.observe_crypto_events",side_effect=observations),              patch("scan_plus.markets.crypto.evaluator.replay_snapshots",return_value=replay):
            report=evaluate_shadow(snaps,min_triggers=30)
        self.assertEqual(report["status"],"INSUFFICIENT_SAMPLE")
        self.assertFalse(report["interpretation_allowed"])
        self.assertEqual(report["funnel"]["state_entries"]["WATCH"],1)
        self.assertEqual(report["funnel"]["state_entries"]["ARMED"],1)

    def test_report_exposes_detector_bottleneck(self):
        snaps=[{"execution":{"price":100}}]
        observations=[obs("WATCH","exhaustion_not_confirmed",("abnormal_pump","spot_perp_divergence"))]
        with patch("scan_plus.markets.crypto.evaluator.observe_crypto_events",side_effect=observations),              patch("scan_plus.markets.crypto.evaluator.replay_snapshots",return_value={"trigger_count":0,"outcomes":[]}):
            report=evaluate_shadow(snaps)
        self.assertEqual(report["detectors"]["abnormal_pump"]["confirmed"],1)
        self.assertEqual(report["detectors"]["absorption"]["confirmed"],0)


if __name__=="__main__":
    unittest.main()
