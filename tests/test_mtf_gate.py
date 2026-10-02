import unittest
from scan_plus.core.mtf_state import build_mtf_state
from scan_plus.decision_aggregator import normalize_result


class MTFGateTests(unittest.TestCase):
    def test_countertrend_candidate_is_downgraded_to_watch(self):
        mtf=build_mtf_state({
            "4h":{"analysis":{"structure":{"state":"uptrend"}}},
            "1h":{"analysis":{"structure":{"state":"uptrend"}}},
            "15m":{"analysis":{"structure":{"state":"downtrend"}}},
        })
        out=normalize_result({
            "market":"forex","symbol":"EURUSD",
            "candidate":{"status":"CANDIDATE","direction":"bullish","reason":"all other blocks"},
            "mtf_state":mtf,
        })
        self.assertEqual(out["status"],"WATCH")
        self.assertTrue(out["mtf_gate_blocked"])

    def test_aligned_candidate_remains_candidate(self):
        mtf=build_mtf_state({
            "4h":{"analysis":{"structure":{"state":"uptrend"}}},
            "1h":{"analysis":{"structure":{"state":"uptrend"}}},
            "15m":{"analysis":{"structure":{"state":"uptrend"}}},
        })
        out=normalize_result({
            "market":"forex","symbol":"EURUSD",
            "candidate":{"status":"CANDIDATE","direction":"bullish"},
            "mtf_state":mtf,
        })
        self.assertEqual(out["status"],"CANDIDATE")
        self.assertFalse(out["mtf_gate_blocked"])

    def test_confirmed_reversal_can_pass_final_gate(self):
        mtf=build_mtf_state({
            "1d":{"analysis":{"structure":{"state":"uptrend"}}},
            "4h":{"analysis":{"structure":{"state":"downtrend"}}},
            "1h":{"analysis":{"structure":{"state":"downtrend"}}},
            "15m":{"analysis":{"structure":{"state":"downtrend"}}},
        })
        out=normalize_result({
            "market":"forex","symbol":"EURUSD",
            "candidate":{"status":"CANDIDATE","direction":"bearish"},
            "mtf_state":mtf,
        })
        self.assertEqual(out["status"],"CANDIDATE")

if __name__=="__main__": unittest.main()
