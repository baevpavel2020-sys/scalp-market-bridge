import unittest
from scan_plus.core.mtf_state import build_mtf_state
from scan_plus.multimarket import MultiMarketOrchestrator


class V37AuditTests(unittest.TestCase):
    def test_mtf_disagreement_without_structural_event_is_not_reversal(self):
        frames={
            "4h":{"analysis":{"structure":{"state":"bullish","last_event":{"type":"BOS","direction":"bullish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "1h":{"analysis":{"structure":{"state":"bearish","last_event":{"type":"BOS","direction":"bearish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "15m":{"analysis":{"structure":{"state":"bearish"}},"data_quality":{"closed_only":True,"last_closed":True}},
        }
        out=build_mtf_state(frames)
        self.assertFalse(out["reversal_confirmed"])
        self.assertEqual(out["regime"],"countertrend_correction")

    def test_mtf_choch_is_structural_reversal_confirmation(self):
        frames={
            "4h":{"analysis":{"structure":{"state":"bullish"}},"data_quality":{"closed_only":True,"last_closed":True}},
            "1h":{"analysis":{"structure":{"state":"bearish","last_event":{"type":"CHOCH","direction":"bearish"}}},"data_quality":{"closed_only":True,"last_closed":True}},
            "15m":{"analysis":{"structure":{"state":"bearish"}},"data_quality":{"closed_only":True,"last_closed":True}},
        }
        out=build_mtf_state(frames)
        self.assertTrue(out["reversal_confirmed"])

    def test_empty_scan_has_no_thread_pool_work(self):
        class R:
            def get(self,m): raise AssertionError("no adapter lookup expected")
        o=MultiMarketOrchestrator(R())
        self.assertEqual(o.scan_many([]), [])

    def test_single_scan_uses_direct_path(self):
        class Fake:
            market="crypto"
            def scan(self,symbol):
                return {"market":"crypto","symbol":symbol,"candidate":{"status":"WATCH","direction":"unknown"}}
        class R:
            def get(self,m): return Fake()
        out=MultiMarketOrchestrator(R()).scan_many([("crypto","BTCUSDT")])
        self.assertEqual(out[0]["status"],"OK")

    def test_parallel_results_are_returned_in_request_order(self):
        class Fake:
            market="crypto"
            def __init__(self): self.seen=[]
            def scan(self,symbol):
                self.seen.append(symbol)
                return {"market":"crypto","symbol":symbol,"candidate":{"status":"WATCH","direction":"unknown"}}
        class R:
            def __init__(self,a): self.a=a
            def get(self,m): return self.a
        a=Fake()
        o=MultiMarketOrchestrator(R(a))
        out=o.scan_many([("crypto","B"),("crypto","A"),("crypto","C")])
        self.assertEqual([x["symbol"] for x in out],["B","A","C"])

if __name__=="__main__":
    unittest.main()
