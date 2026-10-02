import unittest
from scan_plus.operator_view import build_operator_view

class OperatorViewTests(unittest.TestCase):
    def test_shortlist_truncates_without_rewriting(self):
        decision={'count':3,'errors':[],'results':[
            {'market':'crypto','symbol':'BTCUSDT','status':'CANDIDATE','direction':'bearish','situation':'manipulation','reason':'confirmed','priority':{}},
            {'market':'forex','symbol':'EURUSD','status':'CANDIDATE','direction':'bullish','situation':'session_sweep','reason':'confirmed','priority':{}},
            {'market':'stocks','symbol':'NVDA','status':'WATCH','direction':'unknown','situation':'gap','reason':'waiting','priority':{}},
        ]}
        out=build_operator_view(decision,candidate_limit=1,watch_limit=1)
        self.assertEqual(out['summary']['scanned'],3)
        self.assertEqual(out['summary']['candidates'],2)
        self.assertEqual(out['summary']['shown_candidates'],1)
        self.assertEqual(out['shortlist'][0]['symbol'],'BTCUSDT')
        self.assertTrue(out['policy']['presentation_only'])

    def test_empty_decision_is_safe(self):
        out=build_operator_view({})
        self.assertEqual(out['summary']['scanned'],0)
        self.assertEqual(out['shortlist'],[])

if __name__=='__main__': unittest.main()
