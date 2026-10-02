import unittest
from scan_architecture import data_quality, dedupe_events, opportunity_state
from market_event_engine import detect_events, build_setup_plan

class ScanArchitectureTests(unittest.TestCase):
    def test_data_quality_states(self):
        self.assertEqual(data_quality({}, min_closed=2)["state"], "BLOCKED")
        frames={tf:[{"open":1,"high":2,"low":0.5,"close":1}]*2 for tf in ("D","4h","1h","15m","5m")}
        self.assertEqual(data_quality(frames, min_closed=2)["state"], "READY")

    def test_context_events_are_not_collapsed(self):
        out=dedupe_events([
            {"event":"session_context","direction":None,"confidence":"context"},
            {"event":"inventory_event","direction":None,"confidence":"unavailable"},
        ])
        self.assertEqual(len(out),2)

    def test_opportunity_states(self):
        self.assertEqual(opportunity_state({"status":"SETUP"}),"MARKET_READY")
        self.assertEqual(opportunity_state({"status":"WAIT","limit_plan":{"eligible":True}}),"LIMIT_READY")
        self.assertEqual(opportunity_state({"status":"WAIT"}),"WATCH")

    def test_event_engine_requires_closed_candle_history(self):
        self.assertFalse(detect_events("forex","EUR/USD",[])["ready"])

    def test_event_engine_limit_geometry(self):
        rows=[]
        for i in range(40):
            base=100+i*0.02
            rows.append({"open":base,"high":base+1,"low":base-1,"close":base+0.5,"start":i*900000,"volume":100})
        result=detect_events("forex","EUR/USD",rows)
        plan=build_setup_plan("forex","EUR/USD",rows,result)
        self.assertIn("ready",plan)
        self.assertIn("tradeable",plan)

if __name__=="__main__":
    unittest.main()
