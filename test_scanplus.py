import unittest
from market_event_engine import _dedupe_events, detect_events, build_setup_plan
from scan_architecture import MIN_RR
from scan_intelligence import classify_regime, relative_strength, WatchlistStore, alert_payload, enrich_scan, performance_snapshot, backtest_event_setups
class TestScanPlus(unittest.TestCase):
    def test_context_events_survive(self):
        out=_dedupe_events([{"event":"session_context"},{"event":"inventory_event"}])
        self.assertEqual(len(out),2)
    def test_history_gate(self):
        self.assertFalse(detect_events("forex","EUR/USD",[])["ready"])
    def test_minimum_rr_is_centralized_at_two(self):
        self.assertEqual(MIN_RR, 2.0)

    def test_external_limit_plan_is_two_r(self):
        rows=[]
        for i in range(40):
            base=100.0 + i*0.05
            rows.append({"start":i*900000,"open":base,"high":base+1.0,"low":base-1.0,"close":base+0.5,"volume":100})
        event={"ready":True,"reference":{"atr":1.0},"events":[{"event":"failed_breakout","direction":"bearish","confidence":"high","level":102}]}
        plan=build_setup_plan("forex","EUR/USD",rows,event)
        self.assertTrue(plan["tradeable"])
        self.assertGreaterEqual(plan["limit_plan"]["rr"], MIN_RR)
        self.assertEqual(plan["limit_plan"]["minimum_rr"], 2.0)

    def test_health_contract(self):
        from app import app
        with app.test_client() as client:
            response=client.get("/health")
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.get_json()["status"],"healthy")

    def test_collectors_are_lazy_started(self):
        with open("app.py", "r", encoding="utf-8") as fh:
            source = fh.read()
        self.assertNotIn("\ncollector.start()\n", source)
        self.assertNotIn("\nspot_collector.start()\n", source)

    def test_intelligence_regime(self):
        rows=[{"open":100+i*0.5,"high":101+i*0.5,"low":99+i*0.5,"close":100+i*0.5,"volume":100} for i in range(40)]
        self.assertIn(classify_regime(rows)["state"], ("TREND","EXPANSION"))

    def test_relative_strength_ranking(self):
        results=[{"symbol":"A","performance":{"15m":{"change_pct":10}}},{"symbol":"B","performance":{"15m":{"change_pct":2}}}]
        ranked=relative_strength(results,"crypto")
        self.assertEqual(ranked[0]["symbol"],"A")
        self.assertEqual(ranked[0]["rank"],1)

    def test_watchlist_event_driven_state(self):
        store=WatchlistStore()
        scan={"symbol":"TESTUSDT","market":"crypto","direction":"bearish","setup":{"opportunity_state":"LIMIT_READY","limit_plan":{"rr":2.0},"event_basis":[{"event":"failed_breakout","direction":"bearish","level":100}]}}
        self.assertTrue(store.upsert(scan)["state_changed"])
        self.assertFalse(store.upsert(scan)["state_changed"])


    def test_performance_snapshot_uses_one_closed_bar(self):
        rows = [{"close": 100.0}, {"close": 100.0}, {"close": 101.0}]
        perf = performance_snapshot({"15": rows})
        self.assertEqual(perf["15m"]["bars"], 2)
        self.assertAlmostEqual(perf["15m"]["change_pct"], 1.0, places=6)

    def test_watchlist_isolated_by_market(self):
        store = WatchlistStore()
        crypto = {"symbol":"TEST","market":"crypto","setup":{"opportunity_state":"LIMIT_READY","limit_plan":{"rr":2.0}}}
        forex = {"symbol":"TEST","market":"forex","setup":{"opportunity_state":"LIMIT_READY","limit_plan":{"rr":2.0}}}
        self.assertTrue(store.upsert(crypto)["state_changed"])
        self.assertTrue(store.upsert(forex)["state_changed"])
        self.assertEqual(len(store.snapshot()), 2)

    def test_limit_backtest_requires_actual_fill(self):
        rows = [{"start":i, "open":100, "high":101, "low":99, "close":100, "confirm":True} for i in range(40)]
        event = {"ready":True, "reference":{"atr":1.0}, "events":[{"event":"failed_breakout","direction":"bearish","confidence":"high","level":102}]}
        def detect(*args):
            return event
        def plan(*args):
            return {"tradeable":True,"direction":"bearish","limit_plan":{"entry":98,"stop":103,"take_profit":88,"rr":2.0}}
        out = backtest_event_setups("crypto","TESTUSDT",rows,detect,plan,max_checkpoints=1)
        self.assertEqual(out["not_filled"], 1)
        self.assertEqual(out["tp"], 0)

    def test_intelligence_version_is_v2(self):
        self.assertEqual(enrich_scan({"symbol":"V2","market":"crypto","setup":{}})["intelligence_version"], "intelligence_v2")

    def test_alert_duplicate_suppression(self):
        scan={"symbol":"REPEATUSDT","direction":"SHORT","setup":{"opportunity_state":"LIMIT_READY","limit_plan":{"entry":100,"stop":105,"take_profit":90,"rr":2.0},"event_basis":[{"event":"failed_breakout","direction":"bearish","level":100}]}}
        first=enrich_scan(scan)
        second=enrich_scan(scan)
        self.assertTrue(first["alert"]["eligible"])
        self.assertFalse(second["alert"]["eligible"])

    def test_market_policies_keep_crypto_flow_isolated(self):
        from scan_architecture import market_block_policy
        self.assertTrue(market_block_policy("crypto")["flow"])
        for market in ("stocks","forex","commodities"):
            self.assertFalse(market_block_policy(market)["flow"])
            self.assertFalse(market_block_policy(market)["manipulation"])
    def test_xstocks_is_24_7_secondary_market(self):
        from multi_market_adapters import market_profile
        self.assertEqual(market_profile("stocks")["session_model"], "secondary_market_24_7")

    def test_market_policies_have_distinct_event_priority(self):
        from scan_architecture import market_block_policy
        self.assertEqual(market_block_policy("stocks")["event_priority"][0], "gap")
        self.assertEqual(market_block_policy("forex")["event_priority"][0], "session_failed_high")
        self.assertEqual(market_block_policy("commodities")["event_priority"][0], "failed_breakout")
        self.assertEqual(market_block_policy("crypto")["event_priority"][0], "pump_exhaustion")
    def test_external_limit_geometry_uses_shared_structure(self):
        from market_event_engine import build_setup_plan
        analysis = {}
        for tf, state in (("1h","uptrend"),("15m","uptrend")):
            analysis[tf] = {
                "last_confirmed_close": 100.0,
                "structure":{"state":state},
                "technical":{"atr14":1.0},
                "regime_levels":{"supports":[99.0,97.0],"resistances":[105.0,108.0]},
            }
        core={"analysis":analysis}
        rows=[{"start":i,"open":100,"high":101,"low":99,"close":100,"volume":100} for i in range(40)]
        event={"ready":True,"reference":{"atr":1.0},"events":[]}
        plan=build_setup_plan("forex","EUR/USD",rows,event,analysis_core=core)
        self.assertTrue(plan["tradeable"])
        self.assertTrue(plan["limit_plan"]["eligible"])
        self.assertGreaterEqual(plan["limit_plan"]["rr"], 2.0)
        self.assertEqual(plan["limit_plan"]["side"], "BUY_LIMIT")
    def test_manipulation_is_not_a_trade_direction_override(self):
        from pump_exhaustion import detect
        self.assertEqual(detect({}, {}).get("signal"), "NONE")


if __name__=="__main__":
    unittest.main()
