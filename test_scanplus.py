import unittest
from market_event_engine import _dedupe_events, detect_events, build_setup_plan
from scan_architecture import MIN_RR, market_block_policy
from limit_engine import generate_candidates
from scenario_engine import build_scenarios, EventLifecycle
from risk_engine import execution_cost, mae_mfe, exposure_cluster
from scan_intelligence import classify_regime, relative_strength, WatchlistStore, alert_payload, enrich_scan, performance_snapshot, backtest_event_setups, walk_forward_backtest
class TestScanPlus(unittest.TestCase):

    def test_market_policy_does_not_enable_crypto_flow_elsewhere(self):
        self.assertTrue(market_block_policy("crypto")["flow"])
        for market in ("stocks","forex","commodities"):
            self.assertFalse(market_block_policy(market)["flow"])
            self.assertFalse(market_block_policy(market)["manipulation"])

    def test_scenario_engine_keeps_reversal_as_alternative(self):
        core={"analysis":{
            "1h":{"ready":True,"structure":{"state":"uptrend"}},
            "15m":{"ready":True,"structure":{"state":"uptrend"}},
        }}
        scenarios=build_scenarios(core,[{"event":"failed_breakout","direction":"bearish"}])
        self.assertEqual(scenarios[0]["type"],"CONTINUATION")
        self.assertTrue(any(x["type"]=="REVERSAL_CANDIDATE" for x in scenarios))

    def test_event_lifecycle_is_symbol_scoped(self):
        lifecycle=EventLifecycle()
        a=lifecycle.update({"market":"crypto","symbol":"BTCUSDT","event":"failed_breakout","direction":"bearish","level":100},confirmed=True)
        b=lifecycle.update({"market":"crypto","symbol":"ETHUSDT","event":"failed_breakout","direction":"bearish","level":100},confirmed=True)
        self.assertNotEqual(a["fingerprint"],b["fingerprint"])

    def test_limit_candidates_are_multi_source_and_two_r(self):
        core={"analysis":{
            "5m":{"ready":True,"regime_levels":{"supports":[99.0,98.0],"resistances":[105.0]},"technical":{"atr14":1}},
            "15m":{"ready":True,"last_confirmed_close":100.0,"regime_levels":{"supports":[99.0,97.0],"resistances":[105.0]},"technical":{"atr14":1}},
            "1h":{"ready":True,"regime_levels":{"supports":[97.0],"resistances":[106.0]}},
            "4h":{"ready":True,"regime_levels":{"supports":[95.0],"resistances":[110.0]}},
        }}
        candidates=generate_candidates("forex","EUR/USD",core,"bullish",3)
        self.assertTrue(candidates)
        self.assertTrue(all(x["rr"]>=MIN_RR for x in candidates))
        self.assertLessEqual(len(candidates),3)

    def test_cost_engine_and_excursion_metrics(self):
        cost=execution_cost("crypto",100,98,104,spread=0.1,commission_bps=5)
        self.assertTrue(cost["ready"])
        self.assertGreater(cost["gross_rr"],2)
        excursion=mae_mfe("bullish",100,[{"high":105,"low":99},{"high":107,"low":98}])
        self.assertEqual(excursion["mfe"],7)
        self.assertEqual(excursion["mae"],2)

    def test_exposure_clusters_same_direction_correlated(self):
        setups=[{"market":"crypto","symbol":"BTC","direction":"bearish"},{"market":"crypto","symbol":"ETH","direction":"bearish"},{"market":"forex","symbol":"EUR/USD","direction":"bearish"}]
        clusters=exposure_cluster(setups,{("BTC","ETH"):0.9})
        self.assertTrue(any(x["size"]==2 for x in clusters))

    def test_enrich_scan_initializes_setup_before_scenario(self):
        out=enrich_scan({"symbol":"SAFE","market":"crypto","timeframes":{},"setup":{}})
        self.assertIn("scenario",out)
        self.assertIn("execution_cost",out)


    def test_event_only_cannot_create_trade(self):
        rows=[{"start":i,"open":100,"high":101,"low":99,"close":100,"volume":100,"confirm":True} for i in range(40)]
        event={"ready":True,"reference":{"atr":1.0},"events":[{"event":"failed_breakout","direction":"bearish","confidence":"high","level":102}]}
        plan=build_setup_plan("forex","EUR/USD",rows,event)
        self.assertFalse(plan["tradeable"])
        self.assertTrue(plan["wait_for_confirmation"])

    def test_walk_forward_has_strict_windows(self):
        rows=[{"start":i,"open":100+i*0.01,"high":101+i*0.01,"low":99+i*0.01,"close":100+i*0.01,"volume":100,"confirm":True} for i in range(160)]
        event={"ready":True,"reference":{"atr":1.0},"events":[]}
        def detect(*args): return event
        def plan(*args): return {"tradeable":False}
        out=walk_forward_backtest("crypto","TESTUSDT",rows,detect,plan,train_bars=100,test_bars=30,step=30)
        self.assertTrue(out["windows"])
        self.assertEqual(out["windows"][0]["test_start"],rows[100]["start"])


    def test_xstocks_daily_rejection_is_market_specific(self):
        rows=[]
        base=1700000000000
        for i in range(35):
            rows.append({"start":base+i*900000,"open":100,"high":101,"low":99,"close":100,"volume":100,"confirm":True})
        rows[-1]["high"]=103; rows[-1]["close"]=100
        out=detect_events("stocks","AAPLXUSDT",rows)
        self.assertTrue(out["ready"])
        self.assertTrue(any(e.get("event")=="daily_failed_high" for e in out["events"]))

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
        core={"analysis":{
            "5m":{"ready":True,"regime_levels":{"supports":[99.0],"resistances":[105.0]},"technical":{"atr14":1.0}},
            "15m":{"ready":True,"last_confirmed_close":100.0,"structure":{"state":"downtrend"},"regime_levels":{"supports":[99.0,97.0],"resistances":[105.0]},"technical":{"atr14":1.0}},
            "1h":{"ready":True,"structure":{"state":"downtrend"},"regime_levels":{"supports":[97.0],"resistances":[104.0]}},
            "4h":{"ready":True,"regime_levels":{"supports":[95.0],"resistances":[110.0]}},
        }}
        plan=build_setup_plan("forex","EUR/USD",rows,event,analysis_core=core)
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
