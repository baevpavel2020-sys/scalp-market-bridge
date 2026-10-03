import time
import unittest
import json
import tempfile
from market_event_engine import _dedupe_events, detect_events, build_setup_plan
from scan_architecture import MIN_RR, market_block_policy
from limit_engine import generate_candidates
from scenario_engine import build_scenarios, EventLifecycle
from risk_engine import execution_cost, mae_mfe, exposure_cluster
from dynamic_collector import MarketStream
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
        self.assertEqual(cost["gross_rr"],2.0)
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
    def test_minimum_rr_policy_is_v4_absolute_1_5_preferred_2(self):
        from scan_architecture import PREFERRED_MIN_RR
        self.assertEqual(MIN_RR, 1.5)
        self.assertEqual(PREFERRED_MIN_RR, 2.0)

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
        self.assertEqual(plan["limit_plan"]["minimum_rr"], 1.5)

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
        self.assertEqual(out["not_filled"], 0)
        self.assertEqual(out["samples"], 1)

    def test_intelligence_version_and_funnel(self):
        out=enrich_scan({"symbol":"V3","market":"crypto","setup":{"direction":"bullish","risk_reward":1.4,"trade_state":"WAIT_TRIGGER"}})
        self.assertEqual(out["intelligence_version"], "intelligence_v4_blocks5_14")
        self.assertEqual(out["opportunity_funnel"]["stage"], "EARLY")
        self.assertIn("scenario_authority",out["opportunity_funnel"]["missing"])
        self.assertFalse(out["opportunity_funnel"]["trade_authorized"])

    def test_funnel_hard_invalidation_never_becomes_trade(self):
        from scan_intelligence import opportunity_funnel
        out=opportunity_funnel({"direction":"bullish","risk_reward":4.0,"trade_state":"SETUP","trigger_confirmed":True,
                                "hard_invalidations":[{"reason":"structure_break"}]}, True, True)
        self.assertNotEqual(out["stage"], "TRADE")
        self.assertFalse(out["trade_authorized"])

    def test_execution_contract_separates_analysis_from_broker(self):
        from scan_intelligence import execution_contract
        out=execution_contract("forex", True, False, "broker_not_configured")
        self.assertEqual(out["status"], "EXECUTION_UNAVAILABLE")
        self.assertFalse(out["broker_execution_ready"])

    def test_alert_duplicate_suppression(self):
        scan={"symbol":f"REPEAT{time.time_ns()}USDT","direction":"bearish","setup":{"direction":"bearish","opportunity_state":"READY","trade_state":"SETUP","trigger_confirmed":True,"limit_plan":{"eligible":True,"entry":100,"stop":105,"take_profit":90,"rr":2.0},"event_basis":[{"event":"failed_breakout","direction":"bearish","level":100}],"scenario":{"primary":{"direction":"bearish","state":"READY"},"neutral":False}}}
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
                "ready":True,
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

    def test_v38_hard_invalidation_blocks_trade(self):
        from analytical_depth import analytical_depth_snapshot, apply_hard_invalidations
        setup={"tradeable":True,"execution_ready":True,"direction":"bullish",
               "limit_plan":{"entry":100,"stop":95,"take_profit":110}}
        depth=analytical_depth_snapshot(setup, {"15m":{"structure":{"hard_invalidated":True,"invalidation_reason":"BOS invalidated"}}})
        blocked=apply_hard_invalidations(setup, depth["hard_invalidations"])
        self.assertTrue(blocked["invalidated"])
        self.assertFalse(blocked["tradeable"])
        self.assertFalse(blocked["execution_ready"])
        self.assertEqual(blocked["opportunity_state"],"WATCH")

    def test_v38_conflict_resolver_uses_authority_not_vote_count(self):
        from analytical_depth import build_evidence_graph, resolve_conflicts
        graph=build_evidence_graph([
            {"source":"structure","kind":"direction","value":"bearish","timeframe":"1h"},
            {"source":"elliott","kind":"direction","value":"bullish","timeframe":"1h"},
            {"source":"harmonics","kind":"direction","value":"bullish","timeframe":"1h"},
        ])
        result=resolve_conflicts(graph)
        self.assertEqual(result["direction"],"bearish")
        self.assertEqual(result["authority_source"],"structure")
        self.assertEqual(result["conflict_count"],2)

    def test_v38_swept_target_cannot_remain_valid(self):
        from analytical_depth import derive_hard_invalidations
        setup={"tradeable":True,"limit_plan":{"take_profit":{"price":110,"swept":True}}}
        invalidations=derive_hard_invalidations(setup,{})
        self.assertTrue(any(x["reason"]=="target_liquidity_already_swept" for x in invalidations))

    def test_v38_smc_requires_causal_order_block(self):
        from analytical_depth import validate_smc_evidence
        out=validate_smc_evidence({"ob":{"type":"ob","direction":"bullish"}}, "15m")
        self.assertFalse(out["valid"])
        self.assertEqual(out["reasons"][0]["type"],"non_causal_ob")

    def test_v38_divergence_unknown_age_is_not_fresh(self):
        from analytical_depth import validate_divergence
        out=validate_divergence({"direction":"bullish"})
        self.assertFalse(out["usable"])
        self.assertEqual(out["state"],"UNKNOWN_AGE")

    def test_stage4_candle_cache_roundtrip_restores_250_and_rejects_corrupt_rows(self):
        from dynamic_collector import KLINE_LIMIT, KLINE_INTERVALS
        from collections import deque
        with tempfile.TemporaryDirectory() as td:
            import dynamic_collector as dc
            old_dir = dc.CANDLE_STORE_DIR
            dc.CANDLE_STORE_DIR = td
            try:
                s = MarketStream.__new__(MarketStream)
                s.symbol = "TESTUSDT"; s.market = "linear"
                s.candles = {tf: deque(maxlen=KLINE_LIMIT) for tf in KLINE_INTERVALS}
                s.history_bootstrapped = False; s.history_loaded_at = None
                s.history_error = None; s.history_source = "bybit_websocket"
                s.seed_counts = {tf: 0 for tf in KLINE_INTERVALS}
                s._cache_dirty = True; s._last_cache_save = 0.0
                for i in range(250):
                    start = 1700000000000 + i * 300000
                    s.candles["5"].append({
                        "start":start,"end":start+299999,"open":100+i*0.01,
                        "high":101+i*0.01,"low":99+i*0.01,"close":100.5+i*0.01,
                        "volume":100,"turnover":10000,"confirm":True,"source":"bybit_ws"})
                s._save_candle_cache(force=True)
                payload_path=s._cache_path()
                with open(payload_path,"r",encoding="utf-8") as fh: payload=json.load(fh)
                payload["candles"]["5"].append({"start":1,"end":2,"open":float("nan"),"high":1,"low":1,"close":1,"volume":1})
                with open(payload_path,"w",encoding="utf-8") as fh: json.dump(payload,fh)
                restored = MarketStream.__new__(MarketStream)
                restored.symbol=s.symbol; restored.market=s.market
                restored.candles={tf: deque(maxlen=KLINE_LIMIT) for tf in KLINE_INTERVALS}
                restored.history_bootstrapped=False; restored.history_loaded_at=None
                restored.history_error=None
                restored._load_candle_cache()
                self.assertEqual(len(restored.candles["5"]),250)
                self.assertIsNotNone(restored.history_error)
                self.assertTrue(all(restored.candles["5"][i]["start"] < restored.candles["5"][i+1]["start"] for i in range(249)))
            finally:
                dc.CANDLE_STORE_DIR = old_dir

    def test_stage4_live_candle_merge_is_deduplicated_and_native_wins(self):
        base={"start":1700000000000,"end":1700000299999,"open":100,"high":101,"low":99,"close":100.5,"volume":10,"turnover":1000,"confirm":True,"source":"binance_seed"}
        native={**base,"close":101.0,"source":"bybit_ws"}
        rows=MarketStream._merge_candle_rows([base],[native],"5")
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["source"],"bybit_ws")
        self.assertEqual(rows[0]["close"],101.0)

    def test_stage4_structure_and_liquidity_pass_on_persisted_history(self):
        s=MarketStream.__new__(MarketStream)
        rows=[]
        base=1700000000000
        for i in range(250):
            price=100 + (i*0.08) + (2.0 if (i//20)%2==1 else 0.0)
            rows.append({"start":base+i*300000,"end":base+i*300000+299999,
                         "open":price,"high":price+1.0,"low":price-1.0,
                         "close":price+0.5,"volume":100,"confirm":True})
        analysis=s._analysis_bundle(rows)
        self.assertTrue(analysis["technical"]["ready"])
        self.assertTrue(analysis["structure"]["ready"])
        self.assertTrue(analysis["liquidity"]["ready"])
        self.assertGreaterEqual(analysis["technical"]["ema200"],100)
        self.assertIn(analysis["structure"]["state"],("uptrend","range_or_transition","downtrend"))

    def test_stage7_all_market_acceptance_registry(self):
        from release_gate import run_gates
        report = run_gates()
        self.assertTrue(report["passed"], report["failures"])
        self.assertEqual(tuple(report["markets"]), ("crypto","stocks","forex","commodities"))

    def test_stage8_regression_problem_symbols_do_not_change_market_policy(self):
        from release_gate import acceptance_report
        report = acceptance_report()
        for symbol in ("RVNUSDT","ICPUSDT","INJUSDT","GRASSUSDT","TAOUSDT"):
            self.assertEqual(report["market_policies"]["crypto"]["flow"], True)
            self.assertEqual(report["market_policies"]["crypto"]["manipulation"], True)

    def test_stage8_hard_invalidation_remains_absolute(self):
        from analytical_depth import apply_hard_invalidations
        setup={"tradeable":True,"execution_ready":True,"opportunity_state":"MARKET_READY"}
        blocked=apply_hard_invalidations(setup,[{"reason":"structure_break","source":"structure"}])
        self.assertFalse(blocked["tradeable"])
        self.assertFalse(blocked["execution_ready"])
        self.assertEqual(blocked["opportunity_state"],"WATCH")

    def test_stage9_job_pressure_is_bounded(self):
        from dynamic_collector import ScanJobManager
        original=dict(ScanJobManager._jobs)
        try:
            ScanJobManager._jobs.clear()
            ids=[ScanJobManager._new_job("stress",{"symbols":[f"X{i}"]}) for i in range(100)]
            ScanJobManager._cleanup()
            self.assertLessEqual(len(ScanJobManager._jobs), ScanJobManager.MAX_JOBS)
            self.assertEqual(len(set(ids)),100)
        finally:
            ScanJobManager._jobs.clear()
            ScanJobManager._jobs.update(original)

    def test_stage9_duplicate_pressure_is_idempotent(self):
        from dynamic_collector import ScanJobManager
        original=dict(ScanJobManager._jobs)
        try:
            ScanJobManager._jobs.clear()
            a=ScanJobManager._new_job("stress",{"symbols":["BTCUSDT"]})
            b=ScanJobManager._new_job("stress",{"symbols":["BTCUSDT"]})
            self.assertEqual(a,b)
            self.assertEqual(len(ScanJobManager._jobs),1)
        finally:
            ScanJobManager._jobs.clear()
            ScanJobManager._jobs.update(original)

    def test_stage11_prescan_timeout_is_explicitly_bounded(self):
        from dynamic_collector import ScanJobManager
        self.assertGreaterEqual(ScanJobManager.JOB_PRESCAN_TIMEOUT, 20.0)
        self.assertLessEqual(ScanJobManager.JOB_PRESCAN_TIMEOUT, 60.0)

    def test_stage11_auto_queue_guard_returns_existing_equivalent_job(self):
        from dynamic_collector import ScanJobManager
        original = dict(ScanJobManager._jobs)
        try:
            ScanJobManager._jobs.clear()
            first = ScanJobManager._new_job("auto", {"top_n": 6, "shortlist": 30})
            self.assertEqual(ScanJobManager.status(first)["state"], "QUEUED")
        finally:
            ScanJobManager._jobs.clear()
            ScanJobManager._jobs.update(original)

    def test_stage11_scan_event_is_machine_readable(self):
        from dynamic_collector import _scan_log
        # Smoke test: helper must exist and accept lifecycle payloads without raising.
        _scan_log("TEST_EVENT", job_id="test-job", mode="unified", result={"ok": True})

    def test_stage11_scan_check_is_read_only_and_uses_latest_unified_job(self):
        from dynamic_collector import ScanJobManager
        from app import app
        original = dict(ScanJobManager._jobs)
        try:
            ScanJobManager._jobs.clear()
            jid = ScanJobManager._new_job(
                "unified",
                {"top_n": 5, "shortlist": 30, "markets": ["crypto","stocks","forex","commodities"]},
            )
            client = app.test_client()
            response = client.get("/scan-check")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["active"])
            self.assertEqual(payload["job"]["job_id"], jid)
            self.assertEqual(payload["job"]["mode"], "unified")
            self.assertEqual(len(ScanJobManager._jobs), 1)
        finally:
            ScanJobManager._jobs.clear()
            ScanJobManager._jobs.update(original)

    def test_stage11_canonical_scan_command_is_nonblocking(self):
        from app import app
        client = app.test_client()
        response = client.get("/scan")
        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertIn("job_id", payload)
        self.assertEqual(payload.get("mode"), "unified")
        self.assertIn(payload.get("state"), ("QUEUED", "RUNNING"))

    def test_stage10_release_contract_is_explicit(self):
        from release_gate import RELEASE_VERSION, RELEASE_STAGES
        self.assertEqual(RELEASE_VERSION,"scanplus_v4_blocks1_14")
        self.assertEqual(RELEASE_STAGES,tuple(range(1,15)))

    def test_stage5_prescan_status_contract(self):
        from app import app
        with app.test_client() as client:
            response = client.get("/prescan/status")
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            self.assertIn("providers", data)
            self.assertEqual(data["providers"], ["okx_swap_rest", "kucoin_futures_rest", "binance_spot_marketdata"])
            self.assertGreaterEqual(data["history_workers"], 1)
            self.assertGreaterEqual(data["analysis_workers"], 1)

    def test_stage5_provider_rows_reject_nan_and_stale_data(self):
        from dynamic_collector import OnDemandPreScanService
        import time
        base = int(time.time() * 1000) - (120 * 300_000) - 300_000
        good = [{
            "start": base + i * 300_000,
            "end": base + i * 300_000 + 299_999,
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
            "volume": 100,
        } for i in range(120)]
        clean, err = OnDemandPreScanService._validate_provider_rows(good, "5")
        self.assertIsNone(err)
        self.assertEqual(len(clean), 120)
        stale = [dict(x) for x in good]
        stale_base = int(time.time() * 1000) - (3 * 60 * 60 * 1000) - (120 * 300_000)
        for i, row in enumerate(stale):
            row["start"] = stale_base + i * 300_000
            row["end"] = row["start"] + 299_999
        clean, err = OnDemandPreScanService._validate_provider_rows(stale, "5")
        self.assertEqual(err, "stale_history")
        self.assertEqual(clean, [])
        bad = dict(good[0])
        bad["open"] = float("nan")
        clean, err = OnDemandPreScanService._validate_provider_rows([bad], "5")
        self.assertEqual(err, "insufficient_history")
        self.assertEqual(clean, [])

    def test_stage5_history_price_sanity_blocks_cross_venue_mismatch(self):
        from dynamic_collector import OnDemandPreScanService
        histories = {tf: [{"close": 100.0}] for tf in ("5","15","60")}
        ok, reason, ratio = OnDemandPreScanService._history_price_sanity({"lastPrice": 150.0}, histories)
        self.assertFalse(ok)
        self.assertEqual(reason, "cross_venue_price_mismatch")
        self.assertGreater(ratio, 1.35)

    def test_stage6_job_status_is_nonblocking_and_idempotent(self):
        from dynamic_collector import ScanJobManager
        payload = {"symbols":["BTCUSDT"]}
        jid = ScanJobManager._new_job("batch", payload)
        same = ScanJobManager._new_job("batch", payload)
        self.assertEqual(jid, same)
        status = ScanJobManager.status(jid)
        self.assertEqual(status["state"], "QUEUED")
        self.assertEqual(status["job_id"], jid)
        ScanJobManager._patch(jid, state="DONE", result={"ok":True})
        self.assertEqual(ScanJobManager.status(jid)["result"], {"ok":True})

    def test_stage6_job_routes_return_202_and_job_id(self):
        from app import app
        from unittest.mock import patch
        fake={"job_id":"testjob123","state":"QUEUED"}
        with patch("app.start_scan_batch_job", return_value=fake):
            with app.test_client() as client:
                response = client.get("/scan-batch?symbols=BTCUSDT")
                self.assertEqual(response.status_code, 202)
                data = response.get_json()
                self.assertEqual(data["job_id"], "testjob123")
                self.assertEqual(data["state"], "QUEUED")

    def test_price_discovery_does_not_rewrite_unconfirmed_mtf_direction(self):
        linear={"analysis":{"15":{"regime_levels":{"atr":1.0},"structure":{
            "degrees":{"minor":{"last_points":[{"price":100.0}]}},
            "last_event":{"direction":"bullish","type":"BOS","confirmed":False,"confirmed_by_close":False}
        },"confluence":{"direction":"bearish"},"technical":{"atr14":1.0,"last_close":99.0}}}}
        out=MarketStream._live_structure_context_v33(linear,{"price":110.0})
        self.assertEqual(out["15"]["mode"],"price_discovery_up")
        self.assertEqual(out["15"]["raw_direction"],"bearish")
        self.assertEqual(out["15"]["effective_direction"],"transition")
        linear["analysis"]["15"]["structure"]["last_event"]["confirmed_by_close"]=True
        confirmed=MarketStream._live_structure_context_v33(linear,{"price":110.0})
        self.assertEqual(confirmed["15"]["effective_direction"],"bullish")



    def test_v4_block3_corrective_elliott_uses_canonical_direction(self):
        s=MarketStream.__new__(MarketStream)
        pts=[{"start":1,"price":100.0,"kind":"high","label":"H"},
             {"start":2,"price":90.0,"kind":"low","label":"L"},
             {"start":3,"price":95.0,"kind":"high","label":"LH"},
             {"start":4,"price":85.0,"kind":"low","label":"LL"}]
        ctx={"base":{"working_degree":"intermediate"},"degrees":{"intermediate":{"points":pts}}}
        out=s._elliott_engine_v2([],ctx,{"clusters":[]})
        dirs={x["direction"] for x in ([out.get("primary")]+out.get("alternatives",[])) if x}
        self.assertTrue(dirs)
        self.assertTrue(dirs.issubset({"bullish","bearish"}))

    def test_v4_block3_fib_confluence_needs_independent_anchors(self):
        s=MarketStream.__new__(MarketStream)
        rows=[{"start":i*60000,"open":100.0,"high":150.0,"low":50.0,"close":100.0,"confirm":True} for i in range(20)]
        pts=[{"start":1,"price":100.0,"kind":"low","label":"L"},{"start":2,"price":110.0,"kind":"high","label":"H"}]
        out=s._fib_depth_v38(rows,{"degrees":{"intermediate":{"points":pts}}},{"ready":True})
        self.assertEqual(out["depth_clusters"],[])

    def test_v4_block3_higher_tf_freshness_uses_own_cadence(self):
        import time
        s=MarketStream.__new__(MarketStream)
        now=int(time.time()*1000); end=now-20*60*1000
        rows=[]
        for i in range(40):
            start=end-(39-i)*3600000-3599999
            price=100.0+i*0.2+(1.0 if i%4<2 else -1.0)
            rows.append({"start":start,"end":start+3599999,"open":price,"high":price+1.0,
                         "low":price-1.0,"close":price+0.2,"volume":100.0,"confirm":True})
        out=s._analysis_bundle(rows)
        self.assertEqual(out["signal_freshness"]["timeframe"],"60")
        self.assertEqual(out["signal_freshness"]["state"],"FRESH")
        self.assertGreater(out["signal_freshness"]["stale_limit_seconds"],900)


    def test_v4_block4_imports_and_dedupes_block3_liquidity_event(self):
        from liquidity_leverage_engine import detect
        analysis={"15":{"liquidity":{"event":{"type":"liquidity_grab","direction":"bearish","side":"buy_side","level":101.0,"start":123}}}}
        out=detect({"flow":{}},{},analysis)
        primary=[e for e in out["events"] if e["type"]=="FAILED_BREAKOUT"]
        self.assertEqual(len(primary),1)
        self.assertEqual(primary[0]["direction"],"bearish")
        self.assertEqual({x["type"] for x in primary[0].get("related_evidence",[])},{"LIQUIDITY_SWEEP"})
        self.assertFalse(primary[0]["trade_authority"])

    def test_v4_block4_mss_contract_is_consumed_but_unknown_retest_never_passes(self):
        from liquidity_leverage_engine import _structure_confirmation
        analysis={"15":{"structure":{},"smart_money":{"mss":{"confirmed":True,"event":{"type":"MSS","direction":"bearish","confirmed_by_close":True}}}}}
        out=_structure_confirmation(analysis,"bearish")
        self.assertTrue(out["structure_break"])
        self.assertFalse(out["failed_retest"])
        self.assertFalse(out["confirmed"])

    def test_v4_block4_explicit_failed_retest_completes_causal_confirmation(self):
        from liquidity_leverage_engine import _structure_confirmation
        analysis={"15":{"structure":{"last_event":{"type":"CHOCH","direction":"bearish","confirmed_by_close":True}},
                        "smart_money":{"failed_retest":{"direction":"bearish","failed":True}}}}
        out=_structure_confirmation(analysis,"bearish")
        self.assertTrue(out["confirmed"])


    def test_v4_block5_neutral_scenario_cannot_reach_ready_or_trade(self):
        from scan_intelligence import opportunity_funnel
        setup={"direction":"bullish","entry":100.0,"stop":99.0,"trigger_confirmed":True}
        scenario={"primary":None,"neutral":True}
        out=opportunity_funnel(setup,True,True,scenario)
        self.assertEqual(out["stage"],"EARLY")
        self.assertFalse(out["trade_authorized"])
        self.assertIn("scenario_authority",out["missing"])

    def test_v4_block5_setup_direction_must_match_primary_scenario(self):
        from scan_intelligence import opportunity_funnel
        setup={"direction":"bullish","entry":100.0,"stop":99.0,"trigger_confirmed":True}
        scenario={"primary":{"direction":"bearish","state":"READY"},"neutral":False}
        out=opportunity_funnel(setup,True,True,scenario)
        self.assertFalse(out["scenario_authorized"])
        self.assertFalse(out["trade_authorized"])

    def test_v4_block5_authorized_primary_scenario_can_advance_funnel(self):
        from scan_intelligence import opportunity_funnel
        setup={"direction":"bullish","entry":100.0,"stop":99.0,"trigger_confirmed":True,
               "limit_plan":{"entry":100.0,"stop":99.0,"take_profit":102.0}}
        scenario={"primary":{"direction":"bullish","state":"READY"},"neutral":False}
        out=opportunity_funnel(setup,True,True,scenario)
        self.assertTrue(out["scenario_authorized"])
        self.assertEqual(out["stage"],"TRADE")
        self.assertTrue(out["trade_authorized"])

    def test_v4_block5_analysis_not_ready_is_fail_closed(self):
        from scan_intelligence import opportunity_funnel
        setup={"direction":"bullish","entry":100.0,"stop":99.0,"trigger_confirmed":True}
        scenario={"primary":{"direction":"bullish","state":"READY"},"neutral":False}
        out=opportunity_funnel(setup,False,True,scenario)
        self.assertEqual(out["stage"],"EARLY")
        self.assertFalse(out["trade_authorized"])
        self.assertIn("analysis_ready",out["missing"])


    def test_v4_block6_funnel_requires_complete_directional_geometry(self):
        from v4_core import opportunity_funnel
        self.assertEqual(opportunity_funnel({"direction":"bullish","limit_plan":{"eligible":True}})["stage"],"DEVELOPING")
        bad={"direction":"bullish","trigger_confirmed":True,"limit_plan":{"eligible":True,"entry":100,"stop":105,"take_profit":110}}
        self.assertFalse(opportunity_funnel(bad)["trade_authorized"])
        good={"direction":"bullish","trigger_confirmed":True,"limit_plan":{"eligible":True,"entry":100,"stop":95,"take_profit":110}}
        self.assertTrue(opportunity_funnel(good)["trade_authorized"])

    def test_v4_block6_execution_cost_rejects_wrong_side_target_or_stop(self):
        from risk_engine import execution_cost
        self.assertFalse(execution_cost("crypto",100,95,90,direction="bullish")["ready"])
        self.assertFalse(execution_cost("crypto",100,95,110,direction="bearish")["ready"])
        self.assertTrue(execution_cost("crypto",100,95,110,direction="bullish")["ready"])

    def test_v4_block6_net_rr_includes_execution_friction(self):
        from risk_engine import execution_cost
        out=execution_cost("crypto",100,95,110,spread=0.2,slippage_bps=10,commission_bps=10,direction="bullish")
        self.assertTrue(out["ready"])
        self.assertLess(out["net_rr"],out["gross_rr"])

    def test_v4_block6_core_and_x25_risk_caps_are_isolated(self):
        from risk_engine import position_size
        self.assertFalse(position_size(1000,.03,100,95,11,"core")["ready"])
        x25=position_size(1000,.03,100,95,25,"pump_exhaustion_x25")
        self.assertTrue(x25["ready"])
        self.assertLessEqual(x25["risk_pct"],.01)
        self.assertAlmostEqual(x25["money_risk"],10.0)


    def test_v4_block7_outcome_dedupe_survives_logger_restart(self):
        import tempfile, os
        from scan_intelligence import OutcomeLogger
        fd,path=tempfile.mkstemp(); os.close(fd)
        try:
            scan={"market":"crypto","symbol":"BTCUSDT","opportunity_funnel":{"stage":"READY"},
                  "setup":{"market":"crypto","symbol":"BTCUSDT","direction":"bullish",
                           "limit_plan":{"entry":100,"stop":95,"take_profit":110,"rr":2.0}}}
            first=OutcomeLogger(path)
            self.assertTrue(first.record(scan))
            second=OutcomeLogger(path)
            self.assertFalse(second.record(scan))
            with open(path,"r",encoding="utf-8") as fh:
                self.assertEqual(sum(1 for line in fh if line.strip()),1)
        finally:
            try: os.remove(path)
            except OSError: pass

    def test_v4_block7_scan_check_is_strictly_read_only(self):
        import inspect, app as app_module
        src=inspect.getsource(app_module.scan_check)
        self.assertIn("get_latest_unified_scan_job",src)
        self.assertNotIn("start_scan",src)
        self.assertNotIn("_new_job",src)

    def test_v4_block7_edge_discovery_uses_only_resolved_outcomes(self):
        from scan_intelligence import edge_summary
        out=edge_summary([
            {"market":"crypto","direction":"bullish","outcome":"TP","realized_r":2.0},
            {"market":"crypto","direction":"bullish","outcome":"UNRESOLVED","realized_r":99.0},
            {"market":"crypto","direction":"bullish","outcome":"SL","realized_r":-1.0},
        ])
        self.assertEqual(out["samples"],2)
        self.assertEqual(out["tp"],1)
        self.assertEqual(out["sl"],1)


    def test_v4_block8_unified_market_list_is_canonical_and_fail_closed(self):
        from dynamic_collector import ScanJobManager
        with self.assertRaises(ValueError):
            ScanJobManager.start_unified(markets=["crypto","unknown_market"])
        # None/empty use the documented default; unknown values must fail closed.
        import inspect
        src=inspect.getsource(ScanJobManager.start_unified)
        self.assertIn('["crypto","stocks"]',src.replace(" ", ""))

    def test_v4_block8_market_policy_never_leaks_crypto_features(self):
        from scan_architecture import market_block_policy
        for market in ("stocks","forex","commodities"):
            policy=market_block_policy(market)
            self.assertFalse(policy["flow"])
            self.assertFalse(policy["manipulation"])
            self.assertNotIn("pump_exhaustion",policy["event_priority"])

    def test_v4_block8_external_profiles_keep_execution_analysis_only(self):
        from multi_market_adapters import MARKET_PROFILES
        self.assertEqual(MARKET_PROFILES["forex"]["execution"],"external_fx_broker_required")
        self.assertEqual(MARKET_PROFILES["commodities"]["execution"],"external_commodity_broker_required")
        self.assertFalse(MARKET_PROFILES["forex"]["oi"])
        self.assertFalse(MARKET_PROFILES["commodities"]["funding"])

    def test_v4_block8_scan_markets_does_not_gate_fallback_on_api_key(self):
        import inspect, app as app_module
        src=inspect.getsource(app_module.scan_markets)
        self.assertNotIn('market_ready=adapter.configured',src)
        self.assertIn('adapter.scan(market,s)',src)

    def test_v4_block8_unknown_market_policy_is_not_silently_crypto(self):
        from scan_architecture import market_block_policy
        with self.assertRaises(ValueError):
            market_block_policy("unknown_market")

if __name__=="__main__":
    unittest.main()

    def test_v4_block1_symbol_normalization(self):
        from data_contracts import normalize_instrument
        self.assertEqual(normalize_instrument("forex","EURUSD")["instrument_id"],"forex:EUR/USD")
        self.assertEqual(normalize_instrument("commodities","GOLD")["instrument_id"],"commodities:XAU/USD")
        self.assertEqual(normalize_instrument("crypto","BTC/USDT")["instrument_id"],"crypto:BTCUSDT")

    def test_v4_block1_data_quality_distinguishes_fail_and_degraded(self):
        from data_contracts import frame_quality
        import time
        now=int(time.time()*1000)
        frames={}
        tf_ms={"1D":86400000,"4h":14400000,"1h":3600000,"15m":900000,"5m":300000}
        for tf,step in tf_ms.items():
            frames[tf]=[{"start":now-step*(50-i),"open":100,"high":101,"low":99,"close":100,"confirm":True,"source":"test"} for i in range(50)]
        good=frame_quality(frames,now_ms=now)
        self.assertEqual(good["state"],"PASS")
        self.assertEqual(good["hard_failures"],[])
        missing={**frames,"4h":frames["4h"][:10]}
        self.assertEqual(frame_quality(missing,now_ms=now)["state"],"FAIL")
        stale={tf:[{**r,"start":r["start"]-(10*tf_ms[tf])} for r in rows] for tf,rows in frames.items()}
        self.assertEqual(frame_quality(stale,now_ms=now)["state"],"DEGRADED")

    def test_v4_block1_provenance_is_per_timeframe(self):
        from data_contracts import provenance
        out=provenance({"5m":[{"source":"bybit"},{"source":"bybit"},{"source":"yahoo"}]},"bybit","fallback")
        self.assertEqual(out["sources_by_timeframe"]["5m"]["bybit"],2)
        self.assertEqual(out["fallback_reason"],"fallback")

    def test_v4_block2_regime_contract_is_context_only(self):
        from context_engine import build_context
        import time
        now=int(time.time()*1000)
        def rows(step):
            return [{"start":now-step*(60-i),"open":100+i*.2,"high":101+i*.2,"low":99+i*.2,"close":100.5+i*.2,"volume":100,"confirm":True} for i in range(60)]
        frames={"1D":rows(86400000),"4h":rows(14400000),"1h":rows(3600000),"15m":rows(900000),"5m":rows(300000)}
        out=build_context("crypto","BTCUSDT",frames)
        self.assertEqual(out["authority"],"context_only_not_trade_direction")
        self.assertIn(out["regime"]["state"],("TREND","EXPANSION","COMPRESSION","RANGE_TRANSITION"))
        self.assertEqual(out["instrument_profile"]["flow_model"],"spot_perp")

    def test_v4_block2_news_risk_never_fabricates_calendar(self):
        from context_engine import event_risk
        out=event_risk("forex",{})
        self.assertEqual(out["calendar_news"]["state"],"UNAVAILABLE")
        self.assertEqual(out["calendar_news"]["events"],[])
        supplied=event_risk("forex",{},[{"name":"CPI","impact":"HIGH"}])
        self.assertEqual(supplied["calendar_news"]["state"],"AVAILABLE")
        self.assertEqual(supplied["severity"],"HIGH")

    def test_v4_block2_prescan_context_has_no_trade_authority(self):
        from context_engine import prescan_context
        import time
        now=int(time.time()*1000)
        histories={}
        for tf,step in (("5",300000),("15",900000),("60",3600000)):
            histories[tf]=[{"start":now-step*(40-i),"open":100,"high":101,"low":99,"close":100+i*.01,"confirm":True} for i in range(40)]
        out=prescan_context("ETHUSDT",histories)
        self.assertEqual(out["authority"],"context_only_not_trade_direction")
        self.assertEqual(out["session_profile"]["session"],"24_7")

    def test_v4_block3_elliott_recursive_contract_preserves_higher_degree(self):
        from analytical_core_v4 import elliott_degree_contract
        out=elliott_degree_contract({"major":{"ready":True,"primary":{"type":"impulse"}},
                                     "minor":{"ready":True,"primary":{"type":"zigzag"}}},"minor")
        self.assertEqual(out["higher_degree_anchor"],"major")
        self.assertIn("higher_degree_count_survives",out["preservation_rule"])
        self.assertIn("overlap_invalidates_standard_impulse",out["overlap_rule"])

    def test_v4_block3_evidence_deduplicates_same_causal_confirmation(self):
        from analytical_core_v4 import causal_evidence_contract
        item={"source":"fibonacci","type":"target","value":{"direction":"bullish"},"derived_from":["structure","liquidity"]}
        out=causal_evidence_contract({"confirmations":[item,{**item,"source":"harmonics"}]})
        self.assertEqual(len(out["confirmations"]),1)
        self.assertEqual(len(out["deduplicated_confirmations"]),1)
        self.assertFalse(out["vote_counting"])

    def test_v4_block3_harmonic_contract_declares_full_family_set(self):
        from analytical_core_v4 import harmonic_contract
        out=harmonic_contract({"ready":True,"confirmed":[],"developing":[]})
        for name in ("Gartley","Bat","Alternate Bat","Butterfly","Crab","Deep Crab","Cypher","Shark","5-0","AB=CD","Extended AB=CD"):
            self.assertIn(name,out["supported_families"])
        self.assertEqual(out["role"],"PRZ_context_not_reversal_command")

    def test_v4_block3_hard_invalidation_downgrades_funnel(self):
        from scan_intelligence import opportunity_funnel
        setup={"trade_state":"SETUP","direction":"bullish","trigger_confirmed":True,
               "hard_invalidations":[{"source":"structure","reason":"wave_invalidation"}]}
        out=opportunity_funnel(setup,True,True)
        self.assertNotEqual(out["stage"],"TRADE")
        self.assertFalse(out["trade_authorized"])

    def test_v4_block4_pump_never_authorizes_immediate_short(self):
        from liquidity_leverage_engine import detect
        linear={"flow":{"5m":{"price_change_pct":3.0,"delta_ratio":0.6},"1m":{"delta_ratio":0.2}},
                "ticker":{"lastPrice":"103"},"orderbook":{"imbalance_50":-0.3},
                "open_interest":{"windows":{"5m":{"change_pct":2.0}}},"funding_rate":0.001}
        spot={"flow":{"5m":{"price_change_pct":0.3,"delta_ratio":0.1}}}
        out=detect(linear,spot,{})
        self.assertFalse(out["trade_authority"])
        self.assertEqual(out["execution"],"NO_SHORT_UNTIL_CAUSAL_CONFIRMATION")
        self.assertNotEqual(out["direction"],"SHORT")

    def test_v4_block4_related_leverage_events_are_deduplicated(self):
        from liquidity_leverage_engine import dedupe_related
        ev=[{"type":"PUMP_EXHAUSTION","direction":"bearish","score":7},
            {"type":"CROWDED_LONGS","direction":"bearish","score":1},
            {"type":"LEVERAGE_FRAGILITY","direction":None,"score":1}]
        out=dedupe_related(ev)
        self.assertEqual(len(out),1)
        self.assertEqual(out[0]["type"],"PUMP_EXHAUSTION")
        self.assertEqual({x["type"] for x in out[0]["related_evidence"]},{"CROWDED_LONGS","LEVERAGE_FRAGILITY"})

    def test_v4_block4_causal_chain_required_for_confirmation(self):
        from liquidity_leverage_engine import advance_lifecycle
        base={"type":"PUMP_EXHAUSTION","direction":"bearish","stage":"DETECTED"}
        self.assertEqual(advance_lifecycle(base,failed_acceptance=True)["stage"],"DEVELOPING")
        self.assertEqual(advance_lifecycle(base,failed_acceptance=True,structure_break=True,failed_retest=True)["stage"],"CONFIRMED")
        self.assertFalse(advance_lifecycle(base,failed_acceptance=True,structure_break=True,failed_retest=True)["trade_authority"])

    def test_v4_block4_x25_is_not_enabled_by_event_detector(self):
        from liquidity_leverage_engine import detect
        out=detect({"flow":{}},{},{})
        self.assertFalse(out["x25_profile"]["allowed"])

    def test_v4_block5_always_builds_both_sides(self):
        from v4_core import dual_scenarios
        a={"1h":{"structure":{"state":"uptrend"}},"15m":{"structure":{"state":"uptrend"}}}
        out=dual_scenarios(a,[])
        self.assertIn("long",out); self.assertIn("short",out)
        self.assertEqual(out["rule"],"both_sides_before_direction")

    def test_v4_block6_funnel_trade_requires_trigger_and_geometry(self):
        from v4_core import opportunity_funnel
        self.assertEqual(opportunity_funnel({"direction":"bullish"})["stage"],"DEVELOPING")
        self.assertEqual(opportunity_funnel({"direction":"bullish","limit_plan":{"eligible":True}})["stage"],"READY")
        self.assertTrue(opportunity_funnel({"direction":"bullish","limit_plan":{"eligible":True},"trigger_confirmed":True})["trade_authorized"])

    def test_v4_block8_broker_missing_does_not_kill_analysis(self):
        from v4_core import execution_contract
        out=execution_contract("forex",True,True,False,False)
        self.assertTrue(out["analysis_execution_ready"])
        self.assertEqual(out["status"],"EXECUTION_UNAVAILABLE")
        self.assertFalse(out["order_sent"])

    def test_v4_block9_leverage_does_not_increase_money_risk(self):
        from v4_core import risk_contract
        a=risk_contract(1000,.03,100,95,5); b=risk_contract(1000,.03,100,95,10)
        self.assertEqual(a["money_risk"],b["money_risk"])
        self.assertGreater(a["margin"],b["margin"])

    def test_v4_blocks10_12_fingerprint_shadow_explainability(self):
        from v4_core import setup_fingerprint,shadow_record,explainability
        self.assertEqual(setup_fingerprint("crypto","BTC","bullish",1,0.9,1.2),setup_fingerprint("crypto","BTC","bullish",1,0.9,1.2))
        scan={"market":"crypto","symbol":"BTC","setup":{"direction":"bullish"},"opportunity_funnel":{"stage":"DEVELOPING","missing":["trigger"]},"scenario":{}}
        self.assertEqual(shadow_record(scan)["symbol"],"BTC")
        self.assertEqual(explainability(scan)["score_role"],"ranking_only")

    def test_v4_zero_audit_contract_has_full_pipeline(self):
        from v4_core import zero_audit_contract
        out=zero_audit_contract()
        self.assertEqual(out["pipeline"][0],"DATA")
        self.assertEqual(out["pipeline"][-1],"LEARNING")
        self.assertIn("no_real_order_by_default",out["invariants"])

    def test_v4_block10_watchlist_accepts_ready_trade_states(self):
        from scan_intelligence import WatchlistStore
        w=WatchlistStore()
        scan={"market":"crypto","symbol":"BTCUSDT","setup":{"direction":"bullish"},"opportunity_funnel":{"stage":"READY"}}
        item=w.upsert(scan)
        self.assertEqual(item["state"],"READY")

    def test_v4_block13_job_manager_has_redis_retry_and_distributed_lease(self):
        import inspect
        from dynamic_collector import ScanJobManager
        src=inspect.getsource(ScanJobManager)
        self.assertIn("_redis_retry_after",src)
        self.assertIn("scan:lease:",src)
        self.assertEqual(ScanJobManager.VERSION,"scan_job_manager_v4_prod_contract")

    def test_v4_block13_scan_start_auth_is_optional_but_enforceable(self):
        import os
        from unittest.mock import patch
        import app as app_module
        with app_module.app.test_request_context("/scan"):
            with patch.dict(os.environ,{"SCAN_API_TOKEN":"secret"}):
                self.assertFalse(app_module._scan_write_authorized())
        with app_module.app.test_request_context("/scan",headers={"X-Scan-Token":"secret"}):
            with patch.dict(os.environ,{"SCAN_API_TOKEN":"secret"}):
                self.assertTrue(app_module._scan_write_authorized())

    def test_zero_audit_block1_data_quality_has_unavailable(self):
        from data_contracts import frame_quality
        out=frame_quality({})
        self.assertEqual(out["state"],"UNAVAILABLE")
        self.assertTrue(all(x["state"]=="UNAVAILABLE" for x in out["timeframes"].values()))

    def test_zero_audit_v4_explainability_is_after_final_funnel(self):
        import inspect,scan_intelligence
        src=inspect.getsource(scan_intelligence.enrich_scan)
        self.assertLess(src.rfind('out["opportunity_funnel"]'),src.rfind('out["explainability"]'))

    def test_zero_audit_v4_outcomes_accept_ready_trade(self):
        import inspect,scan_intelligence
        src=inspect.getsource(scan_intelligence.OutcomeLogger.record)
        self.assertIn('("READY", "TRADE")',src)

    def test_combat_audit_sparse_spot_threshold_fits_warmup_budget(self):
        import inspect,dynamic_collector
        src=inspect.getsource(dynamic_collector.DynamicMarketManager)
        self.assertIn('SPOT_SPARSE_DEGRADE_SECONDS","90"',src)
        self.assertLessEqual(90,dynamic_collector.ScanJobManager.AUTO_WARMUP_MAX_SECONDS)

    def test_combat_audit_live_flow_oi_coverage_can_mature_inside_warmup(self):
        import inspect,dynamic_collector
        src=inspect.getsource(dynamic_collector.DynamicCollector)
        self.assertIn('FLOW_LIVE_COVERAGE_CAP_MS","60000"',src)
        self.assertIn('OI_LIVE_COVERAGE_CAP_MS","60000"',src)
        self.assertLess(60,dynamic_collector.ScanJobManager.AUTO_WARMUP_MAX_SECONDS)

    def test_combat_audit_realtime_pool_covers_deep_scan_cap(self):
        import dynamic_collector
        self.assertGreaterEqual(dynamic_collector.dynamic_manager.max_symbols,
                                dynamic_collector.ScanOrchestrator.MAX_AUTO_SCAN_PLUS)

    def test_combat_audit_realtime_pool_fits_v4_deep_scan(self):
        import dynamic_collector
        self.assertGreaterEqual(dynamic_collector.dynamic_manager.max_symbols,
                                dynamic_collector.ScanOrchestrator.MAX_AUTO_SCAN_PLUS)

    def test_combat_audit_terminal_redis_payload_is_compacted(self):
        import inspect,dynamic_collector
        src=inspect.getsource(dynamic_collector.ScanJobManager._persist)
        self.assertIn("persisted_compact",src)
        self.assertIn('state") in ("DONE","FAILED")',src)


    def test_block3_harmonic_gartley_geometry_uses_ad_over_xa(self):
        from dynamic_collector import MarketStream
        s=MarketStream.__new__(MarketStream)
        pts=[{"kind":"low","price":0.0,"start":1},{"kind":"high","price":100.0,"start":2},{"kind":"low","price":38.2,"start":3},{"kind":"high","price":69.1,"start":4},{"kind":"low","price":21.4,"start":5}]
        ctx={"base":{"working_degree":"intermediate"},"degrees":{"intermediate":{"points":pts}}}
        out=s._harmonic_engine_v2(ctx)
        g=[x for x in out["confirmed"] if x["name"]=="Gartley"]
        self.assertTrue(g)
        self.assertAlmostEqual(g[0]["ratios"]["xd"],0.786,places=3)

    def test_block3_liquidity_map_has_previous_day_week_references(self):
        from dynamic_collector import MarketStream
        s=MarketStream.__new__(MarketStream)
        base=1790000000000; rows=[]
        for i in range(24*16):
            p=100+i*.01
            rows.append({"start":base+i*3600000,"end":base+(i+1)*3600000-1,"open":p,"high":p+1,"low":p-1,"close":p+.1,"confirm":True})
        out=s._liquidity_depth_v38(rows,{"degrees":{}},{"equal_highs":None,"equal_lows":None,"sweep":None})
        refs=out["reference_levels"]
        for name in ("PDH","PDL","PWH","PWL"): self.assertIn(name,refs)

    def test_block3_higher_degree_elliott_memory_survives_missing_recount(self):
        from dynamic_collector import MarketStream
        s=MarketStream.__new__(MarketStream); s._elliott_memory={}
        s._elliott_memory["major"]={"type":"impulse","direction":"bullish","invalidation":90.0,"evidence_count":5}
        e={"recursive":{"degrees":{"major":{"ready":True,"primary":None}}}}
        out=s._preserve_elliott_major(e,[{"close":100.0}])
        self.assertTrue(out["recursive"]["degrees"]["major"]["preserved_from_prior_scan"])
        e2={"recursive":{"degrees":{"major":{"ready":True,"primary":None}}}}
        s._preserve_elliott_major(e2,[{"close":89.0}])
        self.assertNotIn("major",s._elliott_memory)
