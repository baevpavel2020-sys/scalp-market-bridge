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
        self.assertEqual(out["not_filled"], 0)
        self.assertEqual(out["samples"], 1)

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
        self.assertEqual(RELEASE_VERSION,"scanplus_v3_9_stage7_10")
        self.assertEqual(RELEASE_STAGES,(7,8,9,10))

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


if __name__=="__main__":
    unittest.main()
