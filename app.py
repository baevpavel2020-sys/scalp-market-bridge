import concurrent.futures
import os
import hmac
import json
import requests
from flask import Flask, jsonify, request

from collector import collector
from spot_collector import spot_collector
from dynamic_collector import (dynamic_manager, run_bybit_prescan_ws_probe, run_prescan, start_scan_auto_job, start_scan_unified_job, start_scan_batch_job, get_scan_job, get_latest_unified_scan_job)
from multi_market_adapters import ExternalMarketAdapter, external_universe, MarketProfileRouter
from market_event_engine import detect_events, build_setup_plan
from scan_intelligence import WATCHLIST, backtest_event_setups, walk_forward_backtest, edge_discovery

import time
import threading
app = Flask(__name__)


def ensure_market_collectors():
    collector.ensure_running()
    spot_collector.ensure_running()

BYBIT_URLS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]


@app.get("/system/status")
def system_status():
    """Deterministic runtime contract/status surface; no market calls."""
    try:
        from release_gate import run_gates
        from dynamic_collector import ScanJobManager
        gate=run_gates()
        return jsonify({
            "service":"scalp-market-bridge",
            "release":gate["release_version"],
            "release_gates":gate,
            "job_manager": {
                "version":ScanJobManager.VERSION,
                "max_jobs":ScanJobManager.MAX_JOBS,
                "ttl_seconds":ScanJobManager.JOB_TTL_SECONDS,
                "workers":getattr(ScanJobManager._executor,"_max_workers",None),
            },
        }),200 if gate["passed"] else 503
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/audit/v4")
def audit_v4():
    """Deterministic executable V4 audit: release gates + unittest regression suite."""
    try:
        import io, unittest
        suite=unittest.defaultTestLoader.loadTestsFromName("test_scanplus")
        stream=io.StringIO()
        result=unittest.TextTestRunner(stream=stream,verbosity=1).run(suite)
        from release_gate import run_gates
        gates=run_gates()
        passed=bool(result.wasSuccessful() and gates.get("passed"))
        payload={"status":"PASS" if passed else "FAIL","tests":{"run":result.testsRun,"failures":len(result.failures),"errors":len(result.errors),
                        "failed":[{"test":str(t),"traceback":tb[-4000:]} for t,tb in (result.failures+result.errors)],"details":stream.getvalue()[-12000:]},"release_gates":gates}
        app._last_v4_audit=payload
        print("V4_AUDIT_RESULT "+json.dumps(payload,default=str,separators=(",",":")),flush=True)
        return jsonify(payload),200 if passed else 503
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/audit/v4/run-scan")
def audit_v4_run_scan():
    """Combat-audit helper: starts the same unified production job used by Scan."""
    denied=_require_scan_write_auth()
    if denied: return denied
    top=max(1,min(int(request.args.get("top",8)),8))
    shortlist=max(top,min(int(request.args.get("shortlist",30)),30))
    job_id=start_scan_unified_job(top_n=top,shortlist=shortlist,markets=["crypto","stocks","forex","commodities"])
    return jsonify({"status":"STARTED","job_id":job_id,"audit":"v4_combat"}),202

@app.get("/audit/v4/last")
def audit_v4_last():
    """Read the latest in-process audit result without re-running tests."""
    return jsonify(getattr(app,"_last_v4_audit",{"status":"NOT_RUN"})),200

@app.get("/health")
def health():
    # Render health probe: no market/API calls and no locks.
    return jsonify({"service":"scalp-market-bridge","status":"healthy","version":"health_v1"}), 200

@app.get("/")
def home():
    return jsonify({"service": "scalp-market-bridge", "status": "online"})


@app.get("/test-bybit")
def test_bybit():
    results = {}
    for base in BYBIT_URLS:
        try:
            r = requests.get(f"{base}/v5/market/time", timeout=8)
            results[base] = {"status": r.status_code, "text": r.text[:500]}
        except Exception as exc:
            results[base] = {"error": str(exc)}
    return jsonify(results)


@app.get("/provider-test")
def provider_test():
    """One-shot Render -> Bybit public linear WebSocket feasibility test."""
    try:
        raw_symbols = request.args.get("symbols", "")
        symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()] or None

        raw_timeout = request.args.get("timeout", "12")
        try:
            timeout = float(raw_timeout)
        except (TypeError, ValueError):
            return jsonify({"error": "timeout must be a number"}), 400

        result = run_bybit_prescan_ws_probe(symbols=symbols, timeout=timeout)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({
            "status": "FAIL",
            "error": f"{type(exc).__name__}: {exc}",
            "source": "bybit_public_linear_websocket",
        }), 500


@app.get("/prescan")
def prescan_manual():
    try:
        raw_top = request.args.get("top", "6")
        raw_shortlist = request.args.get("shortlist", "30")
        try:
            top_n = int(raw_top)
            shortlist = int(raw_shortlist)
        except (TypeError, ValueError):
            return jsonify({"error": "top and shortlist must be integers"}), 400
        raw_symbols = request.args.get("symbols", "")
        symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()] or None
        return jsonify(run_prescan(universe=symbols, top_n=top_n, shortlist=shortlist))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/prescan/status")
def prescan_status():
    """Read-only PreScan cache/provider readiness; never starts a Scan+ job."""
    try:
        from dynamic_collector import OnDemandPreScanService
        return jsonify({
            "engine_version": OnDemandPreScanService.VERSION,
            "warm_cache": OnDemandPreScanService.warm_status(),
            "warm_cache_enabled": OnDemandPreScanService.WARM_CACHE_ENABLED,
            "ticker_cache_ttl_seconds": OnDemandPreScanService.TICKER_CACHE_TTL,
            "history_cache_ttl_seconds": OnDemandPreScanService.CACHE_TTL,
            "disk_cache_ttl_seconds": OnDemandPreScanService.DISK_CACHE_TTL,
            "history_workers": OnDemandPreScanService.HISTORY_WORKERS,
            "analysis_workers": OnDemandPreScanService.ANALYSIS_WORKERS,
            "history_global_timeout_seconds": OnDemandPreScanService.HISTORY_GLOBAL_TIMEOUT,
            "providers": ["okx_swap_rest", "kucoin_futures_rest", "binance_spot_marketdata"],
        })
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/market/BTCUSDT")
def market_btcusdt():
    ensure_market_collectors()
    return jsonify(collector.get_snapshot())


@app.get("/market/BTCUSDT/spot")
def market_btcusdt_spot():
    ensure_market_collectors()
    return jsonify(spot_collector.get_snapshot())


@app.get("/market/<symbol>")
def market_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.snapshot(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/scan-markets")
def scan_markets():
    """Analysis-only scan for Forex, commodities and Bybit xStocks."""
    try:
        adapter=ExternalMarketAdapter()
        requested=request.args.get("markets","forex,commodities,stocks").split(",")
        universe=external_universe()
        out={"adapter_version":adapter.VERSION,"markets":{}}
        for market in requested:
            market=market.strip().lower()
            if market not in universe: continue
            symbols=universe[market]
            # ExternalMarketAdapter owns provider fallback semantics. Forex and
            # commodities can use public Yahoo data when Twelve Data is absent;
            # readiness must be decided per symbol/timeframe, never by API-key presence.
            workers=min(3,len(symbols)) or 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                out["markets"][market]=list(pool.map(lambda s: adapter.scan(market,s), symbols))
        return jsonify(out)
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/intelligence/backtest/<symbol>")
def intelligence_backtest(symbol):
    try:
        market=request.args.get("market","crypto").strip().lower()
        if market not in ("crypto","stocks","forex","commodities"):
            return jsonify({"error":"unsupported market"}),400
        limit=max(30,min(int(request.args.get("limit","500")),1000))
        if market=="crypto":
            symbol=dynamic_manager.normalize_symbol(symbol)
            snap=dynamic_manager.snapshot(symbol)
            rows=list(((snap.get("linear") or {}).get("candles") or {}).get("15") or [])[-limit:]
        else:
            data=ExternalMarketAdapter().scan(market,symbol)
            rows=list((data.get("frames") or {}).get("15m") or [])[-limit:]
        return jsonify(backtest_event_setups(market,symbol,rows,detect_events,build_setup_plan,max_checkpoints=100))
    except ValueError as exc:
        return jsonify({"error":str(exc)}),400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/intelligence/walkforward/<symbol>")
def intelligence_walkforward(symbol):
    try:
        market=request.args.get("market","crypto").strip().lower()
        if market not in ("crypto","stocks","forex","commodities"):
            return jsonify({"error":"unsupported market"}),400
        limit=max(30,min(int(request.args.get("limit","1000")),5000))
        train=max(100,min(int(request.args.get("train","300")),2000))
        test=max(20,min(int(request.args.get("test","100")),1000))
        if market=="crypto":
            symbol=dynamic_manager.normalize_symbol(symbol)
            snap=dynamic_manager.snapshot(symbol)
            rows=list(((snap.get("linear") or {}).get("candles") or {}).get("15") or [])[-limit:]
        else:
            data=ExternalMarketAdapter().scan(market,symbol)
            rows=list((data.get("frames") or {}).get("15m") or [])[-limit:]
        return jsonify(walk_forward_backtest(market,symbol,rows,detect_events,build_setup_plan,
                                             train_bars=train,test_bars=test))
    except ValueError as exc:
        return jsonify({"error":str(exc)}),400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/intelligence/watchlist")
def intelligence_watchlist():
    market=request.args.get("market")
    return jsonify({"version":"intelligence_v2","items":WATCHLIST.snapshot(market=market)})


@app.get("/intelligence/edge")
def intelligence_edge():
    return jsonify(edge_discovery())

@app.get("/scan-live")
def scan_live():
    """Unified LIVE scan: Crypto + xStocks + Forex + Commodities."""
    try:
        top_n=max(1,min(int(request.args.get("top","5")),6))
        shortlist=int(request.args.get("shortlist","30"))
        raw=request.args.get("markets","crypto,stocks,forex,commodities")
        markets=[x.strip().lower() for x in raw.split(",") if x.strip()]
        return jsonify(start_scan_unified_job(top_n=top_n,shortlist=shortlist,markets=markets)),202
    except ValueError as exc:
        return jsonify({"error":str(exc)}),400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500


def _scan_write_authorized():
    """Optional production auth. If SCAN_API_TOKEN is configured, starts require it."""
    token=os.environ.get("SCAN_API_TOKEN")
    if not token: return True
    supplied=request.headers.get("X-Scan-Token") or request.args.get("token")
    return bool(supplied and hmac.compare_digest(str(supplied),str(token)))

def _require_scan_write_auth():
    if _scan_write_authorized(): return None
    return jsonify({"status":"UNAUTHORIZED","error":"scan start authorization required"}),401

@app.get("/scan")
def scan_command():
    denied=_require_scan_write_auth()
    if denied: return denied
    """Canonical user-facing Scan command: Prescan -> eligible candidates -> Scan+.
    Non-blocking: returns a job handle; poll /scan-job/<job_id> for the final result.
    """
    try:
        top_n = max(1, min(int(request.args.get("top", "8")), 8))
        shortlist = max(top_n, min(int(request.args.get("shortlist", "30")), 100))
        return jsonify(start_scan_auto_job(top_n=top_n, shortlist=shortlist)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500

@app.get("/scan-auto")
def scan_auto():
    denied=_require_scan_write_auth()
    if denied: return denied
    """Compatibility route: now STARTS a background job instead of blocking."""
    try:
        top_n = int(request.args.get("top", "8"))
        shortlist = int(request.args.get("shortlist", "30"))
        return jsonify(start_scan_auto_job(top_n=top_n, shortlist=shortlist)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/scan-auto/start")
def scan_auto_start():
    denied=_require_scan_write_auth()
    if denied: return denied
    try:
        top_n = int(request.args.get("top", "8"))
        shortlist = int(request.args.get("shortlist", "30"))
        return jsonify(start_scan_auto_job(top_n=top_n, shortlist=shortlist)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/scan-batch")
def scan_batch():
    denied=_require_scan_write_auth()
    if denied: return denied
    """Compatibility route: now STARTS a background job instead of blocking."""
    try:
        raw = request.args.get("symbols", "")
        symbols = [s.strip() for s in raw.split(",") if s.strip()]
        return jsonify(start_scan_batch_job(symbols)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/scan-batch/start")
def scan_batch_start():
    denied=_require_scan_write_auth()
    if denied: return denied
    try:
        raw = request.args.get("symbols", "")
        symbols = [s.strip() for s in raw.split(",") if s.strip()]
        return jsonify(start_scan_batch_job(symbols)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


def _compact_scan_result(job):
    """Small user-facing projection; never recomputes trading decisions."""
    result=(job or {}).get("result") or {}
    items=[]
    buckets=[("crypto",(result.get("crypto") or {}).get("scan_plus_results") or [])]
    for market,bucket in (result.get("external") or {}).items():
        if isinstance(bucket,dict): buckets.append((market,bucket.get("results") or bucket.get("scan_plus_results") or []))
    for market,rows in buckets:
        for row in rows:
            funnel=row.get("opportunity_funnel") or {}
            setup=row.get("setup") or {}
            stage=funnel.get("stage") or setup.get("opportunity_state") or row.get("trade_state")
            if stage not in ("TRADE","READY","WATCH","DEVELOPING","EARLY"): continue
            items.append({"market":market,"symbol":row.get("symbol"),"state":stage,
                "direction":setup.get("side") or row.get("direction"),"entry":setup.get("entry"),
                "entry_zone":setup.get("entry_zone"),"stop":setup.get("stop"),"targets":setup.get("targets"),
                "risk_reward":setup.get("risk_reward"),"trigger_state":row.get("trigger_state"),
                "failed_requirements":setup.get("failed_requirements") or funnel.get("missing") or []})
    rank={"TRADE":0,"READY":1,"WATCH":2,"DEVELOPING":3,"EARLY":4}
    items.sort(key=lambda x:(rank.get(x.get("state"),9), -(x.get("risk_reward") or 0)))
    manipulation=[]
    for row in (result.get("crypto") or {}).get("scan_plus_results") or []:
        m=row.get("manipulation") or {}
        if isinstance(m,dict):
            manipulation.append({"symbol":row.get("symbol"),"status":m.get("status"),
                "signal":m.get("signal"),"score":m.get("score"),"classification":m.get("classification")})
    telemetry=result.get("telemetry") or {}
    return {"job_id":job.get("job_id"),"state":job.get("state"),"markets":result.get("markets") or [],
        "summary":{"trade":sum(x["state"]=="TRADE" for x in items),"ready":sum(x["state"]=="READY" for x in items),
                   "watch":sum(x["state"] in ("WATCH","DEVELOPING","EARLY") for x in items)},
        "opportunities":items,"manipulation":manipulation,"health":{"error_count":telemetry.get("error_count",len(result.get("errors") or {})),
        "provider_degraded":telemetry.get("provider_degraded",0)},"error":job.get("error")}

@app.get("/scan-check")
def scan_check():
    """Read-only canonical check: never starts a job."""
    try:
        job = get_latest_unified_scan_job()
        if job is None:
            return jsonify({
                "status": "NO_SCAN",
                "message": "No unified Scan job exists in the current process.",
            }), 404
        active = job.get("state") in ("QUEUED", "RUNNING")
        payload={"status":"RUNNING" if active else job.get("state"),"active":active,
                 "job_id":job.get("job_id"),"progress":job.get("progress") or {}}
        if not active: payload["result"]=_compact_scan_result(job)
        return jsonify(payload), 200
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"}),500


@app.get("/scan-latest")
def scan_latest():
    """Alias for the read-only canonical Scan check endpoint."""
    return scan_check()


@app.get("/scan-job/<job_id>")
def scan_job_status(job_id):
    job = get_scan_job(job_id)
    if job is None:
        return jsonify({"error":"job not found or expired","job_id":job_id}), 404
    return jsonify(job)


@app.get("/scan/<symbol>")
def scan_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.scan(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/diagnostics/<symbol>")
def diagnostics_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.diagnostics(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


def _start_scan_on_boot_if_enabled():
    """Transport hook: lets an authenticated deployment trigger one canonical Scan job."""
    if str(os.environ.get("SCAN_ON_BOOT","0")).strip().lower() not in ("1","true","yes","on"): return
    def _boot_scan():
        try:
            time.sleep(2)
            job=start_scan_unified_job(top_n=8,shortlist=30,markets=["crypto","stocks","forex","commodities"])
            job_id=job.get("job_id") if isinstance(job,dict) else str(job)
            state=job.get("state") if isinstance(job,dict) else "STARTED"
            print("SCAN_BOOT_TRIGGER "+json.dumps({"job_id":job_id,"state":state},separators=(",",":")),flush=True)
            # Operator transport: wait for this exact job and emit only the compact
            # user-facing projection. Trading decisions are never recomputed here.
            deadline=time.time()+1200
            while time.time()<deadline:
                current=get_scan_job(job_id)
                if isinstance(current,dict) and current.get("state") not in ("QUEUED","RUNNING"):
                    print("SCAN_BOOT_RESULT "+json.dumps(_compact_scan_result(current),separators=(",",":"),default=str),flush=True)
                    return
                time.sleep(5)
            print("SCAN_BOOT_RESULT "+json.dumps({"job_id":job_id,"status":"TIMEOUT"},separators=(",",":")),flush=True)
        except Exception as exc:
            print("SCAN_BOOT_TRIGGER "+json.dumps({"status":"FAIL","error":f"{type(exc).__name__}:{exc}"},separators=(",",":")),flush=True)
    threading.Thread(target=_boot_scan,name="scan-boot-trigger",daemon=True).start()

_start_scan_on_boot_if_enabled()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
