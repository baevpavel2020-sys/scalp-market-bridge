import os
import requests
import threading
import uuid
import time
import concurrent.futures
from flask import Flask, jsonify, request

from collector import collector
from spot_collector import spot_collector
from dynamic_collector import (dynamic_manager, run_bybit_prescan_ws_probe, run_prescan, run_scan_auto, run_scan_single, run_scan_batch, start_scan_auto_job, start_scan_batch_job, get_scan_job)
from scan_plus.markets.crypto.shadow_report import evaluate_recorded_symbol
from scan_plus.default_registry import build_default_registry
from scan_plus.multimarket import MultiMarketOrchestrator

# Full multi-market scans run asynchronously so Gunicorn never waits on provider
# calls long enough to kill the HTTP worker.
MULTIMARKET_JOBS = {}
MULTIMARKET_JOBS_LOCK = threading.RLock()
MULTIMARKET_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="multimarket-scan")
MULTIMARKET_JOB_TTL = 3600

def _multimarket_cleanup_jobs():
    now = time.time()
    with MULTIMARKET_JOBS_LOCK:
        for jid in list(MULTIMARKET_JOBS):
            if now - float(MULTIMARKET_JOBS[jid].get("updated_at", now)) > MULTIMARKET_JOB_TTL:
                MULTIMARKET_JOBS.pop(jid, None)

def _multimarket_start(command):
    _multimarket_cleanup_jobs()
    jid = uuid.uuid4().hex[:16]
    now = time.time()
    with MULTIMARKET_JOBS_LOCK:
        MULTIMARKET_JOBS[jid] = {
            "job_id": jid, "state": "QUEUED", "command": command,
            "created_at": now, "updated_at": now, "result": None, "error": None,
        }

    def worker():
        with MULTIMARKET_JOBS_LOCK:
            job = MULTIMARKET_JOBS.get(jid)
            if job:
                job["state"] = "RUNNING"
                job["started_at"] = time.time()
                job["updated_at"] = time.time()
        try:
            result = MultiMarketOrchestrator(build_default_registry()).scan(command)
            with MULTIMARKET_JOBS_LOCK:
                job = MULTIMARKET_JOBS.get(jid)
                if job:
                    job["state"] = "DONE"
                    job["result"] = result
                    job["finished_at"] = time.time()
                    job["updated_at"] = time.time()
        except Exception as exc:
            with MULTIMARKET_JOBS_LOCK:
                job = MULTIMARKET_JOBS.get(jid)
                if job:
                    job["state"] = "FAILED"
                    job["error"] = f"{type(exc).__name__}: {exc}"
                    job["finished_at"] = time.time()
                    job["updated_at"] = time.time()

    MULTIMARKET_EXECUTOR.submit(worker)
    return {"status": "QUEUED", "job_id": jid, "poll": f"/scan-plus/job/{jid}"}

app = Flask(__name__)

# Do not block module import on background WebSocket initialization.
# Render/Gunicorn must be able to boot the HTTP worker first.
@app.before_request
def ensure_background_collectors():
    collector.ensure_running()
    spot_collector.ensure_running()

BYBIT_URLS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]


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


@app.get("/scan-plus")
def scan_plus():
    try:
        command=request.args.get("command","скан")
        return jsonify(_multimarket_start(command)), 202
    except ValueError as exc:
        return jsonify({"status":"FAIL","error":str(exc)}),400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}),500


@app.get("/scan-plus/job/<job_id>")
def scan_plus_job(job_id):
    _multimarket_cleanup_jobs()
    with MULTIMARKET_JOBS_LOCK:
        job = MULTIMARKET_JOBS.get(job_id)
        if job is None:
            return jsonify({"error":"job not found or expired","job_id":job_id}),404
        return jsonify(dict(job))


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


@app.get("/market/BTCUSDT")
def market_btcusdt():
    return jsonify(collector.get_snapshot())


@app.get("/market/BTCUSDT/spot")
def market_btcusdt_spot():
    return jsonify(spot_collector.get_snapshot())


@app.get("/market/<symbol>")
def market_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.snapshot(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/scan-auto")
def scan_auto():
    """Compatibility route: now STARTS a background job instead of blocking."""
    try:
        top_n = int(request.args.get("top", "6"))
        shortlist = int(request.args.get("shortlist", "30"))
        return jsonify(start_scan_auto_job(top_n=top_n, shortlist=shortlist)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/scan-auto/start")
def scan_auto_start():
    try:
        top_n = int(request.args.get("top", "6"))
        shortlist = int(request.args.get("shortlist", "30"))
        return jsonify(start_scan_auto_job(top_n=top_n, shortlist=shortlist)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/scan-batch")
def scan_batch():
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
    try:
        raw = request.args.get("symbols", "")
        symbols = [s.strip() for s in raw.split(",") if s.strip()]
        return jsonify(start_scan_batch_job(symbols)), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


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


@app.get("/shadow-x25/<symbol>")
def shadow_x25_report(symbol):
    """Diagnostics only. Never authorizes a trade."""
    try:
        min_triggers = int(request.args.get("min_triggers", "30"))
        if min_triggers < 1:
            raise ValueError("min_triggers must be >= 1")
        return jsonify(evaluate_recorded_symbol(symbol, min_triggers=min_triggers))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"status":"FAIL","error":f"{type(exc).__name__}: {exc}"}), 500


@app.get("/diagnostics/<symbol>")
def diagnostics_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.diagnostics(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
