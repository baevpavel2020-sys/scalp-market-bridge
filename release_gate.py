"""Deterministic Scan+ release gates for Stages 7-10."""
from scan_architecture import MARKETS, market_block_policy, pipeline_contract
from analytical_depth import VERSION as ANALYTICAL_DEPTH_VERSION
from dynamic_collector import ScanJobManager

RELEASE_VERSION = "scanplus_v4_ru_stocks_release_gate"
RELEASE_STAGES = (7, 8, 9, 10)

def acceptance_report():
    policies = {m: market_block_policy(m) for m in MARKETS}
    return {
        "release_version": RELEASE_VERSION,
        "stages": list(RELEASE_STAGES),
        "markets": list(MARKETS),
        "market_policies": policies,
        "architecture": pipeline_contract(),
        "analytical_depth_version": ANALYTICAL_DEPTH_VERSION,
        "job_manager_version": ScanJobManager.VERSION,
        "limits": {
            "max_jobs": ScanJobManager.MAX_JOBS,
            "job_ttl_seconds": ScanJobManager.JOB_TTL_SECONDS,
            "workers": getattr(ScanJobManager._executor, "_max_workers", None),
        },
        "network_free": True,
    }

def run_gates():
    report = acceptance_report()
    failures = []
    if tuple(report["markets"]) != ("crypto", "stocks", "ru_stocks", "forex", "commodities"):
        failures.append("market_registry")
    for market, policy in report["market_policies"].items():
        if not policy["enabled"] or not policy["priority"]:
            failures.append(f"market_policy:{market}")
        if market != "crypto" and policy["flow"]:
            failures.append(f"flow_gate:{market}")
        if market != "crypto" and policy["manipulation"]:
            failures.append(f"manipulation_gate:{market}")
    workers = report["limits"]["workers"]
    if workers is not None and not (1 <= workers <= 2):
        failures.append("job_worker_bound")
    if report["limits"]["max_jobs"] < 1 or report["limits"]["job_ttl_seconds"] < 60:
        failures.append("job_retention_policy")
    report["passed"] = not failures
    report["failures"] = failures
    return report


def validate_unified_result(result):
    """Runtime release gate for one completed five-market unified job."""
    result=result or {}
    failures=[]
    expected=("crypto","stocks","ru_stocks","forex","commodities")
    markets=tuple(result.get("markets") or ())
    if markets != expected:
        failures.append("runtime_market_registry")
    if result.get("five_market_complete") is not True:
        failures.append("five_market_incomplete")
    if result.get("missing_markets"):
        failures.append("missing_markets")
    errors=result.get("errors") or {}
    if errors:
        failures.append("runtime_errors")
    health=result.get("market_health") or {}
    for market in expected:
        state=(health.get(market) or {}).get("status")
        if state not in ("PASS",):
            failures.append(f"market_health:{market}:{state or 'MISSING'}")
    crypto=result.get("crypto") or {}
    for item in crypto.get("scan_plus_results") or []:
        manipulation=item.get("manipulation")
        if manipulation is None:
            failures.append(f"manipulation_missing:{item.get('symbol','unknown')}")
    external=result.get("external") or {}
    for item in (external.get("ru_stocks") or {}).get("results") or []:
        for obj in (item,item.get("setup") or {}):
            direction=str(obj.get("direction") or obj.get("side") or "").lower()
            if direction in ("short","sell","bearish") and (obj.get("tradeable") is True or obj.get("execution_ready") is True):
                failures.append(f"ru_short_escape:{item.get('symbol','unknown')}")
    return {"passed":not failures,"failures":failures,"expected_markets":list(expected)}
