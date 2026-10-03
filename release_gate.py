"""Deterministic Scan+ release gates for Stages 7-10."""
from scan_architecture import MARKETS, market_block_policy, pipeline_contract
from analytical_depth import VERSION as ANALYTICAL_DEPTH_VERSION
from dynamic_collector import ScanJobManager

RELEASE_VERSION = "scanplus_v3_9_stage7_10"
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
