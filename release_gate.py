"""Deterministic Scan+ V4 release gates and zero audit."""
from scan_architecture import MARKETS, market_block_policy, pipeline_contract
from analytical_depth import VERSION as ANALYTICAL_DEPTH_VERSION
from dynamic_collector import ScanJobManager
from v4_core import VERSION as V4_VERSION, zero_audit_contract

RELEASE_VERSION = "scanplus_v4_blocks1_14"
RELEASE_STAGES = tuple(range(1,15))

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
        "v4_version":V4_VERSION,
        "zero_audit":zero_audit_contract(),
    }

def run_gates():
    report = acceptance_report()
    failures = []
    if tuple(report["markets"]) != ("crypto", "stocks", "forex", "commodities"):
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
    audit=report["zero_audit"]
    required={"both_sides_before_direction","hard_invalidation_absolute","event_has_no_trade_authority","net_rr_before_trade","no_real_order_by_default"}
    if not required.issubset(set(audit.get("invariants") or [])): failures.append("zero_audit_invariants")
    report["passed"] = not failures
    report["failures"] = failures
    return report
