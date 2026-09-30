"""Conservative post-pump detector pipeline built from observable Crypto data.

Every stage reports observed/confirmed separately. Unknown inputs stay unavailable.
Thresholds are initial engineering defaults and must be calibrated from replay data.
"""
from typing import Any, Mapping

from scan_plus.markets.crypto.observables import extract_crypto_observables


def _stage(observed, confirmed, score=0.0, reasons=()):
    return {"observed":bool(observed),"confirmed":bool(observed and confirmed),
            "score":round(max(0.0,min(1.0,float(score or 0.0))),4),
            "reasons":list(reasons)}


def detect_post_pump_pipeline(scan: Mapping[str, Any], observables=None, adaptive_abnormal=None):
    o=observables or extract_crypto_observables(scan)
    name=o["window_name"]
    price_change=o["price_change_pct"]
    perp_delta=o["perp_delta_ratio"]
    spot_delta=o["spot_delta_ratio"]
    oi_change=o["oi_change_pct"]
    funding=o["funding_rate"]
    book_imb=o["book_imbalance"]
    quality=o["sample_quality"]

    # Initial abnormality detector: strong short-horizon move plus aggressive buying.
    # It deliberately does not claim statistical abnormality without a rolling baseline.
    abnormal_observed=price_change is not None and perp_delta is not None
    fixed_abnormal=bool(abnormal_observed and price_change >= 1.0 and perp_delta >= 0.35 and quality >= 0.20)
    if adaptive_abnormal is None:
        abnormal_confirmed=fixed_abnormal
        abnormal_source="fixed_fallback"
    else:
        abnormal_confirmed=bool(abnormal_observed and adaptive_abnormal and perp_delta >= 0.35 and quality >= 0.20)
        abnormal_source="adaptive_baseline"
    abnormal=_stage(abnormal_observed,abnormal_confirmed,
                    min(1.0,(max(price_change or 0,0)/2.5 + max(perp_delta or 0,0))/2),
                    (f"window={name}",f"price_change_pct={price_change}",f"perp_delta={perp_delta}",f"source={abnormal_source}"))

    # Aggression efficiency: high aggressive-buy delta with weak price progress.
    eff_observed=price_change is not None and perp_delta is not None
    buy_aggression=max(perp_delta or 0.0,0.0)
    progress=max(price_change or 0.0,0.0)
    inefficient=bool(eff_observed and buy_aggression >= 0.45 and progress <= 0.35)
    efficiency=_stage(eff_observed,inefficient,
                      min(1.0,buy_aggression*(1.0-min(progress/0.35,1.0)) if progress>=0 else buy_aggression),
                      ("aggressive_buying_without_price_progress",) if inefficient else ())

    # Absorption proxy needs book opposition; otherwise unavailable rather than false.
    absorption_observed=perp_delta is not None and book_imb is not None
    absorption_confirmed=bool(absorption_observed and perp_delta >= 0.35 and book_imb <= -0.12)
    absorption=_stage(absorption_observed,absorption_confirmed,
                      min(1.0,max(perp_delta or 0,0)*max(-(book_imb or 0),0)*4),
                      ("buyers_aggressive","ask_depth_dominant") if absorption_confirmed else ())

    # Spot/perp divergence.
    divergence_observed=perp_delta is not None and spot_delta is not None
    divergence_confirmed=bool(divergence_observed and perp_delta >= 0.25 and spot_delta <= 0.05)
    divergence=_stage(divergence_observed,divergence_confirmed,
                      min(1.0,max((perp_delta or 0)-(spot_delta or 0),0)),
                      ("perp_buying_not_confirmed_by_spot",) if divergence_confirmed else ())

    # Leverage fragility: rising OI during the pump plus non-negative funding.
    fragility_observed=oi_change is not None and funding is not None
    fragility_confirmed=bool(fragility_observed and oi_change > 0 and funding >= 0)
    fragility=_stage(fragility_observed,fragility_confirmed,
                     min(1.0,max(oi_change or 0,0)/2.0 + max(funding or 0,0)*10),
                     ("oi_expanding","funding_nonnegative") if fragility_confirmed else ())

    # Failed acceptance uses actual liquidity sweep plus inability of execution state to confirm continuation.
    sweep=o["sweep_direction"]=="bearish"
    setup=o["setup"]
    failed_observed=bool(sweep)
    failed_confirmed=bool(sweep and (setup.get("trigger_direction_aligned") is False or scan.get("trade_state") in ("WAIT_TRIGGER","WAIT_FLOW")))
    failed=_stage(failed_observed,failed_confirmed,0.7 if failed_confirmed else 0.0,
                  ("buy_side_sweep","continuation_not_confirmed") if failed_confirmed else ())

    return {
        "version":"post_pump_v1_observer",
        "source_window":name,
        "abnormal_pump":abnormal,
        "aggression_inefficiency":efficiency,
        "absorption":absorption,
        "spot_perp_divergence":divergence,
        "leverage_fragility":fragility,
        "failed_acceptance":failed,
        "limitations":[
            "abnormal_pump_uses_fixed_fallback_until_adaptive_baseline_is_mature",
            "no_native_liquidation_cluster_feed_yet",
            "structure_break_displacement_retest_not_inferred_here",
        ],
    }

