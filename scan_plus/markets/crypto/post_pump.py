"""Conservative post-pump detector pipeline built from observable Crypto data.

Every stage reports observed/confirmed separately. Unknown inputs stay unavailable.
Thresholds are initial engineering defaults and must be calibrated from replay data.
"""
from typing import Any, Mapping


def _f(v):
    try: return float(v)
    except (TypeError, ValueError): return None


def _stage(observed, confirmed, score=0.0, reasons=()):
    return {"observed":bool(observed),"confirmed":bool(observed and confirmed),
            "score":round(max(0.0,min(1.0,float(score or 0.0))),4),
            "reasons":list(reasons)}


def detect_post_pump_pipeline(scan: Mapping[str, Any]):
    ex=scan.get("execution") or {}
    windows=ex.get("windows") or {}
    usable=[(name,w) for name,w in windows.items()
            if w.get("perp_flow_usable") and _f(w.get("perp_price_change_pct")) is not None]
    usable.sort(key=lambda x: {"1m":1,"5m":2,"15m":3,"1h":4}.get(x[0],99))
    name,w=(usable[0] if usable else (None,{}))

    price_change=_f(w.get("perp_price_change_pct"))
    perp_delta=_f(w.get("perp_delta_ratio"))
    spot_delta=_f(w.get("spot_delta_ratio"))
    oi_change=_f(w.get("oi_change_pct"))
    funding=_f(ex.get("funding_rate"))
    book_imb=_f(ex.get("book_imbalance"))
    quality=_f(w.get("sample_quality")) or 0.0

    # Initial abnormality detector: strong short-horizon move plus aggressive buying.
    # It deliberately does not claim statistical abnormality without a rolling baseline.
    abnormal_observed=price_change is not None and perp_delta is not None
    abnormal_confirmed=bool(abnormal_observed and price_change >= 1.0 and perp_delta >= 0.35 and quality >= 0.20)
    abnormal=_stage(abnormal_observed,abnormal_confirmed,
                    min(1.0,(max(price_change or 0,0)/2.5 + max(perp_delta or 0,0))/2),
                    (f"window={name}",f"price_change_pct={price_change}",f"perp_delta={perp_delta}"))

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
    sweep=False
    for tf in ("1","5","15"):
        sweep = sweep or ((_tf(scan,tf).get("liquidity") or {}).get("sweep")=="buy_side_swept")
    setup=scan.get("setup") or {}
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
            "abnormal_pump_uses_fixed_threshold_until_rolling_baseline_is_added",
            "no_native_liquidation_cluster_feed_yet",
            "structure_break_displacement_retest_not_inferred_here",
        ],
    }


def _tf(scan,tf):
    return ((scan.get("timeframes") or {}).get(tf) or {})
