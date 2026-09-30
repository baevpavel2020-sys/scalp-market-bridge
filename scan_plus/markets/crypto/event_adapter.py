"""Normalize legacy Crypto Scan+ output into event-engine evidence.

This module is deliberately conservative: missing evidence stays missing/False.
It never invents a liquidation cluster, absorption signal, or healthy Spot demand.
"""
from typing import Any, Mapping

from scan_plus.core.liquidity_leverage import classify_liquidity_leverage
from scan_plus.markets.crypto.manipulation_x25 import manipulation_x25_state
from scan_plus.markets.crypto.post_pump import detect_post_pump_pipeline


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tf(scan, tf):
    return ((scan.get("timeframes") or {}).get(tf) or {})


def build_crypto_event_evidence(scan: Mapping[str, Any]):
    execution=scan.get("execution") or {}
    direction=(scan.get("direction") or {}).get("bias") or scan.get("direction")
    if isinstance(direction,dict):
        direction=direction.get("direction")

    sweeps=[]
    for tf in ("1","5","15","60"):
        liq=_tf(scan,tf).get("liquidity") or {}
        sweep=liq.get("sweep")
        if sweep:
            sweeps.append((tf,sweep))

    # A buy-side sweep is bearish evidence only when acceptance/structure later fail;
    # a sell-side sweep is bullish evidence under the mirror condition.
    sweep_direction=None
    if any(v=="buy_side_swept" for _,v in sweeps):
        sweep_direction="bearish"
    elif any(v=="sell_side_swept" for _,v in sweeps):
        sweep_direction="bullish"

    windows=execution.get("windows") or {}
    window=windows.get("5m") or windows.get("15m") or {}
    perp_delta=_num(window.get("perp_delta_ratio"))
    spot_delta=_num(window.get("spot_delta_ratio"))
    oi_change=_num(window.get("oi_change_pct"))
    funding=_num(execution.get("funding_rate"))

    divergences=execution.get("flow_divergences") or []
    spot_not_confirming_up=any(
        d.get("type")=="price_up_perp_led_spot_not_confirming" for d in divergences if isinstance(d,dict)
    )

    # Conservative observable proxies. These are candidates, not final truth.
    crowded_long=bool(funding is not None and funding > 0 and oi_change is not None and oi_change > 0)
    perp_led_up=bool(perp_delta is not None and perp_delta > 0 and
                     (spot_delta is None or spot_delta <= 0 or spot_not_confirming_up))

    setup=scan.get("setup") or {}
    trade_state=scan.get("trade_state")
    failed_acceptance=bool(
        sweep_direction=="bearish" and
        (setup.get("trigger_direction_aligned") is False or trade_state in ("WAIT_TRIGGER","WAIT_FLOW"))
    )

    normalized={
        "direction": direction,
        "event_anchor": str(scan.get("generated_at") or "current"),
        "liquidity_sweep": {
            "active": bool(sweep_direction),
            "direction": sweep_direction,
            "confidence": 0.65 if sweep_direction else 0.0,
            "evidence": tuple(f"{tf}:{kind}" for tf,kind in sweeps),
        },
        "failed_acceptance": {
            "active": failed_acceptance,
            "direction": "bearish" if failed_acceptance else None,
            "confidence": 0.55 if failed_acceptance else 0.0,
            "evidence": ("sweep_plus_execution_nonacceptance",) if failed_acceptance else (),
        },
        "crowded_positioning": {
            "active": crowded_long,
            "direction": "bullish" if crowded_long else None,
            "confidence": 0.55 if crowded_long else 0.0,
            "evidence": ("positive_funding","oi_expansion") if crowded_long else (),
        },
        "pump_exhaustion": {
            "active": bool(perp_led_up and failed_acceptance),
            "confidence": 0.65 if perp_led_up and failed_acceptance else 0.0,
            "evidence": tuple(x for x,ok in (
                ("perp_led_up",perp_led_up),
                ("spot_not_confirming",spot_not_confirming_up or (spot_delta is not None and spot_delta<=0)),
                ("failed_acceptance",failed_acceptance),
            ) if ok),
        },
        # No trustworthy liquidation-feed evidence exists in the legacy snapshot yet.
        "liquidation_cascade": {"active": False},
    }

    x25={
        "abnormal_pump": normalized["pump_exhaustion"]["active"],
        "exhaustion": normalized["pump_exhaustion"]["active"],
        "leverage_fragility": crowded_long,
        "failed_acceptance": failed_acceptance,
        "healthy_spot_demand": bool(spot_delta is not None and spot_delta > 0 and not spot_not_confirming_up),
        "acceptance_above_high": False,  # requires explicit acceptance engine; do not infer.
        "structure_break": False,        # observer mode: not inferred from trade_state.
        "bearish_displacement": False,   # requires explicit displacement mapping.
        "failed_retest": False,          # requires explicit retest mapping.
    }
    return normalized, x25


def observe_crypto_events(scan: Mapping[str, Any]):
    normalized,x25=build_crypto_event_evidence(scan)
    pipeline=detect_post_pump_pipeline(scan)

    # Upgrade only with explicitly confirmed detector stages. Execution trigger
    # remains unavailable here, so observer integration still cannot trigger a trade.
    x25["abnormal_pump"] = pipeline["abnormal_pump"]["confirmed"]
    x25["exhaustion"] = bool(
        pipeline["aggression_inefficiency"]["confirmed"]
        or pipeline["absorption"]["confirmed"]
        or pipeline["spot_perp_divergence"]["confirmed"]
    )
    x25["leverage_fragility"] = pipeline["leverage_fragility"]["confirmed"]
    x25["failed_acceptance"] = pipeline["failed_acceptance"]["confirmed"]
    events=classify_liquidity_leverage(normalized)
    return {
        "mode":"observer",
        "affects_trade_decision":False,
        "post_pump_pipeline":pipeline,
        "events":[
            {"event_id":e.event_id,"family":e.family,"direction":e.direction,
             "confidence":e.confidence,"evidence":list(e.evidence),"metadata":dict(e.metadata)}
            for e in events
        ],
        "manipulation_x25":manipulation_x25_state(x25),
        "evidence":normalized,
        "x25_evidence":x25,
    }
