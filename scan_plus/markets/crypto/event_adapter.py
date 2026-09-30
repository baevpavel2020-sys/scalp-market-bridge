"""Normalize legacy Crypto Scan+ output into event-engine evidence.

This module is deliberately conservative: missing evidence stays missing/False.
It never invents a liquidation cluster, absorption signal, or healthy Spot demand.
"""
from typing import Any, Mapping

from scan_plus.core.liquidity_leverage import classify_liquidity_leverage
from scan_plus.markets.crypto.manipulation_x25 import manipulation_x25_state
from scan_plus.markets.crypto.post_pump import detect_post_pump_pipeline
from scan_plus.markets.crypto.reversal_trigger import detect_bearish_reversal_trigger
from scan_plus.markets.crypto.observables import extract_crypto_observables
from scan_plus.markets.crypto.evidence_fusion import fuse_exhaustion, healthy_continuation_veto
from scan_plus.markets.crypto.causality import validate_reversal_causality


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tf(scan, tf):
    return ((scan.get("timeframes") or {}).get(tf) or {})


def build_crypto_event_evidence(scan: Mapping[str, Any], observables=None):
    o=observables or extract_crypto_observables(scan)
    execution=scan.get("execution") or {}
    direction=(scan.get("direction") or {}).get("bias") or scan.get("direction")
    if isinstance(direction,dict):
        direction=direction.get("direction")

    sweeps=o["sweeps"]
    sweep_direction=o["sweep_direction"]
    perp_delta=o["perp_delta_ratio"]
    spot_delta=o["spot_delta_ratio"]
    oi_change=o["oi_change_pct"]
    funding=o["funding_rate"]
    spot_not_confirming_up=o["spot_not_confirming_up"]

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


def observe_crypto_events(scan: Mapping[str, Any], adaptive_abnormal=None):
    observables=extract_crypto_observables(scan)
    normalized,x25=build_crypto_event_evidence(scan, observables)
    pipeline=detect_post_pump_pipeline(scan, observables, adaptive_abnormal=adaptive_abnormal)
    reversal=detect_bearish_reversal_trigger(scan)
    causality=validate_reversal_causality(pipeline,reversal)

    # Fuse independent evidence channels instead of treating correlated clues as
    # separate votes. One clue remains useful context but cannot ARM x25 by itself.
    exhaustion=fuse_exhaustion(pipeline)
    continuation=healthy_continuation_veto(observables,pipeline)
    x25["abnormal_pump"] = pipeline["abnormal_pump"]["confirmed"]
    x25["exhaustion"] = exhaustion["confirmed"]
    x25["healthy_continuation"] = continuation["active"]
    x25["leverage_fragility"] = pipeline["leverage_fragility"]["confirmed"]
    x25["failed_acceptance"] = pipeline["failed_acceptance"]["confirmed"]
    best_trigger=reversal.get("best") or {}
    x25["structure_break"] = bool((best_trigger.get("structure_break") or {}).get("confirmed"))
    x25["bearish_displacement"] = bool((best_trigger.get("bearish_displacement") or {}).get("confirmed"))
    x25["failed_retest"] = bool((best_trigger.get("failed_retest") or {}).get("confirmed"))
    if not causality["eligible_for_trigger"]:
        x25["failed_retest"] = False
    events=classify_liquidity_leverage(normalized)
    return {
        "mode":"observer",
        "affects_trade_decision":False,
        "post_pump_pipeline":pipeline,
        "exhaustion_fusion":exhaustion,
        "healthy_continuation_veto":continuation,
        "reversal_trigger":reversal,
        "causality":causality,
        "events":[
            {"event_id":e.event_id,"family":e.family,"direction":e.direction,
             "confidence":e.confidence,"evidence":list(e.evidence),"metadata":dict(e.metadata)}
            for e in events
        ],
        "manipulation_x25":manipulation_x25_state(x25),
        "evidence":normalized,
        "x25_evidence":x25,
    }
