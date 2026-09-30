"""Non-executing commodity execution and risk planner.

All commodity groups use the same cash-risk contract, while group-specific provider
context remains informational/confirmatory. No futures leverage, contract multiplier,
tick value or margin is invented by this layer.
"""
from typing import Mapping


def _structure_stop(frames, direction, preferred_tf):
    tfs=[preferred_tf] + [tf for tf in ("15m","5m","1h","4h") if tf!=preferred_tf]
    for tf in tfs:
        structure=((frames.get(tf) or {}).get("analysis") or {}).get("structure") or {}
        point=structure.get("last_swing_low" if direction=="bullish" else "last_swing_high")
        if isinstance(point,Mapping) and point.get("price") is not None:
            try:
                return float(point["price"]),tf
            except (TypeError,ValueError):
                pass
    return None,None


def _fibo_target(frames, direction, preferred_tf):
    tfs=[preferred_tf] + [tf for tf in ("15m","1h","4h") if tf!=preferred_tf]
    for tf in tfs:
        fib=((frames.get(tf) or {}).get("analysis") or {}).get("fibonacci") or {}
        if fib.get("ready") is not True:
            continue
        ext=fib.get("extensions") or {}
        for ratio in ("1.618","1.272"):
            if ratio in ext:
                try:
                    return float(ext[ratio]),tf,ratio
                except (TypeError,ValueError):
                    pass
    return None,None,None


def build_commodity_execution_plan(*,candidate,frames,price,equity,risk_fraction,
                                   min_rr=1.5,entry_policy="limit_or_retest"):
    if not isinstance(candidate,Mapping) or candidate.get("status")!="CANDIDATE":
        return {"status":"WAIT","reason":"candidate_not_confirmed"}
    direction=candidate.get("direction")
    if direction not in ("bullish","bearish"):
        return {"status":"WAIT","reason":"direction_unresolved"}
    group=str(candidate.get("group") or "").lower()
    if group not in ("metals","energy","softs"):
        return {"status":"WAIT","reason":"unknown_commodity_group"}
    try:
        px=float(price); eq=float(equity); rf=float(risk_fraction)
    except (TypeError,ValueError):
        return {"status":"WAIT","reason":"invalid_account_or_price"}
    if px<=0 or eq<=0 or rf<=0 or rf>=1:
        return {"status":"WAIT","reason":"invalid_account_or_risk"}

    preferred_tf=candidate.get("confirmation_timeframe")
    if preferred_tf not in frames:
        return {"status":"WAIT","reason":"confirmation_timeframe_unavailable"}

    stop,stop_tf=_structure_stop(frames,direction,preferred_tf)
    if stop is None:
        return {"status":"WAIT","reason":"structural_invalidation_unavailable"}
    if direction=="bullish" and stop>=px:
        return {"status":"WAIT","reason":"bullish_invalidation_not_below_price"}
    if direction=="bearish" and stop<=px:
        return {"status":"WAIT","reason":"bearish_invalidation_not_above_price"}

    risk_per_unit=abs(px-stop)
    if risk_per_unit<=0:
        return {"status":"WAIT","reason":"zero_price_risk"}
    risk_cash=eq*rf
    units=risk_cash/risk_per_unit

    target,target_tf,ratio=_fibo_target(frames,direction,preferred_tf)
    if target is None or (direction=="bullish" and target<=px) or (direction=="bearish" and target>=px):
        target=px+risk_per_unit*min_rr if direction=="bullish" else px-risk_per_unit*min_rr
        target_source="risk_multiple"
    else:
        target_source=f"fibonacci_{ratio}_{target_tf}"

    rr=abs(target-px)/risk_per_unit
    if rr<min_rr:
        return {"status":"WAIT","reason":"rr_below_minimum","rr":rr}

    return {
        "status":"PLAN",
        "side":"BUY" if direction=="bullish" else "SELL",
        "entry":{"type":"LIMIT","reference_price":px,"policy":entry_policy},
        "stop":{"price":stop,"source":"structure","timeframe":stop_tf},
        "target":{"price":target,"source":target_source},
        "risk":{"account_equity":eq,"risk_fraction":rf,"risk_cash":risk_cash,
                "risk_per_price_unit":risk_per_unit,"units":units},
        "commodity_group":group,
        "execution_policy":{
            "submit":False,
            "provider_context_confirmatory":True,
            "no_invented_contract_multiplier_or_margin":True,
            "cancel_if_candidate_invalidates":True,
            "recheck_provider_context_before_entry":True,
        },
    }
