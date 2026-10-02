"""Non-executing Stocks execution and risk planner.

Produces a deterministic order plan from an already confirmed candidate. It never
submits an order and never invents an entry when the evidence is incomplete.
"""
from typing import Mapping


def _last_structure(frames, direction):
    for tf in ("15m","5m","1h","4h"):
        structure=((frames.get(tf) or {}).get("analysis") or {}).get("structure") or {}
        point=structure.get("last_swing_low" if direction=="bullish" else "last_swing_high")
        if isinstance(point,Mapping) and point.get("price") is not None:
            return float(point["price"]),tf
    return None,None


def _fibo_targets(frames, direction):
    for tf in ("15m","1h","4h"):
        fib=((frames.get(tf) or {}).get("analysis") or {}).get("fibonacci") or {}
        ext=fib.get("extensions") or {}
        key="1.618"
        if key in ext:
            try: return float(ext[key]),tf
            except (TypeError,ValueError): pass
    return None,None


def build_stock_execution_plan(*, candidate, frames, price, equity, risk_fraction,
                               fee_buffer=0.001, min_rr=2.0):
    """Return PLAN or WAIT. Position sizing uses cash-risk, not leverage assumptions."""
    if not isinstance(candidate,Mapping) or candidate.get("status")!="CANDIDATE":
        return {"status":"WAIT","reason":"candidate_not_confirmed"}

    direction=candidate.get("direction")
    if direction not in ("bullish","bearish"):
        return {"status":"WAIT","reason":"direction_unresolved"}

    try:
        px=float(price)
    except (TypeError,ValueError):
        return {"status":"WAIT","reason":"invalid_price"}
    if px <= 0:
        return {"status":"WAIT","reason":"invalid_price"}
    eq=rf=None
    if equity is not None and risk_fraction is not None:
        try:
            eq=float(equity); rf=float(risk_fraction)
        except (TypeError,ValueError):
            return {"status":"WAIT","reason":"invalid_account_or_risk"}
        if eq <= 0 or rf <= 0 or rf >= 1:
            return {"status":"WAIT","reason":"invalid_account_or_risk"}

    invalidation,inv_tf=_last_structure(frames,direction)
    target,target_tf=_fibo_targets(frames,direction)
    if invalidation is None:
        return {"status":"WAIT","reason":"structural_invalidation_unavailable"}

    # A stop must be on the invalid side of price.
    if direction=="bullish" and invalidation >= px:
        return {"status":"WAIT","reason":"bullish_invalidation_not_below_price"}
    if direction=="bearish" and invalidation <= px:
        return {"status":"WAIT","reason":"bearish_invalidation_not_above_price"}

    risk_per_share=abs(px-invalidation)
    if risk_per_share <= 0:
        return {"status":"WAIT","reason":"zero_price_risk"}

    risk_cash=(eq*rf) if eq is not None and rf is not None else None
    qty=(risk_cash/risk_per_share) if risk_cash is not None else None

    if target is None or (direction=="bullish" and target <= px) or (direction=="bearish" and target >= px):
        # Fall back to a deterministic RR target rather than inventing a market level.
        target=px + risk_per_share*min_rr if direction=="bullish" else px-risk_per_share*min_rr
        target_source="risk_multiple"
    else:
        target_source=f"fibonacci_{target_tf}"

    reward=abs(target-px)
    rr=(reward/risk_per_share) if risk_per_share else 0
    if rr < min_rr:
        return {"status":"WAIT","reason":"rr_below_minimum","rr":rr}

    side="BUY" if direction=="bullish" else "SELL"
    return {
        "status":"PLAN",
        "eligible":True,
        "side":side,
        "entry":{"type":"LIMIT","reference_price":px,"price_policy":"candidate_or_retest"},
        "stop":{"price":invalidation,"source":"structure","timeframe":inv_tf},
        "target":{"price":target,"source":target_source},
        "risk":{"account_equity":eq,"risk_fraction":rf,"risk_cash":risk_cash,
                "risk_per_unit":risk_per_share,"quantity":qty,"fee_buffer":fee_buffer,
                "sizing_status":"READY" if qty is not None else "ACCOUNT_CONTEXT_REQUIRED"},
        "rr":rr,
        "execution_policy":{
            "submit":False,
            "xstock_market_24_7":True,
            "underlying_session_context_required":True,
            "cancel_if_candidate_invalidates":True,
        },
    }
