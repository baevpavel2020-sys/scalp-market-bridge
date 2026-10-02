"""Non-executing Forex execution and risk planner.

FX has no crypto-style funding/OI/liquidation assumptions here. Position sizing is
based on account cash risk and the price-distance to structural invalidation.
"""
from typing import Mapping


def quote_currency(symbol):
    s=str(symbol or '').upper().replace('/','').replace('=X','')
    return s[3:6] if len(s)==6 else None


def fx_risk_per_unit(symbol, price_risk, account_currency='USD', quote_to_account=1.0):
    quote=quote_currency(symbol)
    if quote is None or str(account_currency).upper()!=str(quote).upper():
        if quote_to_account is None:
            return None
    return float(price_risk) * float(quote_to_account or 1.0)



def _invalidation(frames,direction):
    for tf in ("5m","15m","1h","4h"):
        structure=((frames.get(tf) or {}).get("analysis") or {}).get("structure") or {}
        key="last_swing_low" if direction=="bullish" else "last_swing_high"
        point=structure.get(key)
        if isinstance(point,Mapping) and point.get("price") is not None:
            try: return float(point["price"]),tf
            except (TypeError,ValueError): pass
    return None,None


def _target(frames,direction):
    for tf in ("15m","1h","4h"):
        fib=((frames.get(tf) or {}).get("analysis") or {}).get("fibonacci") or {}
        ext=fib.get("extensions") or {}
        for key in ("1.618","1.272"):
            if key in ext:
                try:
                    value=float(ext[key])
                    return value,tf,key
                except (TypeError,ValueError): pass
    return None,None,None


def build_forex_execution_plan(*,candidate,frames,price,equity,risk_fraction,
                               symbol=None,account_currency="USD",quote_to_account=None,
                               min_rr=2.0,entry_policy="retest_or_limit"):
    if not isinstance(candidate,Mapping) or candidate.get("status")!="CANDIDATE":
        return {"status":"WAIT","reason":"candidate_not_confirmed"}
    direction=candidate.get("direction")
    if direction not in ("bullish","bearish"):
        return {"status":"WAIT","reason":"direction_unresolved"}
    try: px=float(price)
    except (TypeError,ValueError): return {"status":"WAIT","reason":"invalid_price"}
    if px<=0: return {"status":"WAIT","reason":"invalid_price"}
    eq=rf=None
    if equity is not None and risk_fraction is not None:
        try: eq=float(equity); rf=float(risk_fraction)
        except (TypeError,ValueError): return {"status":"WAIT","reason":"invalid_account_or_risk"}
        if eq<=0 or rf<=0 or rf>=1: return {"status":"WAIT","reason":"invalid_account_or_risk"}
    stop,stop_tf=_invalidation(frames,direction)
    if stop is None:
        return {"status":"WAIT","reason":"structural_invalidation_unavailable"}
    if direction=="bullish" and stop>=px:
        return {"status":"WAIT","reason":"bullish_invalidation_not_below_price"}
    if direction=="bearish" and stop<=px:
        return {"status":"WAIT","reason":"bearish_invalidation_not_above_price"}
    risk_per_unit=abs(px-stop)
    if risk_per_unit<=0:
        return {"status":"WAIT","reason":"zero_price_risk"}
    risk_value_per_unit=fx_risk_per_unit(symbol,risk_per_unit,account_currency,quote_to_account)
    if eq is not None and risk_value_per_unit is None:
        return {"status":"WAIT","reason":"fx_quote_to_account_conversion_required"}
    risk_cash=(eq*rf) if eq is not None and rf is not None else None
    units=(risk_cash/risk_value_per_unit) if risk_cash is not None else None
    target,target_tf,target_ratio=_target(frames,direction)
    if target is None or (direction=="bullish" and target<=px) or (direction=="bearish" and target>=px):
        target=px+risk_per_unit*min_rr if direction=="bullish" else px-risk_per_unit*min_rr
        target_source="risk_multiple"
    else:
        target_source=f"fibonacci_{target_ratio}_{target_tf}"
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
                "risk_per_price_unit":risk_per_unit,"risk_value_per_unit":risk_value_per_unit,
                "units":units,"sizing_status":"READY" if units is not None else "ACCOUNT_CONTEXT_REQUIRED"},
        "rr":rr,
        "execution_policy":{
            "submit":False,
            "spot_fx":True,
            "no_crypto_leverage_assumption":True,
            "cancel_if_candidate_invalidates":True,
            "recheck_session_context_before_entry":True,
        },
    }
