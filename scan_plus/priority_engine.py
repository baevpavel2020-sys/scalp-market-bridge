"""Situation-aware priority resolution.

Priority changes ordering/weight only; it never disables a low-priority analytical block.
"""
from copy import deepcopy
from scan_plus.market_profiles import get_profile

SITUATION_OVERRIDES={
    "reversal":{"boost":["structure_mtf","liquidity","elliott","fibonacci"],"deemphasize":["continuation"]},
    "continuation":{"boost":["structure_mtf","volume","order_flow","session_liquidity"],"deemphasize":["harmonics"]},
    "gap":{"boost":["gaps","underlying_volume","fibonacci","structure_mtf"],"deemphasize":[]},
    "manipulation":{"boost":["order_flow","open_interest","liquidations","structure_mtf","liquidity"],"deemphasize":[]},
}

def resolve_priorities(market,symbol=None,situation=None):
    profile=get_profile(market,symbol)
    base=list(profile.get("scan") or [])
    boost=(SITUATION_OVERRIDES.get(str(situation or "").lower()) or {}).get("boost",[])
    result=[]
    for key in boost+base:
        if key not in result: result.append(key)
    return {
        "market":profile["market"],"symbol":profile.get("symbol"),
        "situation":situation,"ordered":result,
        "profile":deepcopy(profile),
    }
