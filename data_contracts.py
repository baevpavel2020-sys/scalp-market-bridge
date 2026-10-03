"""Canonical market-data contracts for Scan+ V4 Block 1."""
from datetime import datetime, timezone
import math, time

VERSION = "data_contract_v4_block1"

ALIASES = {
    "GOLD": ("commodities","XAU/USD"), "XAUUSD": ("commodities","XAU/USD"),
    "SILVER": ("commodities","XAG/USD"), "XAGUSD": ("commodities","XAG/USD"),
    "WTI": ("commodities","WTI/USD"), "WTIUSD": ("commodities","WTI/USD"),
    "BRENT": ("commodities","BRENT/USD"), "BRENTUSD": ("commodities","BRENT/USD"),
}
TF_MS={"1m":60000,"5m":300000,"15m":900000,"1h":3600000,"4h":14400000,"1D":86400000,
       "1":60000,"5":300000,"15":900000,"60":3600000,"240":14400000,"D":86400000}

def normalize_instrument(market, symbol):
    market=str(market or "").lower().strip()
    raw=str(symbol or "").upper().strip().replace(" ","")
    alias=ALIASES.get(raw)
    if alias:
        market, canonical=alias
    elif market=="crypto":
        canonical=raw.replace("/","").replace("-","")
    elif market=="stocks":
        canonical=raw.replace("/","").replace("-","")
    elif market=="forex":
        compact=raw.replace("/","").replace("-","")
        canonical=compact[:3]+"/"+compact[3:6] if len(compact)==6 else raw
    elif market=="commodities":
        compact=raw.replace("/","").replace("-","")
        canonical=(compact[:3]+"/"+compact[3:6]) if len(compact)==6 and compact.startswith(("XAU","XAG")) else raw
    else:
        canonical=raw
    return {"market":market,"symbol":canonical,"instrument_id":f"{market}:{canonical}"}

def _finite(v):
    try: return math.isfinite(float(v))
    except (TypeError,ValueError,OverflowError): return False

def frame_quality(frames, required=("1D","4h","1h","15m","5m"), min_closed=50, now_ms=None):
    now_ms=int(now_ms or time.time()*1000)
    details={}; hard=[]; soft=[]; sources=set()
    for tf in required:
        rows=list((frames or {}).get(tf,[]) or [])
        clean=[]
        for r in rows:
            if not isinstance(r,dict): continue
            if all(_finite(r.get(k)) for k in ("open","high","low","close")):
                clean.append(r)
                if r.get("source"): sources.add(str(r["source"]))
        closed=[r for r in clean if r.get("confirm",True)]
        last=max((int(r.get("start") or 0) for r in clean),default=0)
        age_ms=(now_ms-last) if last else None
        tf_ms=TF_MS.get(tf)
        stale=bool(age_ms is not None and tf_ms and age_ms > max(tf_ms*3, 15*60*1000))
        state="PASS"
        if not clean:
            state="UNAVAILABLE"; hard.append(f"{tf}:unavailable")
        elif len(closed)<min_closed:
            state="FAIL"; hard.append(f"{tf}:insufficient_history")
        elif stale:
            state="DEGRADED"; soft.append(f"{tf}:stale")
        details[tf]={"state":state,"rows":len(clean),"closed":len(closed),"last_start":last or None,
                     "age_ms":age_ms,"stale":stale}
    states=[x["state"] for x in details.values()]
    state="UNAVAILABLE" if states and all(x=="UNAVAILABLE" for x in states) else ("FAIL" if hard else ("DEGRADED" if soft else "PASS"))
    return {"contract_version":VERSION,"state":state,"timeframes":details,
            "hard_failures":hard,"soft_degradations":soft,"sources":sorted(sources),
            "evaluated_at":datetime.now(timezone.utc).isoformat()}

def provenance(frames, provider=None, fallback_reason=None):
    src={}
    for tf,rows in (frames or {}).items():
        counts={}
        for r in rows or []:
            s=str(r.get("source") or provider or "unknown"); counts[s]=counts.get(s,0)+1
        src[tf]=counts
    return {"contract_version":VERSION,"provider":provider,"fallback_reason":fallback_reason,
            "sources_by_timeframe":src}
