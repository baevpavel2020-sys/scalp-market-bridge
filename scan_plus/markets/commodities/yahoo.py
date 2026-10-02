"""Public Yahoo Finance chart loader for commodity futures.

Mappings keep the market identity explicit: metals, energy and cocoa futures are
not treated as crypto/FX instruments.
"""
import requests
import time
import threading

YAHOO_REQUEST_LOCK = threading.Lock()
YAHOO_LAST_REQUEST = 0.0
YAHOO_MIN_INTERVAL = 1.25
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

def _yahoo_get(path, params, timeout, headers):
    global YAHOO_LAST_REQUEST
    with YAHOO_REQUEST_LOCK:
        delay = YAHOO_MIN_INTERVAL - (time.monotonic() - YAHOO_LAST_REQUEST)
        if delay > 0:
            time.sleep(delay)
        last_error = None
        for host in YAHOO_HOSTS:
            for attempt in range(4):
                try:
                    response = requests.get(f"https://{host}{path}", params=params, timeout=timeout, headers=headers)
                    YAHOO_LAST_REQUEST = time.monotonic()
                    if response.status_code != 429:
                        response.raise_for_status()
                        return response
                    retry_after=response.headers.get("Retry-After")
                    time.sleep(float(retry_after) if retry_after else min(2.0 ** attempt, 8.0))
                except requests.RequestException as exc:
                    last_error=exc
                    time.sleep(min(2.0 ** attempt, 8.0))
        if last_error:
            raise last_error
        raise requests.HTTPError("Yahoo Finance rate limit persisted")


SYMBOLS={"XAUUSD":"GC=F","XAGUSD":"SI=F","WTI":"CL=F","BRENT":"BZ=F","COCOA":"CC=F"}
INTERVALS={"5":"5m","15":"15m","60":"60m","240":"1h","d":"1d"}


def _aggregate_4h(rows):
    rows=sorted(rows,key=lambda x:int(x["timestamp_ms"]))
    buckets={}
    for row in rows:
        ts=int(row["timestamp_ms"])
        bucket_start=(ts//14400000)*14400000
        buckets.setdefault(bucket_start,[]).append(row)

    out=[]
    for start, group in sorted(buckets.items()):
        group=sorted(group,key=lambda x:int(x["timestamp_ms"]))
        # A synthetic 4H candle is valid only when all four hourly bars are present
        # and contiguous. Never manufacture a 4H bar across provider gaps.
        expected=[start + i*3600000 for i in range(4)]
        actual=[int(r["timestamp_ms"]) for r in group]
        if actual != expected:
            continue
        first,last=group[0],group[-1]
        out.append({
            "timestamp_ms":start,
            "open":first["open"],
            "high":max(r["high"] for r in group),
            "low":min(r["low"] for r in group),
            "close":last["close"],
            "volume":sum(r.get("volume",0) or 0 for r in group),
            "source_bars":4,
            "derived_4h":True,
        })
    return out

class YahooCommodityLoader:
    def __init__(self,base_url="https://query1.finance.yahoo.com/v8/finance/chart",timeout=8,session=None):
        self.base_url=base_url.rstrip("/")
        self.timeout=float(timeout)
        self.session=session or requests.Session()
    def candles(self,symbol,interval="60"):
        key=str(symbol).upper()
        yahoo_symbol=SYMBOLS.get(key)
        if not yahoo_symbol: raise LookupError(f"unsupported commodity symbol: {symbol}")
        interval=str(interval).lower()
        if interval not in INTERVALS: raise NotImplementedError(f"unsupported commodity interval: {interval}")
        params={"interval":INTERVALS[interval],"range":"30d","events":"history"}
        response = _yahoo_get(
            f"/v8/finance/chart/{yahoo_symbol}",
            params=params,
            timeout=self.timeout,
            headers={"User-Agent":"scalp-market-bridge/1.0"},
        )
        payload=response.json()
        result=(payload.get("chart") or {}).get("result") or []
        if not result:
            error=(payload.get("chart") or {}).get("error") or {}
            raise LookupError(error.get("description") or f"Yahoo commodity data unavailable: {symbol}")
        item=result[0]; timestamps=item.get("timestamp") or []
        quote=((item.get("indicators") or {}).get("quote") or [{}])[0]
        rows=[]
        for i,ts in enumerate(timestamps):
            try:
                o,h,l,c=[quote[k][i] for k in ("open","high","low","close")]
                if None in (o,h,l,c): continue
                rows.append({"timestamp_ms":int(ts)*1000,"open":float(o),"high":float(h),
                             "low":float(l),"close":float(c),
                             "volume":float((quote.get("volume") or [0]*len(timestamps))[i] or 0)})
            except (IndexError,TypeError,ValueError):
                continue
        if interval=="240": rows=_aggregate_4h(rows)
        if not rows: raise LookupError(f"Yahoo commodity returned no candles: {symbol} {interval}")
        return {"source":"yahoo_chart","product":"commodity_future","symbol":key,
                "provider_symbol":yahoo_symbol,"interval":interval,"candles":rows}
