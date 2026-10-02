"""Public Yahoo Finance chart loader for intraday FX candles.

Yahoo symbols use the conventional FX form EURUSD=X. Intraday availability is
provider-dependent; unsupported intervals return a clear provider error rather
than silently substituting daily candles.
"""
import requests
import time
import threading


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

YAHOO_REQUEST_LOCK = threading.Lock()
YAHOO_LAST_REQUEST = 0.0
YAHOO_MIN_INTERVAL = 1.25
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

def _yahoo_get(base_url, path, params, timeout, headers, session):
    global YAHOO_LAST_REQUEST
    with YAHOO_REQUEST_LOCK:
        default_hosts = base_url.startswith("https://query1.finance.yahoo.com")
        hosts = YAHOO_HOSTS if default_hosts else (base_url.split("://",1)[-1].split("/",1)[0],)
        last_error = None
        for host in hosts:
            delay = YAHOO_MIN_INTERVAL - (time.monotonic() - YAHOO_LAST_REQUEST)
            if delay > 0:
                time.sleep(delay)
            url = f"https://{host}/v8/finance/chart/{path}" if default_hosts else f"{base_url}/{path}"
            for attempt in range(4):
                try:
                    response=session.get(url,params=params,timeout=timeout,headers=headers)
                    YAHOO_LAST_REQUEST=time.monotonic()
                    if getattr(response,"status_code",200) != 429:
                        response.raise_for_status()
                        return response
                    retry_after=response.headers.get("Retry-After")
                    time.sleep(float(retry_after) if retry_after else min(2.0**attempt,8.0))
                except requests.RequestException as exc:
                    last_error=exc
                    if attempt >= 3:
                        break
                    time.sleep(min(2.0**attempt,8.0))
        if last_error:
            raise last_error
        raise requests.HTTPError("Yahoo Finance rate limit persisted")

class YahooFXLoader:
    SYMBOL_SUFFIX="=X"
    INTERVALS={"5":"5m","15":"15m","60":"60m","240":"1h","d":"1d"}
    def __init__(self,base_url="https://query1.finance.yahoo.com/v8/finance/chart",timeout=8,session=None):
        self.base_url=base_url.rstrip("/")
        self.timeout=float(timeout)
        self.session=session or requests.Session()
    def candles(self,symbol="EURUSD",interval="60"):
        interval=str(interval).lower()
        if interval not in self.INTERVALS:
            raise NotImplementedError(f"unsupported Yahoo FX interval: {interval}")
        pair=str(symbol).upper().replace("/","")
        ticker=pair if pair.endswith("=X") else pair+self.SYMBOL_SUFFIX
        params={"interval":self.INTERVALS[interval],"range":"30d" if interval!="d" else "1y","events":"history"}
        response = _yahoo_get(
            self.base_url, ticker, params=params, timeout=self.timeout,
            headers={"User-Agent":"scalp-market-bridge/1.0"}, session=self.session,
        )
        payload=response.json()
        result=(payload.get("chart") or {}).get("result") or []
        if not result:
            error=(payload.get("chart") or {}).get("error") or {}
            raise LookupError(error.get("description") or f"Yahoo FX data unavailable: {symbol}")
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
        if not rows: raise LookupError(f"Yahoo FX returned no candles: {symbol} {interval}")
        return {"source":"yahoo_chart","product":"fx","symbol":pair,
                "interval":interval,"candles":rows}
