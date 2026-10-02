"""Public Yahoo Finance chart loader for intraday FX candles.

Yahoo symbols use the conventional FX form EURUSD=X. Intraday availability is
provider-dependent; unsupported intervals return a clear provider error rather
than silently substituting daily candles.
"""
import requests


def _aggregate_4h(rows):
    out=[]
    bucket=None
    for row in rows:
        start=int(row["timestamp_ms"]) // 14400000
        if bucket is None or bucket["bucket"]!=start:
            if bucket is not None: out.append(bucket["row"])
            bucket={"bucket":start,"row":dict(row)}
            bucket["row"]["timestamp_ms"]=start*14400000
        else:
            r=bucket["row"]
            r["high"]=max(r["high"],row["high"]); r["low"]=min(r["low"],row["low"])
            r["close"]=row["close"]; r["volume"]+=row.get("volume",0) or 0
    if bucket is not None: out.append(bucket["row"])
    return out

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
        response=self.session.get(f"{self.base_url}/{ticker}",params=params,timeout=self.timeout,
                                  headers={"User-Agent":"scalp-market-bridge/1.0"})
        response.raise_for_status()
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
