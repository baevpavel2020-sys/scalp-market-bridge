"""Public Yahoo Finance chart loader for commodity futures.

Mappings keep the market identity explicit: metals, energy and cocoa futures are
not treated as crypto/FX instruments.
"""
import requests

SYMBOLS={"XAUUSD":"GC=F","XAGUSD":"SI=F","WTI":"CL=F","BRENT":"BZ=F","COCOA":"CC=F"}
INTERVALS={"5":"5m","15":"15m","60":"60m","240":"1h","d":"1d"}

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
        response=self.session.get(f"{self.base_url}/{yahoo_symbol}",params=params,timeout=self.timeout,
                                  headers={"User-Agent":"scalp-market-bridge/1.0"})
        response.raise_for_status()
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
        if not rows: raise LookupError(f"Yahoo commodity returned no candles: {symbol} {interval}")
        return {"source":"yahoo_chart","product":"commodity_future","symbol":key,
                "provider_symbol":yahoo_symbol,"interval":interval,"candles":rows}
