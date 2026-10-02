"""Public Bybit xStocks market-data loader.

Only market data is handled here. Analytical logic remains in Scan+ core.
Bybit xStocks are Spot instruments; the loader maps underlying tickers such as
NVDA to their xStock symbols (e.g. NVDAXUSDT) and keeps product metadata explicit.
"""
from typing import Any, Mapping
import requests
from scan_plus.core.sessions import active_sessions
from scan_plus.core.gaps import gap_from_previous

XSTOCK_MAP={"NVDA":"NVDAXUSDT","AAPL":"AAPLXUSDT","MSFT":"MSFTXUSDT","TSLA":"TSLAXUSDT",
            "META":"METAXUSDT","GOOGL":"GOOGLXUSDT","AMZN":"AMZNXUSDT","COIN":"COINXUSDT",
            "HOOD":"HOODXUSDT","CRCL":"CRCLXUSDT","MSTR":"MSTRXUSDT"}

class BybitXStocksLoader:
    def __init__(self, base_url="https://api.bybit.com", timeout=8, session=None):
        self.base_url=base_url.rstrip("/")
        self.timeout=float(timeout)
        self.session=session or requests.Session()

    def default_symbols(self, limit=15):
        result=self._get("/v5/market/tickers",{"category":"spot"})
        ranked=[]
        for item in result.get("list") or []:
            token=str(item.get("symbol") or "").upper()
            if not token.endswith("XUSDT"):
                continue
            underlying=token[:-5]
            try:
                turnover=float(item.get("turnover24h") or item.get("volume24h") or 0)
            except (TypeError,ValueError):
                turnover=0.0
            ranked.append((turnover,underlying))
        ranked.sort(reverse=True)
        return [symbol for _,symbol in ranked[:int(limit)]]

    def token_symbol(self, symbol):
        key=str(symbol).upper().replace("USDT","")
        if key.endswith("X"): key=key[:-1]
        return XSTOCK_MAP.get(key, f"{key}XUSDT")

    def _get(self,path,params):
        response=self.session.get(self.base_url+path,params=params,timeout=self.timeout)
        response.raise_for_status()
        payload=response.json()
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit API {payload.get('retCode')}: {payload.get('retMsg')}")
        return payload.get("result") or {}

    def ticker(self,symbol):
        token=self.token_symbol(symbol)
        result=self._get("/v5/market/tickers",{"category":"spot","symbol":token})
        item=(result.get("list") or [None])[0]
        if not item: raise LookupError(f"xStock not found: {token}")
        return {"source":"bybit","product":"xstock_spot","symbol":token,"underlying":str(symbol).upper(),
                "last_price":float(item["lastPrice"]),"bid":float(item.get("bid1Price") or 0),
                "ask":float(item.get("ask1Price") or 0),"volume_24h":float(item.get("volume24h") or 0)}

    def context(self,symbol,underlying_quote=None):
        """Return xStock/underlying/session context without merging analytical evidence."""
        token=self.token_symbol(symbol)
        context={"product":"xstock_spot","token_symbol":token,"underlying":str(symbol).upper(),
                 "xstock_24_7":True,"active_underlying_sessions":active_sessions("stocks_us")}
        if underlying_quote is not None:
            context["underlying_quote"]=dict(underlying_quote)
        return context

    def gap_from_daily(self,symbol,limit=3):
        data=self.klines(symbol,interval="D",limit=limit)
        candles=data["candles"]
        if len(candles)<2: return None
        return gap_from_previous(candles[-1]["open"],candles[-2]["close"])

    def klines(self,symbol,interval="5",limit=200):
        token=self.token_symbol(symbol)
        result=self._get("/v5/market/kline",{"category":"spot","symbol":token,"interval":str(interval),"limit":int(limit)})
        rows=[]
        for row in reversed(result.get("list") or []):
            rows.append({"timestamp_ms":int(row[0]),"open":float(row[1]),"high":float(row[2]),
                          "low":float(row[3]),"close":float(row[4]),"volume":float(row[5]),
                          "turnover":float(row[6])})
        return {"source":"bybit","product":"xstock_spot","symbol":token,"underlying":str(symbol).upper(),
                "interval":str(interval),"candles":rows}
