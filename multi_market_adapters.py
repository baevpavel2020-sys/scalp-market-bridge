"""
Multi-market external data adapters for Scan+.
Provider: Twelve Data, configured with TWELVE_DATA_API_KEY.
This layer is analysis-only until a broker/execution connector is explicitly added.
"""
import json, os, urllib.parse, urllib.request

BASE="https://api.twelvedata.com"
TF={"1m":"1min","5m":"5min","15m":"15min","1h":"1h","4h":"4h","1D":"1day"}
FOREX=("EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","NZD/USD","USD/CAD","EUR/GBP","EUR/JPY","GBP/JPY")
COMMODITIES=("XAU/USD","XAG/USD","WTI/USD","BRENT/USD","HG1")
STOCKS=("AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","NFLX","AVGO","JPM","XOM")

class ExternalMarketAdapter:
    VERSION="external_market_adapter_v1"
    def __init__(self,api_key=None):
        self.api_key=(api_key or os.environ.get("TWELVE_DATA_API_KEY","")).strip()
    @property
    def configured(self): return bool(self.api_key)
    def _get(self,path,params):
        if not self.configured: raise RuntimeError("TWELVE_DATA_API_KEY_not_configured")
        q=dict(params); q["apikey"]=self.api_key
        url=BASE+path+"?"+urllib.parse.urlencode(q)
        req=urllib.request.Request(url,headers={"User-Agent":"scalp-market-bridge/1.0"})
        with urllib.request.urlopen(req,timeout=15) as r:
            data=json.loads(r.read().decode("utf-8"))
        if str(data.get("status","")).lower()=="error":
            raise RuntimeError(str(data.get("message") or data))
        return data
    def candles(self,symbol,interval,outputsize=500):
        data=self._get("/time_series",{"symbol":symbol,"interval":TF[interval],"outputsize":min(int(outputsize),5000),"order":"ASC","timezone":"UTC"})
        out=[]
        for x in data.get("values",[]) or []:
            try:
                out.append({"datetime":x["datetime"],"open":float(x["open"]),"high":float(x["high"]),"low":float(x["low"]),"close":float(x["close"]),"volume":float(x.get("volume") or 0),"source":"twelve_data"})
            except (KeyError,TypeError,ValueError): pass
        return out
    def quote(self,symbol): return self._get("/quote",{"symbol":symbol})
    def scan(self,market,symbol):
        frames={}; errors={}
        for tf in ("1D","4h","1h","15m","5m"):
            try: frames[tf]=self.candles(symbol,tf)
            except Exception as e: errors[tf]=f"{type(e).__name__}:{e}"
        try: quote=self.quote(symbol)
        except Exception as e: quote={"error":f"{type(e).__name__}:{e}"}
        ready=all(len(frames.get(tf,[]))>=50 for tf in ("1D","4h","1h","15m","5m"))
        return {"market":market,"symbol":symbol,"adapter_version":self.VERSION,"provider":"twelve_data","configured":self.configured,"analysis_ready":ready,"execution_ready":False,"execution_reason":"external_market_execution_connector_not_configured","frames":frames,"quote":quote,"errors":errors,"capabilities":{"ohlcv":True,"realtime_quote":True,"orderbook":False,"open_interest":False,"funding":False,"spot_cvd":False}}
def external_universe():
    return {"forex":list(FOREX),"commodities":list(COMMODITIES),"stocks":list(STOCKS)}
