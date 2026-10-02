"""
Multi-market external data adapters for Scan+.
Provider: Twelve Data, configured with TWELVE_DATA_API_KEY.
This layer is analysis-only until a broker/execution connector is explicitly added.
"""
import json, os, math, urllib.parse, urllib.request

BASE="https://api.twelvedata.com"
TF={"1m":"1min","5m":"5min","15m":"15min","1h":"1h","4h":"4h","1D":"1day"}
FOREX=("EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","NZD/USD","USD/CAD","EUR/GBP","EUR/JPY","GBP/JPY")
COMMODITIES=("XAU/USD","XAG/USD","WTI/USD","BRENT/USD","HG1")
STOCKS=("AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","NFLX","AVGO","JPM","XOM")


BYBIT_BASE="https://api.bybit.com"
def _bybit_get(path,params):
    q=urllib.parse.urlencode(params); req=urllib.request.Request(BYBIT_BASE+path+"?"+q,headers={"User-Agent":"scalp-market-bridge/1.0"})
    with urllib.request.urlopen(req,timeout=12) as r: data=json.loads(r.read().decode("utf-8"))
    if data.get("retCode") not in (0,None): raise RuntimeError(str(data))
    return data

def bybit_xstocks_top15():
    data=_bybit_get("/v5/market/instruments-info",{"category":"spot","symbolType":"xstocks","limit":1000})
    symbols=[x["symbol"] for x in data.get("result",{}).get("list",[]) if x.get("status")=="Trading"]
    if not symbols: return []
    tick=_bybit_get("/v5/market/tickers",{"category":"spot"})
    rows={x.get("symbol"):x for x in tick.get("result",{}).get("list",[])}
    return sorted(symbols,key=lambda s:float(rows.get(s,{}).get("turnover24h") or 0),reverse=True)[:15]

def bybit_xstock_candles(symbol,interval="15",limit=500):
    data=_bybit_get("/v5/market/kline",{"category":"spot","symbol":symbol,"interval":interval,"limit":min(int(limit),1000)})
    out=[]
    for row in reversed(data.get("result",{}).get("list",[]) or []):
        try: out.append({"start":int(row[0]),"open":float(row[1]),"high":float(row[2]),"low":float(row[3]),"close":float(row[4]),"volume":float(row[5]),"turnover":float(row[6]),"confirm":True,"source":"bybit_xstocks"})
        except (IndexError,TypeError,ValueError): pass
    return out


MARKET_PROFILES = {
    "crypto": {"session_model":"24_7","volume_model":"exchange_volume","oi":True,"funding":True,"liquidations":True,"orderbook":True,"cvd":True,"execution":"exchange_perpetual_or_spot","special_events":["pump_exhaustion","short_squeeze","long_liquidation_cascade","liquidity_sweep"]},
    "stocks": {"session_model":"exchange_session","volume_model":"exchange_volume","oi":False,"funding":False,"liquidations":False,"orderbook":True,"cvd":False,"execution":"bybit_xstock_spot","special_events":["gap","opening_range","session_liquidity","failed_breakout"]},
    "forex": {"session_model":"asia_london_newyork","volume_model":"tick_or_provider_volume","oi":False,"funding":False,"liquidations":False,"orderbook":False,"cvd":False,"execution":"external_fx_broker_required","special_events":["session_liquidity","london_breakout","ny_reversal","failed_breakout"]},
    "commodities": {"session_model":"instrument_session","volume_model":"provider_volume","oi":False,"funding":False,"liquidations":False,"orderbook":False,"cvd":False,"execution":"external_commodity_broker_required","special_events":["session_liquidity","inventory_event","contract_rollover","failed_breakout"]},
}

def market_profile(market):
    return dict(MARKET_PROFILES.get(str(market).lower(), {}))

def session_context(market, now_epoch=None):
    import datetime as _dt
    ts=_dt.datetime.fromtimestamp(now_epoch or __import__("time").time(), _dt.timezone.utc)
    hour=ts.hour + ts.minute/60.0
    market=str(market).lower()
    if market=="forex":
        if 0 <= hour < 8: session="ASIA"
        elif 8 <= hour < 13: session="LONDON"
        elif 13 <= hour < 17: session="LONDON_NY_OVERLAP"
        elif 17 <= hour < 22: session="NEW_YORK"
        else: session="ROLLOVER"
    elif market=="stocks": session="GLOBAL_XSTOCKS_24_7"
    elif market=="commodities": session="INSTRUMENT_SESSION"
    else: session="24_7"
    return {"market":market,"session":session,"utc_hour":round(hour,2),"profile":market_profile(market)}


class MarketProfileRouter:
    """Routes external-market data into the shared technical core without
    importing crypto-only flow metrics or execution assumptions."""
    VERSION="market_profile_router_v1"

    @staticmethod
    def _rows_by_tf(frames):
        return {
            tf:list(rows or [])
            for tf,rows in (frames or {}).items()
            if isinstance(rows,list)
        }

    @staticmethod
    def _normalize(rows):
        out=[]
        for i,row in enumerate(rows or []):
            try:
                ts=row.get("start")
                if ts is None and row.get("datetime"):
                    import datetime as dt
                    raw=str(row["datetime"]).replace("Z","+00:00")
                    parsed=dt.datetime.fromisoformat(raw)
                    ts=int(parsed.timestamp()*1000)
                o,h,l,cl=[float(row[k]) for k in ("open","high","low","close")]
                if not all(math.isfinite(x) for x in (o,h,l,cl)) or h<max(o,cl) or l>min(o,cl) or h<l:
                    continue
                out.append({"start":int(ts or i),"open":o,"high":h,"low":l,"close":cl,
                            "volume":float(row.get("volume") or 0),"confirm":True})
            except (KeyError,TypeError,ValueError,OverflowError):
                continue
        return out

    @classmethod
    def analyze(cls, market, symbol, frames):
        """Technical analysis only. No market-specific execution decision is fabricated."""
        from dynamic_collector import MarketStream
        normalized={tf:cls._normalize(rows) for tf,rows in cls._rows_by_tf(frames).items()}
        analyzer=MarketStream.__new__(MarketStream)
        analysis={}
        for tf,rows in normalized.items():
            analysis[tf]=analyzer._analysis_bundle(rows) if rows else {"ready":False}
        profile=market_profile(market)
        return {
            "router_version":cls.VERSION,
            "market":str(market).lower(),"symbol":symbol,
            "profile":profile,"session_context":session_context(market),
            "analysis":analysis,
            "execution_ready":False,
            "execution_reason":profile.get("execution","external_execution_not_configured"),
            "flow_policy":{
                "use_oi":bool(profile.get("oi")),
                "use_funding":bool(profile.get("funding")),
                "use_liquidations":bool(profile.get("liquidations")),
                "use_orderbook":bool(profile.get("orderbook")),
                "use_cvd":bool(profile.get("cvd")),
            },
        }

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
        provider="twelve_data"
        if market=="stocks":
            provider="bybit_xstocks"
            tf_intervals={"1D":"D","4h":"240","1h":"60","15m":"15","5m":"5"}
            for tf,itv in tf_intervals.items():
                try: frames[tf]=bybit_xstock_candles(symbol,itv)
                except Exception as e: errors[tf]=f"{type(e).__name__}:{e}"
            try:
                data=_bybit_get("/v5/market/tickers",{"category":"spot","symbol":symbol})
                quote=(data.get("result",{}).get("list") or [{}])[0]
            except Exception as e: quote={"error":f"{type(e).__name__}:{e}"}
            configured=True
        else:
            for tf in ("1D","4h","1h","15m","5m"):
                try: frames[tf]=self.candles(symbol,tf)
                except Exception as e: errors[tf]=f"{type(e).__name__}:{e}"
            try: quote=self.quote(symbol)
            except Exception as e: quote={"error":f"{type(e).__name__}:{e}"}
            configured=self.configured
        ready=all(len(frames.get(tf,[]))>=50 for tf in ("1D","4h","1h","15m","5m"))
        return {"market":market,"symbol":symbol,"adapter_version":self.VERSION,"provider":provider,"configured":configured,"analysis_ready":ready,"execution_ready":False,"execution_reason":"external_market_execution_connector_not_configured","analysis_core":MarketProfileRouter.analyze(market,symbol,frames),"frames":frames,"quote":quote,"errors":errors,"capabilities":{"ohlcv":True,"realtime_quote":True,"orderbook":False,"open_interest":False,"funding":False,"spot_cvd":False},"market_profile":market_profile(market),"session_context":session_context(market)}
def external_universe():
    try: stocks=bybit_xstocks_top15()
    except Exception: stocks=list(DEFAULT_STOCKS)
    return {"forex":list(FOREX),"commodities":list(COMMODITIES),"stocks":stocks}
