"""
Multi-market external data adapters for Scan+.
Provider: Twelve Data, configured with TWELVE_DATA_API_KEY.
This layer is analysis-only until a broker/execution connector is explicitly added.
"""
import concurrent.futures
import json, math, os, threading, time, urllib.parse, urllib.request
from market_event_engine import detect_events, build_setup_plan
from scan_architecture import market_block_policy
from scan_intelligence import enrich_external_result
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

BASE="https://api.twelvedata.com"
TF={"1m":"1min","5m":"5min","15m":"15min","1h":"1h","4h":"4h","1D":"1day"}
FOREX=("EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","NZD/USD","USD/CAD","EUR/GBP","EUR/JPY","GBP/JPY")
COMMODITIES=("XAU/USD","XAG/USD","WTI/USD","BRENT/USD","HG1")
RU_STOCKS=("SBER","GAZP","LKOH","YDEX","T","ROSN","NVTK","GMKN","PLZL","SIBN","TATN","MOEX","MGNT","X5","VTBR")
MOEX_BASE="https://iss.moex.com/iss"
MOEX_BOARD="TQBR"
MOEX_TF={"1h":60,"1D":24}
STOCKS=("AAPLXUSDT","NVDAXUSDT","TSLAXUSDT","GOOGLXUSDT","AMZNXUSDT","METAXUSDT","COINXUSDT","HOODXUSDT","CRCLXUSDT","MSTRXUSDT","SPCXXUSDT","NFLXXUSDT","AVGOXUSDT","MSFTXUSDT","JPMXUSDT")
_XSTOCKS_CACHE={"expires":0.0,"symbols":None}
_XSTOCKS_CACHE_LOCK=threading.Lock()
_XSTOCKS_CACHE_TTL=60.0


YAHOO_BASE="https://query1.finance.yahoo.com"
YAHOO_STOCK_MAP = {
    "AAPLXUSDT":"AAPL","NVDAXUSDT":"NVDA","TSLAXUSDT":"TSLA","GOOGLXUSDT":"GOOGL",
    "AMZNXUSDT":"AMZN","METAXUSDT":"META","COINXUSDT":"COIN","HOODXUSDT":"HOOD",
    "CRCLXUSDT":"CRCL","MSTRXUSDT":"MSTR","SPCXXUSDT":None,"NFLXXUSDT":"NFLX",
    "AVGOXUSDT":"AVGO","MSFTXUSDT":"MSFT","JPMXUSDT":"JPM",
}
YAHOO_FOREX_MAP = {s: s.replace("/","")+"=X" for s in FOREX}
YAHOO_COMMODITY_MAP = {"XAU/USD":"GC=F","XAG/USD":"SI=F","WTI/USD":"CL=F","BRENT/USD":"BZ=F","HG1":"HG=F"}

def _yahoo_get(symbol, interval, period1, period2):
    params=urllib.parse.urlencode({"period1":int(period1),"period2":int(period2),"interval":interval,"events":"history","includeAdjustedClose":"true"})
    req=urllib.request.Request(f"{YAHOO_BASE}/v8/finance/chart/{urllib.parse.quote(symbol, safe='')}?{params}",headers={"User-Agent":"Mozilla/5.0 scalp-market-bridge/1.0"})
    with urllib.request.urlopen(req,timeout=15) as r: data=json.loads(r.read().decode("utf-8"))
    result=((data.get("chart") or {}).get("result") or [])
    if not result: raise RuntimeError(str(((data.get("chart") or {}).get("error") or {}) or "yahoo_no_result"))
    return result[0]

def _yahoo_period(interval):
    return {"5m":60*86400,"15m":60*86400,"1h":365*86400,"4h":730*86400,"1D":5*365*86400}.get(interval,60*86400)

def _resample_ohlcv(rows, bucket_ms):
    buckets={}
    for row in rows or []:
        key=(int(row["start"])//bucket_ms)*bucket_ms
        b=buckets.get(key)
        if b is None:
            buckets[key]={"start":key,"open":row["open"],"high":row["high"],"low":row["low"],"close":row["close"],"volume":row.get("volume",0.0),"source":row.get("source","yahoo_finance"),"confirm":True}
        else:
            b["high"]=max(b["high"],row["high"]); b["low"]=min(b["low"],row["low"]); b["close"]=row["close"]; b["volume"]+=row.get("volume",0.0)
    return [buckets[k] for k in sorted(buckets)]

def _yahoo_candles(symbol, interval):
    now=int(time.time())
    base_interval="1h" if interval=="4h" else {"1D":"1d"}.get(interval,interval)
    raw=_yahoo_get(symbol,base_interval,now-_yahoo_period(interval),now)
    ts=raw.get("timestamp") or []
    q=((raw.get("indicators") or {}).get("quote") or [{}])[0]
    opens=q.get("open") or []; highs=q.get("high") or []; lows=q.get("low") or []; closes=q.get("close") or []; vols=q.get("volume") or []
    rows=[]
    for i,t in enumerate(ts):
        try:
            o,h,l,cl=[float(x[i]) for x in (opens,highs,lows,closes)]
            if not all(math.isfinite(x) for x in (o,h,l,cl)) or h<max(o,cl) or l>min(o,cl) or h<l: continue
            rows.append({"start":int(t)*1000,"open":o,"high":h,"low":l,"close":cl,"volume":float(vols[i] or 0) if i<len(vols) else 0.0,"source":"yahoo_finance","confirm":True})
        except (TypeError,ValueError,IndexError): continue
    return _resample_ohlcv(rows,4*3600000) if interval=="4h" else rows

def _yahoo_quote(symbol):
    raw=_yahoo_get(symbol,"1d",int(time.time())-3*86400,int(time.time()))
    price=(raw.get("meta") or {}).get("regularMarketPrice")
    if price is None: raise RuntimeError("yahoo_quote_missing_price")
    return {"symbol":symbol,"price":float(price),"source":"yahoo_finance"}


def _moex_get(path, params=None):
    q=dict(params or {}); q.setdefault("iss.meta","off"); q.setdefault("iss.only","candles")
    req=urllib.request.Request(MOEX_BASE+path+"?"+urllib.parse.urlencode(q),headers={"User-Agent":"scalp-market-bridge/1.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=15) as r: return json.loads(r.read().decode("utf-8"))

def _moex_candles_native(symbol, interval, start=0):
    data=_moex_get(f"/engines/stock/markets/shares/boards/{MOEX_BOARD}/securities/{urllib.parse.quote(symbol)}/candles.json",{"interval":1 if interval=="1m" else MOEX_TF[interval],"start":int(start)})
    block=data.get("candles") or {}; cols=block.get("columns") or []; out=[]
    for values in block.get("data") or []:
        row=dict(zip(cols,values))
        try:
            parsed=datetime.fromisoformat(str(row.get("begin") or "").replace(" ","T"))
            if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=ZoneInfo("Europe/Moscow") if ZoneInfo else timezone.utc)
            out.append({"start":int(parsed.timestamp()*1000),"open":float(row["open"]),"high":float(row["high"]),"low":float(row["low"]),"close":float(row["close"]),"volume":float(row.get("volume") or 0),"value":float(row.get("value") or 0),"source":"moex_iss","confirm":True})
        except (KeyError,TypeError,ValueError,OverflowError): continue
    return out

def moex_candles(symbol, interval, target=500):
    # MOEX ISS candle intervals do not provide native 5m/15m on this endpoint.
    # Build intraday TFs from the native 1-minute stream to keep one authoritative source.
    if interval in ("5m","15m"):
        factor=5 if interval=="5m" else 15
        rows=[]; start=0
        while len(rows)<min(target*factor, 1000):
            page=_moex_candles_native(symbol,"1m",start=start)
            if not page: break
            rows.extend(page)
            if len(page)<500: break
            start+=len(page)
        return _resample_ohlcv(rows,factor*60000)[-target:]
    if interval=="4h": return _resample_ohlcv(moex_candles(symbol,"1h",target=max(target*4,500)),4*3600000)[-target:]
    rows=[]; start=0
    while len(rows)<target:
        page=_moex_candles_native(symbol,interval,start=start)
        if not page: break
        rows.extend(page)
        if len(page)<500: break
        start+=len(page)
    return rows[-target:]

def moex_quote(symbol):
    rows=moex_candles(symbol,"5m",target=2)
    if not rows: raise RuntimeError("moex_quote_missing_price")
    return {"symbol":symbol,"price":float(rows[-1]["close"]),"source":"moex_iss","delayed":True}

BYBIT_BASE="https://api.bybit.com"
def _bybit_get(path,params):
    q=urllib.parse.urlencode(params); req=urllib.request.Request(BYBIT_BASE+path+"?"+q,headers={"User-Agent":"scalp-market-bridge/1.0"})
    with urllib.request.urlopen(req,timeout=12) as r: data=json.loads(r.read().decode("utf-8"))
    if data.get("retCode") not in (0,None): raise RuntimeError(str(data))
    return data

def bybit_xstocks_top15():
    now=time.time()
    with _XSTOCKS_CACHE_LOCK:
        if _XSTOCKS_CACHE["symbols"] and now < _XSTOCKS_CACHE["expires"]:
            return list(_XSTOCKS_CACHE["symbols"])
    data=_bybit_get("/v5/market/instruments-info",{"category":"spot","symbolType":"xstocks","limit":1000})
    symbols=[x["symbol"] for x in data.get("result",{}).get("list",[]) if x.get("status")=="Trading"]
    if not symbols: return []
    tick=_bybit_get("/v5/market/tickers",{"category":"spot"})
    rows={x.get("symbol"):x for x in tick.get("result",{}).get("list",[])}
    top=sorted(symbols,key=lambda s:float(rows.get(s,{}).get("turnover24h") or 0),reverse=True)[:15]
    with _XSTOCKS_CACHE_LOCK:
        _XSTOCKS_CACHE["symbols"]=list(top)
        _XSTOCKS_CACHE["expires"]=now+_XSTOCKS_CACHE_TTL
    return top

def _interval_ms(interval):
    return {"1m":60000,"5m":300000,"15m":900000,"1h":3600000,"4h":14400000,"1D":86400000,
            "1":"60000","5":"300000","15":"900000","60":"3600000","240":"14400000","D":"86400000"}.get(str(interval),900000)

def _with_close_state(row, interval):
    start=int(row["start"])
    end=start+_interval_ms(interval)
    return {**row,"end":end,"confirm":bool(end<=int(time.time()*1000))}

def bybit_xstock_candles(symbol,interval="15",limit=500):
    data=_bybit_get("/v5/market/kline",{"category":"spot","symbol":symbol,"interval":interval,"limit":min(int(limit),1000)})
    out=[]
    for row in reversed(data.get("result",{}).get("list",[]) or []):
        try:
            out.append(_with_close_state({
                "start":int(row[0]),"open":float(row[1]),"high":float(row[2]),
                "low":float(row[3]),"close":float(row[4]),"volume":float(row[5]),
                "turnover":float(row[6]),"source":"bybit_xstocks"
            }, interval))
        except (IndexError,TypeError,ValueError): pass
    return out

MARKET_PROFILES = {
    "crypto": {"session_model":"24_7","volume_model":"exchange_volume","oi":True,"funding":True,"liquidations":True,"orderbook":True,"cvd":True,"execution":"exchange_perpetual_or_spot","special_events":["pump_exhaustion","short_squeeze","long_liquidation_cascade","liquidity_sweep"]},
    "stocks": {"session_model":"secondary_market_24_7","volume_model":"exchange_volume","oi":False,"funding":False,"liquidations":False,"orderbook":True,"cvd":False,"execution":"bybit_xstock_spot","special_events":["gap","opening_range","session_liquidity","failed_breakout"]},
    "ru_stocks": {"session_model":"moex_equities_session","volume_model":"exchange_volume","oi":False,"funding":False,"liquidations":False,"orderbook":False,"cvd":False,"execution":"moex_spot_long_only","direction_policy":"long_only","special_events":["gap","opening_range","session_liquidity","failed_breakout"]},
    "forex": {"session_model":"asia_london_newyork","volume_model":"tick_or_provider_volume","oi":False,"funding":False,"liquidations":False,"orderbook":False,"cvd":False,"execution":"external_fx_broker_required","special_events":["session_liquidity","london_breakout","ny_reversal","failed_breakout"]},
    "commodities": {"session_model":"instrument_session","volume_model":"provider_volume","oi":False,"funding":False,"liquidations":False,"orderbook":False,"cvd":False,"execution":"external_commodity_broker_required","special_events":["session_liquidity","inventory_event","contract_rollover","failed_breakout"]},
}

def market_profile(market):
    return dict(MARKET_PROFILES.get(str(market).lower(), {}))

def session_context(market, now_epoch=None):
    market=str(market).lower()
    now=float(now_epoch) if now_epoch is not None else time.time()
    utc_dt=datetime.fromtimestamp(now, timezone.utc)
    if market=="forex" and ZoneInfo:
        london=utc_dt.astimezone(ZoneInfo("Europe/London"))
        newyork=utc_dt.astimezone(ZoneInfo("America/New_York"))
        lh=london.hour+london.minute/60.0
        nh=newyork.hour+newyork.minute/60.0
        if 8 <= lh < 13 and 8 <= nh < 17: session="LONDON_NY_OVERLAP"
        elif 8 <= lh < 17: session="LONDON"
        elif 8 <= nh < 17: session="NEW_YORK"
        elif 0 <= utc_dt.hour < 8: session="ASIA"
        else: session="ROLLOVER"
    elif market=="stocks": session="GLOBAL_XSTOCKS_24_7"
    elif market=="ru_stocks": session="MOEX_EQUITIES"
    elif market=="commodities": session="INSTRUMENT_SESSION"
    else: session="24_7"
    return {"market":market,"session":session,"utc_hour":round(utc_dt.hour+utc_dt.minute/60.0,2),"profile":market_profile(market)}

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
                            "volume":float(row.get("volume") or 0),
                            "confirm":bool(row.get("confirm",True)),
                            "end":row.get("end")})
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
        policy=market_block_policy(market)
        return {
            "router_version":cls.VERSION,
            "market":str(market).lower(),"symbol":symbol,
            "profile":profile,"policy":policy,"session_context":session_context(market),
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
        return self._parse_values(data.get("values",[]) or [], interval)
    def quote(self,symbol): return self._get("/quote",{"symbol":symbol})

    @staticmethod
    def _parse_values(values, interval):
        out=[]
        for x in values or []:
            try:
                raw_dt=str(x["datetime"]).replace("Z","+00:00")
                parsed=datetime.fromisoformat(raw_dt)
                out.append(_with_close_state({
                    "start":int(parsed.timestamp()*1000),"datetime":x["datetime"],
                    "open":float(x["open"]),"high":float(x["high"]),
                    "low":float(x["low"]),"close":float(x["close"]),
                    "volume":float(x.get("volume") or 0),"source":"twelve_data"
                }, interval))
            except (KeyError,TypeError,ValueError,OverflowError):
                pass
        return out

    def candles_batch(self,symbols,interval,outputsize=500):
        symbols=[str(s) for s in symbols if str(s)]
        if not symbols:
            return {}
        data=self._get("/time_series",{
            "symbol":",".join(symbols),
            "interval":TF[interval],
            "outputsize":min(int(outputsize),5000),
            "order":"ASC","timezone":"UTC",
        })
        # Twelve Data returns a single response for one symbol and keyed
        # responses for multi-symbol batch requests. Normalize both forms.
        if isinstance(data.get("values"),list):
            key=str((data.get("meta") or {}).get("symbol") or symbols[0])
            return {key:self._parse_values(data.get("values"),interval)}
        result={}
        for key,payload in (data.items() if isinstance(data,dict) else []):
            if not isinstance(payload,dict) or key in ("status","message"):
                continue
            if isinstance(payload.get("values"),list):
                result[str(key)]=self._parse_values(payload.get("values"),interval)
        return result

    def quotes_batch(self,symbols):
        symbols=[str(s) for s in symbols if str(s)]
        if not symbols:
            return {}
        data=self._get("/quote",{"symbol":",".join(symbols)})
        if isinstance(data,dict) and "symbol" in data:
            return {str(data.get("symbol")):data}
        return {str(k):v for k,v in data.items()
                if isinstance(v,dict) and ("close" in v or "price" in v or "symbol" in v)}

    def scan_many(self,market,symbols):
        symbols=list(dict.fromkeys(str(s) for s in symbols if str(s)))
        if not symbols:
            return {}
        if market in ("stocks","ru_stocks"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(3,len(symbols))) as pool:
                vals=list(pool.map(lambda s:self.scan(market,s),symbols))
            return {v.get("symbol"):v for v in vals}

        frames_by_symbol={s:{} for s in symbols}
        errors_by_symbol={s:{} for s in symbols}
        for tf in ("1D","4h","1h","15m","5m"):
            try:
                batch=self.candles_batch(symbols,tf)
                for s in symbols:
                    frames_by_symbol[s][tf]=batch.get(s,[])
                    if not batch.get(s):
                        errors_by_symbol[s][tf]="no_data_in_batch_response"
            except Exception as exc:
                for s in symbols:
                    frames_by_symbol[s][tf]=[]
                    errors_by_symbol[s][tf]=f"{type(exc).__name__}:{exc}"
        try:
            quotes=self.quotes_batch(symbols)
        except Exception as exc:
            quotes={}; quote_error=f"{type(exc).__name__}:{exc}"
        results={}
        for symbol in symbols:
            frames=frames_by_symbol[symbol]
            errors=errors_by_symbol[symbol]
            quote=quotes.get(symbol)
            if quote is None and "quote_error" in locals():
                quote={"error":quote_error}
            ready=all(sum(1 for row in frames.get(tf,[]) if row.get("confirm",True))>=50 for tf in ("1D","4h","1h","15m","5m"))
            event_frames={tf:frames[tf] for tf in ("1D","4h","1h","15m","5m") if frames.get(tf)}
            events={tf:detect_events(market,symbol,rows) for tf,rows in event_frames.items()}
            analysis_core=MarketProfileRouter.analyze(market,symbol,frames)
            setup={tf:build_setup_plan(market,symbol,rows,events[tf],analysis_core=analysis_core) for tf,rows in event_frames.items()}
            result={"market":market,"symbol":symbol,"adapter_version":self.VERSION,"provider":"twelve_data",
                    "configured":self.configured,"analysis_ready":ready,"execution_ready":False,
                    "execution_reason":"external_market_execution_connector_not_configured",
                    "frames":frames,"quote":quote,"analysis_core":analysis_core,"events":events,
                    "setup_plans":setup,"errors":errors,
                    "capabilities":{"ohlcv":True,"realtime_quote":True,"orderbook":False,
                                    "open_interest":False,"funding":False,"spot_cvd":False},
                    "market_profile":market_profile(market),"session_context":session_context(market),
                    "data_quality":{"state":"READY" if ready else "PARTIAL",
                                    "missing_timeframes":[tf for tf in ("1D","4h","1h","15m","5m")
                                                          if len(frames.get(tf,[]))<50]}}
            enriched=enrich_external_result(result,market)
            enriched.pop("frames",None)
            if isinstance(enriched.get("analysis_core"),dict):
                enriched["analysis_core"].pop("frames",None)
            results[symbol]=enriched
        return results
    def scan(self,market,symbol):
        frames={}; errors={}
        provider="twelve_data"
        if market=="ru_stocks":
            provider="moex_iss"
            def fetch_ru(tf):
                try: return tf, moex_candles(symbol,tf,target=500), None
                except Exception as exc: return tf, [], f"{type(exc).__name__}:{exc}"
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                for tf, rows, err in pool.map(fetch_ru, ("1D","4h","1h","15m","5m")):
                    frames[tf]=rows
                    if err: errors[tf]=err
            try: quote=moex_quote(symbol)
            except Exception as quote_exc: quote={"error":f"{type(quote_exc).__name__}:{quote_exc}"}
            configured=True
        elif market=="stocks":
            provider="bybit_xstocks+yahoo_fallback"
            tf_intervals={"1D":"D","4h":"240","1h":"60","15m":"15","5m":"5"}
            yahoo_symbol=YAHOO_STOCK_MAP.get(symbol)
            def fetch_xstock(item):
                tf,itv=item
                try:
                    return tf, bybit_xstock_candles(symbol,itv), None
                except Exception as bybit_exc:
                    if yahoo_symbol:
                        try: return tf, _yahoo_candles(yahoo_symbol,tf), f"bybit_fallback:yahoo:{type(bybit_exc).__name__}"
                        except Exception as yahoo_exc: return tf, [], f"bybit:{type(bybit_exc).__name__};yahoo:{type(yahoo_exc).__name__}"
                    return tf, [], f"{type(bybit_exc).__name__}:no_yahoo_mapping"
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                for tf, rows, err in pool.map(fetch_xstock, tuple(tf_intervals.items())):
                    frames[tf]=rows
                    if err: errors[tf]=err
            try: quote=_yahoo_quote(yahoo_symbol) if yahoo_symbol else bybit_xstock_candles(symbol,"5",limit=2)[-1]
            except Exception as quote_exc: quote={"error":f"{type(quote_exc).__name__}:{quote_exc}"}
            configured=True
        else:
            def fetch_tf(tf):
                try: return tf, self.candles(symbol,tf), None
                except Exception as e: return tf, [], f"{type(e).__name__}:{e}"
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                for tf, rows, err in pool.map(fetch_tf, ("1D","4h","1h","15m","5m")):
                    frames[tf]=rows
                    if err: errors[tf]=err
            try: quote=self.quote(symbol)
            except Exception as e: quote={"error":f"{type(e).__name__}:{e}"}
            configured=self.configured
            if not self.configured:
                yahoo_symbol=YAHOO_FOREX_MAP.get(symbol) if market=="forex" else YAHOO_COMMODITY_MAP.get(symbol)
                if yahoo_symbol:
                    provider="yahoo_finance"
                    for tf in ("1D","4h","1h","15m","5m"):
                        try: frames[tf]=_yahoo_candles(yahoo_symbol,tf)
                        except Exception as exc: frames[tf]=[]; errors[tf]=f"{type(exc).__name__}:{exc}"
                    try: quote=_yahoo_quote(yahoo_symbol)
                    except Exception as exc: quote={"error":f"{type(exc).__name__}:{exc}"}
                    configured=True
        ready=all(sum(1 for row in frames.get(tf,[]) if row.get("confirm",True))>=50 for tf in ("1D","4h","1h","15m","5m"))
        event_frames={tf:frames[tf] for tf in ("1D","4h","1h","15m","5m") if frames.get(tf)}
        events={tf:detect_events(market,symbol,rows) for tf,rows in event_frames.items()}
        analysis_core=MarketProfileRouter.analyze(market,symbol,frames)
        setup={tf:build_setup_plan(market,symbol,rows,events[tf],analysis_core=analysis_core) for tf,rows in event_frames.items()}
        result={"market":market,"symbol":symbol,"adapter_version":self.VERSION,"provider":provider,"configured":configured,"analysis_ready":ready,"execution_ready":False,"execution_reason":"external_market_execution_connector_not_configured","frames":frames,"quote":quote,"analysis_core":analysis_core,"events":events,"setup_plans":setup,"errors":errors,"capabilities":{"ohlcv":True,"realtime_quote":True,"orderbook":False,"open_interest":False,"funding":False,"spot_cvd":False},"market_profile":market_profile(market),"session_context":session_context(market),"data_quality":{"state":"READY" if ready else "PARTIAL","missing_timeframes":[tf for tf in ("1D","4h","1h","15m","5m") if sum(1 for row in frames.get(tf,[]) if row.get("confirm",True))<50]}}
        enriched=enrich_external_result(result,market)
        if market=="ru_stocks":
            enriched["direction_policy"]="long_only"
            setup_obj=enriched.get("setup")
            if isinstance(setup_obj,dict) and str(setup_obj.get("direction") or "").lower() in ("short","sell","bearish"):
                setup_obj["tradeable"]=False; setup_obj["status"]="WATCH"; setup_obj["block_reason"]="ru_stocks_spot_long_only"
            for plan in (enriched.get("setup_plans") or {}).values():
                if isinstance(plan,dict) and str(plan.get("direction") or "").lower() in ("short","sell","bearish"):
                    plan["tradeable"]=False; plan["status"]="WATCH"; plan["block_reason"]="ru_stocks_spot_long_only"
        # Raw OHLCV is an internal input, not part of the public unified payload.
        enriched.pop("frames",None)
        if isinstance(enriched.get("analysis_core"),dict):
            enriched["analysis_core"].pop("frames",None)
        return enriched
def external_universe():
    try:
        stocks = bybit_xstocks_top15()
        source = "bybit_xstocks"
    except Exception as exc:
        # Public fallback keeps unified scanning alive if xStocks discovery is unavailable.
        stocks = list(STOCKS)
        source = f"fallback:{type(exc).__name__}"
    return {
        "forex": list(FOREX),
        "commodities": list(COMMODITIES),
        "stocks": stocks,
        "ru_stocks": list(RU_STOCKS),
        "_meta": {"stocks_source": source},
    }
