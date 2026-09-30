"""Independent commodities market adapter.

Metals, energy and softs share the commodity boundary but keep distinct instrument
profiles. This layer does not invent futures-session, inventory or macro data when
the provider has not supplied it.
"""
from typing import Any, Mapping
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile
from scan_plus.priority_engine import resolve_priorities
from scan_plus.core.analytical_engine import AnalyticalEngine, CanonicalPricePatternBackend
from scan_plus.markets.commodities.loader import CommodityLoader


class CommoditiesMarketAdapter(MarketAdapter):
    market="commodities"

    def __init__(self, loader, engine=None):
        self.loader=loader if isinstance(loader,CommodityLoader) else CommodityLoader(loader)
        self.engine=engine or AnalyticalEngine(CanonicalPricePatternBackend())

    def profile(self,symbol=None):
        return get_profile(self.market,symbol)

    @staticmethod
    def _normalize(rows):
        out=[]
        for row in rows or []:
            item=dict(row)
            try:
                if "timestamp_ms" in item:
                    item["timestamp_ms"]=int(float(item["timestamp_ms"]))
                elif "timestamp" in item:
                    value=float(item["timestamp"])
                    item["timestamp_ms"]=int(value*1000 if value<1e11 else value)
            except (TypeError,ValueError,OverflowError):
                pass
            out.append(item)
        return out

    def _load(self,symbol,interval):
        try:
            return self.loader.candles(symbol,interval=interval)
        except (LookupError,NotImplementedError,RuntimeError,TypeError) as exc:
            return {"source":type(self.loader).__name__,"product":"commodity",
                    "symbol":str(symbol).upper(),"interval":interval,
                    "candles":[],"error":str(exc)}

    def prescan(self,symbol=None,**kwargs)->Mapping[str,Any]:
        if not symbol:
            return {"market":self.market,"status":"PRESCAN_ONLY","profile":self.profile(),
                    "reason":"symbol_required"}
        symbol=str(symbol).upper()
        interval=kwargs.get("interval","60")
        data=self._load(symbol,interval)
        rows=self._normalize(data.get("candles"))
        return {"market":self.market,"symbol":symbol,"status":"OK",
                "profile":self.profile(symbol),"source":data.get("source"),
                "interval":interval,"bars":len(rows),"provider_error":data.get("error"),
                "provider_context":{k:v for k,v in data.items()
                                    if k not in {"candles"}}}

    def scan(self,symbol:str)->Mapping[str,Any]:
        symbol=str(symbol).upper()
        profile=self.profile(symbol)
        frames={}
        for label,interval in (("5m","5"),("15m","15"),("1h","60"),("4h","240")):
            data=self._load(symbol,interval)
            rows=self._normalize(data.get("candles"))
            frames[label]={
                "bars":len(rows),
                "provider_error":data.get("error"),
                "analysis":self.engine.analyze_profiled(rows,profile.get("scan") or []),
                "provider_context":{k:v for k,v in data.items() if k!="candles"},
                "latest_timestamp_ms":max(
                    [r.get("timestamp_ms") for r in rows if r.get("timestamp_ms") is not None],
                    default=None,
                ),
            }
        latest=max(
            [f.get("latest_timestamp_ms") for f in frames.values()
             if f.get("latest_timestamp_ms") is not None],
            default=None,
        )
        return {
            "market":self.market,"symbol":symbol,"status":"OK","profile":profile,
            "priority":resolve_priorities("commodities",symbol,"continuation"),
            "frames":frames,
            "instrument_group":profile.get("group"),
            "latest_timestamp_ms":latest,
            "execution_context":{
                "product":"commodity",
                "instrument_group":profile.get("group"),
                "provider_context_only":True,
                "no_unprovided_inventory_or_macro_assumptions":True,
            },
        }

    def diagnostics(self,symbol:str)->Mapping[str,Any]:
        return {"market":self.market,"symbol":str(symbol).upper(),
                "loader":type(self.loader).__name__,
                "shared_engine":type(self.engine).__name__,
                "group":self.profile(symbol).get("group")}
