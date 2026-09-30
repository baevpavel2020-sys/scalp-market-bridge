"""Independent Forex market adapter.

Forex owns its feed, sessions and session liquidity context. Crypto and stock-specific
data assumptions are deliberately excluded.
"""
from typing import Any, Mapping
from datetime import datetime, timezone
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile
from scan_plus.priority_engine import resolve_priorities
from scan_plus.core.analytical_engine import AnalyticalEngine, LegacyCryptoBackend
from scan_plus.core.sessions import active_sessions, session_overlap
from scan_plus.core.session_levels import session_high_low
from scan_plus.markets.forex.stooq import StooqFXLoader


class ForexMarketAdapter(MarketAdapter):
    market = "forex"

    def __init__(self, loader=None, engine=None):
        self.loader = loader or StooqFXLoader()
        self.engine = engine or AnalyticalEngine(LegacyCryptoBackend())

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    @staticmethod
    def _normalize_rows(rows):
        out=[]
        for row in rows or []:
            item=dict(row)
            if "timestamp_ms" not in item:
                raw=item.get("date") or item.get("datetime") or item.get("timestamp")
                if raw is not None:
                    try:
                        if isinstance(raw,(int,float)):
                            ts=float(raw)
                            if ts < 1e11: ts *= 1000
                            item["timestamp_ms"]=int(ts)
                        else:
                            value=str(raw).replace("Z","+00:00")
                            dt=datetime.fromisoformat(value)
                            if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
                            item["timestamp_ms"]=int(dt.timestamp()*1000)
                    except (TypeError,ValueError,OverflowError):
                        pass
            out.append(item)
        return out

    @staticmethod
    def _latest_timestamp(rows):
        values=[r.get("timestamp_ms") for r in rows or [] if r.get("timestamp_ms") is not None]
        return max(values) if values else None

    def _session_context(self, rows):
        context={}
        ts=self._latest_timestamp(rows)
        for name in ("asia","london","new_york"):
            context[name]=session_high_low(rows,market="forex",session=name)
        context["active"]=active_sessions("forex",ts)
        context["overlap"]=session_overlap("forex",ts)
        context["timestamp_ms"]=ts
        return context

    def prescan(self, symbol=None, **kwargs) -> Mapping[str, Any]:
        if not symbol:
            return {"market":self.market,"status":"PRESCAN_ONLY",
                    "profile":self.profile(),"reason":"symbol_required"}
        symbol=str(symbol).upper().replace("/","")
        data=self.loader.candles(symbol,interval=kwargs.get("interval","60"))
        rows=self._normalize_rows(data.get("candles"))
        return {
            "market":self.market,"symbol":symbol,"status":"OK",
            "profile":self.profile(symbol),
            "source":data.get("source"),"interval":data.get("interval"),
            "bars":len(rows),"session":self._session_context(rows),
        }

    def scan(self, symbol: str) -> Mapping[str, Any]:
        symbol=str(symbol).upper().replace("/","")
        profile=self.profile(symbol)
        frames={}
        for label,interval in (("5m","5"),("15m","15"),("1h","60"),("4h","240")):
            data=self.loader.candles(symbol,interval=interval)
            rows=self._normalize_rows(data.get("candles"))
            frames[label]={
                "bars":len(rows),
                "analysis":self.engine.analyze_profiled(rows,profile.get("scan") or []),
                "sessions":self._session_context(rows),
            }
        # Use the newest candle across frames for the situation timestamp.
        all_ts=[]
        for frame in frames.values():
            ts=frame.get("sessions",{}).get("timestamp_ms")
            if ts is not None: all_ts.append(ts)
        latest_ts=max(all_ts) if all_ts else None
        situation="continuation"
        active=active_sessions("forex",latest_ts)
        if len(active)>=2: situation="session_overlap"
        return {
            "market":self.market,"symbol":symbol,"status":"OK",
            "profile":profile,
            "priority":resolve_priorities("forex",symbol,situation),
            "active_sessions":active,
            "session_overlap":session_overlap("forex"),
            "frames":frames,
            "execution_context":{
                "product":"spot_fx",
                "session_sensitive":True,
                "no_crypto_oi_funding_liquidations":True,
            },
        }

    def diagnostics(self, symbol: str) -> Mapping[str, Any]:
        return {"market":self.market,"symbol":str(symbol).upper().replace("/",""),
                "loader":type(self.loader).__name__,
                "shared_engine":type(self.engine).__name__}
