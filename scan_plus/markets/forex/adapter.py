"""Independent Forex market adapter.

Forex owns its feed, sessions and session liquidity context. Crypto and stock-specific
data assumptions are deliberately excluded.
"""
from typing import Any, Mapping
from datetime import datetime, timezone
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile
from scan_plus.priority_engine import resolve_priorities
from scan_plus.core.analytical_engine import AnalyticalEngine, CanonicalPricePatternBackend
from scan_plus.core.sessions import active_sessions, session_overlap
from scan_plus.core.session_levels import session_high_low
from scan_plus.core.data_contract import normalize_candles
from scan_plus.markets.forex.yahoo import YahooFXLoader
from scan_plus.markets.forex.session_liquidity import session_interaction, session_sweep
from scan_plus.markets.forex.candidate import build_forex_candidate
from scan_plus.markets.forex.execution import build_forex_execution_plan


class ForexMarketAdapter(MarketAdapter):
    market = "forex"

    def __init__(self, loader=None, engine=None):
        self.loader = loader or YahooFXLoader()
        self.engine = engine or AnalyticalEngine(CanonicalPricePatternBackend())

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    def default_symbols(self, limit=9):
        universe=("EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","NZDUSD","EURJPY","EURGBP")
        return list(universe[:int(limit)])

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
            context[name]=session_high_low(rows,market="forex",session=name,before_timestamp_ms=ts)
        context["active"]=active_sessions("forex",ts)
        context["overlap"]=session_overlap("forex",ts)
        context["timestamp_ms"]=ts
        return context

    def _load(self,symbol,interval):
        try:
            return self.loader.candles(symbol,interval=interval)
        except (NotImplementedError,LookupError) as exc:
            return {"source":type(self.loader).__name__,"product":"fx",
                    "symbol":symbol,"interval":interval,"candles":[],"error":str(exc)}

    def prescan(self, symbol=None, **kwargs) -> Mapping[str, Any]:
        if not symbol:
            return {"market":self.market,"status":"PRESCAN_ONLY",
                    "profile":self.profile(),"reason":"symbol_required"}
        symbol=str(symbol).upper().replace("/","")
        data=self._load(symbol,kwargs.get("interval","60"))
        rows=self._normalize_rows(data.get("candles"))
        rows, quality=normalize_candles(rows, symbol=symbol, interval=data.get("interval", kwargs.get("interval","60")), closed_only=True)
        return {
            "market":self.market,"symbol":symbol,"status":"OK",
            "profile":self.profile(symbol),
            "source":data.get("source"),"interval":data.get("interval"),
            "bars":len(rows),"data_quality":quality,"session":self._session_context(rows),
            "provider_error":data.get("error"),
        }

    def scan(self, symbol: str) -> Mapping[str, Any]:
        symbol=str(symbol).upper().replace("/","")
        profile=self.profile(symbol)
        frames={}
        daily=self._load(symbol,"d")
        daily_rows=self._normalize_rows(daily.get("candles"))
        daily_rows,daily_quality=normalize_candles(
            daily_rows,symbol=symbol,interval="d",closed_only=True
        )
        frames["1d"]={
            "bars":len(daily_rows),
            "data_quality":daily_quality,
            "provider_error":daily.get("error"),
            "analysis":self.engine.analyze_profiled(daily_rows,profile.get("scan") or []),
            "sessions":self._session_context(daily_rows),
        }
        for label,interval in (("5m","5"),("15m","15"),("1h","60"),("4h","240")):
            data=self._load(symbol,interval)
            rows=self._normalize_rows(data.get("candles"))
            rows, quality=normalize_candles(rows, symbol=symbol, interval=data.get("interval",interval), closed_only=True)
            sessions=self._session_context(rows)
            frames[label]={
                "bars":len(rows),
                "data_quality":quality,
                "latest_close":rows[-1].get("close") if rows else None,
                "provider_error":data.get("error"),
                "analysis":self.engine.analyze_profiled(rows,profile.get("scan") or []),
                "sessions":sessions,
                "session_liquidity":{
                    "asia":session_interaction(rows[-1].get("close") if rows else None,sessions.get("asia")),
                    "london":session_interaction(rows[-1].get("close") if rows else None,sessions.get("london")),
                    "new_york":session_interaction(rows[-1].get("close") if rows else None,sessions.get("new_york")),
                    "latest_candle_sweeps":{
                        "asia":session_sweep(rows,sessions.get("asia")),
                        "london":session_sweep(rows,sessions.get("london")),
                        "new_york":session_sweep(rows,sessions.get("new_york")),
                    },
                },
            }
        # Use the newest candle across frames for the situation timestamp.
        all_ts=[]
        for frame in frames.values():
            ts=frame.get("sessions",{}).get("timestamp_ms")
            if ts is not None: all_ts.append(ts)
        latest_ts=max(all_ts) if all_ts else None
        sweep_exists=any(
            ((frame.get("session_liquidity") or {}).get("latest_candle_sweeps") or {}).get(name,{}).get("state")
            in ("high_sweep","low_sweep")
            for frame in frames.values()
            for name in ("asia","london","new_york")
        )
        situation="continuation"
        active=active_sessions("forex",latest_ts)
        if sweep_exists: situation="session_sweep"
        elif len(active)>=2: situation="session_overlap"
        candidate=build_forex_candidate(
            frames=frames,
            active_sessions=active,
            overlap=session_overlap("forex",latest_ts),
        )
        latest_price=(frames.get("5m") or {}).get("latest_close")
        limit_plan=build_forex_execution_plan(
            candidate=candidate, frames=frames, price=latest_price, equity=None, risk_fraction=None, symbol=symbol
        ) if latest_price is not None else {"status":"WAIT","reason":"latest_price_unavailable"}
        candidate=dict(candidate)
        candidate["limit_plan"]=limit_plan if limit_plan.get("status")=="PLAN" else None
        return {
            "market":self.market,"symbol":symbol,"status":"OK",
            "profile":profile,
            "priority":resolve_priorities("forex",symbol,situation),
            "active_sessions":active,
            "session_overlap":session_overlap("forex",latest_ts),
            "frames":frames,
            "candidate":candidate,
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
