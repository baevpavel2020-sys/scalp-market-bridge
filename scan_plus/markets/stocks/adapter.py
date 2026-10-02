"""Independent tokenized-stock market adapter."""
from typing import Any, Mapping
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile
from scan_plus.priority_engine import resolve_priorities
from scan_plus.core.analytical_engine import AnalyticalEngine, CanonicalPricePatternBackend
from scan_plus.core.sessions import active_sessions
from scan_plus.core.gaps import gap_from_previous
from scan_plus.core.session_levels import session_high_low
from scan_plus.markets.stocks.bybit_xstocks import BybitXStocksLoader
from scan_plus.markets.stocks.session_gap import classify_gap, gap_fill_progress
from scan_plus.markets.stocks.candidate import build_stock_candidate


class StocksMarketAdapter(MarketAdapter):
    market = "stocks"

    def __init__(self, loader=None, underlying_provider=None, engine=None):
        self.loader = loader or BybitXStocksLoader()
        self.underlying_provider = underlying_provider
        self.engine = engine or AnalyticalEngine(CanonicalPricePatternBackend())

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    def default_symbols(self, limit=15):
        return [str(s).upper() for s in self.loader.default_symbols(limit=limit)]

    def _underlying(self, symbol):
        if self.underlying_provider is None:
            return None
        return self.underlying_provider.normalize(symbol, self.underlying_provider.quote(symbol))

    @staticmethod
    def _gap_context(underlying):
        if not underlying or not underlying.get("ready", True):
            return {"ready": False, "reason": (underlying or {}).get("reason", "underlying_provider_unavailable")}
        current_open = underlying.get("session_open", underlying.get("open"))
        previous_close = underlying.get("previous_close", underlying.get("prev_close"))
        if current_open is None or previous_close is None:
            return {"ready": False, "reason": "underlying_gap_fields_unavailable"}
        gap = gap_from_previous(current_open, previous_close)
        gap["ready"] = True
        return gap

    def prescan(self, symbol=None, **kwargs) -> Mapping[str, Any]:
        if not symbol:
            return {"market": self.market, "status": "PRESCAN_ONLY",
                    "profile": self.profile(), "reason": "symbol_required"}
        symbol = str(symbol).upper()
        ticker = self.loader.ticker(symbol)
        underlying = self._underlying(symbol)
        gap = self._gap_context(underlying)
        return {
            "market": self.market, "symbol": symbol, "status": "OK",
            "profile": self.profile(symbol),
            "product": ticker["product"], "token_symbol": ticker["symbol"],
            "last_price": ticker["last_price"], "volume_24h": ticker["volume_24h"],
            "underlying": underlying,
            "gap": gap,
            "gap_classification": classify_gap(gap),
            "underlying_sessions": active_sessions("stocks_us"),
        }

    def scan(self, symbol: str) -> Mapping[str, Any]:
        symbol = str(symbol).upper()
        profile = self.profile(symbol)
        underlying = self._underlying(symbol)
        frames = {}
        for label, interval in (("5m","5"),("15m","15"),("1h","60"),("4h","240")):
            data = self.loader.klines(symbol, interval=interval, limit=240)
            rows = data["candles"]
            frame = {"bars": len(rows),
                     "latest_timestamp_ms": max(
                         [r.get("timestamp_ms") for r in rows if r.get("timestamp_ms") is not None],
                         default=None,
                     ),
                     "analysis": self.engine.analyze_profiled(rows, profile.get("scan") or [])}
            if label == "15m":
                frame["session_levels"] = session_high_low(
                    rows, market="stocks_us", session="regular"
                )
            frames[label] = frame
        ticker = self.loader.ticker(symbol)
        gap = self._gap_context(underlying)
        latest_ts=max(
            [frame.get("latest_timestamp_ms") for frame in frames.values()
             if frame.get("latest_timestamp_ms") is not None],
            default=None,
        )
        latest_session=active_sessions("stocks_us",latest_ts)
        fill = None
        if underlying and gap.get("ready"):
            fill = gap_fill_progress(
                underlying.get("session_open"),
                underlying.get("previous_close"),
                ticker.get("last_price"),
            )
        return {
            "market": self.market, "symbol": symbol, "status": "OK",
            "profile": profile, "ticker": ticker, "underlying": underlying,
            "gap": gap,
            "gap_classification": classify_gap(gap),
            "gap_fill": fill,
            "underlying_sessions": latest_session,
            "candidate": build_stock_candidate(
                frames=frames, gap=gap, underlying=underlying,
                ticker=ticker, session=latest_session,
            ),
            "priority": resolve_priorities(
                "stocks", symbol,
                "gap" if classify_gap(gap).get("state") in ("gap_up","gap_down") else "continuation"
            ),
            "frames": frames,
            "execution_context": {
                "product": "xstock_spot", "xstock_24_7": True,
                "underlying_session_required_for_session_signals": True,
                "order_submission": "disabled_by_analysis_layer",
            },
        }

    def diagnostics(self, symbol: str) -> Mapping[str, Any]:
        return {"market": self.market, "symbol": str(symbol).upper(),
                "loader": type(self.loader).__name__,
                "underlying_provider": (
                    type(self.underlying_provider).__name__
                    if self.underlying_provider else None
                ),
                "shared_engine": type(self.engine).__name__}
