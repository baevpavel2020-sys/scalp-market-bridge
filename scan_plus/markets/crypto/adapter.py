"""Compatibility adapter for the existing Crypto engine.

During extraction the legacy engine remains the source of truth. This wrapper gives
the multi-market orchestrator a stable boundary without changing Crypto decisions.
"""
from typing import Any, Mapping

from dynamic_collector import dynamic_manager, run_prescan
from scan_plus.contracts import MarketAdapter
from scan_plus.market_profiles import get_profile
from scan_plus.markets.crypto.event_adapter import observe_crypto_events
from scan_plus.core.rolling_baseline import RollingMoveBaseline
from scan_plus.markets.crypto.shadow_recorder import ShadowRecorder


class CryptoMarketAdapter(MarketAdapter):
    market = "crypto"

    def __init__(self, manager=None, prescan_runner=None):
        self._manager = manager or dynamic_manager
        self._prescan_runner = prescan_runner or run_prescan
        self._move_baselines = {}
        self._shadow_recorder = ShadowRecorder()

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    def prescan(self, **kwargs) -> Mapping[str, Any]:
        return self._prescan_runner(**kwargs)

    def scan(self, symbol: str) -> Mapping[str, Any]:
        # Compatibility invariant: legacy result remains authoritative.
        legacy = self._manager.scan(symbol)
        result = dict(legacy)
        event_engine = observe_crypto_events(legacy)

        # Learn each symbol independently from quality-ready rolling windows.
        key = str(legacy.get("symbol") or symbol).upper()
        baseline = self._move_baselines.setdefault(key, RollingMoveBaseline())
        windows = ((legacy.get("execution") or {}).get("windows") or {})
        sample = windows.get("5m") or windows.get("15m") or {}
        move = sample.get("perp_price_change_pct")
        if sample.get("perp_flow_usable") and move is not None:
            adaptive = baseline.is_abnormal(move)
            threshold = baseline.threshold()
            event_engine["adaptive_abnormal_pump"] = {
                "ready": baseline.ready(),
                "sample_count": len(baseline.values),
                "threshold_abs_pct": threshold,
                "current_abs_move_pct": abs(float(move)),
                "confirmed": adaptive,
            }
            # Evaluate against prior history; only then learn the current observation.
            baseline.add(move)
        else:
            event_engine["adaptive_abnormal_pump"] = {
                "ready": baseline.ready(), "sample_count": len(baseline.values),
                "threshold_abs_pct": baseline.threshold(), "current_abs_move_pct": None,
                "confirmed": None,
            }

        result["event_engine"] = event_engine
        # Record the exact point-in-time evidence used by shadow mode. Recording
        # failure must never break the live scanner.
        try:
            self._shadow_recorder.record(legacy, event_engine)
        except (OSError, TypeError, ValueError) as exc:
            result["event_engine"]["shadow_recording"] = {
                "ok": False, "error": f"{type(exc).__name__}: {exc}"
            }
        else:
            result["event_engine"]["shadow_recording"] = {"ok": True}
        return result

    def diagnostics(self, symbol: str) -> Mapping[str, Any]:
        return self._manager.diagnostics(symbol)
