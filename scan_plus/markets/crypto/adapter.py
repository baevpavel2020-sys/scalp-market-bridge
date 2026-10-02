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
from scan_plus.markets.crypto.event_memory import CausalEventMemory


class CryptoMarketAdapter(MarketAdapter):
    market = "crypto"

    def __init__(self, manager=None, prescan_runner=None):
        self._manager = manager or dynamic_manager
        self._prescan_runner = prescan_runner or run_prescan
        self._move_baselines = {}
        self._shadow_recorder = ShadowRecorder()
        self._event_memory = CausalEventMemory()

    def profile(self, symbol=None):
        return get_profile(self.market, symbol)

    def default_symbols(self, limit=30):
        prescan=self._prescan_runner(top_n=int(limit),shortlist=max(int(limit),30))
        out=[]
        for item in prescan.get("candidates") or prescan.get("scan_plus_candidates") or []:
            symbol=item.get("symbol") if isinstance(item,Mapping) else None
            if symbol and symbol not in out:
                out.append(str(symbol).upper())
            if len(out)>=int(limit):
                break
        return out

    def prescan(self, **kwargs) -> Mapping[str, Any]:
        return self._prescan_runner(**kwargs)

    def scan(self, symbol: str) -> Mapping[str, Any]:
        # Compatibility invariant: legacy result remains authoritative.
        legacy = self._manager.scan(symbol)
        result = dict(legacy)

        # Evaluate adaptive abnormality BEFORE the event decision, using prior
        # observations only. This keeps the current move out of its own baseline.
        key = str(legacy.get("symbol") or symbol).upper()
        baseline = self._move_baselines.setdefault(key, RollingMoveBaseline())
        windows = ((legacy.get("execution") or {}).get("windows") or {})
        sample = windows.get("5m") or windows.get("15m") or {}
        move = sample.get("perp_price_change_pct")
        adaptive = None
        threshold = baseline.threshold()
        if sample.get("perp_flow_usable") and move is not None:
            adaptive = baseline.is_abnormal(move)

        event_engine = observe_crypto_events(legacy, adaptive_abnormal=adaptive)

        if sample.get("perp_flow_usable") and move is not None:
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

        timestamp = legacy.get("generated_at")
        try:
            if isinstance(timestamp, str) and ("T" in timestamp or "-" in timestamp):
                from datetime import datetime
                timestamp = datetime.fromisoformat(timestamp.replace("Z","+00:00")).timestamp()
            else:
                timestamp = float(timestamp)
                # Normalize Unix milliseconds to seconds.
                if abs(timestamp) > 100_000_000_000:
                    timestamp /= 1000.0
            timestamp = int(timestamp)
        except (TypeError, ValueError, OverflowError):
            timestamp = None
        if timestamp is not None:
            x25 = event_engine.get("x25_evidence") or {}
            event_engine["causal_memory"] = self._event_memory.update(
                key, timestamp, {
                    "abnormal_pump": bool(x25.get("abnormal_pump")),
                    "exhaustion": bool(x25.get("exhaustion")),
                    "failed_acceptance": bool(x25.get("failed_acceptance")),
                    "structure_break": bool(x25.get("structure_break")),
                    "bearish_displacement": bool(x25.get("bearish_displacement")),
                    "failed_retest": bool(x25.get("failed_retest")),
                })
        else:
            event_engine["causal_memory"] = {
                "sequence_complete": False,
                "causal_order_confirmed": None,
                "reason": "generated_at_unavailable",
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
