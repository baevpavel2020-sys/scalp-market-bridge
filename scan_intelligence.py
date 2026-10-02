"""Scan+ Intelligence Layer.

Market-agnostic services downstream of market-specific data/analysis.
This layer classifies, ranks, remembers and measures; it never fabricates
market-specific facts or rewrites upstream direction.
"""
import hashlib
import json
import math
import os
import threading
import time

INTELLIGENCE_VERSION = "intelligence_v2"
WATCHLIST_TTL = 6 * 3600
OUTCOME_PATH = os.environ.get("SCAN_OUTCOME_PATH", "/tmp/scalp-market-bridge/outcomes.jsonl")
PERFORMANCE_BARS = {"5m": 2, "15m": 2, "1h": 2, "4h": 2}


def _num(x):
    try:
        value = float(x)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def classify_regime_from_analysis(timeframes):
    states, directions = [], []
    for analysis in (timeframes or {}).values():
        levels = analysis.get("levels") or {}
        if levels.get("volatility_regime"):
            states.append(str(levels["volatility_regime"]).upper())
        direction = (analysis.get("confluence") or {}).get("direction")
        if direction in ("bullish", "bearish"):
            directions.append(direction)
    if "EXPANSION" in states:
        state = "EXPANSION"
    elif "COMPRESSION" in states:
        state = "COMPRESSION"
    elif directions and len(directions) >= 2 and len(set(directions)) == 1:
        state = "TREND"
    elif directions:
        state = "TRANSITION"
    else:
        state = "UNKNOWN"
    return {
        "state": state,
        "confidence": round(min(1.0, 0.45 + 0.1 * min(len(directions), 5)), 3),
        "source": "analysis_bundles",
    }


def _closed_rows(rows):
    return [
        row for row in rows or []
        if row.get("confirm", True) is not False
    ]


def classify_regime(rows):
    rows = [
        row for row in _closed_rows(rows)
        if all(_num(row.get(key)) is not None for key in ("open", "high", "low", "close"))
    ]
    if len(rows) < 30:
        return {"state": "UNKNOWN", "confidence": 0.0, "reason": "insufficient_history"}
    closes = [_num(row["close"]) for row in rows[-30:]]
    ranges = [max(0.0, _num(row["high"]) - _num(row["low"])) for row in rows[-30:]]
    atr = sum(ranges) / len(ranges)
    mean = sum(closes) / len(closes)
    if not atr or not mean:
        return {"state": "UNKNOWN", "confidence": 0.0, "reason": "invalid_volatility"}
    slope = (closes[-1] - closes[0]) / max(1, len(closes) - 1)
    slope_atr = abs(slope * 10) / atr
    recent = sum(ranges[-5:]) / 5
    baseline = sum(ranges[:-5]) / max(1, len(ranges) - 5)
    expansion = recent / max(baseline, 1e-12)
    if expansion >= 1.6:
        state = "EXPANSION"
    elif expansion <= 0.65:
        state = "COMPRESSION"
    elif slope_atr >= 1.2:
        state = "TREND"
    else:
        state = "RANGE"
    confidence = min(
        1.0,
        0.45 + min(0.5, abs(slope_atr) / 3) + min(0.25, abs(expansion - 1) * 0.5),
    )
    return {
        "state": state,
        "confidence": round(confidence, 3),
        "slope_atr": round(slope_atr, 3),
        "range_expansion": round(expansion, 3),
        "atr": round(atr, 10),
        "atr_pct": round(atr / mean * 100, 4),
    }


def mtf_state_matrix(timeframes):
    out = {}
    for tf, analysis in (timeframes or {}).items():
        structure = analysis.get("structure") or {}
        confluence = analysis.get("confluence") or {}
        out[tf] = {
            "ready": bool(analysis.get("ready")),
            "structure": structure.get("state"),
            "direction": confluence.get("direction"),
            "phase": structure.get("phase"),
            "event": (
                (structure.get("event") or {}).get("type")
                if isinstance(structure.get("event"), dict)
                else structure.get("event")
            ),
            "volatility": (analysis.get("levels") or {}).get("volatility_regime"),
            "signal_freshness": analysis.get("signal_freshness"),
        }
    return out


def performance_snapshot(frames, lookbacks=("5m", "15m", "1h", "4h")):
    """Return the latest completed-bar return for each timeframe."""
    out = {}
    aliases = {"5m": "5", "15m": "15", "1h": "60", "4h": "240"}
    for tf in lookbacks:
        rows = _closed_rows(frames.get(tf) or frames.get(aliases.get(tf, "")) or [])
        if len(rows) < PERFORMANCE_BARS.get(tf, 2):
            continue
        bars = PERFORMANCE_BARS.get(tf, 2)
        first = _num(rows[-bars].get("close"))
        last = _num(rows[-1].get("close"))
        if first and last:
            out[tf] = {
                "change_pct": round((last / first - 1) * 100, 4),
                "price": last,
                "bars": bars,
                "closed_only": True,
            }
    return out


def relative_strength(results, market_key=None, lookback="15m"):
    values = []
    for result in results or []:
        row = (result.get("performance") or {}).get(lookback) or {}
        change = _num(row.get("change_pct"))
        symbol = result.get("symbol")
        if change is not None and symbol:
            values.append((str(symbol), change))
    values.sort(key=lambda item: (-item[1], item[0]))
    if not values:
        return []
    low = min(value for _, value in values)
    high = max(value for _, value in values)
    span = max(high - low, 1e-9)
    return [
        {
            "symbol": symbol,
            "change_pct": round(change, 4),
            "relative_strength": round((change - low) / span, 3),
            "rank": rank,
            "universe": market_key,
        }
        for rank, (symbol, change) in enumerate(values, start=1)
    ]


def _event_fingerprint(setup):
    events = setup.get("event_basis") or []
    normalized = []
    for event in events:
        if not isinstance(event, dict):
            continue
        normalized.append({
            "event": event.get("event") or event.get("type"),
            "direction": event.get("direction"),
            "level": event.get("level") or event.get("price"),
            "timeframe": event.get("timeframe"),
            "confirmed": event.get("confirmed") or event.get("confirmed_by_close"),
        })
    limit_plan = setup.get("limit_plan") or {}
    targets = setup.get("targets")
    target = (
        limit_plan.get("take_profit") or limit_plan.get("target")
        or (targets.get("t1") if isinstance(targets, dict) else None)
    )
    payload = {
        "state": setup.get("opportunity_state"),
        "direction": setup.get("direction") or setup.get("side"),
        "entry": limit_plan.get("entry") or setup.get("entry"),
        "stop": limit_plan.get("stop") or setup.get("stop"),
        "target": target,
        "rr": setup.get("risk_reward") or limit_plan.get("risk_reward") or limit_plan.get("rr"),
        "events": normalized,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def alert_payload(scan):
    setup = scan.get("setup") or {}
    state = setup.get("opportunity_state")
    if state not in ("MARKET_READY", "LIMIT_READY"):
        return {"eligible": False, "state": state or "WATCH"}
    limit_plan = setup.get("limit_plan") or {}
    targets = setup.get("targets")
    if isinstance(targets, dict):
        target = targets.get("t1") or targets.get("target")
    elif isinstance(targets, list) and targets:
        target = targets[0]
    else:
        target = limit_plan.get("take_profit") or limit_plan.get("target")
    return {
        "eligible": True,
        "state": state,
        "symbol": scan.get("symbol"),
        "direction": setup.get("side") or setup.get("direction") or scan.get("direction"),
        "entry": setup.get("entry") or limit_plan.get("entry"),
        "stop": setup.get("stop") or limit_plan.get("stop"),
        "target": target,
        "rr": setup.get("risk_reward") or limit_plan.get("risk_reward") or limit_plan.get("rr"),
        "reason": "setup_state_changed",
    }


class WatchlistStore:
    def __init__(self, ttl=WATCHLIST_TTL):
        self.ttl = ttl
        self.lock = threading.RLock()
        self.items = {}

    @staticmethod
    def _key(scan):
        market = str(scan.get("market") or "crypto").lower()
        symbol = str(scan.get("symbol") or "").upper()
        return f"{market}:{symbol}" if symbol else ""

    def upsert(self, scan):
        key = self._key(scan)
        if not key:
            return None
        setup = scan.get("setup") or {}
        state = setup.get("opportunity_state") or "WATCH"
        fingerprint = _event_fingerprint(setup)
        with self.lock:
            previous = self.items.get(key)
            changed = (
                previous is None
                or previous.get("state") != state
                or previous.get("fingerprint") != fingerprint
            )
            item = {
                "key": key,
                "symbol": str(scan.get("symbol") or "").upper(),
                "market": str(scan.get("market") or "crypto").lower(),
                "state": state,
                "direction": setup.get("direction") or setup.get("side") or scan.get("direction"),
                "rr": setup.get("risk_reward")
                or (setup.get("limit_plan") or {}).get("risk_reward")
                or (setup.get("limit_plan") or {}).get("rr"),
                "fingerprint": fingerprint,
                "updated_at": time.time(),
                "state_changed": changed,
            }
            self.items[key] = item
            return dict(item)

    def snapshot(self, market=None):
        now = time.time()
        market = str(market).lower() if market else None
        with self.lock:
            for key, item in list(self.items.items()):
                if now - item["updated_at"] > self.ttl:
                    self.items.pop(key, None)
            values = list(self.items.values())
        if market:
            values = [item for item in values if item.get("market") == market]
        return sorted(
            values,
            key=lambda item: (
                item["state"] != "MARKET_READY",
                item["state"] != "LIMIT_READY",
                -float(item.get("rr") or 0),
                item["symbol"],
            ),
        )


class OutcomeLogger:
    def __init__(self, path=OUTCOME_PATH):
        self.path = path
        self.lock = threading.Lock()
        self._seen = set()

    def record(self, scan):
        setup = scan.get("setup") or {}
        state = setup.get("opportunity_state")
        if state not in ("MARKET_READY", "LIMIT_READY"):
            return False
        event = {
            "ts": time.time(),
            "symbol": scan.get("symbol"),
            "market": scan.get("market", "crypto"),
            "state": state,
            "direction": setup.get("side") or setup.get("direction") or scan.get("direction"),
            "entry": setup.get("entry") or (setup.get("limit_plan") or {}).get("entry"),
            "stop": setup.get("stop") or (setup.get("limit_plan") or {}).get("stop"),
            "target": (setup.get("limit_plan") or {}).get("take_profit") or ((setup.get("targets") or {}).get("t1") if isinstance(setup.get("targets"), dict) else None),
            "rr": setup.get("risk_reward")
            or (setup.get("limit_plan") or {}).get("risk_reward")
            or (setup.get("limit_plan") or {}).get("rr"),
            "regime": scan.get("regime"),
            "event_basis": setup.get("event_basis") or [],
            "features": scan.get("mtf_matrix") or {},
        }
        fingerprint = _event_fingerprint({
            **setup,
            "market": scan.get("market", "crypto"),
            "symbol": scan.get("symbol"),
        })
        event["fingerprint"] = fingerprint
        with self.lock:
            if fingerprint in self._seen:
                return False
            self._seen.add(fingerprint)
            try:
                directory = os.path.dirname(self.path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(event, separators=(",", ":"), default=str) + "\n")
                return True
            except OSError:
                self._seen.discard(fingerprint)
                return False


WATCHLIST = WatchlistStore()
OUTCOMES = OutcomeLogger()


def enrich_external_result(result, market):
    frames = result.get("frames") or {}
    analysis = result.get("analysis_core", {}).get("analysis") or {}
    tf = {}
    for tf_name, analysis_item in analysis.items():
        structure = analysis_item.get("structure") or {}
        tf[tf_name] = {
            "ready": analysis_item.get("ready"),
            "structure": {
                "state": structure.get("state"),
                "phase": structure.get("phase"),
                "event": structure.get("last_event"),
            },
            "confluence": analysis_item.get("confluence") or {},
            "levels": analysis_item.get("regime_levels") or {},
        }
    setup_plans = result.get("setup_plans") or {}
    setup = setup_plans.get("15m") or setup_plans.get("1h") or next(iter(setup_plans.values()), {})
    opportunity_state = (
        "MARKET_READY"
        if setup.get("tradeable") and setup.get("execution_ready")
        else "LIMIT_READY"
        if setup.get("tradeable")
        else "WATCH"
    )
    out = dict(result)
    out["regime"] = {tf_name: classify_regime(rows) for tf_name, rows in frames.items()}
    out["mtf_matrix"] = mtf_state_matrix(tf)
    out["performance"] = performance_snapshot(frames)
    out["setup"] = {**setup, "opportunity_state": opportunity_state}
    out["opportunity_state"] = opportunity_state
    out["intelligence_version"] = INTELLIGENCE_VERSION
    watch = WATCHLIST.upsert(out)
    out["watchlist"] = watch
    out["alert"] = alert_payload(out)
    if watch and not watch.get("state_changed"):
        out["alert"]["eligible"] = False
        out["alert"]["reason"] = "no_new_setup_state_or_event"
    if watch and watch.get("state_changed"):
        OUTCOMES.record(out)
    return out


def enrich_scan(scan, market="crypto"):
    out = dict(scan or {})
    frames = scan.get("_frames") or {}
    out.pop("_frames", None)
    if frames:
        out["regime"] = {
            tf: classify_regime(rows)
            for tf, rows in frames.items()
            if tf in ("1D", "D", "4h", "240", "1h", "60", "15m", "15", "5m", "5")
        }
    if not out.get("regime"):
        out["regime"] = {"composite": classify_regime_from_analysis(out.get("timeframes") or {})}
    out["mtf_matrix"] = mtf_state_matrix(out.get("timeframes") or {})
    out["performance"] = performance_snapshot(frames)
    out["intelligence_version"] = INTELLIGENCE_VERSION
    out["setup"] = dict(out.get("setup") or {})
    out["opportunity_state"] = out["setup"].get("opportunity_state") or "WATCH"
    out["setup"]["opportunity_state"] = out["opportunity_state"]
    watch = WATCHLIST.upsert(out)
    out["watchlist"] = watch
    out["alert"] = alert_payload(out)
    if watch and not watch.get("state_changed"):
        out["alert"]["eligible"] = False
        out["alert"]["reason"] = "no_new_setup_state_or_event"
    if watch and watch.get("state_changed"):
        OUTCOMES.record(out)
    return out


def _is_filled(direction, candle, entry):
    high, low = _num(candle.get("high")), _num(candle.get("low"))
    if high is None or low is None:
        return False
    return low <= entry if direction == "bullish" else high >= entry


def _resolve_after_fill(direction, candle, stop, target):
    high, low = _num(candle.get("high")), _num(candle.get("low"))
    if high is None or low is None:
        return None
    stop_hit = low <= stop if direction == "bullish" else high >= stop
    target_hit = high >= target if direction == "bullish" else low <= target
    if stop_hit and target_hit:
        return "SL"
    if stop_hit:
        return "SL"
    if target_hit:
        return "TP"
    return None


def backtest_event_setups(market, symbol, rows, detect_fn, plan_fn, max_checkpoints=100):
    """No-lookahead backtest with explicit limit-fill simulation."""
    rows = _closed_rows(rows)
    results = []
    if len(rows) < 31:
        return {
            "version": INTELLIGENCE_VERSION, "market": market, "symbol": symbol,
            "samples": 0, "tp": 0, "sl": 0, "unresolved": 0, "not_filled": 0,
            "hit_rate": 0.0, "results": [],
        }
    start = max(30, len(rows) - 1 - max_checkpoints)
    for checkpoint in range(start, len(rows) - 1):
        prefix = rows[: checkpoint + 1]
        detected = detect_fn(market, symbol, prefix)
        plan = plan_fn(market, symbol, prefix, detected)
        if not plan.get("tradeable"):
            continue
        limit_plan = plan.get("limit_plan") or {}
        entry, stop, target = (
            _num(limit_plan.get("entry")),
            _num(limit_plan.get("stop")),
            _num(limit_plan.get("take_profit")),
        )
        direction = str(plan.get("direction") or "").lower()
        if None in (entry, stop, target) or direction not in ("bullish", "bearish"):
            continue
        outcome, filled_at, resolved_at = "UNRESOLVED", None, None
        for index in range(checkpoint + 1, len(rows)):
            candle = rows[index]
            if filled_at is None:
                if _is_filled(direction, candle, entry):
                    filled_at = index
                else:
                    continue
            resolution = _resolve_after_fill(direction, candle, stop, target)
            if resolution:
                outcome, resolved_at = resolution, index
                break
        if filled_at is None:
            outcome = "NOT_FILLED"
        results.append({
            "checkpoint": rows[checkpoint].get("end", rows[checkpoint].get("start", checkpoint)),
            "entry": entry, "stop": stop, "target": target,
            "rr": limit_plan.get("rr") or limit_plan.get("risk_reward"),
            "outcome": outcome,
            "filled_at_bar": None if filled_at is None else filled_at - checkpoint,
            "bars_to_resolution": None if resolved_at is None else resolved_at - checkpoint,
        })
    resolved = [item for item in results if item["outcome"] in ("TP", "SL")]
    tp = sum(item["outcome"] == "TP" for item in resolved)
    sl = sum(item["outcome"] == "SL" for item in resolved)
    return {
        "version": INTELLIGENCE_VERSION, "market": market, "symbol": symbol,
        "samples": len(results), "tp": tp, "sl": sl,
        "unresolved": sum(item["outcome"] == "UNRESOLVED" for item in results),
        "not_filled": sum(item["outcome"] == "NOT_FILLED" for item in results),
        "hit_rate": round(tp / max(1, tp + sl), 4), "results": results,
    }


def edge_summary(records):
    resolved = [r for r in records or [] if r.get("outcome") in ("TP", "SL")]
    if not resolved:
        return {"samples": 0, "tp": 0, "sl": 0, "hit_rate": 0.0, "groups": []}
    groups = {}
    for record in resolved:
        regime = record.get("regime")
        regime_state = regime.get("state") if isinstance(regime, dict) else "unknown"
        key = (record.get("market", "unknown"), record.get("direction", "unknown"), regime_state)
        bucket = groups.setdefault(key, {"samples": 0, "tp": 0, "sl": 0})
        bucket["samples"] += 1
        bucket["tp"] += record.get("outcome") == "TP"
        bucket["sl"] += record.get("outcome") == "SL"
    for bucket in groups.values():
        bucket["hit_rate"] = round(bucket["tp"] / max(1, bucket["tp"] + bucket["sl"]), 4)
    return {
        "samples": len(resolved),
        "tp": sum(r.get("outcome") == "TP" for r in resolved),
        "sl": sum(r.get("outcome") == "SL" for r in resolved),
        "hit_rate": round(sum(r.get("outcome") == "TP" for r in resolved) / len(resolved), 4),
        "groups": [
            {"market": key[0], "direction": key[1], "regime": key[2], **value}
            for key, value in sorted(groups.items())
        ],
    }


def load_outcome_records(path=OUTCOME_PATH):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    except (OSError, json.JSONDecodeError):
        return []


def edge_discovery(path=OUTCOME_PATH):
    return {
        "version": INTELLIGENCE_VERSION,
        "source": path,
        "summary": edge_summary(load_outcome_records(path)),
    }
