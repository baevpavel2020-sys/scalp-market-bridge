"""Cross-market decision normalization.

This layer ranks attention, not trade quality. It never invents direction and never
lets a market-specific candidate override another market's evidence contract.
"""
from typing import Any, Mapping


STATUS_ORDER = {"CANDIDATE": 3, "SETUP": 3, "WATCH": 1, "NO_TRADE": 0, "DATA_ERROR": -1}
MARKET_ORDER = {"crypto": 0, "stocks": 1, "forex": 2, "commodities": 3}


def _candidate(result: Mapping[str, Any]) -> Mapping[str, Any]:
    value = result.get("candidate")
    if isinstance(value, Mapping):
        return value
    # Crypto remains legacy-authoritative: adapt its existing decision fields
    # without changing the underlying Crypto strategy.
    status = result.get("trade_state") or result.get("setup_state")
    if status in ("READY", "SETUP", "CANDIDATE"):
        normalized = "CANDIDATE" if status != "SETUP" else "SETUP"
    elif status in ("BLOCKED", "NO_TRADE"):
        normalized = "NO_TRADE"
    else:
        normalized = "WATCH"
    return {
        "status": normalized,
        "direction": result.get("direction") or "unknown",
        "situation": result.get("trade_style") or result.get("context") or "continuation",
        "reason": result.get("block_reasons") or [],
        "legacy": True,
    }


def normalize_result(result: Mapping[str, Any]) -> Mapping[str, Any]:
    market = str(result.get("market") or "").lower()
    symbol = str(result.get("symbol") or "").upper()
    if not symbol:
        raise ValueError("missing symbol provenance")
    if market not in MARKET_ORDER:
        raise ValueError(f"unknown market provenance: {market or 'missing'}")
    candidate = _candidate(result)
    status = str(candidate.get("status") or "WATCH").upper()
    if status not in STATUS_ORDER:
        raise ValueError(f"unknown candidate status: {status}")
    if status == "DATA_ERROR":
        raise ValueError("candidate cannot enter normalized results as DATA_ERROR")
    mtf = result.get("mtf_state") or candidate.get("mtf_state")
    mtf_gate_blocked=False
    if status in ("CANDIDATE","SETUP") and isinstance(mtf, Mapping):
        regime=str(mtf.get("regime") or "")
        if regime in ("countertrend_correction","context_only","unresolved") and not mtf.get("reversal_confirmed"):
            status="WATCH"
            mtf_gate_blocked=True

    priority = result.get("priority") or {}
    weights = priority.get("weights") if isinstance(priority, Mapping) else {}
    ordered = priority.get("ordered") if isinstance(priority, Mapping) else []
    top_weight = max((float(v) for v in (weights or {}).values()), default=0.0)

    # Attention rank only separates state classes. It must not turn the number of
    # enabled blocks or a market-specific weight into a cross-market quality score.
    attention_rank = (
        STATUS_ORDER[status],
        -MARKET_ORDER.get(market, 99),
        str(result.get("symbol") or candidate.get("symbol") or "").upper(),
    )

    mtf = result.get("mtf_state") or candidate.get("mtf_state")
    return {
        "market": market,
        "symbol": symbol or str(candidate.get("symbol") or "").upper(),
        "status": status,
        "direction": str(candidate.get("direction") or "unknown").lower(),
        "situation": candidate.get("situation"),
        "reason": candidate.get("reason"),
        "mtf_gate_blocked": mtf_gate_blocked,
        "mtf_state": mtf,
        "priority": {
            "top_weight": top_weight,
            "focus": list(ordered[:3] if isinstance(ordered, list) else []),
            "ordered": list(ordered or []),
        },
        "attention_rank": attention_rank,
        "source": {
            "candidate": dict(candidate),
            "engine_version": result.get("engine_version"),
        },
    }


def aggregate_results(results):
    normalized = []
    errors = []
    for item in results or []:
        if not isinstance(item, Mapping):
            errors.append({"status": "DATA_ERROR", "reason": "non_mapping_result"})
            continue
        if item.get("status") in ("DATA_ERROR","NO_UNIVERSE"):
            error=dict(item)
            if error.get("status")=="NO_UNIVERSE":
                error["status"]="DATA_ERROR"
                error["error"]=error.get("reason","market_native_universe_unavailable")
            errors.append(error)
            continue
        payload = item.get("result") if isinstance(item.get("result"), Mapping) else item
        try:
            wrapper_market=str(item.get("market") or "").lower()
            payload_market=str(payload.get("market") or "").lower()
            if wrapper_market and payload_market and wrapper_market != payload_market:
                raise ValueError(
                    f"market provenance mismatch: wrapper={wrapper_market}, payload={payload_market}"
                )
            normalized.append(normalize_result(payload))
        except (TypeError, ValueError, AttributeError) as exc:
            errors.append({
                "status": "DATA_ERROR",
                "market": str(item.get("market") or "").lower(),
                "error": f"{type(exc).__name__}: {exc}",
            })

    ordered = sorted(
        normalized,
        key=lambda x: x["attention_rank"],
        reverse=True,
    )
    return {
        "count": len(ordered),
        "candidate_count": sum(x["status"] in ("CANDIDATE", "SETUP") for x in ordered),
        "watch_count": sum(x["status"] == "WATCH" for x in ordered),
        "error_count": len(errors),
        "results": ordered,
        "errors": errors,
        "policy": {
            "market_specific_evidence_preserved": True,
            "no_universal_trade_score": True,
            "no_cross_market_direction_vote": True,
            "attention_rank_is_not_probability": True,
        },
    }
