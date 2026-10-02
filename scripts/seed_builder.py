#!/usr/bin/env python3
"""Build an offline Scan+ historical candle seed from Bybit public REST.

This script is deliberately separate from the production WebSocket collector.
Production never calls Bybit REST for candle history. Run this builder offline,
then place the generated JSON into CANDLE_STORE_DIR before starting the service.

Example:
    python scripts/seed_builder.py --symbol BTCUSDT --category linear \
        --output /tmp/seed/BTCUSDT_linear.json --bars 500
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from typing import Any

BYBIT_BASE = os.environ.get("BYBIT_PUBLIC_API", "https://api.bybit.com")
ENDPOINT = "/v5/market/kline"
INTERVALS = {
    "1": "1m",
    "5": "5m",
    "15": "15m",
    "60": "1h",
    "240": "4h",
    "D": "1d",
}
DEFAULT_BARS = 500
PAGE_LIMIT = 1000


def _request(params: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{BYBIT_BASE}{ENDPOINT}?{query}",
        headers={"User-Agent": "scalp-market-bridge-seed-builder/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}"
        )
    return payload


def fetch_candles(symbol: str, category: str, interval: str, bars: int) -> list[dict[str, Any]]:
    """Fetch the newest closed bars, paging backwards until the requested count."""
    target = min(max(int(bars), 1), 5000)
    rows: dict[int, dict[str, Any]] = {}
    end_ms = int(time.time() * 1000)
    duration_ms = (
        86_400_000 if interval == "D" else int(interval) * 60_000
    )

    while len(rows) < target:
        payload = _request({
            "category": category,
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": PAGE_LIMIT,
            "end": end_ms,
        })
        result = payload.get("result") or {}
        batch = result.get("list") or []
        if not batch:
            break

        oldest = None
        now_ms = int(time.time() * 1000)
        for item in batch:
            if len(item) < 7:
                continue
            try:
                start = int(item[0])
                open_, high, low, close = map(float, item[1:5])
                volume = float(item[5])
                turnover = float(item[6])
            except (TypeError, ValueError):
                continue
            close_end = start + duration_ms
            # Exclude the currently forming candle.
            if close_end > now_ms:
                continue
            if low > high or high < max(open_, close) or low > min(open_, close):
                continue
            rows[start] = {
                "start": start,
                "end": close_end - 1,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "turnover": turnover,
                "confirm": True,
                "source": "bybit_seed",
            }
            oldest = start if oldest is None else min(oldest, start)

        if oldest is None:
            break
        next_end = oldest - 1
        if next_end >= end_ms:
            break
        end_ms = next_end

        # Bybit returns newest-first. A full page normally means we can continue.
        if len(batch) < PAGE_LIMIT:
            break

    return [rows[k] for k in sorted(rows)[-target:]]


def build_seed(symbol: str, category: str, bars: int) -> dict[str, Any]:
    candles = {}
    errors = {}
    for interval in INTERVALS:
        try:
            candles[interval] = fetch_candles(symbol, category, interval, bars)
        except Exception as exc:
            candles[interval] = []
            errors[interval] = f"{type(exc).__name__}: {exc}"

    return {
        "version": 2,
        "symbol": symbol.upper(),
        "market": category,
        "saved_at": time.time(),
        "source": "bybit_historical_market_data",
        "bars_requested": int(bars),
        "intervals": INTERVALS,
        "errors": errors,
        "candles": candles,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--category", default="linear", choices=("linear", "spot"))
    parser.add_argument("--bars", type=int, default=DEFAULT_BARS)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    payload = build_seed(args.symbol, args.category, args.bars)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    tmp = args.output + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    os.replace(tmp, args.output)

    counts = {tf: len(rows) for tf, rows in payload["candles"].items()}
    print(json.dumps({
        "status": "OK" if not payload["errors"] else "PARTIAL",
        "symbol": payload["symbol"],
        "category": payload["market"],
        "counts": counts,
        "errors": payload["errors"],
        "output": args.output,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
