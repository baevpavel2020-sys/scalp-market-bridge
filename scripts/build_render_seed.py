#!/usr/bin/env python3
"""Build the production historical seed from Bybit linear perpetuals.

The runtime collector does NOT call REST for history. This script is a build/CI
step: discover the current top-N USDT perpetuals by 24h turnover, then create
offline seed files consumed by dynamic_collector.py.
"""
from __future__ import annotations

import argparse
import sys
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.seed_builder import fetch_candles

BYBIT_BASE = os.environ.get("BYBIT_PUBLIC_API", "https://api.bybit.com")
TICKERS_ENDPOINT = "/v5/market/tickers"
DEFAULT_TOP_N = 30
DEFAULT_BARS = 500
INTERVALS = ("1", "5", "15", "60", "240", "D")


def _request_tickers():
    query = urllib.parse.urlencode({"category": "linear"})
    req = urllib.request.Request(
        f"{BYBIT_BASE}{TICKERS_ENDPOINT}?{query}",
        headers={"User-Agent": "scalp-market-bridge-render-seed/1.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}"
        )
    return payload.get("result", {}).get("list", [])


def discover_top_symbols(limit):
    rows = _request_tickers()
    ranked = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        if row.get("status") not in (None, "Trading"):
            continue
        try:
            turnover = float(row.get("turnover24h") or 0.0)
        except (TypeError, ValueError):
            continue
        if turnover <= 0:
            continue
        ranked.append((turnover, symbol))
    ranked.sort(reverse=True)
    return [symbol for _, symbol in ranked[:max(1, int(limit))]], ranked


def build_one(symbol, bars):
    candles = {}
    errors = {}
    for interval in INTERVALS:
        try:
            candles[interval] = fetch_candles(symbol, "linear", interval, bars)
        except Exception as exc:
            candles[interval] = []
            errors[interval] = f"{type(exc).__name__}: {exc}"
    return {
        "version": 2,
        "symbol": symbol,
        "market": "linear",
        "saved_at": time.time(),
        "source": "bybit_historical_market_data",
        "bars_requested": int(bars),
        "candles": candles,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--bars", type=int, default=DEFAULT_BARS)
    parser.add_argument("--output-dir", default="seed_data")
    args = parser.parse_args()

    symbols, ranked = discover_top_symbols(args.top_n)
    os.makedirs(args.output_dir, exist_ok=True)

    manifest = {
        "version": 1,
        "generated_at": time.time(),
        "source": "bybit_linear_tickers_turnover24h",
        "top_n": len(symbols),
        "symbols": symbols,
        "ranked": [
            {"rank": i + 1, "symbol": symbol, "turnover24h": turnover}
            for i, (turnover, symbol) in enumerate(ranked[:args.top_n])
        ],
        "bars": args.bars,
        "intervals": list(INTERVALS),
        "files": [],
        "errors": {},
    }

    def job(symbol):
        return symbol, build_one(symbol, args.bars)

    workers = min(6, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(job, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                symbol, payload = future.result()
                filename = f"{symbol}_linear_seed.json"
                tmp = os.path.join(args.output_dir, filename + ".tmp")
                final = os.path.join(args.output_dir, filename)
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, separators=(",", ":"))
                os.replace(tmp, final)
                manifest["files"].append(filename)
                if payload["errors"]:
                    manifest["errors"][symbol] = payload["errors"]
            except Exception as exc:
                manifest["errors"][symbol] = f"{type(exc).__name__}: {exc}"

    manifest["files"].sort()
    with open(os.path.join(args.output_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    complete = sum(1 for name in manifest["files"])
    print(json.dumps({
        "status": "OK" if complete == len(symbols) else "PARTIAL",
        "symbols": symbols,
        "generated_files": complete,
        "errors": manifest["errors"],
    }, ensure_ascii=False))

    if complete == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
