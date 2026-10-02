#!/usr/bin/env python3
"""Build the production historical seed without exchange REST.

CI runners may be geo-blocked by Bybit REST. We therefore use public WebSockets:
1) Bybit linear ticker snapshots to rank the current top-N USDT perpetuals by
   24h turnover.
2) TradingView's public chart websocket for BYBIT:<symbol>.P historical bars.
The runtime collector still never performs historical REST fetches.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import websocket

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
TV_WS = "wss://data.tradingview.com/socket.io/websocket"
DEFAULT_TOP_N = 30
DEFAULT_BARS = 500
INTERVALS = ("1", "5", "15", "60", "240", "D")
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,24}USDT$")


def _tv_frame(method, params):
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload.encode('utf-8'))}~m~{payload}"


def _tv_payloads(raw):
    out, pos = [], 0
    while True:
        start = raw.find("~m~", pos)
        if start < 0:
            break
        a = start + 3
        b = raw.find("~m~", a)
        if b < 0:
            break
        try:
            size = int(raw[a:b])
        except ValueError:
            pos = b + 3
            continue
        p0 = b + 3
        payload = raw[p0:p0 + size]
        if len(payload.encode("utf-8")) != size:
            break
        out.append(payload)
        pos = p0 + len(payload)
    return out


def discover_top_symbols(limit):
    """Use one Bybit linear WS to rank currently listed USDT perpetuals."""
    symbols = []
    # instruments-info is intentionally not used here; ticker stream tells us
    # which requested symbols are active, while the static universe supplies breadth.
    universe = (
        "BTCUSDT ETHUSDT SOLUSDT XRPUSDT DOGEUSDT ADAUSDT BNBUSDT LINKUSDT "
        "AVAXUSDT SUIUSDT ENAUSDT TAOUSDT AAVEUSDT LTCUSDT BCHUSDT NEARUSDT "
        "APTUSDT ARBUSDT OPUSDT WIFUSDT 1000PEPEUSDT 1000SHIBUSDT DOTUSDT "
        "UNIUSDT ATOMUSDT ETCUSDT FILUSDT TRXUSDT TONUSDT ICPUSDT INJUSDT "
        "SEIUSDT TIAUSDT JUPUSDT PYTHUSDT RENDERUSDT FETUSDT RUNEUSDT "
        "GALAUSDT SANDUSDT MANAUSDT CRVUSDT LDOUSDT MKRUSDT ONDOUSDT "
        "PENDLEUSDT STXUSDT IMXUSDT GRTUSDT ALGOUSDT HBARUSDT VETUSDT "
        "KASUSDT ZECUSDT XLMUSDT EOSUSDT FLOWUSDT DYDXUSDT SNXUSDT COMPUSDT "
        "SUSHIUSDT APEUSDT CHZUSDT MINAUSDT ORDIUSDT WLDUSDT ARKMUSDT "
        "STRKUSDT POLUSDT NOTUSDT JASMYUSDT BONKUSDT 1000BONKUSDT FLOKIUSDT "
        "1000FLOKIUSDT MEMEUSDT PEOPLEUSDT BLURUSDT GMXUSDT EIGENUSDT "
        "ETHFIUSDT WUSDT ZROUSDT HYPEUSDT TAOUSDT BERAUSDT GRASSUSDT"
    ).split()
    universe = list(dict.fromkeys(s for s in universe if SYMBOL_RE.fullmatch(s)))

    ws = websocket.create_connection(BYBIT_WS, timeout=10)
    ws.settimeout(1.0)
    try:
        topics = [f"tickers.{s}" for s in universe]
        for i in range(0, len(topics), 10):
            ws.send(json.dumps({"op": "subscribe", "args": topics[i:i + 10]}))
        deadline = time.time() + 8.0
        rows = {}
        while time.time() < deadline:
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not raw:
                continue
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            topic = str(msg.get("topic") or "")
            data = msg.get("data")
            if topic.startswith("tickers.") and isinstance(data, dict):
                symbol = topic.rsplit(".", 1)[-1].upper()
                try:
                    turnover = float(data.get("turnover24h") or 0.0)
                except (TypeError, ValueError):
                    continue
                if turnover > 0:
                    rows[symbol] = turnover
            if len(rows) >= len(universe) * 0.8:
                break
    finally:
        try:
            ws.close()
        except Exception:
            pass

    ranked = sorted(((turnover, symbol) for symbol, turnover in rows.items()), reverse=True)
    selected = [symbol for _, symbol in ranked[:max(1, int(limit))]]
    return selected, ranked


def fetch_tv_history(symbol, bars):
    """Fetch closed Bybit perpetual bars from TradingView in one websocket."""
    cs = "cs_" + uuid.uuid4().hex[:12]
    ws = websocket.create_connection(
        TV_WS,
        timeout=10,
        origin="https://data.tradingview.com",
        header=["User-Agent: Mozilla/5.0"],
    )
    ws.settimeout(0.75)
    rows = {tf: {} for tf in INTERVALS}
    series = {tf: f"sds_{tf}" for tf in INTERVALS}
    try:
        def send(method, params):
            ws.send(_tv_frame(method, params))

        send("set_auth_token", ["unauthorized_user_token"])
        send("chart_create_session", [cs, ""])
        send("resolve_symbol", [
            cs, "prescan_sym",
            "=" + json.dumps(
                {"symbol": f"BYBIT:{symbol}.P", "adjustment": "splits", "session": "regular"},
                separators=(",", ":"),
            ),
        ])
        for tf in INTERVALS:
            tv_tf = "D" if tf == "D" else tf
            send("create_series", [cs, series[tf], f"ser_{tf}", "prescan_sym", tv_tf, int(bars) + 5])

        deadline = time.monotonic() + 15.0
        completed = set()
        now_ms = int(time.time() * 1000)
        while time.monotonic() < deadline and len(completed) < len(INTERVALS):
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not raw:
                continue
            for payload in _tv_payloads(raw):
                if payload.startswith("~h~"):
                    ws.send(f"~m~{len(payload)}~m~{payload}")
                    continue
                try:
                    msg = json.loads(payload)
                except Exception:
                    continue
                method = msg.get("m")
                params = msg.get("p") or []
                if method in ("symbol_error", "series_error"):
                    raise RuntimeError(f"{method}:{params[-2:]}")
                if method in ("du", "timescale_update") and len(params) > 1 and isinstance(params[1], dict):
                    data = params[1]
                    for tf, sid in series.items():
                        node = data.get(sid)
                        if not isinstance(node, dict):
                            continue
                        items = node.get("s")
                        if not isinstance(items, list):
                            continue
                        parsed = {}
                        interval_ms = 86_400_000 if tf == "D" else int(tf) * 60_000
                        for item in items:
                            vals = item.get("v") if isinstance(item, dict) else None
                            if not isinstance(vals, list) or len(vals) < 5:
                                continue
                            try:
                                start = int(float(vals[0]) * 1000)
                                candle = {
                                    "start": start,
                                    "end": start + interval_ms - 1,
                                    "open": float(vals[1]),
                                    "high": float(vals[2]),
                                    "low": float(vals[3]),
                                    "close": float(vals[4]),
                                    "volume": float(vals[5]) if len(vals) > 5 and vals[5] is not None else 0.0,
                                    "turnover": 0.0,
                                    "confirm": True,
                                    "source": "tradingview_ws_bybit",
                                }
                            except (TypeError, ValueError, OverflowError):
                                continue
                            if candle["low"] <= candle["high"] and candle["end"] < now_ms:
                                parsed[start] = candle
                        if parsed:
                            rows[tf] = parsed
                if method == "series_completed" and len(params) >= 2:
                    completed.add(next((tf for tf, sid in series.items() if sid == str(params[1])), ""))
    finally:
        try:
            ws.close()
        except Exception:
            pass

    return {
        "version": 2,
        "symbol": symbol,
        "market": "linear",
        "saved_at": time.time(),
        "source": "tradingview_ws_bybit",
        "bars_requested": int(bars),
        "candles": {
            tf: [rows[tf][k] for k in sorted(rows[tf])[-int(bars):]]
            for tf in INTERVALS
        },
        "errors": {
            tf: f"insufficient_history:{len(rows[tf])}"
            for tf in INTERVALS
            if len(rows[tf]) < min(int(bars), 80)
        },
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
        "source": "bybit_ws_ticker+tradingview_ws_bybit_perpetual",
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
        return symbol, fetch_tv_history(symbol, args.bars)

    with ThreadPoolExecutor(max_workers=min(5, max(1, len(symbols)))) as pool:
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

    print(json.dumps({
        "status": "OK" if len(manifest["files"]) == len(symbols) else "PARTIAL",
        "symbols": symbols,
        "generated_files": len(manifest["files"]),
        "errors": manifest["errors"],
    }, ensure_ascii=False))
    if not manifest["files"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
