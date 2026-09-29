import os
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

from collector import collector
from spot_collector import spot_collector
from dynamic_collector import dynamic_manager

app = Flask(__name__)

collector.start()
spot_collector.start()

BYBIT_URLS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]

@app.get("/")
def home():
    return jsonify({"service": "scalp-market-bridge", "status": "online"})

@app.get("/test-bybit")
def test_bybit():
    results = {}
    for base in BYBIT_URLS:
        try:
            r = requests.get(f"{base}/v5/market/time", timeout=8)
            results[base] = {"status": r.status_code, "text": r.text[:500]}
        except Exception as exc:
            results[base] = {"error": str(exc)}
    return jsonify(results)

@app.get("/market/BTCUSDT")
def market_btcusdt():
    return jsonify(collector.get_snapshot())

@app.get("/market/BTCUSDT/spot")
def market_btcusdt_spot():
    return jsonify(spot_collector.get_snapshot())

@app.get("/market/<symbol>")
def market_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.snapshot(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

@app.get("/diagnostics/<symbol>")
def diagnostics_dynamic(symbol):
    try:
        return jsonify(dynamic_manager.diagnostics(symbol))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

def _history_url(symbol, day):
    symbol = symbol.upper().strip()
    return f"https://public.bybit.com/trading/{symbol}/{symbol}{day}.csv.gz"

def _probe_history(symbol, max_days=14):
    symbol = symbol.upper().strip()

    if not symbol.isalnum() or len(symbol) > 30:
        return {
            "symbol": symbol,
            "archive_access": False,
            "error": "invalid symbol",
        }

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 scalp-market-bridge/1.0",
        "Accept": "*/*",
    })

    # Archive publication can lag behind realtime, so walk backwards.
    today = datetime.now(timezone.utc).date()
    attempts = []

    for offset in range(1, max_days + 1):
        day = (today - timedelta(days=offset)).isoformat()
        url = _history_url(symbol, day)

        try:
            # Stream GET is more reliable than HEAD on archive/CDN endpoints.
            with session.get(url, stream=True, timeout=(8, 15), allow_redirects=True) as r:
                attempts.append({"date": day, "status": r.status_code})

                if r.status_code == 200:
                    first_bytes = b""
                    for chunk in r.iter_content(chunk_size=128):
                        if chunk:
                            first_bytes = chunk[:32]
                            break

                    return {
                        "symbol": symbol,
                        "archive_access": True,
                        "file_found": True,
                        "date": day,
                        "http_status": r.status_code,
                        "url": url,
                        "content_type": r.headers.get("Content-Type"),
                        "content_length": r.headers.get("Content-Length"),
                        "content_encoding": r.headers.get("Content-Encoding"),
                        "gzip_magic_ok": first_bytes[:2] == b"\x1f\x8b",
                        "first_bytes_hex": first_bytes[:16].hex(),
                        "attempts": attempts,
                    }

                # 403 is especially important: Render can reach the host,
                # but Bybit/CDN is refusing the request.
                if r.status_code == 403:
                    return {
                        "symbol": symbol,
                        "archive_access": False,
                        "file_found": False,
                        "http_status": 403,
                        "url": url,
                        "error": "archive reachable but request forbidden",
                        "attempts": attempts,
                    }

        except requests.RequestException as exc:
            return {
                "symbol": symbol,
                "archive_access": False,
                "file_found": False,
                "error": f"{type(exc).__name__}: {exc}",
                "attempts": attempts,
            }

    return {
        "symbol": symbol,
        "archive_access": True,
        "file_found": False,
        "error": f"no daily archive found in previous {max_days} days",
        "attempts": attempts,
    }

@app.get("/test-history/<symbol>")
def test_history(symbol):
    return jsonify(_probe_history(symbol))

@app.get("/test-history")
def test_history_batch():
    symbols = ("ENAUSDT", "BTCUSDT", "ETHUSDT")
    results = {symbol: _probe_history(symbol) for symbol in symbols}

    return jsonify({
        "test": "Bybit public historical trades",
        "all_pass": all(
            item.get("archive_access") and item.get("file_found")
            for item in results.values()
        ),
        "results": results,
    })


KLINE_HOSTS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
    "https://api.bybit.kz",
    "https://api.bybitgeorgia.ge",
    "https://api.bybit.ae",
    "https://api.bybit.id",
    "https://api.spark-fintech.com",
]

def _probe_kline_host(base, symbol, interval="D", category="linear", limit=500):
    url = f"{base}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol.upper().strip(),
        "interval": interval,
        "limit": limit,
    }
    try:
        r = requests.get(
            url,
            params=params,
            timeout=(8, 15),
            headers={
                "User-Agent": "Mozilla/5.0 scalp-market-bridge/1.0",
                "Accept": "application/json",
            },
        )
        out = {
            "host": base,
            "http_status": r.status_code,
            "final_url": r.url,
        }
        if r.status_code != 200:
            out["pass"] = False
            out["body"] = r.text[:250]
            return out

        try:
            data = r.json()
        except Exception:
            out["pass"] = False
            out["error"] = "HTTP 200 but response is not JSON"
            out["body"] = r.text[:250]
            return out

        rows = (((data or {}).get("result") or {}).get("list") or [])
        out.update({
            "pass": data.get("retCode") == 0 and len(rows) > 0,
            "retCode": data.get("retCode"),
            "retMsg": data.get("retMsg"),
            "candles": len(rows),
            "newest_start": rows[0][0] if rows else None,
            "oldest_start": rows[-1][0] if rows else None,
            "sample": rows[0][:7] if rows else None,
        })
        return out
    except requests.RequestException as exc:
        return {
            "host": base,
            "pass": False,
            "error": f"{type(exc).__name__}: {exc}",
        }

@app.get("/test-kline/<symbol>")
def test_kline(symbol):
    symbol = symbol.upper().strip()
    if not symbol.isalnum() or len(symbol) > 30:
        return jsonify({"error": "invalid symbol"}), 400

    # Test a small D request first. If a host works, immediately verify
    # the exact 500-candle payload needed by the future seed builder.
    results = []
    winner = None
    for base in KLINE_HOSTS:
        probe = _probe_kline_host(base, symbol, interval="D", limit=500)
        results.append(probe)
        if probe.get("pass"):
            winner = base
            break

    return jsonify({
        "symbol": symbol,
        "category": "linear",
        "interval": "D",
        "requested_candles": 500,
        "pass": winner is not None,
        "working_host": winner,
        "results": results,
    })

@app.get("/test-kline")
def test_kline_batch():
    symbols = ("ENAUSDT", "BTCUSDT", "ETHUSDT")
    output = {}
    for symbol in symbols:
        results = []
        winner = None
        for base in KLINE_HOSTS:
            probe = _probe_kline_host(base, symbol, interval="D", limit=500)
            results.append(probe)
            if probe.get("pass"):
                winner = base
                break
        output[symbol] = {
            "pass": winner is not None,
            "working_host": winner,
            "results": results,
        }

    return jsonify({
        "test": "Bybit trade-price Kline REST from Render",
        "all_pass": all(item["pass"] for item in output.values()),
        "results": output,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
