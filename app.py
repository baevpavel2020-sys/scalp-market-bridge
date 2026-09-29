import os
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

from collector import collector
from spot_collector import spot_collector
from dynamic_collector import dynamic_manager
from coinapi_test import register_coinapi_test

app = Flask(__name__)

# CoinAPI historical OHLCV test endpoint
register_coinapi_test(app)

collector.start()
spot_collector.start()

BYBIT_URLS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]


@app.get("/")
def home():
    return jsonify({
        "service": "scalp-market-bridge",
        "status": "online"
    })


@app.get("/test-bybit")
def test_bybit():
    results = {}

    for base in BYBIT_URLS:
        try:
            r = requests.get(
                f"{base}/v5/market/time",
                timeout=8
            )

            results[base] = {
                "status": r.status_code,
                "text": r.text[:500]
            }

        except Exception as exc:
            results[base] = {
                "error": str(exc)
            }

    return jsonify(results)


@app.get("/market/BTCUSDT")
def market_btcusdt():
    return jsonify(
        collector.get_snapshot()
    )


@app.get("/market/BTCUSDT/spot")
def market_btcusdt_spot():
    return jsonify(
        spot_collector.get_snapshot()
    )


@app.get("/market/<symbol>")
def market_dynamic(symbol):
    try:
        return jsonify(
            dynamic_manager.snapshot(symbol)
        )

    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400


@app.get("/diagnostics/<symbol>")
def diagnostics_dynamic(symbol):
    try:
        return jsonify(
            dynamic_manager.diagnostics(symbol)
        )

    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400


# ============================================================
# BYBIT PUBLIC TRADE ARCHIVE TEST
# ============================================================

def _history_url(symbol, day):
    symbol = symbol.upper().strip()

    return (
        f"https://public.bybit.com/trading/"
        f"{symbol}/{symbol}{day}.csv.gz"
    )


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
        "User-Agent":
            "Mozilla/5.0 scalp-market-bridge/1.0",
        "Accept": "*/*",
    })

    # Archive publication can lag behind realtime,
    # so walk backwards.

    today = datetime.now(
        timezone.utc
    ).date()

    attempts = []

    for offset in range(
        1,
        max_days + 1
    ):

        day = (
            today -
            timedelta(days=offset)
        ).isoformat()

        url = _history_url(
            symbol,
            day
        )

        try:

            # Stream GET is more reliable than HEAD
            # on archive/CDN endpoints.

            with session.get(
                url,
                stream=True,
                timeout=(8, 15),
                allow_redirects=True
            ) as r:

                attempts.append({
                    "date": day,
                    "status": r.status_code
                })

                if r.status_code == 200:

                    first_bytes = b""

                    for chunk in r.iter_content(
                        chunk_size=128
                    ):

                        if chunk:
                            first_bytes = chunk[:32]
                            break

                    return {
                        "symbol": symbol,
                        "archive_access": True,
                        "file_found": True,
                        "date": day,
                        "http_status":
                            r.status_code,
                        "url": url,
                        "content_type":
                            r.headers.get(
                                "Content-Type"
                            ),
                        "content_length":
                            r.headers.get(
                                "Content-Length"
                            ),
                        "content_encoding":
                            r.headers.get(
                                "Content-Encoding"
                            ),
                        "gzip_magic_ok":
                            first_bytes[:2]
                            == b"\x1f\x8b",
                        "first_bytes_hex":
                            first_bytes[:16].hex(),
                        "attempts": attempts,
                    }

                if r.status_code == 403:

                    return {
                        "symbol": symbol,
                        "archive_access": False,
                        "file_found": False,
                        "http_status": 403,
                        "url": url,
                        "error":
                            "archive reachable "
                            "but request forbidden",
                        "attempts": attempts,
                    }

        except requests.RequestException as exc:

            return {
                "symbol": symbol,
                "archive_access": False,
                "file_found": False,
                "error":
                    f"{type(exc).__name__}: "
                    f"{exc}",
                "attempts": attempts,
            }

    return {
        "symbol": symbol,
        "archive_access": True,
        "file_found": False,
        "error":
            f"no daily archive found "
            f"in previous {max_days} days",
        "attempts": attempts,
    }


@app.get("/test-history/<symbol>")
def test_history(symbol):

    return jsonify(
        _probe_history(symbol)
    )


@app.get("/test-history")
def test_history_batch():

    symbols = (
        "ENAUSDT",
        "BTCUSDT",
        "ETHUSDT"
    )

    results = {
        symbol: _probe_history(symbol)
        for symbol in symbols
    }

    return jsonify({
        "test":
            "Bybit public historical trades",

        "all_pass":
            all(
                item.get("archive_access")
                and item.get("file_found")
                for item
                in results.values()
            ),

        "results": results,
    })


# ============================================================
# BYBIT REST KLINE TEST
# ============================================================

KLINE_HOSTS = [

    "https://api.bybit.com",
    "https://api.bytick.com",
    "https://api.bybit.kz",
    "https://api.bybitgeorgia.ge",
    "https://api.bybit.ae",
    "https://api.bybit.id",
    "https://api.spark-fintech.com",
]


def _probe_kline_host(
    base,
    symbol,
    interval="D",
    category="linear",
    limit=500
):

    url = (
        f"{base}/v5/market/kline"
    )

    params = {
        "category": category,
        "symbol":
            symbol.upper().strip(),
        "interval": interval,
        "limit": limit,
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=(8, 15),
            headers={
                "User-Agent":
                    "Mozilla/5.0 "
                    "scalp-market-bridge/1.0",

                "Accept":
                    "application/json",
            },
        )

        out = {
            "host": base,
            "http_status":
                r.status_code,
            "final_url":
                r.url,
        }

        if r.status_code != 200:

            out["pass"] = False
            out["body"] = r.text[:250]

            return out

        try:

            data = r.json()

        except Exception:

            out["pass"] = False

            out["error"] = (
                "HTTP 200 but "
                "response is not JSON"
            )

            out["body"] = r.text[:250]

            return out

        rows = (
            ((data or {})
             .get("result") or {})
            .get("list") or []
        )

        out.update({
            "pass":
                data.get("retCode") == 0
                and len(rows) > 0,

            "retCode":
                data.get("retCode"),

            "retMsg":
                data.get("retMsg"),

            "candles":
                len(rows),

            "newest_start":
                rows[0][0]
                if rows else None,

            "oldest_start":
                rows[-1][0]
                if rows else None,

            "sample":
                rows[0][:7]
                if rows else None,
        })

        return out

    except requests.RequestException as exc:

        return {
            "host": base,
            "pass": False,
            "error":
                f"{type(exc).__name__}: "
                f"{exc}",
        }


@app.get("/test-kline/<symbol>")
def test_kline(symbol):

    symbol = symbol.upper().strip()

    if (
        not symbol.isalnum()
        or len(symbol) > 30
    ):

        return jsonify({
            "error": "invalid symbol"
        }), 400

    results = []
    winner = None

    for base in KLINE_HOSTS:

        probe = _probe_kline_host(
            base,
            symbol,
            interval="D",
            limit=500
        )

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

    symbols = (
        "ENAUSDT",
        "BTCUSDT",
        "ETHUSDT"
    )

    output = {}

    for symbol in symbols:

        results = []
        winner = None

        for base in KLINE_HOSTS:

            probe = _probe_kline_host(
                base,
                symbol,
                interval="D",
                limit=500
            )

            results.append(probe)

            if probe.get("pass"):
                winner = base
                break

        output[symbol] = {
            "pass":
                winner is not None,

            "working_host":
                winner,

            "results":
                results,
        }

    return jsonify({
        "test":
            "Bybit trade-price "
            "Kline REST from Render",

        "all_pass":
            all(
                item["pass"]
                for item
                in output.values()
            ),

        "results":
            output,
    })


# ============================================================
# START SERVER
# ============================================================

@app.get("/test-public-kline")
def test_public_kline():
    symbols = ("ENAUSDT", "BTCUSDT", "ETHUSDT")
    today = datetime.now(timezone.utc).date()
    results = {}

    for symbol in symbols:
        symbol_results = []
        found = None

        # Проверяем последние 7 завершённых дней:
        # архив может публиковаться с задержкой.
        for offset in range(1, 8):
            day = (today - timedelta(days=offset)).isoformat()

            url = (
                f"https://public.bybit.com/kline/"
                f"{symbol}/{day}/1min.csv.gz"
            )

            try:
                with requests.get(
                    url,
                    stream=True,
                    timeout=(8, 15),
                    headers={
                        "User-Agent": "Mozilla/5.0 scalp-market-bridge/1.0",
                        "Accept": "*/*",
                    },
                ) as r:

                    item = {
                        "date": day,
                        "http": r.status_code,
                        "url": url,
                        "content_type": r.headers.get("Content-Type"),
                        "content_length": r.headers.get("Content-Length"),
                    }

                    if r.status_code == 200:
                        first_bytes = b""

                        for chunk in r.iter_content(chunk_size=128):
                            if chunk:
                                first_bytes = chunk[:32]
                                break

                        item["gzip_magic_ok"] = (
                            first_bytes[:2] == b"\x1f\x8b"
                        )

                        found = item
                        symbol_results.append(item)
                        break

                    symbol_results.append(item)

            except requests.RequestException as exc:
                symbol_results.append({
                    "date": day,
                    "url": url,
                    "error": f"{type(exc).__name__}: {exc}",
                })

        results[symbol] = {
            "pass": found is not None
                    and found.get("http") == 200
                    and found.get("gzip_magic_ok") is True,
            "found": found,
            "attempts": symbol_results,
        }

    return jsonify({
        "test": "Bybit public Kline archive",
        "all_pass": all(
            item["pass"] for item in results.values()
        ),
        "results": results,
    })

@app.get("/test-binance-seed")
def test_binance_seed():
    tests = {
        "ENA_1d": (
            "https://data.binance.vision/data/futures/um/monthly/"
            "klines/ENAUSDT/1d/ENAUSDT-1d-2026-08.zip"
        ),
        "ENA_4h": (
            "https://data.binance.vision/data/futures/um/monthly/"
            "klines/ENAUSDT/4h/ENAUSDT-4h-2026-08.zip"
        ),
        "BTC_1d": (
            "https://data.binance.vision/data/futures/um/monthly/"
            "klines/BTCUSDT/1d/BTCUSDT-1d-2026-08.zip"
        ),
    }

    results = {}

    for name, url in tests.items():
        try:
            with requests.get(
                url,
                stream=True,
                timeout=(8, 20),
                headers={
                    "User-Agent": "Mozilla/5.0 scalp-market-bridge/1.0",
                    "Accept": "*/*",
                },
            ) as r:

                first_bytes = b""

                if r.status_code == 200:
                    for chunk in r.iter_content(chunk_size=64):
                        if chunk:
                            first_bytes = chunk[:16]
                            break

                results[name] = {
                    "pass": (
                        r.status_code == 200
                        and first_bytes[:2] == b"PK"
                    ),
                    "http": r.status_code,
                    "content_type": r.headers.get("Content-Type"),
                    "content_length": r.headers.get("Content-Length"),
                    "zip_magic_ok": first_bytes[:2] == b"PK",
                    "url": url,
                }

        except requests.RequestException as exc:
            results[name] = {
                "pass": False,
                "error": f"{type(exc).__name__}: {exc}",
                "url": url,
            }

    return jsonify({
        "test": "Binance USD-M Futures historical Kline seed",
        "all_pass": all(x["pass"] for x in results.values()),
        "results": results,
    })

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
