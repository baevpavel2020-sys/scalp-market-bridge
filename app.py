import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

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
    results = []

    for base_url in BYBIT_URLS:
        url = f"{base_url}/v5/market/tickers"
        try:
            r = requests.get(
                url,
                params={"category": "linear", "symbol": "BTCUSDT"},
                timeout=10,
            )

            item = {
                "host": base_url,
                "http_status": r.status_code,
            }

            try:
                data = r.json()
                item["retCode"] = data.get("retCode")
                item["retMsg"] = data.get("retMsg")

                rows = data.get("result", {}).get("list", [])
                if rows:
                    item["lastPrice"] = rows[0].get("lastPrice")
            except Exception:
                item["response"] = r.text[:300]

            results.append(item)

        except Exception as e:
            results.append({
                "host": base_url,
                "error": str(e)
            })

    return jsonify({
        "render_region_test": "Bybit REST",
        "results": results
    })


@app.get("/test-bybit-ws")
def test_bybit_ws():
    import websocket
    import json
    import time

    endpoints = [
        "wss://stream.bybit.com/v5/public/linear",
        "wss://stream.bybit.com/v5/public/spot",
    ]

    results = []

    for endpoint in endpoints:
        ws = None

        try:
            started = time.time()

            ws = websocket.create_connection(
                endpoint,
                timeout=10
            )

            connect_ms = round((time.time() - started) * 1000)

            subscribe = {
                "op": "subscribe",
                "args": ["publicTrade.BTCUSDT"]
            }

            ws.send(json.dumps(subscribe))

            messages = []

            for _ in range(5):
                raw = ws.recv()
                data = json.loads(raw)

                messages.append({
                    "topic": data.get("topic"),
                    "op": data.get("op"),
                    "success": data.get("success"),
                    "type": data.get("type")
                })

                if data.get("topic") == "publicTrade.BTCUSDT":
                    break

            trade_received = any(
                x.get("topic") == "publicTrade.BTCUSDT"
                for x in messages
            )

            results.append({
                "endpoint": endpoint,
                "connected": True,
                "connect_ms": connect_ms,
                "trade_received": trade_received,
                "messages": messages
            })

        except Exception as e:
            results.append({
                "endpoint": endpoint,
                "connected": False,
                "error": str(e)
            })

        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

    return jsonify({
        "render_region_test": "Bybit WebSocket",
        "results": results
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
