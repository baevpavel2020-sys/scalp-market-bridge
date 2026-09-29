import os
import requests
from flask import Flask, jsonify

from collector import collector
from spot_collector import spot_collector

app = Flask(__name__)

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

@app.get("/test-bybit-streams")
def test_bybit_streams():
    import websocket
    import json
    import time

    endpoint = "wss://stream.bybit.com/v5/public/linear"

    topics = [
        "publicTrade.BTCUSDT",
        "orderbook.50.BTCUSDT",
        "tickers.BTCUSDT"
    ]

    ws = None

    try:
        started = time.time()

        ws = websocket.create_connection(
            endpoint,
            timeout=10
        )

        ws.send(json.dumps({
            "op": "subscribe",
            "args": topics
        }))

        received = {
            "publicTrade.BTCUSDT": False,
            "orderbook.50.BTCUSDT": False,
            "tickers.BTCUSDT": False
        }

        samples = {}

        deadline = time.time() + 15

        while time.time() < deadline and not all(received.values()):
            raw = ws.recv()
            data = json.loads(raw)

            topic = data.get("topic")

            if topic in received:
                received[topic] = True

                if topic == "publicTrade.BTCUSDT":
                    trades = data.get("data", [])
                    samples["trade_count"] = len(trades)

                    if trades:
                        samples["trade"] = {
                            "price": trades[0].get("p"),
                            "size": trades[0].get("v"),
                            "side": trades[0].get("S")
                        }

                elif topic == "orderbook.50.BTCUSDT":
                    book = data.get("data", {})

                    bids = book.get("b", [])
                    asks = book.get("a", [])

                    samples["orderbook"] = {
                        "bid_levels": len(bids),
                        "ask_levels": len(asks),
                        "best_bid": bids[0] if bids else None,
                        "best_ask": asks[0] if asks else None
                    }

                elif topic == "tickers.BTCUSDT":
                    ticker = data.get("data", {})

                    samples["ticker"] = {
                        "lastPrice": ticker.get("lastPrice"),
                        "markPrice": ticker.get("markPrice"),
                        "indexPrice": ticker.get("indexPrice"),
                        "openInterest": ticker.get("openInterest"),
                        "fundingRate": ticker.get("fundingRate")
                    }

        return jsonify({
            "test": "Bybit Scan+ WS streams",
            "connected": True,
            "connect_ms": round((time.time() - started) * 1000),
            "received": received,
            "all_streams_work": all(received.values()),
            "samples": samples
        })

    except Exception as e:
        return jsonify({
            "test": "Bybit Scan+ WS streams",
            "connected": False,
            "error": str(e)
        })

    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass

@app.get("/market/BTCUSDT")
def market_btcusdt():
    return jsonify(collector.get_snapshot())

@app.get("/market/BTCUSDT/spot")
def market_btcusdt_spot():
    return jsonify(spot_collector.get_snapshot())

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
