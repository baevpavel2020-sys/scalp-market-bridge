import os
import requests
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
