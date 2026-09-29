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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
