import json
import os
import urllib.error
import urllib.parse
import urllib.request

from flask import Blueprint, jsonify

coinapi_test = Blueprint("coinapi_test", __name__)

BASE = "https://rest.coinapi.io/v1"
SYMBOL = "BYBIT_PERP_ENA_USDT"
PERIODS = ("1MIN", "5MIN", "15MIN", "1HRS", "4HRS", "1DAY")


def _get(path, params=None):
    key = os.environ.get("COINAPI_KEY", "").strip()
    if not key:
        return None, 500, "COINAPI_KEY is not configured"

    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(
        url,
        headers={
            "X-CoinAPI-Key": key,
            "Accept": "application/json",
            "User-Agent": "scalp-market-bridge/coinapi-probe",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw), r.status, None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:1000]
        return None, e.code, body
    except Exception as e:
        return None, 0, f"{type(e).__name__}: {e}"


@coinapi_test.get("/test-coinapi")
def test_coinapi():
    result = {
        "symbol": SYMBOL,
        "key_present": bool(os.environ.get("COINAPI_KEY")),
        "periods": {},
        "all_pass": True,
    }

    # Official CoinAPI example uses this exact Bybit perpetual symbol format.
    for period in PERIODS:
        data, status, error = _get(
            f"/ohlcv/{SYMBOL}/latest",
            {"period_id": period, "limit": 500},
        )

        rows = data if isinstance(data, list) else []
        passed = status == 200 and len(rows) == 500

        item = {
            "pass": passed,
            "http": status,
            "candles": len(rows),
        }

        if rows:
            newest = rows[0]  # /latest is newest -> oldest
            oldest = rows[-1]
            item["newest"] = {
                "time": newest.get("time_period_start"),
                "open": newest.get("price_open"),
                "high": newest.get("price_high"),
                "low": newest.get("price_low"),
                "close": newest.get("price_close"),
                "volume": newest.get("volume_traded"),
            }
            item["oldest_time"] = oldest.get("time_period_start")

        if error:
            item["error"] = error

        result["periods"][period] = item
        result["all_pass"] = result["all_pass"] and passed

        # If authentication/symbol access itself fails, don't burn more quota.
        if status in (401, 403, 404, 429) and not passed:
            break

    return jsonify(result)


def register_coinapi_test(app):
    app.register_blueprint(coinapi_test)
