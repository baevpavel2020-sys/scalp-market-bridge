import csv
import io
import json
import os
import re
import urllib.error
import urllib.request
import zipfile
import threading
import time
import uuid
from collections import deque

import websocket


# ============================================================
# CONFIG
# ============================================================

WS_URLS = {
    "linear": "wss://stream.bybit.com/v5/public/linear",
    "spot": "wss://stream.bybit.com/v5/public/spot",
}

WINDOWS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
}

SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,24}USDT$")


def fnum(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ============================================================
# ONE MARKET STREAM
# ============================================================

KLINE_INTERVALS = ("1", "5", "15", "60", "240", "D")
KLINE_LIMIT = 500
CANDLE_STORE_DIR = os.environ.get(
    "CANDLE_STORE_DIR",
    "/tmp/scalp-market-bridge/candles",
)

BINANCE_PUBLIC_DATA = "https://data.binance.vision/data/futures/um"
BINANCE_INTERVALS = {
    "1": "1m",
    "5": "5m",
    "15": "15m",
    "60": "1h",
    "240": "4h",
    "D": "1d",
}
BINANCE_SEED_MONTHS = 30


class MarketStream:

    def __init__(self, symbol, market):

        self.symbol = symbol
        self.market = market

        self.lock = threading.RLock()
        self.stop_event = threading.Event()

        self.thread = None
        self.ws = None

        self.status = "created"
        self.connected = False
        self.available = None

        self.last_error = None
        self.last_message_at = None

        self.session_id = None
        self.session_started_at = None

        self.connect_attempts = 0
        self.successful_connections = 0
        self.reconnects = 0

        # trades:
        # timestamp_ms, side, price, qty
        self.trades = deque()

        # OI history:
        # timestamp_ms, open_interest
        self.oi_samples = deque()

        self.bids = {}
        self.asks = {}

        self.orderbook_ready = False

        self.ticker = {}
        # Kline / candle storage for Technical Engine
        self.candles = {
            interval: deque(maxlen=KLINE_LIMIT)
            for interval in KLINE_INTERVALS
        }
        self.history_bootstrapped = False
        self.history_error = None
        self.history_loaded_at = None
        self.history_source = "websocket_cache"
        self.seed_source = None
        self.seed_counts = {interval: 0 for interval in KLINE_INTERVALS}
        self._cache_dirty = False
        self._last_cache_save = 0.0

        # Restore native Bybit candles first, then fill missing history from
        # Binance USD-M public archives. Binance is seed only; incoming Bybit
        # WebSocket candles always replace an overlapping seed candle.
        self._load_candle_cache()
        if self.market == "linear":
            self._bootstrap_binance_seed()

        # cumulative delta from current collector session
        self.cvd_session = 0.0

    # ========================================================
    # START / STOP
    # ========================================================

    def start(self):

        with self.lock:

            if (
                self.thread
                and self.thread.is_alive()
            ):
                return

            self.stop_event.clear()

            self.thread = threading.Thread(
                target=self._run_forever,
                daemon=True,
                name=f"market-{self.market}-{self.symbol}",
            )

            self.thread.start()

    def stop(self):

        self.stop_event.set()

        with self.lock:

            self.status = "stopping"
            ws = self.ws

        if ws:

            try:
                ws.close()

            except Exception:
                pass

    # ========================================================
    # CONNECTION LOOP
    # ========================================================

    def _run_forever(self):

        backoff = 1

        while not self.stop_event.is_set():

            with self.lock:

                self.status = "connecting"
                self.connect_attempts += 1

            try:

                self._run_connection()

                backoff = 1

            except Exception as exc:

                with self.lock:

                    self.connected = False
                    self.orderbook_ready = False

                    self.status = "reconnecting"

                    self.last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

                if not self.stop_event.wait(backoff):

                    backoff = min(
                        backoff * 2,
                        30,
                    )

        with self.lock:

            self.connected = False
            self.status = "stopped"

    # ========================================================
    # WEBSOCKET
    # ========================================================

    def _run_connection(self):

        ws = websocket.create_connection(
            WS_URLS[self.market],
            timeout=30,
        )

        # recv timeout allows heartbeat / stop checks
        ws.settimeout(5)

        with self.lock:

            self.ws = ws

            if self.successful_connections:
                self.reconnects += 1

            self.successful_connections += 1

            self.connected = True
            self.available = True

            self.status = "connected"
            self.last_error = None

            self.session_id = str(uuid.uuid4())

            self.session_started_at = time.time()
            self.last_message_at = time.time()

            # New connection = new trustworthy session.
            self.trades.clear()
            self.oi_samples.clear()

            self.bids.clear()
            self.asks.clear()

            self.orderbook_ready = False

            self.ticker.clear()

            self.cvd_session = 0.0

        topics = [
            f"publicTrade.{self.symbol}",
            f"orderbook.50.{self.symbol}",
            f"tickers.{self.symbol}",

            # Candles for Scan+ Technical Engine
            f"kline.1.{self.symbol}",
            f"kline.5.{self.symbol}",
            f"kline.15.{self.symbol}",
            f"kline.60.{self.symbol}",
            f"kline.240.{self.symbol}",
            f"kline.D.{self.symbol}",
        ]

        ws.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "args": topics,
                }
            )
        )

        last_ping = time.time()

        try:

            while not self.stop_event.is_set():

                # Heartbeat
                if time.time() - last_ping >= 20:

                    ws.send(
                        json.dumps(
                            {
                                "op": "ping"
                            }
                        )
                    )

                    last_ping = time.time()

                try:

                    raw = ws.recv()

                except websocket.WebSocketTimeoutException:

                    continue

                if not raw:
                    continue

                message = json.loads(raw)

                with self.lock:
                    self.last_message_at = time.time()

                # Subscription rejected.
                # Useful for coins without spot market.
                if (
                    message.get("op") == "subscribe"
                    and message.get("success") is False
                ):

                    reason = (
                        message.get("ret_msg")
                        or message.get("retMsg")
                        or "subscription rejected"
                    )

                    with self.lock:

                        self.available = False
                        self.status = "unavailable"
                        self.last_error = reason

                    return

                self._handle_message(message)

        finally:

            with self.lock:

                self.connected = False
                self.ws = None

            try:
                ws.close()

            except Exception:
                pass

    # ========================================================
    # MESSAGE ROUTER
    # ========================================================

    # ========================================================
    # HISTORICAL KLINES / TECHNICAL ENGINE
    # ========================================================

    def _cache_path(self):
        safe_symbol = re.sub(r"[^A-Z0-9]", "", self.symbol)
        safe_market = re.sub(r"[^a-z]", "", self.market.lower())
        return os.path.join(
            CANDLE_STORE_DIR,
            f"{safe_symbol}_{safe_market}.json",
        )

    def _load_candle_cache(self):
        path = self._cache_path()
        try:
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            loaded = 0
            for interval in KLINE_INTERVALS:
                rows = payload.get("candles", {}).get(interval, [])
                clean = []
                seen = set()
                for row in rows[-KLINE_LIMIT:]:
                    try:
                        candle = {
                            "start": int(row["start"]),
                            "end": int(row["end"]),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row["volume"]),
                            "turnover": float(row.get("turnover", 0.0)),
                            "confirm": bool(row.get("confirm", True)),
                            "source": str(row.get("source", "bybit_ws")),
                        }
                    except (KeyError, TypeError, ValueError):
                        continue
                    if candle["start"] in seen:
                        continue
                    if candle["low"] > candle["high"]:
                        continue
                    seen.add(candle["start"])
                    clean.append(candle)
                clean.sort(key=lambda c: c["start"])
                self.candles[interval].extend(clean[-KLINE_LIMIT:])
                loaded += len(clean)
            if loaded:
                self.history_bootstrapped = True
                self.history_loaded_at = time.time()
                self.history_error = None
        except Exception as exc:
            self.history_error = f"cache load: {type(exc).__name__}: {exc}"

    @staticmethod
    def _month_shift(year, month, delta):
        total = year * 12 + (month - 1) + delta
        return total // 12, total % 12 + 1

    @staticmethod
    def _binance_row_to_candle(row, interval):
        # Binance public Kline CSV:
        # open_time, open, high, low, close, volume, close_time,
        # quote_asset_volume, number_of_trades, ...
        if len(row) < 8:
            return None
        try:
            start = int(row[0])
            candle = {
                "start": start,
                "end": int(row[6]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[7]),
                "confirm": True,
                "source": "binance_seed",
            }
        except (TypeError, ValueError):
            return None
        if candle["low"] > candle["high"]:
            return None
        return candle

    def _download_binance_month(self, interval, year, month):
        binance_interval = BINANCE_INTERVALS[interval]
        filename = (
            f"{self.symbol}-{binance_interval}-{year:04d}-{month:02d}.zip"
        )
        url = (
            f"{BINANCE_PUBLIC_DATA}/monthly/klines/{self.symbol}/"
            f"{binance_interval}/{filename}"
        )
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "scalp-market-bridge/1.0",
                "Accept": "*/*",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return []
            raise

        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [
                name for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".csv")
            ]
            if not names:
                return []
            text = archive.read(names[0]).decode("utf-8-sig")

        candles = []
        for row in csv.reader(io.StringIO(text)):
            # Header rows, if present, naturally fail integer parsing.
            candle = self._binance_row_to_candle(row, interval)
            if candle is not None:
                candles.append(candle)
        return candles

    def _merge_seed(self, interval, seed_rows):
        # Existing rows are native Bybit cache and therefore win on overlap.
        existing = {int(c["start"]): dict(c) for c in self.candles[interval]}
        combined = {int(c["start"]): dict(c) for c in seed_rows}
        combined.update(existing)
        rows = sorted(combined.values(), key=lambda c: c["start"])[-KLINE_LIMIT:]
        self.candles[interval].clear()
        self.candles[interval].extend(rows)

    def _bootstrap_binance_seed(self):
        # Spot is intentionally not seeded from Binance: Scan+ historical
        # technical analysis uses the perpetual seed, while Bybit spot remains
        # native realtime flow/orderbook data.
        if self.market != "linear":
            return

        now = time.gmtime()
        # Start with the latest completed month. Current month is available as
        # daily archives, but avoiding it keeps the bootstrap simple and stable;
        # Bybit WS supplies current candles.
        year, month = self._month_shift(now.tm_year, now.tm_mon, -1)

        errors = []
        total_seeded = 0

        for interval in KLINE_INTERVALS:
            native_rows = list(self.candles[interval])
            if len(native_rows) >= KLINE_LIMIT:
                continue

            seed_rows = []
            for back in range(BINANCE_SEED_MONTHS):
                y, mo = self._month_shift(year, month, -back)
                try:
                    rows = self._download_binance_month(interval, y, mo)
                except Exception as exc:
                    errors.append(
                        f"{interval} {y:04d}-{mo:02d}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue

                if rows:
                    seed_rows.extend(rows)
                    if len(seed_rows) >= KLINE_LIMIT:
                        break

            seed_rows.sort(key=lambda c: c["start"])
            self._merge_seed(interval, seed_rows[-KLINE_LIMIT:])
            seeded = sum(
                1 for c in self.candles[interval]
                if c.get("source") == "binance_seed"
            )
            self.seed_counts[interval] = seeded
            total_seeded += seeded

        if total_seeded:
            self.history_bootstrapped = True
            self.history_loaded_at = time.time()
            self.history_source = "binance_seed+bybit_websocket"
            self._cache_dirty = True
            self._save_candle_cache(force=True)

        self.history_error = "; ".join(errors[-6:]) if errors else None

    def _save_candle_cache(self, force=False):
        now = time.time()
        if not force and (not self._cache_dirty or now - self._last_cache_save < 5.0):
            return
        path = self._cache_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                "version": 1,
                "symbol": self.symbol,
                "market": self.market,
                "saved_at": now,
                "source": self.history_source,
                "candles": {
                    interval: list(rows)
                    for interval, rows in self.candles.items()
                },
            }
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, separators=(",", ":"))
            os.replace(tmp, path)
            self._cache_dirty = False
            self._last_cache_save = now
            self.history_error = None
        except Exception as exc:
            self.history_error = f"cache save: {type(exc).__name__}: {exc}"

    @staticmethod
    def _candle_end_ms(start_ms, interval):
        return start_ms + (86_400_000 if interval == "D" else int(interval) * 60_000) - 1

    @staticmethod
    def _ema(values, period):
        if len(values) < period:
            return None
        alpha = 2.0 / (period + 1.0)
        value = sum(values[:period]) / period
        for item in values[period:]:
            value = alpha * item + (1.0 - alpha) * value
        return value

    @staticmethod
    def _ema_series(values, period):
        if len(values) < period:
            return []
        alpha = 2.0 / (period + 1.0)
        value = sum(values[:period]) / period
        out = [None] * (period - 1) + [value]
        for item in values[period:]:
            value = alpha * item + (1.0 - alpha) * value
            out.append(value)
        return out

    @staticmethod
    def _rsi(values, period=14):
        if len(values) < period + 1:
            return None
        gains, losses = [], []
        for previous, current in zip(values[-period-1:-1], values[-period:]):
            change = current - previous
            gains.append(max(change, 0.0))
            losses.append(max(-change, 0.0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    @classmethod
    def _macd(cls, values, fast=12, slow=26, signal=9):
        if len(values) < slow + signal:
            return None
        fast_s = cls._ema_series(values, fast)
        slow_s = cls._ema_series(values, slow)
        line = [
            fast_s[i] - slow_s[i]
            for i in range(len(values))
            if i < len(fast_s) and i < len(slow_s)
            and fast_s[i] is not None and slow_s[i] is not None
        ]
        if len(line) < signal:
            return None
        sig = cls._ema(line, signal)
        return {"macd": line[-1], "signal": sig, "histogram": line[-1] - sig}

    @staticmethod
    def _stochastic(candles, period=14, smooth_k=3, smooth_d=3):
        if len(candles) < period + smooth_k + smooth_d - 2:
            return None
        raw = []
        for end in range(period, len(candles) + 1):
            chunk = candles[end-period:end]
            hi = max(c["high"] for c in chunk)
            lo = min(c["low"] for c in chunk)
            raw.append(50.0 if hi == lo else 100.0 * (chunk[-1]["close"] - lo) / (hi - lo))
        ks = [sum(raw[i-smooth_k+1:i+1]) / smooth_k for i in range(smooth_k-1, len(raw))]
        if len(ks) < smooth_d:
            return None
        return {"k": ks[-1], "d": sum(ks[-smooth_d:]) / smooth_d}

    @staticmethod
    def _atr(candles, period=14):
        if len(candles) < period + 1:
            return None
        rows = candles[-period-1:]
        trs = [
            max(cur["high"] - cur["low"],
                abs(cur["high"] - prev["close"]),
                abs(cur["low"] - prev["close"]))
            for prev, cur in zip(rows[:-1], rows[1:])
        ]
        return sum(trs) / period

    @staticmethod
    def _vwap(candles):
        volume = sum(c["volume"] for c in candles)
        if volume <= 0:
            return None
        return sum(
            ((c["high"] + c["low"] + c["close"]) / 3.0) * c["volume"]
            for c in candles
        ) / volume

    def _technical_metrics(self, candles):
        rows = list(candles)
        closes = [c["close"] for c in rows]
        macd = self._macd(closes)
        stoch = self._stochastic(rows)
        def rnd(v):
            return None if v is None else round(float(v), 10)
        return {
            "count": len(rows),
            "ready": len(rows) >= 200,
            "last_start": rows[-1]["start"] if rows else None,
            "last_close": rows[-1]["close"] if rows else None,
            "last_confirm": rows[-1]["confirm"] if rows else None,
            "ema20": rnd(self._ema(closes, 20)),
            "ema50": rnd(self._ema(closes, 50)),
            "ema200": rnd(self._ema(closes, 200)),
            "rsi14": rnd(self._rsi(closes)),
            "macd": None if macd is None else {k: rnd(v) for k, v in macd.items()},
            "stochastic": None if stoch is None else {k: rnd(v) for k, v in stoch.items()},
            "atr14": rnd(self._atr(rows)),
            "vwap": rnd(self._vwap(rows)),
        }

    @staticmethod
    def _swing_points(rows, left=2, right=2):
        highs, lows = [], []
        if len(rows) < left + right + 1:
            return highs, lows
        for i in range(left, len(rows) - right):
            cur = rows[i]
            before = rows[i-left:i]
            after = rows[i+1:i+right+1]
            if all(cur["high"] > x["high"] for x in before + after):
                highs.append({"index": i, "start": cur["start"], "price": cur["high"]})
            if all(cur["low"] < x["low"] for x in before + after):
                lows.append({"index": i, "start": cur["start"], "price": cur["low"]})
        return highs, lows

    def _structure_metrics(self, candles):
        rows = list(candles)
        highs, lows = self._swing_points(rows)
        last_close = rows[-1]["close"] if rows else None
        state = "insufficient_data"
        bos = None
        choch = None

        if len(highs) >= 2 and len(lows) >= 2:
            h1, h2 = highs[-2]["price"], highs[-1]["price"]
            l1, l2 = lows[-2]["price"], lows[-1]["price"]
            if h2 > h1 and l2 > l1:
                state = "uptrend"
            elif h2 < h1 and l2 < l1:
                state = "downtrend"
            else:
                state = "range_or_transition"

            if last_close is not None:
                if last_close > highs[-1]["price"]:
                    bos = "bullish"
                elif last_close < lows[-1]["price"]:
                    bos = "bearish"

            if state == "uptrend" and bos == "bearish":
                choch = "bearish"
            elif state == "downtrend" and bos == "bullish":
                choch = "bullish"

        return {
            "ready": len(rows) >= 20 and len(highs) >= 2 and len(lows) >= 2,
            "state": state,
            "bos": bos,
            "choch": choch,
            "last_swing_high": highs[-1] if highs else None,
            "last_swing_low": lows[-1] if lows else None,
            "swing_high_count": len(highs),
            "swing_low_count": len(lows),
        }

    def _liquidity_metrics(self, candles):
        rows = list(candles)
        highs, lows = self._swing_points(rows)
        atr = self._atr(rows)
        tolerance = (atr * 0.15) if atr is not None else None

        def nearest_equal(points):
            if tolerance is None or len(points) < 2:
                return None
            for a, b in zip(reversed(points[:-1]), reversed(points[1:])):
                if abs(a["price"] - b["price"]) <= tolerance:
                    return {
                        "price": round((a["price"] + b["price"]) / 2.0, 10),
                        "first_start": a["start"],
                        "second_start": b["start"],
                        "distance": round(abs(a["price"] - b["price"]), 10),
                    }
            return None

        eqh = nearest_equal(highs)
        eql = nearest_equal(lows)
        sweep = None
        if len(rows) >= 2:
            cur = rows[-1]
            if eqh and cur["high"] > eqh["price"] and cur["close"] < eqh["price"]:
                sweep = "buy_side_swept"
            elif eql and cur["low"] < eql["price"] and cur["close"] > eql["price"]:
                sweep = "sell_side_swept"

        return {
            "ready": len(rows) >= 20 and atr is not None,
            "equal_highs": eqh,
            "equal_lows": eql,
            "sweep": sweep,
            "tolerance": None if tolerance is None else round(tolerance, 10),
        }

    def diagnostics(self):
        with self.lock:
            technical = {i: self._technical_metrics(c) for i, c in self.candles.items()}
            structure = {i: self._structure_metrics(c) for i, c in self.candles.items()}
            liquidity = {i: self._liquidity_metrics(c) for i, c in self.candles.items()}
            counts = {i: len(c) for i, c in self.candles.items()}
            checks = {
                "connected": bool(self.connected),
                "orderbook_ready": bool(self.orderbook_ready),
                "candles_receiving": any(v > 0 for v in counts.values()),
                "technical_full_6_of_6": all(technical[i]["ready"] for i in KLINE_INTERVALS),
            }
            return {
                "symbol": self.symbol,
                "market": self.market,
                "status": self.status,
                "checks": checks,
                "realtime_pass": (
                    checks["connected"]
                    and checks["orderbook_ready"]
                    and checks["candles_receiving"]
                ),
                "full_technical_pass": checks["technical_full_6_of_6"],
                "history": {
                    "source": self.history_source,
                    "cache_loaded": self.history_bootstrapped,
                    "loaded_at": self.history_loaded_at,
                    "error": self.history_error,
                    "store_path": self._cache_path(),
                    "seed_source": self.seed_source or (
                        "binance_usdm_public_archive"
                        if any(self.seed_counts.values())
                        else None
                    ),
                    "seed_counts": dict(self.seed_counts),
                },
                "candle_counts": counts,
                "technical": technical,
                "structure": structure,
                "liquidity": liquidity,
            }

    def _handle_message(self, message):

        topic = message.get(
            "topic",
            "",
        )

        if topic.startswith("publicTrade."):

            self._handle_trades(
                message.get(
                    "data",
                    [],
                )
            )

        elif topic.startswith("orderbook."):

            self._handle_orderbook(
                message
            )

        elif topic.startswith("tickers."):

            self._handle_ticker(
                message.get(
                    "data",
                    {},
                )
            )

        elif topic.startswith("kline."):

            self._handle_kline(
                topic,
                message.get(
                    "data",
                    [],
                ),
            )

    # ========================================================
    # KLINES / CANDLES
    # ========================================================

    def _handle_kline(self, topic, rows):

        if not isinstance(rows, list):
            return

        parts = topic.split(".")

        # Expected:
        # kline.1.BTCUSDT
        # kline.60.ETHUSDT
        # kline.D.ENAUSDT
        if len(parts) < 3:
            return

        interval = parts[1]

        if interval not in self.candles:
            return

        with self.lock:

            for row in rows:

                try:

                    start = int(
                        row["start"]
                    )

                    end = int(
                        row["end"]
                    )

                    candle = {
                        "start": start,
                        "end": end,

                        "open": float(
                            row["open"]
                        ),

                        "high": float(
                            row["high"]
                        ),

                        "low": float(
                            row["low"]
                        ),

                        "close": float(
                            row["close"]
                        ),

                        "volume": float(
                            row["volume"]
                        ),

                        "turnover": float(
                            row.get(
                                "turnover",
                                0,
                            )
                        ),

                        "confirm": bool(
                            row.get(
                                "confirm",
                                False,
                            )
                        ),
                        "source": "bybit_ws",
                    }

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                ):

                    continue

                candles = self.candles[
                    interval
                ]

                # Native Bybit data always wins over Binance seed.
                # Replace any matching timestamp; otherwise append and keep
                # the deque chronologically ordered.
                replaced = False
                for index in range(len(candles) - 1, -1, -1):
                    if candles[index]["start"] == start:
                        candles[index] = candle
                        replaced = True
                        break
                    if candles[index]["start"] < start:
                        break

                if not replaced:
                    merged = list(candles)
                    merged.append(candle)
                    merged.sort(key=lambda c: c["start"])
                    candles.clear()
                    candles.extend(merged[-KLINE_LIMIT:])

                self.seed_counts[interval] = sum(
                    1 for c in candles
                    if c.get("source") == "binance_seed"
                )
                self._cache_dirty = True

            self._save_candle_cache()

    # ========================================================
    # TRADES / DELTA / CVD
    # ========================================================

    def _handle_trades(self, rows):

        now_ms = int(
            time.time() * 1000
        )

        with self.lock:

            for trade in rows:

                try:

                    timestamp_ms = int(
                        trade["T"]
                    )

                    side = trade["S"]

                    price = float(
                        trade["p"]
                    )

                    qty = float(
                        trade["v"]
                    )

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                ):

                    continue

                self.trades.append(
                    (
                        timestamp_ms,
                        side,
                        price,
                        qty,
                    )
                )

                if side == "Buy":

                    self.cvd_session += qty

                elif side == "Sell":

                    self.cvd_session -= qty

            self._cleanup(
                now_ms
            )

    # ========================================================
    # ORDERBOOK
    # ========================================================

    def _handle_orderbook(self, message):

        data = (
            message.get("data")
            or {}
        )

        message_type = (
            message.get("type")
        )

        bids = data.get(
            "b",
            [],
        )

        asks = data.get(
            "a",
            [],
        )

        with self.lock:

            if message_type == "snapshot":

                self.bids.clear()
                self.asks.clear()

                self._apply_book(
                    self.bids,
                    bids,
                )

                self._apply_book(
                    self.asks,
                    asks,
                )

                self.orderbook_ready = True

            elif message_type == "delta":

                if not self.orderbook_ready:
                    return

                self._apply_book(
                    self.bids,
                    bids,
                )

                self._apply_book(
                    self.asks,
                    asks,
                )

    @staticmethod
    def _apply_book(book, rows):

        for row in rows:

            try:

                price = float(
                    row[0]
                )

                size = float(
                    row[1]
                )

            except (
                IndexError,
                TypeError,
                ValueError,
            ):

                continue

            if size == 0:

                book.pop(
                    price,
                    None,
                )

            else:

                book[price] = size

    # ========================================================
    # TICKER / OI / FUNDING
    # ========================================================

    def _handle_ticker(self, data):

        if not isinstance(
            data,
            dict,
        ):
            return

        now_ms = int(
            time.time() * 1000
        )

        with self.lock:

            # Bybit derivative ticker can send deltas,
            # therefore keep previous fields and update only
            # fields present in current message.
            self.ticker.update(
                data
            )

            self.ticker[
                "updated_at"
            ] = time.time()
