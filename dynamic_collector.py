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
        self.seed_counts = {interval: 0 for interval in KLINE_INTERVALS}
        self._cache_dirty = False
        self._last_cache_save = 0.0

        # Restore native Bybit cache first. Linear history is then filled
        # from Binance USD-M public archives; live Bybit always wins.
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
    def _binance_row_to_candle(row):
        if len(row) < 8:
            return None
        try:
            candle = {
                "start": int(row[0]),
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
        tf = BINANCE_INTERVALS[interval]
        filename = f"{self.symbol}-{tf}-{year:04d}-{month:02d}.zip"
        url = (
            f"{BINANCE_PUBLIC_DATA}/monthly/klines/{self.symbol}/"
            f"{tf}/{filename}"
        )
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "scalp-market-bridge/1.0", "Accept": "*/*"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return []
            raise

        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            if not names:
                return []
            text = archive.read(names[0]).decode("utf-8-sig")

        rows = []
        for row in csv.reader(io.StringIO(text)):
            candle = self._binance_row_to_candle(row)
            if candle is not None:
                rows.append(candle)
        return rows

    def _merge_seed(self, interval, seed_rows):
        # Existing cache is treated as native Bybit and wins on timestamp overlap.
        existing = {int(c["start"]): dict(c) for c in self.candles[interval]}
        merged = {int(c["start"]): dict(c) for c in seed_rows}
        merged.update(existing)
        rows = sorted(merged.values(), key=lambda c: c["start"])[-KLINE_LIMIT:]
        self.candles[interval].clear()
        self.candles[interval].extend(rows)

    def _bootstrap_binance_seed(self):
        if self.market != "linear":
            return

        now = time.gmtime()
        # Monthly archive for the current month is incomplete/not published yet.
        year, month = self._month_shift(now.tm_year, now.tm_mon, -1)
        errors = []
        total_seeded = 0

        for interval in KLINE_INTERVALS:
            if len(self.candles[interval]) >= KLINE_LIMIT:
                continue

            seed_rows = []
            for back in range(BINANCE_SEED_MONTHS):
                y, mo = self._month_shift(year, month, -back)
                try:
                    rows = self._download_binance_month(interval, y, mo)
                except Exception as exc:
                    errors.append(
                        f"{interval} {y:04d}-{mo:02d}: {type(exc).__name__}: {exc}"
                    )
                    continue
                if rows:
                    seed_rows.extend(rows)
                    if len(seed_rows) >= KLINE_LIMIT:
                        break

            seed_rows.sort(key=lambda c: c["start"])
            self._merge_seed(interval, seed_rows[-KLINE_LIMIT:])
            self.seed_counts[interval] = sum(
                1 for c in self.candles[interval]
                if c.get("source") == "binance_seed"
            )
            total_seeded += self.seed_counts[interval]

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
        """Confirmed fractal pivots. Kept for liquidity compatibility."""
        highs, lows = [], []
        if len(rows) < left + right + 1:
            return highs, lows
        for i in range(left, len(rows) - right):
            cur = rows[i]
            before = rows[i-left:i]
            after = rows[i+1:i+right+1]
            if all(cur["high"] > x["high"] for x in before + after):
                highs.append({
                    "index": i, "start": cur["start"],
                    "price": cur["high"], "kind": "high",
                })
            if all(cur["low"] < x["low"] for x in before + after):
                lows.append({
                    "index": i, "start": cur["start"],
                    "price": cur["low"], "kind": "low",
                })
        return highs, lows

    @staticmethod
    def _atr_at(rows, end_index, period=14):
        if end_index < 1:
            return None
        begin = max(1, end_index - period + 1)
        trs = []
        for i in range(begin, end_index + 1):
            prev, cur = rows[i-1], rows[i]
            trs.append(max(
                cur["high"] - cur["low"],
                abs(cur["high"] - prev["close"]),
                abs(cur["low"] - prev["close"]),
            ))
        return sum(trs) / len(trs) if trs else None

    @classmethod
    def _hierarchical_pivots(cls, rows):
        """
        Three structural degrees. The ATR displacement filter suppresses
        insignificant pivots while preserving the raw confirmed fractal.
        """
        configs = {
            "minor": (2, 0.35),
            "intermediate": (4, 0.75),
            "major": (7, 1.25),
        }
        result = {}
        for degree, (width, min_atr) in configs.items():
            highs, lows = cls._swing_points(rows, width, width)
            points = sorted(highs + lows, key=lambda p: p["index"])

            # Collapse consecutive pivots of the same kind to the more extreme one.
            collapsed = []
            for p in points:
                p = dict(p)
                p["degree"] = degree
                if collapsed and collapsed[-1]["kind"] == p["kind"]:
                    better = (
                        p["price"] > collapsed[-1]["price"]
                        if p["kind"] == "high"
                        else p["price"] < collapsed[-1]["price"]
                    )
                    if better:
                        collapsed[-1] = p
                else:
                    collapsed.append(p)

            # Require meaningful displacement from the previous opposite pivot.
            filtered = []
            for p in collapsed:
                if not filtered:
                    filtered.append(p)
                    continue
                atr = cls._atr_at(rows, p["index"])
                displacement = abs(p["price"] - filtered[-1]["price"])
                if atr is None or atr <= 0 or displacement >= atr * min_atr:
                    filtered.append(p)
                else:
                    # If noise produces the same kind after filtering, retain
                    # only the more extreme point.
                    if filtered[-1]["kind"] == p["kind"]:
                        better = (
                            p["price"] > filtered[-1]["price"]
                            if p["kind"] == "high"
                            else p["price"] < filtered[-1]["price"]
                        )
                        if better:
                            filtered[-1] = p
            result[degree] = filtered
        return result

    @staticmethod
    def _label_pivots(points):
        """Label structural pivots HH/LH/HL/LL relative to same-kind predecessor."""
        last_high = None
        last_low = None
        labelled = []
        for point in points:
            p = dict(point)
            if p["kind"] == "high":
                if last_high is None:
                    label = "H"
                else:
                    label = "HH" if p["price"] > last_high["price"] else "LH"
                last_high = p
            else:
                if last_low is None:
                    label = "L"
                else:
                    label = "HL" if p["price"] > last_low["price"] else "LL"
                last_low = p
            p["label"] = label
            labelled.append(p)
        return labelled

    @staticmethod
    def _leg_metrics(rows, a, b):
        if not a or not b or b["index"] <= a["index"]:
            return None
        start_price = float(a["price"])
        end_price = float(b["price"])
        change = end_price - start_price
        pct = (change / start_price * 100.0) if start_price else None
        segment = rows[a["index"]:b["index"]+1]
        duration = b["index"] - a["index"]
        volume = sum(float(c.get("volume", 0.0)) for c in segment)
        atr = MarketStream._atr_at(rows, b["index"])
        strength_atr = abs(change) / atr if atr and atr > 0 else None
        return {
            "from": {
                "start": a["start"], "price": start_price,
                "kind": a["kind"], "label": a.get("label"),
            },
            "to": {
                "start": b["start"], "price": end_price,
                "kind": b["kind"], "label": b.get("label"),
            },
            "direction": "up" if change > 0 else "down" if change < 0 else "flat",
            "change": round(change, 10),
            "change_pct": None if pct is None else round(pct, 6),
            "candles": duration,
            "volume": round(volume, 8),
            "strength_atr": None if strength_atr is None else round(strength_atr, 4),
        }

    @staticmethod
    def _overlap_between_legs(first, second):
        if not first or not second:
            return None
        a0, a1 = first["from"]["price"], first["to"]["price"]
        b0, b1 = second["from"]["price"], second["to"]["price"]
        lo1, hi1 = min(a0, a1), max(a0, a1)
        lo2, hi2 = min(b0, b1), max(b0, b1)
        overlap = max(0.0, min(hi1, hi2) - max(lo1, lo2))
        span = min(hi1 - lo1, hi2 - lo2)
        return {
            "exists": overlap > 0,
            "price_overlap": round(overlap, 10),
            "of_smaller_leg_pct": (
                round(overlap / span * 100.0, 4) if span > 0 else 0.0
            ),
        }

    def _structure_metrics(self, candles):
        rows = list(candles)
        empty = {
            "ready": False,
            "state": "insufficient_data",
            "phase": "unknown",
            "bos": None,
            "choch": None,
            "last_event": None,
            "last_swing_high": None,
            "last_swing_low": None,
            "swing_high_count": 0,
            "swing_low_count": 0,
            "sequence": [],
            "degrees": {},
            "current_leg": None,
            "previous_leg": None,
            "overlap": None,
        }
        if len(rows) < 20:
            return empty

        hierarchy = self._hierarchical_pivots(rows)
        labelled = {
            degree: self._label_pivots(points)
            for degree, points in hierarchy.items()
        }

        # Intermediate is the working structure. Fall back to minor if needed.
        working_degree = "intermediate"
        points = labelled[working_degree]
        if len(points) < 4:
            working_degree = "minor"
            points = labelled[working_degree]

        highs = [p for p in points if p["kind"] == "high"]
        lows = [p for p in points if p["kind"] == "low"]
        state = "range_or_transition"

        if len(highs) >= 2 and len(lows) >= 2:
            h_up = highs[-1]["price"] > highs[-2]["price"]
            l_up = lows[-1]["price"] > lows[-2]["price"]
            h_down = highs[-1]["price"] < highs[-2]["price"]
            l_down = lows[-1]["price"] < lows[-2]["price"]
            if h_up and l_up:
                state = "uptrend"
            elif h_down and l_down:
                state = "downtrend"

        # BOS/CHoCH uses the latest confirmed structural level BEFORE current bar.
        last = rows[-1]
        previous_close = rows[-2]["close"]
        atr = self._atr(rows)
        break_buffer = (atr * 0.05) if atr else 0.0
        prior_highs = [p for p in highs if p["index"] < len(rows) - 1]
        prior_lows = [p for p in lows if p["index"] < len(rows) - 1]
        ref_high = prior_highs[-1] if prior_highs else None
        ref_low = prior_lows[-1] if prior_lows else None

        bos = None
        choch = None
        event = None
        if ref_high and last["close"] > ref_high["price"] + break_buffer:
            bos = "bullish"
            choch = "bullish" if state == "downtrend" else None
            event = {
                "type": "CHOCH" if choch else "BOS",
                "direction": "bullish",
                "level": ref_high["price"],
                "close": last["close"],
                "confirmed_by_close": True,
                "break_atr": (
                    round((last["close"] - ref_high["price"]) / atr, 4)
                    if atr else None
                ),
            }
        elif ref_low and last["close"] < ref_low["price"] - break_buffer:
            bos = "bearish"
            choch = "bearish" if state == "uptrend" else None
            event = {
                "type": "CHOCH" if choch else "BOS",
                "direction": "bearish",
                "level": ref_low["price"],
                "close": last["close"],
                "confirmed_by_close": True,
                "break_atr": (
                    round((ref_low["price"] - last["close"]) / atr, 4)
                    if atr else None
                ),
            }
        elif ref_high and last["high"] > ref_high["price"] and last["close"] <= ref_high["price"]:
            event = {
                "type": "SWEEP",
                "direction": "buy_side",
                "level": ref_high["price"],
                "confirmed_by_close": False,
            }
        elif ref_low and last["low"] < ref_low["price"] and last["close"] >= ref_low["price"]:
            event = {
                "type": "SWEEP",
                "direction": "sell_side",
                "level": ref_low["price"],
                "confirmed_by_close": False,
            }

        legs = [
            self._leg_metrics(rows, a, b)
            for a, b in zip(points[:-1], points[1:])
        ]
        legs = [leg for leg in legs if leg is not None]
        previous_leg = legs[-2] if len(legs) >= 2 else None
        current_leg = legs[-1] if legs else None
        overlap = self._overlap_between_legs(previous_leg, current_leg)

        # Phase describes the last confirmed structural leg relative to trend.
        phase = "unknown"
        if current_leg:
            if state == "uptrend":
                phase = "impulse" if current_leg["direction"] == "up" else "correction"
            elif state == "downtrend":
                phase = "impulse" if current_leg["direction"] == "down" else "correction"
            else:
                phase = "transition"

        degree_summary = {}
        for degree, pts in labelled.items():
            degree_summary[degree] = {
                "count": len(pts),
                "sequence": [p["label"] for p in pts[-8:]],
                "last_points": [
                    {
                        "start": p["start"],
                        "price": p["price"],
                        "kind": p["kind"],
                        "label": p["label"],
                    }
                    for p in pts[-8:]
                ],
            }

        return {
            "ready": len(points) >= 4 and len(highs) >= 2 and len(lows) >= 2,
            "working_degree": working_degree,
            "state": state,
            "phase": phase,
            "bos": bos,
            "choch": choch,
            "last_event": event,
            "last_swing_high": highs[-1] if highs else None,
            "last_swing_low": lows[-1] if lows else None,
            "swing_high_count": len(highs),
            "swing_low_count": len(lows),
            "sequence": [p["label"] for p in points[-10:]],
            "degrees": degree_summary,
            "current_leg": current_leg,
            "previous_leg": previous_leg,
            "overlap": overlap,
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
                    "seed_source": (
                        "binance_usdm_public_archive"
                        if any(self.seed_counts.values()) else None
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
                    1 for c in candles if c.get("source") == "binance_seed"
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

            # OI exists only on derivatives.
            if self.market == "linear":

                oi = fnum(
                    data.get(
                        "openInterest"
                    )
                )

                if oi is not None:

                    if (
                        not self.oi_samples
                        or oi
                        != self.oi_samples[-1][1]
                    ):

                        self.oi_samples.append(
                            (
                                now_ms,
                                oi,
                            )
                        )

                    self._cleanup(
                        now_ms
                    )

    # ========================================================
    # ROLLING STORAGE
    # ========================================================

    def _cleanup(self, now_ms):

        cutoff = (
            now_ms
            - WINDOWS["1h"]
        )

        while (
            self.trades
            and self.trades[0][0]
            < cutoff
        ):

            self.trades.popleft()

        while (
            self.oi_samples
            and self.oi_samples[0][0]
            < cutoff
        ):

            self.oi_samples.popleft()

    # ========================================================
    # FLOW METRICS
    # ========================================================

    def _flow_metrics(
        self,
        now_ms,
    ):

        result = {}

        for (
            name,
            duration,
        ) in WINDOWS.items():

            cutoff = (
                now_ms
                - duration
            )

            buy = 0.0
            sell = 0.0

            count = 0

            first_price = None
            last_price = None

            for (
                timestamp_ms,
                side,
                price,
                qty,
            ) in reversed(
                self.trades
            ):

                if timestamp_ms < cutoff:
                    break

                count += 1

                # Because we iterate backwards,
                # last_price is newest trade,
                # first_price ends as oldest trade.
                first_price = price

                if last_price is None:
                    last_price = price

                if side == "Buy":

                    buy += qty

                elif side == "Sell":

                    sell += qty

            total = (
                buy
                + sell
            )

            delta = (
                buy
                - sell
            )

            delta_ratio = (
                delta / total
                if total
                else None
            )

            price_change_pct = None

            if (
                first_price
                and last_price
            ):

                price_change_pct = (
                    (
                        last_price
                        - first_price
                    )
                    / first_price
                    * 100
                )

            result[name] = {

                "buy_volume":
                    round(
                        buy,
                        8,
                    ),

                "sell_volume":
                    round(
                        sell,
                        8,
                    ),

                "total_volume":
                    round(
                        total,
                        8,
                    ),

                "delta":
                    round(
                        delta,
                        8,
                    ),

                # Normalized delta.
                # +1 = all aggressive buys
                # -1 = all aggressive sells
                "delta_ratio": (
                    round(
                        delta_ratio,
                        6,
                    )
                    if delta_ratio is not None
                    else None
                ),

                "trade_count":
                    count,

                "price_change_pct": (
                    round(
                        price_change_pct,
                        6,
                    )
                    if price_change_pct is not None
                    else None
                ),
            }

        return result

    # ========================================================
    # OPEN INTEREST METRICS
    # ========================================================

    def _oi_metrics(
        self,
        now_ms,
    ):

        if self.market != "linear":
            return None

        current = fnum(
            self.ticker.get(
                "openInterest"
            )
        )

        result = {
            "current":
                current,

            "windows":
                {},
        }

        for (
            name,
            duration,
        ) in WINDOWS.items():

            cutoff = (
                now_ms
                - duration
            )

            base = None

            for (
                timestamp_ms,
                oi,
            ) in self.oi_samples:

                if timestamp_ms >= cutoff:

                    base = oi
                    break

            change = None
            change_pct = None

            if (
                current is not None
                and base is not None
            ):

                change = (
                    current
                    - base
                )

                if base != 0:

                    change_pct = (
                        change
                        / base
                        * 100
                    )

            result[
                "windows"
            ][name] = {

                "start":
                    base,

                "change": (
                    round(
                        change,
                        8,
                    )
                    if change is not None
                    else None
                ),

                "change_pct": (
                    round(
                        change_pct,
                        6,
                    )
                    if change_pct is not None
                    else None
                ),
            }

        return result

    # ========================================================
    # ORDERBOOK METRICS
    # ========================================================

    def _book_metrics(self):

        if (
            not self.orderbook_ready
            or not self.bids
            or not self.asks
        ):

            return {
                "ready": False
            }

        best_bid = max(
            self.bids
        )

        best_ask = min(
            self.asks
        )

        mid = (
            best_bid
            + best_ask
        ) / 2

        spread = (
            best_ask
            - best_bid
        )

        bid_depth = sum(
            self.bids.values()
        )

        ask_depth = sum(
            self.asks.values()
        )

        total_depth = (
            bid_depth
            + ask_depth
        )

        imbalance = (
            (
                bid_depth
                - ask_depth
            )


            / total_depth
            if total_depth
            else None
        )

        top_bids = sorted(
            self.bids.items(),
            reverse=True,
        )[:5]

        top_asks = sorted(
            self.asks.items()
        )[:5]

        return {

            "ready":
                True,

            "best_bid":
                best_bid,

            "best_ask":
                best_ask,

            "spread":
                round(
                    spread,
                    10,
                ),

            "spread_bps": (
                round(
                    spread
                    / mid
                    * 10_000,
                    6,
                )
                if mid
                else None
            ),

            "bid_depth_50":
                round(
                    bid_depth,
                    8,
                ),

            "ask_depth_50":
                round(
                    ask_depth,
                    8,
                ),

            "imbalance_50": (
                round(
                    imbalance,
                    6,
                )
                if imbalance is not None
                else None
            ),

            "top5_bids": [
                [price, qty]
                for price, qty
                in top_bids
            ],

            "top5_asks": [
                [price, qty]
                for price, qty
                in top_asks
            ],
        }

    # ========================================================
    # PUBLIC SNAPSHOT
    # ========================================================

    def snapshot(self):

        now = time.time()

        now_ms = int(
            now * 1000
        )

        with self.lock:

            self._cleanup(
                now_ms
            )

            session_age = (
                now
                - self.session_started_at
                if self.session_started_at
                else None
            )

            message_age = (
                now
                - self.last_message_at
                if self.last_message_at
                else None
            )

            return {

                "market":
                    self.market,

                "symbol":
                    self.symbol,

                "status":
                    self.status,

                "connected":
                    self.connected,

                "available":
                    self.available,

                "session_id":
                    self.session_id,

                "session_age_seconds": (
                    round(
                        session_age,
                        2,
                    )
                    if session_age is not None
                    else None
                ),

                "last_message_age_seconds": (
                    round(
                        message_age,
                        2,
                    )
                    if message_age is not None
                    else None
                ),

                "reconnects":
                    self.reconnects,

                "last_error":
                    self.last_error,

                # Window is trustworthy only after
                # collector has lived through full window.
                "warmup": {

                    name: bool(
                        session_age is not None
                        and session_age * 1000
                        >= duration
                    )

                    for (
                        name,
                        duration,
                    )
                    in WINDOWS.items()
                },

                # Session CVD starts from zero
                # after collector/reconnect.
                "cvd_session":
                    round(
                        self.cvd_session,
                        8,
                    ),

                "history": {
                    "source": self.history_source,
                    "cache_loaded": self.history_bootstrapped,
                    "error": self.history_error,
                    "loaded_at": self.history_loaded_at,
                    "seed_source": (
                        "binance_usdm_public_archive"
                        if any(self.seed_counts.values()) else None
                    ),
                    "seed_counts": dict(self.seed_counts),
                },

                "structure": {
                    interval: self._structure_metrics(candles)
                    for interval, candles in self.candles.items()
                },

                "liquidity": {
                    interval: self._liquidity_metrics(candles)
                    for interval, candles in self.candles.items()
                },

                "technical": {
                    interval: self._technical_metrics(candles)
                    for interval, candles
                    in self.candles.items()
                },

                "candles": {
                    interval: list(candles)
                    for interval, candles
                    in self.candles.items()
                },

                "flow":
                    self._flow_metrics(
                        now_ms
                    ),

                "open_interest":
                    self._oi_metrics(
                        now_ms
                    ),

                "funding_rate": (
                    fnum(
                        self.ticker.get(
                            "fundingRate"
                        )
                    )
                    if self.market == "linear"
                    else None
                ),

                "ticker":
                    dict(
                        self.ticker
                    ),

                "orderbook":
                    self._book_metrics(),
            }


# ============================================================
# DYNAMIC MANAGER
# ============================================================

class DynamicMarketManager:

    def __init__(
        self,
        max_symbols=6,
        idle_timeout=3600,
    ):

        self.lock = threading.RLock()

        self.max_symbols = (
            max_symbols
        )

        self.idle_timeout = (
            idle_timeout
        )

        self.streams = {}
        self.last_access = {}

    # ========================================================
    # SYMBOL
    # ========================================================

    @staticmethod
    def normalize_symbol(symbol):

        symbol = (
            symbol
            .upper()
            .strip()
        )

        if not symbol.endswith(
            "USDT"
        ):

            symbol += "USDT"

        if not SYMBOL_RE.fullmatch(
            symbol
        ):

            raise ValueError(
                "invalid symbol"
            )

        return symbol

    # ========================================================
    # ACTIVATE
    # ========================================================

    def activate(
        self,
        symbol,
    ):

        symbol = (
            self.normalize_symbol(
                symbol
            )
        )

        with self.lock:

            self._cleanup_idle()

            self.last_access[
                symbol
            ] = time.time()

            if symbol not in self.streams:

                self._make_room()

                linear = MarketStream(
                    symbol,
                    "linear",
                )

                spot = MarketStream(
                    symbol,
                    "spot",
                )

                self.streams[
                    symbol
                ] = {

                    "linear":
                        linear,

                    "spot":
                        spot,
                }

                linear.start()
                spot.start()

            return symbol

    # ========================================================
    # SNAPSHOT
    # ========================================================

    def snapshot(
        self,
        symbol,
    ):

        symbol = self.activate(
            symbol
        )

        with self.lock:

            self.last_access[
                symbol
            ] = time.time()

            pair = self.streams[
                symbol
            ]

        linear = (
            pair["linear"]
            .snapshot()
        )

        spot = (
            pair["spot"]
            .snapshot()
        )

        return {

            "symbol":
                symbol,

            "generated_at":
                time.time(),

            # Raw factual data
            "linear":
                linear,

            "spot":
                spot,

            # Diagnostic comparison.
            # NOT a trade signal.
            "driver":
                self._driver(
                    linear,
                    spot,
                ),
        }

    def diagnostics(self, symbol):
        symbol = self.activate(symbol)
        with self.lock:
            self.last_access[symbol] = time.time()
            pair = self.streams[symbol]
        linear = pair["linear"].diagnostics()
        spot = pair["spot"].diagnostics()
        return {
            "symbol": symbol,
            "generated_at": time.time(),
            "linear": linear,
            "spot": spot,
            "overall": {
                "realtime_pass": bool(
                    linear["realtime_pass"] and spot["realtime_pass"]
                ),
                "full_technical_pass": bool(
                    linear["full_technical_pass"] and spot["full_technical_pass"]
                ),
            },
        }

    # ========================================================
    # DRIVER
    #
    # Answers:
    # "Who appears to be pushing current price flow?"
    #
    # This is diagnostic evidence for Scan+.
    # It is NOT LONG / SHORT logic.
    # ========================================================

    def _driver(
        self,
        linear,
        spot,
    ):

        result = {}

        for window in WINDOWS:

            perp_ready = (
                linear
                .get(
                    "warmup",
                    {}
                )
                .get(
                    window,
                    False,
                )
            )

            spot_ready = (
                spot
                .get(
                    "warmup",
                    {}
                )
                .get(
                    window,
                    False,
                )
            )

            ready = (
                perp_ready
                and spot_ready
                and linear.get(
                    "connected"
                )
                and spot.get(
                    "connected"
                )
            )

            perp_flow = (
                linear
                .get(
                    "flow",
                    {}
                )
                .get(
                    window,
                    {}
                )
            )

            spot_flow = (
                spot
                .get(
                    "flow",
                    {}
                )
                .get(
                    window,
                    {}
                )
            )

            perp_ratio = (
                perp_flow.get(
                    "delta_ratio"
                )
            )

            spot_ratio = (
                spot_flow.get(
                    "delta_ratio"
                )
            )

            price_change = (
                perp_flow.get(
                    "price_change_pct"
                )
            )

            if not ready:

                state = "warming_up"
                leader = "unknown"
                confidence = "low"

            elif (
                perp_ratio is None
                or spot_ratio is None
            ):

                state = "insufficient_data"
                leader = "unknown"
                confidence = "low"

            else:

                perp_strength = abs(
                    perp_ratio
                )

                spot_strength = abs(
                    spot_ratio
                )

                # Ignore very small delta imbalance.
                threshold = 0.05

                if (
                    perp_strength < threshold
                    and spot_strength < threshold
                ):

                    state = "balanced"
                    leader = "mixed"
                    confidence = "low"

                # Both buying
                elif (
                    perp_ratio > threshold
                    and spot_ratio > threshold
                ):

                    state = "both_buying"

                    if (
                        spot_strength
                        > perp_strength * 1.25
                    ):

                        leader = "spot"

                    elif (
                        perp_strength
                        > spot_strength * 1.25
                    ):

                        leader = "perp"

                    else:

                        leader = "both"

                    confidence = "high"

                # Both selling
                elif (
                    perp_ratio < -threshold
                    and spot_ratio < -threshold
                ):

                    state = "both_selling"

                    if (
                        spot_strength
                        > perp_strength * 1.25
                    ):

                        leader = "spot"

                    elif (
                        perp_strength
                        > spot_strength * 1.25
                    ):

                        leader = "perp"

                    else:

                        leader = "both"

                    confidence = "high"

                # Direct disagreement
                elif (
                    perp_ratio > threshold
                    and spot_ratio < -threshold
                ):

                    state = (
                        "perp_buying_spot_selling"
                    )

                    leader = "conflict"
                    confidence = "high"

                elif (
                    perp_ratio < -threshold
                    and spot_ratio > threshold
                ):

                    state = (
                        "perp_selling_spot_buying"
                    )

                    leader = "conflict"
                    confidence = "high"

                # Perp only
                elif perp_strength >= threshold:

                    if perp_ratio > 0:

                        state = (
                            "perp_led_buying"
                        )

                    else:

                        state = (

                            "perp_led_selling"
                        )

                    leader = "perp"
                    confidence = "medium"

                # Spot only
                else:

                    if spot_ratio > 0:

                        state = (
                            "spot_led_buying"
                        )

                    else:

                        state = (
                            "spot_led_selling"
                        )

                    leader = "spot"
                    confidence = "medium"

            oi_window = (
                (
                    linear.get(
                        "open_interest"
                    )
                    or {}
                )
                .get(
                    "windows",
                    {}
                )
                .get(
                    window,
                    {}
                )
            )

            result[
                window
            ] = {

                "state":
                    state,

                "leader":
                    leader,

                "confidence":
                    confidence,

                "price_change_pct":
                    price_change,

                "perp_delta_ratio":
                    perp_ratio,

                "spot_delta_ratio":
                    spot_ratio,

                "oi_change_pct":
                    oi_window.get(
                        "change_pct"
                    ),

                "funding_rate":
                    linear.get(
                        "funding_rate"
                    ),
            }

        return result

    # ========================================================
    # CLEANUP
    # ========================================================

    def _cleanup_idle(self):

        now = time.time()

        stale = [

            symbol

            for (
                symbol,
                last_seen,
            )
            in self.last_access.items()

            if (
                now
                - last_seen
                > self.idle_timeout
            )
        ]

        for symbol in stale:

            pair = self.streams.pop(
                symbol,
                None,
            )

            self.last_access.pop(
                symbol,
                None,
            )

            if pair:

                pair[
                    "linear"
                ].stop()

                pair[
                    "spot"
                ].stop()

    # ========================================================
    # MAX ACTIVE SYMBOLS
    # ========================================================

    def _make_room(self):

        if (
            len(self.streams)
            < self.max_symbols
        ):

            return

        oldest = min(
            self.last_access,
            key=self.last_access.get,
        )

        pair = self.streams.pop(
            oldest
        )

        self.last_access.pop(
            oldest,
            None,
        )

        pair[
            "linear"
        ].stop()

        pair[
            "spot"
        ].stop()


# ============================================================
# GLOBAL MANAGER
# ============================================================

dynamic_manager = DynamicMarketManager(
    max_symbols=6,
    idle_timeout=3600,
)
