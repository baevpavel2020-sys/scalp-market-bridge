import csv
import datetime as dt
import io
import json
import math
import os
import re
import urllib.error
import urllib.request
import zipfile
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def _download_binance_day(self, interval, day):
        tf = BINANCE_INTERVALS[interval]
        stamp = day.strftime("%Y-%m-%d")
        filename = f"{self.symbol}-{tf}-{stamp}.zip"
        url = (
            f"{BINANCE_PUBLIC_DATA}/daily/klines/{self.symbol}/"
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

        now_dt = dt.datetime.now(dt.timezone.utc)
        now = time.gmtime()
        year, month = self._month_shift(now.tm_year, now.tm_mon, -1)
        errors = []
        total_seeded = 0

        # Phase A: old history. A full cache may skip monthly downloads,
        # but it must NEVER skip the recent daily refresh below.
        for interval in KLINE_INTERVALS:
            if len(self.candles[interval]) < KLINE_LIMIT:
                seed_rows = []
                for back in range(BINANCE_SEED_MONTHS):
                    y, mo = self._month_shift(year, month, -back)
                    try:
                        rows = self._download_binance_month(interval, y, mo)
                    except Exception as exc:
                        errors.append(
                            f"{interval} monthly {y:04d}-{mo:02d}: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        continue
                    if rows:
                        seed_rows.extend(rows)
                        if len(seed_rows) >= KLINE_LIMIT:
                            break
                seed_rows.sort(key=lambda c: c["start"])
                self._merge_seed(interval, seed_rows[-KLINE_LIMIT:])

        # Phase B: fill the gap from the latest cached/seed candle through yesterday.
        # Binance monthly archives do not contain the current incomplete month.
        jobs = []
        yesterday = now_dt.date() - dt.timedelta(days=1)
        for interval in KLINE_INTERVALS:
            rows = list(self.candles[interval])
            if rows:
                last_day = dt.datetime.fromtimestamp(
                    int(rows[-1]["start"]) / 1000.0, tz=dt.timezone.utc
                ).date()
                first_day = last_day + dt.timedelta(days=1)
            else:
                first_day = max(
                    yesterday - dt.timedelta(days=34),
                    now_dt.date().replace(day=1),
                )

            # Safety cap prevents a damaged cache from creating hundreds of requests.
            first_day = max(first_day, yesterday - dt.timedelta(days=40))
            day = first_day
            while day <= yesterday:
                jobs.append((interval, day))
                day += dt.timedelta(days=1)

        daily_by_tf = {tf: [] for tf in KLINE_INTERVALS}
        if jobs:
            with ThreadPoolExecutor(max_workers=min(12, len(jobs))) as pool:
                futures = {
                    pool.submit(self._download_binance_day, interval, day):
                    (interval, day)
                    for interval, day in jobs
                }
                for future in as_completed(futures):
                    interval, day = futures[future]
                    try:
                        rows = future.result()
                    except Exception as exc:
                        errors.append(
                            f"{interval} daily {day.isoformat()}: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        continue
                    if rows:
                        daily_by_tf[interval].extend(rows)

        for interval in KLINE_INTERVALS:
            if daily_by_tf[interval]:
                daily_by_tf[interval].sort(key=lambda c: c["start"])
                self._merge_seed(interval, daily_by_tf[interval])

            self.seed_counts[interval] = sum(
                1 for c in self.candles[interval]
                if c.get("source") == "binance_seed"
            )
            total_seeded += self.seed_counts[interval]

        if total_seeded:
            self.history_bootstrapped = True
            self.history_loaded_at = time.time()
            self.history_source = "binance_seed_daily+monthly+bybit_websocket"
            self._cache_dirty = True
            self._save_candle_cache(force=True)

        self.history_error = "; ".join(errors[-8:]) if errors else None

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
            result[degree] = cls._sanitize_pivot_sequence(filtered)
        return result

    @staticmethod
    def _sanitize_pivot_sequence(points):
        """Return one chronological, strictly alternating, geometrically valid pivot chain."""
        clean=[]
        for raw in sorted(points, key=lambda p:(p.get("start",0), p.get("index",0))):
            if raw.get("kind") not in ("high","low"):
                continue
            try:
                price=float(raw.get("price"))
                start=int(raw.get("start"))
            except (TypeError,ValueError):
                continue
            if not math.isfinite(price) or price<=0:
                continue
            p=dict(raw); p["price"]=price; p["start"]=start

            # Same timestamp: keep only the point that extends the current leg.
            if clean and p["start"]==clean[-1]["start"]:
                if p["kind"]==clean[-1]["kind"]:
                    better=(p["price"]>clean[-1]["price"] if p["kind"]=="high"
                            else p["price"]<clean[-1]["price"])
                    if better: clean[-1]=p
                continue

            # Consecutive same-kind pivots are never allowed into pattern engines.
            if clean and p["kind"]==clean[-1]["kind"]:
                better=(p["price"]>clean[-1]["price"] if p["kind"]=="high"
                        else p["price"]<clean[-1]["price"])
                if better: clean[-1]=p
                continue

            # Geometry invariant: high must be above preceding low; low below preceding high.
            if clean:
                if p["kind"]=="high" and p["price"]<=clean[-1]["price"]:
                    continue
                if p["kind"]=="low" and p["price"]>=clean[-1]["price"]:
                    continue
            clean.append(p)
        return clean

    @staticmethod
    def _valid_pattern_sequence(seq, expected_len):
        if len(seq)!=expected_len:
            return False
        starts=[p.get("start") for p in seq]
        kinds=[p.get("kind") for p in seq]
        prices=[p.get("price") for p in seq]
        if any(x is None for x in starts+kinds+prices):
            return False
        if any(starts[i] >= starts[i+1] for i in range(len(starts)-1)):
            return False
        if any(kinds[i] == kinds[i+1] for i in range(len(kinds)-1)):
            return False
        for i in range(len(seq)-1):
            a=float(prices[i]); b=float(prices[i+1])
            if kinds[i]=="low" and not b>a: return False
            if kinds[i]=="high" and not b<a: return False
        return True

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
                "start": last.get("start"),
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
                "start": last.get("start"),
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
                "start": last.get("start"),
            }
        elif ref_low and last["low"] < ref_low["price"] and last["close"] >= ref_low["price"]:
            event = {
                "type": "SWEEP",
                "direction": "sell_side",
                "level": ref_low["price"],
                "confirmed_by_close": False,
                "start": last.get("start"),
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


    @staticmethod
    def _safe_ratio(a, b):
        return None if b is None or abs(b) < 1e-12 else abs(a / b)

    def _fib_metrics(self, rows, structure):
        points = structure.get("degrees", {}).get(
            structure.get("working_degree", "intermediate"), {}
        ).get("last_points", [])
        if len(points) < 2:
            return {"ready": False, "anchors": None, "retracements": {}, "extensions": {}, "clusters": []}
        a, b = points[-2], points[-1]
        p0, p1 = float(a["price"]), float(b["price"])
        move = p1 - p0
        if abs(move) < 1e-12:
            return {"ready": False, "anchors": None, "retracements": {}, "extensions": {}, "clusters": []}
        retr = {}
        for r in (0.236, 0.382, 0.5, 0.618, 0.705, 0.786, 0.886):
            retr[str(r)] = round(p1 - move * r, 10)
        ext = {}
        for r in (1.0, 1.272, 1.414, 1.618, 2.0, 2.618):
            ext[str(r)] = round(p0 + move * r, 10)

        # Cluster fib levels from the last few confirmed structural legs.
        pts = structure.get("degrees", {}).get(
            structure.get("working_degree", "intermediate"), {}
        ).get("last_points", [])
        levels = []
        for x, y in zip(pts[-7:-1], pts[-6:]):
            x0, y0 = float(x["price"]), float(y["price"])
            d = y0 - x0
            if abs(d) < 1e-12:
                continue
            for r in (0.382, 0.5, 0.618, 0.786, 1.272, 1.618):
                price = y0 - d * r if r < 1 else x0 + d * r
                levels.append((price, r))
        atr = self._atr(rows)
        tol = (atr * 0.15) if atr else abs(move) * 0.002
        clusters = []
        for price, ratio in sorted(levels):
            if clusters and abs(price - clusters[-1]["price"]) <= tol:
                c = clusters[-1]
                c["levels"].append(ratio)
                c["count"] += 1
                c["price"] = round((c["price"] * (c["count"] - 1) + price) / c["count"], 10)
            else:
                clusters.append({"price": round(price, 10), "count": 1, "levels": [ratio]})
        clusters = sorted((c for c in clusters if c["count"] >= 2), key=lambda c: (-c["count"], c["price"]))[:8]
        return {
            "ready": True,
            "direction": "up" if move > 0 else "down",
            "anchors": {"from": a, "to": b},
            "retracements": retr,
            "extensions": ext,
            "clusters": clusters,
        }

    def _elliott_metrics(self, rows, structure):
        degree = structure.get("working_degree", "intermediate")
        pts = structure.get("degrees", {}).get(degree, {}).get("last_points", [])
        candidates = []
        # Rule-based candidates only; ambiguous markets deliberately keep alternatives.
        for n in (6, 4):
            if len(pts) < n:
                continue
            for end in range(len(pts), n - 1, -1):
                seq = pts[end-n:end]
                prices = [float(p["price"]) for p in seq]
                kinds = [p["kind"] for p in seq]
                alternating = all(kinds[i] != kinds[i-1] for i in range(1, len(kinds)))
                if not alternating:
                    continue
                if n == 6:
                    bullish = kinds[0] == "low"
                    p0,p1,p2,p3,p4,p5 = prices
                    if bullish:
                        hard = p2 > p0 and p3 > p1 and p4 > p1 and p5 > p3
                        lengths = [p1-p0, p3-p2, p5-p4]
                    else:
                        hard = p2 < p0 and p3 < p1 and p4 < p1 and p5 < p3
                        lengths = [p0-p1, p2-p3, p4-p5]
                    wave3_not_shortest = lengths[1] >= min(lengths[0], lengths[2]) if all(x > 0 for x in lengths) else False
                    standard_no_overlap = (p4 > p1) if bullish else (p4 < p1)
                    valid = hard and wave3_not_shortest
                    if valid:
                        score = 4 + int(standard_no_overlap)
                        candidates.append({
                            "type": "impulse_1_5",
                            "direction": "bullish" if bullish else "bearish",
                            "degree": degree,
                            "score": score,
                            "standard_impulse": bool(standard_no_overlap),
                            "diagonal_overlap_possible": not standard_no_overlap,
                            "rules": {
                                "wave2_does_not_break_origin": True,
                                "wave3_not_shortest": bool(wave3_not_shortest),
                                "wave4_no_wave1_overlap": bool(standard_no_overlap),
                            },
                            "points": [
                                {"wave": str(i), **seq[i]} for i in range(6)
                            ],
                            "invalidation": p0,
                        })
                else:
                    # ABC candidate: A and C travel same direction, B retraces A.
                    p0,pA,pB,pC = prices
                    bullish_corr = pA < p0 and pB > pA and pC < pB
                    bearish_corr = pA > p0 and pB < pA and pC > pB
                    if bullish_corr or bearish_corr:
                        a_len = abs(pA-p0); b_len = abs(pB-pA); c_len = abs(pC-pB)
                        br = self._safe_ratio(b_len, a_len)
                        cr = self._safe_ratio(c_len, a_len)
                        candidates.append({
                            "type": "abc",
                            "direction": "down" if bullish_corr else "up",
                            "degree": degree,
                            "score": 2 + int(br is not None and 0.236 <= br <= 0.886) + int(cr is not None and 0.5 <= cr <= 1.618),
                            "ratios": {"B/A": None if br is None else round(br,4), "C/A": None if cr is None else round(cr,4)},
                            "points": [{"wave": w, **p} for w,p in zip(("0","A","B","C"),seq)],
                            "invalidation": p0,
                        })
                if len(candidates) >= 8:
                    break
            if len(candidates) >= 8:
                break
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return {
            "ready": len(pts) >= 4,
            "method": "rule_based_candidates",
            "primary": candidates[0] if candidates else None,
            "alternatives": candidates[1:4],
            "candidate_count": len(candidates),
        }

    def _harmonic_metrics(self, structure):
        degree = structure.get("working_degree", "intermediate")
        pts = structure.get("degrees", {}).get(degree, {}).get("last_points", [])
        patterns = []
        templates = {
            "Gartley": ((0.55,0.70),(0.382,0.886),(1.13,1.70),(0.72,0.84)),
            "Bat": ((0.35,0.55),(0.382,0.886),(1.50,2.70),(0.84,0.93)),
            "Butterfly": ((0.72,0.84),(0.382,0.886),(1.50,2.70),(1.20,1.70)),
            "Crab": ((0.35,0.70),(0.382,0.886),(2.20,3.70),(1.50,1.75)),
            "Deep Crab": ((0.84,0.93),(0.382,0.886),(2.00,3.70),(1.50,1.75)),
        }
        if len(pts) < 5:
            return {"ready": False, "patterns": []}
        for seq in [pts[i:i+5] for i in range(max(0,len(pts)-10),len(pts)-4)]:
            x,a,b,c,d=[float(p["price"]) for p in seq]
            xa=a-x; ab=b-a; bc=c-b; cd=d-c
            if min(abs(xa),abs(ab),abs(bc)) < 1e-12:
                continue
            vals=(abs(ab/xa),abs(bc/ab),abs(cd/bc),abs((d-x)/xa))
            for name,ranges in templates.items():
                passed=[lo <= v <= hi for v,(lo,hi) in zip(vals,ranges)]
                if sum(passed) >= 3:
                    patterns.append({
                        "name":name,
                        "direction":"bullish" if d < c else "bearish",
                        "score":sum(passed),
                        "ratios":{"AB_XA":round(vals[0],4),"BC_AB":round(vals[1],4),"CD_BC":round(vals[2],4),"XD_XA":round(vals[3],4)},
                        "points":[{"point":n,**p} for n,p in zip("XABCD",seq)],
                    })
            # AB=CD is useful independently of XABCD families.
            abcd=abs(cd/ab)
            if 0.90 <= abcd <= 1.10 or 1.20 <= abcd <= 1.75:
                patterns.append({
                    "name":"AB=CD" if 0.90 <= abcd <= 1.10 else "Extended AB=CD",
                    "direction":"bullish" if d < c else "bearish",
                    "score":4,
                    "ratios":{"CD_AB":round(abcd,4)},
                    "points":[{"point":n,**p} for n,p in zip("XABCD",seq)],
                })
        patterns.sort(key=lambda p:p["score"], reverse=True)
        return {"ready": True, "patterns": patterns[:6]}

    def _divergence_metrics(self, rows, structure):
        degree=structure.get("working_degree","intermediate")
        pts=structure.get("degrees",{}).get(degree,{}).get("last_points",[])
        closes=[r["close"] for r in rows]
        index_by_start = {r["start"]: i for i, r in enumerate(rows)}
        def rsi_at(point):
            idx = index_by_start.get(point.get("start"))
            if idx is None or idx < 15:
                return None
            return self._rsi(closes[:idx+1])
        events=[]
        for kind in ("high","low"):
            pp=[p for p in pts if p["kind"]==kind]
            if len(pp)<2: continue
            a,b=pp[-2],pp[-1]
            ra,rb=rsi_at(a),rsi_at(b)
            if ra is None or rb is None: continue
            if kind=="high":
                if b["price"]>a["price"] and rb<ra: typ="regular_bearish"
                elif b["price"]<a["price"] and rb>ra: typ="hidden_bearish"
                else: typ=None
            else:
                if b["price"]<a["price"] and rb>ra: typ="regular_bullish"
                elif b["price"]>a["price"] and rb<ra: typ="hidden_bullish"
                else: typ=None
            if typ:
                events.append({"type":typ,"indicator":"RSI14","from":a,"to":b,"indicator_from":round(ra,4),"indicator_to":round(rb,4)})
        return {"ready":len(pts)>=4,"events":events}

    def _smart_money_metrics(self, rows, structure, liquidity):
        if len(rows)<5:
            return {"ready":False,"fvg":[],"order_blocks":[],"dealing_range":None}
        fvg=[]
        for i in range(max(2,len(rows)-80),len(rows)):
            a,c=rows[i-2],rows[i]
            if c["low"]>a["high"]:
                fvg.append({"type":"bullish","from":a["high"],"to":c["low"],"start":c["start"]})
            elif c["high"]<a["low"]:
                fvg.append({"type":"bearish","from":c["high"],"to":a["low"],"start":c["start"]})
        fvg=fvg[-8:]
        degree=structure.get("working_degree","intermediate")
        pts=structure.get("degrees",{}).get(degree,{}).get("last_points",[])
        dealing=None
        if len(pts)>=2:
            lo=min(float(p["price"]) for p in pts[-6:])
            hi=max(float(p["price"]) for p in pts[-6:])
            mid=(lo+hi)/2
            close=rows[-1]["close"]
            dealing={"low":lo,"high":hi,"equilibrium":round(mid,10),"zone":"premium" if close>mid else "discount" if close<mid else "equilibrium"}
        obs=[]
        event=structure.get("last_event")
        if event and event.get("type") in ("BOS","CHOCH"):
            direction=event.get("direction")
            for c in reversed(rows[-30:-1]):
                bearish=c["close"]<c["open"]
                bullish=c["close"]>c["open"]
                if (direction=="bullish" and bearish) or (direction=="bearish" and bullish):
                    obs.append({"direction":direction,"low":c["low"],"high":c["high"],"start":c["start"]})
                    break
        return {
            "ready":True,
            "fvg":fvg,
            "order_blocks":obs,
            "dealing_range":dealing,
            "liquidity_event":liquidity.get("sweep"),
            "structure_event":event,
        }

    def _structure_context_v2(self, rows):
        base = self._structure_metrics(rows)
        degrees = {}
        for degree, data in base.get("degrees", {}).items():
            points = self._sanitize_pivot_sequence(data.get("last_points", []))
            # Keep the public base synchronized with the validated chain so every
            # downstream engine sees the same pivots.
            data["last_points"] = points
            data["sequence"] = [p.get("label") for p in points]
            legs = []
            for a, b in zip(points[:-1], points[1:]):
                ia = next((i for i, r in enumerate(rows) if r["start"] == a["start"]), None)
                ib = next((i for i, r in enumerate(rows) if r["start"] == b["start"]), None)
                if ia is None or ib is None:
                    continue
                aa, bb = dict(a), dict(b)
                aa["index"], bb["index"] = ia, ib
                leg = self._leg_metrics(rows, aa, bb)
                if leg:
                    legs.append(leg)
            degrees[degree] = {"points": points, "legs": legs, "sequence": data.get("sequence", [])}
        return {"ready": base.get("ready", False), "base": base, "degrees": degrees}

    @staticmethod
    def _fib_near(value, target, tolerance):
        return value is not None and abs(value - target) <= tolerance

    def _fib_engine_v2(self, rows, ctx):
        base = ctx["base"]
        legacy = self._fib_metrics(rows, base)
        degree = base.get("working_degree", "intermediate")
        points = ctx.get("degrees", {}).get(degree, {}).get("points", [])
        relationships = []
        if len(points) >= 3:
            for a, b, c in zip(points[:-2], points[1:-1], points[2:]):
                ab = abs(float(b["price"]) - float(a["price"]))
                bc = abs(float(c["price"]) - float(b["price"]))
                ratio = self._safe_ratio(bc, ab)
                if ratio is not None:
                    relationships.append({
                        "from": a["start"], "pivot": b["start"], "to": c["start"],
                        "retracement": round(ratio, 5)
                    })
        return {**legacy, "relationships": relationships[-10:]}

    def _elliott_engine_v2(self, rows, ctx, fib):
        degree = ctx["base"].get("working_degree", "intermediate")
        points = ctx.get("degrees", {}).get(degree, {}).get("points", [])
        candidates = []

        def add(candidate):
            candidate["evidence_count"] = sum(1 for v in candidate["checks"].values() if v is True)
            candidate["hard_fail"] = any(v is False for k, v in candidate["checks"].items() if k.startswith("hard_"))
            if not candidate["hard_fail"]:
                candidates.append(candidate)

        # Impulse / diagonal: 0-1-2-3-4-5.
        for i in range(max(0, len(points)-14), max(0, len(points)-5)):
            s = points[i:i+6]
            if len(s) < 6:
                continue
            if not self._valid_pattern_sequence(s,6):
                continue
            kinds = [p["kind"] for p in s]
            p=[float(x["price"]) for x in s]
            bull=kinds[0]=="low"
            w1=abs(p[1]-p[0]); w3=abs(p[3]-p[2]); w5=abs(p[5]-p[4])
            r2=self._safe_ratio(p[2]-p[1], p[1]-p[0])
            r3=self._safe_ratio(p[3]-p[2], p[1]-p[0])
            r4=self._safe_ratio(p[4]-p[3], p[3]-p[2])
            r5=self._safe_ratio(p[5]-p[4], p[1]-p[0])
            hard2=(p[2]>p[0]) if bull else (p[2]<p[0])
            hard3=w3 >= min(w1,w5)
            wave3_ext=(p[3]>p[1]) if bull else (p[3]<p[1])
            no_overlap=(p[4]>p[1]) if bull else (p[4]<p[1])
            wave5_ext=(p[5]>p[3]) if bull else (p[5]<p[3])
            common_fib = (
                (r2 is not None and 0.236 <= r2 <= 0.886) and
                (r3 is not None and 0.8 <= r3 <= 3.8) and
                (r4 is not None and 0.146 <= r4 <= 0.886)
            )
            base={
                "direction":"bullish" if bull else "bearish",
                "degree":degree,
                "points":[{"wave":str(j),**s[j]} for j in range(6)],
                "ratios":{"w2_w1":None if r2 is None else round(r2,4),"w3_w1":None if r3 is None else round(r3,4),"w4_w3":None if r4 is None else round(r4,4),"w5_w1":None if r5 is None else round(r5,4)},
                "invalidation":p[0],
            }
            add({**base,"type":"impulse","checks":{
                "hard_wave2_origin":hard2,
                "hard_wave3_not_shortest":hard3,
                "hard_wave3_exceeds_wave1":wave3_ext,
                "hard_wave4_no_overlap":no_overlap,
                "hard_wave5_exceeds_wave3":wave5_ext,
                "fib_typical":common_fib,
            }})
            # Diagonal is a separate candidate, never an excuse for an invalid impulse.
            contracting = w3 <= w1*1.35 and w5 <= w3*1.35
            add({**base,"type":"diagonal","checks":{
                "hard_wave2_origin":hard2,
                "hard_wave3_progress":wave3_ext,
                "hard_wave5_progress":wave5_ext,
                "hard_overlap_expected":not no_overlap,
                "contracting_or_near":contracting,
            }})

        # Corrective families on 0-A-B-C.
        for i in range(max(0,len(points)-14), max(0,len(points)-3)):
            s=points[i:i+4]
            if not self._valid_pattern_sequence(s,4):
                continue
            p=[float(x["price"]) for x in s]
            a=abs(p[1]-p[0]); b=abs(p[2]-p[1]); c=abs(p[3]-p[2])
            if min(a,b,c)<1e-12: continue
            # A and C must travel in the same direction; B must oppose A.
            leg_a=p[1]-p[0]; leg_b=p[2]-p[1]; leg_c=p[3]-p[2]
            if not (leg_a*leg_b<0 and leg_a*leg_c>0):
                continue
            br=b/a; cr=c/a
            direction="down" if leg_a<0 else "up"
            common={"direction":direction,"degree":degree,
                    "points":[{"wave":w,**x} for w,x in zip(("0","A","B","C"),s)],
                    "ratios":{"B_A":round(br,4),"C_A":round(cr,4)},"invalidation":p[0]}
            add({**common,"type":"zigzag","checks":{
                "hard_B_below_A_origin":br < 1.0,
                "hard_C_progresses": (p[3]<p[1]) if direction=="down" else (p[3]>p[1]),
                "B_typical":0.236 <= br <= 0.886,
                "C_typical":0.5 <= cr <= 2.0,
            }})
            add({**common,"type":"flat","checks":{
                "hard_B_deep":0.8 <= br <= 1.38,
                "hard_C_exists":c>0,
                "B_near_origin":0.9 <= br <= 1.1,
                "C_typical":0.8 <= cr <= 1.65,
            }})

        clusters = fib.get("clusters", []) if isinstance(fib, dict) else []
        atr = self._atr(rows)
        for candidate in candidates:
            endpoint = float(candidate["points"][-1]["price"])
            near_cluster = False
            if atr and atr > 0:
                near_cluster = any(abs(endpoint - float(c["price"])) <= atr * 0.25 for c in clusters)
            candidate["checks"]["fib_cluster_confluence"] = near_cluster
            candidate["evidence_count"] = sum(1 for v in candidate["checks"].values() if v is True)

        unique={}
        for c in candidates:
            key=(c["type"],c["direction"],tuple(p["start"] for p in c["points"]))
            old=unique.get(key)
            if old is None or c["evidence_count"]>old["evidence_count"]:
                unique[key]=c
        candidates=list(unique.values())
        candidates.sort(key=lambda x:(x["evidence_count"], x["type"]=="impulse"), reverse=True)
        primary=candidates[0] if candidates else None
        return {
            "ready":len(points)>=4,
            "method":"strict_rule_tree_v2",
            "primary":primary,
            "alternatives":candidates[1:4],
            "candidate_count":len(candidates),
            "ambiguous":len(candidates)>1 and primary is not None and candidates[1]["evidence_count"]>=primary["evidence_count"]-1,
        }

    def _harmonic_engine_v2(self, ctx):
        degree=ctx["base"].get("working_degree","intermediate")
        points=ctx.get("degrees",{}).get(degree,{}).get("points",[])
        templates={
            "Gartley":{"ab":(0.60,0.65),"bc":(0.382,0.886),"cd":(1.13,1.618),"xd":(0.76,0.81)},
            "Bat":{"ab":(0.382,0.50),"bc":(0.382,0.886),"cd":(1.618,2.618),"xd":(0.86,0.91)},
            "Butterfly":{"ab":(0.76,0.81),"bc":(0.382,0.886),"cd":(1.618,2.618),"xd":(1.24,1.31)},
            "Crab":{"ab":(0.382,0.618),"bc":(0.382,0.886),"cd":(2.24,3.618),"xd":(1.58,1.66)},
            "Deep Crab":{"ab":(0.86,0.91),"bc":(0.382,0.886),"cd":(2.0,3.618),"xd":(1.58,1.66)},
        }
        confirmed=[]; developing=[]
        for i in range(max(0,len(points)-12),max(0,len(points)-4)):
            s=points[i:i+5]
            if not self._valid_pattern_sequence(s,5):
                continue
            x,a,b,c,d=[float(q["price"]) for q in s]
            xa=a-x; ab=b-a; bc=c-b; cd=d-c
            if min(abs(xa),abs(ab),abs(bc))<1e-12: continue
            vals={"ab":abs(ab/xa),"bc":abs(bc/ab),"cd":abs(cd/bc),"xd":abs((d-x)/xa)}
            # Harmonic completion direction is determined by the final CD leg.
            # Reject degenerate shapes where D fails to extend beyond B in CD direction.
            bullish = cd < 0
            d_progress = (d < b) if bullish else (d > b)
            if not d_progress:
                continue
            for name,t in templates.items():
                checks={k:(lo<=vals[k]<=hi) for k,(lo,hi) in t.items()}
                rec={"name":name,"direction":"bullish" if bullish else "bearish",
                     "ratios":{k:round(v,4) for k,v in vals.items()},
                     "checks":checks,"points":[{"point":n,**q} for n,q in zip("XABCD",s)]}
                if all(checks.values()): confirmed.append(rec)
                elif sum(checks.values())==3: developing.append(rec)
            abcd=abs(cd/ab)
            if 0.95<=abcd<=1.05:
                confirmed.append({"name":"AB=CD","direction":"bullish" if bullish else "bearish",
                                  "ratios":{"CD_AB":round(abcd,4)},"checks":{"AB_CD":True},
                                  "points":[{"point":n,**q} for n,q in zip("XABCD",s)]})
        return {"ready":len(points)>=5,"confirmed":confirmed[-6:],"developing":developing[-6:]}

    def _divergence_engine_v2(self, rows, ctx):
        degree=ctx["base"].get("working_degree","intermediate")
        points=ctx.get("degrees",{}).get(degree,{}).get("points",[])
        idx={r["start"]:i for i,r in enumerate(rows)}
        def osc(point):
            i=idx.get(point["start"])
            if i is None or i<35: return None
            tech=self._technical_metrics(rows[:i+1]); macd=tech.get("macd") or {}; st=tech.get("stochastic") or {}
            return {"rsi":tech.get("rsi14"),"macd":macd.get("macd"),"stoch":st.get("k"),"atr":tech.get("atr14")}
        events=[]
        for kind in ("high","low"):
            pp=[p for p in points if p["kind"]==kind]
            if len(pp)<2: continue
            a,b=pp[-2],pp[-1]; oa,ob=osc(a),osc(b)
            if not oa or not ob: continue
            atr=float(ob.get("atr") or oa.get("atr") or 0.0)
            price_move=abs(float(b["price"])-float(a["price"]))
            price_floor=max(0.25*atr, max(abs(float(a["price"])),abs(float(b["price"]))) * 0.0005)
            if price_move < price_floor: continue
            for name in ("rsi","macd","stoch"):
                va,vb=oa.get(name),ob.get(name)
                if va is None or vb is None: continue
                osc_move=abs(float(vb)-float(va))
                if name=="rsi" and osc_move < 2.0: continue
                if name=="stoch" and osc_move < 5.0: continue
                if name=="macd":
                    macd_floor=max(0.03*atr, 0.10*max(abs(float(va)),abs(float(vb))), max(abs(float(a["price"])),abs(float(b["price"]))) * 1e-6)
                    if osc_move < macd_floor: continue
                typ=None
                if kind=="high":
                    if b["price"]>a["price"] and vb<va: typ="regular_bearish"
                    elif b["price"]<a["price"] and vb>va: typ="hidden_bearish"
                else:
                    if b["price"]<a["price"] and vb>va: typ="regular_bullish"
                    elif b["price"]>a["price"] and vb<va: typ="hidden_bullish"
                if typ:
                    events.append({"type":typ,"indicator":name,"from":a,"to":b,"from_value":round(va,7),"to_value":round(vb,7),
                                   "price_move_atr":round(price_move/atr,4) if atr else None})
        consensus={}
        for e in events: consensus[e["type"]]=consensus.get(e["type"],0)+1
        return {"ready":len(points)>=4,"events":events,"consensus":consensus}

    def _smart_money_engine_v2(self, rows, ctx, liquidity):
        base=self._smart_money_metrics(rows,ctx["base"],liquidity)
        # Mitigation status for FVGs; only retain useful recent zones.
        fvg=[]
        for z in base.get("fvg",[]):
            lo=min(z["from"],z["to"]); hi=max(z["from"],z["to"])
            z=dict(z)
            later=[r for r in rows if r.get("start",0)>z.get("start",0)]
            touched=any(float(r["low"])<=hi and float(r["high"])>=lo for r in later)
            fully_filled=any(float(r["low"])<=lo and float(r["high"])>=hi for r in later)
            z["mitigated"]=bool(touched)
            z["fully_filled"]=bool(fully_filled)
            fvg.append(z)
        base["fvg"]=fvg
        return base

    def _confluence_engine_v2(self, technical, ctx, fib, elliott, harmonics, divergences, liquidity, smc):
        bull=0
        bear=0
        evidence=[]
        state=ctx["base"].get("state")
        if state=="uptrend":
            bull+=2
            evidence.append("structure_up")
        elif state=="downtrend":
            bear+=2
            evidence.append("structure_down")

        ep=elliott.get("primary")
        if ep:
            if not elliott.get("ambiguous",False):
                if ep.get("direction") in ("bullish","up"):
                    bull+=2
                elif ep.get("direction") in ("bearish","down"):
                    bear+=2
            evidence.append("elliott_"+ep["type"]+("_ambiguous" if elliott.get("ambiguous",False) else ""))

        for p in harmonics.get("confirmed",[]):
            if p["direction"]=="bullish":
                bull+=2
            else:
                bear+=2
            evidence.append("harmonic_"+p["name"])

        for typ,count in divergences.get("consensus",{}).items():
            weight=min(2,count)
            if "bullish" in typ:
                bull+=weight
            else:
                bear+=weight
            evidence.append("divergence_"+typ)

        # Momentum is confirmation only, never a structural override.
        rsi=technical.get("rsi14")
        macd=technical.get("macd") or {}
        if rsi is not None:
            if rsi >= 55:
                bull+=1
                evidence.append("rsi_bullish")
            elif rsi <= 45:
                bear+=1
                evidence.append("rsi_bearish")
        if macd.get("macd") is not None and macd.get("signal") is not None:
            if macd["macd"] > macd["signal"]:
                bull+=1
                evidence.append("macd_bullish")
            elif macd["macd"] < macd["signal"]:
                bear+=1
                evidence.append("macd_bearish")

        # Structure/liquidity event has priority over raw indicator momentum.
        ev=ctx["base"].get("last_event")
        if ev and ev.get("direction")=="bullish":
            bull+=1
            evidence.append("structure_event_bullish")
        elif ev and ev.get("direction")=="bearish":
            bear+=1
            evidence.append("structure_event_bearish")

        sweep=liquidity.get("sweep")
        if isinstance(sweep, dict):
            direction=sweep.get("direction")
            if direction in ("sell_side","bullish"):
                bull+=1
                evidence.append("sell_side_sweep")
            elif direction in ("buy_side","bearish"):
                bear+=1
                evidence.append("buy_side_sweep")

        # Fib and SMC add location context; they do not create direction alone.
        fib_cluster_count=len(fib.get("clusters",[])) if isinstance(fib,dict) else 0
        smc_context={
            "dealing_range":smc.get("dealing_range"),
            "fvg_count":len(smc.get("fvg",[])),
            "order_block_count":len(smc.get("order_blocks",[])),
        }

        direction="bullish" if bull>bear else "bearish" if bear>bull else "neutral"
        return {
            "bullish":bull,
            "bearish":bear,
            "direction":direction,
            "evidence":evidence,
            "fib_cluster_count":fib_cluster_count,
            "smc_context":smc_context,
            "structural_uncertainty":elliott.get("ambiguous",False),
        }


    def _regime_levels_v2(self, rows, technical, structure, fib):
        if not rows:
            return {"ready":False}
        close=float(rows[-1]["close"])
        atr=technical.get("atr14")
        atr_pct=(atr/close*100.0) if atr and close else None
        pts=structure.get("degrees",{}).get(structure.get("working_degree","intermediate"),{}).get("last_points",[])
        supports=sorted({round(float(p["price"]),10) for p in pts if p["kind"]=="low" and float(p["price"])<close},reverse=True)[:4]
        resistances=sorted({round(float(p["price"]),10) for p in pts if p["kind"]=="high" and float(p["price"])>close})[:4]
        clusters=fib.get("clusters",[]) if isinstance(fib,dict) else []
        return {
            "ready":True,
            "price":close,
            "atr":atr,
            "atr_pct":None if atr_pct is None else round(atr_pct,5),
            "volatility_regime":"high" if atr_pct and atr_pct>=3 else "normal" if atr_pct and atr_pct>=1 else "low" if atr_pct is not None else "unknown",
            "supports":supports,
            "resistances":resistances,
            "fib_clusters":[c["price"] for c in clusters[:5]],
        }

    def _analysis_bundle(self, candles):
        raw_rows=list(candles)
        rows=[r for r in raw_rows if r.get("confirm", True)]
        technical=self._technical_metrics(rows)
        ctx=self._structure_context_v2(rows)
        liquidity=self._liquidity_metrics(rows)
        fib=self._fib_engine_v2(rows,ctx)
        elliott=self._elliott_engine_v2(rows,ctx,fib)
        harmonics=self._harmonic_engine_v2(ctx)
        divergences=self._divergence_engine_v2(rows,ctx)
        smc=self._smart_money_engine_v2(rows,ctx,liquidity)
        regime=self._regime_levels_v2(rows,technical,ctx["base"],fib)
        confluence=self._confluence_engine_v2(technical,ctx,fib,elliott,harmonics,divergences,liquidity,smc)
        ready=bool(technical.get("ready") and ctx.get("ready"))
        return {
            "ready":ready,
            "engine_version":"scan_plus_v3_7_1",
            "closed_candles":len(rows),
            "excluded_open_candles":max(0,len(raw_rows)-len(rows)),
            "last_confirmed_start":rows[-1].get("start") if rows else None,
            "last_confirmed_close":rows[-1].get("close") if rows else None,
            "pipeline":["technical","structure","fibonacci","elliott","harmonics","divergences","liquidity","smart_money","regime_levels","confluence"],
            "technical":technical,
            "structure":ctx["base"],
            "fibonacci":fib,
            "elliott":elliott,
            "harmonics":harmonics,
            "divergences":divergences,
            "liquidity":liquidity,
            "smart_money":smc,
            "regime_levels":regime,
            "confluence":confluence,
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
            analysis = {i: self._analysis_bundle(c) for i, c in self.candles.items()}
            technical = {i: analysis[i]["technical"] for i in KLINE_INTERVALS}
            structure = {i: analysis[i]["structure"] for i in KLINE_INTERVALS}
            liquidity = {i: analysis[i]["liquidity"] for i in KLINE_INTERVALS}
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
                "analysis": analysis,
                "analysis_full_6_of_6": all(analysis[i]["ready"] for i in KLINE_INTERVALS),
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

                # A reconnect must not make already collected flow "cold".
                # Warmth is based on actual trade-history coverage, not websocket session age.
                "warmup": {
                    name: bool(
                        len(self.trades) >= 2
                        and (self.trades[-1][0] - self.trades[0][0]) >= duration * 0.80
                    )
                    for name, duration in WINDOWS.items()
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

                "analysis": {
                    interval: self._analysis_bundle(candles)
                    for interval, candles in self.candles.items()
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


    @staticmethod
    def _live_structure_context_v33(linear, execution):
        analysis=linear.get("analysis",{})
        price=execution.get("price")
        result={}
        for tf in ("D","240","60","15","5","1"):
            a=analysis.get(tf,{})
            reg=a.get("regime_levels",{})
            structure=a.get("structure",{})
            technical=a.get("technical",{})
            atr=technical.get("atr14") or reg.get("atr")
            last_close=a.get("last_confirmed_close") or technical.get("last_close")
            points=[]
            for degree in structure.get("degrees",{}).values():
                points.extend(degree.get("last_points",[]))
            prices=[float(p["price"]) for p in points if p.get("price") is not None]
            structural_low=min(prices) if prices else None
            structural_high=max(prices) if prices else None

            mode="inside_structure"
            effective_direction=(a.get("confluence") or {}).get("direction","neutral")
            broken_level=None
            structural_gap=None
            if price is not None and structural_low is not None and price < structural_low:
                mode="price_discovery_down"; effective_direction="bearish"; broken_level=structural_low
                structural_gap=structural_low-price
            elif price is not None and structural_high is not None and price > structural_high:
                mode="price_discovery_up"; effective_direction="bullish"; broken_level=structural_high
                structural_gap=price-structural_high
            elif price is not None and structural_low is not None and structural_high is not None:
                structural_gap=0.0

            gap_atr=(structural_gap/atr) if structural_gap is not None and atr else None
            projections=[]
            if structural_low is not None and structural_high is not None:
                width=structural_high-structural_low
                if width>0:
                    ratios=(0.272,0.618,1.0,1.272,1.618,2.0,2.618,3.618)
                    if mode=="price_discovery_down":
                        projections=[round(structural_low-width*r,10) for r in ratios if structural_low-width*r>0]
                    elif mode=="price_discovery_up":
                        projections=[round(structural_high+width*r,10) for r in ratios]
            result[tf]={
                "mode":mode,"effective_direction":effective_direction,
                "structural_low":structural_low,"structural_high":structural_high,
                "broken_level":broken_level,"last_confirmed_close":last_close,
                "price_gap_atr":None if gap_atr is None else round(gap_atr,4),
                "stale_or_dislocated":bool(mode!="inside_structure" and gap_atr is not None and gap_atr>=3.0),
                "projection_targets":projections,
            }
        return result

    @staticmethod
    def _mtf_engine_v3(linear, execution=None):
        analysis=linear.get("analysis",{})
        execution=execution or {}
        live=DynamicMarketManager._live_structure_context_v33(linear,execution)
        order=["D","240","60","15","5","1"]
        weights={"D":5,"240":4,"60":3,"15":2,"5":1,"1":1}
        bull=bear=0
        frames={}
        for tf in order:
            a=analysis.get(tf,{})
            c=a.get("confluence",{})
            raw_direction=c.get("direction","neutral")
            lc=live.get(tf,{})
            direction=lc.get("effective_direction",raw_direction)
            state=a.get("structure",{}).get("state","unknown")
            # A one-vote momentum edge must not flip an established opposite structure.
            # External live structure breaks are still allowed to override immediately.
            if lc.get("mode")=="inside_structure":
                cb=float(c.get("bullish",0) or 0); cr=float(c.get("bearish",0) or 0)
                if state=="downtrend" and direction=="bullish" and (cb-cr)<=1:
                    direction="neutral"
                elif state=="uptrend" and direction=="bearish" and (cr-cb)<=1:
                    direction="neutral"
            w=weights[tf]
            if direction=="bullish": bull+=w
            elif direction=="bearish": bear+=w
            frames[tf]={
                "ready":a.get("ready",False),
                "structure":state,
                "direction":direction,
                "raw_direction":raw_direction,
                "live_mode":lc.get("mode"),
                "price_gap_atr":lc.get("price_gap_atr"),
                "elliott":(a.get("elliott",{}).get("primary") or {}).get("type"),
                "elliott_ambiguous":a.get("elliott",{}).get("ambiguous"),
                "bull":c.get("bullish",0),
                "bear":c.get("bearish",0),
            }
        total=bull+bear
        bias="bullish" if bull>bear else "bearish" if bear>bull else "neutral"
        agreement=(max(bull,bear)/total) if total else 0.0
        higher=[frames[x]["direction"] for x in ("D","240","60") if frames[x]["ready"]]
        lower=[frames[x]["direction"] for x in ("15","5","1") if frames[x]["ready"]]
        def group_consensus(vals):
            b=sum(v=="bullish" for v in vals); r=sum(v=="bearish" for v in vals)
            if b>=2 and b>r: return "bullish"
            if r>=2 and r>b: return "bearish"
            return "neutral"
        higher_consensus=group_consensus(higher); lower_consensus=group_consensus(lower)
        conflict=bool(higher_consensus!="neutral" and lower_consensus!="neutral" and higher_consensus!=lower_consensus)
        return {
            "ready":all(frames[x]["ready"] for x in ("D","240","60","15","5")),
            "bias":bias,
            "weighted_bull":bull,
            "weighted_bear":bear,
            "agreement":round(agreement,4),
            "higher_lower_conflict":conflict,
            "higher_consensus":higher_consensus,
            "lower_consensus":lower_consensus,
            "live_context":live,
            "frames":frames,
        }

    @staticmethod
    def _execution_engine_v3(linear, spot):
        book=linear.get("orderbook",{})
        lf=linear.get("flow",{})
        sf=spot.get("flow",{})
        oi=linear.get("open_interest") or {}
        funding=linear.get("funding_rate")
        ticker=linear.get("ticker",{})
        price=fnum(ticker.get("lastPrice")) or fnum(ticker.get("markPrice"))
        windows={}
        driver_votes={"spot":0.0,"perp":0.0,"mixed":0.0}
        divergence_events=[]

        for w in WINDOWS:
            l=lf.get(w,{})
            s=sf.get(w,{})
            o=(oi.get("windows") or {}).get(w,{})
            pd=l.get("delta_ratio")
            sd=s.get("delta_ratio")
            pp=l.get("price_change_pct")
            sp=s.get("price_change_pct")
            warm=bool(linear.get("warmup",{}).get(w,False) and spot.get("warmup",{}).get(w,False))

            driver="unknown"
            confidence=0.0
            if warm and pd is not None and sd is not None:
                ap,as_=abs(pd),abs(sd)
                if as_ >= ap*1.35 and as_ >= 0.08:
                    driver="spot"
                    confidence=min(1.0,(as_-ap)+0.45)
                    driver_votes["spot"]+=confidence
                elif ap >= as_*1.35 and ap >= 0.08:
                    driver="perp"
                    confidence=min(1.0,(ap-as_)+0.45)
                    driver_votes["perp"]+=confidence
                else:
                    driver="mixed"
                    confidence=min(1.0,max(ap,as_)+0.35)
                    driver_votes["mixed"]+=confidence

                # Flow disagreement / non-confirmation.
                if pd*sd < 0 and abs(pd-sd)>=0.18:
                    divergence_events.append({
                        "window":w,
                        "type":"spot_perp_opposition",
                        "perp_delta_ratio":pd,
                        "spot_delta_ratio":sd,
                    })
                if pp is not None and pp > 0:
                    if pd > 0.08 and sd <= 0.02:
                        divergence_events.append({"window":w,"type":"price_up_perp_led_spot_not_confirming"})
                    elif sd > 0.08 and pd <= 0.02:
                        divergence_events.append({"window":w,"type":"price_up_spot_led_perp_not_confirming"})
                elif pp is not None and pp < 0:
                    if pd < -0.08 and sd >= -0.02:
                        divergence_events.append({"window":w,"type":"price_down_perp_led_spot_not_confirming"})
                    elif sd < -0.08 and pd >= -0.02:
                        divergence_events.append({"window":w,"type":"price_down_spot_led_perp_not_confirming"})

            windows[w]={
                "perp_delta_ratio":pd,
                "spot_delta_ratio":sd,
                "perp_price_change_pct":pp,
                "spot_price_change_pct":sp,
                "oi_change_pct":o.get("change_pct"),
                "linear_warm":linear.get("warmup",{}).get(w,False),
                "spot_warm":spot.get("warmup",{}).get(w,False),
                "driver":driver,
                "driver_confidence":round(confidence,4),
            }

        # Prefer evidence accumulated across fully warmed windows.
        ranked=sorted(driver_votes.items(),key=lambda kv:kv[1],reverse=True)
        overall_driver=ranked[0][0] if ranked and ranked[0][1]>0 else "unknown"
        total_votes=sum(driver_votes.values())
        overall_conf=(ranked[0][1]/total_votes) if total_votes and ranked else 0.0

        imbalance=book.get("imbalance_50") if book.get("ready") else None
        pressure="neutral"
        if imbalance is not None:
            pressure="bullish" if imbalance>=0.12 else "bearish" if imbalance<=-0.12 else "neutral"

        # OI interpretation is context, not an automatic direction signal.
        oi_context="unknown"
        preferred=("5m","15m","1m")
        for w in preferred:
            ow=windows.get(w,{})
            ch=ow.get("oi_change_pct")
            pc=ow.get("perp_price_change_pct")
            if ch is None or pc is None:
                continue
            if ch>0 and pc>0: oi_context="new_longs_or_trend_participation"
            elif ch>0 and pc<0: oi_context="new_shorts_or_bearish_participation"
            elif ch<0 and pc>0: oi_context="short_covering_or_position_reduction"
            elif ch<0 and pc<0: oi_context="long_liquidation_or_position_reduction"
            break

        transport_ready=bool(linear.get("connected") and spot.get("connected") and book.get("ready"))
        warmed_flow_windows=[
            w for w,data in windows.items()
            if data.get("linear_warm") and data.get("spot_warm")
            and data.get("perp_delta_ratio") is not None
            and data.get("spot_delta_ratio") is not None
        ]
        flow_ready=bool(warmed_flow_windows)
        driver_ready=bool(overall_driver!="unknown" and overall_conf>=0.35)
        trade_data_ready=bool(transport_ready and price is not None and flow_ready and driver_ready)

        return {
            "ready":transport_ready,
            "trade_data_ready":trade_data_ready,
            "flow_ready":flow_ready,
            "driver_ready":driver_ready,
            "warmed_flow_windows":warmed_flow_windows,
            "price":price,
            "spread_bps":book.get("spread_bps"),
            "book_imbalance":imbalance,
            "book_pressure":pressure,
            "funding_rate":funding,
            "open_interest_current":oi.get("current"),
            "oi_context":oi_context,
            "driver":{
                "primary":overall_driver,
                "confidence":round(overall_conf,4),
                "votes":{k:round(v,4) for k,v in driver_votes.items()},
            },
            "flow_divergences":divergence_events,
            "windows":windows,
        }

    @staticmethod
    def _setup_engine_v3(linear, mtf, execution):
        analysis=linear.get("analysis",{})
        price=execution.get("price")
        if not price:
            return {"ready":False,"status":"NO_SETUP","reason":"no_realtime_price"}

        bias=mtf.get("bias","neutral")
        agreement=float(mtf.get("agreement") or 0.0)
        execution_dir=execution.get("book_pressure","neutral")
        driver=(execution.get("driver") or {}).get("primary","unknown")
        driver_conf=float((execution.get("driver") or {}).get("confidence") or 0.0)
        flow_divs=execution.get("flow_divergences",[])
        direction=bias if bias in ("bullish","bearish") else "neutral"
        live=mtf.get("live_context",{})

        uncertainty=any(
            analysis.get(tf,{}).get("elliott",{}).get("ambiguous",False)
            for tf in ("240","60","15")
        )

        # Execution ATR is deliberately taken from 5m first. Using the minimum ATR
        # across all TFs can create absurdly tight stops on low-priced assets.
        atr5=(analysis.get("5",{}).get("technical") or {}).get("atr14")
        if atr5 is None:
            atr5=(analysis.get("5",{}).get("regime_levels") or {}).get("atr")
        atr15=(analysis.get("15",{}).get("technical") or {}).get("atr14")
        if atr15 is None:
            atr15=(analysis.get("15",{}).get("regime_levels") or {}).get("atr")
        execution_atr=float(atr5 or atr15 or 0.0) or None

        spread_bps=execution.get("spread_bps")
        spread_abs=(price*float(spread_bps)/10000.0) if spread_bps is not None else None

        # Costs/noise floor. Structural stops are placed BEYOND the level and must
        # also clear normal execution noise. No artificial tiny denominator for R:R.
        level_buffer=max(
            execution_atr*0.25 if execution_atr else 0.0,
            spread_abs*2.0 if spread_abs else 0.0,
        )
        min_risk_distance=max(
            execution_atr*0.75 if execution_atr else 0.0,
            spread_abs*3.0 if spread_abs else 0.0,
        )
        min_reward_distance=max(
            execution_atr*1.00 if execution_atr else 0.0,
            spread_abs*3.0 if spread_abs else 0.0,
        )

        supports=set()
        resistances=set()
        for tf in ("5","15","60","240","D"):
            reg=analysis.get(tf,{}).get("regime_levels",{})
            supports.update(float(x) for x in reg.get("supports",[]) if x is not None)
            resistances.update(float(x) for x in reg.get("resistances",[]) if x is not None)

        target_candidates=[]
        invalid_levels=[]
        target_source=None
        invalid_source=None

        if direction=="bullish":
            target_candidates.extend(x for x in resistances if x>price)
            invalid_levels.extend(x for x in supports if x<price)
            for tf in ("15","60","240","D"):
                lc=live.get(tf,{})
                target_candidates.extend(x for x in lc.get("projection_targets",[]) if x>price)
                broken=lc.get("broken_level")
                if lc.get("mode")=="price_discovery_up" and broken is not None and broken<price:
                    invalid_levels.append(float(broken))
        elif direction=="bearish":
            target_candidates.extend(x for x in supports if x<price)
            invalid_levels.extend(x for x in resistances if x>price)
            for tf in ("15","60","240","D"):
                lc=live.get(tf,{})
                target_candidates.extend(x for x in lc.get("projection_targets",[]) if 0<x<price)
                broken=lc.get("broken_level")
                if lc.get("mode")=="price_discovery_down" and broken is not None and broken>price:
                    invalid_levels.append(float(broken))

        target_candidates=sorted(set(round(x,10) for x in target_candidates))
        invalid_levels=sorted(set(round(x,10) for x in invalid_levels))

        # Candidate target must be meaningfully beyond market noise.
        if direction=="bullish":
            valid_targets=[x for x in target_candidates if (x-price)>=min_reward_distance]
            target=min(valid_targets) if valid_targets else None
            structural_level=max(invalid_levels) if invalid_levels else None
        elif direction=="bearish":
            valid_targets=[x for x in target_candidates if (price-x)>=min_reward_distance]
            target=max(valid_targets) if valid_targets else None
            structural_level=min(invalid_levels) if invalid_levels else None
        else:
            valid_targets=[]
            target=structural_level=None

        # Put invalidation beyond the structural level, then widen it to the minimum
        # ATR/spread noise floor when necessary.
        invalid=None
        raw_invalid=None
        if structural_level is not None:
            if direction=="bullish":
                raw_invalid=structural_level-level_buffer
                invalid=min(raw_invalid, price-min_risk_distance)
            elif direction=="bearish":
                raw_invalid=structural_level+level_buffer
                invalid=max(raw_invalid, price+min_risk_distance)
            invalid_source="buffered_structural_level"
        elif execution_atr and direction=="bullish":
            invalid=price-max(1.5*execution_atr,min_risk_distance)
            invalid_source="atr_fallback"
        elif execution_atr and direction=="bearish":
            invalid=price+max(1.5*execution_atr,min_risk_distance)
            invalid_source="atr_fallback"

        if target is not None:
            in_discovery=any(
                live.get(tf,{}).get("mode") in ("price_discovery_down","price_discovery_up")
                for tf in ("15","60","240","D")
            )
            target_source="structural_projection" if in_discovery else "confirmed_level"

        # Hard price-order invariants.
        price_order_valid=False
        if direction=="bullish" and invalid is not None and target is not None:
            price_order_valid=invalid < price < target
        elif direction=="bearish" and invalid is not None and target is not None:
            price_order_valid=target < price < invalid

        risk=abs(price-invalid) if invalid is not None else None
        reward=abs(target-price) if target is not None else None
        risk_distance_valid=bool(risk is not None and risk+1e-18>=min_risk_distance)
        reward_distance_valid=bool(reward is not None and reward+1e-18>=min_reward_distance)
        rr=(reward/risk) if risk and reward is not None and price_order_valid else None

        # Fresh lower-TF structure may veto a setup against an active execution impulse.
        lower_tfs=("1","5","15")
        opposing_lower=[]
        for tf in lower_tfs:
            frame=(mtf.get("frames") or {}).get(tf,{})
            a=analysis.get(tf,{})
            ev=(a.get("structure") or {}).get("last_event") or {}
            mode=(live.get(tf) or {}).get("mode")
            frame_dir=frame.get("direction")
            active_bull=bool(
                frame_dir=="bullish" and
                (mode=="price_discovery_up" or
                 (ev.get("direction")=="bullish" and ev.get("type") in ("BOS","CHoCH")))
            )
            active_bear=bool(
                frame_dir=="bearish" and
                (mode=="price_discovery_down" or
                 (ev.get("direction")=="bearish" and ev.get("type") in ("BOS","CHoCH")))
            )
            if direction=="bearish" and active_bull:
                opposing_lower.append(tf)
            elif direction=="bullish" and active_bear:
                opposing_lower.append(tf)

        # One noisy 1m event is not enough; two execution TFs opposing the trade is a veto.
        lower_tf_not_opposed=len(opposing_lower)<2

        opposed_flow=False
        for ev in flow_divs:
            typ=ev.get("type","")
            if direction=="bullish" and typ=="price_up_perp_led_spot_not_confirming":
                opposed_flow=True
            if direction=="bearish" and typ=="price_down_perp_led_spot_not_confirming":
                opposed_flow=True

        dislocated=[
            tf for tf in ("D","240","60","15","5")
            if live.get(tf,{}).get("stale_or_dislocated")
        ]

        driver_quality=(
            "strong" if driver=="spot"
            else "normal" if driver=="mixed"
            else "fragile" if driver=="perp"
            else "unknown"
        )

        required={
            "mtf_ready":bool(mtf.get("ready")),
            "directional_bias":direction!="neutral",
            "mtf_agreement_ge_65":agreement>=0.65,
            "higher_lower_not_conflicted":not bool(mtf.get("higher_lower_conflict")),
            "transport_ready":bool(execution.get("ready")),
            "trade_data_ready":bool(execution.get("trade_data_ready")),
            "driver_known":driver!="unknown" and driver_conf>=0.35,
            "invalidation_known":invalid is not None,
            "target_known":target is not None,
            "price_order_valid":price_order_valid,
            "risk_distance_valid":risk_distance_valid,
            "reward_distance_valid":reward_distance_valid,
            "rr_min_1_5":rr is not None and rr>=1.5,
            "execution_not_opposed":execution_dir in ("neutral",direction),
            "lower_tf_not_opposed":lower_tf_not_opposed,
            "flow_not_opposed":not opposed_flow,
            "history_not_severely_dislocated":len(dislocated)<3,
            "elliott_not_fully_ambiguous":not all(
                analysis.get(tf,{}).get("elliott",{}).get("ambiguous",False)
                for tf in ("240","60","15")
            ),
        }
        status="SETUP" if all(required.values()) else "NO_SETUP"

        confirmations={
            "spot_or_mixed_driver":driver in ("spot","mixed"),
            "elliott_clear":not uncertainty,
            "mtf_agreement_ge_75":agreement>=0.75,
            "book_confirms":execution_dir==direction,
        }

        return {
            "ready":True,
            "status":status,
            "direction":direction if direction!="neutral" else None,
            "entry_reference":price,
            "structural_invalidation_level":structural_level,
            "raw_buffered_invalidation":raw_invalid,
            "invalidation":invalid,
            "invalidation_source":invalid_source,
            "target_1":target,
            "target_source":target_source,
            "risk_reward":None if rr is None else round(rr,3),
            "risk_distance":risk,
            "reward_distance":reward,
            "execution_atr":execution_atr,
            "spread_abs":spread_abs,
            "spread_bps":spread_bps,
            "level_buffer":level_buffer,
            "min_risk_distance":min_risk_distance,
            "min_reward_distance":min_reward_distance,
            "opposing_lower_timeframes":opposing_lower,
            "required":required,
            "confirmations":confirmations,
            "confirmation_score":sum(1 for v in confirmations.values() if v),
            "driver":driver,
            "driver_confidence":round(driver_conf,4),
            "driver_quality":driver_quality,
            "flow_warning":opposed_flow,
            "elliott_uncertainty":uncertainty,
            "mtf_agreement":agreement,
            "execution_pressure":execution_dir,
            "dislocated_timeframes":dislocated,
            "note":"Position size is intentionally not calculated here; it requires current account equity and chosen risk percent.",
        }


    # ========================================================
    # SCAN+ V3.7 DECISION ARCHITECTURE
    # Context (D/4H) -> Direction (1H/15m) -> Setup (5m)
    # -> Trigger (1m) -> Execution -> Thesis risk/targets.
    # ========================================================

    @staticmethod
    def _direction_from_frame_v37(a):
        """Direction without allowing live price-discovery to rewrite confirmed TF structure."""
        c=a.get("confluence",{}) or {}
        state=(a.get("structure",{}) or {}).get("state","unknown")
        bull=float(c.get("bullish",0) or 0); bear=float(c.get("bearish",0) or 0)
        raw=c.get("direction","neutral")
        if state=="uptrend" and bull>=bear: return "bullish"
        if state=="downtrend" and bear>=bull: return "bearish"
        if raw in ("bullish","bearish") and abs(bull-bear)>=2: return raw
        return "neutral"

    @staticmethod
    def _signal_freshness_v37(a, tf):
        # TTL is bar-based; expired items remain diagnostic only.
        ttl={"1":8,"5":10,"15":12,"60":14,"240":16,"D":20}.get(tf,10)
        last_start=a.get("last_confirmed_start")
        event=(a.get("structure",{}) or {}).get("last_event") or {}
        event_start=event.get("start") or event.get("at") or event.get("timestamp")
        # Most structure events do not expose a timestamp in older schema. In that
        # case we do not invent age; freshness is unknown rather than expired.
        return {"ttl_bars":ttl,"last_confirmed_start":last_start,
                "structure_event_start":event_start,"structure_event_expired":False if event else None}

    @staticmethod
    def _smc_scenario_v37(a, direction):
        smc=a.get("smart_money",{}) or {}
        liq=a.get("liquidity",{}) or {}
        structure=a.get("structure",{}) or {}
        ev=structure.get("last_event") or {}
        sweep=liq.get("sweep")
        fvgs=[x for x in smc.get("fvg",[]) if not x.get("fully_filled")]
        obs=smc.get("order_blocks",[]) or []
        displacement=bool(ev.get("confirmed_by_close") and ev.get("type") in ("BOS","CHoCH"))
        mss=bool(ev.get("confirmed_by_close") and ev.get("type")=="CHoCH")
        dir_ok=(ev.get("direction")==direction) if ev else False
        stage="idle"
        if sweep: stage="liquidity_sweep"
        if displacement and dir_ok: stage="displacement"
        if mss and dir_ok: stage="mss"
        if (mss or displacement) and dir_ok and fvgs: stage="poi_created"
        return {
            "direction":direction if direction in ("bullish","bearish") else None,
            "stage":stage,"sweep":sweep,"displacement":displacement and dir_ok,
            "mss":mss and dir_ok,"active_fvg_count":len(fvgs),"order_block_count":len(obs),
            "ready_for_retrace":bool(dir_ok and (mss or displacement) and (fvgs or obs)),
        }

    @staticmethod
    def _execution_engine_v37(linear, spot):
        # Preserve V3.6 calculations, but capability-aware transport/flow readiness.
        base=DynamicMarketManager._execution_engine_v3(linear,spot)
        spot_available=spot.get("available")
        # False is authoritative subscription rejection; None means not established yet.
        mode="perp_only" if spot_available is False else "spot_perp"
        windows=base.get("windows",{})
        if mode=="perp_only":
            warmed=[w for w,d in windows.items() if d.get("linear_warm") and d.get("perp_delta_ratio") is not None]
            base["warmed_flow_windows"]=warmed
            base["flow_ready"]=bool(warmed)
            base["driver_ready"]=bool(warmed)
            base["driver"]={"primary":"perp_only" if warmed else "unknown",
                            "confidence":1.0 if warmed else 0.0,"votes":{"perp_only":1.0 if warmed else 0.0}}
            base["ready"]=bool(linear.get("connected") and (linear.get("orderbook") or {}).get("ready"))
            base["trade_data_ready"]=bool(base["ready"] and base.get("price") is not None and warmed)
            base["flow_divergences"]=[]
        base["execution_mode"]=mode
        base["market_capabilities"]={"linear":linear.get("available") is not False,
                                     "spot":spot_available is not False,
                                     "spot_state":spot_available}

        # Rich execution regime; context only, never a direction vote by itself.
        regime="unknown"
        for w in ("1m","5m","15m","1h"):
            d=windows.get(w,{})
            if not d.get("linear_warm"): continue
            pd=d.get("perp_delta_ratio"); sd=d.get("spot_delta_ratio"); oi=d.get("oi_change_pct"); pc=d.get("perp_price_change_pct")
            if pd is None or pc is None: continue
            if mode=="perp_only": regime="perp_only"; break
            if sd is not None and pd*sd<0 and abs(pd-sd)>=0.18: regime="spot_perp_divergence"
            elif pc>0 and pd>0.08 and oi is not None and oi<0: regime="short_covering"
            elif pc<0 and pd<-0.08 and oi is not None and oi<0: regime="long_liquidation"
            elif sd is not None and sd>0.12 and pc>=0: regime="spot_led_accumulation"
            elif sd is not None and sd<-0.12 and pc<=0: regime="spot_led_distribution"
            elif abs(pd)>=0.12 and oi is not None and oi>0: regime="perp_led_expansion"
            elif oi is not None and oi<0: regime="deleveraging"
            else: regime="mixed"
            break
        base["execution_regime"]=regime
        return base

    @staticmethod
    def _mtf_engine_v37(linear, execution):
        analysis=linear.get("analysis",{})
        live=DynamicMarketManager._live_structure_context_v33(linear,execution)
        frames={}
        for tf in ("D","240","60","15","5","1"):
            a=analysis.get(tf,{})
            confirmed=DynamicMarketManager._direction_from_frame_v37(a)
            lc=live.get(tf,{})
            frames[tf]={"ready":a.get("ready",False),"direction":confirmed,
                        "raw_confluence":(a.get("confluence") or {}).get("direction","neutral"),
                        "structure":(a.get("structure") or {}).get("state","unknown"),
                        "live_mode":lc.get("mode"),"price_gap_atr":lc.get("price_gap_atr"),
                        "transition":lc.get("mode") in ("price_discovery_up","price_discovery_down") and
                                     ((lc.get("mode")=="price_discovery_up" and confirmed!="bullish") or
                                      (lc.get("mode")=="price_discovery_down" and confirmed!="bearish"))}
        # D/4H = context only.
        ctx_dirs=[frames[x]["direction"] for x in ("D","240") if frames[x]["ready"]]
        if ctx_dirs.count("bullish")>ctx_dirs.count("bearish"): context_dir="bullish"
        elif ctx_dirs.count("bearish")>ctx_dirs.count("bullish"): context_dir="bearish"
        else: context_dir="neutral"
        # 1H/15m = direction. 55/45, but a neutral frame does not manufacture agreement.
        d60=frames["60"]["direction"]; d15=frames["15"]["direction"]
        score=(0.55 if d60=="bullish" else -0.55 if d60=="bearish" else 0)+(0.45 if d15=="bullish" else -0.45 if d15=="bearish" else 0)
        if d60 in ("bullish","bearish") and d15 in ("bullish","bearish") and d60!=d15:
            direction="neutral"; direction_state="uncertain"
        elif score>=0.45: direction="bullish"; direction_state="confirmed" if d60==d15=="bullish" else "provisional"
        elif score<=-0.45: direction="bearish"; direction_state="confirmed" if d60==d15=="bearish" else "provisional"
        else: direction="neutral"; direction_state="uncertain"
        if direction=="neutral": style="mixed_context"
        elif context_dir=="neutral": style="mixed_context"
        elif context_dir==direction: style="with_context"
        else: style="countertrend"
        setup_dir=frames["5"]["direction"]; trigger_dir=frames["1"]["direction"]
        setup_state="aligned" if direction!="neutral" and setup_dir==direction else "pullback" if direction!="neutral" and setup_dir not in ("neutral",direction) else "forming"
        trigger_state="aligned" if direction!="neutral" and trigger_dir==direction else "opposed" if direction!="neutral" and trigger_dir not in ("neutral",direction) else "waiting"
        return {"ready":all(frames[x]["ready"] for x in ("D","240","60","15","5")),
                "context":{"direction":context_dir,"frames":{"D":frames["D"],"240":frames["240"]}},
                "direction":{"direction":direction,"state":direction_state,"score":round(score,3),"frames":{"60":frames["60"],"15":frames["15"]}},
                "setup_state":setup_state,"trigger_state":trigger_state,"trade_style":style,
                "frames":frames,"live_context":live}

    @staticmethod
    def _trade_engine_v37(linear, mtf, execution):
        """V3.7.1 scenario-coherent trade construction.

        Order is intentionally strict:
        direction -> setup/trigger -> thesis invalidation -> T1/T2 -> RR/scale.
        RR is never used to move the stop or manufacture a farther T1.
        """
        analysis=linear.get("analysis",{}); price=execution.get("price")
        direction=(mtf.get("direction") or {}).get("direction","neutral")
        if not price:
            return {"ready":False,"status":"DATA_BLOCK","trade_state":"DATA_BLOCK","block_class":"DATA_BLOCK","block_reasons":["no_realtime_price"],"retryable":True}

        def reg(tf): return (analysis.get(tf,{}) or {}).get("regime_levels",{}) or {}
        def smc(tf):
            a=analysis.get(tf,{}) or {}
            return a.get("smart_money") or a.get("smc") or {}
        def liq(tf): return (analysis.get(tf,{}) or {}).get("liquidity",{}) or {}
        def add_unique(items, value, tf, source, rank, meta=None):
            try: value=float(value)
            except (TypeError,ValueError): return
            if not math.isfinite(value) or value<=0: return
            key=(round(value,12),tf,source)
            if any(x[5]==key for x in items): return
            items.append((value,tf,source,rank,meta or {},key))

        atr5=((analysis.get("5",{}).get("technical") or {}).get("atr14") or reg("5").get("atr"))
        atr15=((analysis.get("15",{}).get("technical") or {}).get("atr14") or reg("15").get("atr"))
        atr=float(atr5 or atr15 or 0) or None
        spread_bps=execution.get("spread_bps"); spread_abs=price*float(spread_bps)/10000 if spread_bps is not None else 0.0
        noise=max((atr or 0)*0.75,spread_abs*3)
        buffer=max((atr or 0)*0.25,spread_abs*2)

        # ---------- thesis invalidation ----------
        # Prefer the same scale as the setup: 1m trigger -> 5m setup -> 15m direction.
        # HTF levels are fallback only and are later rejected by the scale gate if unsuitable.
        invalid=[]
        inv_rank={"1":0,"5":1,"15":2,"60":4,"240":6,"D":7}
        for tf in ("1","5","15","60","240","D"):
            r=reg(tf); m=smc(tf); q=liq(tf); rank=inv_rank[tf]
            levels=r.get("supports",[]) if direction=="bullish" else r.get("resistances",[])
            for x in levels:
                if (direction=="bullish" and x<price) or (direction=="bearish" and x>price):
                    add_unique(invalid,x,tf,"structural_swing",rank,{"thesis":"structure"})
            # Order block boundary is a thesis level only when it is on the invalidation side.
            for ob in m.get("order_blocks",[]) or []:
                if ob.get("direction")!=direction: continue
                x=ob.get("low") if direction=="bullish" else ob.get("high")
                if x and ((direction=="bullish" and x<price) or (direction=="bearish" and x>price)):
                    add_unique(invalid,x,tf,"order_block",rank-0.15,{"start":ob.get("start")})
            sw=q.get("sweep") or {}
            sx=sw.get("price") or sw.get("level")
            if sx and ((direction=="bullish" and sx<price) or (direction=="bearish" and sx>price)):
                add_unique(invalid,sx,tf,"liquidity_sweep_extreme",rank-0.25,{"sweep":sw})

        # Rank by scenario scale first, then nearest valid level inside that scale.
        invalid.sort(key=lambda x:(x[3],abs(x[0]-price)))
        inv=invalid[0] if invalid else None
        logical=inv[0] if inv else None
        if logical is not None:
            stop=(logical-buffer) if direction=="bullish" else (logical+buffer)
            if direction=="bullish": stop=min(stop,price-noise)
            elif direction=="bearish": stop=max(stop,price+noise)
            inv_source=f"{inv[2]}_{inv[1]}"; inv_tf=inv[1]
        elif atr and direction in ("bullish","bearish"):
            stop=price-1.5*atr if direction=="bullish" else price+1.5*atr
            inv_source="atr_fallback"; inv_tf="5"
        else:
            stop=None; inv_source=None; inv_tf=None

        # ---------- scenario targets ----------
        # T1 must be a realistic objective on the same intraday scale. T2 may be farther.
        targets=[]
        target_rank={"1":0,"5":1,"15":2,"60":3,"240":5,"D":6}
        for tf in ("1","5","15","60","240","D"):
            r=reg(tf); m=smc(tf); q=liq(tf); rank=target_rank[tf]
            levels=r.get("resistances",[]) if direction=="bullish" else r.get("supports",[])
            for x in levels:
                if (direction=="bullish" and x>price) or (direction=="bearish" and x<price):
                    add_unique(targets,x,tf,"previous_swing",rank,{"thesis":"structure"})
            # External/equal liquidity is a preferred target when it lies ahead of price.
            eq=q.get("equal_highs") if direction=="bullish" else q.get("equal_lows")
            if eq:
                x=eq.get("price")
                if x and ((direction=="bullish" and x>price) or (direction=="bearish" and x<price)):
                    add_unique(targets,x,tf,"external_liquidity",rank-0.25,{"liquidity":eq})
            # Unfilled FVG can be a magnet/target; use nearest boundary beyond price.
            for f in m.get("fvg",[]) or []:
                if f.get("fully_filled"): continue
                vals=[v for v in (f.get("from"),f.get("to")) if isinstance(v,(int,float))]
                ahead=[v for v in vals if (direction=="bullish" and v>price) or (direction=="bearish" and v<price)]
                if ahead:
                    x=min(ahead) if direction=="bullish" else max(ahead)
                    add_unique(targets,x,tf,"fvg",rank+0.1,{"start":f.get("start"),"type":f.get("type")})

        # Price-discovery projection is fallback and never confirms direction.
        for tf in ("15","60","240","D"):
            lc=(mtf.get("live_context") or {}).get(tf,{})
            for x in lc.get("projection_targets",[]):
                if (direction=="bullish" and x>price) or (direction=="bearish" and 0<x<price):
                    add_unique(targets,x,tf,"projection",8,{"live_mode":lc.get("mode")})

        min_target_distance=max(atr or 0,spread_abs*3)
        targets=[x for x in targets if abs(x[0]-price)>=min_target_distance]
        targets.sort(key=lambda x:(x[3],abs(x[0]-price)))
        target1=targets[0] if targets else None
        # T2 must be materially beyond T1 and may use a higher scale.
        target2=next((x for x in targets[1:] if target1 and abs(x[0]-price)>=abs(target1[0]-price)*1.25),None)
        t1=target1[0] if target1 else None; t2=target2[0] if target2 else None

        order_ok=bool((direction=="bullish" and stop is not None and t1 is not None and stop<price<t1) or (direction=="bearish" and stop is not None and t1 is not None and t1<price<stop))
        risk=abs(price-stop) if stop is not None else None; reward=abs(t1-price) if t1 is not None else None
        rr=reward/risk if order_ok and risk else None
        stop_pct=(risk/price*100) if risk else None; target_pct=(reward/price*100) if reward else None; stop_atr=(risk/atr) if risk and atr else None

        # Scale coherence: percentage + ATR + source timeframe. Never tighten a logical stop just to pass.
        intraday_inv_tf=inv_tf in ("1","5","15","60") if inv_tf else False
        scale_ok=bool(risk and (stop_atr is None or stop_atr<=6.0) and (stop_pct is None or stop_pct<=12.0) and (intraday_inv_tf or inv_source=="atr_fallback"))
        scale_reason=None
        if risk and not scale_ok:
            if inv_tf in ("240","D"): scale_reason="invalidation_from_htf"
            elif stop_atr is not None and stop_atr>6.0: scale_reason="stop_too_many_atr"
            elif stop_pct is not None and stop_pct>12.0: scale_reason="stop_pct_too_large"
            else: scale_reason="scale_mismatch"

        dislocated=[tf for tf in ("15","5","1") if (mtf.get("live_context") or {}).get(tf,{}).get("stale_or_dislocated")]
        extreme_extension=len(dislocated)>=2

        # Setup/trigger are timing gates, not direction votes.
        setup_state=mtf.get("setup_state"); trigger_state=mtf.get("trigger_state")
        smc5=DynamicMarketManager._smc_scenario_v37(analysis.get("5",{}),direction)
        smc1=DynamicMarketManager._smc_scenario_v37(analysis.get("1",{}),direction)
        trigger_ok=(trigger_state=="aligned" and (smc1.get("mss") or smc1.get("displacement") or setup_state=="aligned"))

        flow_divs=execution.get("flow_divergences",[]) or []
        flow_opposed=any((direction=="bullish" and e.get("type")=="price_up_perp_led_spot_not_confirming") or (direction=="bearish" and e.get("type")=="price_down_perp_led_spot_not_confirming") for e in flow_divs)
        book=execution.get("book_pressure","neutral"); execution_ok=book in ("neutral",direction)

        data_reasons=[]; market_reasons=[]; exec_reasons=[]
        if not mtf.get("ready"): data_reasons.append("mtf_not_ready")
        if not execution.get("ready"): data_reasons.append("transport_not_ready")
        if not execution.get("trade_data_ready"): data_reasons.append("trade_data_not_ready")
        if direction=="neutral": market_reasons.append("direction_not_confirmed")
        if (mtf.get("direction") or {}).get("state")!="confirmed": market_reasons.append("direction_provisional")
        if extreme_extension: market_reasons.append("extended_wait_pullback")
        if stop is None: market_reasons.append("invalidation_missing")
        if t1 is None: market_reasons.append("target_missing")
        if not order_ok: market_reasons.append("price_order_invalid")
        if rr is None or rr<1.5: market_reasons.append("rr_below_1_5")
        if not scale_ok: market_reasons.append(scale_reason or "scale_mismatch")
        if setup_state=="pullback": exec_reasons.append("setup_pullback_active")
        elif setup_state!="aligned": exec_reasons.append("setup_not_aligned")
        if not trigger_ok: exec_reasons.append("trigger_not_confirmed")
        if flow_opposed: exec_reasons.append("flow_opposed")
        if not execution_ok: exec_reasons.append("book_opposed")

        if data_reasons:
            state="DATA_BLOCK"; block_class="DATA_BLOCK"; retry=True
        elif direction=="neutral":
            state="WAIT_DIRECTION"; block_class="MARKET_BLOCK"; retry=True
        elif extreme_extension or setup_state=="pullback":
            state="WAIT_PULLBACK"; block_class="EXECUTION_BLOCK"; retry=True
        elif setup_state!="aligned":
            state="WAIT_SETUP"; block_class="EXECUTION_BLOCK"; retry=True
        elif not trigger_ok:
            state="WAIT_TRIGGER"; block_class="EXECUTION_BLOCK"; retry=True
        elif flow_opposed or not execution_ok:
            state="WAIT_FLOW"; block_class="EXECUTION_BLOCK"; retry=True
        elif market_reasons:
            state="INVALID"; block_class="MARKET_BLOCK"; retry=False
        else:
            state="SETUP"; block_class=None; retry=False

        reasons=data_reasons+market_reasons+exec_reasons
        return {
            "ready":True,"status":state,"trade_state":state,"direction":direction if direction!="neutral" else None,
            "entry_reference":price,
            "invalidation":stop,
            "invalidation_thesis":{"logical_level":logical,"source":inv_source,"source_timeframe":inv_tf,"buffer":buffer,"noise_floor":noise,"rule":"stop_beyond_thesis_level_then_noise_buffer"},
            "targets":{
                "t1":t1,"t1_source":None if not target1 else f"{target1[2]}_{target1[1]}","t1_timeframe":None if not target1 else target1[1],
                "t2":t2,"t2_source":None if not target2 else f"{target2[2]}_{target2[1]}","t2_timeframe":None if not target2 else target2[1],
                "rr_basis":"t1","rule":"nearest_valid_scenario_target_same_intraday_scale_first"
            },
            "risk_reward":None if rr is None else round(rr,3),"risk_distance":risk,"reward_distance":reward,
            "trade_scale":{"stop_pct":None if stop_pct is None else round(stop_pct,4),"target_pct":None if target_pct is None else round(target_pct,4),"stop_atr":None if stop_atr is None else round(stop_atr,3),"valid":scale_ok,"reason":scale_reason,"execution_atr":atr,"invalidation_timeframe":inv_tf},
            "scenario_coherence":{"direction":direction,"setup_timeframe":"5","trigger_timeframe":"1","invalidation_timeframe":inv_tf,"t1_timeframe":None if not target1 else target1[1],"rr_uses_t1":True,"stop_moved_for_rr":False},
            "smc_scenario":{"5m":smc5,"1m":smc1},"block_class":block_class,"block_reasons":reasons,"retryable":retry,
            "setup_state":setup_state,"trigger_state":trigger_state,"trade_style":mtf.get("trade_style"),
            "execution_regime":execution.get("execution_regime"),"dislocated_execution_timeframes":dislocated,
            "required":{"direction_confirmed":direction!="neutral","transport_ready":bool(execution.get("ready")),"trade_data_ready":bool(execution.get("trade_data_ready")),"price_order_valid":order_ok,"rr_min_1_5":rr is not None and rr>=1.5,"trade_scale_valid":scale_ok,"setup_aligned":setup_state=="aligned","trigger_confirmed":trigger_ok,"flow_not_opposed":not flow_opposed,"execution_not_opposed":execution_ok}
        }

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

        execution = self._execution_engine_v37(linear, spot)
        mtf = self._mtf_engine_v37(linear, execution)
        setup = self._trade_engine_v37(linear, mtf, execution)

        return {
            "symbol": symbol,
            "generated_at": time.time(),
            "engine_version": "scan_plus_v3_7_1",
            "linear": linear,
            "spot": spot,
            "driver": self._driver(linear, spot),
            "scan_plus": {
                "mtf": mtf,
                "execution": execution,
                "setup": setup,
            },
        }

    def scan(self, symbol):
        full=self.snapshot(symbol)
        linear=full.get("linear",{})
        analysis=linear.get("analysis",{})
        sp=full.get("scan_plus",{})
        mtf=sp.get("mtf",{})
        execution=sp.get("execution",{})
        setup=sp.get("setup",{})

        tf_summary={}
        for tf in ("D","240","60","15","5","1"):
            a=analysis.get(tf,{})
            ell=a.get("elliott",{})
            harm=a.get("harmonics",{})
            div=a.get("divergences",{})
            reg=a.get("regime_levels",{})
            smc=a.get("smart_money",{})
            liq=a.get("liquidity",{})
            tf_summary[tf]={
                "ready":a.get("ready",False),
                "structure":{
                    "state":a.get("structure",{}).get("state"),
                    "phase":a.get("structure",{}).get("phase"),
                    "event":a.get("structure",{}).get("last_event"),
                    "overlap":a.get("structure",{}).get("overlap"),
                },
                "elliott":{
                    "primary":ell.get("primary"),
                    "alternatives":ell.get("alternatives",[])[:2],
                    "ambiguous":ell.get("ambiguous",False),
                },
                "fib_clusters":(a.get("fibonacci",{}).get("clusters") or [])[:5],
                "harmonics":{
                    "confirmed":harm.get("confirmed",[])[:3],
                    "developing":harm.get("developing",[])[:2],
                },
                "divergences":{
                    "consensus":div.get("consensus",{}),
                    "events":div.get("events",[])[:5],
                },
                "levels":{
                    "supports":reg.get("supports",[])[:4],
                    "resistances":reg.get("resistances",[])[:4],
                    "atr":reg.get("atr"),
                    "atr_pct":reg.get("atr_pct"),
                    "volatility_regime":reg.get("volatility_regime"),
                },
                "liquidity":{
                    "sweep":liq.get("sweep"),
                    "equal_highs":liq.get("equal_highs"),
                    "equal_lows":liq.get("equal_lows"),
                },
                "smc":{
                    "dealing_range":smc.get("dealing_range"),
                    "fvg":smc.get("fvg",[])[:4],
                    "order_blocks":smc.get("order_blocks",[])[:3],
                },
                "confluence":a.get("confluence",{}),
                "signal_freshness":self._signal_freshness_v37(a,tf),
            }

        failed=[k for k,v in setup.get("required",{}).items() if not v]
        return {
            "symbol":full.get("symbol"),
            "generated_at":full.get("generated_at"),
            "engine_version":full.get("engine_version"),
            "price":execution.get("price"),
            "status":{
                "linear_connected":linear.get("connected"),
                "spot_connected":full.get("spot",{}).get("connected"),
                "execution_ready":execution.get("ready"),
                "trade_data_ready":execution.get("trade_data_ready"),
                "flow_ready":execution.get("flow_ready"),
                "driver_ready":execution.get("driver_ready"),
                "historical_ready":all(analysis.get(tf,{}).get("ready",False) for tf in ("D","240","60","15","5")),
            },
            "mtf":mtf,
            "context":mtf.get("context"),
            "direction":mtf.get("direction"),
            "setup_state":setup.get("setup_state"),
            "trigger_state":setup.get("trigger_state"),
            "trade_state":setup.get("trade_state"),
            "trade_style":setup.get("trade_style"),
            "execution_mode":execution.get("execution_mode"),
            "market_capabilities":execution.get("market_capabilities"),
            "execution_regime":execution.get("execution_regime"),
            "block_class":setup.get("block_class"),
            "block_reasons":setup.get("block_reasons",[]),
            "retryable":setup.get("retryable"),
            "timeframes":tf_summary,
            "execution":{
                "driver":execution.get("driver"),
                "execution_mode":execution.get("execution_mode"),
                "execution_regime":execution.get("execution_regime"),
                "market_capabilities":execution.get("market_capabilities"),
                "trade_data_ready":execution.get("trade_data_ready"),
                "flow_ready":execution.get("flow_ready"),
                "driver_ready":execution.get("driver_ready"),
                "warmed_flow_windows":execution.get("warmed_flow_windows",[]),
                "flow_divergences":execution.get("flow_divergences",[]),
                "oi_context":execution.get("oi_context"),
                "funding_rate":execution.get("funding_rate"),
                "book_pressure":execution.get("book_pressure"),
                "book_imbalance":execution.get("book_imbalance"),
                "spread_bps":execution.get("spread_bps"),
                "windows":execution.get("windows",{}),
            },
            "setup":{
                **setup,
                "failed_requirements":failed,
            },
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
                "realtime_pass": bool(linear["realtime_pass"] and spot["realtime_pass"]),
                "historical_analysis_pass": bool(linear.get("analysis_full_6_of_6")),
                "linear_technical_pass": bool(linear["full_technical_pass"]),
                "spot_realtime_only": True,
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
