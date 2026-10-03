from scan_architecture import MIN_RR

SCAN_ARCHITECTURE_VERSION = "scan_architecture_v2"
SCAN_PIPELINE = ("market","data_quality","regime_structure","event","evidence","setup","execution")
OPPORTUNITY_STATES = ("MARKET_READY","LIMIT_READY","WATCH")

import csv
import concurrent.futures
import datetime as dt
import io
import json
import math
import os
import re
import urllib.error
import urllib.request
import urllib.parse
import zipfile
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

import websocket
from pump_exhaustion import detect as detect_pump_exhaustion
from scan_intelligence import enrich_scan, relative_strength
from risk_engine import exposure_cluster, exposure_buckets


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


def _scan_log(event, **payload):
    """Emit machine-readable Scan lifecycle records to Render logs."""
    record = {"event": event, **payload}
    try:
        print("SCAN_EVENT " + json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str), flush=True)
    except Exception:
        pass


def fnum(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ============================================================
# ONE MARKET STREAM
# ============================================================

KLINE_INTERVALS = ("1", "5", "15", "60", "240", "D")
KLINE_LIMIT = 2000
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
BINANCE_SEED_MONTHS = 72
PERSISTENCE_SEED_TARGET = 250


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
        # Capability is independent from transport connectivity.
        # unknown -> subscribed -> active; explicit unsupported is authoritative.
        self.capability_state = "unknown"

        self.last_error = None
        self.last_message_at = None

        self.session_id = None
        self.session_started_at = None
        # Monotonic lifetime of this symbol collector across websocket reconnects.
        # Used for liquidity sufficiency decisions; session age resets on reconnect.
        self.collector_started_at = None

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
        self.orderbook_updated_at = None

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
            if self.collector_started_at is None:
                self.collector_started_at = time.time()

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

                # Explicit unsupported is a stable capability result, not a reason
                # to hammer the websocket endpoint in a zero-delay reconnect loop.
                with self.lock:
                    unsupported = self.available is False and self.capability_state == "unsupported"
                if unsupported:
                    break

                backoff = 1
                # A clean remote close is still a reconnect event; throttle it.
                if not self.stop_event.wait(backoff):
                    backoff = min(backoff * 2, 30)

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
            self.status = "unavailable" if self.available is False and self.capability_state == "unsupported" else "stopped"

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
            # A websocket connection only proves transport availability, not that
            # this symbol/topic exists on the selected market.
            if self.available is not False:
                self.available = None
                self.capability_state = "unknown"

            self.status = "connected"
            self.last_error = None

            self.session_id = str(uuid.uuid4())

            self.session_started_at = time.time()
            self.last_message_at = time.time()

            # Preserve rolling trades/OI across reconnects. A transport reconnect must
            # not erase still-fresh market evidence. _cleanup() expires it by timestamp.
            # Only transport-local state is reset here.
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

                # Subscription acknowledgement establishes capability separately
                # from transport connectivity.
                if message.get("op") == "subscribe" and message.get("success") is True:
                    with self.lock:
                        self.available = True
                        self.capability_state = "subscribed"
                        self.status = "subscribed"
                        self.last_error = None
                    continue

                # Subscription rejected. Useful for coins without spot market.
                if (
                    message.get("op") == "subscribe"
                    and message.get("success") is False
                ):

                    reason = (
                        message.get("ret_msg")
                        or message.get("retMsg")
                        or "subscription rejected"
                    )

                    reason_l = str(reason).lower()
                    # Only an explicit symbol/topic-not-supported rejection is an
                    # authoritative capability result. Rate limits, server errors,
                    # auth/transport glitches and generic rejects are transient.
                    unsupported_tokens = (
                        "symbol is invalid", "invalid symbol", "symbol not found",
                        "does not exist", "not supported", "unsupported",
                        "topic is invalid", "invalid topic", "unknown symbol",
                    )
                    explicitly_unsupported = any(t in reason_l for t in unsupported_tokens)
                    with self.lock:
                        self.available = False if explicitly_unsupported else None
                        self.capability_state = "unsupported" if explicitly_unsupported else "transient_unavailable"
                        self.status = "unavailable" if explicitly_unsupported else "reconnecting"
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

    @staticmethod
    def _sanitize_candle(row, interval, default_source="bybit_ws"):
        """Canonical candle validator used by disk restore and live WS ingestion."""
        if not isinstance(row, dict) or interval not in KLINE_INTERVALS:
            return None
        try:
            start = int(row["start"]); end = int(row["end"])
            values = {k: float(row[k]) for k in ("open", "high", "low", "close", "volume")}
            turnover = float(row.get("turnover", 0.0) or 0.0)
        except (KeyError, TypeError, ValueError, OverflowError):
            return None
        if start <= 0 or end < start:
            return None
        expected_end = MarketStream._candle_end_ms(start, interval)
        if abs(end - expected_end) > 1000:
            return None
        if not all(math.isfinite(v) for v in values.values()) or not math.isfinite(turnover):
            return None
        o, h, l, cl = values["open"], values["high"], values["low"], values["close"]
        if min(o, h, l, cl) <= 0 or h < max(o, cl) or l > min(o, cl) or h < l or values["volume"] < 0 or turnover < 0:
            return None
        return {
            "start": start, "end": end, "open": o, "high": h, "low": l, "close": cl,
            "volume": values["volume"], "turnover": turnover,
            "confirm": bool(row.get("confirm", True)),
            "source": str(row.get("source", default_source) or default_source),
        }

    @staticmethod
    def _merge_candle_rows(existing, incoming, interval):
        """Deduplicate by start; native Bybit data wins over archive seed."""
        priority = {"bybit_ws": 3, "bybit_rest": 3, "binance_seed": 1}
        merged = {int(x["start"]): dict(x) for x in existing if x.get("start") is not None}
        for raw in incoming:
            candle = MarketStream._sanitize_candle(raw, interval)
            if candle is None:
                continue
            key = candle["start"]; old = merged.get(key)
            if old is None or priority.get(candle["source"], 2) >= priority.get(str(old.get("source")), 2):
                merged[key] = candle
        return sorted(merged.values(), key=lambda x: x["start"])[-KLINE_LIMIT:]

    def _load_candle_cache(self):
        path = self._cache_path()
        try:
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            loaded = rejected = 0
            for interval in KLINE_INTERVALS:
                rows = payload.get("candles", {}).get(interval, [])
                clean = []
                for row in rows:
                    candle = self._sanitize_candle(row, interval)
                    if candle is None:
                        rejected += 1
                    else:
                        clean.append(candle)
                clean = self._merge_candle_rows([], clean, interval)
                self.candles[interval].extend(clean)
                loaded += len(clean)
            if loaded:
                self.history_bootstrapped = True
                self.history_loaded_at = time.time()
                self.history_error = None if not rejected else f"cache load rejected {rejected} malformed candles"
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
            if len(self.candles[interval]) < PERSISTENCE_SEED_TARGET:
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
                self._merge_seed(interval, seed_rows[-PERSISTENCE_SEED_TARGET:])

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
                "version": 2,
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
            direction="bearish" if leg_a<0 else "bullish"
            common={"direction":direction,"degree":degree,
                    "points":[{"wave":w,**x} for w,x in zip(("0","A","B","C"),s)],
                    "ratios":{"B_A":round(br,4),"C_A":round(cr,4)},"invalidation":p[0]}
            add({**common,"type":"zigzag","checks":{
                "hard_B_below_A_origin":br < 1.0,
                "hard_C_progresses": (p[3]<p[1]) if direction=="bearish" else (p[3]>p[1]),
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

    # ========================================================
    # SCAN+ V3.8 ANALYTICAL DEPTH
    # Facts flow forward. Later blocks may interpret, never rewrite.
    # ========================================================

    @staticmethod
    def _age_bars_v38(rows, start):
        if start is None: return None
        for i,r in enumerate(rows):
            if r.get("start")==start: return max(0,len(rows)-1-i)
        return None

    @staticmethod
    def _fresh_v38(age, ttl):
        # Unknown age is never actionable freshness. This prevents orphaned/stale
        # events from silently surviving a cache refresh or pivot rebuild.
        return age is not None and age <= ttl

    @staticmethod
    def _tf_from_rows_v382(rows):
        if len(rows) < 2:
            return "unknown"
        diffs=[]
        for a,b in zip(rows[-12:-1], rows[-11:]):
            try:
                d=int(b.get("start"))-int(a.get("start"))
                if d>0: diffs.append(d)
            except (TypeError,ValueError):
                pass
        if not diffs: return "unknown"
        diffs.sort(); d=diffs[len(diffs)//2]
        minute=60_000
        if d <= 2*minute: return "1"
        if d <= 7*minute: return "5"
        if d <= 20*minute: return "15"
        if d <= 90*minute: return "60"
        if d <= 360*minute: return "240"
        return "D"

    @classmethod
    def _ttl_bars_v382(cls, rows, event_type="structure"):
        tf=cls._tf_from_rows_v382(rows)
        # Bar-based TTLs deliberately expand with structural timeframe.
        maps={
            "structure":{"1":8,"5":10,"15":12,"60":16,"240":20,"D":24,"unknown":12},
            "divergence":{"1":8,"5":10,"15":12,"60":14,"240":16,"D":18,"unknown":12},
            "liquidity":{"1":8,"5":10,"15":12,"60":14,"240":16,"D":18,"unknown":12},
        }
        return maps.get(event_type,maps["structure"]).get(tf,12)

    def _technical_depth_v38(self, rows, base):
        if not rows: return {**base,"depth_ready":False}
        closes=[float(r["close"]) for r in rows]; atr=base.get("atr14")
        price=closes[-1]; atr_pct=(atr/price*100) if atr and price else None
        def slope(vals,n=5):
            if len(vals)<n: return None
            a,b=vals[-n],vals[-1]
            return (b-a)/(n-1)
        e20=self._ema_series(closes,20); e50=self._ema_series(closes,50); e200=self._ema_series(closes,200)
        bodies=[abs(float(r["close"])-float(r["open"])) for r in rows[-30:]]
        ranges=[float(r["high"])-float(r["low"]) for r in rows[-30:]]
        body_ratio=sum(bodies)/sum(ranges) if sum(ranges)>0 else None
        recent=(sum(ranges[-5:])/5) if len(ranges)>=5 else None
        prior=(sum(ranges[-20:-5])/15) if len(ranges)>=20 else None
        expansion=(recent/prior) if recent and prior else None
        sep=None
        if e20 and e50 and e20[-1] is not None and e50[-1] is not None and atr:
            sep=abs(e20[-1]-e50[-1])/atr
        vol_state="unknown"
        if atr_pct is not None:
            vol_state="compressed" if expansion is not None and expansion<0.7 else "expanded" if expansion is not None and expansion>1.5 else "normal"
        trend_strength="unknown"
        if sep is not None: trend_strength="strong" if sep>=1 else "moderate" if sep>=0.4 else "weak"
        momentum="neutral"; rsi=base.get("rsi14"); macd=base.get("macd") or {}
        if rsi is not None and macd.get("histogram") is not None:
            if rsi>=55 and macd["histogram"]>0: momentum="bullish"
            elif rsi<=45 and macd["histogram"]<0: momentum="bearish"
        return {**base,"depth_ready":True,"atr_pct":atr_pct,"volatility_state":vol_state,
                "range_expansion":None if expansion is None else round(expansion,4),"body_efficiency":None if body_ratio is None else round(body_ratio,4),
                "ema20_slope":slope([x for x in e20 if x is not None]),"ema50_slope":slope([x for x in e50 if x is not None]),
                "ema_separation_atr":None if sep is None else round(sep,4),"trend_strength":trend_strength,"momentum_state":momentum}

    def _event_lifecycle_v381(self, rows, event, ttl_bars, invalidated=False, invalidation_reason=None):
        """Shared bar-based lifecycle. Never fabricates freshness when the event cannot be located."""
        if not isinstance(event, dict) or not event:
            return {"created_at":None,"confirmed_at":None,"age_bars":None,"ttl_bars":ttl_bars,
                    "freshness":"missing","fresh":False,"expired":False,"invalidated":False,"invalidation_reason":None}
        created=event.get("start") or event.get("created_at") or event.get("at") or event.get("timestamp")
        confirmed=event.get("confirmed_at") or created
        age=self._age_bars_v38(rows, created)
        expired=(age is not None and age>ttl_bars)
        fresh=(age is not None and not expired and not invalidated)
        return {"created_at":created,"confirmed_at":confirmed,"age_bars":age,"ttl_bars":ttl_bars,
                "freshness":"invalidated" if invalidated else "expired" if expired else "fresh" if fresh else "unknown",
                "fresh":fresh,"expired":expired,"invalidated":bool(invalidated),"invalidation_reason":invalidation_reason}

    def _structure_depth_v38(self, rows, ctx):
        base=ctx["base"]; degree=base.get("working_degree","intermediate")
        pts=ctx.get("degrees",{}).get(degree,{}).get("points",[]); close=float(rows[-1]["close"]) if rows else None
        highs=[p for p in pts if p["kind"]=="high"]; lows=[p for p in pts if p["kind"]=="low"]
        ev=base.get("last_event") or {}
        # Protected level is tied to the swing that precedes the confirmed structural break,
        # not merely to the last same-side pivot in the current trend.
        protected_low=protected_high=None
        if ev.get("confirmed_by_close") and ev.get("start") is not None:
            prior=[p for p in pts if p.get("start",0)<ev.get("start",0)]
            if ev.get("direction")=="bullish":
                cand=[p for p in prior if p.get("kind")=="low"]
                protected_low=cand[-1] if cand else None
            elif ev.get("direction")=="bearish":
                cand=[p for p in prior if p.get("kind")=="high"]
                protected_high=cand[-1] if cand else None
        if protected_low is None and base.get("state")=="uptrend" and lows: protected_low=lows[-1]
        if protected_high is None and base.get("state")=="downtrend" and highs: protected_high=highs[-1]
        if highs and lows:
            recent=pts[-8:]; lo=min(float(p["price"]) for p in recent); hi=max(float(p["price"]) for p in recent)
            structural_range={"low":lo,"high":hi,"equilibrium":(lo+hi)/2,"position":None if close is None or hi==lo else round((close-lo)/(hi-lo),4)}
        else: structural_range=None
        nested={}
        for d in ("minor","intermediate","major"):
            ps=ctx.get("degrees",{}).get(d,{}).get("points",[]); hs=[p for p in ps if p["kind"]=="high"]; ls=[p for p in ps if p["kind"]=="low"]
            st="range_or_transition"
            if len(hs)>=2 and len(ls)>=2:
                if hs[-1]["price"]>hs[-2]["price"] and ls[-1]["price"]>ls[-2]["price"]: st="uptrend"
                elif hs[-1]["price"]<hs[-2]["price"] and ls[-1]["price"]<ls[-2]["price"]: st="downtrend"
            nested[d]={"state":st,"last_high":hs[-1] if hs else None,"last_low":ls[-1] if ls else None}
        life=self._event_lifecycle_v381(rows,ev,self._ttl_bars_v382(rows,"structure"))
        transition=bool(ev.get("type")=="CHOCH" and ev.get("confirmed_by_close") and life.get("fresh"))
        return {**base,"protected_high":protected_high,"protected_low":protected_low,"structural_range":structural_range,
                "nested":nested,"transition":transition,"event_lifecycle":life}

    def _fib_depth_v38(self, rows, ctx, base):
        levels=[]; atr=self._atr(rows); price=float(rows[-1]["close"]) if rows else None
        for degree,d in ctx.get("degrees",{}).items():
            ps=d.get("points",[])
            for a,b in list(zip(ps[:-1],ps[1:]))[-4:]:
                x,y=float(a["price"]),float(b["price"]); move=y-x
                if abs(move)<1e-12: continue
                for typ,ratios in (("retracement",(0.382,0.5,0.618,0.705,0.786,0.886)),("extension",(1.272,1.414,1.618,2.0,2.618))):
                    for r in ratios:
                        v=y-move*r if typ=="retracement" else x+move*r
                        levels.append({"price":v,"ratio":r,"type":typ,"degree":degree,"anchor_from":a["start"],"anchor_to":b["start"]})
        tol=(atr*0.2) if atr else (price*0.001 if price else 0)
        clusters=[]
        for l in sorted(levels,key=lambda z:z["price"]):
            if clusters and abs(l["price"]-clusters[-1]["price"])<=tol:
                c=clusters[-1]; c["members"].append(l); c["price"]=sum(x["price"] for x in c["members"])/len(c["members"])
            else: clusters.append({"price":l["price"],"members":[l]})
        out=[]
        for c in clusters:
            degrees={m["degree"] for m in c["members"]}
            anchors={(m.get("degree"),m.get("anchor_from"),m.get("anchor_to")) for m in c["members"]}
            # Nearby ratios from one leg are one measurement, not independent confluence.
            if len(anchors)>=2:
                out.append({"price":round(c["price"],10),"count":len(c["members"]),"independent_anchor_count":len(anchors),
                            "degrees":sorted(degrees),"multi_degree":len(degrees)>1,
                            "distance_atr":None if not atr or price is None else round(abs(c["price"]-price)/atr,3),"members":c["members"][:8]})
        out.sort(key=lambda c:(not c["multi_degree"],-c["count"],c["distance_atr"] if c["distance_atr"] is not None else 999))
        return {**base,"depth_clusters":out[:10],"level_count":len(levels)}

    def _elliott_depth_v38(self, rows, ctx, fib, base):
        # Preserve hard-rule candidates; enrich them, never move structure pivots.
        cands=[]
        for c in ([base.get("primary")] + list(base.get("alternatives",[]))):
            if not c: continue
            q=dict(c); pts=q.get("points",[]); prices=[float(p["price"]) for p in pts]
            q["degree"]=q.get("degree") or ctx["base"].get("working_degree")
            q["proportionality"]={"price_span":round(max(prices)-min(prices),10) if prices else None,
                                  "time_span_bars":None}
            if len(pts)>=2:
                idx={r["start"]:i for i,r in enumerate(rows)}; a=idx.get(pts[0].get("start")); b=idx.get(pts[-1].get("start"))
                q["proportionality"]["time_span_bars"]=(b-a) if a is not None and b is not None else None
            q["status"]="valid_candidate"; cands.append(q)
        # Run the same hard-rule tree across every available structural degree.
        by_degree={}
        for degree,dctx in ctx.get("degrees",{}).items():
            if len(dctx.get("points",[])) < 4: continue
            local_ctx={"ready":ctx.get("ready"),"degrees":ctx.get("degrees"),"base":dict(ctx["base"])}
            local_ctx["base"]["working_degree"]=degree
            local=self._elliott_engine_v2(rows,local_ctx,fib)
            by_degree[degree]={"primary":local.get("primary"),"alternatives":local.get("alternatives",[]),
                               "candidate_count":local.get("candidate_count",0),"ambiguous":local.get("ambiguous",False)}
        # Ambiguity is explicit; Elliott remains interpretive evidence only.
        return {**base,"candidates":cands,"wave_degree":ctx["base"].get("working_degree"),"degree_candidates":by_degree,
                "recursive_degrees":True,"role":"interpretation_only","structure_authority":False}

    def _harmonic_depth_v38(self, rows, base):
        atr=self._atr(rows); confirmed=[]; developing=[]
        for bucket,out in ((base.get("confirmed",[]),confirmed),(base.get("developing",[]),developing)):
            for p in bucket:
                q=dict(p); pts=q.get("points",[]); d=float(pts[-1]["price"]) if pts else None
                ratios=q.get("ratios",{}); checks=q.get("checks",{})
                quality=(sum(1 for v in checks.values() if v)/max(1,len(checks))) if checks else (1.0 if q.get("name")=="AB=CD" else 0.5)
                width=(atr*0.35) if atr else (abs(d)*0.003 if d else 0)
                q["prz"]={"low":None if d is None else d-width,"high":None if d is None else d+width,"center":d,"width_atr":0.7 if atr else None}
                q["quality"]=round(quality,3); q["completion"]="completed" if out is confirmed else "developing"
                q["invalidation"]=(q["prz"]["low"] if q.get("direction")=="bullish" else q["prz"]["high"]) if d is not None else None
                out.append(q)
        return {**base,"confirmed":confirmed,"developing":developing,"role":"PRZ_context_not_reversal_command"}



    def _divergence_depth_v38(self, rows, base):
        groups={}
        for e in base.get("events",[]):
            key=(e.get("type"), (e.get("from") or {}).get("start"), (e.get("to") or {}).get("start"))
            g=groups.setdefault(key,{"type":e.get("type"),"from":e.get("from"),"to":e.get("to"),"indicators":[],"evidence":[]})
            g["indicators"].append(e.get("indicator")); g["evidence"].append(e)
        events=[]
        for g in groups.values():
            age=self._age_bars_v38(rows,(g.get("to") or {}).get("start")); n=len(set(g["indicators"]))
            g["indicators"]=sorted(set(g["indicators"])); g["strength"]="strong" if n>=3 else "moderate" if n==2 else "single"
            ttl=self._ttl_bars_v382(rows,"divergence")
            g["age_bars"]=age; g["ttl_bars"]=ttl; g["fresh"]=self._fresh_v38(age,ttl); g["expired"]=(age is not None and age>ttl); g["freshness"]="fresh" if g["fresh"] else "expired" if g["expired"] else "unknown"
            events.append(g)
        consensus={}
        for e in events:
            if e["fresh"]: consensus[e["type"]]=consensus.get(e["type"],0)+1
        return {**base,"events_raw":base.get("events",[]),"events":events,"consensus":consensus,"deduplicated":True}

    def _liquidity_depth_v38(self, rows, ctx, base):
        atr=self._atr(rows); close=float(rows[-1]["close"]) if rows else None; pools=[]
        for degree,d in ctx.get("degrees",{}).items():
            for p in d.get("points",[])[-10:]:
                side="buy_side" if p["kind"]=="high" else "sell_side"; price=float(p["price"])
                later=[r for r in rows if r.get("start",0)>p.get("start",0)]
                accepted=any(float(r["close"])>price for r in later) if side=="buy_side" else any(float(r["close"])<price for r in later)
                taken=any(float(r["high"])>price for r in later) if side=="buy_side" else any(float(r["low"])<price for r in later)
                age=self._age_bars_v38(rows,p.get("start"))
                pools.append({"side":side,"price":price,"degree":degree,"source":"structural_swing","start":p.get("start"),"age_bars":age,
                              "taken":taken,"state":"accepted_through" if accepted else "swept_or_touched" if taken else "untouched",
                              "distance_atr":None if not atr or close is None else round(abs(price-close)/atr,3)})
        for key,side in (("equal_highs","buy_side"),("equal_lows","sell_side")):
            z=base.get(key)
            if z: pools.append({"side":side,"price":float(z["price"]),"degree":"internal","source":key,"start":z.get("second_start"),
                                "age_bars":self._age_bars_v38(rows,z.get("second_start")),"taken":False,"state":"pool"})
        unt=[p for p in pools if p.get("state") in ("untouched","pool")]
        above=sorted([p for p in unt if close is not None and p["price"]>close],key=lambda p:p["price"])
        below=sorted([p for p in unt if close is not None and p["price"]<close],key=lambda p:p["price"],reverse=True)
        # Persistent liquidity event: search recent CLOSED bars, then keep it until TTL/invalidation.
        events=[]; ttl=self._ttl_bars_v382(rows,"liquidity")
        if atr and rows:
            idx0=max(0,len(rows)-1-ttl)
            for i in range(idx0,len(rows)):
                cur=rows[i]; ts=cur.get("start")
                for p in pools:
                    if p.get("start",0)>=ts: continue
                    level=float(p["price"])
                    if p["side"]=="buy_side" and float(cur["high"])>level and float(cur["close"])<=level:
                        events.append({"type":"liquidity_grab","direction":"bearish","side":"buy_side","level":level,"penetration_atr":round((float(cur["high"])-level)/atr,3),"start":ts,"source_pool":p.get("source")})
                    elif p["side"]=="sell_side" and float(cur["low"])<level and float(cur["close"])>=level:
                        events.append({"type":"liquidity_grab","direction":"bullish","side":"sell_side","level":level,"penetration_atr":round((level-float(cur["low"]))/atr,3),"start":ts,"source_pool":p.get("source")})
        # Deduplicate same bar/side/level and prefer the most recent event.
        uniq={}
        for e in events: uniq[(e["start"],e["side"],round(e["level"],10))]=e
        events=sorted(uniq.values(),key=lambda e:e.get("start",0))
        event=events[-1] if events else (base.get("sweep") if isinstance(base.get("sweep"),dict) else None)
        life=self._event_lifecycle_v381(rows,event,ttl) if event else None
        if event and life: event={**event,"lifecycle":life}
        return {**base,"pools":pools[-40:],"external_above":above[:6],"external_below":below[:6],
                "nearest_buy_side":above[0] if above else None,"nearest_sell_side":below[0] if below else None,
                "events":events[-8:],"event":event}

    def _smc_depth_v38(self, rows, ctx, liquidity, base):
        atr=self._atr(rows); structure=ctx["base"]; close=float(rows[-1]["close"]) if rows else None
        pivots=ctx.get("degrees",{}).get("minor",{}).get("points",[])
        liqev=liquidity.get("event") if isinstance(liquidity.get("event"),dict) else None
        liq_start=(liqev or {}).get("start"); liq_dir=(liqev or {}).get("direction")
        # Multi-bar displacement detector (1..4 bars), AFTER the liquidity event when one exists.
        displacements=[]
        if atr and len(rows)>=2:
            start_i=0
            if liq_start is not None:
                for i,r in enumerate(rows):
                    if r.get("start")==liq_start: start_i=i+1; break
            for end_i in range(max(start_i,len(rows)-16),len(rows)):
                for n in range(1,5):
                    a=end_i-n+1
                    if a<start_i or a<0: continue
                    win=rows[a:end_i+1]; op=float(win[0]["open"]); cl=float(win[-1]["close"])
                    hi=max(float(r["high"]) for r in win); lo=min(float(r["low"]) for r in win); rng=max(1e-12,hi-lo)
                    body=abs(cl-op); direction="bullish" if cl>op else "bearish" if cl<op else None
                    efficiency=body/rng; body_atr=body/atr
                    if direction and body_atr>=0.9 and efficiency>=0.55:
                        displacements.append({"direction":direction,"start":win[0].get("start"),"end":win[-1].get("start"),"bars":n,
                                              "body_atr":round(body_atr,3),"efficiency":round(efficiency,3)})
        displacement=None
        aligned=[d for d in displacements if liq_dir is None or d["direction"]==liq_dir]
        if aligned: displacement=max(aligned,key=lambda d:(d["end"],d["body_atr"]*d["efficiency"]))
        # MSS is independent of CHOCH: displacement must close through a relevant INTERNAL pivot after the sweep.
        mss_event=None
        if displacement:
            dstart=displacement["start"]; dend=displacement["end"]; direction=displacement["direction"]
            prior=[p for p in pivots if p.get("start",0)<dstart]
            candidates=[p for p in prior if p.get("kind")==('high' if direction=='bullish' else 'low')]
            internal=candidates[-1] if candidates else None
            if internal:
                endrow=next((r for r in rows if r.get("start")==dend),None)
                crossed=bool(endrow and ((float(endrow["close"])>float(internal["price"])) if direction=='bullish' else (float(endrow["close"])<float(internal["price"]))))
                if crossed:
                    mss_event={"type":"MSS","direction":direction,"start":dend,"broken_level":float(internal["price"]),"broken_pivot_start":internal.get("start"),"confirmed_by_close":True}
        # FVG lifecycle + CE. Creation must be after MSS to belong to this causal scenario.
        fvgs=[]
        for z in base.get("fvg",[]):
            q=dict(z); lo=min(float(q["from"]),float(q["to"])); hi=max(float(q["from"]),float(q["to"])); q["ce"]=(lo+hi)/2
            age=self._age_bars_v38(rows,q.get("start")); invalid=bool(q.get("fully_filled")); life=self._event_lifecycle_v381(rows,q,30,invalid,"fully_filled" if invalid else None)
            q.update({"age_bars":age,"fresh":life["fresh"],"expired":life["expired"] or invalid,"lifecycle":life,"size_atr":None if not atr else round((hi-lo)/atr,3)})
            fvgs.append(q)
        active_fvg=[z for z in fvgs if not z.get("expired")]
        # Causal BPR: opposite FVGs must be sequential and close enough in the same delivery sequence.
        bprs=[]
        ordered=sorted(active_fvg,key=lambda z:z.get("start",0))
        for i,a in enumerate(ordered):
            for b in ordered[i+1:]:
                if a.get("type")==b.get("type"): continue
                age_gap=abs((self._age_bars_v38(rows,a.get("start")) or 0)-(self._age_bars_v38(rows,b.get("start")) or 0))
                if age_gap>12: continue
                lo=max(min(float(a["from"]),float(a["to"])),min(float(b["from"]),float(b["to"]))); hi=min(max(float(a["from"]),float(a["to"])),max(float(b["from"]),float(b["to"])))
                if lo<hi: bprs.append({"low":lo,"high":hi,"equilibrium":(lo+hi)/2,"sources":[a.get("start"),b.get("start")],"causal":True})
        # OB quality is causal: the OB must PRECEDE and be close to the displacement that produced MSS.
        obs=[]
        for ob in base.get("order_blocks",[]):
            q=dict(ob); later=[r for r in rows if r.get("start",0)>q.get("start",0)]; lo=float(q["low"]); hi=float(q["high"])
            q["mitigated"]=any(float(r["low"])<=hi and float(r["high"])>=lo for r in later)
            q["invalidated"]=any((float(r["close"])<lo if q.get("direction")=="bullish" else float(r["close"])>hi) for r in later)
            causal=bool(displacement and mss_event and q.get("direction")==displacement.get("direction") and q.get("start",0)<displacement.get("start",0))
            origin_touch=False
            if causal:
                oi=next((i for i,r in enumerate(rows) if r.get("start")==q.get("start")),None); di=next((i for i,r in enumerate(rows) if r.get("start")==displacement.get("start")),None)
                causal=bool(oi is not None and di is not None and 0<di-oi<=8)
                if causal:
                    first=rows[di]
                    # A causal OB must be the actual origin/retest zone of the impulse,
                    # not merely an older same-direction candle located nearby in time.
                    if q.get("direction")=="bullish":
                        origin_touch=float(first["low"]) <= hi
                    else:
                        origin_touch=float(first["high"]) >= lo
                    if atr:
                        mid=(lo+hi)/2.0; origin_price=float(first["open"])
                        origin_touch=origin_touch and abs(origin_price-mid) <= max(atr*1.25, hi-lo)
                    causal=bool(origin_touch)
            q["origin_interaction"]=origin_touch; q["caused_displacement"]=causal
            q["lifecycle"]=self._event_lifecycle_v381(rows,q,self._ttl_bars_v382(rows,"order_block"),q["invalidated"],"zone_invalidated" if q["invalidated"] else None)
            q["fresh"]=bool(q["lifecycle"].get("fresh") and not q["invalidated"])
            q["expired"]=bool(q["lifecycle"].get("expired"))
            q["quality"]="high" if causal and q["fresh"] else "basic"
            obs.append(q)
        # Protected level is inherited from Structure authority. Inducement must sit between it and the liquidity event.
        protected=structure.get("protected_low") if liq_dir=="bullish" else structure.get("protected_high") if liq_dir=="bearish" else None
        inducement=None
        if protected and liq_start is not None:
            want="low" if liq_dir=="bullish" else "high"
            cand=[p for p in pivots if p.get("kind")==want and p.get("start",0)>protected.get("start",0) and p.get("start",0)<liq_start]
            inducement=cand[-1] if cand else None
        # Strict temporal chain.
        chain={"liquidity":liqev,"displacement":displacement,"mss":mss_event}
        ordered_chain=bool(liqev and displacement and mss_event and liq_start < displacement.get("start",0) <= displacement.get("end",0) <= mss_event.get("start",0))
        scenario_dir=mss_event.get("direction") if mss_event else liq_dir
        scenario_fvg=[z for z in active_fvg if ordered_chain and z.get("type")==scenario_dir and z.get("start",0)>=mss_event.get("start",0)]
        pois=[]
        for z in scenario_fvg: pois.append({"type":"fvg","direction":z.get("type"),"low":min(z["from"],z["to"]),"high":max(z["from"],z["to"]),"start":z.get("start"),"fresh":True,"rank":3})
        for z in obs:
            if z.get("fresh") and z.get("caused_displacement") and z.get("direction")==scenario_dir: pois.append({"type":"order_block","direction":z.get("direction"),"low":z["low"],"high":z["high"],"start":z.get("start"),"fresh":True,"rank":4})
        causal_bprs=[]
        for z in bprs:
            sources=sorted(z.get("sources") or [])
            same_sequence=bool(ordered_chain and len(sources)==2 and sources[0]>=displacement.get("start",0) and sources[1]>=mss_event.get("start",0))
            z["same_delivery_sequence"]=same_sequence
            z["sequence_anchor"]={"liquidity":liq_start,"displacement":displacement.get("start") if displacement else None,"mss":mss_event.get("start") if mss_event else None}
            if same_sequence:
                causal_bprs.append(z)
                pois.append({"type":"bpr","direction":scenario_dir,"low":z["low"],"high":z["high"],"start":max(sources),"fresh":True,"rank":3})
        pois.sort(key=lambda p:(-p["rank"],abs(((p["low"]+p["high"])/2)-close) if close else 0))
        stage="LIQUIDITY_FORMING"
        if liqev: stage="LIQUIDITY_TAKEN"
        if liqev and displacement: stage="DISPLACEMENT"
        if ordered_chain: stage="MSS_CONFIRMED"
        if ordered_chain and pois: stage="POI_CREATED"
        in_poi=bool(pois and close is not None and pois[0]["low"]<=close<=pois[0]["high"])
        if stage=="POI_CREATED": stage="IN_POI" if in_poi else "WAIT_RETRACE"
        target=liquidity.get("nearest_buy_side") if scenario_dir=="bullish" else liquidity.get("nearest_sell_side") if scenario_dir=="bearish" else None
        invalidation=protected or ((liqev and {"price":liqev.get("level"),"start":liqev.get("start"),"source":"sweep_extreme"}) if liqev else None)
        missing=None
        if not liqev: missing="liquidity_event"
        elif not displacement: missing="displacement_after_liquidity"
        elif not mss_event: missing="mss_after_displacement"
        elif not pois: missing="causal_poi_after_mss"
        return {**base,"fvg":fvgs,"bpr":causal_bprs[-6:],"bpr_detected":bprs[-12:],"order_blocks":obs,"protected_level":protected,"inducement":inducement,
                "liquidity_event":liqev,"displacement":displacement,"mss":{"confirmed":bool(mss_event),"event":mss_event},"poi":pois[:6],
                "causal_chain":{**chain,"ordered":ordered_chain},
                "scenario":{"direction":scenario_dir,"stage":stage,"ready_for_retrace":stage in ("WAIT_RETRACE","IN_POI"),
                            "invalidation_thesis":invalidation,"target_liquidity":target,"missing_confirmation":missing},
                "role":"scenario_builder_not_trade_authority"}

    def _evidence_graph_v38(self, technical, structure, fib, elliott, harmonics, divergences, liquidity, smc):
        facts=[]; confirms=[]; warnings=[]; contradictions=[]; hard=[]
        st=structure.get("state")
        struct_dir="bullish" if st=="uptrend" else "bearish" if st=="downtrend" else None
        if st in ("uptrend","downtrend"):
            facts.append({"source":"structure","type":"trend","value":st,"authority":"hard_fact"})
        ev=structure.get("last_event")
        if ev: facts.append({"source":"structure","type":"event","value":ev,"authority":"hard_fact"})
        le=liquidity.get("event")
        if le: facts.append({"source":"liquidity","type":"event","value":le,"authority":"hard_fact"})

        sc=smc.get("scenario") or {}; smc_dir=sc.get("direction")
        if sc.get("stage") not in (None,"LIQUIDITY_FORMING"):
            confirms.append({"source":"smart_money","type":"causal_scenario","value":sc,"derived_from":["structure","liquidity"]})
        if struct_dir and smc_dir and struct_dir!=smc_dir:
            contradictions.append({"between":["structure","smart_money"],"structure_direction":struct_dir,"smc_direction":smc_dir,
                                   "resolution":"structure_keeps_direction_authority_smc_is_counter_scenario"})

        ep=elliott.get("primary")
        if ep:
            edir=ep.get("direction")
            (warnings if elliott.get("ambiguous") else confirms).append({"source":"elliott","type":"wave_interpretation","value":{"type":ep.get("type"),"direction":edir},"derived_from":["structure","fibonacci"]})
            if struct_dir and edir and edir!=struct_dir:
                contradictions.append({"between":["structure","elliott"],"structure_direction":struct_dir,"elliott_direction":edir,
                                       "resolution":"elliott_cannot_override_structure"})

        confirmed_h=harmonics.get("confirmed") or []
        if confirmed_h:
            h=confirmed_h[0]; confirms.append({"source":"harmonics","type":"prz","value":h,"derived_from":["structure","fibonacci"]})
            hdir=h.get("direction")
            if struct_dir and hdir and hdir!=struct_dir:
                contradictions.append({"between":["structure","harmonics"],"structure_direction":struct_dir,"harmonic_direction":hdir,
                                       "resolution":"harmonic_is_prz_context_not_direction_authority"})

        for d in divergences.get("events",[]):
            if d.get("fresh"):
                warnings.append({"source":"divergence","type":d.get("type"),"value":{"indicators":d.get("indicators"),"strength":d.get("strength")},"derived_from":["technical","structure"]})

        life=structure.get("event_lifecycle") or {}
        if life.get("invalidated"):
            hard.append({"source":"structure","type":"event_invalidated","reason":life.get("invalidation_reason")})
        causal=smc.get("causal_chain") or {}
        if sc.get("stage") not in (None,"LIQUIDITY_FORMING","LIQUIDITY_TAKEN") and not causal.get("ordered"):
            hard.append({"source":"smart_money","type":"causal_order_invalid","reason":"scenario_events_not_temporally_ordered"})

        return {"facts":facts,"confirmations":confirms,"warnings":warnings,"contradictions":contradictions,"hard_invalidations":hard,
                "authority_order":["hard_structural_fact","scenario_interpretation","pattern_evidence","indicator_evidence"],
                "conflict_policy":"later_blocks_interpret_but_never_rewrite_upstream_facts","vote_counting":False}


    def _analysis_bundle(self, candles):
        raw_rows=list(candles)
        rows=[r for r in raw_rows if r.get("confirm", True)]
        technical0=self._technical_metrics(rows)
        technical=self._technical_depth_v38(rows,technical0)
        ctx=self._structure_context_v2(rows)
        structure=self._structure_depth_v38(rows,ctx)
        ctx["base"]=structure
        fib0=self._fib_engine_v2(rows,ctx)
        fib=self._fib_depth_v38(rows,ctx,fib0)
        ell0=self._elliott_engine_v2(rows,ctx,fib)
        elliott=self._elliott_depth_v38(rows,ctx,fib,ell0)
        harm0=self._harmonic_engine_v2(ctx)
        harmonics=self._harmonic_depth_v38(rows,harm0)
        div0=self._divergence_engine_v2(rows,ctx)
        divergences=self._divergence_depth_v38(rows,div0)
        liquidity0=self._liquidity_metrics(rows)
        liquidity=self._liquidity_depth_v38(rows,ctx,liquidity0)
        smc0=self._smart_money_engine_v2(rows,ctx,liquidity)
        smc=self._smc_depth_v38(rows,ctx,liquidity,smc0)
        regime=self._regime_levels_v2(rows,technical,structure,fib)
        evidence=self._evidence_graph_v38(technical,structure,fib,elliott,harmonics,divergences,liquidity,smc)
        # Legacy confluence remains diagnostic only. It is NOT a decision authority in V3.8.
        legacy=self._confluence_engine_v2(technical,ctx,fib,elliott,harmonics,divergences,liquidity,smc)
        confluence={**legacy,"decision_authority":False,"deprecated_vote_model":True,"evidence_graph":evidence}
        ready=bool(technical.get("ready") and ctx.get("ready"))
        now_ms=int(time.time()*1000)
        last_start=rows[-1].get("start") if rows else None
        last_end=rows[-1].get("end") if rows else None
        age_seconds=None
        try:
            age_seconds=max(0.0,(now_ms-int(last_end if last_end is not None else last_start))/1000.0)
        except (TypeError,ValueError):
            pass
        # Freshness is timeframe-aware: higher-TF closes remain usable through their own cadence.
        tf=self._tf_from_rows_v382(rows)
        tf_seconds={"1":60,"5":300,"15":900,"60":3600,"240":14400,"D":86400}.get(tf)
        fresh_limit=(tf_seconds*1.5) if tf_seconds else 120
        stale_limit=(tf_seconds*3.0) if tf_seconds else 900
        freshness_state="UNKNOWN"
        if age_seconds is not None:
            freshness_state="FRESH" if age_seconds<=fresh_limit else "STALE" if age_seconds>stale_limit else "AGING"
        freshness={"state":freshness_state,"age_seconds":age_seconds,"timeframe":tf,
                   "fresh_limit_seconds":fresh_limit,"stale_limit_seconds":stale_limit,
                   "last_confirmed_start":last_start,"last_confirmed_end":last_end}
        return {"ready":ready,"engine_version":"scan_plus_v3_9_limit_plan","closed_candles":len(rows),
                "excluded_open_candles":max(0,len(raw_rows)-len(rows)),"last_confirmed_start":last_start,
                "last_confirmed_close":rows[-1].get("close") if rows else None,
                "signal_freshness":freshness,
                "pipeline":["technical","structure","fibonacci","elliott","harmonics","divergences","liquidity","smart_money","freshness","evidence_graph"],
                "technical":technical,"structure":structure,"fibonacci":fib,"elliott":elliott,"harmonics":harmonics,"divergences":divergences,
                "liquidity":liquidity,"smart_money":smc,"regime_levels":regime,"evidence_graph":evidence,"confluence":confluence}


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
            persistence_counts = {i: counts[i] for i in KLINE_INTERVALS}
            checks = {
                "connected": bool(self.connected),
                "orderbook_ready": bool(self.orderbook_ready),
                "candles_receiving": any(v > 0 for v in counts.values()),
                "technical_full_6_of_6": all(technical[i]["ready"] for i in KLINE_INTERVALS),
                "persistence_250_all_6": all(v >= PERSISTENCE_SEED_TARGET for v in persistence_counts.values()),
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
                "persistence_pass": checks["persistence_250_all_6"],
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
                    "persistence_target": PERSISTENCE_SEED_TARGET,
                    "persistence_ready": checks["persistence_250_all_6"],
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
        if topic:
            # First valid market-data topic is definitive proof that the symbol is
            # active on this market.
            with self.lock:
                self.available = True
                self.capability_state = "active"

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
                candle = self._sanitize_candle(
                    {**row, "source": "bybit_ws"},
                    interval,
                )
                if candle is None:
                    continue

                candles = self.candles[interval]
                merged = self._merge_candle_rows(list(candles), [candle], interval)
                candles.clear()
                candles.extend(merged)
                self.seed_counts[interval] = sum(
                    1 for c in candles if c.get("source") == "binance_seed"
                )
                self.history_bootstrapped = True
                self.history_loaded_at = self.history_loaded_at or time.time()
                self.history_source = (
                    "binance_seed+bybit_websocket"
                    if any(self.seed_counts.values())
                    else "bybit_websocket"
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
                self.orderbook_updated_at = time.time()

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
                self.orderbook_updated_at = time.time()

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

    def _flow_window_stats(self, now_ms, duration):
        """Quality-aware rolling-flow readiness.

        A horizon is warm only after observing a meaningful fraction of that actual
        horizon. Nested windows therefore cannot all become warm from the same brief
        burst of trades. Counts protect illiquid symbols; coverage protects semantics.
        """
        min_counts = {
            WINDOWS["1m"]: 3,
            WINDOWS["5m"]: 4,
            WINDOWS["15m"]: 6,
            WINDOWS["1h"]: 8,
        }
        min_count = min_counts.get(duration, 3)
        min_coverage = max(10_000, int(duration * 0.50))
        cutoff = now_ms - duration
        valid=[]
        for timestamp_ms, side, price, qty in reversed(self.trades):
            if timestamp_ms < cutoff:
                break
            if side not in ("Buy", "Sell") or price is None or qty is None or qty <= 0:
                continue
            valid.append((timestamp_ms, side, price, qty))
        count=len(valid)
        newest=valid[0][0] if valid else None
        oldest=valid[-1][0] if valid else None
        coverage=max(0, newest-oldest) if newest is not None and oldest is not None else 0
        total=sum(row[3] for row in valid)
        ready=bool(count >= min_count and total > 0 and coverage >= min_coverage)
        count_quality=min(1.0, count / max(min_count * 3.0, 1.0))
        coverage_ratio=min(1.0, coverage / max(duration, 1))
        # Full confidence requires broad temporal coverage, not merely crossing the
        # readiness threshold. At 50% coverage the coverage component is 0.5.
        quality=(count_quality * coverage_ratio) ** 0.5 if ready else 0.0
        return {
            "ready":ready,"trade_count":count,"coverage_ms":coverage,
            "window_ms":duration,"coverage_ratio":round(coverage_ratio,4),
            "required_trade_count":min_count,"required_coverage_ms":min_coverage,
            "quality":round(quality,4),
        }

    def _flow_window_ready(self, now_ms, duration):
        return self._flow_window_stats(now_ms, duration)["ready"]

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
                if side not in ("Buy", "Sell") or price is None or qty is None or qty <= 0:
                    continue

                count += 1

                # Because we iterate backwards, last_price is newest and
                # first_price ends as oldest valid trade.
                first_price = price
                if last_price is None:
                    last_price = price

                if side == "Buy":
                    buy += qty
                else:
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

    def _oi_metrics(self, now_ms):
        if self.market != "linear":
            return None
        current=fnum(self.ticker.get("openInterest"))
        result={"current":current,"windows":{}}
        # OI needs temporal coverage too; a 20-second collector history must not be
        # labelled as a 1h OI change.
        samples=list(self.oi_samples)
        for name,duration in WINDOWS.items():
            cutoff=now_ms-duration
            inside=[(ts,oi) for ts,oi in samples if ts>=cutoff]
            base=inside[0][1] if inside else None
            oldest_ts=inside[0][0] if inside else None
            newest_ts=inside[-1][0] if inside else None
            coverage=max(0,newest_ts-oldest_ts) if oldest_ts is not None and newest_ts is not None else 0
            required=max(10_000, int(duration * 0.50))
            usable=bool(current is not None and base not in (None,0) and len(inside)>=2 and coverage>=required)
            change=(current-base) if usable else None
            change_pct=(change/base*100) if usable else None
            result["windows"][name]={
                "start":base,"change":round(change,8) if change is not None else None,
                "change_pct":round(change_pct,6) if change_pct is not None else None,
                "sample_count":len(inside),"coverage_ms":coverage,"window_ms":duration,
                "coverage_ratio":round(min(1.0, coverage/max(duration,1)),4),
                "required_coverage_ms":required,"usable":usable,
            }
        return result

    # ========================================================
    # ORDERBOOK METRICS
    # ========================================================

    def _book_metrics(self):

        age_seconds = (time.time() - self.orderbook_updated_at) if self.orderbook_updated_at else None
        fresh = bool(age_seconds is not None and age_seconds <= 15.0)
        if (
            not self.orderbook_ready
            or not self.bids
            or not self.asks
            or not fresh
        ):

            return {
                "ready": False,
                "fresh": fresh,
                "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
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
            "fresh": True,
            "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,

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
            collector_age = (
                now - self.collector_started_at
                if self.collector_started_at
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

                "capability_state":
                    self.capability_state,

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
                "collector_age_seconds": (round(collector_age,2) if collector_age is not None else None),

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
                    name: self._flow_window_ready(now_ms, duration)
                    for name, duration in WINDOWS.items()
                },
                "flow_quality": {
                    name: self._flow_window_stats(now_ms, duration)
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


    @staticmethod
    def _live_structure_context_v33(linear, execution):
        """Backward-compatible facade; authority remains DynamicMarketManager."""
        return DynamicMarketManager._live_structure_context_v33(linear, execution)


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
    # INITIAL REALTIME RESOLUTION
    # ========================================================

    @staticmethod
    def _initial_stream_state(stream):
        """Cheap transport/capability probe used only during cold start.

        Do not call snapshot() here: it runs the full analysis pipeline.  We only
        need enough state to know whether the websocket has resolved.
        """
        with stream.lock:
            ticker_updated = fnum(stream.ticker.get("updated_at"))
            ticker_age = (time.time() - ticker_updated) if ticker_updated is not None else None
            last_price = fnum(stream.ticker.get("lastPrice"))
            return {
                "connected": bool(stream.connected),
                "capability_state": stream.capability_state,
                "available": stream.available,
                "ticker_fresh": bool(last_price is not None and ticker_age is not None and ticker_age <= 15.0),
                "status": stream.status,
            }

    def _wait_initial_realtime_resolution(self, pair, timeout=8.0):
        """Bounded cold-start barrier for /scan and /snapshot.

        Linear must produce a fresh realtime ticker (the source of current price)
        or reach an explicit terminal/transient capability result. Spot only has
        to resolve its subscription capability; it must not delay a valid linear
        price while waiting for its first trade on an illiquid pair.
        """
        deadline = time.monotonic() + max(0.0, float(timeout))
        # Only an explicit unsupported result is terminal.  A transient reject is
        # deliberately *not* treated as resolved: the stream reconnect loop may
        # recover inside this same bounded cold-start window.
        linear_terminal = {"unsupported"}
        spot_resolved = {"subscribed", "active", "unsupported"}

        while True:
            linear = self._initial_stream_state(pair["linear"])
            spot = self._initial_stream_state(pair["spot"])

            linear_resolved = linear["ticker_fresh"] or linear["capability_state"] in linear_terminal
            spot_done = spot["capability_state"] in spot_resolved

            if linear_resolved and spot_done:
                return {"resolved": True, "linear": linear, "spot": spot}
            if time.monotonic() >= deadline:
                return {"resolved": False, "linear": linear, "spot": spot}
            time.sleep(0.05)

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
            raw_direction=(a.get("confluence") or {}).get("direction","neutral")
            effective_direction=raw_direction
            pending_direction=None
            broken_level=None
            structural_gap=None
            if price is not None and structural_low is not None and price < structural_low:
                mode="price_discovery_down"; pending_direction="bearish"; broken_level=structural_low
                structural_gap=structural_low-price
            elif price is not None and structural_high is not None and price > structural_high:
                mode="price_discovery_up"; pending_direction="bullish"; broken_level=structural_high
                structural_gap=price-structural_high
            elif price is not None and structural_low is not None and structural_high is not None:
                structural_gap=0.0
            last_event=structure.get("last_event") or {}
            event_dir=last_event.get("direction")
            event_type=last_event.get("type")
            # Price discovery is an event, not a direction rewrite. A same-TF
            # structure event may change effective direction only after close confirmation.
            event_confirmed=(
                bool(last_event.get("confirmed_by_close") or last_event.get("confirmed"))
                and event_type in ("BOS", "CHoCH")
            )
            if pending_direction and event_confirmed and event_dir==pending_direction:
                effective_direction=pending_direction
            elif pending_direction:
                effective_direction="transition" if raw_direction in ("bullish","bearish") else "neutral"

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
                "mode":mode,"raw_direction":raw_direction,"effective_direction":effective_direction,
                "pending_direction":pending_direction,
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
            lq=(linear.get("flow_quality") or {}).get(w,{})
            sq=(spot.get("flow_quality") or {}).get(w,{})
            warm=bool(linear.get("warmup",{}).get(w,False) and spot.get("warmup",{}).get(w,False))
            sample_quality=min(float(lq.get("quality") or 0.0), float(sq.get("quality") or 0.0))

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

                # Sparse/short-lived samples must never report perfect confidence.
                # Re-weight the vote that was just added by quality.
                if driver in driver_votes:
                    driver_votes[driver] -= confidence
                    confidence *= sample_quality
                    driver_votes[driver] += confidence

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
                "perp_trade_count":l.get("trade_count",0),
                "spot_trade_count":s.get("trade_count",0),
                "perp_flow_usable":bool(lq.get("ready") and pd is not None and pp is not None),
                "spot_flow_usable":bool(sq.get("ready") and sd is not None and sp is not None),
                "perp_flow_quality":lq,
                "spot_flow_quality":sq,
                "sample_quality":round(sample_quality,4),
                "oi_usable":bool(o.get("usable")),
                "oi_coverage_ms":o.get("coverage_ms"),
                "driver":driver,
                "driver_confidence":round(confidence,4),
            }

        # Prefer evidence accumulated across fully warmed windows.
        ranked=sorted(driver_votes.items(),key=lambda kv:kv[1],reverse=True)
        overall_driver=ranked[0][0] if ranked and ranked[0][1]>0 else "unknown"
        total_votes=sum(driver_votes.values())
        category_share=(ranked[0][1]/total_votes) if total_votes and ranked else 0.0
        # Confidence is not category share: four nested windows are correlated.
        # Cap it by the strongest quality-adjusted single-window evidence.
        strongest=max((float(d.get("driver_confidence") or 0.0) for d in windows.values() if d.get("driver")==overall_driver), default=0.0)
        # Nested horizons are correlated. Confidence may mature only as genuinely
        # distinct rolling horizons become quality-ready; one minute of tape must
        # not produce a categorical 1.0 driver call.
        agreeing_ready=[w for w,d in windows.items() if d.get("driver")==overall_driver and d.get("linear_warm") and d.get("spot_warm")]
        horizon_cap={0:0.0,1:0.55,2:0.75,3:0.90,4:0.97}.get(len(agreeing_ready),0.97)
        overall_conf=min(category_share, strongest, horizon_cap)

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
            if not ow.get("oi_usable") or ch is None or pc is None:
                continue
            if ch>0 and pc>0: oi_context="new_longs_or_trend_participation"
            elif ch>0 and pc<0: oi_context="new_shorts_or_bearish_participation"
            elif ch<0 and pc>0: oi_context="short_covering_or_position_reduction"
            elif ch<0 and pc<0: oi_context="long_liquidation_or_position_reduction"
            break

        ticker_updated=fnum(ticker.get("updated_at"))
        ticker_age=(time.time()-ticker_updated) if ticker_updated is not None else None
        ticker_fresh=bool(ticker_age is not None and ticker_age <= 15.0)
        transport_ready=bool(linear.get("connected") and spot.get("connected") and book.get("ready") and ticker_fresh)
        warmed_flow_windows=[
            w for w,data in windows.items()
            if data.get("linear_warm") and data.get("spot_warm")
            and data.get("perp_flow_usable") and data.get("spot_flow_usable")
            and float(data.get("sample_quality") or 0.0) > 0
        ]
        flow_ready=bool(warmed_flow_windows)
        best_quality=max((float(windows.get(w,{}).get("sample_quality") or 0.0) for w in warmed_flow_windows), default=0.0)
        driver_ready=bool(overall_driver!="unknown" and overall_conf>=0.20 and best_quality>=0.20)
        trade_data_ready=bool(transport_ready and price is not None and flow_ready and driver_ready)

        return {
            "ready":transport_ready,
            "trade_data_ready":trade_data_ready,
            "flow_ready":flow_ready,
            "driver_ready":driver_ready,
            "warmed_flow_windows":warmed_flow_windows,
            "price":price,
            "spread_bps":book.get("spread_bps"),
            "orderbook_age_seconds":book.get("age_seconds"),
            "ticker_age_seconds":round(ticker_age,3) if ticker_age is not None else None,
            "ticker_fresh":ticker_fresh,
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
    # ========================================================
    # SCAN+ V3.7 DECISION ARCHITECTURE
    # Context (D/4H) -> Direction (1H/15m) -> Setup (5m)
    # -> Trigger (1m) -> Execution -> Thesis risk/targets.
    # ========================================================

    @staticmethod
    def _direction_from_frame_v37(a):
        """V3.8 structural direction. Pattern/indicator votes cannot rewrite structure."""
        structure=a.get("structure",{}) or {}; state=structure.get("state","unknown"); ev=structure.get("last_event") or {}
        # Fresh confirmed CHOCH means transition until subsequent structure confirms a new trend.
        life=structure.get("event_lifecycle") or {}
        if ev.get("type")=="CHOCH" and ev.get("confirmed_by_close") and life.get("fresh",True): return "neutral"
        if state=="uptrend": return "bullish"
        if state=="downtrend": return "bearish"
        # A confirmed BOS in transition can provide provisional direction, never a full trend rewrite.
        if ev.get("type")=="BOS" and ev.get("confirmed_by_close") and ev.get("direction") in ("bullish","bearish"): return ev.get("direction")
        return "neutral"

    @staticmethod
    def _signal_freshness_v37(a, tf):
        ttl={"1":8,"5":10,"15":12,"60":14,"240":16,"D":20}.get(tf,10)
        structure=a.get("structure",{}) or {}; life=structure.get("event_lifecycle") or {}


        smc=a.get("smart_money",{}) or {}; liq=smc.get("liquidity_event") or {}
        liq_life=liq.get("lifecycle") or {}
        return {"ttl_bars":ttl,"last_confirmed_start":a.get("last_confirmed_start"),
                "structure_event_start":life.get("created_at"),"structure_event_age_bars":life.get("age_bars"),
                "structure_event_expired":life.get("expired"),"structure_event_freshness":life.get("freshness","unknown"),
                "liquidity_event_start":liq_life.get("created_at"),"liquidity_event_age_bars":liq_life.get("age_bars"),
                "liquidity_event_expired":liq_life.get("expired"),"liquidity_event_freshness":liq_life.get("freshness","unknown")}

    @staticmethod
    def _smc_scenario_v37(a, direction):
        smc=a.get("smart_money",{}) or {}; sc=dict(smc.get("scenario") or {})
        sc.setdefault("direction",direction if direction in ("bullish","bearish") else None)
        sc["direction_aligned"]=sc.get("direction") in (None,direction) if direction in ("bullish","bearish") else False
        sc["active_fvg_count"]=sum(1 for z in smc.get("fvg",[]) if not z.get("expired"))
        sc["order_block_count"]=sum(1 for z in smc.get("order_blocks",[]) if z.get("fresh",True))
        sc["bpr_count"]=len(smc.get("bpr",[])); sc["poi_count"]=len(smc.get("poi",[]))
        return sc

    @staticmethod
    def _execution_engine_v37(linear, spot):
        # Preserve V3.6 calculations, but capability-aware transport/flow readiness.
        base=DynamicMarketManager._execution_engine_v3(linear,spot)
        spot_available=spot.get("available")
        # False is authoritative subscription rejection; None means not established yet.
        mode="perp_only" if spot_available is False else "spot_perp"
        windows=base.get("windows",{})

        # A listed Spot market can be technically active yet too illiquid to be a
        # useful execution/driver source.  Do not leave such symbols in DATA_BLOCK
        # forever and do not pretend Spot is absent: after a real observation period
        # degrade explicitly to perp_dominant.  This mode requires usable perp tape,
        # usable OI on the same horizon, and fresh linear book/ticker.
        # Collector age survives reconnects; session age does not.
        spot_age=float(spot.get("collector_age_seconds") or spot.get("session_age_seconds") or 0.0)
        perp_quality_windows=[w for w,d in windows.items()
                              if d.get("linear_warm") and d.get("perp_flow_usable")
                              and float(d.get("perp_flow_quality",{}).get("quality") or 0.0)>0]
        paired_windows=[w for w,d in windows.items()
                        if d.get("linear_warm") and d.get("spot_warm")
                        and d.get("perp_flow_usable") and d.get("spot_flow_usable")]
        spot15=windows.get("15m",{})
        spot_sparse=bool(
            spot_available is True
            and spot_age >= 600.0
            and perp_quality_windows
            and not paired_windows
            and (int(spot15.get("spot_trade_count") or 0) < 6
                 or float((spot15.get("spot_flow_quality") or {}).get("coverage_ratio") or 0.0) < 0.50)
        )
        if spot_sparse:
            mode="perp_dominant"

        if mode in ("perp_only","perp_dominant"):
            warmed=[w for w,d in windows.items()
                    if d.get("linear_warm") and d.get("perp_flow_usable")
                    and d.get("oi_usable")
                    and float(d.get("perp_flow_quality",{}).get("quality") or 0.0)>0]
            base["warmed_flow_windows"]=warmed
            base["flow_ready"]=bool(warmed)
            base["driver_ready"]=bool(warmed)
            pq=max((float(windows[w].get("perp_flow_quality",{}).get("quality") or 0.0) for w in warmed), default=0.0)
            maturity_caps = ({0:0.0,1:0.45,2:0.60,3:0.72,4:0.80}
                             if mode=="perp_dominant"
                             else {0:0.0,1:0.55,2:0.75,3:0.90,4:0.97})
            maturity_cap=maturity_caps.get(len(warmed), max(maturity_caps.values()))
            pq=min(pq,maturity_cap)
            driver_name=mode if warmed else "unknown"
            base["driver"]={"primary":driver_name,
                            "confidence":round(pq,4),"votes":{mode:round(pq,4)}}
            base["driver_ready"]=bool(warmed and pq>=0.20)
            ticker=(linear.get("ticker") or {})
            tu=fnum(ticker.get("updated_at")); tfresh=bool(tu is not None and time.time()-tu<=15.0)
            base["ticker_age_seconds"]=round(time.time()-tu,3) if tu is not None else None
            base["ticker_fresh"]=tfresh
            base["ready"]=bool(linear.get("connected") and (linear.get("orderbook") or {}).get("ready") and tfresh)
            base["trade_data_ready"]=bool(base["ready"] and base.get("price") is not None and base["driver_ready"] and warmed)
            base["flow_divergences"]=[]
            base["spot_liquidity_degraded"] = bool(mode=="perp_dominant")
            base["spot_observation_age_seconds"] = round(spot_age,2)
            base["spot_degradation_reason"] = ("spot_insufficient_liquidity" if mode=="perp_dominant" else None)
        else:
            base["spot_liquidity_degraded"] = False
            base["spot_observation_age_seconds"] = round(spot_age,2)
            base["spot_degradation_reason"] = None
        base["execution_mode"]=mode
        base["market_capabilities"]={"linear":linear.get("available") is not False,
                                     "spot":spot_available is not False,
                                     "spot_state":spot_available,
                                     "linear_capability_state":linear.get("capability_state","unknown"),
                                     "spot_capability_state":spot.get("capability_state","unknown")}

        # Rich execution regime; context only, never a direction vote by itself.
        regime="unknown"
        for w in ("1m","5m","15m","1h"):
            d=windows.get(w,{})
            if not d.get("linear_warm") or not d.get("perp_flow_usable"): continue
            if mode=="spot_perp" and (not d.get("spot_warm") or not d.get("spot_flow_usable")): continue
            if mode in ("perp_only","perp_dominant") and not d.get("oi_usable"): continue
            pd=d.get("perp_delta_ratio"); sd=d.get("spot_delta_ratio"); oi=d.get("oi_change_pct"); pc=d.get("perp_price_change_pct")
            if pd is None or pc is None: continue
            if mode=="perp_only": regime="perp_only"; break
            if mode=="perp_dominant": regime="perp_dominant"; break
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
                if not ob.get("fresh") or ob.get("expired") or ob.get("invalidated") or not ob.get("caused_displacement"): continue
                x=ob.get("low") if direction=="bullish" else ob.get("high")
                if x and ((direction=="bullish" and x<price) or (direction=="bearish" and x>price)):
                    add_unique(invalid,x,tf,"order_block",rank-0.15,{"start":ob.get("start")})
            sw=q.get("event") if isinstance(q.get("event"),dict) else (q.get("sweep") if isinstance(q.get("sweep"),dict) else {})
            sx=sw.get("price") or sw.get("level")
            if sx and ((direction=="bullish" and sx<price) or (direction=="bearish" and sx>price)):
                add_unique(invalid,sx,tf,"liquidity_sweep_extreme",rank-0.35,{"sweep":sw})
            # Deep SMC thesis has precedence over generic swing levels, but only on the correct side.
            sc=m.get("scenario") or {}; thesis=sc.get("invalidation_thesis") or {}; tx=thesis.get("price")
            if tx and ((direction=="bullish" and tx<price) or (direction=="bearish" and tx>price)):
                add_unique(invalid,tx,tf,"smc_thesis",rank-0.5,{"thesis":thesis})

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
            # Actionable liquidity targets come only from the lifecycle-aware liquidity map.
            # Legacy equal_high/equal_low diagnostics are never used directly because they may already be swept.
            objective=q.get("nearest_buy_side") if direction=="bullish" else q.get("nearest_sell_side")
            if isinstance(objective,dict):
                x=objective.get("price")
                if x and ((direction=="bullish" and x>price) or (direction=="bearish" and x<price)):
                    add_unique(targets,x,tf,"liquidity_objective",rank-0.4,{"liquidity":objective})
            # Unfilled FVG can be a magnet/target; use nearest boundary beyond price.
            for f in m.get("fvg",[]) or []:
                if f.get("fully_filled") or f.get("expired"): continue
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
        missed_entry=bool(atr and ((direction=="bullish" and price-(reg("5").get("supports") or [price])[0] > 3*atr) if (reg("5").get("supports") or []) else False))
        if direction=="bearish" and atr and (reg("5").get("resistances") or []):
            missed_entry=bool((reg("5").get("resistances") or [price])[0]-price > 3*atr)

        # Setup/trigger are timing gates, not direction votes.
        setup_state=mtf.get("setup_state"); trigger_state=mtf.get("trigger_state")
        smc5=DynamicMarketManager._smc_scenario_v37(analysis.get("5",{}),direction)
        smc1=DynamicMarketManager._smc_scenario_v37(analysis.get("1",{}),direction)
        smc1_stage=smc1.get("stage")
        trigger_ok=bool(trigger_state=="aligned" and (smc1_stage in ("MSS_CONFIRMED","POI_CREATED","WAIT_RETRACE","IN_POI") or smc1.get("mss",{}).get("confirmed") or smc1.get("displacement")))

        flow_divs=execution.get("flow_divergences",[]) or []
        flow_opposed=any((direction=="bullish" and e.get("type")=="price_up_perp_led_spot_not_confirming") or (direction=="bearish" and e.get("type")=="price_down_perp_led_spot_not_confirming") for e in flow_divs)
        book=execution.get("book_pressure","neutral"); execution_ok=book in ("neutral",direction)

        data_reasons=[]; market_reasons=[]; exec_reasons=[]
        # V3.8.3 invariant: no path to SETUP may bypass an upstream hard invalidation.
        hard_invalidations=[]
        for tf in ("60","15","5","1"):
            eg=(analysis.get(tf,{}) or {}).get("evidence_graph") or {}
            for item in eg.get("hard_invalidations",[]) or []:
                hard_invalidations.append({"timeframe":tf,**item})
        if hard_invalidations:
            market_reasons.append("upstream_hard_invalidation")
        if not mtf.get("ready"): data_reasons.append("mtf_not_ready")
        if not execution.get("ready"): data_reasons.append("transport_not_ready")
        if not execution.get("trade_data_ready"): data_reasons.append("trade_data_not_ready")
        if direction=="neutral": market_reasons.append("direction_not_confirmed")
        if (mtf.get("direction") or {}).get("state")!="confirmed": market_reasons.append("direction_provisional")
        if extreme_extension: market_reasons.append("extended_wait_pullback")
        if stop is None: market_reasons.append("invalidation_missing")
        if t1 is None: market_reasons.append("target_missing")
        if not order_ok: market_reasons.append("price_order_invalid")
        if rr is None or rr<MIN_RR: market_reasons.append("rr_below_min_2_0")
        if not scale_ok: market_reasons.append(scale_reason or "scale_mismatch")
        if setup_state=="pullback": exec_reasons.append("setup_pullback_active")
        elif setup_state!="aligned": exec_reasons.append("setup_not_aligned")
        if not trigger_ok: exec_reasons.append("trigger_not_confirmed")
        if flow_opposed: exec_reasons.append("flow_opposed")
        if not execution_ok: exec_reasons.append("book_opposed")

        if data_reasons:
            state="DATA_BLOCK"; block_class="DATA_BLOCK"; retry=True
        elif hard_invalidations:
            state="INVALID"; block_class="MARKET_BLOCK"; retry=False
        elif direction=="neutral" or "direction_provisional" in market_reasons:
            # A missed entry cannot be declared before the directional thesis exists.
            state="WAIT_DIRECTION"; block_class="MARKET_BLOCK"; retry=True
        elif any(r in market_reasons for r in ("invalidation_missing","target_missing","price_order_invalid","rr_below_min_2_0","invalidation_from_htf","stop_too_many_atr","stop_pct_too_large","scale_mismatch")):
            state="INVALID"; block_class="MARKET_BLOCK"; retry=False
        elif missed_entry:
            state="MISSED_ENTRY"; block_class="EXECUTION_BLOCK"; retry=True
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

        # ---------- V3.11 conditional LIMIT_PLAN ----------
        # A limit plan is a conditional order proposal, not a market-entry signal.
        # Offer it for a confirmed structural pullback/setup even when current-price
        # RR is below 1.5. The proposA limit fill itself must provide >=2.0R using
        # the SAME thesis stop and T1. Never bypass hard invalidation.
        limit_plan={"eligible":False,"state":"NO_LIMIT_PLAN","reason":None}
        if state in ("WAIT_TRIGGER","WAIT_PULLBACK","WAIT_SETUP"):
            lp_required=bool(
                direction in ("bullish","bearish")
                and (mtf.get("direction") or {}).get("state")=="confirmed"
                and stop is not None and t1 is not None
                and order_ok and scale_ok
                and not hard_invalidations
                and not any(r in market_reasons for r in ("invalidation_missing","target_missing","price_order_invalid"))
            )
            if lp_required:
                min_rr=MIN_RR
                # Exact entry boundary required to preserve min_rr with the SAME stop/T1.
                if direction=="bullish":
                    rr_boundary=(t1 + min_rr*stop)/(1.0+min_rr)
                    retrace_ok=lambda x: stop < x < price
                    rr_ok=lambda x: (t1-x)/(x-stop) if x>stop else None
                    side="BUY_LIMIT"
                else:
                    rr_boundary=(t1 + min_rr*stop)/(1.0+min_rr)
                    retrace_ok=lambda x: price < x < stop
                    rr_ok=lambda x: (x-t1)/(stop-x) if x<stop else None
                    side="SELL_LIMIT"

                candidates=[]
                def lp_add(value, tf, source, weight):
                    try: x=float(value)
                    except (TypeError,ValueError): return
                    if not math.isfinite(x) or not retrace_ok(x): return
                    r=rr_ok(x)
                    if r is None or r < min_rr: return
                    # Do not park the order effectively on top of the stop.
                    risk_abs=abs(x-stop)
                    floor=max((atr or 0)*0.20, spread_abs*3)
                    if floor and risk_abs < floor: return
                    candidates.append({"price":x,"timeframe":tf,"source":source,
                                       "rr":r,"weight":weight,
                                       "distance_pct":abs(x-price)/price*100})

                # Structure first.
                for tf,weight in (("1",0.0),("5",0.1),("15",0.25)):
                    r=reg(tf)
                    levels=r.get("supports",[]) if direction=="bullish" else r.get("resistances",[])
                    for x in levels: lp_add(x,tf,"structural_retest",weight)

                    # Fibonacci clusters are independent confluence levels.
                    fib=(analysis.get(tf,{}) or {}).get("fibonacci",{}) or {}
                    for c in fib.get("clusters",[]) or []:
                        if isinstance(c,dict):
                            lp_add(c.get("price"),tf,"fib_cluster",weight+0.05)

                    # Fresh same-direction order block boundary/POI.
                    m=smc(tf)
                    for ob in m.get("order_blocks",[]) or []:
                        if ob.get("direction")!=direction: continue
                        if not ob.get("fresh") or ob.get("expired") or ob.get("invalidated"): continue
                        vals=[ob.get("low"),ob.get("high")]
                        for x in vals: lp_add(x,tf,"order_block",weight-0.05)

                # Prefer nearest fillable valid retracement; source weight breaks ties.
                candidates.sort(key=lambda c:(c["distance_pct"],c["weight"],-c["rr"]))
                chosen=candidates[0] if candidates else None
                if chosen:
                    entry=chosen["price"]
                    zone_half=max(spread_abs*2, (atr or 0)*0.10)
                    if direction=="bullish":
                        zlo=max(stop+1e-12,entry-zone_half); zhi=min(price-1e-12,entry+zone_half)
                    else:
                        zlo=max(price+1e-12,entry-zone_half); zhi=min(stop-1e-12,entry+zone_half)
                    final_rr=rr_ok(entry)
                    limit_plan={
                        "eligible":True,
                        "state":"LIMIT_PLAN",
                        "side":side,
                        "entry":round(entry,10),
                        "entry_zone":[round(zlo,10),round(zhi,10)],
                        "stop":round(stop,10),
                        "targets":{
                            "t1":round(t1,10),
                            "t2":None if t2 is None else round(t2,10),
                            "t1_source":None if not target1 else f"{target1[2]}_{target1[1]}",
                            "t2_source":None if not target2 else f"{target2[2]}_{target2[1]}",
                        },
                        "risk_reward":round(final_rr,3),
                        "minimum_rr":min_rr,
                        "rr_basis":"limit_entry_to_stop_and_t1",
                        "rr_boundary":round(rr_boundary,10),
                        "entry_basis":{
                            "source":chosen["source"],"timeframe":chosen["timeframe"],
                            "distance_from_market_pct":round(chosen["distance_pct"],4)
                        },
                        "execution_warning":"conditional_limit_not_market_signal",
                        "requires_before_fill":["trigger_confirmation","flow_not_opposed","book_not_opposed"],
                        "cancel_if":[
                            "direction_not_confirmed",
                            "structural_invalidation",
                            "upstream_hard_invalidation",
                            "trade_data_not_ready",
                            "target_or_scale_invalid",
                        ],
                        "place_now":bool(not flow_opposed and execution_ok),
                    }
                else:
                    limit_plan={"eligible":False,"state":"NO_LIMIT_PLAN",
                                "reason":"no_structural_retracement_level_preserves_min_rr",
                                "rr_boundary":round(rr_boundary,10)}
            else:
                limit_plan={"eligible":False,"state":"NO_LIMIT_PLAN",
                            "reason":"conditional_limit_prerequisites_failed"}

        reasons=data_reasons+market_reasons+exec_reasons
        return {
            "ready":True,"status":state,"trade_state":state,"direction":direction if direction!="neutral" else None,
            "entry_reference":price,
            "invalidation":stop,
            "invalidation_thesis":{"logical_level":logical,"source":inv_source,"source_timeframe":inv_tf,"buffer":buffer,"noise_floor":noise,"rule":"stop_beyond_thesis_level_then_noise_buffer"},
            "targets":{
                "t1":t1,"t1_source":None if not target1 else f"{target1[2]}_{target1[1]}","t1_timeframe":None if not target1 else target1[1],
                "t2":t2,"t2_source":None if not target2 else f"{target2[2]}_{target2[1]}","t2_timeframe":None if not target2 else target2[1],
                "rr_basis":"scan_price_to_stop_and_t1","minimum_rr":MIN_RR,"rule":"nearest_valid_scenario_target_same_intraday_scale_first"
            },
            "risk_reward":None if rr is None else round(rr,3),"risk_distance":risk,"reward_distance":reward,
            "trade_scale":{"stop_pct":None if stop_pct is None else round(stop_pct,4),"target_pct":None if target_pct is None else round(target_pct,4),"stop_atr":None if stop_atr is None else round(stop_atr,3),"valid":scale_ok,"reason":scale_reason,"execution_atr":atr,"invalidation_timeframe":inv_tf},
            "scenario_coherence":{"direction":direction,"setup_timeframe":"5","trigger_timeframe":"1","invalidation_timeframe":inv_tf,"t1_timeframe":None if not target1 else target1[1],"rr_uses_t1":True,"stop_moved_for_rr":False},
            "smc_scenario":{"5m":smc5,"1m":smc1},"hard_invalidations":hard_invalidations,"block_class":block_class,"block_reasons":reasons,"retryable":retry,
            "setup_state":setup_state,"trigger_state":trigger_state,
            "limit_plan":limit_plan,
            "opportunity_state":(
                "MARKET_READY" if state=="SETUP"
                else "LIMIT_READY" if limit_plan.get("eligible")
                else "WATCH"
            ),
            "trigger_direction_aligned":bool(trigger_state=="aligned"),
            "trigger_confirmed":trigger_ok,
            "trade_style":mtf.get("trade_style"),
            "execution_regime":execution.get("execution_regime"),"dislocated_execution_timeframes":dislocated,
            "required":{"direction_confirmed":bool(direction!="neutral" and (mtf.get("direction") or {}).get("state")=="confirmed"),"transport_ready":bool(execution.get("ready")),"trade_data_ready":bool(execution.get("trade_data_ready")),"price_order_valid":order_ok,"rr_min_2_0":rr is not None and rr>=MIN_RR,"trade_scale_valid":scale_ok,"setup_aligned":setup_state=="aligned","trigger_confirmed":trigger_ok,"flow_not_opposed":not flow_opposed,"execution_not_opposed":execution_ok,"no_upstream_hard_invalidation":not bool(hard_invalidations)}
        }

    def realtime_readiness(self, symbol):
        """Probe only realtime execution/flow readiness; no full Scan+ decision pass."""
        symbol = self.activate(symbol)
        with self.lock:
            self.last_access[symbol] = time.time()
            pair = self.streams[symbol]
        linear = pair["linear"].snapshot()
        spot = pair["spot"].snapshot()
        execution = self._execution_engine_v37(linear, spot)
        return {
            "symbol": symbol,
            "ready": bool(execution.get("ready")),
            "trade_data_ready": bool(execution.get("trade_data_ready")),
            "flow_ready": bool(execution.get("flow_ready")),
            "driver_ready": bool(execution.get("driver_ready")),
            "execution_mode": execution.get("execution_mode"),
            "warmed_flow_windows": execution.get("warmed_flow_windows") or [],
        }

    @staticmethod
    def _data_quality_engine_v1(linear, spot, execution, mtf, setup):
        """Separate data/readiness diagnostics from trade-decision state."""
        analysis = linear.get("analysis") or {}
        windows = execution.get("windows") or {}
        now = time.time()

        def fresh_ticker():
            ts = fnum((linear.get("ticker") or {}).get("updated_at"))
            return bool(ts is not None and now - ts <= 15.0)

        tf_ready = {
            tf: bool((analysis.get(tf) or {}).get("ready"))
            for tf in ("D", "240", "60", "15", "5", "1")
        }
        flow_ready = {
            w: bool((windows.get(w) or {}).get("linear_warm")
                    and (windows.get(w) or {}).get("perp_flow_usable"))
            for w in ("1m", "5m", "15m", "1h")
        }
        oi_ready = {
            w: bool((windows.get(w) or {}).get("oi_usable"))
            for w in ("1m", "5m", "15m", "1h")
        }

        checks = {
            "linear_transport": bool(linear.get("connected")),
            "linear_capability": linear.get("capability_state") in ("subscribed", "active"),
            "realtime_price": execution.get("price") is not None,
            "ticker_fresh": fresh_ticker(),
            "orderbook_ready": bool((linear.get("orderbook") or {}).get("ready")),
            "history_D": tf_ready["D"],
            "history_4H": tf_ready["240"],
            "history_1H": tf_ready["60"],
            "history_15m": tf_ready["15"],
            "history_5m": tf_ready["5"],
            "history_1m": tf_ready["1"],
            "flow_1m": flow_ready["1m"],
            "flow_5m": flow_ready["5m"],
            "flow_15m": flow_ready["15m"],
            "flow_1h": flow_ready["1h"],
            "oi_1m": oi_ready["1m"],
            "oi_5m": oi_ready["5m"],
            "oi_15m": oi_ready["15m"],
            "oi_1h": oi_ready["1h"],
        }

        missing = [name for name, ok in checks.items() if not ok]
        analysis_ready = all(checks[k] for k in (
            "history_D", "history_4H", "history_1H", "history_15m", "history_5m"
        ))
        realtime_ready = all(checks[k] for k in (
            "linear_transport", "linear_capability", "realtime_price",
            "ticker_fresh", "orderbook_ready"
        ))
        flow_windows = [w for w, ok in flow_ready.items() if ok]
        oi_windows = [w for w, ok in oi_ready.items() if ok]
        trade_data_ready = bool(execution.get("trade_data_ready"))
        execution_ready = bool(execution.get("ready") and trade_data_ready)
        decision_ready = bool(
            analysis_ready and realtime_ready and execution_ready
            and setup.get("block_class") != "DATA_BLOCK"
        )

        return {
            "version": "data_quality_v1",
            "checks": checks,
            "missing": missing,
            "analysis_ready": analysis_ready,
            "realtime_ready": realtime_ready,
            "flow_ready": bool(flow_windows),
            "oi_ready": bool(oi_windows),
            "trade_data_ready": trade_data_ready,
            "execution_ready": execution_ready,
            "decision_ready": decision_ready,
            "flow_windows": flow_windows,
            "oi_windows": oi_windows,
            "execution_mode": execution.get("execution_mode"),
            "spot_capability_state": spot.get("capability_state", "unknown"),
            "spot_available": spot.get("available"),
            "spot_capability_resolved": spot.get("capability_state")
                in ("subscribed", "active", "unsupported"),
            "block_reasons": list(dict.fromkeys(
                (setup.get("block_reasons") or [])
                + ([] if analysis_ready else ["analysis_history_not_ready"])
                + ([] if realtime_ready else ["realtime_market_data_not_ready"])
                + ([] if trade_data_ready else ["execution_trade_data_not_ready"])
            )),
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

        # Cold-start race guard: activate() starts websocket threads asynchronously.
        # Wait outside the manager lock so stream threads can resolve normally.
        self._wait_initial_realtime_resolution(pair)

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
        data_quality = self._data_quality_engine_v1(linear, spot, execution, mtf, setup)

        return {
            "symbol": symbol,
            "generated_at": time.time(),
            "engine_version": "scan_plus_v3_10_data_quality",
            "linear": linear,
            "spot": spot,
            "driver": self._driver(linear, spot),
            "data_quality": data_quality,
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
        data_quality=full.get("data_quality") or {}

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
                    "dealing_range":smc.get("dealing_range"),"scenario":smc.get("scenario"),"protected_level":smc.get("protected_level"),"inducement":smc.get("inducement"),
                    "displacement":smc.get("displacement"),"mss":smc.get("mss"),"poi":smc.get("poi",[])[:4],"bpr":smc.get("bpr",[])[:3],
                    "fvg":smc.get("fvg",[])[:4],"order_blocks":smc.get("order_blocks",[])[:3],
                },
                "evidence_graph":a.get("evidence_graph",{}),
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
            "data_quality":data_quality,
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
                "orderbook_age_seconds":execution.get("orderbook_age_seconds"),
                "ticker_age_seconds":execution.get("ticker_age_seconds"),
                "ticker_fresh":execution.get("ticker_fresh"),
                "spot_liquidity_degraded":execution.get("spot_liquidity_degraded",False),
                "spot_observation_age_seconds":execution.get("spot_observation_age_seconds"),
                "spot_degradation_reason":execution.get("spot_degradation_reason"),
                "windows":execution.get("windows",{}),
            },
            "setup":{
                **setup,
                "failed_requirements":failed,
            },
            "manipulation": detect_pump_exhaustion(linear, full.get("spot",{}))
        }
        result["_frames"]={tf:list(rows) for tf,rows in (linear.get("candles") or {}).items()}
        return enrich_scan(result, market="crypto")

    def replay_no_lookahead(self, candles_by_tf, checkpoints=None):
        """Deterministic prefix replay for regression tests.

        Each checkpoint analyzes only candles that were fully closed by that
        checkpoint. A candle whose start precedes the checkpoint but whose end is in
        the future is excluded even if the stored row later has confirm=True. This is
        intentionally a test harness, not a trading signal endpoint.
        """
        checkpoints = list(checkpoints or [])
        if not checkpoints:
            ends=sorted({int(r.get("end")) for rows in candles_by_tf.values() for r in rows if r.get("end") is not None and r.get("confirm")})
            checkpoints=ends[-10:]
        out=[]
        # Use a bare MarketStream analysis object: analysis methods do not require WS.
        analyzer=MarketStream.__new__(MarketStream)
        for cp in checkpoints:
            frame={}
            for tf,rows in candles_by_tf.items():
                prefix=[dict(r) for r in rows
                        if r.get("confirm") and r.get("end") is not None and int(r.get("end",0))<=int(cp)]
                frame[tf]=analyzer._analysis_bundle(prefix) if prefix else {"ready":False}
            out.append({"checkpoint":cp,"analysis":frame})
        return out

    def diagnostics(self, symbol):
        symbol = self.activate(symbol)
        with self.lock:
            self.last_access[symbol] = time.time()
            pair = self.streams[symbol]
        # Diagnostics must observe the same cold-start barrier as /scan; otherwise
        # the two endpoints can contradict each other for a newly activated symbol.
        self._wait_initial_realtime_resolution(pair)
        linear = pair["linear"].diagnostics()
        spot = pair["spot"].diagnostics()
        return {
            "symbol": symbol,
            "generated_at": time.time(),
            "linear": linear,
            "spot": spot,
            "overall": {
                "execution_mode": "perp_only" if spot.get("available") is False else "spot_perp",
                "realtime_pass": bool(linear["realtime_pass"] and (spot["realtime_pass"] if spot.get("available") is not False else True)),
                "historical_analysis_pass": bool(linear.get("analysis_full_6_of_6")),
                "linear_technical_pass": bool(linear["full_technical_pass"]),
                "spot_realtime_only": True,
                "spot_required": spot.get("available") is not False,
                "spot_capability_state": spot.get("capability_state","unknown"),
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
# PRESCAN V1 -- LIGHTWEIGHT MARKET DISCOVERY ONLY
# ============================================================

class PreScanEngine:
    """Lightweight candidate discovery layer for Scan+.

    PreScan deliberately does NOT call _analysis_bundle() and does not produce a
    trade decision.  It reuses the canonical Structure/ATR primitives from
    MarketStream, then ranks only whether a symbol deserves a full Scan+ run.
    """

    VERSION = "prescan_v1"
    TF_REQUIRED = ("5", "15", "60")
    MIN_CANDLES = {"5": 80, "15": 80, "60": 80}
    STATUS_ORDER = {"REJECT": 0, "COLD": 1, "WARMING": 2, "HOT": 3}

    @staticmethod
    def _closed_rows(stream, interval):
        """Return closed candles only; never let the live candle drive discovery."""
        with stream.lock:
            rows = [dict(c) for c in stream.candles.get(interval, ())]
        return [c for c in rows if c.get("confirm") is True]

    @staticmethod
    def _direction(structure):
        state = structure.get("state")
        if state == "uptrend":
            return "bullish"
        if state == "downtrend":
            return "bearish"
        return "neutral"

    @staticmethod
    def _median(values):
        vals = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
        if not vals:
            return None
        n = len(vals)
        mid = n // 2
        return vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2.0

    @classmethod
    def _activity(cls, rows):
        """Closed-candle RVOL + range expansion/compression; measurement, not signal."""
        if len(rows) < 31:
            return {"ready": False}
        recent = rows[-1]
        baseline = rows[-31:-1]
        volumes = [fnum(c.get("volume")) or 0.0 for c in baseline]
        ranges = [max(0.0, float(c["high"]) - float(c["low"])) for c in baseline]
        base_vol = cls._median(volumes)
        base_range = cls._median(ranges)
        cur_vol = fnum(recent.get("volume")) or 0.0
        cur_range = max(0.0, float(recent["high"]) - float(recent["low"]))
        rvol = cur_vol / base_vol if base_vol and base_vol > 0 else None
        range_ratio = cur_range / base_range if base_range and base_range > 0 else None

        short_ranges = [max(0.0, float(c["high"]) - float(c["low"])) for c in rows[-6:-1]]
        long_ranges = [max(0.0, float(c["high"]) - float(c["low"])) for c in rows[-31:-6]]
        short_med = cls._median(short_ranges)
        long_med = cls._median(long_ranges)
        compression_ratio = short_med / long_med if short_med is not None and long_med and long_med > 0 else None

        if rvol is not None and range_ratio is not None and rvol >= 1.5 and range_ratio >= 1.2:
            lifecycle = "awakening"
        elif compression_ratio is not None and compression_ratio <= 0.70:
            lifecycle = "compression"
        elif range_ratio is not None and range_ratio >= 2.0:
            lifecycle = "extended_activity"
        else:
            lifecycle = "normal"
        return {
            "ready": True,
            "rvol": None if rvol is None else round(rvol, 4),
            "range_ratio": None if range_ratio is None else round(range_ratio, 4),
            "compression_ratio": None if compression_ratio is None else round(compression_ratio, 4),
            "lifecycle": lifecycle,
        }

    @staticmethod
    def _location(rows, structure, direction):
        """5m location only. No OB/FVG/SMC/entry/target inference lives here."""
        if not rows:
            return {"ready": False}
        price = fnum(rows[-1].get("close"))
        atr = MarketStream._atr(rows)
        hi = structure.get("last_swing_high") or {}
        lo = structure.get("last_swing_low") or {}
        hi_p, lo_p = fnum(hi.get("price")), fnum(lo.get("price"))
        if price is None or atr is None or atr <= 0:
            return {"ready": False, "price": price, "atr": atr}
        dist_hi = abs(hi_p - price) / atr if hi_p is not None else None
        dist_lo = abs(price - lo_p) / atr if lo_p is not None else None
        nearest = min(v for v in (dist_hi, dist_lo) if v is not None) if any(v is not None for v in (dist_hi, dist_lo)) else None
        phase = structure.get("phase", "unknown")
        # Extension is intentionally conservative and symmetric. It only says
        # whether price is far from the nearest confirmed structural reference.
        extended = bool(nearest is not None and nearest > 3.0)
        favorable_phase = phase in ("correction", "transition")
        if direction == "neutral":
            favorable_phase = False
        return {
            "ready": True,
            "price": price,
            "atr": round(float(atr), 10),
            "phase": phase,
            "distance_to_swing_high_atr": None if dist_hi is None else round(dist_hi, 4),
            "distance_to_swing_low_atr": None if dist_lo is None else round(dist_lo, 4),
            "nearest_structure_atr": None if nearest is None else round(nearest, 4),
            "extended": extended,
            "favorable_phase": favorable_phase,
        }

    @staticmethod
    def _perp_participation(stream):
        """Small participation check from already-collected linear trades/OI only."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - 15 * 60_000
        with stream.lock:
            # Native MarketStream storage is tuple-based:
            # trades=(timestamp_ms, side, price, qty), OI=(timestamp_ms, value).
            trades = [tuple(t) for t in stream.trades]
            oi = [tuple(o) for o in stream.oi_samples]
        valid = []
        for t in trades:
            if len(t) < 4:
                continue
            try:
                ts = int(t[0])
            except (TypeError, ValueError):
                continue
            price, qty = fnum(t[2]), fnum(t[3])
            if ts >= cutoff and price is not None and qty is not None and price > 0 and qty > 0:
                valid.append((ts, price, qty))
        turnover = sum(p * q for _, p, q in valid)
        coverage = (max(x[0] for x in valid) - min(x[0] for x in valid)) if len(valid) >= 2 else 0
        oi_change_pct = None
        clean_oi = []
        for x in oi:
            if len(x) < 2:
                continue
            try:
                ts = int(x[0])
            except (TypeError, ValueError):
                continue
            value = fnum(x[1])
            if ts >= cutoff and value is not None and value > 0:
                clean_oi.append((ts, value))
        clean_oi.sort(key=lambda item: item[0])
        if len(clean_oi) >= 2 and clean_oi[0][1] > 0:
            oi_change_pct = (clean_oi[-1][1] / clean_oi[0][1] - 1.0) * 100.0
        return {
            "ready": len(valid) >= 6 and coverage >= 7.5 * 60_000,
            "trade_count_15m": len(valid),
            "coverage_ms_15m": coverage,
            "turnover_15m": round(turnover, 4),
            "oi_change_pct_15m": None if oi_change_pct is None else round(oi_change_pct, 4),
        }

    @classmethod
    def analyze_stream(cls, stream):
        """Rank one *existing linear stream* without running the heavy Scan+ bundle."""
        if stream.market != "linear":
            raise ValueError("PreScan requires a linear MarketStream")
        frames = {}
        for tf in cls.TF_REQUIRED:
            rows = cls._closed_rows(stream, tf)
            enough = len(rows) >= cls.MIN_CANDLES[tf]
            structure = stream._structure_metrics(rows) if enough else {"ready": False, "state": "insufficient_data", "phase": "unknown"}
            frames[tf] = {"rows": rows, "structure": structure, "direction": cls._direction(structure)}

        missing = [tf for tf in cls.TF_REQUIRED if not frames[tf]["structure"].get("ready")]
        if missing:
            return {
                "engine_version": cls.VERSION,
                "symbol": stream.symbol,
                "status": "REJECT",
                "eligible_for_scan_plus": False,
                "priority": 0.0,
                "reasons": ["insufficient_closed_structure:" + ",".join(missing)],
            }

        d60, d15 = frames["60"]["direction"], frames["15"]["direction"]
        aligned = d60 == d15 and d60 in ("bullish", "bearish")
        direction = d60 if aligned else "neutral"
        loc = cls._location(frames["5"]["rows"], frames["5"]["structure"], direction)
        activity5 = cls._activity(frames["5"]["rows"])
        activity15 = cls._activity(frames["15"]["rows"])
        perp = cls._perp_participation(stream)

        reasons = []
        if not aligned:
            reasons.append("1h_15m_not_aligned")
        if loc.get("extended"):
            reasons.append("price_extended_from_structure")

        # Ranking is intentionally NOT a trade probability. It only orders which
        # candidates should consume the expensive full Scan+ next.
        priority = 0.0
        if aligned:
            priority += 45.0
        if loc.get("favorable_phase"):
            priority += 15.0
        if not loc.get("extended"):
            priority += 10.0
        if activity5.get("lifecycle") in ("awakening", "compression"):
            priority += 12.0
        if activity15.get("lifecycle") in ("awakening", "compression"):
            priority += 8.0
        if perp.get("ready"):
            priority += 10.0
        priority = round(min(priority, 100.0), 2)

        hard_reject = not aligned or bool(loc.get("extended"))
        if hard_reject:
            status = "COLD"
        elif priority >= 80:
            status = "HOT"
        elif priority >= 60:
            status = "WARMING"
        else:
            status = "COLD"
        if not reasons:
            reasons.append("candidate_discovery_only")

        return {
            "engine_version": cls.VERSION,
            "symbol": stream.symbol,
            "status": status,
            "eligible_for_scan_plus": status in ("HOT", "WARMING"),
            "priority": priority,
            "direction": {
                "aligned": aligned,
                "effective": direction,
                "1h": d60,
                "15m": d15,
            },
            "location_5m": loc,
            "activity": {"5m": activity5, "15m": activity15},
            "perp_participation": perp,
            "reasons": reasons,
            "guardrails": {
                "trade_decision": False,
                "uses_analysis_bundle": False,
                "uses_spot": False,
                "uses_elliott": False,
                "uses_harmonics": False,
                "uses_fib": False,
                "uses_smc": False,
                "uses_orderbook": False,
                "uses_entry_stop_targets": False,
            },
        }

    @classmethod
    def rank_existing_streams(cls, manager, limit=8):
        """Development hook only: rank streams already active in Scan+.

        Universe acquisition is intentionally a separate next step; this method
        never activates symbols and therefore cannot churn DynamicMarketManager.
        """
        with manager.lock:
            linear_streams = [pair["linear"] for pair in manager.streams.values()]
        rows = []
        for stream in linear_streams:
            try:
                rows.append(cls.analyze_stream(stream))
            except Exception as exc:
                rows.append({
                    "engine_version": cls.VERSION,
                    "symbol": getattr(stream, "symbol", None),
                    "status": "REJECT",
                    "eligible_for_scan_plus": False,
                    "priority": 0.0,
                    "reasons": [f"prescan_error:{type(exc).__name__}"],
                })
        rows.sort(key=lambda x: (x.get("priority", 0.0), x.get("symbol") or ""), reverse=True)
        return rows[:max(1, int(limit))]


prescan_engine = PreScanEngine()

# ============================================================
# GLOBAL MANAGER
# ============================================================

dynamic_manager = DynamicMarketManager(
    max_symbols=6,
    idle_timeout=3600,
)

# ============================================================
# BYBIT WS PRESCAN PROBE -- ISOLATED, NO SCAN+ SIDE EFFECTS
# ============================================================

class BybitPreScanWSProbe:
    """One-shot feasibility probe for a lightweight Bybit-linear PreScan feed.

    This class is deliberately isolated from DynamicMarketManager.  It creates
    its own short-lived public linear websocket, subscribes only to ticker and
    5m/15m/1h kline topics, records snapshots, then closes the connection.
    It never starts MarketStream, never consumes the manager's six symbol slots,
    and never invokes Scan+ analysis.
    """

    VERSION = "bybit_prescan_ws_probe_v1"
    DEFAULT_SYMBOLS = (
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
        "ADAUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT", "ENAUSDT",
        "TAOUSDT", "AAVEUSDT", "LTCUSDT", "BCHUSDT", "NEARUSDT",
        "APTUSDT", "ARBUSDT", "OPUSDT", "WIFUSDT", "1000PEPEUSDT",
    )

    @staticmethod
    def _topic_symbol(topic):
        parts = str(topic or "").split(".")
        if not parts:
            return None
        symbol = parts[-1].upper()
        return symbol if SYMBOL_RE.match(symbol) else None

    @classmethod
    def run(cls, symbols=None, timeout=12.0):
        requested = []
        seen = set()
        for raw in (symbols or cls.DEFAULT_SYMBOLS):
            symbol = str(raw or "").upper().strip()
            if not SYMBOL_RE.match(symbol) or symbol in seen:
                continue
            seen.add(symbol)
            requested.append(symbol)
        # A probe should stay intentionally small.  The production UniverseStream
        # can be widened only after this proves the Render -> Bybit WS path.
        requested = requested[:50]
        timeout = max(3.0, min(float(timeout), 25.0))

        started = time.time()
        result = {
            "probe_version": cls.VERSION,
            "source": "bybit_public_linear_websocket",
            "url": WS_URLS["linear"],
            "requested_symbols": requested,
            "requested_symbol_count": len(requested),
            "connected": False,
            "subscribe_ack": None,
            "ticker_symbols": [],
            "kline_symbols": {"5": [], "15": [], "60": []},
            "ticker_samples": {},
            "closed_kline_samples": {},
            "messages": 0,
            "errors": [],
        }
        if not requested:
            result["status"] = "FAIL"
            result["errors"].append("no_valid_symbols")
            return result

        ws = None
        tickers = {}
        klines = {"5": {}, "15": {}, "60": {}}
        topics = []
        for symbol in requested:
            topics.append(f"tickers.{symbol}")
            for interval in ("5", "15", "60"):
                topics.append(f"kline.{interval}.{symbol}")

        try:
            ws = websocket.create_connection(WS_URLS["linear"], timeout=10)
            ws.settimeout(1.0)
            result["connected"] = True
            result["connect_ms"] = round((time.time() - started) * 1000.0, 2)
            ws.send(json.dumps({"op": "subscribe", "args": topics}))
            deadline = time.time() + timeout
            last_ping = time.time()
            while time.time() < deadline:
                if time.time() - last_ping >= 10.0:
                    try:
                        ws.send(json.dumps({"op": "ping"}))
                    except Exception:
                        pass
                    last_ping = time.time()
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    continue
                result["messages"] += 1
                try:
                    msg = json.loads(raw)
                except Exception as exc:
                    result["errors"].append(f"json:{type(exc).__name__}")
                    continue

                if msg.get("op") == "subscribe":
                    result["subscribe_ack"] = {
                        "success": msg.get("success"),
                        "ret_msg": msg.get("ret_msg") or msg.get("retMsg"),
                    }
                    if msg.get("success") is False:
                        break
                    continue

                topic = str(msg.get("topic") or "")
                symbol = cls._topic_symbol(topic)
                data = msg.get("data")
                if topic.startswith("tickers.") and symbol and isinstance(data, dict):
                    current = tickers.setdefault(symbol, {})
                    # Ticker is snapshot+delta: merge fields so a delta cannot erase
                    # turnover/OI received in the initial snapshot.
                    current.update(data)
                    current["_ts"] = msg.get("ts")
                elif topic.startswith("kline.") and symbol and isinstance(data, list):
                    parts = topic.split(".")
                    interval = parts[1] if len(parts) >= 3 else None
                    if interval in klines and data:
                        row = data[-1]
                        if isinstance(row, dict):
                            klines[interval][symbol] = dict(row)

                # We do not need to burn the full timeout once every requested
                # symbol has produced a ticker and all three kline topics.
                if len(tickers) == len(requested) and all(len(x) == len(requested) for x in klines.values()):
                    break
        except Exception as exc:
            result["errors"].append(f"{type(exc).__name__}: {exc}")
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

        now_ms = int(time.time() * 1000)
        result["elapsed_ms"] = round((time.time() - started) * 1000.0, 2)
        result["ticker_symbols"] = sorted(tickers)
        result["kline_symbols"] = {tf: sorted(rows) for tf, rows in klines.items()}

        for symbol, row in tickers.items():
            ts = row.get("_ts")
            age_ms = now_ms - int(ts) if isinstance(ts, (int, float)) else None
            result["ticker_samples"][symbol] = {
                "lastPrice": row.get("lastPrice"),
                "turnover24h": row.get("turnover24h"),
                "volume24h": row.get("volume24h"),
                "openInterest": row.get("openInterest"),
                "openInterestValue": row.get("openInterestValue"),
                "fundingRate": row.get("fundingRate"),
                "bid1Price": row.get("bid1Price"),
                "ask1Price": row.get("ask1Price"),
                "age_ms": age_ms,
            }

        for tf, rows in klines.items():
            for symbol, row in rows.items():
                result["closed_kline_samples"].setdefault(symbol, {})[tf] = {
                    "start": row.get("start"),
                    "end": row.get("end"),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                    "turnover": row.get("turnover"),
                    "confirm": row.get("confirm"),
                    "timestamp": row.get("timestamp"),
                }

        ticker_count = len(tickers)
        kline_counts = {tf: len(rows) for tf, rows in klines.items()}
        ticker_ratio = ticker_count / len(requested)
        all_kline_ratio = min(kline_counts.values()) / len(requested)
        ack_ok = result.get("subscribe_ack", {}).get("success") is True if isinstance(result.get("subscribe_ack"), dict) else False
        if result["connected"] and ack_ok and ticker_ratio >= 0.8 and all_kline_ratio >= 0.8:
            status = "PASS"
        elif result["connected"] and (ticker_count > 0 or max(kline_counts.values()) > 0):
            status = "PARTIAL"
        else:
            status = "FAIL"
        result["status"] = status
        result["coverage"] = {
            "ticker": round(ticker_ratio, 4),
            "kline_5": round(kline_counts["5"] / len(requested), 4),
            "kline_15": round(kline_counts["15"] / len(requested), 4),
            "kline_60": round(kline_counts["60"] / len(requested), 4),
        }
        result["guardrails"] = {
            "uses_dynamic_manager": False,
            "starts_market_stream": False,
            "uses_rest": False,
            "uses_spot": False,
            "runs_scan_plus": False,
            "trade_decision": False,
        }
        return result


def run_bybit_prescan_ws_probe(symbols=None, timeout=12.0):
    """Public callable for the HTTP layer: return JSON-serialisable probe data."""
    return BybitPreScanWSProbe.run(symbols=symbols, timeout=timeout)


# ============================================================
# ON-DEMAND PRESCAN V1.1 -- BYBIT WS DISCOVERY + FRESH HISTORY
# ============================================================

class OnDemandPreScanService:
    """Manual PreScan. No permanent universe collector and no Scan+ activation."""

    VERSION = "prescan_v3_2_final"
    # Deliberately broad but static v1 universe. Invalid/delisted symbols are
    # isolated by recursive WS batches and cannot poison the whole run.
    DEFAULT_UNIVERSE = (
        "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","BNBUSDT","LINKUSDT","AVAXUSDT","SUIUSDT",
        "ENAUSDT","TAOUSDT","AAVEUSDT","LTCUSDT","BCHUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","WIFUSDT",
        "1000PEPEUSDT","1000SHIBUSDT","DOTUSDT","UNIUSDT","ATOMUSDT","ETCUSDT","FILUSDT","TRXUSDT","TONUSDT","ICPUSDT",
        "INJUSDT","SEIUSDT","TIAUSDT","JUPUSDT","PYTHUSDT","RENDERUSDT","FETUSDT","RUNEUSDT","GALAUSDT","SANDUSDT",
        "MANAUSDT","CRVUSDT","LDOUSDT","MKRUSDT","ONDOUSDT","PENDLEUSDT","STXUSDT","IMXUSDT","GRTUSDT","ALGOUSDT",
        "HBARUSDT","VETUSDT","KASUSDT","ZECUSDT","XLMUSDT","EOSUSDT","FLOWUSDT","DYDXUSDT","SNXUSDT","COMPUSDT",
        "SUSHIUSDT","APEUSDT","CHZUSDT","MINAUSDT","ORDIUSDT","WLDUSDT","ARKMUSDT","STRKUSDT","MATICUSDT","POLUSDT",
        "NOTUSDT","JASMYUSDT","BONKUSDT","1000BONKUSDT","FLOKIUSDT","1000FLOKIUSDT","MEMEUSDT","PEOPLEUSDT","BLURUSDT","GMXUSDT",
        "EIGENUSDT","ETHFIUSDT","WUSDT","ZROUSDT","ZKUSDT","AEROUSDT","BERAUSDT","HYPEUSDT","QNTUSDT","RVNUSDT",
    )
    TF_MAP = {"5": "5m", "15": "15m", "60": "1h"}
    HISTORY_LIMIT = 120
    TOP_BY_TURNOVER = 30
    CACHE_TTL = 120.0
    DISK_CACHE_TTL = 6 * 3600.0
    PRESCAN_CACHE_DIR = os.environ.get("PRESCAN_CACHE_DIR", "/tmp/scalp-market-bridge/prescan")
    HISTORY_WORKERS = 6
    HISTORY_GLOBAL_TIMEOUT = 16.0
    _cache_lock = threading.RLock()
    _history_cache = {}
    _provider_route_cache = {}

    # Single-flight: one network history fetch per symbol at a time.
    # Concurrent callers share the same Future instead of duplicating 3-TF REST work.
    _inflight_lock = threading.RLock()
    _history_inflight = {}

    # Lightweight background snapshot cache. No Scan+ analysis runs here.
    WARM_CACHE_ENABLED = os.environ.get("PRESCAN_WARM_CACHE", "1").strip().lower() not in ("0","false","no")
    WARM_CACHE_INTERVAL = 240.0
    WARM_CACHE_START_DELAY = 0.8
    TICKER_CACHE_TTL = 90.0
    _ticker_cache = None
    _ticker_cache_at = 0.0
    _warm_symbols = []
    _warmer_lock = threading.RLock()
    _warmer_started = False
    _warmer_thread = None
    _warm_status = {
        "started_at": None, "last_started_at": None, "last_finished_at": None,
        "last_elapsed_ms": None, "last_success": 0, "last_failed": 0,
        "ready_symbols": 0, "last_error": None,
    }

    # Cross-venue history is allowed only when its latest close is plausibly
    # consistent with the current Bybit perpetual ticker. Prevents e.g. wrong
    # contract/unit mappings from entering structural analysis.
    PRICE_SANITY_MAX_RATIO = 1.35
    ANALYSIS_WORKERS = max(2, min(8, int(os.environ.get("PRESCAN_ANALYSIS_WORKERS", "6"))))

    @staticmethod
    def _clean_symbols(symbols):
        out, seen = [], set()
        for raw in symbols:
            s = str(raw or "").upper().strip()
            if SYMBOL_RE.match(s) and s not in seen:
                seen.add(s); out.append(s)
        return out

    @classmethod
    def _ticker_batch(cls, symbols, timeout=5.0):
        """Collect ticker snapshots over one WS connection.

        Subscription requests are chunked but share the same socket. If Bybit
        rejects a request because one topic has no handler (stale/delisted
        symbol), only that topic is removed and the affected chunk is retried
        on the same connection. No recursive reconnects are allowed.
        """
        symbols = list(dict.fromkeys(str(s).upper() for s in (symbols or [])))
        if not symbols:
            return {}, []

        ws = None
        rows, errors = {}, []
        invalid_symbols = set()
        request_topics = {}
        request_seq = 0

        def send_topics(topics):
            nonlocal request_seq
            topics = [t for t in topics if t.rsplit(".", 1)[-1].upper() not in invalid_symbols]
            if not topics:
                return
            request_seq += 1
            req_id = f"prescan-{request_seq}"
            request_topics[req_id] = list(topics)
            ws.send(json.dumps({"req_id": req_id, "op": "subscribe", "args": topics}))

        try:
            ws = websocket.create_connection(WS_URLS["linear"], timeout=min(4.0, timeout + 1.0))
            ws.settimeout(0.15)

            topics = [f"tickers.{s}" for s in symbols]
            # Small subscription messages isolate stale universe entries while
            # keeping exactly one TCP/TLS/WebSocket connection.
            for i in range(0, len(topics), 10):
                send_topics(topics[i:i + 10])

            deadline = time.time() + timeout
            first_ticker_at = None
            last_ticker_at = None
            quiet_grace = 0.22
            while time.time() < deadline:
                expected = len(symbols) - len(invalid_symbols)
                if expected > 0 and len(rows) >= expected:
                    break
                # Bybit normally sends the subscribed ticker snapshot as one burst.
                # Once that burst has gone quiet, do not burn the full discovery timeout
                # waiting for stale/non-linear symbols that will be reported as skipped.
                if (first_ticker_at is not None and last_ticker_at is not None
                        and time.time() - last_ticker_at >= quiet_grace
                        and len(rows) >= max(1, int(len(symbols) * 0.70))):
                    break
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                if msg.get("op") == "subscribe" and msg.get("success") is False:
                    ret_msg = str(msg.get("ret_msg") or msg.get("retMsg") or "subscription_rejected")
                    # Live Bybit response example:
                    # "error:handler not found, topic:tickers.1000SHIBUSDT"
                    match = re.search(r"topic\s*:\s*tickers\.([A-Z0-9]+)", ret_msg, re.IGNORECASE)
                    if match:
                        bad_symbol = match.group(1).upper()
                        invalid_symbols.add(bad_symbol)
                        req_id = str(msg.get("req_id") or "")
                        failed_topics = request_topics.get(req_id, [])
                        if not failed_topics:
                            bad_topic = f"tickers.{bad_symbol}"
                            failed_topics = next(
                                (batch for batch in request_topics.values() if bad_topic in batch),
                                []
                            )
                        retry_topics = [
                            t for t in failed_topics
                            if t.rsplit(".", 1)[-1].upper() != bad_symbol
                        ]
                        if retry_topics:
                            send_topics(retry_topics)
                        continue
                    errors.append(ret_msg)
                    continue

                topic = str(msg.get("topic") or "")
                data = msg.get("data")
                if topic.startswith("tickers.") and isinstance(data, dict):
                    symbol = topic.rsplit(".", 1)[-1].upper()
                    if symbol in symbols and symbol not in invalid_symbols:
                        current = rows.setdefault(symbol, {})
                        current.update(data)
                        current["_ts"] = msg.get("ts")
                        stamp = time.time()
                        if first_ticker_at is None:
                            first_ticker_at = stamp
                        last_ticker_at = stamp
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
        return rows, errors

    @classmethod
    def _discover_tickers(cls, symbols, batch_size=None):
        """Discover tickers with one bounded WS session.

        A missing ticker is normal universe hygiene (invalid/delisted/not-linear),
        not a transport failure. Transport/protocol errors stay separate so a
        partially stale fixed universe cannot fail the whole PreScan.
        """
        symbols = list(dict.fromkeys(
            str(s).strip().upper() for s in (symbols or [])
            if SYMBOL_RE.fullmatch(str(s).strip().upper())
        ))
        if not symbols:
            return {}, ["empty_universe"], []

        results, transport_errors = cls._ticker_batch(symbols, timeout=2.4)
        skipped = [s for s in symbols if s not in results]
        return results, transport_errors, skipped

    @staticmethod
    def _rest_candle(row):
        if not isinstance(row, list) or len(row) < 8:
            return None
        try:
            c = {"start": int(row[0]), "open": float(row[1]), "high": float(row[2]), "low": float(row[3]),
                 "close": float(row[4]), "volume": float(row[5]), "end": int(row[6]), "turnover": float(row[7]),
                 "confirm": True, "source": "binance_futures_rest"}
        except (TypeError, ValueError):
            return None
        return c if c["low"] <= c["high"] and c["end"] < int(time.time() * 1000) else None

    @staticmethod
    def _tv_frame(method, params):
        payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
        return f"~m~{len(payload.encode('utf-8'))}~m~{payload}"

    @staticmethod
    def _tv_payloads(raw):
        """Parse concatenated TradingView ~m~ frames without trusting packet boundaries."""
        if not isinstance(raw, str):
            return []
        out = []
        pos = 0
        marker = "~m~"
        while True:
            start = raw.find(marker, pos)
            if start < 0:
                break
            len_start = start + len(marker)
            len_end = raw.find(marker, len_start)
            if len_end < 0:
                break
            try:
                size = int(raw[len_start:len_end])
            except ValueError:
                pos = len_end + len(marker)
                continue
            payload_start = len_end + len(marker)
            # Protocol length is UTF-8 bytes. Messages used here are ASCII JSON/heartbeat,
            # so character slicing is equivalent; reject truncated frames defensively.
            payload = raw[payload_start:payload_start + size]
            if len(payload.encode("utf-8")) != size:
                break
            out.append(payload)
            pos = payload_start + len(payload)
        return out

    @classmethod
    def _tv_history_bundle(cls, symbol, timeout=6.0):
        """Fetch 5m/15m/1h Bybit perpetual history through TradingView WebSocket.

        One symbol = one short-lived socket and three series. No exchange REST is used.
        Only fully closed bars are returned.
        """
        intervals = {"5": "5", "15": "15", "60": "60"}
        series_ids = {"5": "sds_5", "15": "sds_15", "60": "sds_60"}
        rows = {tf: [] for tf in intervals}
        errors = {}
        ws = None
        now_ms = int(time.time() * 1000)
        cs = "cs_" + uuid.uuid4().hex[:12]
        tv_symbol = f"BYBIT:{symbol}.P"

        total_deadline = time.monotonic() + timeout
        try:
            ws = websocket.create_connection(
                "wss://data.tradingview.com/socket.io/websocket",
                timeout=min(3.0, timeout),
                origin="https://data.tradingview.com",
                header=["User-Agent: Mozilla/5.0"],
            )
            ws.settimeout(0.50)

            def send(method, params):
                ws.send(cls._tv_frame(method, params))

            send("set_auth_token", ["unauthorized_user_token"])
            send("chart_create_session", [cs, ""])
            descriptor = "=" + json.dumps(
                {"symbol": tv_symbol, "adjustment": "splits", "session": "regular"},
                separators=(",", ":"),
            )
            send("resolve_symbol", [cs, "prescan_sym", descriptor])
            for tf, tv_tf in intervals.items():
                sid = series_ids[tf]
                send("create_series", [cs, sid, f"ser_{tf}", "prescan_sym", tv_tf, cls.HISTORY_LIMIT + 5])

            completed = set()
            deadline = total_deadline
            while time.monotonic() < deadline and len(completed) < len(series_ids):
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    continue
                for payload in cls._tv_payloads(raw):
                    if payload.startswith("~h~"):
                        # Heartbeat must be echoed with the same TradingView framing.
                        ws.send(f"~m~{len(payload)}~m~{payload}")
                        continue
                    try:
                        msg = json.loads(payload)
                    except Exception:
                        continue
                    method = msg.get("m")
                    params = msg.get("p") or []

                    if method in ("symbol_error", "series_error"):
                        detail = ":".join(str(x) for x in params[-2:])
                        for tf in intervals:
                            errors.setdefault(tf, f"{method}:{detail}")
                        if method == "symbol_error":
                            return rows, errors
                        continue

                    if method in ("du", "timescale_update") and len(params) > 1 and isinstance(params[1], dict):
                        series_map = params[1]
                        for tf, sid in series_ids.items():
                            node = series_map.get(sid)
                            if not isinstance(node, dict):
                                continue
                            items = node.get("s")
                            if not isinstance(items, list):
                                continue
                            parsed = {}
                            interval_ms = int(tf) * 60_000
                            for item in items:
                                vals = item.get("v") if isinstance(item, dict) else None
                                if not isinstance(vals, list) or len(vals) < 5:
                                    continue
                                try:
                                    start_ms = int(float(vals[0]) * 1000)
                                    candle = {
                                        "start": start_ms,
                                        "end": start_ms + interval_ms - 1,
                                        "open": float(vals[1]),
                                        "high": float(vals[2]),
                                        "low": float(vals[3]),
                                        "close": float(vals[4]),
                                        "volume": float(vals[5]) if len(vals) > 5 and vals[5] is not None else 0.0,
                                        "turnover": 0.0,
                                        "confirm": True,
                                        "source": "tradingview_ws_bybit",
                                    }
                                except (TypeError, ValueError, OverflowError):
                                    continue
                                if (all(math.isfinite(candle[k]) for k in ("open","high","low","close","volume"))
                                        and candle["low"] <= candle["high"]
                                        and candle["end"] < now_ms):
                                    parsed[start_ms] = candle
                            if parsed:
                                rows[tf] = [parsed[k] for k in sorted(parsed)][-cls.HISTORY_LIMIT:]

                    if method == "series_completed" and len(params) >= 2:
                        sid = str(params[1])
                        for tf, expected_sid in series_ids.items():
                            if sid == expected_sid:
                                completed.add(tf)

            for tf in intervals:
                if len(rows[tf]) < PreScanEngine.MIN_CANDLES[tf]:
                    errors.setdefault(tf, "insufficient_history")
                    continue
                interval_ms = int(tf) * 60_000
                latest_end = int(rows[tf][-1]["end"])
                # Two bars tolerance handles source propagation delay but rejects stale history.
                if now_ms - latest_end > interval_ms * 2 + 60_000:
                    errors.setdefault(tf, "stale_history")
            return rows, errors
        except Exception as exc:
            err = f"{type(exc).__name__}:{exc}"
            return rows, {tf: err for tf in intervals}
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    _provider_rate_lock = threading.Lock()
    _provider_next_at = {}
    PROVIDER_MIN_GAPS = {
        "okx": 0.055,      # conservative below OKX candle endpoint ceiling
        "kucoin": 0.030,   # independent venue; never queued behind OKX
        "binance": 0.020,  # independent fallback venue
        "other": 0.055,
    }

    @classmethod
    def _provider_key(cls, url):
        host = urllib.parse.urlparse(url).netloc.lower()
        if "okx.com" in host:
            return "okx"
        if "kucoin.com" in host:
            return "kucoin"
        if "binance.vision" in host:
            return "binance"
        return "other"

    @classmethod
    def _provider_throttle(cls, url):
        key = cls._provider_key(url)
        gap = cls.PROVIDER_MIN_GAPS.get(key, cls.PROVIDER_MIN_GAPS["other"])
        with cls._provider_rate_lock:
            now = time.monotonic()
            next_at = cls._provider_next_at.get(key, 0.0)
            delay = next_at - now
            if delay > 0:
                time.sleep(delay)
            cls._provider_next_at[key] = time.monotonic() + gap

    @classmethod
    def _http_json(cls, url, timeout=4.0):
        cls._provider_throttle(url)
        req = urllib.request.Request(url, headers={
            "User-Agent": "scalp-market-bridge/3.1",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if getattr(resp, "status", 200) != 200:
                raise RuntimeError(f"http_{getattr(resp, 'status', 'unknown')}")
            return json.loads(resp.read().decode("utf-8"))

    @staticmethod
    def _provider_base(symbol):
        base = str(symbol).upper()
        if base.endswith("USDT"):
            base = base[:-4]
        # Bybit uses multiplier contracts for several meme coins while other
        # venues expose the underlying token symbol. Price scale is irrelevant
        # to the structural ratios used by PreScan.
        if base.startswith("1000") and len(base) > 4:
            base = base[4:]
        return base

    @classmethod
    def _validate_provider_rows(cls, rows, tf):
        interval_ms = {"5": 300_000, "15": 900_000, "60": 3_600_000}[tf]
        now_ms = int(time.time() * 1000)
        clean = {}
        for c in rows or []:
            try:
                start = int(c["start"])
                vals = [float(c[k]) for k in ("open", "high", "low", "close", "volume")]
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if not all(math.isfinite(x) for x in vals):
                continue
            if vals[2] > vals[1] or start <= 0:
                continue
            end = int(c.get("end") or (start + interval_ms - 1))
            if end >= now_ms:
                continue
            item = dict(c)
            item.update({"start": start, "end": end, "confirm": True})
            clean[start] = item
        ordered = [clean[k] for k in sorted(clean)][-cls.HISTORY_LIMIT:]
        if len(ordered) < PreScanEngine.MIN_CANDLES[tf]:
            return [], "insufficient_history"
        # Current history must actually be current. A provider returning an old
        # market/listing is rejected rather than silently poisoning discovery.
        max_age = max(interval_ms * 3, 20 * 60_000)
        if now_ms - ordered[-1]["end"] > max_age:
            return [], "stale_history"
        return ordered, None

    @staticmethod
    def _provider_symbol_missing(error_text):
        s = str(error_text or "").lower()
        needles = (
            "doesn't exist", "does not exist", "not exist", "instrument id does not exist",
            "symbol_not_found", "symbol not found", "contract not exist", "invalid symbol",
            "400100", "51001",
        )
        return any(x in s for x in needles)

    @classmethod
    def _okx_history_bundle(cls, symbol):
        base = cls._provider_base(symbol)
        inst = f"{base}-USDT-SWAP"
        bars = {"5": "5m", "15": "15m", "60": "1H"}
        bundle, errors = {}, {}
        for tf, bar in bars.items():
            try:
                q = urllib.parse.urlencode({"instId": inst, "bar": bar, "limit": cls.HISTORY_LIMIT + 2})
                payload = cls._http_json("https://www.okx.com/api/v5/market/candles?" + q)
                if str(payload.get("code")) != "0":
                    raise RuntimeError("okx:" + str(payload.get("msg") or payload.get("code")))
                parsed = []
                for row in payload.get("data") or []:
                    if not isinstance(row, list) or len(row) < 9 or str(row[8]) != "1":
                        continue
                    start = int(row[0])
                    parsed.append({"start": start, "end": start + {"5":300_000,"15":900_000,"60":3_600_000}[tf]-1,
                                   "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]),
                                   "volume": float(row[5]), "turnover": float(row[7] or 0), "confirm": True,
                                   "source": "okx_swap_rest"})
                rows, err = cls._validate_provider_rows(parsed, tf)
                bundle[tf] = rows
                if err: errors[tf] = err
            except Exception as exc:
                bundle[tf] = []
                errors[tf] = f"{type(exc).__name__}:{exc}"
                if cls._provider_symbol_missing(errors[tf]):
                    break
        return bundle, errors

    @classmethod
    def _kucoin_history_bundle(cls, symbol):
        base = cls._provider_base(symbol)
        kc_base = "XBT" if base == "BTC" else base
        contract = f"{kc_base}USDTM"
        bundle, errors = {}, {}
        for tf, granularity in {"5":5, "15":15, "60":60}.items():
            try:
                q = urllib.parse.urlencode({"symbol": contract, "granularity": granularity})
                payload = cls._http_json("https://api-futures.kucoin.com/api/v1/kline/query?" + q)
                if str(payload.get("code")) != "200000":
                    raise RuntimeError("kucoin:" + str(payload.get("msg") or payload.get("code")))
                parsed = []
                for row in payload.get("data") or []:
                    if not isinstance(row, list) or len(row) < 7:
                        continue
                    ts = int(row[0]); start = ts * 1000 if ts < 10**12 else ts
                    parsed.append({"start": start, "end": start + {"5":300_000,"15":900_000,"60":3_600_000}[tf]-1,
                                   "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]),
                                   "volume": float(row[5]), "turnover": float(row[6] or 0), "confirm": True,
                                   "source": "kucoin_futures_rest"})
                rows, err = cls._validate_provider_rows(parsed, tf)
                bundle[tf] = rows
                if err: errors[tf] = err
            except Exception as exc:
                bundle[tf] = []
                errors[tf] = f"{type(exc).__name__}:{exc}"
                if cls._provider_symbol_missing(errors[tf]):
                    break
        return bundle, errors

    @classmethod
    def _binance_spot_history_bundle(cls, symbol):
        base = cls._provider_base(symbol)
        bsymbol = f"{base}USDT"
        bundle, errors = {}, {}
        for tf, interval in {"5":"5m", "15":"15m", "60":"1h"}.items():
            try:
                q = urllib.parse.urlencode({"symbol": bsymbol, "interval": interval, "limit": cls.HISTORY_LIMIT + 2})
                payload = cls._http_json("https://data-api.binance.vision/api/v3/klines?" + q)
                if not isinstance(payload, list):
                    raise RuntimeError("binance_spot_bad_response")
                parsed = []
                for row in payload:
                    if not isinstance(row, list) or len(row) < 8:
                        continue
                    parsed.append({"start": int(row[0]), "end": int(row[6]), "open": float(row[1]), "high": float(row[2]),
                                   "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                                   "turnover": float(row[7] or 0), "confirm": True, "source": "binance_spot_marketdata"})
                rows, err = cls._validate_provider_rows(parsed, tf)
                bundle[tf] = rows
                if err: errors[tf] = err
            except Exception as exc:
                bundle[tf] = []
                errors[tf] = f"{type(exc).__name__}:{exc}"
        return bundle, errors

    @classmethod
    def _disk_cache_path(cls, symbol):
        safe = re.sub(r"[^A-Z0-9_-]", "_", str(symbol).upper())
        return os.path.join(cls.PRESCAN_CACHE_DIR, safe + ".json")

    @classmethod
    def _load_disk_history(cls, symbol):
        """Warm-start cache. It is only a seed; freshness is checked before use."""
        path = cls._disk_cache_path(symbol)
        try:
            st = os.stat(path)
            if time.time() - st.st_mtime > cls.DISK_CACHE_TTL:
                return None
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            bundle = payload.get("bundle") or {}
            source = str(payload.get("provider") or "disk_cache")
            checked = {}
            for tf in ("5","15","60"):
                rows, err = cls._validate_provider_rows(bundle.get(tf) or [], tf)
                if err or len(rows) < PreScanEngine.MIN_CANDLES[tf]:
                    return None
                checked[tf] = rows
            return checked, source
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    @classmethod
    def _save_disk_history(cls, symbol, bundle, provider):
        """Atomic best-effort cache write; cache failure must never fail PreScan."""
        try:
            os.makedirs(cls.PRESCAN_CACHE_DIR, exist_ok=True)
            path = cls._disk_cache_path(symbol)
            tmp = path + "." + uuid.uuid4().hex + ".tmp"
            payload = {
                "saved_at": time.time(),
                "provider": provider,
                "bundle": {tf: list(bundle.get(tf) or [])[-cls.HISTORY_LIMIT:] for tf in ("5","15","60")},
            }
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, separators=(",",":"), allow_nan=False)
            os.replace(tmp, path)
        except Exception:
            try:
                if 'tmp' in locals() and os.path.exists(tmp):
                    os.unlink(tmp)
            except OSError:
                pass

    @classmethod
    def _fetch_history_bundle_core(cls, symbol):
        """Fetch a compact fresh 3-TF history without TradingView.

        Provider order deliberately prefers derivative markets (OKX, KuCoin)
        before Binance spot.  PreScan only discovers candidates; the heavy
        Scan+ decision remains Bybit-native.  A provider must supply all three
        fresh TFs or the next provider is tried, preventing mixed-TF geometry.
        """
        now = time.time()
        cached_rows = {}
        complete_cache = True
        with cls._cache_lock:
            for tf in ("5", "15", "60"):
                cached = cls._history_cache.get((symbol, tf))
                if cached and now - cached[0] <= cls.CACHE_TTL:
                    cached_rows[tf] = [dict(x) for x in cached[1]]
                else:
                    complete_cache = False
        if complete_cache:
            return cached_rows, {tf: "memory_cache" for tf in ("5","15","60")}, {}

        disk = cls._load_disk_history(symbol)
        if disk is not None:
            disk_rows, disk_provider = disk
            with cls._cache_lock:
                for tf in ("5","15","60"):
                    cls._history_cache[(symbol, tf)] = (now, [dict(x) for x in disk_rows[tf]])
            return disk_rows, {tf: "disk_cache:" + disk_provider for tf in ("5","15","60")}, {}

        attempts = {}
        providers = [
            ("okx_swap_rest", cls._okx_history_bundle),
            ("kucoin_futures_rest", cls._kucoin_history_bundle),
            ("binance_spot_marketdata", cls._binance_spot_history_bundle),
        ]
        preferred = cls._provider_route_cache.get(symbol)
        if preferred:
            providers.sort(key=lambda item: 0 if item[0] == preferred else 1)
        for provider_name, fetcher in providers:
            bundle, errors = fetcher(symbol)
            attempts[provider_name] = dict(errors)
            if all(len(bundle.get(tf) or []) >= PreScanEngine.MIN_CANDLES[tf] and tf not in errors for tf in ("5","15","60")):
                with cls._cache_lock:
                    for tf in ("5","15","60"):
                        cls._history_cache[(symbol, tf)] = (now, [dict(x) for x in bundle[tf]])
                cls._provider_route_cache[symbol] = provider_name
                cls._save_disk_history(symbol, bundle, provider_name)
                return bundle, {tf: provider_name for tf in ("5","15","60")}, {}

        # Fail closed. We do not resurrect the unofficial TradingView transport
        # and we do not mix timeframes from different venues for one symbol.
        compact = {name: errs for name, errs in attempts.items() if errs}
        return {tf: [] for tf in ("5","15","60")}, {tf: "unavailable" for tf in ("5","15","60")}, {
            tf: "all_history_providers_failed:" + json.dumps(compact, separators=(",",":"))[:1200]
            for tf in ("5","15","60")
        }

    @classmethod
    def _fetch_history_bundle(cls, symbol):
        """Single-flight wrapper around the validated provider chain.

        Exactly one caller owns a symbol's network fetch. Other concurrent
        callers wait for and reuse that same result. This prevents the warmer
        and /prescan from doubling REST load during startup.
        """
        # Fast path: validated memory/disk/provider logic may already return
        # immediately. Single-flight is only useful around the whole operation,
        # including a possible disk read, and keeps behavior deterministic.
        owner = False
        with cls._inflight_lock:
            fut = cls._history_inflight.get(symbol)
            if fut is None:
                fut = concurrent.futures.Future()
                cls._history_inflight[symbol] = fut
                owner = True

        if not owner:
            try:
                return fut.result(timeout=cls.HISTORY_GLOBAL_TIMEOUT + 2.0)
            except Exception as exc:
                return ({tf: [] for tf in ("5","15","60")},
                        {tf: "unavailable" for tf in ("5","15","60")},
                        {tf: f"shared_history_fetch_failed:{type(exc).__name__}:{exc}" for tf in ("5","15","60")})

        try:
            result = cls._fetch_history_bundle_core(symbol)
            fut.set_result(result)
            return result
        except Exception as exc:
            fut.set_exception(exc)
            raise
        finally:
            with cls._inflight_lock:
                if cls._history_inflight.get(symbol) is fut:
                    cls._history_inflight.pop(symbol, None)



    @classmethod
    def _cache_symbol_ready(cls, symbol):
        now = time.time()
        with cls._cache_lock:
            for tf in ("5","15","60"):
                cached = cls._history_cache.get((symbol, tf))
                if not cached or now - cached[0] > cls.CACHE_TTL:
                    return False
        return True

    @classmethod
    def _warm_once(cls):
        started = time.time()
        base = cls._clean_symbols(cls.DEFAULT_UNIVERSE)
        tickers, ws_errors, _ = cls._discover_tickers(base)
        if not tickers:
            raise RuntimeError("warm_ticker_discovery_failed:" + ",".join(ws_errors[:3]))

        ranked = sorted(tickers, key=lambda s: fnum(tickers[s].get("turnover24h")) or 0.0,
                        reverse=True)[:cls.TOP_BY_TURNOVER]
        with cls._cache_lock:
            cls._ticker_cache = {k: dict(v) for k,v in tickers.items()}
            cls._ticker_cache_at = time.time()
            cls._warm_symbols = list(ranked)

        ok = failed = 0
        with ThreadPoolExecutor(max_workers=min(cls.HISTORY_WORKERS, max(1,len(ranked)))) as ex:
            fs = {ex.submit(cls._fetch_history_bundle, s): s for s in ranked}
            done, pending = wait(list(fs), timeout=cls.HISTORY_GLOBAL_TIMEOUT)
            for f in done:
                try:
                    b, _, e = f.result()
                    good = not e and all(len(b.get(tf) or []) >= PreScanEngine.MIN_CANDLES[tf]
                                         for tf in ("5","15","60"))
                    ok += int(good); failed += int(not good)
                except Exception:
                    failed += 1
            for f in pending:
                f.cancel(); failed += 1

        with cls._warmer_lock:
            cls._warm_status.update({
                "last_finished_at": time.time(),
                "last_elapsed_ms": round((time.time()-started)*1000.0,2),
                "last_success": ok, "last_failed": failed,
                "ready_symbols": sum(1 for s in ranked if cls._cache_symbol_ready(s)),
            })

    @classmethod
    def _warmer_loop(cls):
        time.sleep(cls.WARM_CACHE_START_DELAY)
        while True:
            with cls._warmer_lock:
                cls._warm_status["last_started_at"] = time.time()
            try:
                cls._warm_once()
                with cls._warmer_lock:
                    cls._warm_status["last_error"] = None
            except Exception as exc:
                with cls._warmer_lock:
                    cls._warm_status["last_error"] = f"{type(exc).__name__}:{exc}"
            time.sleep(cls.WARM_CACHE_INTERVAL)

    @classmethod
    def ensure_warmer(cls):
        if not cls.WARM_CACHE_ENABLED:
            return False
        with cls._warmer_lock:
            if cls._warmer_started and cls._warmer_thread and cls._warmer_thread.is_alive():
                return True
            cls._warmer_started = True
            cls._warm_status["started_at"] = time.time()
            th = threading.Thread(target=cls._warmer_loop, name="prescan-cache-warmer", daemon=True)
            cls._warmer_thread = th
            th.start()
            return True

    @classmethod
    def warm_status(cls):
        with cls._warmer_lock:
            return dict(cls._warm_status)

    @staticmethod
    def _structure(rows):
        # Canonical Scan+ Structure implementation, without constructing a
        # MarketStream (constructor would bootstrap six TFs and defeat PreScan).
        adapter = object.__new__(MarketStream)
        return MarketStream._structure_metrics(adapter, rows)

    @classmethod
    def _history_price_sanity(cls, ticker, histories):
        bybit = fnum(ticker.get("lastPrice")) or fnum(ticker.get("last_price"))
        if not bybit or bybit <= 0:
            return False, "bybit_price_unavailable", None
        closes = []
        for tf in ("5","15","60"):
            rows = histories.get(tf) or []
            if not rows:
                return False, f"history_price_unavailable:{tf}", None
            c = fnum(rows[-1].get("close"))
            if not c or c <= 0:
                return False, f"history_price_invalid:{tf}", None
            closes.append(c)
        hist = closes[0]  # 5m latest close is the tightest current comparison.
        ratio = max(bybit, hist) / min(bybit, hist)
        if ratio > cls.PRICE_SANITY_MAX_RATIO:
            return False, "cross_venue_price_mismatch", round(ratio, 6)
        return True, None, round(ratio, 6)

    @classmethod
    def _analyze_rows(cls, symbol, ticker, histories, sources):
        sane, sanity_error, sanity_ratio = cls._history_price_sanity(ticker, histories)
        if not sane:
            return {"engine_version": cls.VERSION, "symbol": symbol, "status": "REJECT", "priority": 0.0,
                    "eligible_for_scan_plus": False, "reasons": [sanity_error],
                    "history_sources": sources, "price_sanity_ratio": sanity_ratio}
        frames = {}
        for tf in ("5", "15", "60"):
            rows = histories.get(tf, [])
            if len(rows) < PreScanEngine.MIN_CANDLES[tf]:
                return {"engine_version": cls.VERSION, "symbol": symbol, "status": "REJECT", "priority": 0.0,
                        "eligible_for_scan_plus": False, "reasons": [f"history_unavailable:{tf}"], "history_sources": sources}
            structure = cls._structure(rows)
            frames[tf] = {"rows": rows, "structure": structure, "direction": PreScanEngine._direction(structure)}
        if not all(frames[x]["structure"].get("ready") for x in frames):
            return {"engine_version": cls.VERSION, "symbol": symbol, "status": "REJECT", "priority": 0.0,
                    "eligible_for_scan_plus": False, "reasons": ["structure_not_ready"], "history_sources": sources}

        d60, d15 = frames["60"]["direction"], frames["15"]["direction"]
        aligned = d60 == d15 and d60 in ("bullish", "bearish")
        direction = d60 if aligned else "neutral"
        loc = PreScanEngine._location(frames["5"]["rows"], frames["5"]["structure"], direction)
        a5, a15 = PreScanEngine._activity(frames["5"]["rows"]), PreScanEngine._activity(frames["15"]["rows"])
        turnover = fnum(ticker.get("turnover24h")) or 0.0
        oi_value = fnum(ticker.get("openInterestValue")) or 0.0
        bid, ask = fnum(ticker.get("bid1Price")), fnum(ticker.get("ask1Price"))
        mid = (bid + ask) / 2.0 if bid and ask and bid > 0 and ask > 0 else None
        spread_bps = ((ask - bid) / mid * 10000.0) if mid and ask >= bid else None
        market_quality = turnover > 0 and (spread_bps is None or spread_bps <= 25.0)

        priority = 0.0
        if aligned: priority += 45
        if loc.get("favorable_phase"): priority += 15
        if not loc.get("extended"): priority += 10
        if a5.get("lifecycle") in ("awakening", "compression"): priority += 12
        if a15.get("lifecycle") in ("awakening", "compression"): priority += 8
        if oi_value > 0: priority += 5
        if market_quality: priority += 5
        priority = round(min(priority, 100.0), 2)

        reasons = []
        if not aligned: reasons.append("1h_15m_not_aligned")
        if loc.get("extended"): reasons.append("price_extended_from_structure")
        if not market_quality: reasons.append("market_quality_reject")
        if not market_quality:
            status = "REJECT"
        elif not aligned or loc.get("extended"):
            status = "COLD"
        elif priority >= 80:
            status = "HOT"
        elif priority >= 60:
            status = "WARMING"
        else:
            status = "COLD"
        if not reasons: reasons.append("candidate_discovery_only")
        return {
            "engine_version": cls.VERSION, "symbol": symbol, "status": status,
            "eligible_for_scan_plus": status in ("HOT", "WARMING"), "priority": priority,
            "direction": {"aligned": aligned, "effective": direction, "1h": d60, "15m": d15},
            "location_5m": loc, "activity": {"5m": a5, "15m": a15},
            "market": {"last_price": fnum(ticker.get("lastPrice")), "turnover24h": turnover,
                       "open_interest": fnum(ticker.get("openInterest")), "open_interest_value": oi_value,
                       "funding_rate": fnum(ticker.get("fundingRate")), "spread_bps": None if spread_bps is None else round(spread_bps, 4)},
            "history_sources": sources, "reasons": reasons,
        }

    @staticmethod
    def _json_safe(value):
        """Recursively make PreScan output strict-JSON safe."""
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {str(k): OnDemandPreScanService._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [OnDemandPreScanService._json_safe(v) for v in value]
        return value

    @classmethod
    def run(cls, universe=None, top_n=8, shortlist=30):
        started = time.time()
        cls.ensure_warmer()
        symbols = cls._clean_symbols(universe or cls.DEFAULT_UNIVERSE)
        top_n = max(1, min(int(top_n), 20))
        if not symbols:
            return cls._json_safe({
                "engine_version": cls.VERSION, "mode": "manual_on_demand", "status": "FAIL",
                "universe_size": 0, "ws_ticker_count": 0, "shortlist_size": 0,
                "top_n": top_n, "elapsed_ms": round((time.time() - started) * 1000.0, 2),
                "candidates": [], "scan_plus_candidates": [],
                "diagnostics": {"ws_errors": ["empty_universe"], "ws_error_count": 1,
                                "skipped_symbols": [], "skipped_symbol_count": 0,
                                "history_errors": {}, "history_error_count": 0,
                                "analysis_errors": {}, "analysis_error_count": 0},
                "guardrails": {"runs_continuously": False, "candle_cache_runs_continuously": cls.WARM_CACHE_ENABLED, "uses_dynamic_manager": False,
                               "activates_scan_plus": False, "trade_decision": False,
                               "closed_history_only": True, "exchange_rest_used": True, "scan_plus_logic_unchanged": True},
            })

        shortlist = max(1, min(int(shortlist), cls.TOP_BY_TURNOVER, len(symbols)))
        stage_t0 = time.time()
        with cls._cache_lock:
            cache_ok = cls._ticker_cache is not None and time.time()-cls._ticker_cache_at <= cls.TICKER_CACHE_TTL
            tc = {k:dict(v) for k,v in (cls._ticker_cache or {}).items()} if cache_ok else {}
        if tc:
            tickers = {s:tc[s] for s in symbols if s in tc}
            ws_errors = []
            skipped_symbols = [s for s in symbols if s not in tickers]
            ticker_source = "background_cache"
        else:
            tickers, ws_errors, skipped_symbols = cls._discover_tickers(symbols)
            ticker_source = "bybit_ws_live"
            if tickers:
                with cls._cache_lock:
                    cls._ticker_cache = {k:dict(v) for k,v in tickers.items()}
                    cls._ticker_cache_at = time.time()
        ticker_ms = round((time.time() - stage_t0) * 1000.0, 2)
        ranked = sorted(
            tickers,
            key=lambda s: fnum(tickers[s].get("turnover24h")) or 0.0,
            reverse=True
        )[:shortlist]
        with cls._cache_lock:
            cls._warm_symbols = list(ranked)

        history = {s: {} for s in ranked}
        sources = {s: {} for s in ranked}
        hist_errors = {}
        history_t0 = time.time()

        # Fresh history uses bounded public REST fallback chain; no TradingView.
        # Ten workers overlap network latency; the global pacer still caps aggregate provider request rate.
        executor = None
        future_meta = {}
        try:
            if ranked:
                executor = ThreadPoolExecutor(max_workers=min(cls.HISTORY_WORKERS, len(ranked)))
                for s in ranked:
                    future = executor.submit(cls._fetch_history_bundle, s)
                    future_meta[future] = s

                done, pending = wait(list(future_meta), timeout=cls.HISTORY_GLOBAL_TIMEOUT)
                for future in done:
                    s = future_meta[future]
                    try:
                        bundle, bundle_sources, bundle_errors = future.result()
                    except Exception as exc:
                        bundle, bundle_sources = {}, {}
                        bundle_errors = {tf: f"{type(exc).__name__}:{exc}" for tf in ("5","15","60")}
                    for tf in ("5","15","60"):
                        history[s][tf] = list(bundle.get(tf) or [])
                        sources[s][tf] = bundle_sources.get(tf, "unavailable")
                        err = bundle_errors.get(tf)
                        if err:
                            hist_errors[f"{s}:{tf}"] = err

                for future in pending:
                    s = future_meta[future]
                    future.cancel()
                    for tf in ("5","15","60"):
                        history[s][tf] = []
                        sources[s][tf] = "history_timeout"
                        hist_errors[f"{s}:{tf}"] = "global_history_timeout"
        finally:
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)

        history_ms = round((time.time() - history_t0) * 1000.0, 2)

        # A single pathological market must never fail the endpoint.
        analysis_t0 = time.time()
        candidates = []
        analysis_errors = {}

        # Candidate calculations are independent. Run them concurrently so one
        # symbol's canonical Structure pass does not block the other 29.
        # The mathematical functions and candidate ranking are unchanged.
        if ranked:
            with ThreadPoolExecutor(max_workers=min(cls.ANALYSIS_WORKERS, len(ranked))) as analysis_pool:
                future_symbol = {
                    analysis_pool.submit(cls._analyze_rows, s, tickers[s], history[s], sources[s]): s
                    for s in ranked
                }
                by_symbol = {}
                for future in concurrent.futures.as_completed(future_symbol):
                    s = future_symbol[future]
                    try:
                        by_symbol[s] = future.result()
                    except Exception as exc:
                        err = f"{type(exc).__name__}:{exc}"
                        analysis_errors[s] = err
                        by_symbol[s] = {
                            "engine_version": cls.VERSION, "symbol": s, "status": "REJECT",
                            "priority": 0.0, "eligible_for_scan_plus": False,
                            "reasons": ["analysis_error"], "history_sources": sources[s],
                        }
                # Preserve deterministic pre-sort ordering for exact behavioral parity.
                candidates = [by_symbol[s] for s in ranked]

        analysis_ms = round((time.time() - analysis_t0) * 1000.0, 2)
        candidates.sort(
            key=lambda x: (
                PreScanEngine.STATUS_ORDER.get(x.get("status"), 0),
                fnum(x.get("priority")) or 0.0,
                fnum(x.get("market", {}).get("turnover24h")) or 0.0
            ),
            reverse=True
        )
        actionable = [x for x in candidates if x.get("eligible_for_scan_plus")][:top_n]
        analyzable = [
            x for x in candidates
            if "analysis_error" not in x.get("reasons", [])
            and not any(str(r).startswith("history_unavailable:") for r in x.get("reasons", []))
        ]

        if not tickers:
            overall = "FAIL"
        elif analyzable:
            overall = "PASS"
        else:
            overall = "PARTIAL"

        result = {
            "engine_version": cls.VERSION, "mode": "manual_on_demand", "status": overall,
            "universe_size": len(symbols), "ws_ticker_count": len(tickers),
            "shortlist_size": len(ranked), "top_n": top_n,
            "elapsed_ms": round((time.time() - started) * 1000.0, 2),
            "candidates": candidates[:top_n],
            "scan_plus_candidates": [x["symbol"] for x in actionable],
            "profile": {
                "ticker_ms": ticker_ms,
                "ticker_source": ticker_source,
                "history_ms": history_ms,
                "warm_cache": cls.warm_status(),
                "analysis_ms": analysis_ms,
                "analysis_workers": cls.ANALYSIS_WORKERS,
                "history_workers": cls.HISTORY_WORKERS,
                "history_global_timeout_s": cls.HISTORY_GLOBAL_TIMEOUT,
            },
            "diagnostics": {
                "ws_errors": ws_errors[-20:], "ws_error_count": len(ws_errors),
                "skipped_symbols": skipped_symbols, "skipped_symbol_count": len(skipped_symbols),
                "history_errors": hist_errors, "history_error_count": len(hist_errors),
                "analysis_errors": analysis_errors, "analysis_error_count": len(analysis_errors),
            },
            "guardrails": {
                "runs_continuously": False, "candle_cache_runs_continuously": cls.WARM_CACHE_ENABLED, "uses_dynamic_manager": False,
                "activates_scan_plus": False, "trade_decision": False,
                "closed_history_only": True, "exchange_rest_used": True, "scan_plus_logic_unchanged": True,
            },
        }
        return cls._json_safe(result)

def run_prescan(universe=None, top_n=8, shortlist=30):
    return OnDemandPreScanService.run(universe=universe, top_n=top_n, shortlist=shortlist)

# ============================================================
# SCAN ORCHESTRATOR V1
# ============================================================

class ScanOrchestrator:
    """Routes user intent without changing PreScan or Scan+ internals.

    Modes:
      auto   -> PreScan -> eligible candidates -> full Scan+
      single -> exactly one requested symbol -> full Scan+ (PreScan bypassed)
      batch  -> explicit symbol list -> full Scan+ (PreScan bypassed)

    Full Scan+ calls are intentionally sequential. DynamicMarketManager owns a
    bounded six-symbol realtime pool; sequential orchestration avoids competing
    cold-start websocket activations and preserves the manager's eviction rules.
    """

    VERSION = "scan_orchestrator_v1"
    MAX_AUTO_SCAN_PLUS = 6
    MAX_BATCH_SYMBOLS = 8

    @staticmethod
    def _compact_result(scan):
        setup = scan.get("setup") or {}
        return {
            "symbol": scan.get("symbol"),
            "manipulation": scan.get("manipulation") or {"status":"NO_DATA","signal":"NONE"},
            "architecture": {"version":SCAN_ARCHITECTURE_VERSION,"pipeline":list(SCAN_PIPELINE),"opportunity_states":list(OPPORTUNITY_STATES)},
            "engine_version": scan.get("engine_version"),
            "price": scan.get("price"),
            "setup_state": scan.get("setup_state"),
            "trigger_state": scan.get("trigger_state"),
            "trade_state": scan.get("trade_state"),
            "direction": scan.get("direction"),
            "context": scan.get("context"),
            "trade_style": scan.get("trade_style"),
            "block_class": scan.get("block_class"),
            "block_reasons": scan.get("block_reasons") or [],
            "retryable": scan.get("retryable"),
            "status": scan.get("status") or {},
            "data_quality": scan.get("data_quality") or {},
            "setup": {
                "side": setup.get("side"),
                "entry": setup.get("entry"),
                "entry_zone": setup.get("entry_zone"),
                "stop": setup.get("stop"),
                "targets": setup.get("targets"),
                "risk_reward": setup.get("risk_reward"),
                "limit_plan": setup.get("limit_plan") or {"eligible":False,"state":"NO_LIMIT_PLAN"},
                "opportunity_state": setup.get("opportunity_state") or ("MARKET_READY" if setup.get("status")=="SETUP" else "LIMIT_READY" if (setup.get("limit_plan") or {}).get("eligible") else "WATCH"),
                "failed_requirements": setup.get("failed_requirements") or [],
            },
        }

    @classmethod
    def _scan_symbols(cls, symbols):
        results, errors = [], {}
        for raw in symbols:
            try:
                symbol = dynamic_manager.normalize_symbol(raw)
                scan = dynamic_manager.scan(symbol)
                results.append(cls._compact_result(scan))
            except Exception as exc:
                symbol = str(raw).upper().strip()
                errors[symbol] = f"{type(exc).__name__}:{exc}"
        return results, errors

    @classmethod
    def single(cls, symbol):
        started = time.time()
        normalized = dynamic_manager.normalize_symbol(symbol)
        results, errors = cls._scan_symbols([normalized])
        return {
            "orchestrator_version": cls.VERSION,
            "mode": "single",
            "prescan_used": False,
            "requested_symbols": [normalized],
            "scan_plus_count": len(results),
            "scan_plus_results": results,
            "errors": errors,
            "elapsed_ms": round((time.time()-started)*1000.0, 2),
        }

    @classmethod
    def batch(cls, symbols):
        started = time.time()
        cleaned = []
        seen = set()
        for raw in symbols or []:
            symbol = dynamic_manager.normalize_symbol(raw)
            if symbol not in seen:
                seen.add(symbol)
                cleaned.append(symbol)
        if not cleaned:
            raise ValueError("no symbols")
        if len(cleaned) > cls.MAX_BATCH_SYMBOLS:
            raise ValueError(f"too many symbols; max {cls.MAX_BATCH_SYMBOLS}")
        results, errors = cls._scan_symbols(cleaned)
        return {
            "orchestrator_version": cls.VERSION,
            "mode": "batch",
            "prescan_used": False,
            "requested_symbols": cleaned,
            "scan_plus_count": len(results),
            "scan_plus_results": results,
            "errors": errors,
            "elapsed_ms": round((time.time()-started)*1000.0, 2),
        }

    @classmethod
    def auto(cls, top_n=8, shortlist=30):
        started = time.time()
        top_n = max(1, min(int(top_n), cls.MAX_AUTO_SCAN_PLUS))
        prescan = run_prescan(top_n=top_n, shortlist=shortlist)
        eligible = []
        for candidate in prescan.get("candidates") or []:
            if candidate.get("eligible_for_scan_plus") is True:
                symbol = candidate.get("symbol")
                if symbol and symbol not in eligible:
                    eligible.append(symbol)
            if len(eligible) >= top_n:
                break

        results, errors = cls._scan_symbols(eligible)
        return {
            "orchestrator_version": cls.VERSION,
            "mode": "auto",
            "prescan_used": True,
            "prescan": {
                "engine_version": prescan.get("engine_version"),
                "status": prescan.get("status"),
                "elapsed_ms": prescan.get("elapsed_ms"),
                "scan_plus_candidates": prescan.get("scan_plus_candidates") or [],
                "diagnostics": prescan.get("diagnostics") or {},
            },
            "selected_symbols": eligible,
            "scan_plus_count": len(results),
            "scan_plus_results": results,
            "errors": errors,
            "elapsed_ms": round((time.time()-started)*1000.0, 2),
        }


def run_scan_auto(top_n=8, shortlist=30):
    return ScanOrchestrator.auto(top_n=top_n, shortlist=shortlist)


def run_scan_single(symbol):
    return ScanOrchestrator.single(symbol)


def run_scan_batch(symbols):
    return ScanOrchestrator.batch(symbols)

# ============================================================
# ASYNC SCAN JOB MANAGER V2
# ============================================================

class ScanJobManager:
    """Non-blocking wrapper for batch/auto Scan+ orchestration.

    HTTP start endpoints only enqueue work and return immediately.
    Heavy Scan+ work runs outside the request thread. One job worker is used
    intentionally: DynamicMarketManager owns a small realtime symbol pool and
    each Scan+ activation is already heavy. This avoids the Render timeout that
    occurred when several scans were executed inside one HTTP request.
    """

    VERSION = "scan_job_manager_v3_1_2_queue_guard"
    MAX_JOBS = 20
    JOB_TTL_SECONDS = 3600
    AUTO_WARMUP_SECONDS = max(30, min(90, int(os.environ.get("SCAN_AUTO_WARMUP_SECONDS", "40"))))
    AUTO_WARMUP_MAX_SECONDS = max(AUTO_WARMUP_SECONDS, min(180, int(os.environ.get("SCAN_AUTO_WARMUP_MAX_SECONDS", "120"))))
    AUTO_WARMUP_POLL_SECONDS = max(1, min(10, int(os.environ.get("SCAN_AUTO_WARMUP_POLL_SECONDS", "2"))))
    JOB_PRESCAN_TIMEOUT = max(20.0, min(60.0, float(os.environ.get("SCAN_PRESCAN_TIMEOUT", "45"))))
    _lock = threading.RLock()
    _jobs = {}
    _executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(2, int(os.environ.get("SCAN_JOB_WORKERS", "1")))),
        thread_name_prefix="scan-job",
    )

    @classmethod
    def _cleanup(cls):
        now = time.time()
        with cls._lock:
            expired = [
                jid for jid, job in cls._jobs.items()
                if now - float(job.get("updated_at", job.get("created_at", now))) > cls.JOB_TTL_SECONDS
            ]
            for jid in expired:
                cls._jobs.pop(jid, None)

            if len(cls._jobs) > cls.MAX_JOBS:
                ordered = sorted(
                    cls._jobs.items(),
                    key=lambda kv: float(kv[1].get("updated_at", kv[1].get("created_at", 0))),
                )
                for jid, _ in ordered[:len(cls._jobs)-cls.MAX_JOBS]:
                    cls._jobs.pop(jid, None)

    @classmethod
    def _new_job(cls, mode, payload):
        import uuid
        cls._cleanup()
        # Do not enqueue duplicate auto scans while an equivalent one is queued/running.
        # The worker is intentionally single-threaded; duplicate requests only create
        # stale queue pressure and make the live endpoint look hung.
        with cls._lock:
            for existing in cls._jobs.values():
                if (
                    existing.get("mode") == mode
                    and existing.get("state") in ("QUEUED", "RUNNING")
                    and existing.get("payload") == payload
                ):
                    return existing["job_id"]
        jid = uuid.uuid4().hex[:16]
        now = time.time()
        job = {
            "job_id": jid,
            "job_manager_version": cls.VERSION,
            "mode": mode,
            "state": "QUEUED",
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
            "progress": {"done": 0, "total": None, "current_symbol": None},
            "payload": payload,
            "result": None,
            "error": None,
        }
        with cls._lock:
            cls._jobs[jid] = job
        return jid

    @classmethod
    def _patch(cls, jid, **changes):
        with cls._lock:
            job = cls._jobs.get(jid)
            if not job:
                return
            job.update(changes)
            job["updated_at"] = time.time()

    @classmethod
    def _progress(cls, jid, done=None, total=None, current_symbol=None, stage=None, warmup_remaining=None):
        with cls._lock:
            job = cls._jobs.get(jid)
            if not job:
                return
            p = dict(job.get("progress") or {})
            if done is not None:
                p["done"] = int(done)
            if total is not None:
                p["total"] = int(total)
            p["current_symbol"] = current_symbol
            if stage is not None:
                p["stage"] = stage
            if warmup_remaining is not None:
                p["warmup_remaining_seconds"] = max(0, int(warmup_remaining))
            job["progress"] = p
            job["updated_at"] = time.time()

    @classmethod
    def _activate_and_warm_auto(cls, jid, symbols):
        """Activate all AUTO candidates first, then let their WS flow mature together.

        No full technical analysis is run during warm-up.  This is deliberately
        transport-only: DynamicMarketManager.activate() starts/reuses the realtime
        streams, while the final Scan+ pass remains the sole decision pass.
        """
        symbols = list(symbols or [])
        total = len(symbols)
        activation_errors = {}
        cls._progress(jid, done=0, total=total, current_symbol=None, stage="ACTIVATING",
                      warmup_remaining=cls.AUTO_WARMUP_SECONDS)

        activated = []
        for idx, raw in enumerate(symbols, start=1):
            symbol = dynamic_manager.normalize_symbol(raw)
            cls._progress(jid, done=idx-1, total=total, current_symbol=symbol, stage="ACTIVATING",
                          warmup_remaining=cls.AUTO_WARMUP_SECONDS)
            try:
                dynamic_manager.activate(symbol)
                activated.append(symbol)
            except Exception as exc:
                activation_errors[symbol] = f"{type(exc).__name__}:{exc}"
            cls._progress(jid, done=idx, total=total, current_symbol=None, stage="ACTIVATING",
                          warmup_remaining=cls.AUTO_WARMUP_SECONDS)

        if not activated:
            return [], activation_errors, 0.0

        # Phase 1: mandatory simultaneous warm-up.
        warm_started = time.monotonic()
        minimum_deadline = warm_started + cls.AUTO_WARMUP_SECONDS
        maximum_deadline = warm_started + cls.AUTO_WARMUP_MAX_SECONDS
        while True:
            remaining = minimum_deadline - time.monotonic()
            if remaining <= 0:
                break
            cls._progress(jid, done=0, total=len(activated), current_symbol=None,
                          stage="WARMING", warmup_remaining=remaining)
            time.sleep(min(cls.AUTO_WARMUP_POLL_SECONDS, remaining))

        # Phase 2: after 40s, wait only if actual realtime data is still cold.
        # This probe runs execution/flow readiness only — never MTF/trade analysis.
        last_readiness = {}
        while True:
            ready_count = 0
            cold = []
            for symbol in activated:
                try:
                    rd = dynamic_manager.realtime_readiness(symbol)
                except Exception as exc:
                    rd = {"symbol":symbol, "trade_data_ready":False,
                          "probe_error":f"{type(exc).__name__}:{exc}"}
                last_readiness[symbol] = rd
                if rd.get("trade_data_ready"):
                    ready_count += 1
                else:
                    cold.append(symbol)

            if ready_count == len(activated):
                break
            remaining = maximum_deadline - time.monotonic()
            if remaining <= 0:
                break
            cls._progress(jid, done=ready_count, total=len(activated),
                          current_symbol=(cold[0] if len(cold)==1 else None),
                          stage="WAITING_DATA_READY", warmup_remaining=remaining)
            time.sleep(min(cls.AUTO_WARMUP_POLL_SECONDS, remaining))

        warm_elapsed = time.monotonic() - warm_started
        with cls._lock:
            job = cls._jobs.get(jid)
            if job is not None:
                job["warmup_readiness"] = last_readiness
                job["warmup_ready_count"] = sum(
                    1 for x in last_readiness.values() if x.get("trade_data_ready")
                )
                job["warmup_total_count"] = len(activated)
                job["warmup_timed_out"] = bool(
                    last_readiness
                    and not all(x.get("trade_data_ready") for x in last_readiness.values())
                )
                job["updated_at"] = time.time()
        cls._progress(jid, done=0, total=len(activated), current_symbol=None,
                      stage="FINAL_SCAN", warmup_remaining=0)
        return activated, activation_errors, warm_elapsed

    @classmethod
    def _scan_symbols_progressive(cls, jid, symbols):
        results, errors = [], {}
        total = len(symbols)
        cls._progress(jid, done=0, total=total, current_symbol=None, stage="FINAL_SCAN")
        for idx, raw in enumerate(symbols, start=1):
            symbol = dynamic_manager.normalize_symbol(raw)
            cls._progress(jid, done=idx-1, total=total, current_symbol=symbol, stage="FINAL_SCAN")
            try:
                scan = dynamic_manager.scan(symbol)
                results.append(ScanOrchestrator._compact_result(scan))
            except Exception as exc:
                errors[symbol] = f"{type(exc).__name__}:{exc}"
            cls._progress(jid, done=idx, total=total, current_symbol=None, stage="FINAL_SCAN")
        return results, errors

    @classmethod
    def _run_job(cls, jid):
        with cls._lock:
            job = cls._jobs.get(jid)
            if not job:
                return
            mode = job["mode"]
            payload = dict(job.get("payload") or {})
        cls._patch(jid, state="RUNNING", started_at=time.time())
        _scan_log("JOB_STARTED", job_id=jid, mode=mode, payload=payload)
        started = time.time()
        try:
            if mode == "batch":
                raw_symbols = payload.get("symbols") or []
                cleaned, seen = [], set()
                for raw in raw_symbols:
                    symbol = dynamic_manager.normalize_symbol(raw)
                    if symbol not in seen:
                        seen.add(symbol)
                        cleaned.append(symbol)
                if not cleaned:
                    raise ValueError("no symbols")
                if len(cleaned) > ScanOrchestrator.MAX_BATCH_SYMBOLS:
                    raise ValueError(f"too many symbols; max {ScanOrchestrator.MAX_BATCH_SYMBOLS}")
                results, errors = cls._scan_symbols_progressive(jid, cleaned)
                result = {
                    "orchestrator_version": ScanOrchestrator.VERSION,
                    "mode": "batch",
                    "prescan_used": False,
                    "requested_symbols": cleaned,
                    "scan_plus_count": len(results),
                    "scan_plus_results": results,
                    "errors": errors,
                    "elapsed_ms": round((time.time()-started)*1000.0, 2),
                }

            elif mode == "auto":
                top_n = max(1, min(int(payload.get("top_n", 6)), ScanOrchestrator.MAX_AUTO_SCAN_PLUS))
                shortlist = int(payload.get("shortlist", 30))
                cls._progress(jid, done=0, total=None, current_symbol=None, stage="PRESCAN_START")
                prescan_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="prescan-job"
                )
                prescan_future = prescan_executor.submit(
                    run_prescan, top_n=top_n, shortlist=shortlist
                )
                try:
                    prescan = prescan_future.result(timeout=cls.JOB_PRESCAN_TIMEOUT)
                except concurrent.futures.TimeoutError:
                    prescan_future.cancel()
                    cls._progress(
                        jid, done=0, total=None, current_symbol=None,
                        stage="PRESCAN_TIMEOUT"
                    )
                    raise TimeoutError(
                        f"prescan exceeded {cls.JOB_PRESCAN_TIMEOUT:.0f}s hard timeout"
                    )
                finally:
                    prescan_executor.shutdown(wait=False, cancel_futures=True)
                cls._progress(
                    jid, done=0, total=None, current_symbol=None,
                    stage="PRESCAN_DONE"
                )
                eligible = []
                for candidate in prescan.get("candidates") or []:
                    if candidate.get("eligible_for_scan_plus") is True:
                        symbol = candidate.get("symbol")
                        if symbol and symbol not in eligible:
                            eligible.append(symbol)
                    if len(eligible) >= top_n:
                        break

                activated, activation_errors, warm_elapsed = cls._activate_and_warm_auto(jid, eligible)
                results, scan_errors = cls._scan_symbols_progressive(jid, activated)
                errors = dict(activation_errors)
                errors.update(scan_errors)
                result = {
                    "orchestrator_version": "scan_orchestrator_v2_1_adaptive_warmup",
                    "mode": "auto",
                    "prescan_used": True,
                    "prescan": {
                        "engine_version": prescan.get("engine_version"),
                        "status": prescan.get("status"),
                        "elapsed_ms": prescan.get("elapsed_ms"),
                        "scan_plus_candidates": prescan.get("scan_plus_candidates") or [],
                        "diagnostics": prescan.get("diagnostics") or {},
                    },
                    "selected_symbols": eligible,
                    "activated_symbols": activated,
                    "warmup_seconds": round(warm_elapsed, 2),
                    "warmup_target_seconds": cls.AUTO_WARMUP_SECONDS,
                    "warmup_max_seconds": cls.AUTO_WARMUP_MAX_SECONDS,
                    "warmup_readiness": dict((cls._jobs.get(jid) or {}).get("warmup_readiness") or {}),
                    "warmup_ready_count": (cls._jobs.get(jid) or {}).get("warmup_ready_count"),
                    "warmup_total_count": (cls._jobs.get(jid) or {}).get("warmup_total_count"),
                    "warmup_timed_out": bool((cls._jobs.get(jid) or {}).get("warmup_timed_out")),
                    "scan_plus_count": len(results),
                    "scan_plus_results": results,
                    "errors": errors,
                    "elapsed_ms": round((time.time()-started)*1000.0, 2),
                }
            elif mode == "unified":
                from multi_market_adapters import ExternalMarketAdapter, external_universe
                started_unified=time.time()
                top_n=max(1,min(int(payload.get("top_n",5)),ScanOrchestrator.MAX_AUTO_SCAN_PLUS))
                shortlist=int(payload.get("shortlist",30))
                markets=[str(x).strip().lower() for x in (payload.get("markets") or ["crypto","stocks","forex","commodities"]) if str(x).strip()]
                cls._progress(jid,done=0,total=None,current_symbol=None,stage="UNIFIED_PRESCAN")
                prescan=run_prescan(top_n=top_n,shortlist=shortlist)
                eligible=[]
                for candidate in prescan.get("candidates") or []:
                    if candidate.get("eligible_for_scan_plus") is True:
                        symbol=candidate.get("symbol")
                        if symbol and symbol not in eligible: eligible.append(symbol)
                    if len(eligible)>=top_n: break
                activated,activation_errors,warm_elapsed=cls._activate_and_warm_auto(jid,eligible)
                crypto_results,crypto_errors=cls._scan_symbols_progressive(jid,activated)
                crypto_rankings=relative_strength(crypto_results,market_key="crypto")
                crypto_rank_map={x["symbol"]:x for x in crypto_rankings}
                crypto_setups=[x.get("setup") for x in crypto_results if isinstance(x.get("setup"),dict) and x.get("setup",{}).get("tradeable")]
                crypto_clusters=exposure_cluster([dict(s,market="crypto") for s in crypto_setups])
                crypto_exposure=exposure_buckets([dict(s,market="crypto") for s in crypto_setups])
                for item in crypto_results:
                    item["relative_strength"]=crypto_rank_map.get(item.get("symbol"))
                    item["risk_clusters"]=crypto_clusters
                    item["exposure_buckets"]=crypto_exposure
                errors=dict(activation_errors); errors.update({f"crypto:{k}":v for k,v in crypto_errors.items()})
                external={}
                adapter=ExternalMarketAdapter(); universe=external_universe()
                for market in ("stocks","forex","commodities"):
                    if market not in markets: continue
                    symbols=list(universe.get(market) or [])
                    cls._progress(jid,done=0,total=len(symbols),current_symbol=None,stage=f"EXTERNAL_{market.upper()}")
                    if not symbols:
                        external[market]={"status":"NO_SYMBOLS","results":[]}; continue
                    if market!="stocks" and not adapter.configured:
                        external[market]={"status":"DATA_BLOCK","reason":"TWELVE_DATA_API_KEY_not_configured","symbols":symbols}; continue
                    # External providers are batch-oriented: one request per
                    # timeframe for the whole market is faster and avoids a request
                    # storm across 15 symbols x 5 timeframes.
                    vals_map=adapter.scan_many(market,symbols)
                    vals=[vals_map[s] for s in symbols if s in vals_map]
                    rankings=relative_strength(vals,market_key=market)
                    setups=[v.get("setup") for v in vals if isinstance(v.get("setup"),dict) and v.get("setup",{}).get("tradeable")]
                    clusters=exposure_cluster([dict(s,market=market) for s in setups])
                    exposure=exposure_buckets([dict(s,market=market) for s in setups])
                    rank_map={x["symbol"]:x for x in rankings}
                    for item in vals:
                        item["relative_strength"]=rank_map.get(item.get("symbol"))
                        item["risk_clusters"]=clusters
                        item["exposure_buckets"]=exposure
                    external[market]={"status":"PASS","count":len(vals),"results":vals,"relative_strength_ranking":rankings}
                result={"orchestrator_version":"scan_orchestrator_v3_unified_live","mode":"unified","prescan_used":True,"markets":markets,
                        "crypto":{"selected_symbols":eligible,"activated_symbols":activated,"warmup_seconds":round(warm_elapsed,2),"scan_plus_count":len(crypto_results),"scan_plus_results":crypto_results},
                        "external":external,"prescan":{"status":prescan.get("status"),"engine_version":prescan.get("engine_version"),"scan_plus_candidates":prescan.get("scan_plus_candidates") or [],"diagnostics":prescan.get("diagnostics") or {}},
                        "errors":errors,"elapsed_ms":round((time.time()-started_unified)*1000.0,2)}
            else:
                raise ValueError(f"unsupported job mode: {mode}")

            finished_at = time.time()
            cls._patch(jid, state="DONE", result=result, error=None, finished_at=finished_at)
            _scan_log("JOB_DONE", job_id=jid, mode=mode, finished_at=finished_at, result=result)
        except Exception as exc:
            finished_at = time.time()
            error = f"{type(exc).__name__}: {exc}"
            cls._patch(jid, state="FAILED", error=error, finished_at=finished_at)
            _scan_log("JOB_FAILED", job_id=jid, mode=mode, finished_at=finished_at, error=error)

    @classmethod
    def start_batch(cls, symbols):
        jid = cls._new_job("batch", {"symbols": list(symbols or [])})
        cls._executor.submit(cls._run_job, jid)
        return cls.status(jid)

    @classmethod
    def start_unified(cls, top_n=5, shortlist=30, markets=None):
        payload = {
            "top_n": int(top_n),
            "shortlist": int(shortlist),
            "markets": list(markets or ["crypto", "stocks", "forex", "commodities"]),
        }
        with cls._lock:
            for existing in cls._jobs.values():
                if (
                    existing.get("mode") == "unified"
                    and existing.get("state") in ("QUEUED", "RUNNING")
                    and existing.get("payload") == payload
                ):
                    return cls.status(existing["job_id"])
            jid = cls._new_job("unified", payload)
        cls._executor.submit(cls._run_job, jid)
        return cls.status(jid)

    @classmethod
    def start_auto(cls, top_n=6, shortlist=30):
        payload = {"top_n": int(top_n), "shortlist": int(shortlist)}
        with cls._lock:
            for existing in cls._jobs.values():
                if (
                    existing.get("mode") == "auto"
                    and existing.get("state") in ("QUEUED", "RUNNING")
                    and existing.get("payload") == payload
                ):
                    return cls.status(existing["job_id"])
            jid = cls._new_job("auto", payload)
        cls._executor.submit(cls._run_job, jid)
        return cls.status(jid)

    @classmethod
    def latest_unified(cls):
        """Return the newest unified Scan job; prefer an active one, otherwise newest completed."""
        cls._cleanup()
        with cls._lock:
            jobs = [
                job for job in cls._jobs.values()
                if job.get("mode") == "unified"
            ]
            if not jobs:
                return None
            active = [
                job for job in jobs
                if job.get("state") in ("QUEUED", "RUNNING")
            ]
            pool = active or jobs
            latest = max(
                pool,
                key=lambda job: float(job.get("updated_at", job.get("created_at", 0))),
            )
            jid = latest.get("job_id")
        return cls.status(jid)

    @classmethod
    def status(cls, jid):
        cls._cleanup()
        with cls._lock:
            job = cls._jobs.get(jid)
            if not job:
                return None
            # Copy nested public fields so Flask serialization cannot race mutations.
            return {
                "job_id": job["job_id"],
                "job_manager_version": job["job_manager_version"],
                "mode": job["mode"],
                "state": job["state"],
                "created_at": job["created_at"],
                "updated_at": job["updated_at"],
                "started_at": job["started_at"],
                "finished_at": job["finished_at"],
                "progress": dict(job.get("progress") or {}),
                "result": job.get("result"),
                "error": job.get("error"),
            }


def start_scan_unified_job(top_n=5, shortlist=30, markets=None):
    return ScanJobManager.start_unified(top_n=top_n,shortlist=shortlist,markets=markets)


def start_scan_auto_job(top_n=6, shortlist=30):
    return ScanJobManager.start_unified(
        top_n=min(int(top_n), 5),
        shortlist=shortlist,
        markets=["crypto", "stocks", "forex", "commodities"],
    )


def start_scan_batch_job(symbols):
    return ScanJobManager.start_batch(symbols)


def get_scan_job(job_id):
    return ScanJobManager.status(job_id)


def get_latest_unified_scan_job():
    return ScanJobManager.latest_unified()

