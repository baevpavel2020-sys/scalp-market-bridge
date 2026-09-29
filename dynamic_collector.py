import json
import re
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

BYBIT_REST_URLS = (
    "https://api.bybit.com",
    "https://api.bytick.com",
)
KLINE_INTERVALS = ("1", "5", "15", "60", "240", "D")
KLINE_LIMIT = 500


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

        if not self.history_bootstrapped:
            self._bootstrap_history()

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

    def _bootstrap_history(self):
        category = "linear" if self.market == "linear" else "spot"
        try:
            for interval in KLINE_INTERVALS:
                rows = None
                last_error = None
                for base_url in BYBIT_REST_URLS:
                    try:
                        from urllib.parse import urlencode
                        from urllib.request import Request, urlopen
                        import json as _json

                        query = urlencode({
                            "category": category,
                            "symbol": self.symbol,
                            "interval": interval,
                            "limit": KLINE_LIMIT,
                        })
                        request = Request(
                            f"{base_url}/v5/market/kline?{query}",
                            headers={"User-Agent": "scalp-market-bridge/2.1"},
                        )
                        with urlopen(request, timeout=10) as response:
                            payload = _json.loads(
                                response.read().decode("utf-8")
                            )
                        if payload.get("retCode") != 0:
                            raise RuntimeError(
                                f"Bybit retCode={payload.get('retCode')} "
                                f"retMsg={payload.get('retMsg')}"
                            )
                        rows = payload.get("result", {}).get("list", [])
                        break
                    except Exception as exc:
                        last_error = str(exc)

                if rows is None:
                    raise RuntimeError(f"history {interval} unavailable: {last_error}")

                normalized = []
                for row in reversed(rows):
                    try:
                        start = int(row[0])
                        normalized.append({
                            "start": start,
                            "end": self._candle_end_ms(start, interval),
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": float(row[5]),
                            "turnover": float(row[6]),
                            "confirm": True,
                        })
                    except (IndexError, TypeError, ValueError):
                        continue

                with self.lock:
                    self.candles[interval].clear()
                    self.candles[interval].extend(normalized)

            with self.lock:
                self.history_bootstrapped = True
                self.history_error = None
                self.history_loaded_at = time.time()
        except Exception as exc:
            with self.lock:
                self.history_error = str(exc)

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

    def diagnostics(self):
        with self.lock:
            technical = {i: self._technical_metrics(c) for i, c in self.candles.items()}
            counts = {i: len(c) for i, c in self.candles.items()}
            checks = {
                "connected": bool(self.connected),
                "history_bootstrapped": bool(self.history_bootstrapped),
                "history_6_of_6": all(counts.get(i, 0) >= 200 for i in KLINE_INTERVALS),
                "technical_6_of_6": all(technical[i]["ready"] for i in KLINE_INTERVALS),
                "orderbook_ready": bool(self.orderbook_ready),
            }
            return {
                "symbol": self.symbol,
                "market": self.market,
                "status": self.status,
                "checks": checks,
                "overall_pass": all(checks.values()),
                "history_error": self.history_error,
                "history_loaded_at": self.history_loaded_at,
                "candle_counts": counts,
                "technical": technical,
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

                # Same candle received again:
                # replace its live OHLCV values.
                if (
                    candles
                    and candles[-1]["start"]
                    == start
                ):

                    candles[-1] = candle

                else:

                    candles.append(
                        candle
                    )

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
                    "bootstrapped": self.history_bootstrapped,
                    "error": self.history_error,
                    "loaded_at": self.history_loaded_at,
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
