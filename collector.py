import json
import threading
import time
import uuid
from collections import deque

import websocket


BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


class BybitCollector:
    def __init__(self):
        self.lock = threading.RLock()

        self.symbol = "BTCUSDT"

        # Состояние соединения
        self.connected = False
        self.last_message_at = None
        self.last_error = None
        self.reconnects = 0

        # Каждая новая сессия позволяет нам понимать,
        # откуда именно накоплены session-метрики.
        self.session_id = None
        self.session_started_at = None

        # Последний ticker
        self.ticker = {}

        # Локальная копия стакана
        self.bids = {}
        self.asks = {}
        self.orderbook_ready = False
        self.orderbook_updated_at = None

        # Сделки храним только за последний час.
        # (timestamp_ms, side, price, size)
        self.trades = deque()

        self.thread = None
        self.stop_event = threading.Event()

    # ---------------------------------------------------------
    # PUBLIC
    # ---------------------------------------------------------

    def start(self):
        if self.thread and self.thread.is_alive():
            return

        self.stop_event.clear()

        self.thread = threading.Thread(
            target=self._run_forever,
            daemon=True,
            name="bybit-collector",
        )

        self.thread.start()

    def get_snapshot(self):
        now_ms = int(time.time() * 1000)

        with self.lock:
            self._cleanup_trades(now_ms)

            metrics = self._trade_metrics(now_ms)
            book = self._book_metrics()

            age_seconds = None
            if self.last_message_at:
                age_seconds = round(
                    time.time() - self.last_message_at,
                    2
                )

            session_age_seconds = None
            if self.session_started_at:
                session_age_seconds = round(
                    time.time() - self.session_started_at,
                    2
                )

            return {
                "collector": {
                    "connected": self.connected,
                    "thread_alive": bool(
    self.thread and self.thread.is_alive()
),
"thread_name": (
    self.thread.name
    if self.thread else None
),
                    "symbol": self.symbol,
                    "session_id": self.session_id,
                    "session_started_at": self.session_started_at,
                    "session_age_seconds": session_age_seconds,
                    "last_message_age_seconds": age_seconds,
                    "reconnects": self.reconnects,
                    "last_error": self.last_error,
                },

                "ticker": dict(self.ticker),

                "orderbook": book,

                "flow": metrics,

                "warmup": {
                    "1m": session_age_seconds is not None
                    and session_age_seconds >= 60,

                    "5m": session_age_seconds is not None
                    and session_age_seconds >= 300,

                    "15m": session_age_seconds is not None
                    and session_age_seconds >= 900,

                    "1h": session_age_seconds is not None
                    and session_age_seconds >= 3600,
                },

                "generated_at": time.time(),
            }

    # ---------------------------------------------------------
    # CONNECTION
    # ---------------------------------------------------------

    def _run_forever(self):
        backoff = 1

        while not self.stop_event.is_set():
            try:
                self._run_connection()
                backoff = 1

            except Exception as exc:
                with self.lock:
                    self.connected = False
                    self.last_error = str(exc)
                    self.orderbook_ready = False

                if self.stop_event.is_set():
                    break

                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _run_connection(self):
        ws = None

        try:
            ws = websocket.create_connection(
                BYBIT_LINEAR_WS,
                timeout=30,
            )

            with self.lock:
                self.connected = True
                self.last_error = None
                self.reconnects += 1

                self.session_id = str(uuid.uuid4())
                self.session_started_at = time.time()

                # Новый WS = новая валидная локальная книга.
                self.bids.clear()
                self.asks.clear()
                self.orderbook_ready = False

            subscribe = {
                "op": "subscribe",
                "args": [
                    f"publicTrade.{self.symbol}",
                    f"orderbook.50.{self.symbol}",
                    f"tickers.{self.symbol}",
                ],
            }

            ws.send(json.dumps(subscribe))

            last_ping = time.time()

            while not self.stop_event.is_set():
                # Периодический ping.
                if time.time() - last_ping >= 20:
                    ws.send(json.dumps({"op": "ping"}))
                    last_ping = time.time()

                raw = ws.recv()

                if not raw:
                    continue

                message = json.loads(raw)

                with self.lock:
                    self.last_message_at = time.time()

                self._handle_message(message)

        finally:
            with self.lock:
                self.connected = False
                self.orderbook_ready = False

            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

    # ---------------------------------------------------------
    # MESSAGE ROUTER
    # ---------------------------------------------------------

    def _handle_message(self, message):
        topic = message.get("topic")

        if not topic:
            return

        if topic.startswith("publicTrade."):
            self._handle_trades(message)

        elif topic.startswith("orderbook."):
            self._handle_orderbook(message)

        elif topic.startswith("tickers."):
            self._handle_ticker(message)

    # ---------------------------------------------------------
    # TRADES
    # ---------------------------------------------------------

    def _handle_trades(self, message):
        rows = message.get("data", [])

        with self.lock:
            for trade in rows:
                try:
                    timestamp_ms = int(trade["T"])
                    side = trade["S"]
                    price = float(trade["p"])
                    size = float(trade["v"])

                    self.trades.append(
                        (
                            timestamp_ms,
                            side,
                            price,
                            size,
                        )
                    )

                except (KeyError, TypeError, ValueError):
                    continue

            self._cleanup_trades(int(time.time() * 1000))

    def _cleanup_trades(self, now_ms):
        cutoff = now_ms - 3600_000

        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    # ---------------------------------------------------------
    # ORDERBOOK
    # ---------------------------------------------------------

    def _handle_orderbook(self, message):
        data = message.get("data", {})
        msg_type = message.get("type")

        bids = data.get("b", [])
        asks = data.get("a", [])

        with self.lock:
            if msg_type == "snapshot":
                self.bids.clear()
                self.asks.clear()

                self._apply_book_updates(
                    self.bids,
                    bids
                )

                self._apply_book_updates(
                    self.asks,
                    asks
                )

                self.orderbook_ready = True

            elif msg_type == "delta":
                if not self.orderbook_ready:
                    return

                self._apply_book_updates(
                    self.bids,
                    bids
                )

                self._apply_book_updates(
                    self.asks,
                    asks
                )

            self.orderbook_updated_at = time.time()

    @staticmethod
    def _apply_book_updates(book, updates):
        for row in updates:
            try:
                price = float(row[0])
                size = float(row[1])

                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size

            except (IndexError, TypeError, ValueError):
                continue

    # ---------------------------------------------------------
    # TICKER
    # ---------------------------------------------------------

    def _handle_ticker(self, message):
        data = message.get("data", {})

        wanted = [
            "lastPrice",
            "markPrice",
            "indexPrice",
            "openInterest",
            "openInterestValue",
            "fundingRate",
            "nextFundingTime",
            "bid1Price",
            "bid1Size",
            "ask1Price",
            "ask1Size",
            "volume24h",
            "turnover24h",
        ]

        with self.lock:
            for key in wanted:
                if key in data:
                    self.ticker[key] = data[key]

            self.ticker["updated_at"] = time.time()

    # ---------------------------------------------------------
    # METRICS
    # ---------------------------------------------------------

    def _trade_metrics(self, now_ms):
        windows = {
            "1m": 60_000,
            "5m": 300_000,
            "15m": 900_000,
            "1h": 3_600_000,
        }

        output = {}

        for name, duration in windows.items():
            cutoff = now_ms - duration

            buy_volume = 0.0
            sell_volume = 0.0
            trade_count = 0

            for timestamp_ms, side, price, size in reversed(self.trades):
                if timestamp_ms < cutoff:
                    break

                trade_count += 1

                if side == "Buy":
                    buy_volume += size
                elif side == "Sell":
                    sell_volume += size

            delta = buy_volume - sell_volume
            total = buy_volume + sell_volume

            output[name] = {
                "buy_volume": round(buy_volume, 8),
                "sell_volume": round(sell_volume, 8),
                "delta": round(delta, 8),
                "total_volume": round(total, 8),
                "trade_count": trade_count,
                "buy_ratio": (
                    round(buy_volume / total, 4)
                    if total > 0 else None
                ),
                "sell_ratio": (
                    round(sell_volume / total, 4)
                    if total > 0 else None
                ),
            }

        return output

    def _book_metrics(self):
        if not self.orderbook_ready:
            return {
                "ready": False
            }

        if not self.bids or not self.asks:
            return {
                "ready": False
            }

        best_bid = max(self.bids)
        best_ask = min(self.asks)

        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2

        bid_depth = sum(self.bids.values())
        ask_depth = sum(self.asks.values())

        total_depth = bid_depth + ask_depth

        imbalance = (
            (bid_depth - ask_depth) / total_depth
            if total_depth > 0 else None
        )

        spread_bps = (
            (spread / mid) * 10_000
            if mid > 0 else None
        )

        return {
            "ready": True,

            "best_bid": best_bid,
            "best_ask": best_ask,

            "spread": round(spread, 8),
            "spread_bps": (
                round(spread_bps, 4)
                if spread_bps is not None
                else None
            ),

            "bid_depth_50": round(bid_depth, 8),
            "ask_depth_50": round(ask_depth, 8),

            "imbalance_50": (
                round(imbalance, 4)
                if imbalance is not None
                else None
            ),

            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),

            "updated_at": self.orderbook_updated_at,
        }


collector = BybitCollector()
