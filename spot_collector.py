import json
import threading
import time
import uuid
import traceback
from collections import deque

import websocket


BYBIT_SPOT_WS = "wss://stream.bybit.com/v5/public/spot"


class BybitSpotCollector:
    def __init__(self):
        self.lock = threading.RLock()

        self.symbol = "BTCUSDT"
        self.market = "spot"

        # Thread / lifecycle
        self.thread = None
        self.stop_event = threading.Event()
        self.started_at = None
        self.status = "created"

        # WebSocket state
        self.connected = False
        self.connect_attempts = 0
        self.successful_connections = 0
        self.reconnects = 0

        self.last_message_at = None
        self.last_error = None
        self.last_traceback = None
        self.last_disconnect_at = None

        # Session
        self.session_id = None
        self.session_started_at = None

        # Market state
        self.ticker = {}

        self.bids = {}
        self.asks = {}
        self.orderbook_ready = False
        self.orderbook_updated_at = None

        # (timestamp_ms, side, price, size)
        self.trades = deque()

    # =========================================================
    # START / SUPERVISION
    # =========================================================

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return

            self.stop_event.clear()
            self.started_at = time.time()
            self.status = "starting"
            self.last_error = None
            self.last_traceback = None

            self.thread = threading.Thread(
                target=self._thread_entry,
                daemon=True,
                name="bybit-spot-collector",
            )

            self.thread.start()

    def ensure_running(self):
        if not self.thread or not self.thread.is_alive():
            self.start()

    def _thread_entry(self):
        print(
            "[spot-collector] background thread started",
            flush=True
        )

        try:
            self._run_forever()

        except BaseException as exc:
            tb = traceback.format_exc()

            with self.lock:
                self.connected = False
                self.status = "thread_failed"
                self.last_error = (
                    f"{type(exc).__name__}: {exc}"
                )
                self.last_traceback = tb

            print(
                "[spot-collector] FATAL THREAD ERROR:",
                repr(exc),
                flush=True
            )
            print(tb, flush=True)

        finally:
            print(
                "[spot-collector] background thread stopped",
                flush=True
            )

    # =========================================================
    # SNAPSHOT
    # =========================================================

    def get_snapshot(self):
        self.ensure_running()

        now = time.time()
        now_ms = int(now * 1000)

        with self.lock:
            self._cleanup_trades(now_ms)

            session_age_seconds = None
            if self.session_started_at:
                session_age_seconds = round(
                    now - self.session_started_at,
                    2
                )

            last_message_age_seconds = None
            if self.last_message_at:
                last_message_age_seconds = round(
                    now - self.last_message_at,
                    2
                )

            process_age_seconds = None
            if self.started_at:
                process_age_seconds = round(
                    now - self.started_at,
                    2
                )

            return {
                "collector": {
                    "market": self.market,
                    "status": self.status,
                    "connected": self.connected,

                    "thread_alive": bool(
                        self.thread
                        and self.thread.is_alive()
                    ),

                    "thread_name": (
                        self.thread.name
                        if self.thread
                        else None
                    ),

                    "process_age_seconds":
                        process_age_seconds,

                    "symbol": self.symbol,

                    "connect_attempts":
                        self.connect_attempts,

                    "successful_connections":
                        self.successful_connections,

                    "reconnects":
                        self.reconnects,

                    "session_id":
                        self.session_id,

                    "session_started_at":
                        self.session_started_at,

                    "session_age_seconds":
                        session_age_seconds,

                    "last_message_age_seconds":
                        last_message_age_seconds,

                    "last_disconnect_at":
                        self.last_disconnect_at,

                    "last_error":
                        self.last_error,

                    "last_traceback":
                        self.last_traceback,
                },

                "ticker": dict(self.ticker),

                "orderbook":
                    self._book_metrics(),

                "flow":
                    self._trade_metrics(now_ms),

                "warmup": {
                    "1m": (
                        session_age_seconds is not None
                        and session_age_seconds >= 60
                    ),
                    "5m": (
                        session_age_seconds is not None
                        and session_age_seconds >= 300
                    ),
                    "15m": (
                        session_age_seconds is not None
                        and session_age_seconds >= 900
                    ),
                    "1h": (
                        session_age_seconds is not None
                        and session_age_seconds >= 3600
                    ),
                },

                "generated_at": now,
            }

    # =========================================================
    # CONNECTION LOOP
    # =========================================================

    def _run_forever(self):
        backoff = 1

        while not self.stop_event.is_set():
            with self.lock:
                self.status = "connecting"
                self.connect_attempts += 1

            try:
                self._run_connection()

                if self.stop_event.is_set():
                    break

                with self.lock:
                    self.status = "reconnecting"
                    self.last_disconnect_at = time.time()

                time.sleep(1)
                backoff = 1

            except Exception as exc:
                tb = traceback.format_exc()

                with self.lock:
                    self.connected = False
                    self.orderbook_ready = False
                    self.status = "reconnecting"

                    self.last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

                    self.last_traceback = tb
                    self.last_disconnect_at = time.time()

                print(
                    "[spot-collector] websocket error:",
                    repr(exc),
                    flush=True
                )

                if self.stop_event.is_set():
                    break

                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

        with self.lock:
            self.connected = False
            self.status = "stopped"

    # =========================================================
    # ONE WEBSOCKET SESSION
    # =========================================================

    def _run_connection(self):
        ws = None

        try:
            print(
                f"[spot-collector] connecting to {BYBIT_SPOT_WS}",
                flush=True
            )

            ws = websocket.create_connection(
                BYBIT_SPOT_WS,
                timeout=30,
            )

            with self.lock:
                previous_connections = (
                    self.successful_connections
                )

                self.successful_connections += 1

                if previous_connections > 0:
                    self.reconnects += 1

                self.connected = True
                self.status = "connected"

                self.last_error = None
                self.last_traceback = None

                self.session_id = str(uuid.uuid4())
                self.session_started_at = time.time()

                self.bids.clear()
                self.asks.clear()
                self.orderbook_ready = False
                self.trades.clear()

            print(
                "[spot-collector] websocket connected",
                flush=True
            )

            subscribe = {
                "op": "subscribe",
                "args": [
                    f"publicTrade.{self.symbol}",
                    f"orderbook.50.{self.symbol}",
                    f"tickers.{self.symbol}",
                ],
            }

            ws.send(json.dumps(subscribe))

            print(
                "[spot-collector] subscriptions sent",
                flush=True
            )

            last_ping = time.time()

            while not self.stop_event.is_set():
                if time.time() - last_ping >= 20:
                    ws.send(
                        json.dumps({"op": "ping"})
                    )
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
                self.last_disconnect_at = time.time()

            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

    # =========================================================
    # MESSAGE ROUTER
    # =========================================================

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

    # =========================================================
    # TRADES
    # =========================================================

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

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                ):
                    continue

            self._cleanup_trades(
                int(time.time() * 1000)
            )

    def _cleanup_trades(self, now_ms):
        cutoff = now_ms - 3_600_000

        while (
            self.trades
            and self.trades[0][0] < cutoff
        ):
            self.trades.popleft()

    # =========================================================
    # ORDERBOOK
    # =========================================================

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

            except (
                IndexError,
                TypeError,
                ValueError,
            ):
                continue

    # =========================================================
    # TICKER
    # =========================================================

    def _handle_ticker(self, message):
        data = message.get("data", {})

        wanted = [
            "lastPrice",
            "bid1Price",
            "bid1Size",
            "ask1Price",
            "ask1Size",
            "volume24h",
            "turnover24h",
            "price24hPcnt",
            "highPrice24h",
            "lowPrice24h",
        ]

        with self.lock:
            for key in wanted:
                if key in data:
                    self.ticker[key] = data[key]

            self.ticker["updated_at"] = time.time()

    # =========================================================
    # FLOW METRICS
    # =========================================================

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

            for (
                timestamp_ms,
                side,
                price,
                size,
            ) in reversed(self.trades):

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
                "buy_volume":
                    round(buy_volume, 8),

                "sell_volume":
                    round(sell_volume, 8),

                "delta":
                    round(delta, 8),

                "total_volume":
                    round(total, 8),

                "trade_count":
                    trade_count,

                "buy_ratio": (
                    round(
                        buy_volume / total,
                        4
                    )
                    if total > 0
                    else None
                ),

                "sell_ratio": (
                    round(
                        sell_volume / total,
                        4
                    )
                    if total > 0
                    else None
                ),
            }

        return output

    # =========================================================
    # ORDERBOOK METRICS
    # =========================================================

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
            if total_depth > 0
            else None
        )

        spread_bps = (
            (spread / mid) * 10_000
            if mid > 0
            else None
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

            "bid_depth_50":
                round(bid_depth, 8),

            "ask_depth_50":
                round(ask_depth, 8),

            "imbalance_50": (
                round(imbalance, 4)
                if imbalance is not None
                else None
            ),

            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),

            "updated_at":
                self.orderbook_updated_at,
        }


spot_collector = BybitSpotCollector()
