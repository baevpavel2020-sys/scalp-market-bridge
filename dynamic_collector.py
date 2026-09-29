import json
import threading
import time
import uuid
from collections import deque

import websocket


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


class MarketStream:
    def __init__(self, symbol, market):
        self.symbol = symbol.upper()
        self.market = market

        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread = None

        self.status = "created"
        self.connected = False

        self.started_at = None
        self.session_started_at = None
        self.session_id = None

        self.last_message_at = None
        self.last_error = None

        self.connect_attempts = 0
        self.successful_connections = 0
        self.reconnects = 0

        self.ticker = {}

        self.bids = {}
        self.asks = {}
        self.orderbook_ready = False
        self.orderbook_updated_at = None

        # timestamp_ms, side, price, size
        self.trades = deque()

    # =====================================================
    # LIFECYCLE
    # =====================================================

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return

            self.stop_event.clear()
            self.started_at = time.time()
            self.status = "starting"

            self.thread = threading.Thread(
                target=self._run_forever,
                daemon=True,
                name=f"{self.market}-{self.symbol}",
            )

            self.thread.start()

    def stop(self):
        self.stop_event.set()

        with self.lock:
            self.status = "stopping"

    def_run_forever(self):
        backoff = 1

        while not self.stop_event.is_set():

            with self.lock:
                self.status = "connecting"
                self.connect_attempts += 1

            try:
                self._run_connection()

                if self.stop_event.is_set():
                    break

            except Exception as exc:

                with self.lock:
                    self.connected = False
                    self.orderbook_ready = False
                    self.status = "reconnecting"
                    self.last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )

                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

        with self.lock:
            self.connected = False
            self.status = "stopped"

    # =====================================================
    # WEBSOCKET
    # =====================================================

    def _run_connection(self):

        endpoint = WS_URLS[self.market]

        ws = websocket.create_connection(
            endpoint,
            timeout=30,
        )

        try:
            with self.lock:

                if self.successful_connections > 0:
                    self.reconnects += 1

                self.successful_connections += 1

                self.connected = True
                self.status = "connected"
                self.last_error = None

                self.session_id = str(uuid.uuid4())
                self.session_started_at = time.time()

                self.bids.clear()
                self.asks.clear()
                self.trades.clear()

                self.orderbook_ready = False

            subscription = {
                "op": "subscribe",
                "args": [
                    f"publicTrade.{self.symbol}",
                    f"orderbook.50.{self.symbol}",
                    f"tickers.{self.symbol}",
                ],
            }

            ws.send(json.dumps(subscription))

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

            try:
                ws.close()
            except Exception:
                pass

    # =====================================================
    # ROUTER
    # =====================================================

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

    # =====================================================
    # TRADES
    # =====================================================

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

    # =====================================================
    # ORDERBOOK
    # =====================================================

    def _handle_orderbook(self, message):

        data = message.get("data", {})
        msg_type = message.get("type")

        bids = data.get("b", [])
        asks = data.get("a", [])

        with self.lock:

            if msg_type == "snapshot":

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

            elif msg_type == "delta":

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

    # =====================================================
    # TICKER
    # =====================================================

    def _handle_ticker(self, message):

        data = message.get("data", {})

        with self.lock:

            for key, value in data.items():

                if isinstance(
                    value,
                    (str, int, float, bool)
                ):
                    self.ticker[key] = value

            self.ticker["updated_at"] = time.time()

    # =====================================================
    # FLOW
    # =====================================================

    def flow_metrics(self):

        now_ms = int(time.time() * 1000)

        with self.lock:

            self._cleanup_trades(now_ms)

            result = {}

            for name, duration in WINDOWS.items():

                cutoff = now_ms - duration

                buy = 0.0
                sell = 0.0
                count = 0

                first_price = None
                last_price = None

                for (
                    timestamp_ms,
                    side,
                    price,
                    size,
                ) in reversed(self.trades):

                    if timestamp_ms < cutoff:
                        break

                    count += 1

                    first_price = price

                    if last_price is None:
                        last_price = price

                    if side == "Buy":
                        buy += size

                    elif side == "Sell":
                        sell += size

                total = buy + sell
                delta = buy - sell

                price_change_pct = None

                if (
                    first_price
                    and last_price
                    and first_price != 0
                ):
                    price_change_pct = (
                        (
                            last_price
                            - first_price
                        )
                        / first_price
                    ) * 100

                result[name] = {
                    "buy_volume":
                        round(buy, 8),

                    "sell_volume":
                        round(sell, 8),

                    "delta":
                        round(delta, 8),

                    "total_volume":
                        round(total, 8),

                    "trade_count":
                        count,

                    "buy_ratio": (
                        round(buy / total, 4)
                        if total > 0
                        else None
                    ),

                    "sell_ratio": (
                        round(sell / total, 4)
                        if total > 0
                        else None
                    ),

                    "price_change_pct": (
                        round(
                            price_change_pct,
                            5
                        )
                        if price_change_pct
                        is not None
                        else None
                    ),
                }

            return result

    # =====================================================
    # BOOK METRICS
    # =====================================================

    def book_metrics(self):

        with self.lock:

            if (
                not self.orderbook_ready
                or not self.bids
                or not self.asks
            ):
                return {
                    "ready": False
                }

            best_bid = max(self.bids)
            best_ask = min(self.asks)

            bid_depth = sum(
                self.bids.values()
            )

            ask_depth = sum(
                self.asks.values()
            )

            total = (
                bid_depth
                + ask_depth
            )

            imbalance = (
                (
                    bid_depth
                    - ask_depth
                )
                / total
                if total > 0
                else None
            )

            mid = (
                best_bid
                + best_ask
            ) / 2

            spread = (
                best_ask
                - best_bid
            )

            return {
                "ready": True,

                "best_bid":
                    best_bid,

                "best_ask":
                    best_ask,

                "spread":
                    round(spread, 8),

                "spread_bps": (
                    round(
                        (
                            spread
                            / mid
                        )
                        * 10_000,
                        4,
                    )
                    if mid > 0
                    else None
                ),

                "bid_depth_50":
                    round(
                        bid_depth,
                        8
                    ),

                "ask_depth_50":
                    round(
                        ask_depth,
                        8
                    ),

                "imbalance_50": (
                    round(
                        imbalance,
                        4
                    )
                    if imbalance
                    is not None
                    else None
                ),

                "bid_levels":
                    len(self.bids),

                "ask_levels":
                    len(self.asks),
            }

    # =====================================================
    # SNAPSHOT
    # =====================================================

    def snapshot(self):

        now = time.time()

        with self.lock:

            session_age = None

            if self.session_started_at:
                session_age = (
                    now
                    - self.session_started_at
                )

            message_age = None

            if self.last_message_at:
                message_age = (
                    now
                    - self.last_message_at
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

                "session_id":
                    self.session_id,

                "session_age_seconds": (
                    round(
                        session_age,
                        2
                    )
                    if session_age
                    is not None
                    else None
                ),

                "last_message_age_seconds": (
                    round(
                        message_age,
                        2
                    )
                    if message_age
                    is not None
                    else None
                ),

                "reconnects":
                    self.reconnects,

                "last_error":
                    self.last_error,

                "warmup": {
                    "1m":
                        session_age is not None
                        and session_age >= 60,

                    "5m":
                        session_age is not None
                        and session_age >= 300,

                    "15m":
                        session_age is not None
                        and session_age >= 900,

                    "1h":
                        session_age is not None
                        and session_age >= 3600,
                },

                "ticker":
                    dict(self.ticker),

                "flow":
                    self.flow_metrics(),

                "orderbook":
                    self.book_metrics(),
            }


# =========================================================
# DYNAMIC MANAGER
# =========================================================

class DynamicMarketManager:

    def __init__(
        self,
        max_symbols=6,
        idle_timeout=3600,
    ):

        self.lock = threading.RLock()

        self.max_symbols = max_symbols
        self.idle_timeout = idle_timeout

        self.streams = {}
        self.last_access = {}

    # -----------------------------------------------------

    def activate(self, symbol):

        symbol = symbol.upper()

        if not symbol.endswith("USDT"):
            symbol += "USDT"

        with self.lock:

            self._cleanup_idle()

            self.last_access[symbol] = time.time()

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

                self.streams[symbol] = {
                    "linear": linear,
                    "spot": spot,
                }

                linear.start()
                spot.start()

            return symbol

    # -----------------------------------------------------

    def snapshot(self, symbol):

        symbol = self.activate(symbol)

        with self.lock:

            self.last_access[symbol] = time.time()

            pair = self.streams[symbol]

            linear = pair["linear"].snapshot()
            spot = pair["spot"].snapshot()

        return {
            "symbol": symbol,

            "linear":
                linear,

            "spot":
                spot,

            "driver":
                self._driver_analysis(
                    linear,
                    spot,
                ),

            "generated_at":
                time.time(),
        }

    # -----------------------------------------------------

            def _driver_analysis(
        self,
        linear,
        spot,
    ):

        result = {}

        for window in WINDOWS:

            perp_flow = (
                linear
                .get("flow", {})
                .get(window, {})
            )

            spot_flow = (
                spot
                .get("flow", {})
                .get(window, {})
            )

            perp_ready = (
                linear
                .get("warmup", {})
                .get(window, False)
            )

            spot_ready = (
                spot
                .get("warmup", {})
                .get(window, False)
            )

            perp_delta = (
                perp_flow.get("delta")
            )

            spot_delta = (
                spot_flow.get("delta")
            )

            perp_total = (
                perp_flow.get("total_volume")
            )

            spot_total = (
                spot_flow.get("total_volume")
            )

            perp_price = (
                perp_flow.get(
                    "price_change_pct"
                )
            )

            spot_price = (
                spot_flow.get(
                    "price_change_pct"
                )
            )

            # ---------------------------------------------
            # NORMALIZED DELTA
            #
            # +1.0 = весь объём агрессивные покупки
            # -1.0 = весь объём агрессивные продажи
            #  0.0 = баланс
            # ---------------------------------------------

            perp_delta_ratio = None
            spot_delta_ratio = None

            if (
                perp_delta is not None
                and perp_total
                and perp_total > 0
            ):
                perp_delta_ratio = (
                    perp_delta
                    / perp_total
                )

            if (
                spot_delta is not None
                and spot_total
                and spot_total > 0
            ):
                spot_delta_ratio = (
                    spot_delta
                    / spot_total
                )

            # ---------------------------------------------
            # Пока окно не накоплено полностью,
            # никаких выводов о Driver не делаем.
            # ---------------------------------------------

            if not (
                perp_ready
                and spot_ready
            ):

                state = "warming_up"
                driver = "unknown"

            else:

                state = "balanced"
                driver = "mixed"

                threshold = 0.05

                perp_buy = (
                    perp_delta_ratio
                    is not None
                    and perp_delta_ratio
                    > threshold
                )

                perp_sell = (
                    perp_delta_ratio
                    is not None
                    and perp_delta_ratio
                    < -threshold
                )

                spot_buy = (
                    spot_delta_ratio
                    is not None
                    and spot_delta_ratio
                    > threshold
                )

                spot_sell = (
                    spot_delta_ratio
                    is not None
                    and spot_delta_ratio
                    < -threshold
                )

                # -----------------------------------------
                # BOTH MARKETS CONFIRM
                # -----------------------------------------

                if (
                    perp_buy
                    and spot_buy
                ):

                    state = (
                        "spot_and_perp_buying"
                    )

                    if (
                        abs(spot_delta_ratio)
                        > abs(perp_delta_ratio)
                    ):
                        driver = "spot"

                    elif (
                        abs(perp_delta_ratio)
                        > abs(spot_delta_ratio)
                    ):
                        driver = "perp"

                    else:
                        driver = "both"

                elif (
                    perp_sell
                    and spot_sell
                ):

                    state = (
                        "spot_and_perp_selling"
                    )

                    if (
                        abs(spot_delta_ratio)
                        > abs(perp_delta_ratio)
                    ):
                        driver = "spot"

                    elif (
                        abs(perp_delta_ratio)
                        > abs(spot_delta_ratio)
                    ):
                        driver = "perp"

                    else:
                        driver = "both"

                # -----------------------------------------
                # DISAGREEMENT
                # -----------------------------------------

                elif (
                    perp_buy
                    and spot_sell
                ):

                    state = (
                        "perp_buying_spot_selling"
                    )

                    driver = "perp"

                elif (
                    perp_sell
                    and spot_buy
                ):

                    state = (
                        "perp_selling_spot_buying"
                    )

                    driver = "perp"

                # -----------------------------------------
                # ONLY PERP HAS STRONG AGGRESSION
                # -----------------------------------------

                elif perp_buy:

                    state = "perp_led_buying"
                    driver = "perp"

                elif perp_sell:

                    state = "perp_led_selling"
                    driver = "perp"

                # -----------------------------------------
                # ONLY SPOT HAS STRONG AGGRESSION
                # -----------------------------------------

                elif spot_buy:

                    state = "spot_led_buying"
                    driver = "spot"

                elif spot_sell:

                    state = "spot_led_selling"
                    driver = "spot"

            result[window] = {

                "state":
                    state,

                "driver":
                    driver,

                "perp_delta":
                    perp_delta,

                "spot_delta":
                    spot_delta,

                "perp_delta_ratio": (
                    round(
                        perp_delta_ratio,
                        4
                    )
                    if perp_delta_ratio
                    is not None
                    else None
                ),

                "spot_delta_ratio": (
                    round(
                        spot_delta_ratio,
                        4
                    )
                    if spot_delta_ratio
                    is not None
                    else None
                ),

                "perp_price_change_pct":
                    perp_price,

                "spot_price_change_pct":
                    spot_price,

                "perp_ready":
                    perp_ready,

                "spot_ready":
                    spot_ready,
            }

        return result

    # -----------------------------------------------------

    def _cleanup_idle(self):

        now = time.time()

        stale = []

        for (
            symbol,
            last_seen,
        ) in self.last_access.items():

            if (
                now - last_seen
                > self.idle_timeout
            ):
                stale.append(symbol)

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

                pair["linear"].stop()
                pair["spot"].stop()

    # -----------------------------------------------------

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

        pair["linear"].stop()
        pair["spot"].stop()


dynamic_manager = DynamicMarketManager(
    max_symbols=6,
    idle_timeout=3600,
)
