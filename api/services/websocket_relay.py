"""
Finnhub WebSocket relay — persistent streaming connection for real-time price ticks.

Maintains a WebSocket connection to wss://ws.finnhub.io, subscribes to up to
WEBSOCKET_SYMBOLS_LIMIT symbols, buffers incoming ticks, and exposes them for
periodic flush to the price_ticks table.

Augments the existing REST polling loop: symbols with active WS subscriptions
get real-time updates; the rest continue polling via _price_refresh_loop.
"""

import asyncio
import collections
import json
import logging
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FINNHUB_WS_URL = "wss://ws.finnhub.io"


class FinnhubWebSocketRelay:
    """Async WebSocket relay for Finnhub trade data."""

    def __init__(
        self,
        api_key: str,
        symbols_limit: int = 50,
        flush_interval: int = 5,
        reconnect_max_delay: int = 300,
    ):
        self._api_key = api_key
        self._symbols_limit = symbols_limit
        self._flush_interval = flush_interval
        self._reconnect_max_delay = reconnect_max_delay

        # Connection state
        self._ws = None
        self._connected = False
        self._started_at: float | None = None
        # Defer Event creation to start() when we know we're in the right loop
        self._stop_event: asyncio.Event | None = None

        # Symbol management — protected by _sym_lock for cross-thread safety
        self._subscribed: set[str] = set()
        self._pending_desired: set[str] | None = None
        self._sym_lock = threading.Lock()

        # Tick buffer — flat deque of dicts, drained by flush loop
        self._tick_buffer: collections.deque = collections.deque(maxlen=50000)
        self._buffer_warning_logged = False

        # Callback for real-time price updates (called on each trade)
        self._update_callback = None

        # Event loop reference for cross-thread scheduling
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    def get_active_symbols(self) -> set[str]:
        """Symbols with live WebSocket subscriptions."""
        if not self._connected:
            return set()
        return set(self._subscribed)

    def get_tick_buffer_size(self) -> int:
        return len(self._tick_buffer)

    def get_uptime_seconds(self) -> float:
        if self._started_at is None:
            return 0.0
        return time.time() - self._started_at

    def drain_tick_buffer(self) -> list[dict]:
        """Drain and return all buffered ticks for DB flush."""
        ticks = []
        while self._tick_buffer:
            try:
                ticks.append(self._tick_buffer.popleft())
            except IndexError:
                break
        return ticks

    async def start(self, update_callback=None):
        """
        Main loop: connect, subscribe, receive trades, reconnect on failure.

        update_callback(symbol, price, volume, timestamp) is called on each
        incoming trade for real-time in-memory price updates.
        """
        self._update_callback = update_callback
        self._started_at = time.time()
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        backoff = 1

        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
                backoff = 1  # reset on clean disconnect
            except Exception as e:
                logger.warning("[WS Relay] Connection error: %s", e)
            finally:
                self._connected = False

            if self._stop_event.is_set():
                break

            # Exponential backoff with cap
            logger.info("[WS Relay] Reconnecting in %ds...", backoff)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                break  # stop_event was set during wait
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, self._reconnect_max_delay)

    async def stop(self):
        """Graceful shutdown."""
        if self._stop_event is not None:
            self._stop_event.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._connected = False
        logger.info("[WS Relay] Stopped")

    def update_symbols(self, symbols: list[str]):
        """
        Update the desired subscription set. Thread-safe — can be called from
        sync threads (e.g., _price_refresh_loop). Schedules subscription
        changes on the event loop if connected.
        """
        desired = set(symbols[: self._symbols_limit])
        with self._sym_lock:
            if self._connected and self._ws is not None and self._loop is not None:
                # Schedule on the correct event loop from any thread
                asyncio.run_coroutine_threadsafe(
                    self._sync_subscriptions(desired), self._loop
                )
            else:
                self._pending_desired = desired

    async def _connect_and_listen(self):
        """Connect to Finnhub WS, subscribe, and process messages."""
        try:
            import websockets
        except ImportError:
            logger.error("[WS Relay] websockets package not installed — pip install websockets")
            await asyncio.sleep(60)
            return

        # NOTE: Assumes single uvicorn worker. Multi-worker deployment would cause duplicate WebSocket connections.
        url = f"{_FINNHUB_WS_URL}?token={self._api_key}"
        logger.info("[WS Relay] Connecting to Finnhub WebSocket")
        async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
            self._ws = ws
            self._connected = True
            logger.info("[WS Relay] Connected to Finnhub WebSocket")

            # Apply any pending symbol set from update_symbols called while disconnected
            with self._sym_lock:
                if self._pending_desired is not None:
                    self._subscribed = self._pending_desired
                    self._pending_desired = None

            # Subscribe to current symbol set
            for symbol in list(self._subscribed):
                await self._send_subscribe(ws, symbol)

            # Receive loop
            async for raw_msg in ws:
                if self._stop_event.is_set():
                    break
                try:
                    self._handle_message(raw_msg)
                except Exception as e:
                    logger.debug("[WS Relay] Message parse error: %s", e)

    async def _sync_subscriptions(self, desired: set[str]):
        """Subscribe to new symbols, unsubscribe from removed ones."""
        if self._ws is None:
            return
        with self._sym_lock:
            to_add = desired - self._subscribed
            to_remove = self._subscribed - desired
        for symbol in to_remove:
            await self._send_unsubscribe(self._ws, symbol)
        for symbol in to_add:
            await self._send_subscribe(self._ws, symbol)
        with self._sym_lock:
            self._subscribed = desired

    async def _send_subscribe(self, ws, symbol: str):
        try:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        except Exception as e:
            logger.debug("[WS Relay] Subscribe error for %s: %s", symbol, e)

    async def _send_unsubscribe(self, ws, symbol: str):
        try:
            await ws.send(json.dumps({"type": "unsubscribe", "symbol": symbol}))
        except Exception as e:
            logger.debug("[WS Relay] Unsubscribe error for %s: %s", symbol, e)

    def _handle_message(self, raw_msg: str):
        """Parse a Finnhub trade message and buffer ticks."""
        msg = json.loads(raw_msg)
        if msg.get("type") != "trade":
            return

        trades = msg.get("data", [])
        for trade in trades:
            symbol = trade.get("s", "")
            price = trade.get("p")
            volume = trade.get("v")
            ts_ms = trade.get("t")

            if not symbol or price is None:
                continue

            ts_iso = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
                if ts_ms
                else datetime.now(tz=timezone.utc).isoformat()
            )

            # Buffer for DB flush — warn once when crossing 80% threshold
            if len(self._tick_buffer) > self._tick_buffer.maxlen * 0.8:
                if not self._buffer_warning_logged:
                    logger.warning("[WS Relay] Tick buffer at %d/%d capacity, old ticks being dropped",
                                   len(self._tick_buffer), self._tick_buffer.maxlen)
                    self._buffer_warning_logged = True
            else:
                self._buffer_warning_logged = False
            self._tick_buffer.append({
                "symbol": symbol,
                "price": float(price),
                "volume": float(volume) if volume is not None else None,
                "timestamp": ts_iso,
                "source": "finnhub_ws",
            })

            # Real-time in-memory update callback
            if self._update_callback:
                try:
                    self._update_callback(symbol, float(price), volume, ts_iso)
                except Exception:
                    pass
