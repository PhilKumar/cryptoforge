"""
engine/ws_feed.py — CryptoForge WebSocket Feed Manager
Real-time market data from Delta Exchange via WebSocket.
Enterprise-grade: auto-reconnect, exponential backoff, heartbeat, channel management.

Delta Exchange WebSocket docs:
- Production: wss://socket.india.delta.exchange  (India)
              wss://socket.delta.exchange          (Global)
- Testnet:    wss://testnet-socket.delta.exchange

Channels:
- candlestick_{resolution}_{symbol}   — OHLCV candle updates
- v2/ticker/{symbol}                   — Real-time ticker
- orders                               — Order updates (auth required)
- positions                            — Position updates (auth required)

Usage:
    feed = DeltaWSFeed()
    feed.on_candle = my_candle_handler
    feed.on_ticker = my_ticker_handler
    await feed.connect()
    await feed.subscribe_candles("BTCUSDT", "1m")
"""

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time as _time
from typing import Callable, Optional, Set

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

# ── Constants ──────────────────────────────────────────────────────
HEARTBEAT_INTERVAL = 25  # seconds (Delta timeout is 30s)
RECONNECT_BASE = 1.0  # initial reconnect delay
RECONNECT_MAX = 32.0  # max reconnect delay
RECONNECT_MULTIPLIER = 2.0  # exponential multiplier
MAX_RECONNECT_ATTEMPTS = 50  # before giving up completely


class DeltaWSFeed:
    """
    Delta Exchange WebSocket feed manager.

    Features:
    - Auto-reconnect with exponential backoff (1s, 2s, 4s, 8s, 16s, 32s cap)
    - Heartbeat ping every 25 seconds
    - Channel subscription management (auto-resubscribe on reconnect)
    - Authenticated channels (orders, positions) with HMAC signature
    - Callbacks: on_candle, on_ticker, on_order, on_position, on_error
    """

    def __init__(self):
        # Determine WebSocket URL
        testnet = os.getenv("DELTA_TESTNET", "false").lower() == "true"
        if testnet:
            self._ws_url = "wss://testnet-socket.delta.exchange"
        else:
            region = os.getenv("DELTA_REGION", "india").lower()
            self._ws_url = "wss://socket.india.delta.exchange" if region == "india" else "wss://socket.delta.exchange"

        self._api_key = config.DELTA_API_KEY
        self._api_secret = config.DELTA_API_SECRET

        # Connection state
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._connected = False
        self._authenticated = False
        self._running = False
        self._reconnect_attempt = 0
        self._last_message_time = 0
        self._connected_event = asyncio.Event()

        # Channel management
        self._subscribed_channels: Set[str] = set()
        self._pending_auth_channels: Set[str] = set()

        # Callbacks
        self.on_candle: Optional[Callable] = None  # (symbol, resolution, candle_dict)
        self.on_ticker: Optional[Callable] = None  # (symbol, ticker_dict)
        self.on_order: Optional[Callable] = None  # (order_dict)
        self.on_position: Optional[Callable] = None  # (position_dict)
        self.on_error: Optional[Callable] = None  # (error_str)
        self.on_connect: Optional[Callable] = None  # ()
        self.on_disconnect: Optional[Callable] = None  # (reason_str)

        # Tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_task: Optional[asyncio.Task] = None

        # Stats
        self.messages_received = 0
        self.reconnect_count = 0
        self.last_error = ""

    @staticmethod
    def _to_delta_symbol(symbol: str) -> str:
        """Convert Binance-style XXXUSDT to Delta India XXXUSD format."""
        region = os.getenv("DELTA_REGION", getattr(config, "DELTA_REGION", "india")).lower()
        if region == "india" and symbol and symbol.upper().endswith("USDT"):
            return symbol[:-1]  # strip trailing 'T'
        return symbol

    # ── Properties ──────────────────────────────────────────────
    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None and not self._ws.closed

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    # ── Auth Signature ──────────────────────────────────────────
    def _generate_auth_signature(self) -> dict:
        """Generate HMAC-SHA256 signature for WebSocket authentication."""
        method = "GET"
        timestamp = str(int(_time.time()))
        path = "/live"
        message = method + timestamp + path
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "api-key": self._api_key,
            "timestamp": timestamp,
            "signature": signature,
        }

    # ── Connect ─────────────────────────────────────────────────
    async def connect(self):
        """
        Connect to Delta Exchange WebSocket.
        Starts heartbeat and message reader tasks.
        Auto-reconnects on disconnect.
        Returns once the socket is ready for subscriptions.
        """
        self._running = True
        if self.connected:
            return
        if self._connect_task is None or self._connect_task.done():
            self._connected_event.clear()
            self._connect_task = asyncio.create_task(self._do_connect())
        await asyncio.wait_for(self._connected_event.wait(), timeout=15)

    async def _do_connect(self):
        """Internal connection logic with retry."""
        while self._running and self._reconnect_attempt < MAX_RECONNECT_ATTEMPTS:
            try:
                if self._session is None or self._session.closed:
                    timeout = aiohttp.ClientTimeout(total=30, connect=15)
                    self._session = aiohttp.ClientSession(timeout=timeout)

                print(f"[WS] Connecting to {self._ws_url}...")
                self._ws = await self._session.ws_connect(
                    self._ws_url,
                    heartbeat=HEARTBEAT_INTERVAL,
                    receive_timeout=60,
                )

                self._connected = True
                self._reconnect_attempt = 0
                self._last_message_time = _time.time()
                self._connected_event.set()
                print("[WS] Connected to Delta Exchange WebSocket")

                # Authenticate if API keys are configured
                if self._api_key and self._api_key != "YOUR_API_KEY_HERE":
                    await self._authenticate()

                # Resubscribe to previously subscribed channels
                if self._subscribed_channels:
                    print(f"[WS] Resubscribing to {len(self._subscribed_channels)} channels...")
                    channels_copy = list(self._subscribed_channels)
                    self._subscribed_channels.clear()
                    for channel in channels_copy:
                        await self._send_subscribe(channel)

                # Fire on_connect callback
                if self.on_connect:
                    try:
                        result = self.on_connect()
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        print(f"[WS] on_connect callback error: {e}")

                # Cancel any stale tasks before creating new ones
                for _t in (self._reader_task, self._heartbeat_task):
                    if _t and not _t.done():
                        _t.cancel()

                # Start reader and heartbeat tasks
                self._reader_task = asyncio.create_task(self._reader_loop())
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                # Reader/heartbeat continue in the background. When the
                # socket drops, _reader_loop triggers reconnect handling.
                return

            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                self._connected = False
                self._authenticated = False
                self._connected_event.clear()
                self.last_error = str(e)
                print(f"[WS] Connection failed: {e}")

                if self.on_disconnect:
                    try:
                        result = self.on_disconnect(str(e))
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        pass

                # Exponential backoff
                if not self._running:
                    break
                self._reconnect_attempt += 1
                delay = min(
                    RECONNECT_BASE * (RECONNECT_MULTIPLIER ** (self._reconnect_attempt - 1)),
                    RECONNECT_MAX,
                )
                self.reconnect_count += 1
                print(f"[WS] Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempt}/{MAX_RECONNECT_ATTEMPTS})")
                await asyncio.sleep(delay)

        if self._running:
            print(f"[WS] Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) exhausted")
            self._running = False

    # ── Authentication ──────────────────────────────────────────
    async def _authenticate(self):
        """Send authentication payload for private channels."""
        if not self._ws or self._ws.closed:
            return
        try:
            auth = self._generate_auth_signature()
            payload = {
                "type": "auth",
                "payload": auth,
            }
            await self._ws.send_json(payload)
            print("[WS] Auth request sent")
            # Auth response will be handled in _reader_loop
        except Exception as e:
            print(f"[WS] Auth failed: {e}")

    # ── Subscribe / Unsubscribe ─────────────────────────────────
    async def subscribe_candles(self, symbol: str, resolution: str):
        """Subscribe to candlestick updates."""
        # Delta India uses USD suffix (BTCUSD), map from USDT
        symbol = self._to_delta_symbol(symbol)
        channel = f"candlestick_{resolution}_{symbol}"
        await self._send_subscribe(channel)

    async def subscribe_ticker(self, symbol: str):
        """Subscribe to real-time ticker."""
        # Delta India uses USD suffix (BTCUSD), map from USDT
        symbol = self._to_delta_symbol(symbol)
        channel = f"v2/ticker/{symbol}"
        await self._send_subscribe(channel)

    async def subscribe_orders(self):
        """Subscribe to order updates (requires auth)."""
        channel = "orders"
        if not self._authenticated:
            self._pending_auth_channels.add(channel)
            print(f"[WS] Queued '{channel}' — waiting for auth")
            return
        await self._send_subscribe(channel)

    async def subscribe_positions(self):
        """Subscribe to position updates (requires auth)."""
        channel = "positions"
        if not self._authenticated:
            self._pending_auth_channels.add(channel)
            return
        await self._send_subscribe(channel)

    async def unsubscribe(self, channel: str):
        """Unsubscribe from a channel."""
        if not self._ws or self._ws.closed:
            return
        self._subscribed_channels.discard(channel)
        try:
            await self._ws.send_json(
                {
                    "type": "unsubscribe",
                    "payload": {"channels": [{"name": channel}]},
                }
            )
        except Exception as e:
            print(f"[WS] Unsubscribe error: {e}")

    async def _send_subscribe(self, channel: str):
        """Send subscribe message for a channel."""
        if not self._ws or self._ws.closed:
            self._subscribed_channels.add(channel)  # Will resubscribe on connect
            return
        try:
            payload = {
                "type": "subscribe",
                "payload": {"channels": [{"name": channel}]},
            }
            await self._ws.send_json(payload)
            self._subscribed_channels.add(channel)
            print(f"[WS] Subscribed: {channel}")
        except Exception as e:
            print(f"[WS] Subscribe error for {channel}: {e}")

    # ── Message Reader ──────────────────────────────────────────
    async def _reader_loop(self):
        """Read and dispatch WebSocket messages."""
        try:
            async for msg in self._ws:
                self._last_message_time = _time.time()
                self.messages_received += 1

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._dispatch(data)
                    except json.JSONDecodeError:
                        print(f"[WS] Invalid JSON: {msg.data[:100]}")

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"[WS] Error: {self._ws.exception()}")
                    break

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    print(f"[WS] Connection closed: {msg.data}")
                    break

        except asyncio.CancelledError:
            return
        except Exception as e:
            self.last_error = str(e)
            print(f"[WS] Reader error: {e}")

        finally:
            self._connected = False
            self._authenticated = False
            self._connected_event.clear()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

            if self.on_disconnect:
                try:
                    result = self.on_disconnect("reader_loop_ended")
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass

            # Auto-reconnect — cancel any in-flight connect task before spawning a new one
            if self._running:
                print("[WS] Initiating reconnect...")
                if self._connect_task and not self._connect_task.done():
                    self._connect_task.cancel()
                self._connect_task = asyncio.create_task(self._do_connect())

    # ── Message Dispatch ────────────────────────────────────────
    async def _dispatch(self, data: dict):
        """Route incoming messages to appropriate handlers."""
        msg_type = data.get("type", "")

        # Auth response
        if msg_type == "auth":
            if data.get("payload", {}).get("result") == "success":
                self._authenticated = True
                print("[WS] Authenticated successfully")
                # Subscribe to pending auth channels
                for ch in list(self._pending_auth_channels):
                    await self._send_subscribe(ch)
                self._pending_auth_channels.clear()
            else:
                print(f"[WS] Auth failed: {data}")
            return

        # Subscription confirmation
        if msg_type == "subscriptions":
            channels = data.get("payload", {}).get("channels", [])
            print(f"[WS] Active subscriptions: {[c.get('name') for c in channels]}")
            return

        # Channel data
        channel = data.get("channel", "")

        # Candlestick updates
        if channel.startswith("candlestick_"):
            if self.on_candle:
                parts = channel.split("_", 2)  # candlestick_1m_BTCUSDT
                if len(parts) >= 3:
                    resolution = parts[1]
                    symbol = parts[2]
                    candle = data.get("payload", data.get("data", {}))
                    try:
                        result = self.on_candle(symbol, resolution, candle)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        print(f"[WS] on_candle error: {e}")

        # Ticker updates
        elif channel.startswith("v2/ticker/"):
            if self.on_ticker:
                symbol = channel.split("/")[-1]
                ticker = data.get("payload", data.get("data", {}))
                try:
                    result = self.on_ticker(symbol, ticker)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    print(f"[WS] on_ticker error: {e}")

        # Order updates
        elif channel == "orders":
            if self.on_order:
                order = data.get("payload", data.get("data", {}))
                try:
                    result = self.on_order(order)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    print(f"[WS] on_order error: {e}")

        # Position updates
        elif channel == "positions":
            if self.on_position:
                position = data.get("payload", data.get("data", {}))
                try:
                    result = self.on_position(position)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    print(f"[WS] on_position error: {e}")

    # ── Heartbeat ───────────────────────────────────────────────
    async def _heartbeat_loop(self):
        """Send periodic pings to keep connection alive."""
        try:
            while self._running and self.connected:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self._ws and not self._ws.closed:
                    try:
                        await self._ws.ping()
                    except Exception:
                        print("[WS] Heartbeat ping failed")
                        break

                # Check for stale connection (no messages in 60s)
                if _time.time() - self._last_message_time > 60:
                    print("[WS] No messages for 60s — connection may be stale")
                    if self._ws and not self._ws.closed:
                        await self._ws.close()
                    break

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[WS] Heartbeat error: {e}")

    # ── Disconnect ──────────────────────────────────────────────
    async def disconnect(self):
        """Gracefully disconnect from WebSocket."""
        self._running = False
        self._connected = False
        self._connected_event.clear()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except asyncio.CancelledError:
                pass

        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

        self._heartbeat_task = None
        self._reader_task = None
        self._connect_task = None
        self._ws = None
        self._session = None

        print("[WS] Disconnected")

    # ── Status ──────────────────────────────────────────────────
    def get_status(self) -> dict:
        return {
            "connected": self.connected,
            "authenticated": self._authenticated,
            "ws_url": self._ws_url,
            "subscribed_channels": list(self._subscribed_channels),
            "messages_received": self.messages_received,
            "reconnect_count": self.reconnect_count,
            "last_error": self.last_error,
            "uptime_seconds": int(_time.time() - self._last_message_time) if self._last_message_time > 0 else 0,
        }


# ── Convenience: Create feed with candle callback ───────────────
async def create_candle_feed(symbol: str, resolution: str, callback: Callable) -> DeltaWSFeed:
    """Quick helper to create a WebSocket feed for a single candle stream."""
    feed = DeltaWSFeed()
    feed.on_candle = callback
    await feed.connect()
    await feed.subscribe_candles(symbol, resolution)
    return feed
