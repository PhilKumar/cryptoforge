"""
engine/scalp.py — CryptoForge Scalp Mode Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fast manual crypto scalping with automatic TP/SL exit:
  • Manual entry  → click LONG/SHORT → broker order placed immediately
  • Auto exit     → exits when price target, SL, or fixed $ gain/loss is hit
  • Paper mode    → no real orders, simulates with mark price

Completely isolated from LiveEngine and PaperTradingEngine.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    from engine.ws_feed import DeltaWSFeed

    _HAS_WS = True
except ImportError:
    _HAS_WS = False


def _now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _coerce_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ScalpTrade:
    """Represents a single open crypto scalp position."""

    def __init__(
        self,
        trade_id: int,
        symbol: str,  # e.g. BTCUSDT
        side: str,  # LONG or SHORT
        product_id: int,
        size: int,  # contract units
        entry_price: float,
        leverage: int = 10,
        # Exit rules (at least one should be set)
        target_price: float = 0.0,  # absolute price to take profit
        sl_price: float = 0.0,  # absolute SL price
        target_pct: float = 0.0,  # leveraged % gain target
        sl_pct: float = 0.0,  # leveraged % loss SL
        target_usd: float = 0.0,  # fixed $ profit target
        sl_usd: float = 0.0,  # fixed $ loss SL
        order_id: str = "",
        entry_time: Optional[datetime] = None,
        mode: str = "live",
        guardrail_price: float = 0.0,
    ):
        self.trade_id = trade_id
        self.symbol = symbol
        self.side = side
        self.product_id = product_id
        self.size = size
        self.entry_price = entry_price
        self.current_price = entry_price
        self.leverage = leverage
        self.order_id = order_id
        self.entry_time = entry_time or _now_utc()
        self.mode = mode
        self.guardrail_price = guardrail_price
        self._exit_guard_until = self.entry_time + timedelta(seconds=2 if mode == "live" else 1)
        self._prefer_fresh_rest_mark_until = self.entry_time + timedelta(seconds=5 if mode == "live" else 2)
        self._post_entry_price_ready = False
        self.last_price_source = "entry"
        self.last_price_update = self.entry_time
        self.entry_latency_ms: float = 0.0
        self.exit_latency_ms: float = 0.0

        # Resolve absolute TP/SL from percentage if needed
        self.target_pct = target_pct
        self.sl_pct = sl_pct
        self.target_price = target_price
        self.sl_price = sl_price
        self._apply_percentage_targets()

        self.target_usd = target_usd
        self.sl_usd = sl_usd

        self.exit_price: float = 0.0
        self.exit_time: Optional[datetime] = None
        self.exit_reason: str = ""
        self.exit_order_id: str = ""
        self.pnl: float = 0.0
        self.status: str = "open"

    def _apply_percentage_targets(self) -> None:
        if self.entry_price <= 0:
            return
        if not self.target_price and self.target_pct > 0:
            price_move_pct = self.target_pct / max(self.leverage, 1)
            mult = 1 if self.side == "LONG" else -1
            self.target_price = round(self.entry_price * (1 + mult * price_move_pct / 100), 6)
        if not self.sl_price and self.sl_pct > 0:
            price_move_pct = self.sl_pct / max(self.leverage, 1)
            mult = -1 if self.side == "LONG" else 1
            self.sl_price = round(self.entry_price * (1 + mult * price_move_pct / 100), 6)

    def prime_entry_price(self, price: float) -> bool:
        price = _coerce_float(price, 0.0)
        if price <= 0 or self.entry_price > 0:
            return False
        self.entry_price = price
        self._apply_percentage_targets()
        return True

    def should_prefer_fresh_rest_mark(self, now: Optional[datetime] = None) -> bool:
        now = now or _now_utc()
        return now < self._prefer_fresh_rest_mark_until

    def can_evaluate_exit(self, now: Optional[datetime] = None) -> bool:
        now = now or _now_utc()
        return self._post_entry_price_ready and now >= self._exit_guard_until

    # Delta Exchange India: taker 0.05%, 18% GST on fees
    TAKER_FEE_RATE = 0.0005
    GST_RATE = 0.18

    def _compute_pnl(self, price: float) -> float:
        """Gross PnL in USD (before fees). size = notional in USD (1 contract = $1)."""
        if not price or not self.entry_price or self.entry_price == 0:
            return 0.0
        if self.side == "LONG":
            return (price - self.entry_price) / self.entry_price * self.size
        else:
            return (self.entry_price - price) / self.entry_price * self.size

    def _compute_fees(self, exit_price: float = 0.0) -> float:
        """Trading fees in USD. Entry side always charged; exit side only if exit_price > 0."""
        fee_per_side = self.size * self.TAKER_FEE_RATE * (1 + self.GST_RATE)
        sides = 2 if exit_price > 0 else 1  # open = entry only, closed = entry + exit
        return round(sides * fee_per_side, 4)

    def check_exit(self, price: float) -> Optional[str]:
        """Returns exit reason string if an exit rule fires, else None."""
        if not price or price <= 0:
            return None
        pnl = self._compute_pnl(price)

        if self.side == "LONG":
            if self.target_price > 0 and price >= self.target_price:
                return "target_hit"
            if self.sl_price > 0 and price <= self.sl_price:
                return "sl_hit"
        else:  # SHORT
            if self.target_price > 0 and price <= self.target_price:
                return "target_hit"
            if self.sl_price > 0 and price >= self.sl_price:
                return "sl_hit"

        if self.target_usd > 0 and pnl >= self.target_usd:
            return "target_usd_hit"
        if self.sl_usd > 0 and pnl <= -self.sl_usd:
            return "sl_usd_hit"

        return None

    def to_dict(self) -> dict:
        gross_pnl = round(self._compute_pnl(self.current_price), 2)
        fees = self._compute_fees(self.exit_price)
        price_age_ms = None
        if self.last_price_update:
            price_age_ms = max(0, int((_now_utc() - self.last_price_update).total_seconds() * 1000))
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "side": self.side,
            "product_id": self.product_id,
            "size": self.size,
            "leverage": self.leverage,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "target_price": self.target_price,
            "sl_price": self.sl_price,
            "target_usd": self.target_usd,
            "sl_usd": self.sl_usd,
            "order_id": self.order_id,
            "guardrail_price": self.guardrail_price,
            "entry_time": str(self.entry_time),
            "exit_time": str(self.exit_time) if self.exit_time else None,
            "exit_price": self.exit_price,
            "exit_reason": self.exit_reason,
            "exit_order_id": self.exit_order_id,
            "pnl": gross_pnl,
            "fees": fees,
            "net_pnl": round(gross_pnl - fees, 2),
            "unrealized_pnl": gross_pnl,
            "qty_usdt": round(self.size / max(self.leverage, 1), 2),
            "mark_price": self.current_price,
            "price_source": self.last_price_source,
            "price_updated_at": str(self.last_price_update) if self.last_price_update else None,
            "price_age_ms": price_age_ms,
            "entry_latency_ms": self.entry_latency_ms,
            "exit_latency_ms": self.exit_latency_ms,
            "status": self.status,
            "mode": self.mode,
        }


class PendingScalpEntry:
    """Represents an armed scalp entry waiting for the guardrail trigger."""

    def __init__(
        self,
        entry_id: int,
        symbol: str,
        side: str,
        size: int,
        leverage: int,
        guardrail_price: float,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        mode: str = "live",
    ):
        self.entry_id = entry_id
        self.symbol = symbol
        self.side = side
        self.size = size
        self.leverage = leverage
        self.guardrail_price = guardrail_price
        self.target_price = target_price
        self.sl_price = sl_price
        self.target_pct = target_pct
        self.sl_pct = sl_pct
        self.target_usd = target_usd
        self.sl_usd = sl_usd
        self.mode = mode
        self.created_at = _now_utc()

    def should_trigger(self, price: float) -> bool:
        if price <= 0 or self.guardrail_price <= 0:
            return False
        if self.side == "LONG":
            return price >= self.guardrail_price
        return price <= self.guardrail_price

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "symbol": self.symbol,
            "side": self.side,
            "size": self.size,
            "leverage": self.leverage,
            "guardrail_price": self.guardrail_price,
            "target_price": self.target_price,
            "sl_price": self.sl_price,
            "target_usd": self.target_usd,
            "sl_usd": self.sl_usd,
            "mode": self.mode,
            "status": "pending",
            "created_at": str(self.created_at),
            "qty_usdt": round(self.size / max(self.leverage, 1), 2),
        }


class ScalpEngine:
    """
    Manages all active crypto scalp trades.
    • Runs a background monitoring loop.
    • Uses WebSocket ticker updates first for real-time pricing.
    • Falls back to REST bulk ticker fetch when WS is unavailable.
    """

    def __init__(
        self,
        delta_client,
        on_trade_closed: Optional[Callable[[dict], None]] = None,
        on_event: Optional[Callable[[dict], None]] = None,
        on_update: Optional[Callable[[dict], None]] = None,
    ):
        self.delta = delta_client
        # Callback invoked with trade dict whenever a trade is closed (for disk persistence).
        self._on_trade_closed = on_trade_closed
        self._on_event = on_event
        self._on_update = on_update
        self.open_trades: Dict[int, ScalpTrade] = {}
        self.pending_entries: Dict[int, PendingScalpEntry] = {}
        self.closed_trades: list = []
        self.event_log: list = []
        self._trade_counter: int = int(_now_utc().timestamp() * 1000)
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self._ws_feed = None
        self._ws_prices: Dict[str, float] = {}
        self._last_prices: Dict[str, float] = {}
        self._ws_subscribed_symbols: set[str] = set()
        self._last_price_ts: Dict[str, datetime] = {}
        self._last_price_source: Dict[str, str] = {}
        self._rest_price_fetches: int = 0
        self._last_execution: Dict[str, Any] = {}
        self._update_task: Optional[asyncio.Task] = None
        self._last_update_push: float = 0.0
        self._update_interval_sec: float = 0.25

    # ── Public API ───────────────────────────────────────────────

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._monitor_loop())
            self._schedule_update(force=True)

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        if self._update_task:
            self._update_task.cancel()
            self._update_task = None
        if self._ws_feed:
            try:
                asyncio.get_running_loop().create_task(self._stop_ws_feed())
            except RuntimeError:
                self._ws_feed = None
                self._ws_prices.clear()
                self._ws_subscribed_symbols.clear()

    @staticmethod
    def _extract_order_price(order: dict, fallback: float) -> float:
        for key in (
            "average_fill_price",
            "avg_fill_price",
            "fill_price",
            "average_price",
            "avg_price",
            "entry_price",
            "price",
            "mark_price",
        ):
            price = _coerce_float((order or {}).get(key), 0.0)
            if price > 0:
                return price
        return fallback

    async def _place_verified_order(self, **kwargs) -> dict:
        place_verified = getattr(self.delta, "place_order_verified", None)
        if not callable(place_verified):
            return {"error": "Broker does not support verified order placement", "verified": False}
        return await place_verified(**kwargs)

    def _canonical_symbol(self, symbol: str) -> str:
        if not symbol:
            return ""
        from_delta = getattr(self.delta, "from_delta_symbol", None)
        if callable(from_delta):
            try:
                return str(from_delta(symbol)).upper()
            except Exception:
                pass
        return str(symbol).upper()

    def _handle_ticker(self, sym: str, ticker: dict):
        try:
            mark = ticker.get("mark_price") or ticker.get("close") or ticker.get("last_price")
            price = _coerce_float(mark, 0.0)
            if price <= 0:
                return
            self._record_price(sym, price, source="ws")
            self._schedule_update()
        except Exception:
            pass

    def _record_price(self, symbol: str, price: float, source: str) -> None:
        if price <= 0:
            return
        now = _now_utc()
        canonical = self._canonical_symbol(symbol)
        if source == "ws":
            self._ws_prices[canonical] = price
        self._last_prices[canonical] = price
        self._last_price_ts[canonical] = now
        self._last_price_source[canonical] = source
        for trade in self.open_trades.values():
            if self._canonical_symbol(trade.symbol) == canonical:
                trade.prime_entry_price(price)
                trade.current_price = price
                trade.last_price_source = source
                trade.last_price_update = now
                if not trade._post_entry_price_ready:
                    trade._post_entry_price_ready = True

    def _cached_price(self, symbol: str) -> float:
        canonical = self._canonical_symbol(symbol)
        price = _coerce_float(self._ws_prices.get(canonical), 0.0)
        if price > 0:
            return price
        return _coerce_float(self._last_prices.get(canonical), 0.0)

    async def _emit_update(self):
        if not self._on_update:
            return
        self._last_update_push = asyncio.get_running_loop().time()
        payload = self.get_status()
        payload["closed_trades"] = list(reversed(self.closed_trades[-20:]))
        payload["event_log"] = list(reversed(self.event_log[-40:]))
        try:
            result = self._on_update(payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            pass

    async def _delayed_update(self, delay: float):
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self._emit_update()
        except asyncio.CancelledError:
            return
        finally:
            self._update_task = None

    def _schedule_update(self, force: bool = False):
        if not self._on_update:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if force:
            if self._update_task and not self._update_task.done():
                self._update_task.cancel()
            self._update_task = loop.create_task(self._delayed_update(0))
            return
        if self._update_task and not self._update_task.done():
            return
        elapsed = loop.time() - self._last_update_push
        delay = max(0.0, self._update_interval_sec - elapsed)
        self._update_task = loop.create_task(self._delayed_update(delay))

    async def _ensure_ws_feed(self, symbols: set[str]):
        if not _HAS_WS or not symbols:
            return
        if self._ws_feed is None:
            self._ws_feed = DeltaWSFeed()
            self._ws_feed.on_ticker = self._handle_ticker
            await self._ws_feed.connect()
            self._log("info", "WebSocket ticker connected for scalp pricing")

        pending = {self._canonical_symbol(sym) for sym in symbols} - self._ws_subscribed_symbols
        if not pending:
            return
        for sym in sorted(pending):
            try:
                await self._ws_feed.subscribe_ticker(sym)
                self._ws_subscribed_symbols.add(sym)
            except Exception as e:
                self._log("warn", f"WebSocket subscribe failed for {sym}: {e}")

    async def _stop_ws_feed(self):
        if self._ws_feed:
            try:
                await self._ws_feed.disconnect()
            except Exception:
                pass
        self._ws_feed = None
        self._ws_prices.clear()
        self._ws_subscribed_symbols.clear()

    async def enter_trade(
        self,
        symbol: str,
        side: str,  # LONG or SHORT
        size: int,  # contracts
        leverage: int = 10,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        guardrail_price: float = 0.0,
        mode: str = "live",
    ) -> Dict[str, Any]:
        """Place immediately, or arm a pending guardrail-triggered entry."""

        market_price = self._cached_price(symbol)
        if market_price <= 0 and (mode != "paper" or guardrail_price > 0):
            try:
                ticker = await asyncio.to_thread(self.delta.get_ticker, symbol)
                market_price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                if market_price > 0:
                    self._rest_price_fetches += 1
                    self._record_price(symbol, market_price, source="rest_quote")
            except Exception:
                pass

        if guardrail_price > 0:
            should_enter_now = False
            if market_price > 0:
                if side == "LONG":
                    should_enter_now = market_price >= guardrail_price
                else:
                    should_enter_now = market_price <= guardrail_price
            if not should_enter_now:
                self._trade_counter += 1
                pending = PendingScalpEntry(
                    entry_id=self._trade_counter,
                    symbol=symbol,
                    side=side,
                    size=size,
                    leverage=leverage,
                    guardrail_price=guardrail_price,
                    target_price=target_price,
                    sl_price=sl_price,
                    target_pct=target_pct,
                    sl_pct=sl_pct,
                    target_usd=target_usd,
                    sl_usd=sl_usd,
                    mode=mode,
                )
                self.pending_entries[pending.entry_id] = pending
                trigger_text = ">=" if side == "LONG" else "<="
                self._log(
                    "info",
                    f"🛡 Guardrail armed for {side} {symbol}: waiting for price {trigger_text} ${guardrail_price:,.4f}",
                )
                self._schedule_update(force=True)
                if not self._running:
                    self.start()
                else:
                    try:
                        asyncio.get_running_loop().create_task(self._ensure_ws_feed({symbol}))
                    except RuntimeError:
                        pass
                return {
                    "status": "pending",
                    "entry_id": pending.entry_id,
                    "message": f"Waiting for guardrail price ${guardrail_price:,.4f}",
                    "pending_entry": pending.to_dict(),
                }
            self._log(
                "info",
                f"🛡 Guardrail already satisfied for {side} {symbol} at ${market_price:,.4f} — entering now.",
            )

        return await self._open_trade(
            symbol=symbol,
            side=side,
            size=size,
            leverage=leverage,
            target_price=target_price,
            sl_price=sl_price,
            target_pct=target_pct,
            sl_pct=sl_pct,
            target_usd=target_usd,
            sl_usd=sl_usd,
            mode=mode,
            guardrail_price=guardrail_price,
            market_price=market_price,
        )

    async def _open_trade(
        self,
        *,
        symbol: str,
        side: str,
        size: int,
        leverage: int,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        mode: str = "live",
        guardrail_price: float = 0.0,
        market_price: float = 0.0,
    ) -> Dict[str, Any]:
        """Place a broker order (or simulate in paper mode) and register the scalp trade."""

        product_id = 0
        if mode != "paper":
            try:
                product = await asyncio.to_thread(self.delta.get_product_by_symbol, symbol)
                if product:
                    product_id = int(product.get("id", 0))
            except Exception:
                pass

        entry_price = market_price or self._cached_price(symbol)
        if entry_price <= 0 and mode != "paper":
            try:
                ticker = await asyncio.to_thread(self.delta.get_ticker, symbol)
                entry_price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                if entry_price > 0:
                    self._rest_price_fetches += 1
                    self._record_price(symbol, entry_price, source="rest_quote")
            except Exception:
                pass

        order_id = ""
        result: Dict[str, Any] = {}
        if mode == "paper":
            order_id = "PAPER"
        else:
            if not product_id:
                return {"status": "error", "message": f"Product not found for {symbol}"}
            try:
                order_side = "buy" if side == "LONG" else "sell"
                result = await self._place_verified_order(
                    product_id=product_id,
                    size=size,
                    side=order_side,
                    order_type="market_order",
                    leverage=leverage,
                )
                if isinstance(result, dict) and (result.get("error") or not result.get("verified")):
                    return {"status": "error", "message": result.get("error") or "entry order could not be verified"}
                order_id = str(result.get("id", "placed"))
                entry_price = self._extract_order_price(result, entry_price)
            except Exception as e:
                return {"status": "error", "message": str(e)}

        self._trade_counter += 1
        trade = ScalpTrade(
            trade_id=self._trade_counter,
            symbol=symbol,
            side=side,
            product_id=product_id,
            size=size,
            entry_price=entry_price,
            leverage=leverage,
            target_price=target_price,
            sl_price=sl_price,
            target_pct=target_pct,
            sl_pct=sl_pct,
            target_usd=target_usd,
            sl_usd=sl_usd,
            order_id=order_id,
            mode=mode,
            guardrail_price=guardrail_price,
        )
        self.open_trades[self._trade_counter] = trade
        trade.entry_latency_ms = round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1)
        self._record_price(symbol, entry_price, source="broker_fill" if mode != "paper" else "entry_snapshot")
        self._last_execution = {
            "phase": "entry",
            "symbol": symbol,
            "side": side,
            "mode": mode,
            "verified": bool(result.get("verified", mode == "paper")),
            "latency_ms": round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1),
            "ack_ms": round(_coerce_float(result.get("order_ack_ms"), 0.0), 1),
            "verified_at_attempt": int(result.get("verified_at_attempt", 0) or 0),
            "updated_at": str(_now_utc()),
        }

        mode_label = "[PAPER] " if mode == "paper" else ""
        exec_tail = ""
        if result.get("broker_latency_ms"):
            exec_tail = (
                f" verify={_coerce_float(result.get('broker_latency_ms'), 0.0):,.1f}ms"
                f" ack={_coerce_float(result.get('order_ack_ms'), 0.0):,.1f}ms"
            )
        self._log(
            "entry",
            f"{mode_label}✅ SCALP ENTER: {side} {symbol} @ ${entry_price:,.4f} "
            f"size={size} lev={leverage}x orderId={order_id} "
            f"tp=${trade.target_price or 'none'} sl=${trade.sl_price or 'none'} "
            f"tp_usd=${trade.target_usd or 'none'} sl_usd=${trade.sl_usd or 'none'}"
            f"{exec_tail}",
        )

        if not self._running:
            self.start()
        else:
            try:
                asyncio.get_running_loop().create_task(self._ensure_ws_feed({symbol}))
            except RuntimeError:
                pass
        self._schedule_update(force=True)

        return {"status": "ok", "trade_id": self._trade_counter, "trade": trade.to_dict()}

    async def exit_trade(self, trade_id: int, reason: str = "manual") -> Dict[str, Any]:
        """Manually exit an open scalp trade."""
        trade = self.open_trades.get(trade_id)
        if not trade:
            return {"status": "error", "message": f"Trade {trade_id} not found or already closed"}
        await self._close_trade(trade, reason)
        return {"status": "ok", "trade": trade.to_dict()}

    async def update_trade_targets(self, trade_id: int, **kwargs) -> Dict[str, Any]:
        """Update TP/SL for an open trade."""
        trade = self.open_trades.get(trade_id)
        if not trade:
            return {"status": "error", "message": f"Trade {trade_id} not found"}
        for attr in ("target_price", "sl_price", "target_usd", "sl_usd"):
            if attr in kwargs and kwargs[attr] is not None:
                setattr(trade, attr, kwargs[attr])
        self._log("info", f"🎯 Trade {trade_id} targets updated: {kwargs}")
        self._schedule_update(force=True)
        return {"status": "ok", "trade": trade.to_dict()}

    def get_status(self) -> dict:
        today_utc = _now_utc().date()

        def _exit_date(t: dict):
            try:
                et = t.get("exit_time")
                if et:
                    return datetime.fromisoformat(str(et).split(".")[0]).date()
            except Exception:
                pass
            return None

        today_closed = [t for t in self.closed_trades if _exit_date(t) == today_utc]
        session_realized = round(sum(t.get("net_pnl", t.get("pnl", 0)) for t in today_closed), 2)
        session_fees = round(sum(t.get("fees", 0) for t in today_closed), 2)
        # Unrealized: gross P&L only (fees deducted at close time in net_pnl).
        # This matches what Active Positions displays so the user isn't confused.
        session_unrealized = round(
            sum(t._compute_pnl(t.current_price) for t in self.open_trades.values()),
            2,
        )
        tracked_symbols = {self._canonical_symbol(t.symbol) for t in self.open_trades.values()} | {
            self._canonical_symbol(p.symbol) for p in self.pending_entries.values()
        }
        latest_symbol = ""
        latest_ts = None
        for canonical in tracked_symbols or set(self._last_price_ts.keys()):
            ts = self._last_price_ts.get(canonical)
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts
                latest_symbol = canonical
        price_age_ms = None
        if latest_ts:
            price_age_ms = max(0, int((_now_utc() - latest_ts).total_seconds() * 1000))
        ws_status = self._ws_feed.get_status() if self._ws_feed else {}
        feed_metrics = {
            "ws_connected": bool(self._ws_feed and getattr(self._ws_feed, "connected", False)),
            "authenticated": bool(ws_status.get("authenticated", False)),
            "symbol": latest_symbol or None,
            "source": self._last_price_source.get(latest_symbol, "") if latest_symbol else "",
            "updated_at": str(latest_ts) if latest_ts else None,
            "age_ms": price_age_ms,
            "rest_fallbacks": self._rest_price_fetches,
            "messages_received": int(ws_status.get("messages_received", 0) or 0),
            "reconnect_count": int(ws_status.get("reconnect_count", 0) or 0),
            "last_error": str(ws_status.get("last_error", "") or ""),
            "subscribed_channels": list(ws_status.get("subscribed_channels") or []),
            "pending_auth_channels": list(ws_status.get("pending_auth_channels") or []),
        }

        return {
            "running": self._running,
            "in_trade": len(self.open_trades) > 0,
            "pending_entries": [p.to_dict() for p in self.pending_entries.values()],
            "open_trades": [t.to_dict() for t in self.open_trades.values()],
            "closed_trades": list(reversed(self.closed_trades[-50:])),
            "event_log": list(reversed(self.event_log[-100:])),
            # All-time realized net (after fees)
            "total_pnl": round(sum(t.get("net_pnl", t.get("pnl", 0)) for t in self.closed_trades), 2),
            # Today's session fields (used by Session P&L display)
            "session_realized_pnl": session_realized,
            "session_unrealized_pnl": session_unrealized,
            "session_fees": session_fees,
            "session_pnl": round(session_realized + session_unrealized, 2),
            "feed_metrics": feed_metrics,
            "execution_metrics": dict(self._last_execution),
        }

    # ── Internal monitoring ───────────────────────────────────────

    async def _monitor_loop(self):
        """Monitor open trades and trigger exits with WS-first pricing."""
        while self._running:
            try:
                trades = list(self.open_trades.items())
                pending_entries = list(self.pending_entries.items())
                if not trades and not pending_entries:
                    if self._ws_feed:
                        await self._stop_ws_feed()
                    await asyncio.sleep(1)
                    continue

                tracked_symbols = {trade.symbol for _, trade in trades} | {entry.symbol for _, entry in pending_entries}
                await self._ensure_ws_feed(tracked_symbols)

                for entry_id, pending in pending_entries:
                    price = await self._get_symbol_price(pending.symbol)
                    if price <= 0 or not pending.should_trigger(price):
                        continue
                    self._log(
                        "info",
                        f"🛡 Guardrail triggered for {pending.side} {pending.symbol} @ ${price:,.4f}",
                    )
                    self.pending_entries.pop(entry_id, None)
                    result = await self._open_trade(
                        symbol=pending.symbol,
                        side=pending.side,
                        size=pending.size,
                        leverage=pending.leverage,
                        target_price=pending.target_price,
                        sl_price=pending.sl_price,
                        target_pct=pending.target_pct,
                        sl_pct=pending.sl_pct,
                        target_usd=pending.target_usd,
                        sl_usd=pending.sl_usd,
                        mode=pending.mode,
                        guardrail_price=pending.guardrail_price,
                        market_price=price,
                    )
                    if result.get("status") != "ok":
                        self._log(
                            "error",
                            f"Guardrail entry failed for {pending.symbol}: {result.get('message', 'unknown error')}",
                        )

                price_map = await self._fetch_all_prices(trades)

                for tid, trade in trades:
                    price = price_map.get(trade.symbol, 0.0)
                    if price > 0:
                        if trade.prime_entry_price(price):
                            self._log("info", f"📌 Trade {tid} entry price set @ ${price:,.4f}")
                        trade.current_price = price
                        if not trade._post_entry_price_ready:
                            trade._post_entry_price_ready = True
                            self._log("info", f"📡 Trade {tid} fresh price synced @ ${price:,.4f}")

                    if not trade.can_evaluate_exit():
                        continue
                    reason = trade.check_exit(trade.current_price)
                    if reason:
                        await self._close_trade(trade, reason)
            except Exception as e:
                self._log("error", f"Monitor error: {e}")
            await asyncio.sleep(0.5)

    async def _get_symbol_price(self, symbol: str) -> float:
        canonical = self._canonical_symbol(symbol)
        ws_price = self._ws_prices.get(canonical, 0.0)
        if ws_price > 0:
            return ws_price
        try:
            ticker = await asyncio.to_thread(self.delta.get_ticker, symbol)
            price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
            if price > 0:
                self._rest_price_fetches += 1
                self._record_price(symbol, price, source="rest_quote")
            return price
        except Exception:
            return 0.0

    async def _fetch_all_prices(self, trades: list) -> dict:
        """Fetch mark prices for all unique symbols.
        Prefer WS prices; use REST bulk ticker only for symbols still missing."""
        now = _now_utc()
        symbols = list({trade.symbol for _, trade in trades})
        prefer_fresh_symbols = {trade.symbol for _, trade in trades if trade.should_prefer_fresh_rest_mark(now)}
        result = {}
        missing = []
        for sym in symbols:
            if sym in prefer_fresh_symbols:
                missing.append(sym)
                continue
            price = self._ws_prices.get(self._canonical_symbol(sym), 0.0)
            if price > 0:
                result[sym] = price
            else:
                missing.append(sym)
        if prefer_fresh_symbols:
            for sym in sorted(prefer_fresh_symbols):
                try:
                    ticker = await asyncio.to_thread(self.delta.get_ticker, sym)
                    p = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                    if p > 0:
                        self._rest_price_fetches += 1
                        self._record_price(sym, p, source="rest_quote")
                        result[sym] = p
                except Exception:
                    pass
            missing = [sym for sym in missing if sym not in result]
        if not missing:
            return result
        try:
            self._rest_price_fetches += len(missing)
            tickers = await asyncio.to_thread(self.delta.get_tickers_bulk)
            ticker_map: Dict[str, float] = {}
            for t in tickers:
                sym = t.get("symbol", "")
                price = float(t.get("mark_price", 0) or t.get("close", 0) or 0)
                if price > 0:
                    ticker_map[sym] = price
                    # Also store USDT-normalised name
                    canonical = self.delta.from_delta_symbol(sym)
                    ticker_map[canonical] = price

            for sym in missing:
                if sym in ticker_map:
                    self._record_price(sym, ticker_map[sym], source="rest_bulk")
                    result[sym] = ticker_map[sym]
                else:
                    ds = self.delta.to_delta_symbol(sym)
                    if ds in ticker_map:
                        self._record_price(sym, ticker_map[ds], source="rest_bulk")
                        result[sym] = ticker_map[ds]
        except Exception as e:
            self._log("error", f"Bulk ticker fetch failed: {e}")
            # Fallback: individual fetch per symbol
            for sym in missing:
                try:
                    ticker = await asyncio.to_thread(self.delta.get_ticker, sym)
                    p = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                    if p > 0:
                        self._rest_price_fetches += 1
                        self._record_price(sym, p, source="rest_quote")
                        result[sym] = p
                except Exception:
                    pass
        return result

    async def _close_trade(self, trade: ScalpTrade, reason: str):
        """Place exit order (or simulate) and move trade to closed_trades."""
        exit_order_id = ""
        result: Dict[str, Any] = {}
        if trade.mode == "paper":
            exit_order_id = "PAPER"
        else:
            try:
                close_side = "sell" if trade.side == "LONG" else "buy"
                result = await self._place_verified_order(
                    product_id=trade.product_id,
                    size=trade.size,
                    side=close_side,
                    order_type="market_order",
                    leverage=trade.leverage,
                    reduce_only=True,
                )
                if isinstance(result, dict) and (result.get("error") or not result.get("verified")):
                    # Broker rejected the exit — leave trade open so monitor retries.
                    # Increment attempt counter; after 3 failures alert and halt engine.
                    trade._exit_attempts = getattr(trade, "_exit_attempts", 0) + 1
                    self._log(
                        "error",
                        f"Exit order REJECTED for trade {trade.trade_id} "
                        f"(attempt {trade._exit_attempts}): "
                        f"{result.get('error') or 'exit order could not be verified'}",
                    )
                    if trade._exit_attempts >= 3:
                        self._log(
                            "error",
                            f"CRITICAL: Exit failed 3 times for trade {trade.trade_id} "
                            f"({trade.side} {trade.symbol}). MANUAL INTERVENTION REQUIRED. "
                            f"Engine stopping to prevent further exposure.",
                        )
                        self.stop()
                    return  # keep in open_trades — monitor will retry next cycle
                exit_order_id = str(result.get("id", "closed"))
                trade.current_price = self._extract_order_price(result, trade.current_price)
            except Exception as e:
                trade._exit_attempts = getattr(trade, "_exit_attempts", 0) + 1
                self._log(
                    "error",
                    f"Exit order FAILED for trade {trade.trade_id} (attempt {trade._exit_attempts}): {e}",
                )
                if trade._exit_attempts >= 3:
                    self._log(
                        "error",
                        f"CRITICAL: Exit failed 3 times for trade {trade.trade_id} "
                        f"({trade.side} {trade.symbol}). MANUAL INTERVENTION REQUIRED. "
                        f"Engine stopping to prevent further exposure.",
                    )
                    self.stop()
                return  # keep in open_trades — monitor will retry next cycle

        exit_price = trade.current_price
        pnl = trade._compute_pnl(exit_price)

        trade.exit_time = _now_utc()
        trade.exit_price = exit_price
        trade.exit_reason = reason
        trade.exit_order_id = exit_order_id
        trade.pnl = round(pnl, 2)
        trade.status = "closed"
        trade.exit_latency_ms = round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1)

        closed_dict = trade.to_dict()
        self.closed_trades.append(closed_dict)
        del self.open_trades[trade.trade_id]
        self._last_execution = {
            "phase": "exit",
            "symbol": trade.symbol,
            "side": trade.side,
            "mode": trade.mode,
            "verified": bool(result.get("verified", trade.mode == "paper")),
            "latency_ms": round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1),
            "ack_ms": round(_coerce_float(result.get("order_ack_ms"), 0.0), 1),
            "verified_at_attempt": int(result.get("verified_at_attempt", 0) or 0),
            "updated_at": str(_now_utc()),
        }

        # Persist to disk via callback (handles both auto and manual exits uniformly)
        if self._on_trade_closed:
            try:
                self._on_trade_closed(closed_dict)
            except Exception as _e:
                self._log("error", f"Trade persistence callback failed: {_e}")

        pnl_sign = "+" if pnl >= 0 else ""
        exec_tail = ""
        if result.get("broker_latency_ms"):
            exec_tail = (
                f" verify={_coerce_float(result.get('broker_latency_ms'), 0.0):,.1f}ms"
                f" ack={_coerce_float(result.get('order_ack_ms'), 0.0):,.1f}ms"
            )
        self._log(
            "exit" if pnl >= 0 else "stop",
            f"{'✅' if pnl >= 0 else '🛑'} SCALP EXIT [{reason}]: "
            f"{trade.side} {trade.symbol} "
            f"entry=${trade.entry_price:,.4f} exit=${exit_price:,.4f} "
            f"PnL={pnl_sign}${pnl:.2f}{exec_tail}",
        )
        self._schedule_update(force=True)

    def _log(self, level: str, msg: str):
        now = _now_utc()
        entry = {
            "time": now.strftime("%H:%M:%S"),
            "ts": str(now),
            "level": level,
            "msg": msg,
        }
        self.event_log.append(entry)
        if self._on_event:
            try:
                self._on_event(entry)
            except Exception:
                pass
        print(f"[SCALP][{level.upper()}] {msg}")
