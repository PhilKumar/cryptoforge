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


def _parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value).replace("Z", ""))
    except (TypeError, ValueError):
        return None


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
        qty_mode: str = "usdt",
        qty_value: float = 0.0,
        base_qty: float = 0.0,
        margin_usd: float = 0.0,
        # Exit rules (at least one should be set)
        target_price: float = 0.0,  # absolute price to take profit
        sl_price: float = 0.0,  # absolute SL price
        target_pct: float = 0.0,  # leveraged % gain target
        sl_pct: float = 0.0,  # leveraged % loss SL
        target_usd: float = 0.0,  # fixed $ profit target
        sl_usd: float = 0.0,  # fixed $ loss SL
        entry_limit_price: float = 0.0,
        entry_stop_price: float = 0.0,
        order_id: str = "",
        entry_time: Optional[datetime] = None,
        mode: str = "live",
        guardrail_price: float = 0.0,
        execution_metrics: Optional[Dict[str, Any]] = None,
    ):
        self.trade_id = trade_id
        self.symbol = symbol
        self.side = side
        self.product_id = product_id
        self.size = size
        self.entry_price = entry_price
        self.current_price = entry_price
        self.leverage = leverage
        self.qty_mode = str(qty_mode or "usdt").lower()
        self.qty_value = _coerce_float(qty_value, 0.0)
        self.base_qty = _coerce_float(base_qty, 0.0)
        self.margin_usd = _coerce_float(margin_usd, 0.0)
        self.order_id = order_id
        self.entry_time = entry_time or _now_utc()
        self.mode = mode
        self.entry_limit_price = _coerce_float(entry_limit_price, 0.0)
        self.entry_stop_price = _coerce_float(entry_stop_price or guardrail_price, 0.0)
        self.guardrail_price = self.entry_stop_price
        self._exit_guard_until = self.entry_time + timedelta(seconds=2 if mode == "live" else 1)
        self._prefer_fresh_rest_mark_until = self.entry_time + timedelta(seconds=5 if mode == "live" else 2)
        self._post_entry_price_ready = False
        self.last_price_source = "entry"
        self.last_price_update = self.entry_time
        self.entry_latency_ms: float = 0.0
        self.exit_latency_ms: float = 0.0
        self.execution_metrics = dict(execution_metrics or {})

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
        self._refresh_derived_quantities()

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

    def _refresh_derived_quantities(self) -> None:
        self.margin_usd = round(self.size / max(self.leverage, 1), 4)
        if self.entry_price > 0:
            self.base_qty = round(self.size / self.entry_price, 8)
        if self.qty_mode == "base":
            if self.qty_value <= 0:
                self.qty_value = self.base_qty
        elif self.qty_value <= 0:
            self.qty_value = self.margin_usd

    def prime_entry_price(self, price: float) -> bool:
        price = _coerce_float(price, 0.0)
        if price <= 0 or self.entry_price > 0:
            return False
        self.entry_price = price
        self._refresh_derived_quantities()
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
            "qty_mode": self.qty_mode,
            "qty_value": self.qty_value,
            "base_qty": self.base_qty,
            "margin_usd": self.margin_usd,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "target_price": self.target_price,
            "sl_price": self.sl_price,
            "target_pct": self.target_pct,
            "sl_pct": self.sl_pct,
            "target_usd": self.target_usd,
            "sl_usd": self.sl_usd,
            "entry_limit_price": self.entry_limit_price,
            "entry_stop_price": self.entry_stop_price,
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
            "qty_usdt": round(self.margin_usd, 4),
            "mark_price": self.current_price,
            "price_source": self.last_price_source,
            "price_updated_at": str(self.last_price_update) if self.last_price_update else None,
            "price_age_ms": price_age_ms,
            "entry_latency_ms": self.entry_latency_ms,
            "exit_latency_ms": self.exit_latency_ms,
            "status": self.status,
            "mode": self.mode,
            "execution_metrics": dict(self.execution_metrics or {}),
        }

    @classmethod
    def from_dict(cls, data: dict):
        trade = cls(
            trade_id=int(data.get("trade_id", 0) or 0),
            symbol=str(data.get("symbol", "") or ""),
            side=str(data.get("side", "") or ""),
            product_id=int(data.get("product_id", 0) or 0),
            size=int(data.get("size", 0) or 0),
            entry_price=_coerce_float(data.get("entry_price"), 0.0),
            leverage=int(data.get("leverage", 1) or 1),
            qty_mode=str(data.get("qty_mode", "usdt") or "usdt"),
            qty_value=_coerce_float(data.get("qty_value"), 0.0),
            base_qty=_coerce_float(data.get("base_qty"), 0.0),
            margin_usd=_coerce_float(data.get("margin_usd", data.get("qty_usdt")), 0.0),
            target_price=_coerce_float(data.get("target_price"), 0.0),
            sl_price=_coerce_float(data.get("sl_price"), 0.0),
            target_pct=_coerce_float(data.get("target_pct"), 0.0),
            sl_pct=_coerce_float(data.get("sl_pct"), 0.0),
            target_usd=_coerce_float(data.get("target_usd"), 0.0),
            sl_usd=_coerce_float(data.get("sl_usd"), 0.0),
            entry_limit_price=_coerce_float(data.get("entry_limit_price"), 0.0),
            entry_stop_price=_coerce_float(data.get("entry_stop_price", data.get("guardrail_price")), 0.0),
            order_id=str(data.get("order_id", "") or ""),
            entry_time=_parse_dt(data.get("entry_time")),
            mode=str(data.get("mode", "paper") or "paper"),
            guardrail_price=_coerce_float(data.get("guardrail_price"), 0.0),
            execution_metrics=data.get("execution_metrics") or {},
        )
        trade.current_price = _coerce_float(data.get("current_price", data.get("mark_price")), trade.entry_price)
        trade.last_price_source = str(data.get("price_source", "restored") or "restored")
        trade.last_price_update = _parse_dt(data.get("price_updated_at")) or trade.entry_time
        trade.entry_latency_ms = _coerce_float(data.get("entry_latency_ms"), 0.0)
        trade.exit_latency_ms = _coerce_float(data.get("exit_latency_ms"), 0.0)
        trade.exit_price = _coerce_float(data.get("exit_price"), 0.0)
        trade.exit_time = _parse_dt(data.get("exit_time"))
        trade.exit_reason = str(data.get("exit_reason", "") or "")
        trade.exit_order_id = str(data.get("exit_order_id", "") or "")
        trade.pnl = _coerce_float(data.get("pnl"), 0.0)
        trade.status = str(data.get("status", "open") or "open")
        trade._post_entry_price_ready = trade.current_price > 0
        now = _now_utc()
        if trade._exit_guard_until > now:
            trade._exit_guard_until = now
        trade._prefer_fresh_rest_mark_until = now
        trade._refresh_derived_quantities()
        return trade


class PendingScalpEntry:
    """Represents an armed scalp entry waiting for a price trigger."""

    def __init__(
        self,
        entry_id: int,
        symbol: str,
        side: str,
        size: int,
        leverage: int,
        qty_mode: str = "usdt",
        qty_value: float = 0.0,
        base_qty: float = 0.0,
        margin_usd: float = 0.0,
        entry_limit_price: float = 0.0,
        entry_stop_price: float = 0.0,
        guardrail_price: float = 0.0,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        mode: str = "live",
        execution_metrics: Optional[Dict[str, Any]] = None,
    ):
        self.entry_id = entry_id
        self.symbol = symbol
        self.side = side
        self.size = size
        self.leverage = leverage
        self.qty_mode = str(qty_mode or "usdt").lower()
        self.qty_value = _coerce_float(qty_value, 0.0)
        self.base_qty = _coerce_float(base_qty, 0.0)
        self.margin_usd = _coerce_float(margin_usd, 0.0)
        self.entry_limit_price = _coerce_float(entry_limit_price, 0.0)
        self.entry_stop_price = _coerce_float(entry_stop_price or guardrail_price, 0.0)
        self.guardrail_price = self.entry_stop_price
        self.target_price = target_price
        self.sl_price = sl_price
        self.target_pct = target_pct
        self.sl_pct = sl_pct
        self.target_usd = target_usd
        self.sl_usd = sl_usd
        self.mode = mode
        self.created_at = _now_utc()
        self.execution_metrics = dict(execution_metrics or {})

    def should_trigger(self, price: float) -> bool:
        if price <= 0:
            return False
        stop_hit = False
        limit_hit = False
        if self.entry_stop_price > 0:
            stop_hit = price >= self.entry_stop_price if self.side == "LONG" else price <= self.entry_stop_price
        if self.entry_limit_price > 0:
            limit_hit = price <= self.entry_limit_price if self.side == "LONG" else price >= self.entry_limit_price
        return stop_hit or limit_hit

    def trigger_summary(self) -> str:
        bits = []
        if self.entry_stop_price > 0:
            bits.append(("Stop ≥ " if self.side == "LONG" else "Stop ≤ ") + f"${self.entry_stop_price:,.4f}")
        if self.entry_limit_price > 0:
            bits.append(("Limit ≤ " if self.side == "LONG" else "Limit ≥ ") + f"${self.entry_limit_price:,.4f}")
        return " • ".join(bits) or "Market"

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "symbol": self.symbol,
            "side": self.side,
            "size": self.size,
            "leverage": self.leverage,
            "qty_mode": self.qty_mode,
            "qty_value": self.qty_value,
            "base_qty": self.base_qty,
            "margin_usd": self.margin_usd or round(self.size / max(self.leverage, 1), 4),
            "entry_limit_price": self.entry_limit_price,
            "entry_stop_price": self.entry_stop_price,
            "guardrail_price": self.guardrail_price,
            "target_price": self.target_price,
            "sl_price": self.sl_price,
            "target_pct": self.target_pct,
            "sl_pct": self.sl_pct,
            "target_usd": self.target_usd,
            "sl_usd": self.sl_usd,
            "mode": self.mode,
            "status": "pending",
            "created_at": str(self.created_at),
            "qty_usdt": self.margin_usd or round(self.size / max(self.leverage, 1), 4),
            "trigger_summary": self.trigger_summary(),
            "execution_metrics": dict(self.execution_metrics or {}),
        }

    @classmethod
    def from_dict(cls, data: dict):
        pending = cls(
            entry_id=int(data.get("entry_id", 0) or 0),
            symbol=str(data.get("symbol", "") or ""),
            side=str(data.get("side", "") or ""),
            size=int(data.get("size", 0) or 0),
            leverage=int(data.get("leverage", 1) or 1),
            qty_mode=str(data.get("qty_mode", "usdt") or "usdt"),
            qty_value=_coerce_float(data.get("qty_value"), 0.0),
            base_qty=_coerce_float(data.get("base_qty"), 0.0),
            margin_usd=_coerce_float(data.get("margin_usd", data.get("qty_usdt")), 0.0),
            entry_limit_price=_coerce_float(data.get("entry_limit_price"), 0.0),
            entry_stop_price=_coerce_float(data.get("entry_stop_price", data.get("guardrail_price")), 0.0),
            guardrail_price=_coerce_float(data.get("guardrail_price"), 0.0),
            target_price=_coerce_float(data.get("target_price"), 0.0),
            sl_price=_coerce_float(data.get("sl_price"), 0.0),
            target_pct=_coerce_float(data.get("target_pct"), 0.0),
            sl_pct=_coerce_float(data.get("sl_pct"), 0.0),
            target_usd=_coerce_float(data.get("target_usd"), 0.0),
            sl_usd=_coerce_float(data.get("sl_usd"), 0.0),
            mode=str(data.get("mode", "paper") or "paper"),
            execution_metrics=data.get("execution_metrics") or {},
        )
        pending.created_at = _parse_dt(data.get("created_at")) or pending.created_at
        return pending


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
        self._watch_symbols: set[str] = set()
        self._last_price_ts: Dict[str, datetime] = {}
        self._last_price_source: Dict[str, str] = {}
        self._last_watch_refresh: Dict[str, datetime] = {}
        self._rest_price_fetches: int = 0
        self._last_execution: Dict[str, Any] = {}
        self._update_task: Optional[asyncio.Task] = None
        self._ws_shutdown_task: Optional[asyncio.Task] = None
        self._ws_ensure_tasks: set[asyncio.Task] = set()
        self._ws_ensure_cancel_tasks: list[asyncio.Task] = []
        self._last_update_push: float = 0.0
        self._update_interval_sec: float = 0.25
        self._trade_action_locks: Dict[int, asyncio.Lock] = {}

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
        ensure_tasks = [task for task in self._ws_ensure_tasks if task and not task.done()]
        self._ws_ensure_tasks.clear()
        self._ws_ensure_cancel_tasks = ensure_tasks
        for task in ensure_tasks:
            task.cancel()
        shutdown_task = None
        if self._ws_feed:
            try:
                loop = asyncio.get_running_loop()
                self._ws_shutdown_task = loop.create_task(self._stop_ws_feed())
                shutdown_task = self._ws_shutdown_task
            except RuntimeError:
                self._ws_feed = None
                self._ws_prices.clear()
                self._ws_subscribed_symbols.clear()
        return shutdown_task

    async def shutdown(self):
        shutdown_task = self.stop()
        ensure_tasks = list(self._ws_ensure_cancel_tasks)
        self._ws_ensure_cancel_tasks = []
        for task in ensure_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if shutdown_task:
            try:
                await shutdown_task
            except asyncio.CancelledError:
                pass
            finally:
                if self._ws_shutdown_task is shutdown_task:
                    self._ws_shutdown_task = None

    def watch_symbol(self, symbol: str) -> None:
        canonical = self._canonical_symbol(symbol)
        self._watch_symbols = {canonical} if canonical else set()
        if not canonical:
            return
        if not self._running:
            self.start()
            return
        self._queue_ws_feed_ensure({canonical})

    def _queue_ws_feed_ensure(self, symbols: set[str]) -> None:
        pending = {self._canonical_symbol(sym) for sym in symbols if sym}
        if not _HAS_WS or not pending or not self._running:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _runner() -> None:
            try:
                await self._ensure_ws_feed(pending)
            except asyncio.CancelledError:
                return
            except Exception:
                pass

        task = loop.create_task(_runner())
        self._ws_ensure_tasks.add(task)
        task.add_done_callback(lambda done: self._ws_ensure_tasks.discard(done))

    @staticmethod
    def _entry_freshness_thresholds(source: str) -> tuple[int, int]:
        raw = str(source or "").lower()
        if raw == "ws":
            return (2500, 8000)
        if raw in {"rest_quote", "rest_bulk"}:
            return (4500, 9000)
        if raw in {"broker_fill", "entry_snapshot"}:
            return (3000, 6000)
        return (4000, 8000)

    def _symbol_feed_guard(self, symbol: str) -> dict:
        canonical = self._canonical_symbol(symbol)
        source = str(self._last_price_source.get(canonical, "") or "")
        updated_at = self._last_price_ts.get(canonical)
        price = _coerce_float(self._last_prices.get(canonical) or self._ws_prices.get(canonical), 0.0)
        age_ms = None
        if updated_at:
            age_ms = max(0, int((_now_utc() - updated_at).total_seconds() * 1000))

        if price <= 0 or updated_at is None:
            return {
                "symbol": canonical or symbol or "",
                "price": 0.0,
                "source": source,
                "updated_at": None,
                "age_ms": None,
                "state": "waiting",
                "paper_allowed": False,
                "live_allowed": False,
                "reason": f"Awaiting first market tick for {canonical or symbol or 'selected symbol'}",
            }

        live_limit_ms, paper_limit_ms = self._entry_freshness_thresholds(source)
        if age_ms <= live_limit_ms:
            state = "fresh"
            paper_allowed = True
            live_allowed = True
            reason = f"Fresh {source or 'market'} quote ready for {canonical}"
        elif age_ms <= paper_limit_ms:
            state = "degraded"
            paper_allowed = True
            live_allowed = False
            reason = f"Feed is degraded for {canonical}. Paper entry is allowed, live entry is blocked until a fresh tick arrives."
        else:
            state = "stale"
            paper_allowed = False
            live_allowed = False
            reason = f"Feed is stale for {canonical}. Refresh or wait for a fresh market tick before entering."

        return {
            "symbol": canonical or symbol or "",
            "price": price,
            "source": source,
            "updated_at": str(updated_at) if updated_at else None,
            "age_ms": age_ms,
            "state": state,
            "paper_allowed": paper_allowed,
            "live_allowed": live_allowed,
            "reason": reason,
        }

    def _remember_execution(
        self,
        *,
        phase: str,
        symbol: str,
        side: str,
        mode: str,
        verified: bool,
        result: Optional[dict] = None,
        error: str = "",
        trade_id: int = 0,
        requested_size: float = 0.0,
        requested_qty_value: float = 0.0,
        note: str = "",
        lifecycle: str = "",
        fill_status: str = "",
    ) -> None:
        result = result or {}
        phase_value = str(phase or "").strip().lower()
        default_lifecycle = str(lifecycle or "").strip()
        if not default_lifecycle:
            if verified:
                default_lifecycle = "filled"
            elif phase_value.endswith("_reject"):
                default_lifecycle = "rejected"
            elif phase_value.endswith("_error"):
                default_lifecycle = "error"
            elif phase_value == "targets":
                default_lifecycle = "updated"
            elif phase_value in {"entry", "exit", "scale_in"}:
                default_lifecycle = "submitted"
            else:
                default_lifecycle = "pending"
        lifecycle_value = str(result.get("order_lifecycle") or default_lifecycle or "").strip()
        fill_status_value = str(result.get("fill_status") or fill_status or lifecycle_value or "").strip()
        exchange_state_value = str(
            result.get("exchange_state") or result.get("state") or fill_status_value or ""
        ).strip()
        verification_state_value = str(result.get("verification_state") or lifecycle_value or "").strip()
        verification_summary_value = str(
            result.get("verification_summary") or error or result.get("error") or result.get("note") or note or ""
        ).strip()
        self._last_execution = {
            "phase": phase,
            "symbol": symbol,
            "side": side,
            "mode": mode,
            "verified": bool(verified),
            "trade_id": int(trade_id or 0),
            "requested_size": _coerce_float(requested_size, 0.0),
            "requested_qty_value": _coerce_float(requested_qty_value, 0.0),
            "latency_ms": round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1),
            "ack_ms": round(_coerce_float(result.get("order_ack_ms"), 0.0), 1),
            "verified_at_attempt": int(result.get("verified_at_attempt", 0) or 0),
            "fill_status": fill_status_value,
            "order_lifecycle": lifecycle_value,
            "exchange_state": exchange_state_value,
            "verification_state": verification_state_value,
            "verification_summary": verification_summary_value,
            "position_size": _coerce_float(result.get("position_size"), 0.0),
            "order_id": str(result.get("id") or result.get("order_id") or ""),
            "error": str(error or result.get("error") or ""),
            "note": str(note or result.get("note") or ""),
            "updated_at": str(_now_utc()),
        }
        trade_key = int(trade_id or 0)
        if trade_key > 0:
            execution_snapshot = dict(self._last_execution)
            if trade_key in self.open_trades:
                self.open_trades[trade_key].execution_metrics = execution_snapshot
            if trade_key in self.pending_entries:
                self.pending_entries[trade_key].execution_metrics = execution_snapshot

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

    def _resolve_contract_size(self, *, qty_mode: str, qty_value: float, price: float, leverage: int) -> int:
        mode = str(qty_mode or "usdt").lower()
        value = _coerce_float(qty_value, 0.0)
        lev = max(int(leverage or 1), 1)
        if value <= 0:
            return 0
        if mode == "base":
            if price <= 0:
                return 0
            return max(1, int(round(value * price)))
        return max(1, int(round(value * lev)))

    def _trade_action_lock(self, trade_id: int) -> asyncio.Lock:
        trade_key = int(trade_id or 0)
        lock = self._trade_action_locks.get(trade_key)
        if lock is None:
            lock = asyncio.Lock()
            self._trade_action_locks[trade_key] = lock
        return lock

    async def _run_trade_action(self, trade_id: int, action: str, runner) -> Dict[str, Any]:
        lock = self._trade_action_lock(trade_id)
        if lock.locked():
            return {
                "status": "error",
                "action": action,
                "trade_id": int(trade_id or 0),
                "message": f"Trade {trade_id} already has an action in progress",
                "error_code": "action_in_progress",
                "retryable": True,
            }
        async with lock:
            return await runner()

    async def _refresh_watch_prices(self, symbols: set[str]) -> None:
        now = _now_utc()
        for canonical in sorted({self._canonical_symbol(sym) for sym in symbols if sym}):
            last_seen = self._last_price_ts.get(canonical)
            age_ms = None
            if last_seen:
                age_ms = max(0, int((now - last_seen).total_seconds() * 1000))
            last_refresh = self._last_watch_refresh.get(canonical)
            if last_refresh and (now - last_refresh).total_seconds() < 1.5:
                continue
            if age_ms is not None and age_ms <= 2500:
                continue
            self._last_watch_refresh[canonical] = now
            await self._get_symbol_price(canonical)

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
        if not self._running or not _HAS_WS or not symbols:
            return
        if self._ws_feed is None:
            self._ws_feed = DeltaWSFeed()
            self._ws_feed.on_ticker = self._handle_ticker
            self._ws_feed.on_connect = lambda: self._schedule_update(force=True)
            self._ws_feed.on_disconnect = lambda reason: self._schedule_update(force=True)
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
        self._ws_shutdown_task = None

    async def enter_trade(
        self,
        symbol: str,
        side: str,  # LONG or SHORT
        size: int = 0,  # contract units
        leverage: int = 10,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        guardrail_price: float = 0.0,
        mode: str = "live",
        qty_mode: str = "usdt",
        qty_value: float = 0.0,
        entry_limit_price: float = 0.0,
        entry_stop_price: float = 0.0,
    ) -> Dict[str, Any]:
        """Place immediately, or arm a pending stop/limit-triggered scalp entry."""

        qty_mode = "base" if str(qty_mode or "").lower() in {"base", "qty", "coin"} else "usdt"
        entry_stop_price = _coerce_float(entry_stop_price or guardrail_price, 0.0)
        entry_limit_price = _coerce_float(entry_limit_price, 0.0)
        qty_value = _coerce_float(qty_value, 0.0)
        market_price = self._cached_price(symbol)
        needs_price = (
            qty_mode == "base" or mode != "paper" or entry_stop_price > 0 or entry_limit_price > 0 or size <= 0
        )
        if market_price <= 0 and needs_price:
            try:
                ticker = await asyncio.to_thread(self.delta.get_ticker, symbol)
                market_price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                if market_price > 0:
                    self._rest_price_fetches += 1
                    self._record_price(symbol, market_price, source="rest_quote")
            except Exception:
                pass

        if qty_value <= 0 and size > 0 and qty_mode != "base":
            qty_value = round(size / max(leverage, 1), 4)

        has_price_trigger = entry_stop_price > 0 or entry_limit_price > 0
        if has_price_trigger:
            should_enter_now = False
            if market_price > 0:
                if side == "LONG":
                    if entry_stop_price > 0 and market_price >= entry_stop_price:
                        should_enter_now = True
                    if entry_limit_price > 0 and market_price <= entry_limit_price:
                        should_enter_now = True
                else:
                    if entry_stop_price > 0 and market_price <= entry_stop_price:
                        should_enter_now = True
                    if entry_limit_price > 0 and market_price >= entry_limit_price:
                        should_enter_now = True
            if not should_enter_now:
                self._trade_counter += 1
                size_estimate = size
                if size_estimate <= 0 and market_price > 0:
                    size_estimate = self._resolve_contract_size(
                        qty_mode=qty_mode,
                        qty_value=qty_value,
                        price=market_price,
                        leverage=leverage,
                    )
                margin_usd = (
                    qty_value
                    if qty_mode != "base"
                    else round(size_estimate / max(leverage, 1), 4)
                    if size_estimate > 0
                    else 0.0
                )
                base_qty = (
                    qty_value
                    if qty_mode == "base"
                    else round(size_estimate / market_price, 8)
                    if size_estimate > 0 and market_price > 0
                    else 0.0
                )
                pending = PendingScalpEntry(
                    entry_id=self._trade_counter,
                    symbol=symbol,
                    side=side,
                    size=size_estimate,
                    leverage=leverage,
                    qty_mode=qty_mode,
                    qty_value=qty_value,
                    base_qty=base_qty,
                    margin_usd=margin_usd,
                    entry_limit_price=entry_limit_price,
                    entry_stop_price=entry_stop_price,
                    target_price=target_price,
                    sl_price=sl_price,
                    target_pct=target_pct,
                    sl_pct=sl_pct,
                    target_usd=target_usd,
                    sl_usd=sl_usd,
                    mode=mode,
                )
                self.pending_entries[pending.entry_id] = pending
                self._remember_execution(
                    phase="entry",
                    symbol=symbol,
                    side=side,
                    mode=mode,
                    verified=False,
                    trade_id=pending.entry_id,
                    requested_size=size_estimate,
                    requested_qty_value=qty_value,
                    note=pending.trigger_summary(),
                    lifecycle="armed",
                    fill_status="armed",
                )
                self._log("info", f"⏳ Entry armed for {side} {symbol}: {pending.trigger_summary()}")
                self._schedule_update(force=True)
                if not self._running:
                    self.start()
                else:
                    try:
                        self._queue_ws_feed_ensure({symbol})
                    except RuntimeError:
                        pass
                return {
                    "status": "pending",
                    "entry_id": pending.entry_id,
                    "message": pending.trigger_summary(),
                    "pending_entry": pending.to_dict(),
                }
            self._log(
                "info", f"⏩ Entry trigger already satisfied for {side} {symbol} @ ${market_price:,.4f} — entering now."
            )

        if size <= 0:
            size = self._resolve_contract_size(
                qty_mode=qty_mode, qty_value=qty_value, price=market_price, leverage=leverage
            )
        if size <= 0:
            return {
                "status": "error",
                "message": f"Unable to resolve order size for {symbol}. Wait for a live price and try again.",
            }

        return await self._open_trade(
            symbol=symbol,
            side=side,
            size=size,
            leverage=leverage,
            qty_mode=qty_mode,
            qty_value=qty_value,
            target_price=target_price,
            sl_price=sl_price,
            target_pct=target_pct,
            sl_pct=sl_pct,
            target_usd=target_usd,
            sl_usd=sl_usd,
            mode=mode,
            guardrail_price=entry_stop_price,
            entry_limit_price=entry_limit_price,
            entry_stop_price=entry_stop_price,
            market_price=market_price,
        )

    async def _open_trade(
        self,
        *,
        symbol: str,
        side: str,
        size: int,
        leverage: int,
        qty_mode: str = "usdt",
        qty_value: float = 0.0,
        target_price: float = 0.0,
        sl_price: float = 0.0,
        target_pct: float = 0.0,
        sl_pct: float = 0.0,
        target_usd: float = 0.0,
        sl_usd: float = 0.0,
        mode: str = "live",
        guardrail_price: float = 0.0,
        entry_limit_price: float = 0.0,
        entry_stop_price: float = 0.0,
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

        if size <= 0:
            size = self._resolve_contract_size(
                qty_mode=qty_mode, qty_value=qty_value, price=entry_price, leverage=leverage
            )
        if size <= 0:
            return {"status": "error", "message": f"Unable to resolve order size for {symbol}"}

        order_id = ""
        result: Dict[str, Any] = {}
        if mode == "paper":
            order_id = "PAPER"
        else:
            self._remember_execution(
                phase="entry",
                symbol=symbol,
                side=side,
                mode=mode,
                verified=False,
                requested_size=size,
                requested_qty_value=qty_value,
                note="Broker order submitted",
                lifecycle="submitted",
                fill_status="submitted",
            )
            self._schedule_update(force=True)
            if not product_id:
                self._remember_execution(
                    phase="entry_error",
                    symbol=symbol,
                    side=side,
                    mode=mode,
                    verified=False,
                    error=f"Product not found for {symbol}",
                )
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
                    self._remember_execution(
                        phase="entry_reject",
                        symbol=symbol,
                        side=side,
                        mode=mode,
                        verified=False,
                        result=result,
                        requested_size=size,
                        requested_qty_value=qty_value,
                        note="Entry order rejected",
                    )
                    return {"status": "error", "message": result.get("error") or "entry order could not be verified"}
                order_id = str(result.get("id", "placed"))
                entry_price = self._extract_order_price(result, entry_price)
            except Exception as e:
                self._remember_execution(
                    phase="entry_error",
                    symbol=symbol,
                    side=side,
                    mode=mode,
                    verified=False,
                    error=str(e),
                    requested_size=size,
                    requested_qty_value=qty_value,
                    note="Entry order failed",
                )
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
            qty_mode=qty_mode,
            qty_value=qty_value,
            base_qty=qty_value if qty_mode == "base" else 0.0,
            margin_usd=qty_value if qty_mode != "base" and qty_value > 0 else round(size / max(leverage, 1), 4),
            target_price=target_price,
            sl_price=sl_price,
            target_pct=target_pct,
            sl_pct=sl_pct,
            target_usd=target_usd,
            sl_usd=sl_usd,
            entry_limit_price=entry_limit_price,
            entry_stop_price=entry_stop_price,
            order_id=order_id,
            mode=mode,
            guardrail_price=guardrail_price,
        )
        self.open_trades[self._trade_counter] = trade
        trade.entry_latency_ms = round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1)
        self._record_price(symbol, entry_price, source="broker_fill" if mode != "paper" else "entry_snapshot")
        self._remember_execution(
            phase="entry",
            symbol=symbol,
            side=side,
            mode=mode,
            verified=bool(result.get("verified", mode == "paper")),
            result=result,
            trade_id=self._trade_counter,
            requested_size=size,
            requested_qty_value=qty_value,
            note="Paper fill confirmed" if mode == "paper" else "Entry fill verified",
            lifecycle="filled" if mode == "paper" else "",
            fill_status="paper_fill" if mode == "paper" else "",
        )

        mode_label = "[PAPER] " if mode == "paper" else ""
        exec_tail = ""
        if result.get("broker_latency_ms"):
            exec_tail = (
                f" verify={_coerce_float(result.get('broker_latency_ms'), 0.0):,.1f}ms"
                f" ack={_coerce_float(result.get('order_ack_ms'), 0.0):,.1f}ms"
            )
        trigger_bits = []
        if entry_stop_price > 0:
            trigger_bits.append(f"stop=${entry_stop_price:,.4f}")
        if entry_limit_price > 0:
            trigger_bits.append(f"limit=${entry_limit_price:,.4f}")
        trigger_text = (" " + " ".join(trigger_bits)) if trigger_bits else ""
        self._log(
            "entry",
            f"{mode_label}✅ SCALP ENTER: {side} {symbol} @ ${entry_price:,.4f} "
            f"size={size} lev={leverage}x orderId={order_id} "
            f"tp=${trade.target_price or 'none'} sl=${trade.sl_price or 'none'} "
            f"tp_usd=${trade.target_usd or 'none'} sl_usd=${trade.sl_usd or 'none'}"
            f"{trigger_text}{exec_tail}",
        )

        if not self._running:
            self.start()
        else:
            try:
                self._queue_ws_feed_ensure({symbol})
            except RuntimeError:
                pass
        self._schedule_update(force=True)

        return {"status": "ok", "trade_id": self._trade_counter, "trade": trade.to_dict()}

    async def exit_trade(self, trade_id: int, reason: str = "manual") -> Dict[str, Any]:
        """Manually exit an open scalp trade."""
        trade = self.open_trades.get(trade_id)
        if not trade:
            return {
                "status": "error",
                "action": "exit",
                "trade_id": int(trade_id or 0),
                "message": f"Trade {trade_id} not found or already closed",
                "error_code": "trade_not_found",
            }
        result = await self._close_trade(trade, reason)
        if result.get("status") == "ok" and trade_id in self.open_trades:
            return {
                "status": "error",
                "action": "exit",
                "trade_id": int(trade_id or 0),
                "message": f"Trade {trade_id} exit was not confirmed",
                "trade": trade.to_dict(),
                "error_code": "exit_not_confirmed",
                "retryable": True,
            }
        return result

    async def update_trade_targets(self, trade_id: int, **kwargs) -> Dict[str, Any]:
        """Update TP/SL for an open trade."""

        async def _runner() -> Dict[str, Any]:
            trade = self.open_trades.get(trade_id)
            if not trade:
                return {
                    "status": "error",
                    "action": "targets",
                    "trade_id": int(trade_id or 0),
                    "message": f"Trade {trade_id} not found",
                    "error_code": "trade_not_found",
                }
            updates: Dict[str, float] = {}
            for attr in ("target_price", "sl_price", "target_usd", "sl_usd"):
                if attr in kwargs and kwargs[attr] is not None:
                    updates[attr] = _coerce_float(kwargs[attr], 0.0)
            if not updates:
                return {
                    "status": "error",
                    "action": "targets",
                    "trade_id": int(trade_id or 0),
                    "message": "No target fields provided",
                    "error_code": "no_target_fields",
                }
            changed = any(
                abs(_coerce_float(getattr(trade, attr), 0.0) - value) > 1e-9 for attr, value in updates.items()
            )
            if not changed:
                self._remember_execution(
                    phase="targets",
                    symbol=trade.symbol,
                    side=trade.side,
                    mode=trade.mode,
                    verified=True,
                    trade_id=trade_id,
                    requested_size=trade.size,
                    note="TP/SL already set",
                    lifecycle="updated",
                    fill_status="updated",
                )
                self._schedule_update(force=True)
                return {
                    "status": "ok",
                    "action": "noop",
                    "message": "TP/SL already set",
                    "trade": trade.to_dict(),
                }
            for attr, value in updates.items():
                setattr(trade, attr, value)
            self._remember_execution(
                phase="targets",
                symbol=trade.symbol,
                side=trade.side,
                mode=trade.mode,
                verified=True,
                trade_id=trade_id,
                requested_size=trade.size,
                note="TP/SL updated",
                lifecycle="updated",
                fill_status="updated",
            )
            self._log("info", f"🎯 Trade {trade_id} targets updated: {updates}")
            self._schedule_update(force=True)
            return {"status": "ok", "action": "targets_updated", "trade": trade.to_dict()}

        return await self._run_trade_action(trade_id, "targets", _runner)

    async def add_to_trade(self, trade_id: int, qty_mode: str = "base", qty_value: float = 0.0) -> Dict[str, Any]:
        async def _runner() -> Dict[str, Any]:
            trade = self.open_trades.get(trade_id)
            if not trade:
                return {
                    "status": "error",
                    "action": "add",
                    "trade_id": int(trade_id or 0),
                    "message": f"Trade {trade_id} not found",
                    "error_code": "trade_not_found",
                }

            normalized_qty_mode = "base" if str(qty_mode or "").lower() in {"base", "qty", "coin"} else "usdt"
            normalized_qty_value = _coerce_float(qty_value, 0.0)
            if normalized_qty_value <= 0:
                return {
                    "status": "error",
                    "action": "add",
                    "trade_id": int(trade_id or 0),
                    "message": "Add quantity must be greater than zero",
                    "error_code": "invalid_quantity",
                }

            fill_price = self._cached_price(trade.symbol) or trade.current_price or trade.entry_price
            if fill_price <= 0:
                try:
                    ticker = await asyncio.to_thread(self.delta.get_ticker, trade.symbol)
                    fill_price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                    if fill_price > 0:
                        self._rest_price_fetches += 1
                        self._record_price(trade.symbol, fill_price, source="rest_quote")
                except Exception:
                    pass
            if fill_price <= 0:
                return {
                    "status": "error",
                    "action": "add",
                    "trade_id": int(trade_id or 0),
                    "message": f"No live price available for {trade.symbol}",
                    "error_code": "no_live_price",
                    "retryable": True,
                }

            add_size = self._resolve_contract_size(
                qty_mode=normalized_qty_mode,
                qty_value=normalized_qty_value,
                price=fill_price,
                leverage=trade.leverage,
            )
            if add_size <= 0:
                return {
                    "status": "error",
                    "action": "add",
                    "trade_id": int(trade_id or 0),
                    "message": "Unable to resolve add quantity",
                    "error_code": "quantity_resolution_failed",
                }

            result: Dict[str, Any] = {}
            if trade.mode != "paper":
                self._remember_execution(
                    phase="scale_in",
                    symbol=trade.symbol,
                    side=trade.side,
                    mode=trade.mode,
                    verified=False,
                    trade_id=trade.trade_id,
                    requested_size=add_size,
                    requested_qty_value=normalized_qty_value,
                    note="Scale-in order submitted",
                    lifecycle="submitted",
                    fill_status="submitted",
                )
                self._schedule_update(force=True)
                try:
                    order_side = "buy" if trade.side == "LONG" else "sell"
                    result = await self._place_verified_order(
                        product_id=trade.product_id,
                        size=add_size,
                        side=order_side,
                        order_type="market_order",
                        leverage=trade.leverage,
                    )
                    if isinstance(result, dict) and (result.get("error") or not result.get("verified")):
                        self._remember_execution(
                            phase="scale_in_reject",
                            symbol=trade.symbol,
                            side=trade.side,
                            mode=trade.mode,
                            verified=False,
                            result=result,
                            trade_id=trade.trade_id,
                            requested_size=add_size,
                            requested_qty_value=normalized_qty_value,
                        )
                        return {
                            "status": "error",
                            "action": "add",
                            "trade_id": int(trade_id or 0),
                            "message": result.get("error") or "add order could not be verified",
                            "error_code": "broker_rejected",
                        }
                    fill_price = self._extract_order_price(result, fill_price)
                except Exception as e:
                    self._remember_execution(
                        phase="scale_in_error",
                        symbol=trade.symbol,
                        side=trade.side,
                        mode=trade.mode,
                        verified=False,
                        error=str(e),
                        trade_id=trade.trade_id,
                        requested_size=add_size,
                        requested_qty_value=normalized_qty_value,
                    )
                    return {
                        "status": "error",
                        "action": "add",
                        "trade_id": int(trade_id or 0),
                        "message": str(e),
                        "error_code": "broker_error",
                        "retryable": True,
                    }

            old_size = max(int(trade.size or 0), 0)
            total_size = old_size + add_size
            weighted_price = (
                fill_price if old_size <= 0 else ((trade.entry_price * old_size) + (fill_price * add_size)) / total_size
            )
            trade.size = total_size
            trade.entry_price = round(weighted_price, 6)
            trade.current_price = fill_price
            trade.last_price_source = "broker_fill" if trade.mode != "paper" else "entry_snapshot"
            trade.last_price_update = _now_utc()
            trade._refresh_derived_quantities()
            if trade.target_pct > 0:
                trade.target_price = 0.0
            if trade.sl_pct > 0:
                trade.sl_price = 0.0
            trade._apply_percentage_targets()

            self._record_price(
                trade.symbol, fill_price, source="broker_fill" if trade.mode != "paper" else "entry_snapshot"
            )
            self._remember_execution(
                phase="scale_in",
                symbol=trade.symbol,
                side=trade.side,
                mode=trade.mode,
                verified=bool(result.get("verified", trade.mode == "paper")),
                result=result,
                trade_id=trade.trade_id,
                requested_size=add_size,
                requested_qty_value=normalized_qty_value,
                note=f"Added {add_size} contracts",
                lifecycle="filled" if trade.mode == "paper" else "",
                fill_status="paper_fill" if trade.mode == "paper" else "",
            )
            self._log(
                "info",
                f"➕ SCALP ADD: {trade.side} {trade.symbol} add_size={add_size} fill=${fill_price:,.4f} total_size={trade.size}",
            )
            self._schedule_update(force=True)
            return {
                "status": "ok",
                "action": "quantity_added",
                "trade": trade.to_dict(),
                "added_size": add_size,
                "fill_price": fill_price,
            }

        return await self._run_trade_action(trade_id, "add", _runner)

    def get_status(self, symbol_hint: str = "") -> dict:
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
        tracked_symbols = (
            {self._canonical_symbol(t.symbol) for t in self.open_trades.values()}
            | {self._canonical_symbol(p.symbol) for p in self.pending_entries.values()}
            | set(self._watch_symbols)
        )
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
        watched_symbol = next(iter(sorted(self._watch_symbols)), "") if self._watch_symbols else ""
        preferred_symbol = self._canonical_symbol(symbol_hint) or watched_symbol or latest_symbol
        entry_guard = self._symbol_feed_guard(preferred_symbol)
        ws_status = self._ws_feed.get_status() if self._ws_feed else {}
        feed_metrics = {
            "ws_connected": bool(self._ws_feed and getattr(self._ws_feed, "connected", False)),
            "authenticated": bool(ws_status.get("authenticated", False)),
            "connection_state": str(ws_status.get("connection_state", "idle") or "idle"),
            "symbol": entry_guard.get("symbol") or latest_symbol or None,
            "source": entry_guard.get("source", ""),
            "updated_at": entry_guard.get("updated_at") or (str(latest_ts) if latest_ts else None),
            "age_ms": entry_guard.get("age_ms", price_age_ms),
            "state": entry_guard.get("state", "waiting"),
            "entry_block_reason": entry_guard.get("reason", ""),
            "rest_fallbacks": self._rest_price_fetches,
            "messages_received": int(ws_status.get("messages_received", 0) or 0),
            "reconnect_count": int(ws_status.get("reconnect_count", 0) or 0),
            "last_error": str(ws_status.get("last_error", "") or ""),
            "last_disconnect_reason": str(ws_status.get("last_disconnect_reason", "") or ""),
            "last_message_age_ms": ws_status.get("last_message_age_ms"),
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
            "entry_controls": entry_guard,
            "execution_metrics": dict(self._last_execution),
        }

    # ── Internal monitoring ───────────────────────────────────────

    async def _monitor_loop(self):
        """Monitor open trades and trigger exits with WS-first pricing."""
        while self._running:
            try:
                trades = list(self.open_trades.items())
                pending_entries = list(self.pending_entries.items())
                watched = {sym for sym in self._watch_symbols if sym}
                if not trades and not pending_entries and not watched:
                    if self._ws_feed:
                        await self._stop_ws_feed()
                    await asyncio.sleep(1)
                    continue

                tracked_symbols = (
                    {trade.symbol for _, trade in trades} | {entry.symbol for _, entry in pending_entries} | watched
                )
                await self._ensure_ws_feed(tracked_symbols)
                if watched:
                    await self._refresh_watch_prices(watched)

                for entry_id, pending in pending_entries:
                    price = await self._get_symbol_price(pending.symbol)
                    if price <= 0 or not pending.should_trigger(price):
                        continue
                    self._log(
                        "info",
                        f"🎯 Entry trigger fired for {pending.side} {pending.symbol} @ ${price:,.4f} ({pending.trigger_summary()})",
                    )
                    self.pending_entries.pop(entry_id, None)
                    trigger_size = pending.size or self._resolve_contract_size(
                        qty_mode=pending.qty_mode,
                        qty_value=pending.qty_value,
                        price=price,
                        leverage=pending.leverage,
                    )
                    result = await self._open_trade(
                        symbol=pending.symbol,
                        side=pending.side,
                        size=trigger_size,
                        leverage=pending.leverage,
                        qty_mode=pending.qty_mode,
                        qty_value=pending.qty_value,
                        target_price=pending.target_price,
                        sl_price=pending.sl_price,
                        target_pct=pending.target_pct,
                        sl_pct=pending.sl_pct,
                        target_usd=pending.target_usd,
                        sl_usd=pending.sl_usd,
                        mode=pending.mode,
                        guardrail_price=pending.entry_stop_price,
                        entry_limit_price=pending.entry_limit_price,
                        entry_stop_price=pending.entry_stop_price,
                        market_price=price,
                    )
                    if result.get("status") != "ok":
                        self._log(
                            "error",
                            f"Pending entry failed for {pending.symbol}: {result.get('message', 'unknown error')}",
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
            await asyncio.sleep(0.25)

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

    async def _close_trade(self, trade: ScalpTrade, reason: str) -> Dict[str, Any]:
        return await self._run_trade_action(trade.trade_id, "exit", lambda: self._close_trade_unlocked(trade, reason))

    async def _close_trade_unlocked(self, trade: ScalpTrade, reason: str) -> Dict[str, Any]:
        """Place exit order (or simulate) and move trade to closed_trades."""
        if trade.trade_id not in self.open_trades:
            return {
                "status": "error",
                "action": "exit",
                "trade_id": int(trade.trade_id or 0),
                "message": f"Trade {trade.trade_id} not found or already closed",
                "error_code": "trade_not_found",
            }
        exit_order_id = ""
        result: Dict[str, Any] = {}
        if trade.mode == "paper":
            exit_order_id = "PAPER"
        else:
            self._remember_execution(
                phase="exit",
                symbol=trade.symbol,
                side=trade.side,
                mode=trade.mode,
                verified=False,
                trade_id=trade.trade_id,
                requested_size=trade.size,
                note=f"Exit requested ({reason})",
                lifecycle="submitted",
                fill_status="submitted",
            )
            self._schedule_update(force=True)
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
                    self._remember_execution(
                        phase="exit_reject",
                        symbol=trade.symbol,
                        side=trade.side,
                        mode=trade.mode,
                        verified=False,
                        result=result,
                        trade_id=trade.trade_id,
                        requested_size=trade.size,
                        note=f"Exit rejected ({reason})",
                    )
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
                    return {
                        "status": "error",
                        "action": "exit",
                        "trade_id": int(trade.trade_id or 0),
                        "message": result.get("error") or "exit order could not be verified",
                        "trade": trade.to_dict(),
                        "error_code": "broker_rejected",
                    }
                exit_order_id = str(result.get("id", "closed"))
                trade.current_price = self._extract_order_price(result, trade.current_price)
            except Exception as e:
                trade._exit_attempts = getattr(trade, "_exit_attempts", 0) + 1
                self._remember_execution(
                    phase="exit_error",
                    symbol=trade.symbol,
                    side=trade.side,
                    mode=trade.mode,
                    verified=False,
                    error=str(e),
                    trade_id=trade.trade_id,
                    requested_size=trade.size,
                    note=f"Exit failed ({reason})",
                )
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
                return {
                    "status": "error",
                    "action": "exit",
                    "trade_id": int(trade.trade_id or 0),
                    "message": str(e),
                    "trade": trade.to_dict(),
                    "error_code": "broker_error",
                    "retryable": True,
                }

        exit_price = trade.current_price
        pnl = trade._compute_pnl(exit_price)

        trade.exit_time = _now_utc()
        trade.exit_price = exit_price
        trade.exit_reason = reason
        trade.exit_order_id = exit_order_id
        trade.pnl = round(pnl, 2)
        trade.status = "closed"
        trade.exit_latency_ms = round(_coerce_float(result.get("broker_latency_ms"), 0.0), 1)

        self._remember_execution(
            phase="exit",
            symbol=trade.symbol,
            side=trade.side,
            mode=trade.mode,
            verified=bool(result.get("verified", trade.mode == "paper")),
            result=result,
            trade_id=trade.trade_id,
            requested_size=trade.size,
            note=f"Exit completed ({reason})",
            lifecycle="filled" if trade.mode == "paper" else "",
            fill_status="paper_fill" if trade.mode == "paper" else "",
        )
        closed_dict = trade.to_dict()
        self.closed_trades.append(closed_dict)
        del self.open_trades[trade.trade_id]
        self._trade_action_locks.pop(int(trade.trade_id), None)

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
        return {"status": "ok", "action": "exited", "trade": closed_dict}

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
