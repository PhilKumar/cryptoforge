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
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional


def _now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


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

        # Resolve absolute TP/SL from percentage if needed
        self.target_price = target_price
        self.sl_price = sl_price

        if not self.target_price and target_pct > 0 and entry_price > 0:
            # Convert leveraged % to raw price move
            price_move_pct = target_pct / leverage
            mult = 1 if side == "LONG" else -1
            self.target_price = round(entry_price * (1 + mult * price_move_pct / 100), 6)

        if not self.sl_price and sl_pct > 0 and entry_price > 0:
            price_move_pct = sl_pct / leverage
            mult = -1 if side == "LONG" else 1
            self.sl_price = round(entry_price * (1 + mult * price_move_pct / 100), 6)

        self.target_usd = target_usd
        self.sl_usd = sl_usd

        self.exit_price: float = 0.0
        self.exit_time: Optional[datetime] = None
        self.exit_reason: str = ""
        self.exit_order_id: str = ""
        self.pnl: float = 0.0
        self.status: str = "open"

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
            "status": self.status,
            "mode": self.mode,
        }


class ScalpEngine:
    """
    Manages all active crypto scalp trades.
    • Runs a background monitoring loop (2s interval).
    • Uses bulk ticker fetch for all symbols in one REST call.
    """

    def __init__(self, delta_client, on_trade_closed: Optional[Callable[[dict], None]] = None):
        self.delta = delta_client
        # Callback invoked with trade dict whenever a trade is closed (for disk persistence).
        self._on_trade_closed = on_trade_closed
        self.open_trades: Dict[int, ScalpTrade] = {}
        self.closed_trades: list = []
        self.event_log: list = []
        self._trade_counter: int = 0
        self._running: bool = False
        self._task: Optional[asyncio.Task] = None

    # ── Public API ───────────────────────────────────────────────

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._monitor_loop())

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

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
        mode: str = "live",
    ) -> Dict[str, Any]:
        """Place a broker order (or simulate in paper mode) and register the scalp trade."""

        # Look up product
        product_id = 0
        try:
            product = await asyncio.to_thread(self.delta.get_product_by_symbol, symbol)
            if product:
                product_id = int(product.get("id", 0))
        except Exception:
            pass

        # Get current mark price
        entry_price = 0.0
        try:
            ticker = await asyncio.to_thread(self.delta.get_ticker, symbol)
            entry_price = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
        except Exception:
            pass

        order_id = ""
        if mode == "paper":
            order_id = "PAPER"
        else:
            if not product_id:
                return {"status": "error", "message": f"Product not found for {symbol}"}
            try:
                order_side = "buy" if side == "LONG" else "sell"
                result = await asyncio.to_thread(
                    self.delta.place_order,
                    product_id=product_id,
                    size=size,
                    side=order_side,
                    order_type="market_order",
                    leverage=leverage,
                )
                if isinstance(result, dict) and result.get("error"):
                    return {"status": "error", "message": result["error"]}
                order_id = str(result.get("id", "placed"))
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
        )
        self.open_trades[self._trade_counter] = trade

        mode_label = "[PAPER] " if mode == "paper" else ""
        self._log(
            "entry",
            f"{mode_label}✅ SCALP ENTER: {side} {symbol} @ ${entry_price:,.4f} "
            f"size={size} lev={leverage}x orderId={order_id} "
            f"tp=${trade.target_price or 'none'} sl=${trade.sl_price or 'none'} "
            f"tp_usd=${trade.target_usd or 'none'} sl_usd=${trade.sl_usd or 'none'}",
        )

        if not self._running:
            self.start()

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

        return {
            "running": self._running,
            "in_trade": len(self.open_trades) > 0,
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
        }

    # ── Internal monitoring ───────────────────────────────────────

    async def _monitor_loop(self):
        """Poll prices every ~2s and trigger auto-exits."""
        while self._running:
            try:
                trades = list(self.open_trades.items())
                if not trades:
                    await asyncio.sleep(2)
                    continue

                price_map = await self._fetch_all_prices(trades)

                for tid, trade in trades:
                    price = price_map.get(trade.symbol, 0.0)
                    if price > 0:
                        if trade.entry_price == 0:
                            trade.entry_price = price
                            self._log("info", f"📌 Trade {tid} entry price set @ ${price:,.4f}")
                        trade.current_price = price

                    reason = trade.check_exit(trade.current_price)
                    if reason:
                        await self._close_trade(trade, reason)
            except Exception as e:
                self._log("error", f"Monitor error: {e}")
            await asyncio.sleep(2)

    async def _fetch_all_prices(self, trades: list) -> dict:
        """Fetch mark prices for all unique symbols via bulk ticker call.
        Returns {symbol: mark_price}."""
        symbols = list({trade.symbol for _, trade in trades})
        result = {}
        try:
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

            for sym in symbols:
                if sym in ticker_map:
                    result[sym] = ticker_map[sym]
                else:
                    ds = self.delta.to_delta_symbol(sym)
                    if ds in ticker_map:
                        result[sym] = ticker_map[ds]
        except Exception as e:
            self._log("error", f"Bulk ticker fetch failed: {e}")
            # Fallback: individual fetch per symbol
            for sym in symbols:
                try:
                    ticker = await asyncio.to_thread(self.delta.get_ticker, sym)
                    p = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
                    if p > 0:
                        result[sym] = p
                except Exception:
                    pass
        return result

    async def _close_trade(self, trade: ScalpTrade, reason: str):
        """Place exit order (or simulate) and move trade to closed_trades."""
        exit_order_id = ""
        if trade.mode == "paper":
            exit_order_id = "PAPER"
        else:
            try:
                close_side = "sell" if trade.side == "LONG" else "buy"
                result = await asyncio.to_thread(
                    self.delta.place_order,
                    product_id=trade.product_id,
                    size=trade.size,
                    side=close_side,
                    order_type="market_order",
                    reduce_only=True,
                )
                if isinstance(result, dict) and result.get("error"):
                    # Broker rejected the exit — leave trade open so monitor retries.
                    # Increment attempt counter; after 3 failures alert and halt engine.
                    trade._exit_attempts = getattr(trade, "_exit_attempts", 0) + 1
                    self._log(
                        "error",
                        f"Exit order REJECTED for trade {trade.trade_id} "
                        f"(attempt {trade._exit_attempts}): {result['error']}",
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
            except Exception as e:
                trade._exit_attempts = getattr(trade, "_exit_attempts", 0) + 1
                self._log(
                    "error",
                    f"Exit order FAILED for trade {trade.trade_id} " f"(attempt {trade._exit_attempts}): {e}",
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

        closed_dict = trade.to_dict()
        self.closed_trades.append(closed_dict)
        del self.open_trades[trade.trade_id]

        # Persist to disk via callback (handles both auto and manual exits uniformly)
        if self._on_trade_closed:
            try:
                self._on_trade_closed(closed_dict)
            except Exception as _e:
                self._log("error", f"Trade persistence callback failed: {_e}")

        pnl_sign = "+" if pnl >= 0 else ""
        self._log(
            "exit" if pnl >= 0 else "stop",
            f"{'✅' if pnl >= 0 else '🛑'} SCALP EXIT [{reason}]: "
            f"{trade.side} {trade.symbol} "
            f"entry=${trade.entry_price:,.4f} exit=${exit_price:,.4f} "
            f"PnL={pnl_sign}${pnl:.2f}",
        )

    def _log(self, level: str, msg: str):
        entry = {
            "time": _now_utc().strftime("%H:%M:%S"),
            "level": level,
            "msg": msg,
        }
        self.event_log.append(entry)
        print(f"[SCALP][{level.upper()}] {msg}")
