"""
engine/live.py — CryptoForge Live Trading Engine
Perpetual futures auto-trading via Delta Exchange with REAL orders.
Production-ready: state persistence, event logging, indicator tracking, multi-engine support.
WebSocket-enhanced: real-time ticker for instant SL/TP checks between REST polls.
"""

import asyncio
import json as _json
import math
import os
import sys
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from engine.backtest import eval_condition_group
from engine.indicators import compute_dynamic_indicators

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    """Return current time in IST."""
    return datetime.now(IST)


def _coerce_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _interval_duration(interval: str) -> timedelta:
    raw = str(interval or "5m").strip().lower()
    try:
        qty = int(raw[:-1])
        unit = raw[-1]
    except (TypeError, ValueError):
        return timedelta(minutes=5)
    if unit == "m":
        return timedelta(minutes=qty)
    if unit == "h":
        return timedelta(hours=qty)
    if unit == "d":
        return timedelta(days=qty)
    if unit == "w":
        return timedelta(weeks=qty)
    return timedelta(minutes=5)


def _select_signal_rows(df, interval: str, now: datetime):
    """Use only fully closed candles for signal entry/exit evaluation."""
    if len(df.index) == 0:
        return None, None, None, True

    latest_row = df.iloc[-1]

    latest_idx = df.index[-1]
    latest_dt = latest_idx.to_pydatetime() if hasattr(latest_idx, "to_pydatetime") else None
    if latest_dt is None:
        return latest_row, latest_row, df.iloc[-2] if len(df) > 1 else latest_row, True
    if latest_dt.tzinfo is None:
        latest_dt = latest_dt.replace(tzinfo=timezone.utc)
    candle_closed = now.astimezone(latest_dt.tzinfo) >= (latest_dt + _interval_duration(interval))

    if candle_closed:
        signal_row = latest_row
        signal_prev = df.iloc[-2] if len(df) > 1 else latest_row
        return latest_row, signal_row, signal_prev, True

    if len(df) < 2:
        return latest_row, None, None, False

    signal_row = df.iloc[-2]
    signal_prev = df.iloc[-3] if len(df) > 2 else signal_row
    return latest_row, signal_row, signal_prev, False


# Try to import WebSocket feed (optional enhancement)
try:
    from engine.ws_feed import DeltaWSFeed

    _HAS_WS = True
except ImportError:
    _HAS_WS = False

# ── State File ────────────────────────────────────────────────
_STATE_DIR = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_STATE_FILE = os.path.join(_STATE_DIR, "live_state.json")


class LiveEngine:
    """Live perpetual futures trading engine with Delta Exchange.
    Multi-engine ready (keyed by run_id). State-persistent across restarts."""

    def __init__(self, broker, run_id: str = None):
        self.broker = broker
        self.running = False
        self.run_id = run_id
        self.session_date = None

        # Per-instance state file
        if run_id:
            safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in run_id)
            self._state_file = os.path.join(_STATE_DIR, f"live_state_{safe_id}.json")
        else:
            self._state_file = _DEFAULT_STATE_FILE

        # Strategy configuration
        self.strategy = {}
        self.entry_conditions = []
        self.exit_conditions = []
        self.deploy_config = {}

        # Trading state
        self.in_trade = False
        self.open_trades = []
        self.closed_trades = []
        self.total_pnl = 0.0
        self.trades_today = 0
        self.daily_pnl = 0.0
        self.max_daily_loss = 0.0
        self._last_trade_date = None
        self.capital = 0.0  # effective capital, persisted across restarts
        self._trades_lock = asyncio.Lock()  # guards open_trades + closed_trades mutations

        # Live data tracking for UI
        self.current_indicators = {}
        self.current_candle = {}

        # WebSocket feed for real-time price (optional)
        self._ws_feed = None
        self._ws_price = None  # latest price from WS ticker

        # Event logging
        self.event_log = []

        # Restore last session
        self._load_state()

    # ── STATE PERSISTENCE ─────────────────────────────────────
    def _save_state(self):
        """Persist current session state to disk."""
        try:
            state = {
                "session_date": str(self.session_date) if self.session_date else None,
                "strategy_name": self.strategy.get("run_name", ""),
                "symbol": self.strategy.get("symbol", ""),
                "in_trade": self.in_trade,
                "open_trades": self.open_trades,
                "closed_trades": self.closed_trades,
                "trades_today": self.trades_today,
                "daily_pnl": self.daily_pnl,
                "total_pnl": self.total_pnl,
                "capital": self.capital,
                "current_candle": self.current_candle,
                "current_indicators": {
                    k: (v if not isinstance(v, float) or not math.isnan(v) else None)
                    for k, v in self.current_indicators.items()
                },
                "event_log": [
                    {
                        "time": e["time"].strftime("%Y-%m-%d %H:%M:%S")
                        if isinstance(e["time"], datetime)
                        else str(e["time"]),
                        "type": e["type"],
                        "message": e["message"],
                    }
                    for e in self.event_log[-100:]
                ],
                "saved_at": str(_now_ist()),
            }
            with open(self._state_file, "w") as f:
                _json.dump(state, f, indent=2, default=str)
        except Exception as e:
            print(f"[LIVE] State save failed: {e}")

    def _load_state(self):
        """Load last session state from disk."""
        try:
            if not os.path.exists(self._state_file):
                return
            with open(self._state_file, "r") as f:
                state = _json.load(f)

            saved_date = state.get("session_date")
            today = str(date_type.today())
            if saved_date != today:
                print(f"[LIVE] Stale state from {saved_date} (today={today}) — ignoring")
                return

            self.session_date = date_type.today()
            self.in_trade = state.get("in_trade", False)
            self.open_trades = state.get("open_trades", [])
            self.closed_trades = state.get("closed_trades", [])
            self.trades_today = state.get("trades_today", 0)
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.total_pnl = state.get("total_pnl", 0.0)
            self.capital = state.get("capital", 0.0)
            self.current_candle = state.get("current_candle", {})
            self.current_indicators = state.get("current_indicators", {})

            if state.get("strategy_name"):
                self.strategy["run_name"] = state["strategy_name"]
            if state.get("symbol"):
                self.strategy["symbol"] = state["symbol"]

            raw_log = state.get("event_log", [])
            for entry in raw_log:
                try:
                    t = datetime.strptime(entry["time"], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    t = _now_ist()
                self.event_log.append({"time": t, "type": entry["type"], "message": entry["message"], "data": {}})

            n = len(self.closed_trades)
            pnl = sum(t.get("pnl", 0) for t in self.closed_trades)
            print(f"[LIVE] Restored state: {n} trades, P&L=${pnl:,.2f}")
        except Exception as e:
            print(f"[LIVE] State load failed: {e}")

    def configure(self, strategy: dict, entry_conditions: list, exit_conditions: list, deploy_config: dict = None):
        self.strategy = strategy
        self.entry_conditions = entry_conditions
        self.exit_conditions = exit_conditions
        self.deploy_config = deploy_config or {}
        self.log_event("info", f"Strategy configured: {strategy.get('run_name', 'Unnamed')}")

    def log_event(self, event_type: str, message: str, data: dict = None):
        event = {
            "time": _now_ist(),
            "type": event_type,
            "message": message,
            "data": data or {},
        }
        self.event_log.append(event)
        if len(self.event_log) > 500:
            self.event_log = self.event_log[-300:]
        ts = event["time"].strftime("%H:%M:%S IST")
        print(f"[LIVE] [{ts}] [{event_type.upper()}] {message}")

    async def _start_ws_feed(self, symbol: str):
        """Try to start WebSocket feed for real-time price updates."""
        if not _HAS_WS:
            return
        try:
            self._ws_feed = DeltaWSFeed()

            def _on_ticker(sym, ticker):
                try:
                    mark = ticker.get("mark_price") or ticker.get("close") or ticker.get("last_price")
                    if mark:
                        self._ws_price = float(mark)
                except (TypeError, ValueError):
                    pass

            self._ws_feed.on_ticker = _on_ticker
            await self._ws_feed.connect()
            await self._ws_feed.subscribe_ticker(symbol)
            self.log_event("info", f"WebSocket ticker connected for {symbol}")
        except Exception as e:
            self.log_event("warn", f"WebSocket feed unavailable: {e} — using REST only")
            self._ws_feed = None

    async def _stop_ws_feed(self):
        """Disconnect WebSocket feed."""
        if self._ws_feed:
            try:
                await self._ws_feed.disconnect()
            except Exception:
                pass
            self._ws_feed = None
            self._ws_price = None

    def _next_trade_id(self) -> int:
        max_id = 0
        for trade in [*self.open_trades, *self.closed_trades]:
            try:
                max_id = max(max_id, int(trade.get("id", 0) or 0))
            except (AttributeError, TypeError, ValueError):
                continue
        return max_id + 1

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
        place_verified = getattr(self.broker, "place_order_verified", None)
        if not callable(place_verified):
            return {"error": "Broker does not support verified order placement", "verified": False}
        return await place_verified(**kwargs)

    def _build_reconciled_trade(self, position: dict, symbol: str, leverage: int, product_id: int) -> dict:
        signed_size = _coerce_float(position.get("size"), 0.0)
        size = abs(signed_size)
        side = "LONG" if signed_size >= 0 else "SHORT"
        position_leverage = int(_coerce_float(position.get("leverage"), leverage) or leverage)
        entry_price = self._extract_order_price(position, 0.0)
        notional = round(size * entry_price, 2) if size > 0 and entry_price > 0 else 0.0
        margin = _coerce_float(position.get("margin"), 0.0)
        if margin <= 0 and notional > 0:
            margin = round(notional / max(position_leverage, 1), 2)
        if notional <= 0 and margin > 0:
            notional = round(margin * max(position_leverage, 1), 2)
        return {
            "id": self._next_trade_id(),
            "symbol": symbol,
            "side": side,
            "entry_price": entry_price,
            "entry_time": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            "size": size,
            "notional": round(notional, 2),
            "margin": round(margin, 2),
            "leverage": position_leverage,
            "product_id": product_id,
            "order_id": "reconciled",
            "reconciled": True,
            "_exit_attempts": 0,
        }

    async def _reconcile_exchange_position(self, product_id: int, symbol: str, leverage: int):
        position = await asyncio.to_thread(self.broker.get_position, product_id, True)
        signed_size = _coerce_float(position.get("size"), 0.0)
        if abs(signed_size) <= 0:
            if self.open_trades:
                cleared = len(self.open_trades)
                self.open_trades = []
                self.in_trade = False
                self.log_event(
                    "warn",
                    f"Reconciliation cleared {cleared} restored local trade(s) for {symbol}; "
                    "no open exchange position exists.",
                )
                self._save_state()
            return

        reconciled = self._build_reconciled_trade(position, symbol, leverage, product_id)
        existing = self.open_trades[0] if self.open_trades else None
        mismatches = []
        if existing:
            if str(existing.get("side", "")).upper() != reconciled["side"]:
                mismatches.append("side")
            existing_size = _coerce_float(existing.get("size"), 0.0)
            if abs(existing_size - reconciled["size"]) > max(1e-6, reconciled["size"] * 0.001):
                mismatches.append("size")
            existing_entry = _coerce_float(existing.get("entry_price"), 0.0)
            if (
                existing_entry > 0
                and reconciled["entry_price"] > 0
                and abs(existing_entry - reconciled["entry_price"]) > max(0.5, reconciled["entry_price"] * 0.001)
            ):
                mismatches.append("entry price")

        self.open_trades = [reconciled]
        self.in_trade = True
        if not existing:
            self.log_event(
                "warn",
                f"Recovered exchange position for {symbol}: {reconciled['side']} size={reconciled['size']} "
                f"@ ${reconciled['entry_price']:,.2f}.",
            )
        elif mismatches:
            self.log_event(
                "warn",
                f"Reconciled restored local trade to exchange state for {symbol} ({', '.join(mismatches)} corrected).",
            )
        else:
            self.log_event("info", f"Exchange reconciliation confirmed the restored live position for {symbol}.")
        self._save_state()

    async def start(self, callback: Callable = None):
        self.running = True
        today = date_type.today()
        restored_same_day = self.session_date == today
        self.session_date = today
        if not restored_same_day:
            self.daily_pnl = 0.0
            self.trades_today = 0
        self.max_daily_loss = float(self.strategy.get("max_daily_loss", 0) or 0)
        self._last_trade_date = today

        symbol = self.strategy.get("symbol", "BTCUSDT")
        leverage = int(self.strategy.get("leverage", 10))
        poll_interval = int(self.strategy.get("poll_interval", 30))
        max_tpd = int(self.strategy.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
        indicators = self.strategy.get("indicators", [])
        trade_side = self.strategy.get("trade_side", "LONG").upper()
        sl_pct = float(self.strategy.get("stoploss_pct", 5))
        tp_pct = float(self.strategy.get("target_profit_pct", 10))
        initial_capital = float(self.strategy.get("initial_capital", config.DEFAULT_CAPITAL))
        # Restore effective capital from state (persisted across restarts); fall back to initial
        capital = self.capital if self.capital > 0 else initial_capital
        self.capital = capital
        position_size_pct = float(self.strategy.get("position_size_pct", 100))
        candle_interval = self.strategy.get("candle_interval", "5m")

        # Get product ID for real orders
        product = self.broker.get_product_by_symbol(symbol)
        product_id = product.get("id") if product else None
        if not product_id:
            msg = f"Product not found for {symbol} — refusing to start live engine."
            self.log_event("error", msg)
            self.running = False
            if callback:
                await callback({"type": "error", "message": msg})
            return

        # Set leverage on exchange
        try:
            self.broker.set_leverage(product_id, leverage)
            self.log_event("info", f"Leverage set to {leverage}x on {symbol}")
        except Exception as e:
            self.log_event("warn", f"Could not set leverage: {e}")

        self.log_event("start", f"LIVE Trading Engine Started — {symbol} {leverage}x {trade_side}")
        self.log_event("info", f"Timeframe: {candle_interval} | SL: {sl_pct}% | TP: {tp_pct}%")
        self.log_event("info", f"Max trades/day: {max_tpd} | Capital: ${capital:,.0f}")
        self.log_event("warn", "REAL MONEY — Orders will be placed on Delta Exchange")
        if self.max_daily_loss > 0:
            self.log_event("info", f"Max daily loss: ${self.max_daily_loss:,.0f}")
        try:
            await self._reconcile_exchange_position(product_id, symbol, leverage)
        except Exception as e:
            msg = f"Startup reconciliation failed for {symbol}: {e}"
            self.log_event("error", msg)
            self.running = False
            if callback:
                await callback({"type": "error", "message": msg})
            return

        # Start WebSocket feed for real-time price
        await self._start_ws_feed(symbol)

        if callback:
            await callback({"type": "started", "symbol": symbol, "leverage": leverage, "mode": "live"})

        while self.running:
            try:
                now = _now_ist()
                today = now.date()

                if today != self._last_trade_date:
                    self.trades_today = 0
                    self.daily_pnl = 0.0
                    self._last_trade_date = today

                # Max daily loss check
                if self.max_daily_loss > 0 and self.daily_pnl <= -self.max_daily_loss:
                    self.log_event("warn", f"Max daily loss hit (${self.daily_pnl:,.2f}) — pausing entries")
                    await asyncio.sleep(poll_interval)
                    continue

                # Fetch candle history for indicators
                lookback = now - timedelta(days=3)
                df = await self.broker.async_get_candles(
                    symbol,
                    resolution=candle_interval,
                    start=lookback.strftime("%Y-%m-%d"),
                    end=now.strftime("%Y-%m-%d"),
                )
                if df.empty:
                    await asyncio.sleep(poll_interval)
                    continue

                df = compute_dynamic_indicators(df, indicators)
                row, signal_row, signal_prev, _candle_closed = _select_signal_rows(df, candle_interval, now)
                price = float(row["close"])

                # Use WebSocket ticker price if available (more real-time than REST candle close)
                ws_price = self._ws_price
                exit_price = ws_price if ws_price is not None else price

                # Update UI tracking
                self.current_candle = {
                    "time": str(row.name) if hasattr(row, "name") else str(now),
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": price,
                    "volume": float(row.get("volume", 0)),
                }
                self.current_indicators = {}
                for ind in indicators:
                    col = ind.lower()
                    for c in df.columns:
                        if c.lower().startswith(col) or col in c.lower():
                            val = row.get(c)
                            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                                self.current_indicators[c] = round(float(val), 6)

                in_trade = len(self.open_trades) > 0

                if not in_trade:
                    if self.trades_today < max_tpd:
                        if signal_row is not None and eval_condition_group(
                            signal_row, self.entry_conditions, signal_prev
                        ):
                            margin = capital * (position_size_pct / 100)
                            notional = margin * leverage
                            size = max(1, int(notional / price))
                            side = "buy" if trade_side == "LONG" else "sell"

                            try:
                                result = await self._place_verified_order(
                                    product_id=product_id,
                                    size=size,
                                    side=side,
                                    order_type="market_order",
                                    leverage=leverage,
                                )
                                if isinstance(result, dict) and (result.get("error") or not result.get("verified")):
                                    err = result.get("error") or "entry order could not be verified"
                                    self.log_event("error", f"Order rejected: {err}")
                                    continue
                                order_id = result.get("id", "placed")
                                entry_price = self._extract_order_price(result, price)
                                notional = round(size * entry_price, 2) if entry_price > 0 else round(notional, 2)
                                margin = round(notional / max(leverage, 1), 2) if notional > 0 else round(margin, 2)
                                self.log_event(
                                    "order",
                                    f"Order verified: {side} {size} {symbol} (ID: {order_id}, attempt "
                                    f"{result.get('verified_at_attempt', 1)})",
                                )
                            except Exception as e:
                                self.log_event("error", f"Order failed: {e}")
                                continue

                            trade = {
                                "id": self._next_trade_id(),
                                "symbol": symbol,
                                "side": trade_side,
                                "entry_price": entry_price,
                                "entry_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                                "size": size,
                                "notional": round(notional, 2),
                                "margin": round(margin, 2),
                                "leverage": leverage,
                                "product_id": product_id,
                                "order_id": order_id,
                                "verified_at_attempt": result.get("verified_at_attempt"),
                                "_exit_attempts": 0,  # tracks failed exit order attempts
                            }
                            async with self._trades_lock:
                                self.open_trades.append(trade)
                                self.in_trade = True
                            self.trades_today += 1
                            self.log_event(
                                "entry",
                                f"ENTRY {trade_side} {symbol} @ ${entry_price:,.2f} (size={size})",
                            )

                            if callback:
                                await callback(
                                    {
                                        "type": "entry",
                                        "trade": trade,
                                        "price": entry_price,
                                        "open_positions": len(self.open_trades),
                                        "closed_trades": len(self.closed_trades),
                                        "open_trades": list(self.open_trades),
                                        "recent_trades": self.closed_trades[-50:],
                                        "total_pnl": round(self.total_pnl, 2),
                                    }
                                )

                            self._save_state()
                else:
                    for trade in self.open_trades[:]:
                        ep = trade["entry_price"]
                        # Use real-time WS price for SL/TP, REST price for signals
                        check_price = exit_price
                        trade_side_local = str(trade.get("side", trade_side)).upper()
                        if trade_side_local == "LONG":
                            pnl_pct = (check_price - ep) / ep * 100
                        else:
                            pnl_pct = (ep - check_price) / ep * 100

                        trade_leverage = int(trade.get("leverage", leverage) or leverage)
                        lev_pnl_pct = pnl_pct * trade_leverage
                        trade_notional = _coerce_float(trade.get("notional"), 0.0)
                        if trade_notional <= 0:
                            trade_notional = round(_coerce_float(trade.get("size"), 0.0) * ep, 2)
                        trade_pnl = trade_notional * (pnl_pct / 100)
                        exit_reason = None

                        if sl_pct > 0 and lev_pnl_pct <= -sl_pct:
                            exit_reason = "Stop Loss"
                        elif tp_pct > 0 and lev_pnl_pct >= tp_pct:
                            exit_reason = "Take Profit"
                        elif lev_pnl_pct <= config.LIQUIDATION_THRESHOLD:
                            exit_reason = "Liquidation"
                        elif signal_row is not None and eval_condition_group(
                            signal_row, self.exit_conditions, signal_prev
                        ):
                            exit_reason = "Signal Exit"

                        if exit_reason:
                            # Place exit order on exchange
                            close_side = "sell" if trade_side_local == "LONG" else "buy"
                            exit_ok = True
                            try:
                                exit_result = await self._place_verified_order(
                                    product_id=int(trade.get("product_id", product_id) or product_id),
                                    size=trade.get("size", 1),
                                    side=close_side,
                                    order_type="market_order",
                                    leverage=trade_leverage,
                                    reduce_only=True,
                                )
                                if isinstance(exit_result, dict) and (
                                    exit_result.get("error") or not exit_result.get("verified")
                                ):
                                    trade["_exit_attempts"] = trade.get("_exit_attempts", 0) + 1
                                    attempt = trade["_exit_attempts"]
                                    err = exit_result.get("error") or "exit order could not be verified"
                                    self.log_event("error", f"Exit order rejected (attempt {attempt}/3): {err}")
                                    if attempt >= 3:
                                        self.log_event(
                                            "error",
                                            f"CRITICAL: Exit failed 3 times for trade "
                                            f"{trade.get('id')} ({trade_side_local} {symbol}). "
                                            f"MANUAL INTERVENTION REQUIRED. Engine stopping.",
                                        )
                                        self.running = False
                                    exit_ok = False
                                else:
                                    self.log_event(
                                        "order",
                                        f"Exit order verified: {close_side} {trade.get('size', 1)} {symbol} "
                                        f"(attempt {exit_result.get('verified_at_attempt', 1)})",
                                    )
                            except Exception as e:
                                trade["_exit_attempts"] = trade.get("_exit_attempts", 0) + 1
                                attempt = trade["_exit_attempts"]
                                self.log_event("error", f"Exit order failed (attempt {attempt}/3): {e}")
                                if attempt >= 3:
                                    self.log_event(
                                        "error",
                                        f"CRITICAL: Exit failed 3 times for trade "
                                        f"{trade.get('id')} ({trade_side_local} {symbol}). "
                                        f"MANUAL INTERVENTION REQUIRED. Engine stopping.",
                                    )
                                    self.running = False
                                exit_ok = False

                            if not exit_ok:
                                continue  # Don't close locally if exchange order failed

                            actual_exit_price = self._extract_order_price(exit_result, check_price)
                            if trade_side_local == "LONG":
                                pnl_pct = (actual_exit_price - ep) / ep * 100
                            else:
                                pnl_pct = (ep - actual_exit_price) / ep * 100
                            trade_pnl = trade_notional * (pnl_pct / 100)
                            self.total_pnl += trade_pnl
                            self.daily_pnl += trade_pnl
                            capital += trade_pnl
                            capital = max(capital, 0)
                            self.capital = capital

                            closed = {
                                **trade,
                                "exit_price": actual_exit_price,
                                "exit_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                                "pnl": round(trade_pnl, 2),
                                "exit_reason": exit_reason,
                            }
                            async with self._trades_lock:
                                self.closed_trades.append(closed)
                                self.open_trades.remove(trade)
                                if not self.open_trades:
                                    self.in_trade = False

                            emoji = "+" if trade_pnl >= 0 else ""
                            self.log_event(
                                "exit",
                                f"EXIT {exit_reason} {symbol} @ ${actual_exit_price:,.2f} | "
                                f"P&L: {emoji}${trade_pnl:,.2f}",
                            )

                            if callback:
                                await callback(
                                    {
                                        "type": "exit",
                                        "trade": closed,
                                        "total_pnl": round(self.total_pnl, 2),
                                        "open_positions": len(self.open_trades),
                                        "closed_trades": len(self.closed_trades),
                                        "open_trades": list(self.open_trades),
                                        "recent_trades": self.closed_trades[-50:],
                                    }
                                )

                            self.capital = capital
                            self._save_state()

                # Periodic state save
                self.capital = capital
                self._save_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[LIVE] Error: {e}")
                self.log_event("error", str(e))
                if callback:
                    await callback({"type": "error", "message": str(e)})

            await asyncio.sleep(poll_interval)

        self.log_event("stop", "Live Trading Engine Stopped")
        await self._stop_ws_feed()
        self._save_state()

    def stop(self):
        self.running = False

    def get_status(self) -> dict:
        closed_rows = self.closed_trades[-200:]
        return {
            "running": self.running,
            "run_id": self.run_id or "",
            "mode": "live",
            "strategy_name": self.strategy.get("run_name", ""),
            "run_name": self.strategy.get("run_name", ""),
            "symbol": self.strategy.get("symbol", ""),
            "leverage": self.strategy.get("leverage", 10),
            "trade_side": self.strategy.get("trade_side", "LONG"),
            "side": self.strategy.get("trade_side", "LONG"),
            "open_positions": len(self.open_trades),
            "closed_trades": len(self.closed_trades),
            "closed_trade_rows": closed_rows,
            "recent_trades": closed_rows,
            "trades_today": self.trades_today,
            "total_pnl": round(self.total_pnl, 2),
            "daily_pnl": round(self.daily_pnl, 2),
            "open_trades": self.open_trades,
            "current_candle": self.current_candle,
            "current_indicators": self.current_indicators,
            "event_log": [
                {
                    "time": e["time"].strftime("%H:%M:%S IST") if isinstance(e["time"], datetime) else str(e["time"]),
                    "type": e["type"],
                    "message": e["message"],
                }
                for e in self.event_log[-50:]
            ],
        }
