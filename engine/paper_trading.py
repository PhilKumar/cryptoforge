"""
engine/paper_trading.py — CryptoForge Paper Trading Engine
Simulated perpetual futures trading using real-time market data from Delta Exchange / Binance.
Production-ready: state persistence, event logging, indicator tracking, multi-engine support.
WebSocket-enhanced: real-time ticker for instant SL/TP checks between REST polls.
"""

import asyncio
import json as _json
import math
import os
import sys
from datetime import datetime, date as date_type, timedelta, timezone
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from engine.indicators import compute_dynamic_indicators
from engine.backtest import eval_condition_group
import config

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

def _now_ist() -> datetime:
    """Return current time in IST."""
    return datetime.now(IST)

# Try to import WebSocket feed (optional enhancement)
try:
    from engine.ws_feed import DeltaWSFeed
    _HAS_WS = True
except ImportError:
    _HAS_WS = False

# ── State File ────────────────────────────────────────────────
_STATE_DIR = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_STATE_FILE = os.path.join(_STATE_DIR, "paper_state.json")


class PaperTradingEngine:
    """Paper trading engine — uses real market data, simulated orders.
    Multi-engine ready (keyed by run_id).  State-persistent across restarts."""

    def __init__(self, broker, run_id: str = None):
        self.broker = broker
        self.running = False
        self.run_id = run_id
        self.session_date = None

        # Per-instance state file
        if run_id:
            safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in run_id)
            self._state_file = os.path.join(_STATE_DIR, f"paper_state_{safe_id}.json")
        else:
            self._state_file = _DEFAULT_STATE_FILE

        # Strategy configuration
        self.strategy = {}
        self.entry_conditions = []
        self.exit_conditions = []

        # Trading state
        self.in_trade = False
        self.open_trades = []
        self.closed_trades = []
        self.total_pnl = 0.0
        self.trades_today = 0
        self.daily_pnl = 0.0
        self.max_daily_loss = 0.0
        self._last_trade_date = None

        # Live data tracking for UI
        self.current_indicators = {}
        self.current_candle = {}
        self._prev_row = None

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
                "current_candle": self.current_candle,
                "current_indicators": {
                    k: (v if not isinstance(v, float) or not math.isnan(v) else None)
                    for k, v in self.current_indicators.items()
                },
                "event_log": [
                    {"time": e["time"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(e["time"], datetime) else str(e["time"]),
                     "type": e["type"], "message": e["message"]}
                    for e in self.event_log[-100:]
                ],
                "saved_at": str(_now_ist()),
            }
            with open(self._state_file, "w") as f:
                _json.dump(state, f, indent=2, default=str)
        except Exception as e:
            print(f"[PAPER] State save failed: {e}")

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
                print(f"[PAPER] Stale state from {saved_date} (today={today}) — ignoring")
                return

            self.session_date = date_type.today()
            self.in_trade = state.get("in_trade", False)
            self.open_trades = state.get("open_trades", [])
            self.closed_trades = state.get("closed_trades", [])
            self.trades_today = state.get("trades_today", 0)
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.total_pnl = state.get("total_pnl", 0.0)
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
                self.event_log.append({"time": t, "type": entry["type"],
                                       "message": entry["message"], "data": {}})

            n = len(self.closed_trades)
            pnl = sum(t.get("pnl", 0) for t in self.closed_trades)
            print(f"[PAPER] Restored state: {n} trades, P&L=${pnl:,.2f}")
        except Exception as e:
            print(f"[PAPER] State load failed: {e}")

    def configure(self, strategy: dict, entry_conditions: list,
                  exit_conditions: list):
        self.strategy = strategy
        self.entry_conditions = entry_conditions
        self.exit_conditions = exit_conditions
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
        print(f"[PAPER] [{ts}] [{event_type.upper()}] {message}")

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

    async def start(self, callback: Callable = None):
        self.running = True
        self.session_date = date_type.today()
        self.daily_pnl = 0.0
        self.max_daily_loss = float(self.strategy.get("max_daily_loss", 0) or 0)

        symbol = self.strategy.get("symbol", "BTCUSDT")
        leverage = int(self.strategy.get("leverage", 10))
        poll_interval = int(self.strategy.get("poll_interval", 30))
        max_tpd = int(self.strategy.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
        indicators = self.strategy.get("indicators", [])
        trade_side = self.strategy.get("trade_side", "LONG").upper()
        sl_pct = float(self.strategy.get("stoploss_pct", 5))
        tp_pct = float(self.strategy.get("target_profit_pct", 10))
        trailing_sl_pct = float(self.strategy.get("trailing_sl_pct", 0))
        capital = float(self.strategy.get("initial_capital", config.DEFAULT_CAPITAL))
        position_size_pct = float(self.strategy.get("position_size_pct", 100))
        candle_interval = self.strategy.get("candle_interval", "5m")

        self.log_event("start", f"Paper Trading Engine Started — {symbol} {leverage}x {trade_side}")
        self.log_event("info", f"Timeframe: {candle_interval} | SL: {sl_pct}% | TP: {tp_pct}%")
        if trailing_sl_pct > 0:
            self.log_event("info", f"Trailing SL: {trailing_sl_pct}% from best price")
        self.log_event("info", f"Max trades/day: {max_tpd} | Capital: ${capital:,.0f}")
        if self.max_daily_loss > 0:
            self.log_event("info", f"Max daily loss: ${self.max_daily_loss:,.0f}")

        # Start WebSocket feed for real-time price
        await self._start_ws_feed(symbol)

        if callback:
            await callback({"type": "started", "symbol": symbol, "mode": "paper"})

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
                    symbol, resolution=candle_interval,
                    start=lookback.strftime("%Y-%m-%d"),
                    end=now.strftime("%Y-%m-%d"),
                )
                if df.empty:
                    await asyncio.sleep(poll_interval)
                    continue

                df = compute_dynamic_indicators(df, indicators)
                row = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else row
                price = float(row["close"])

                # Use WebSocket ticker price if available (more real-time than REST candle close)
                ws_price = self._ws_price
                exit_price = ws_price if ws_price is not None else price

                # Update UI tracking
                self.current_candle = {
                    "time": str(row.name) if hasattr(row, 'name') else str(now),
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
                        if eval_condition_group(row, self.entry_conditions, prev):
                            margin = capital * (position_size_pct / 100)
                            notional = margin * leverage
                            trade = {
                                "id": len(self.closed_trades) + len(self.open_trades) + 1,
                                "symbol": symbol,
                                "side": trade_side,
                                "entry_price": price,
                                "entry_time": str(now),
                                "notional": round(notional, 2),
                                "leverage": leverage,
                                "margin": round(margin, 2),
                                "best_price": price,  # for trailing SL
                            }
                            self.open_trades.append(trade)
                            self.in_trade = True
                            self.trades_today += 1
                            self.log_event("entry", f"ENTRY {trade_side} {symbol} @ ${price:,.2f} (${notional:,.0f} notional)")

                            if callback:
                                await callback({"type": "entry", "trade": trade, "price": price})

                            self._save_state()
                else:
                    for trade in self.open_trades[:]:
                        ep = trade["entry_price"]
                        # Use real-time WS price for SL/TP, REST price for signals
                        check_price = exit_price
                        if trade_side == "LONG":
                            pnl_pct = (check_price - ep) / ep * 100
                        else:
                            pnl_pct = (ep - check_price) / ep * 100

                        lev_pnl_pct = pnl_pct * leverage
                        trade_pnl = trade["notional"] * (pnl_pct / 100)
                        exit_reason = None

                        # Trailing SL: track best price and trail from there
                        if trailing_sl_pct > 0:
                            best = trade.get("best_price", ep)
                            if trade_side == "LONG":
                                best = max(best, check_price)
                                trail_trigger = best * (1 - trailing_sl_pct / 100)
                                if check_price <= trail_trigger and pnl_pct > 0:
                                    exit_reason = f"Trailing SL ({trailing_sl_pct}%)"
                            else:
                                best = min(best, check_price)
                                trail_trigger = best * (1 + trailing_sl_pct / 100)
                                if check_price >= trail_trigger and pnl_pct > 0:
                                    exit_reason = f"Trailing SL ({trailing_sl_pct}%)"
                            trade["best_price"] = best

                        if not exit_reason and sl_pct > 0 and lev_pnl_pct <= -sl_pct:
                            exit_reason = "Stop Loss"
                        elif not exit_reason and tp_pct > 0 and lev_pnl_pct >= tp_pct:
                            exit_reason = "Take Profit"
                        elif not exit_reason and lev_pnl_pct <= -90:
                            exit_reason = "Liquidation"
                        elif not exit_reason and eval_condition_group(row, self.exit_conditions, prev):
                            exit_reason = "Signal Exit"

                        if exit_reason:
                            self.total_pnl += trade_pnl
                            self.daily_pnl += trade_pnl
                            capital += trade_pnl
                            capital = max(capital, 0)

                            closed = {
                                **trade,
                                "exit_price": check_price,
                                "exit_time": str(now),
                                "pnl": round(trade_pnl, 2),
                                "exit_reason": exit_reason,
                            }
                            self.closed_trades.append(closed)
                            self.open_trades.remove(trade)
                            if not self.open_trades:
                                self.in_trade = False

                            emoji = "+" if trade_pnl >= 0 else ""
                            self.log_event("exit", f"EXIT {exit_reason} {symbol} @ ${check_price:,.2f} | P&L: {emoji}${trade_pnl:,.2f}")

                            if callback:
                                await callback({
                                    "type": "exit", "trade": closed,
                                    "total_pnl": round(self.total_pnl, 2),
                                })

                            self._save_state()

                # Periodic state save
                self._save_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[PAPER] Error: {e}")
                self.log_event("error", str(e))
                if callback:
                    await callback({"type": "error", "message": str(e)})

            await asyncio.sleep(poll_interval)

        # Final state save on stop
        self.log_event("stop", "Paper Trading Engine Stopped")
        await self._stop_ws_feed()
        self._save_state()

    def stop(self):
        self.running = False

    def get_status(self) -> dict:
        return {
            "running": self.running,
            "run_id": self.run_id or "",
            "mode": "paper",
            "strategy_name": self.strategy.get("run_name", ""),
            "run_name": self.strategy.get("run_name", ""),
            "symbol": self.strategy.get("symbol", ""),
            "leverage": self.strategy.get("leverage", 10),
            "trade_side": self.strategy.get("trade_side", "LONG"),
            "side": self.strategy.get("trade_side", "LONG"),
            "open_positions": len(self.open_trades),
            "closed_trades": len(self.closed_trades),
            "trades_today": self.trades_today,
            "total_pnl": round(self.total_pnl, 2),
            "daily_pnl": round(self.daily_pnl, 2),
            "open_trades": self.open_trades,
            "recent_trades": self.closed_trades[-10:],
            "current_candle": self.current_candle,
            "current_indicators": self.current_indicators,
            "event_log": [
                {"time": e["time"].strftime("%H:%M:%S IST") if isinstance(e["time"], datetime) else str(e["time"]),
                 "type": e["type"], "message": e["message"]}
                for e in self.event_log[-50:]
            ],
        }
