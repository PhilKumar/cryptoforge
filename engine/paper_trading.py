"""
engine/paper_trading.py — CryptoForge Paper Trading Engine
Simulated futures trading using real-time market data from Delta Exchange.
"""

import asyncio
from datetime import datetime
from typing import Callable
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from engine.indicators import compute_dynamic_indicators
from engine.backtest import eval_condition_group
import config


class PaperTradingEngine:
    """Paper trading engine — uses real market data, simulated orders."""

    def __init__(self, broker):
        self.broker = broker
        self.running = False
        self.strategy = {}
        self.entry_conditions = []
        self.exit_conditions = []
        self.open_trades = []
        self.closed_trades = []
        self.total_pnl = 0.0
        self.trades_today = 0
        self._last_trade_date = None

    def configure(self, strategy: dict, entry_conditions: list,
                  exit_conditions: list):
        self.strategy = strategy
        self.entry_conditions = entry_conditions
        self.exit_conditions = exit_conditions

    async def start(self, callback: Callable = None):
        self.running = True
        self.open_trades = []
        self.closed_trades = []
        self.total_pnl = 0.0
        self.trades_today = 0

        symbol = self.strategy.get("symbol", "BTCUSDT")
        leverage = int(self.strategy.get("leverage", 10))
        poll_interval = int(self.strategy.get("poll_interval", 30))
        max_tpd = int(self.strategy.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
        indicators = self.strategy.get("indicators", [])
        trade_side = self.strategy.get("trade_side", "LONG").upper()
        sl_pct = float(self.strategy.get("stoploss_pct", 5))
        tp_pct = float(self.strategy.get("target_profit_pct", 10))
        capital = float(self.strategy.get("initial_capital", config.DEFAULT_CAPITAL))
        position_size_pct = float(self.strategy.get("position_size_pct", 100))
        candle_interval = self.strategy.get("candle_interval", "5m")

        if callback:
            await callback({"type": "started", "symbol": symbol, "mode": "paper"})

        while self.running:
            try:
                now = datetime.utcnow()
                today = now.date()

                if today != self._last_trade_date:
                    self.trades_today = 0
                    self._last_trade_date = today

                # Fetch enough candle history for indicators (3 days)
                from datetime import timedelta
                lookback = now - timedelta(days=3)
                df = await self.broker.async_get_candles(symbol, resolution=candle_interval,
                                             start=lookback.strftime("%Y-%m-%d"),
                                             end=now.strftime("%Y-%m-%d"))
                if df.empty:
                    await asyncio.sleep(poll_interval)
                    continue

                df = compute_dynamic_indicators(df, indicators)
                row = df.iloc[-1]
                prev = df.iloc[-2] if len(df) > 1 else row
                price = float(row["close"])

                in_trade = len(self.open_trades) > 0

                if not in_trade:
                    if self.trades_today < max_tpd:
                        if eval_condition_group(row, self.entry_conditions, prev):
                            margin = capital * (position_size_pct / 100)
                            notional = margin * leverage
                            trade = {
                                "id": len(self.closed_trades) + 1,
                                "symbol": symbol,
                                "side": trade_side,
                                "entry_price": price,
                                "entry_time": str(now),
                                "notional": round(notional, 2),
                                "leverage": leverage,
                                "margin": round(margin, 2),
                            }
                            self.open_trades.append(trade)
                            self.trades_today += 1

                            if callback:
                                await callback({"type": "entry", "trade": trade, "price": price})
                else:
                    for trade in self.open_trades[:]:
                        ep = trade["entry_price"]
                        if trade_side == "LONG":
                            pnl_pct = (price - ep) / ep * 100
                        else:
                            pnl_pct = (ep - price) / ep * 100

                        lev_pnl_pct = pnl_pct * leverage
                        trade_pnl = trade["notional"] * (pnl_pct / 100)
                        exit_reason = None

                        if sl_pct > 0 and lev_pnl_pct <= -sl_pct:
                            exit_reason = "Stop Loss"
                        elif tp_pct > 0 and lev_pnl_pct >= tp_pct:
                            exit_reason = "Take Profit"
                        elif lev_pnl_pct <= -90:
                            exit_reason = "Liquidation"
                        elif eval_condition_group(row, self.exit_conditions, prev):
                            exit_reason = "Signal Exit"

                        if exit_reason:
                            self.total_pnl += trade_pnl
                            capital += trade_pnl
                            capital = max(capital, 0)  # Prevent negative capital
                            closed = {
                                **trade,
                                "exit_price": price,
                                "exit_time": str(now),
                                "pnl": round(trade_pnl, 2),
                                "exit_reason": exit_reason,
                            }
                            self.closed_trades.append(closed)
                            self.open_trades.remove(trade)

                            if callback:
                                await callback({
                                    "type": "exit", "trade": closed,
                                    "total_pnl": round(self.total_pnl, 2),
                                })

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[PAPER] Error: {e}")
                if callback:
                    await callback({"type": "error", "message": str(e)})

            await asyncio.sleep(poll_interval)

    def stop(self):
        self.running = False

    def get_status(self) -> dict:
        return {
            "running": self.running,
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
            "open_trades": self.open_trades,
            "recent_trades": self.closed_trades[-10:],
        }
