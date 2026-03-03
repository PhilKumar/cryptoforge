"""
engine/live.py — CryptoForge Live Trading Engine
Perpetual futures auto-trading via Delta Exchange.
"""

import asyncio
from datetime import datetime
from typing import Callable, Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from engine.indicators import compute_dynamic_indicators
from engine.backtest import eval_condition_group
import config


class LiveEngine:
    """Live perpetual futures trading engine with Delta Exchange."""

    def __init__(self, broker):
        self.broker = broker
        self.running = False
        self.strategy = {}
        self.entry_conditions = []
        self.exit_conditions = []
        self.deploy_config = {}
        self.open_trades = []
        self.closed_trades = []
        self.total_pnl = 0.0
        self.trades_today = 0
        self._last_trade_date = None

    def configure(self, strategy: dict, entry_conditions: list,
                  exit_conditions: list, deploy_config: dict = None):
        self.strategy = strategy
        self.entry_conditions = entry_conditions
        self.exit_conditions = exit_conditions
        self.deploy_config = deploy_config or {}

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

        # Get product ID
        product = self.broker.get_product_by_symbol(symbol)
        product_id = product.get("id") if product else None

        # Set leverage
        if product_id:
            self.broker.set_leverage(product_id, leverage)

        if callback:
            await callback({"type": "started", "symbol": symbol, "leverage": leverage})

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

                # Check open positions
                in_trade = len(self.open_trades) > 0

                if not in_trade:
                    if self.trades_today < max_tpd:
                        if eval_condition_group(row, self.entry_conditions, prev):
                            # Calculate position size from capital (matching paper engine)
                            margin = capital * (position_size_pct / 100)
                            notional = margin * leverage
                            # Size in contracts (notional / price, rounded to int)
                            size = max(1, int(notional / price))
                            side = "buy" if trade_side == "LONG" else "sell"

                            if product_id:
                                result = self.broker.place_order(
                                    product_id=product_id,
                                    size=size, side=side,
                                    order_type="market_order",
                                    leverage=leverage,
                                )
                                order_id = result.get("id", "sim")
                            else:
                                order_id = "sim"

                            trade = {
                                "id": len(self.closed_trades) + 1,
                                "symbol": symbol,
                                "side": trade_side,
                                "entry_price": price,
                                "entry_time": str(now),
                                "size": size,
                                "notional": round(notional, 2),
                                "margin": round(margin, 2),
                                "leverage": leverage,
                                "order_id": order_id,
                            }
                            self.open_trades.append(trade)
                            self.trades_today += 1

                            if callback:
                                await callback({"type": "entry", "trade": trade, "price": price})
                else:
                    # Check exit
                    for trade in self.open_trades[:]:
                        ep = trade["entry_price"]
                        if trade_side == "LONG":
                            pnl_pct = (price - ep) / ep * 100
                        else:
                            pnl_pct = (ep - price) / ep * 100

                        lev_pnl_pct = pnl_pct * leverage
                        exit_reason = None

                        if sl_pct > 0 and lev_pnl_pct <= -sl_pct:
                            exit_reason = "Stop Loss"
                        elif tp_pct > 0 and lev_pnl_pct >= tp_pct:
                            exit_reason = "Take Profit"
                        elif eval_condition_group(row, self.exit_conditions, prev):
                            exit_reason = "Signal Exit"

                        if exit_reason:
                            # Place exit order
                            close_side = "sell" if trade_side == "LONG" else "buy"
                            if product_id:
                                self.broker.place_order(
                                    product_id=product_id,
                                    size=trade["size"],
                                    side=close_side,
                                    order_type="market_order",
                                    reduce_only=True,
                                )

                            # PnL based on notional value (no double-counting leverage)
                            trade_pnl = trade["notional"] * (pnl_pct / 100)
                            self.total_pnl += trade_pnl
                            capital += trade_pnl  # Update capital for next trade sizing

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
                                await callback({"type": "exit", "trade": closed, "total_pnl": self.total_pnl})

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[LIVE] Error: {e}")
                if callback:
                    await callback({"type": "error", "message": str(e)})

            await asyncio.sleep(poll_interval)

    def stop(self):
        self.running = False
        # Note: open positions remain on the exchange.
        # The emergency-stop endpoint or manual intervention should be used
        # to close positions if needed.

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
