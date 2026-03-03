"""
engine/backtest.py — CryptoForge Backtest Engine
Perpetual futures backtesting with leverage, funding rates, and liquidation.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time, date
from typing import List, Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from engine.indicators import compute_dynamic_indicators
import config


# ── Time Parser ────────────────────────────────────────────────────
def _parse_time(val):
    if isinstance(val, time):
        return val
    if not isinstance(val, str):
        return time(0, 0)
    s = val.strip()
    parts = s.split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    return time(h % 24, m)


# ── Condition Evaluator ────────────────────────────────────────────
def eval_condition(row, cond, prev_row=None):
    left = cond["left"]
    op = cond["operator"]

    # Standard indicator conditions
    lv = row.get("close") if left == "current_close" else row.get(left)
    r = cond["right"]

    if r == "current_close":
        rv = row.get("close")
    elif r == "number":
        rv = float(cond.get("right_number_value", 0))
    elif r in ("true", "false"):
        rv = r == "true"
    else:
        rv = row.get(r)

    try:
        if lv is None or rv is None:
            return False
        if isinstance(lv, float) and pd.isna(lv):
            return False
        if not isinstance(rv, bool) and isinstance(rv, float) and pd.isna(rv):
            return False
    except:
        return False

    lv_f = float(lv)
    rv_f = float(rv)

    # Crossover detection
    if op == "crosses_above":
        if prev_row is None:
            return lv_f > rv_f
        plv = prev_row.get("close") if left == "current_close" else prev_row.get(left)
        prv = (prev_row.get("close") if r == "current_close" else
               (float(cond.get("right_number_value", 0)) if r == "number" else prev_row.get(r)))
        try:
            plv_f = float(plv)
            prv_f = float(prv)
        except (TypeError, ValueError):
            return lv_f > rv_f
        return plv_f <= prv_f and lv_f > rv_f

    elif op == "crosses_below":
        if prev_row is None:
            return lv_f < rv_f
        plv = prev_row.get("close") if left == "current_close" else prev_row.get(left)
        prv = (prev_row.get("close") if r == "current_close" else
               (float(cond.get("right_number_value", 0)) if r == "number" else prev_row.get(r)))
        try:
            plv_f = float(plv)
            prv_f = float(prv)
        except (TypeError, ValueError):
            return lv_f < rv_f
        return plv_f >= prv_f and lv_f < rv_f

    if op == "is_above":
        return lv_f > rv_f
    elif op == "is_below":
        return lv_f < rv_f
    elif op == "==":
        return bool(lv) == rv if isinstance(rv, bool) else lv_f == rv_f
    elif op == ">=":
        return lv_f >= rv_f
    elif op == "<=":
        return lv_f <= rv_f
    elif op == "is_true":
        return bool(lv)
    elif op == "is_false":
        return not bool(lv)
    return False


def eval_condition_group(row, conditions, prev_row=None):
    if not conditions:
        return False
    result = eval_condition(row, conditions[0], prev_row)
    for c in conditions[1:]:
        v = eval_condition(row, c, prev_row)
        conn = c.get("logic", c.get("connector", "AND")).upper()
        if conn in ("AND", "IF"):
            result = result and v
        elif conn == "OR":
            result = result or v
    return result


DEFAULT_ENTRY_CONDITIONS = [
    {"left": "current_close", "operator": "is_above", "right": "EMA_20_5m", "connector": "AND"}
]
DEFAULT_EXIT_CONDITIONS = [
    {"left": "current_close", "operator": "is_below", "right": "EMA_20_5m", "connector": "AND"}
]


# ── Trade Helpers ──────────────────────────────────────────────────
def _mk(id_, et, xt, ep, xp, pnl, reason, cum, side="LONG", leverage=1, size=1):
    return {
        "id": id_,
        "entry_time": str(et)[:19],
        "exit_time": str(xt)[:19],
        "entry_price": round(ep, 2),
        "exit_price": round(xp, 2),
        "pnl": round(pnl, 2),
        "exit_reason": reason,
        "cumulative": round(cum, 2),
        "side": side,
        "leverage": leverage,
        "size": size,
    }


# ── Backtest Runner ────────────────────────────────────────────────
def run_backtest(df_raw, entry_conditions=None, exit_conditions=None,
                 strategy_config=None):
    if entry_conditions is None:
        entry_conditions = DEFAULT_ENTRY_CONDITIONS
    if exit_conditions is None:
        exit_conditions = DEFAULT_EXIT_CONDITIONS
    sc = strategy_config or {}

    capital = float(sc.get("initial_capital", config.DEFAULT_CAPITAL))
    leverage = int(sc.get("leverage", 10))
    sl_pct = float(sc.get("stoploss_pct", 5))
    tp_pct = float(sc.get("target_profit_pct", 10))
    max_tpd = int(sc.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
    indicators = sc.get("indicators", []) or []
    side = sc.get("trade_side", "LONG").upper()  # LONG or SHORT
    position_size_pct = float(sc.get("position_size_pct", 100))  # % of capital
    fee_pct = float(sc.get("fee_pct", 0.05))  # taker fee per side (0.05% default for Delta)

    # Compute indicators
    df = compute_dynamic_indicators(df_raw, indicators)

    trades = []
    equity_curve = []
    cum_pnl = 0.0
    total_fees = 0.0
    tid = 0
    in_trade = False
    entry_price = 0
    entry_time = None
    entry_size = 0  # notional position size in USD
    trades_today = 0
    last_trade_date = None

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        ts = df.index[i]
        current_date = ts.date() if hasattr(ts, 'date') else ts

        # Reset daily trade count
        if current_date != last_trade_date:
            trades_today = 0
            last_trade_date = current_date

        price = float(row["close"])

        if not in_trade:
            # Check entry
            if trades_today >= max_tpd:
                continue
            if eval_condition_group(row, entry_conditions, prev):
                in_trade = True
                entry_price = price
                entry_time = ts
                # Position size = (capital * position_size_pct/100) * leverage
                margin_used = capital * (position_size_pct / 100)
                entry_size = margin_used * leverage
                trades_today += 1
        else:
            # Check exit conditions
            if side == "LONG":
                pnl_pct = (price - entry_price) / entry_price * 100
            else:
                pnl_pct = (entry_price - price) / entry_price * 100

            # Leveraged P&L
            lev_pnl_pct = pnl_pct * leverage
            trade_pnl = entry_size * (pnl_pct / 100)

            exit_reason = None

            # Stop-loss
            if sl_pct > 0 and lev_pnl_pct <= -sl_pct:
                exit_reason = "Stop Loss"
            # Take profit
            elif tp_pct > 0 and lev_pnl_pct >= tp_pct:
                exit_reason = "Take Profit"
            # Liquidation check (simplified)
            elif lev_pnl_pct <= -90:
                exit_reason = "Liquidation"
            # Exit conditions met
            elif eval_condition_group(row, exit_conditions, prev):
                exit_reason = "Signal Exit"

            if exit_reason:
                # Calculate fees (entry + exit)
                entry_fee = entry_size * (fee_pct / 100)
                exit_fee = entry_size * (1 + pnl_pct / 100) * (fee_pct / 100)
                trade_fees = entry_fee + exit_fee
                trade_pnl -= trade_fees
                total_fees += trade_fees
                cum_pnl += trade_pnl
                capital += trade_pnl  # compound capital
                tid += 1
                trades.append(_mk(
                    tid, entry_time, ts, entry_price, price,
                    trade_pnl, exit_reason, cum_pnl,
                    side=side, leverage=leverage,
                    size=round(entry_size, 2),
                ))
                in_trade = False

        equity_curve.append({"time": str(ts)[:19], "value": round(capital, 2)})

    # Close open trade at last candle
    if in_trade and len(df) > 0:
        price = float(df.iloc[-1]["close"])
        ts = df.index[-1]
        if side == "LONG":
            pnl_pct = (price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - price) / entry_price * 100
        trade_pnl = entry_size * (pnl_pct / 100)
        # Fees for end-of-data close
        entry_fee = entry_size * (fee_pct / 100)
        exit_fee = entry_size * (1 + pnl_pct / 100) * (fee_pct / 100)
        trade_fees = entry_fee + exit_fee
        trade_pnl -= trade_fees
        total_fees += trade_fees
        cum_pnl += trade_pnl
        capital += trade_pnl
        tid += 1
        trades.append(_mk(
            tid, entry_time, ts, entry_price, price,
            trade_pnl, "End of Data", cum_pnl,
            side=side, leverage=leverage,
            size=round(entry_size, 2),
        ))

    # ── Stats ─────────────────────────────────────────────────────
    total_trades = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]

    win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0
    avg_win = (sum(t["pnl"] for t in wins) / len(wins)) if wins else 0
    avg_loss = (sum(t["pnl"] for t in losses) / len(losses)) if losses else 0
    profit_factor = (sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses))
                     if losses and sum(t["pnl"] for t in losses) != 0 else 0)

    # Max drawdown
    peak = capital
    max_dd = 0
    for eq in equity_curve:
        val = eq["value"]
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Monthly breakdown
    monthly = {}
    for t in trades:
        month_key = t["entry_time"][:7]  # YYYY-MM
        if month_key not in monthly:
            monthly[month_key] = {"month": month_key, "trades": 0, "pnl": 0, "wins": 0, "losses": 0}
        monthly[month_key]["trades"] += 1
        monthly[month_key]["pnl"] += t["pnl"]
        if t["pnl"] > 0:
            monthly[month_key]["wins"] += 1
        else:
            monthly[month_key]["losses"] += 1
    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["pnl"] = round(m["pnl"], 2)

    # Day of week breakdown
    dow_map = {}
    for t in trades:
        try:
            dt = datetime.strptime(t["entry_time"][:10], "%Y-%m-%d")
            day = dt.strftime("%A")
        except:
            day = "Unknown"
        if day not in dow_map:
            dow_map[day] = {"day": day, "trades": 0, "pnl": 0}
        dow_map[day]["trades"] += 1
        dow_map[day]["pnl"] += t["pnl"]
    for d in dow_map.values():
        d["pnl"] = round(d["pnl"], 2)

    stats = {
        "total_trades": total_trades,
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate": round(win_rate, 1),
        "total_pnl": round(cum_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown": round(max_dd, 2),
        "total_return_pct": round(cum_pnl / float(sc.get("initial_capital", config.DEFAULT_CAPITAL)) * 100, 2),
        "initial_capital": float(sc.get("initial_capital", config.DEFAULT_CAPITAL)),
        "final_capital": round(capital, 2),
        "total_fees": round(total_fees, 2),
        "fee_pct": fee_pct,
        "leverage": leverage,
        "side": side,
    }

    # Downsample equity curve for large datasets
    eq_out = equity_curve
    if len(equity_curve) > 500:
        step = len(equity_curve) // 500
        eq_out = equity_curve[::step]

    return {
        "status": "success",
        "stats": stats,
        "trades": trades,
        "equity": eq_out,
        "monthly": monthly_list,
        "day_of_week": list(dow_map.values()),
    }
