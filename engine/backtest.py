"""
engine/backtest.py — CryptoForge Backtest Engine
Perpetual futures backtesting with leverage, funding rates, and liquidation.
"""

import os
import re
import sys
from datetime import datetime, time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _safe_field(name) -> str:
    """Strip non-printable/control characters from user-supplied field names before logging."""
    return re.sub(r"[^\x20-\x7E]", "?", str(name))[:80]


import config
from engine.indicators import compute_dynamic_indicators


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
_PRICE_MAP = {
    "current_open": "open",
    "current_high": "high",
    "current_low": "low",
    "current_close": "close",
    "current_volume": "volume",
}


def _resolve_value(row, key, cond=None):
    """Map a condition field name to the actual DataFrame column value."""
    if key in _PRICE_MAP:
        return row.get(_PRICE_MAP[key])
    if key == "number":
        return float(cond.get("right_number_value", 0)) if cond else 0.0
    if key in ("true", "false"):
        return key == "true"
    return row.get(key)


def eval_condition(row, cond, prev_row=None):
    left = cond["left"]
    op = cond["operator"]

    # ── Time of Day ──
    if left == "Time_Of_Day":
        ts = row.name if hasattr(row, "name") else None
        if ts is None:
            return False
        cur_time = ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)
        cmp_time = cond.get("right_time", "09:30")
        if op == "is_above":
            return cur_time > cmp_time
        elif op == "is_below":
            return cur_time < cmp_time
        elif op == ">=":
            return cur_time >= cmp_time
        elif op == "<=":
            return cur_time <= cmp_time
        return False

    # ── Day of Week ──
    if left == "Day_Of_Week":
        ts = row.name if hasattr(row, "name") else None
        if ts is None:
            return False
        day_name = ts.strftime("%A") if hasattr(ts, "strftime") else ""
        days = cond.get("right_days", [])
        if op == "contains":
            return day_name in days
        elif op == "not_contains":
            return day_name not in days
        return False

    # Standard indicator conditions
    lv = _resolve_value(row, left)

    # ── Boolean operators — handle before RHS resolution ──
    if op == "is_true":
        if lv is None:
            return False
        return bool(lv)
    elif op == "is_false":
        if lv is None:
            return False
        return not bool(lv)

    r = cond.get("right")
    rv = _resolve_value(row, r, cond)

    try:
        if lv is None or rv is None:
            if lv is None:
                print(
                    f"[CONDITION] ⚠ Left '{_safe_field(left)}' not found in row — available columns: {sorted([c for c in row.index if not c.startswith('_')])[:20]}"
                )
            if rv is None and r not in ("true", "false", "number"):
                print(f"[CONDITION] ⚠ Right '{_safe_field(r)}' not found in row")
            return False
        if isinstance(lv, float) and pd.isna(lv):
            return False
        if not isinstance(rv, bool) and isinstance(rv, float) and pd.isna(rv):
            return False
    except Exception:
        return False

    try:
        lv_f = float(lv)
        rv_f = float(rv)
    except (TypeError, ValueError):
        return False

    # Crossover detection
    if op == "crosses_above":
        if prev_row is None:
            return lv_f > rv_f
        plv = _resolve_value(prev_row, left)
        prv = _resolve_value(prev_row, r, cond)
        try:
            plv_f = float(plv)
            prv_f = float(prv)
        except (TypeError, ValueError):
            return lv_f > rv_f
        return plv_f <= prv_f and lv_f > rv_f

    elif op == "crosses_below":
        if prev_row is None:
            return lv_f < rv_f
        plv = _resolve_value(prev_row, left)
        prv = _resolve_value(prev_row, r, cond)
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
    return False


def eval_condition_group(row, conditions, prev_row=None):
    if not conditions:
        # Fail-safe: empty conditions = no signal. Returning True would cause
        # the engine to enter a trade on every single candle with real money.
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


# Default conditions reference the 1m EMA column (interval suffix added by compute_dynamic_indicators).
# app.py injects "EMA_20_1m" into the indicators list whenever these defaults are used,
# so the column is guaranteed to exist in the dataframe.
DEFAULT_ENTRY_CONDITIONS = [{"left": "current_close", "operator": "is_above", "right": "EMA_20_1m", "connector": "AND"}]
DEFAULT_EXIT_CONDITIONS = [{"left": "current_close", "operator": "is_below", "right": "EMA_20_1m", "connector": "AND"}]


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
def run_backtest(df_raw, entry_conditions=None, exit_conditions=None, strategy_config=None):
    if entry_conditions is None:
        entry_conditions = DEFAULT_ENTRY_CONDITIONS
    if exit_conditions is None:
        exit_conditions = DEFAULT_EXIT_CONDITIONS
    sc = strategy_config or {}

    capital = float(sc.get("initial_capital", config.DEFAULT_CAPITAL))
    leverage = int(sc.get("leverage", 10))
    sl_pct = float(sc.get("stoploss_pct", 5))
    tp_pct = float(sc.get("target_profit_pct", 10))
    trail_pct = float(sc.get("trailing_sl_pct", 0))  # 0 = disabled
    max_tpd = int(sc.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
    indicators = sc.get("indicators", []) or []
    side = sc.get("trade_side", "LONG").upper()  # LONG or SHORT
    position_size_pct = float(sc.get("position_size_pct", 100))  # % of capital
    fee_pct = float(sc.get("fee_pct", 0.05))  # taker fee per side (0.05% default for Delta)

    # Compute indicators
    df = compute_dynamic_indicators(df_raw, indicators)

    # ── Diagnostic: log available columns & sample condition values ──
    df_cols = sorted([c for c in df.columns if not c.startswith("_")])
    print(f"[BACKTEST-DIAG] DataFrame rows: {len(df)}, columns ({len(df_cols)}): {df_cols[:40]}")
    print(f"[BACKTEST-DIAG] Entry conditions: {entry_conditions}")
    print(f"[BACKTEST-DIAG] Exit conditions: {exit_conditions}")
    # Check each condition field exists in DataFrame
    for label, conds in [("ENTRY", entry_conditions), ("EXIT", exit_conditions)]:
        for ci, c in enumerate(conds):
            left_key = c.get("left", "")
            right_key = c.get("right", "")
            op = c.get("operator", "")
            mapped_left = _PRICE_MAP.get(left_key, left_key)
            mapped_right = (
                _PRICE_MAP.get(right_key, right_key)
                if right_key not in ("number", "true", "false", "", None)
                else right_key
            )
            left_ok = left_key in ("Time_Of_Day", "Day_Of_Week") or mapped_left in df.columns
            right_ok = (
                right_key in ("number", "true", "false", "", None)
                or op in ("is_true", "is_false")
                or right_key in ("Time_Of_Day", "Day_Of_Week")
                or mapped_right in df.columns
            )
            if not left_ok:
                print(
                    f"[BACKTEST-DIAG] ⚠ {label}[{ci}] LEFT '{left_key}' (mapped: '{mapped_left}') NOT FOUND in DataFrame!"
                )
            if not right_ok:
                print(
                    f"[BACKTEST-DIAG] ⚠ {label}[{ci}] RIGHT '{right_key}' (mapped: '{mapped_right}') NOT FOUND in DataFrame!"
                )
            # Sample values from midpoint of data
            if len(df) > 10:
                mid = len(df) // 2
                sample_row = df.iloc[mid]
                lv = _resolve_value(sample_row, left_key)
                rv = _resolve_value(sample_row, right_key, c) if right_key else None
                print(f"[BACKTEST-DIAG] {label}[{ci}] '{left_key}' {op} '{right_key}' → sample LV={lv}, RV={rv}")

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
    peak_pnl_pct = 0.0  # for trailing SL
    _entry_true_count = 0  # diagnostic counter

    for i in range(2, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        prev_prev = df.iloc[i - 2]
        ts = df.index[i]
        current_date = ts.date() if hasattr(ts, "date") else ts

        # Reset daily trade count
        if current_date != last_trade_date:
            trades_today = 0
            last_trade_date = current_date

        price = float(row["close"])
        h = float(row.get("high", price))
        lo = float(row.get("low", price))

        if not in_trade:
            # Check entry (evaluate PREV candle to avoid look-ahead, enter at current open)
            if trades_today >= max_tpd:
                continue
            if eval_condition_group(prev, entry_conditions, prev_prev):
                _entry_true_count += 1
                in_trade = True
                entry_price = float(row["open"])
                entry_time = ts
                peak_pnl_pct = 0.0  # reset trailing tracker
                # Position size = (capital * position_size_pct/100) * leverage
                margin_used = capital * (position_size_pct / 100)
                entry_size = margin_used * leverage
                trades_today += 1
        else:
            # Check exit conditions using OHLC worst/best-case
            if side == "LONG":
                worst_pnl_pct = (lo - entry_price) / entry_price * 100
                best_pnl_pct = (h - entry_price) / entry_price * 100
                pnl_pct = (price - entry_price) / entry_price * 100
            else:
                worst_pnl_pct = (entry_price - h) / entry_price * 100
                best_pnl_pct = (entry_price - lo) / entry_price * 100
                pnl_pct = (entry_price - price) / entry_price * 100

            # P&L as price percentage (unleveraged)
            trade_pnl = entry_size * (pnl_pct / 100)

            # Track peak price move for trailing SL
            if best_pnl_pct > peak_pnl_pct:
                peak_pnl_pct = best_pnl_pct

            exit_reason = None

            # SL/TP are PRICE percentages (unleveraged), matching CryptoBot behavior
            # Trailing stop-loss (triggers once price move exceeds trail_pct then pulls back)
            if trail_pct > 0 and peak_pnl_pct >= trail_pct and worst_pnl_pct <= (peak_pnl_pct - trail_pct):
                exit_reason = "Trailing SL"
            # Stop-loss (worst-case intra-candle price move)
            elif sl_pct > 0 and worst_pnl_pct <= -sl_pct:
                exit_reason = "Stop Loss"
            # Take profit (best-case intra-candle price move)
            elif tp_pct > 0 and best_pnl_pct >= tp_pct:
                exit_reason = "Take Profit"
            # Liquidation check (leveraged — this IS an account-level concept)
            elif (worst_pnl_pct * leverage) <= config.LIQUIDATION_THRESHOLD:
                exit_reason = "Liquidation"
            # Exit conditions met (evaluate prev candle, exit at current open)
            elif eval_condition_group(prev, exit_conditions, prev_prev):
                exit_reason = "Signal Exit"

            if exit_reason:
                # Calculate actual exit price based on exit reason
                if exit_reason == "Signal Exit":
                    price = float(row["open"])
                elif exit_reason == "Stop Loss":
                    # SL at exact price percentage
                    if side == "LONG":
                        price = entry_price * (1 - sl_pct / 100)
                    else:
                        price = entry_price * (1 + sl_pct / 100)
                elif exit_reason == "Take Profit":
                    # TP at exact price percentage
                    if side == "LONG":
                        price = entry_price * (1 + tp_pct / 100)
                    else:
                        price = entry_price * (1 - tp_pct / 100)
                elif exit_reason == "Trailing SL":
                    # Trailing triggers when price pulls back trail_pct from peak
                    trail_exit_pct = peak_pnl_pct - trail_pct
                    if side == "LONG":
                        price = entry_price * (1 + trail_exit_pct / 100)
                    else:
                        price = entry_price * (1 - trail_exit_pct / 100)
                elif exit_reason == "Liquidation":
                    # Liquidation is leveraged (account-level)
                    liq_price_pct = config.LIQUIDATION_THRESHOLD / leverage
                    if side == "LONG":
                        price = entry_price * (1 + liq_price_pct / 100)
                    else:
                        price = entry_price * (1 - liq_price_pct / 100)

                # Recalculate P&L from actual exit price
                if side == "LONG":
                    pnl_pct = (price - entry_price) / entry_price * 100
                else:
                    pnl_pct = (entry_price - price) / entry_price * 100
                trade_pnl = entry_size * (pnl_pct / 100)
                # Calculate fees (entry + exit)
                entry_fee = entry_size * (fee_pct / 100)
                exit_fee = entry_size * (1 + pnl_pct / 100) * (fee_pct / 100)
                trade_fees = entry_fee + exit_fee
                trade_pnl -= trade_fees
                total_fees += trade_fees
                cum_pnl += trade_pnl
                capital += trade_pnl  # compound capital
                tid += 1
                trades.append(
                    _mk(
                        tid,
                        entry_time,
                        ts,
                        entry_price,
                        price,
                        trade_pnl,
                        exit_reason,
                        cum_pnl,
                        side=side,
                        leverage=leverage,
                        size=round(entry_size, 2),
                    )
                )
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
        trades.append(
            _mk(
                tid,
                entry_time,
                ts,
                entry_price,
                price,
                trade_pnl,
                "End of Data",
                cum_pnl,
                side=side,
                leverage=leverage,
                size=round(entry_size, 2),
            )
        )

    # ── Stats ─────────────────────────────────────────────────────
    total_trades = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]

    win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0
    avg_win = (sum(t["pnl"] for t in wins) / len(wins)) if wins else 0
    avg_loss = (sum(t["pnl"] for t in losses) / len(losses)) if losses else 0
    profit_factor = (
        sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses))
        if losses and sum(t["pnl"] for t in losses) != 0
        else 0
    )

    # Expectancy = (win_rate × avg_win) − (loss_rate × avg_loss)
    loss_rate = (len(losses) / total_trades * 100) if total_trades > 0 else 0
    expectancy = (win_rate / 100 * avg_win) - (loss_rate / 100 * abs(avg_loss)) if total_trades > 0 else 0

    # Max drawdown
    initial_capital = float(sc.get("initial_capital", config.DEFAULT_CAPITAL))
    peak = equity_curve[0]["value"] if equity_curve else capital
    max_dd = 0
    max_dd_dollar = 0
    for eq in equity_curve:
        val = eq["value"]
        if val > peak:
            peak = val
        dd_pct = (peak - val) / peak * 100
        dd_dollar = peak - val
        if dd_pct > max_dd:
            max_dd = dd_pct
            max_dd_dollar = dd_dollar

    # ── Sharpe Ratio (annualized, crypto = 365 days) ──────────
    sharpe_ratio = 0.0
    calmar_ratio = 0.0
    if len(equity_curve) > 1:
        eq_values = np.array([e["value"] for e in equity_curve])
        # Daily returns: group equity curve by date, take last value per day
        eq_dates = {}
        for e in equity_curve:
            d = e["time"][:10]
            eq_dates[d] = e["value"]
        daily_vals = list(eq_dates.values())
        if len(daily_vals) > 1:
            daily_returns = np.diff(daily_vals) / np.array(daily_vals[:-1])
            mean_daily = np.mean(daily_returns)
            std_daily = np.std(daily_returns, ddof=1)
            if std_daily > 0:
                sharpe_ratio = (mean_daily / std_daily) * np.sqrt(365)  # crypto = 365 trading days

            # Calmar Ratio = annualized return / max drawdown
            total_days = len(daily_vals)
            if total_days > 0 and max_dd > 0:
                total_return_dec = (daily_vals[-1] - daily_vals[0]) / daily_vals[0]
                ann_return = ((1 + total_return_dec) ** (365 / max(total_days, 1))) - 1
                calmar_ratio = (ann_return * 100) / max_dd

    # Average trade duration
    avg_duration_str = ""
    if trades:
        durations = []
        for t in trades:
            try:
                entry_dt = datetime.strptime(t["entry_time"][:19], "%Y-%m-%d %H:%M:%S")
                exit_dt = datetime.strptime(t["exit_time"][:19], "%Y-%m-%d %H:%M:%S")
                durations.append((exit_dt - entry_dt).total_seconds())
            except:
                pass
        if durations:
            avg_secs = sum(durations) / len(durations)
            if avg_secs < 3600:
                avg_duration_str = f"{avg_secs / 60:.0f}m"
            elif avg_secs < 86400:
                avg_duration_str = f"{avg_secs / 3600:.1f}h"
            else:
                avg_duration_str = f"{avg_secs / 86400:.1f}d"

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

    # Yearly breakdown
    yearly = {}
    for t in trades:
        year_key = t["entry_time"][:4]
        if year_key not in yearly:
            yearly[year_key] = {"year": year_key, "trades": 0, "pnl": 0, "wins": 0, "losses": 0}
        yearly[year_key]["trades"] += 1
        yearly[year_key]["pnl"] += t["pnl"]
        if t["pnl"] > 0:
            yearly[year_key]["wins"] += 1
        else:
            yearly[year_key]["losses"] += 1
    yearly_list = sorted(yearly.values(), key=lambda x: x["year"])
    for y in yearly_list:
        y["pnl"] = round(y["pnl"], 2)

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
        "max_drawdown_dollar": round(max_dd_dollar, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "calmar_ratio": round(calmar_ratio, 2),
        "expectancy": round(expectancy, 2),
        "avg_trade_duration": avg_duration_str,
        "total_return_pct": round(cum_pnl / initial_capital * 100, 2),
        "initial_capital": initial_capital,
        "final_capital": round(capital, 2),
        "total_fees": round(total_fees, 2),
        "fee_pct": fee_pct,
        "trailing_sl_pct": trail_pct,
        "leverage": leverage,
        "side": side,
    }

    # Downsample equity curve for large datasets
    eq_out = equity_curve
    if len(equity_curve) > 500:
        step = len(equity_curve) // 500
        eq_out = equity_curve[::step]

    print(f"[BACKTEST-DIAG] Entry condition matched on {_entry_true_count} candles, total trades: {total_trades}")

    # Build diagnostic info for 0-trade results
    diagnostics = None
    if total_trades == 0:
        diag_items = []
        diag_items.append(f"DataFrame: {len(df)} rows")
        diag_items.append(f"Entry conditions matched: {_entry_true_count} times")
        for label, conds in [("Entry", entry_conditions), ("Exit", exit_conditions)]:
            for ci, c in enumerate(conds):
                left_key = c.get("left", "")
                right_key = c.get("right", "")
                op = c.get("operator", "")
                mapped_left = _PRICE_MAP.get(left_key, left_key)
                mapped_right = (
                    _PRICE_MAP.get(right_key, right_key)
                    if right_key not in ("number", "true", "false", "", None)
                    else right_key
                )
                left_in_df = left_key in ("Time_Of_Day", "Day_Of_Week") or mapped_left in df.columns
                right_in_df = (
                    right_key in ("number", "true", "false", "", None)
                    or op in ("is_true", "is_false")
                    or right_key in ("Time_Of_Day", "Day_Of_Week")
                    or mapped_right in df.columns
                )
                status = "OK" if (left_in_df and right_in_df) else "MISSING"
                if not left_in_df:
                    status = f"LEFT '{left_key}' NOT IN DF"
                if not right_in_df:
                    status = f"RIGHT '{right_key}' NOT IN DF"
                diag_items.append(f"{label}[{ci}]: {left_key} {op} {right_key} → {status}")
        diagnostics = diag_items

    result = {
        "status": "success",
        "stats": stats,
        "trades": trades,
        "equity": eq_out,
        "monthly": monthly_list,
        "yearly": yearly_list,
        "day_of_week": list(dow_map.values()),
    }
    if diagnostics:
        result["diagnostics"] = diagnostics
    return result
