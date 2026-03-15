"""
engine/backtest.py — CryptoForge Backtest Engine
Perpetual futures backtesting with leverage, funding rates, and liquidation.
Vectorized condition evaluation for 10-50x speed over row-by-row approach.
"""

import os
import re
import sys
import time as _time
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


# ── Condition Evaluator (row-by-row — kept as fallback) ────────────
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

    # ── Day of Week (evaluated in IST to match Delta Exchange India) ──
    if left == "Day_Of_Week":
        ts = row.name if hasattr(row, "name") else None
        if ts is None:
            return False
        # Convert UTC to IST for day-of-week check
        ist_ts = ts + pd.Timedelta(hours=5, minutes=30) if hasattr(ts, "strftime") else ts
        day_name = ist_ts.strftime("%A") if hasattr(ist_ts, "strftime") else ""
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
        try:
            if pd.isna(lv):
                return False
        except (TypeError, ValueError):
            pass
        return bool(lv)
    elif op == "is_false":
        if lv is None:
            return False
        try:
            if pd.isna(lv):
                return False
        except (TypeError, ValueError):
            pass
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


# ── Vectorized Condition Evaluation (10-50x faster) ───────────────
def _vec_single_condition(df, cond):
    """Evaluate a single condition across the entire DataFrame. Returns boolean Series."""
    left_key = cond.get("left", "")
    op = cond.get("operator", "")
    right_key = cond.get("right", "")
    n = len(df)

    # ── Day of Week ──
    if left_key == "Day_Of_Week":
        ist_idx = df.index + pd.Timedelta(hours=5, minutes=30)
        day_names = ist_idx.day_name()
        days = set(cond.get("right_days", []))
        if op == "contains":
            return pd.Series(day_names.isin(days), index=df.index)
        elif op == "not_contains":
            return pd.Series(~day_names.isin(days), index=df.index)
        return pd.Series(False, index=df.index)

    # ── Time of Day ──
    if left_key == "Time_Of_Day":
        cur_times = df.index.strftime("%H:%M")
        cmp_time = cond.get("right_time", "09:30")
        if op == "is_above":
            return pd.Series(cur_times > cmp_time, index=df.index)
        elif op == "is_below":
            return pd.Series(cur_times < cmp_time, index=df.index)
        elif op == ">=":
            return pd.Series(cur_times >= cmp_time, index=df.index)
        elif op == "<=":
            return pd.Series(cur_times <= cmp_time, index=df.index)
        return pd.Series(False, index=df.index)

    # ── Resolve left side ──
    if left_key in _PRICE_MAP:
        lv = df[_PRICE_MAP[left_key]]
    elif left_key in df.columns:
        lv = df[left_key]
    else:
        return pd.Series(False, index=df.index)

    # ── Boolean operators ──
    if op == "is_true":
        try:
            return lv.fillna(0).astype(bool)
        except (TypeError, ValueError):
            return pd.Series(False, index=df.index)
    elif op == "is_false":
        try:
            return ~lv.fillna(1).astype(bool)
        except (TypeError, ValueError):
            return pd.Series(False, index=df.index)

    # ── Resolve right side ──
    if right_key == "number":
        rv = float(cond.get("right_number_value", 0))
    elif right_key in _PRICE_MAP:
        rv = df[_PRICE_MAP[right_key]]
    elif right_key in df.columns:
        rv = df[right_key]
    elif right_key in ("true", "false"):
        rv = right_key == "true"
    else:
        return pd.Series(False, index=df.index)

    # ── Comparison operators ──
    try:
        lv_num = pd.to_numeric(lv, errors="coerce")
        rv_num = pd.to_numeric(rv, errors="coerce") if isinstance(rv, pd.Series) else rv
    except Exception:
        return pd.Series(False, index=df.index)

    # Mask NaN: any row with NaN on either side → False
    nan_mask = pd.isna(lv_num)
    if isinstance(rv_num, pd.Series):
        nan_mask = nan_mask | pd.isna(rv_num)

    if op == "is_above":
        result = lv_num > rv_num
    elif op == "is_below":
        result = lv_num < rv_num
    elif op == ">=":
        result = lv_num >= rv_num
    elif op == "<=":
        result = lv_num <= rv_num
    elif op == "==":
        result = lv_num == rv_num
    elif op == "crosses_above":
        prev_lv = lv_num.shift(1)
        prev_rv = rv_num.shift(1) if isinstance(rv_num, pd.Series) else rv_num
        result = (prev_lv <= prev_rv) & (lv_num > rv_num)
        nan_mask = nan_mask | pd.isna(prev_lv)
        if isinstance(prev_rv, pd.Series):
            nan_mask = nan_mask | pd.isna(prev_rv)
    elif op == "crosses_below":
        prev_lv = lv_num.shift(1)
        prev_rv = rv_num.shift(1) if isinstance(rv_num, pd.Series) else rv_num
        result = (prev_lv >= prev_rv) & (lv_num < rv_num)
        nan_mask = nan_mask | pd.isna(prev_lv)
        if isinstance(prev_rv, pd.Series):
            nan_mask = nan_mask | pd.isna(prev_rv)
    else:
        return pd.Series(False, index=df.index)

    # NaN rows → False
    if isinstance(result, pd.Series):
        result = result & ~nan_mask
    return result


def _vec_conditions(df, conditions):
    """Vectorized evaluation of a condition group. Returns boolean numpy array."""
    if not conditions:
        return np.zeros(len(df), dtype=bool)

    result = _vec_single_condition(df, conditions[0])
    for c in conditions[1:]:
        v = _vec_single_condition(df, c)
        conn = c.get("logic", c.get("connector", "AND")).upper()
        if conn in ("AND", "IF"):
            result = result & v
        elif conn == "OR":
            result = result | v

    # Return as numpy bool array
    if isinstance(result, pd.Series):
        return result.fillna(False).values.astype(bool)
    return np.asarray(result, dtype=bool)


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
    initial_capital_for_sizing = capital  # fixed sizing base (never changes)
    leverage = int(sc.get("leverage", 10))
    sl_pct = float(sc.get("stoploss_pct", 5))
    tp_pct = float(sc.get("target_profit_pct", 10))
    trail_pct = float(sc.get("trailing_sl_pct", 0))  # 0 = disabled
    max_tpd = int(sc.get("max_trades_per_day", config.MAX_TRADES_PER_DAY))
    indicators = sc.get("indicators", []) or []
    side = sc.get("trade_side", "LONG").upper()  # LONG or SHORT
    position_size_pct = float(sc.get("position_size_pct", 100))  # % of capital
    compounding = str(sc.get("compounding", "false")).lower() == "true"
    fee_pct = float(sc.get("fee_pct", 0))  # taker fee per side (0 default; set 0.05 for Delta Exchange realistic fees)
    max_daily_loss = float(sc.get("max_daily_loss", 0))  # 0 = disabled
    # Position sizing mode: "pct" (default) or "fixed_qty" (e.g. 0.1 BTC)
    position_size_mode = sc.get("position_size_mode", "pct")
    fixed_qty = float(sc.get("fixed_qty", 0))

    sizing_label = (
        f"Fixed {fixed_qty} units"
        if position_size_mode == "fixed_qty" and fixed_qty > 0
        else f"{position_size_pct}% of capital"
    )
    print(
        f"[BACKTEST] ═══ FEE: {fee_pct}% per side | SL: {sl_pct}% | TP: {tp_pct}% | Side: {side} | Leverage: {leverage}x | Sizing: {sizing_label} | Compounding: {compounding} ═══"
    )

    t0 = _time.time()

    # Compute indicators (including warm-up data)
    base_interval = sc.get("candle_interval", None)
    df = compute_dynamic_indicators(df_raw, indicators, base_interval=base_interval)

    t_indicators = _time.time()

    # Trim warm-up data: only trade from the user's requested from_date
    # CryptoBot (Delta Exchange India) interprets dates as IST (UTC+5:30).
    # from_date "2025-01-01" means Jan 1 IST = Dec 31 18:30 UTC.
    from_date_str = sc.get("from_date", "")
    if from_date_str and len(df) > 0:
        try:
            # Convert IST date to UTC cutoff
            cutoff = pd.Timestamp(from_date_str) - pd.Timedelta(hours=5, minutes=30)
            # Handle timezone-aware index
            if hasattr(df.index, "tz") and df.index.tz is not None:
                cutoff = cutoff.tz_localize(df.index.tz)
            before_len = len(df)
            df = df[df.index >= cutoff]
            print(
                f"[BACKTEST] Trimmed warm-up data. IST from_date={from_date_str}, UTC cutoff={cutoff}, rows: {before_len}→{len(df)}"
            )
        except Exception as e:
            print(f"[BACKTEST] Warm-up trim failed: {e}")
            pass

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

    # ── Vectorized signal pre-computation ─────────────────────────
    # Evaluate all conditions at once using pandas vectorized ops.
    # entry_mask[i] = True means conditions are met AT row i.
    # In the loop we check prev candle, so entry_signal[i] = entry_mask[i-1].
    entry_mask = _vec_conditions(df, entry_conditions)
    exit_mask = _vec_conditions(df, exit_conditions)

    # Shift by 1: conditions evaluated on prev candle, action on current
    entry_signal = np.zeros(len(df), dtype=bool)
    exit_signal = np.zeros(len(df), dtype=bool)
    if len(df) > 1:
        entry_signal[1:] = entry_mask[:-1]
        exit_signal[1:] = exit_mask[:-1]

    t_vectorize = _time.time()

    # ── Pre-extract OHLC as numpy arrays for fast access ──────────
    opens = df["open"].values.astype(np.float64)
    highs = df["high"].values.astype(np.float64)
    lows = df["low"].values.astype(np.float64)
    closes = df["close"].values.astype(np.float64)
    timestamps = df.index

    # Pre-compute IST dates for daily trade count tracking
    ist_offsets = timestamps + pd.Timedelta(hours=5, minutes=30)
    ist_dates = ist_offsets.date

    # ── Main trading loop ─────────────────────────────────────────
    trades = []
    equity_values = np.full(len(df), float(capital))
    cum_pnl = 0.0
    total_fees = 0.0
    tid = 0
    in_trade = False
    entry_price = 0.0
    entry_time = None
    entry_idx = 0
    entry_size = 0.0  # notional position size in USD
    trades_today = 0
    last_trade_date = None
    daily_pnl = 0.0  # for max_daily_loss check
    daily_loss_hit = False  # stop trading for the rest of the day
    peak_pnl_pct = 0.0  # for trailing SL
    _entry_true_count = 0  # diagnostic counter

    for i in range(2, len(df)):
        ts = timestamps[i]
        current_date = ist_dates[i]

        # Reset daily trade count and daily loss tracker
        if current_date != last_trade_date:
            trades_today = 0
            daily_pnl = 0.0
            daily_loss_hit = False
            last_trade_date = current_date

        if not in_trade:
            # Fast path: skip if no entry signal (vectorized pre-check)
            if not entry_signal[i]:
                equity_values[i] = capital
                continue
            # Check daily limits
            if trades_today >= max_tpd:
                equity_values[i] = capital
                continue
            if daily_loss_hit:
                equity_values[i] = capital
                continue

            _entry_true_count += 1
            in_trade = True
            entry_price = opens[i]
            entry_time = ts
            entry_idx = i
            peak_pnl_pct = 0.0  # reset trailing tracker

            # Position sizing
            if position_size_mode == "fixed_qty" and fixed_qty > 0:
                entry_size = fixed_qty * entry_price  # notional = qty × price
            else:
                sizing_base = capital if compounding else initial_capital_for_sizing
                margin_used = sizing_base * (position_size_pct / 100)
                entry_size = margin_used * leverage

            trades_today += 1
            equity_values[i] = capital
        else:
            # Check exit conditions using OHLC worst/best-case
            price = closes[i]
            h = highs[i]
            lo = lows[i]

            if side == "LONG":
                worst_pnl_pct = (lo - entry_price) / entry_price * 100
                best_pnl_pct = (h - entry_price) / entry_price * 100
                pnl_pct = (price - entry_price) / entry_price * 100
            else:
                worst_pnl_pct = (entry_price - h) / entry_price * 100
                best_pnl_pct = (entry_price - lo) / entry_price * 100
                pnl_pct = (entry_price - price) / entry_price * 100

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
            # Exit conditions met (use vectorized pre-computed signal)
            elif exit_signal[i]:
                exit_reason = "Signal Exit"

            if exit_reason:
                # Calculate actual exit price based on exit reason
                if exit_reason == "Signal Exit":
                    price = opens[i]
                elif exit_reason == "Stop Loss":
                    if side == "LONG":
                        price = entry_price * (1 - sl_pct / 100)
                    else:
                        price = entry_price * (1 + sl_pct / 100)
                elif exit_reason == "Take Profit":
                    if side == "LONG":
                        price = entry_price * (1 + tp_pct / 100)
                    else:
                        price = entry_price * (1 - tp_pct / 100)
                elif exit_reason == "Trailing SL":
                    trail_exit_pct = peak_pnl_pct - trail_pct
                    if side == "LONG":
                        price = entry_price * (1 + trail_exit_pct / 100)
                    else:
                        price = entry_price * (1 - trail_exit_pct / 100)
                elif exit_reason == "Liquidation":
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
                # Track daily PnL for max_daily_loss
                daily_pnl += trade_pnl
                if max_daily_loss > 0 and daily_pnl <= -max_daily_loss:
                    daily_loss_hit = True
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

            equity_values[i] = capital

    # Close open trade at last candle
    if in_trade and len(df) > 0:
        price = closes[-1]
        ts = timestamps[-1]
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

    t_loop = _time.time()

    # ── Build equity curve from numpy array ───────────────────────
    # Forward-fill capital changes through the array
    for i in range(1, len(equity_values)):
        if equity_values[i] == initial_capital_for_sizing and i > 0:
            equity_values[i] = equity_values[i - 1]

    # Downsample equity curve — only need ~500 points for the chart
    n_points = min(500, len(df))
    step = max(1, len(df) // n_points)
    eq_indices = list(range(0, len(df), step))
    if eq_indices[-1] != len(df) - 1:
        eq_indices.append(len(df) - 1)
    equity_curve = [{"time": str(timestamps[i])[:19], "value": round(float(equity_values[i]), 2)} for i in eq_indices]

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

    # Max drawdown (from equity curve)
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
                base_val = 1 + total_return_dec
                if base_val > 0:
                    ann_return = (base_val ** (365 / max(total_days, 1))) - 1
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

    t_stats = _time.time()

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
        "compounding": compounding,
    }

    print(f"[BACKTEST-DIAG] Entry condition matched on {_entry_true_count} candles, total trades: {total_trades}")
    print(
        f"[BACKTEST] ⏱ Timing: indicators={t_indicators - t0:.1f}s, vectorize={t_vectorize - t_indicators:.1f}s, loop={t_loop - t_vectorize:.1f}s, stats={t_stats - t_loop:.1f}s, TOTAL={t_stats - t0:.1f}s"
    )
    if total_fees > 0:
        print(
            f"[BACKTEST] ⚠ TOTAL FEES CHARGED: ${total_fees:,.2f} (fee_pct={fee_pct}% per side). Set fee to 0 for CryptoBot comparison."
        )

    # Build diagnostic info for 0-trade results
    diagnostics = None
    if total_trades == 0:
        diag_items = []
        diag_items.append(f"DataFrame: {len(df)} rows")
        diag_items.append(f"Entry conditions matched: {_entry_true_count} times")
        diag_items.append(f"Vectorized entry signals: {entry_mask.sum()} raw, {entry_signal.sum()} shifted")
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
        "equity": equity_curve,
        "monthly": monthly_list,
        "yearly": yearly_list,
        "day_of_week": list(dow_map.values()),
    }
    if diagnostics:
        result["diagnostics"] = diagnostics
    return result
