"""
engine/indicators.py — CryptoForge Technical Indicators
Crypto-optimized: Supertrend, EMA, RSI, MACD, Bollinger Bands, VWAP, ATR
"""

import numpy as np
import pandas as pd


def _clean(s):
    """Replace ±Inf with NaN so they never propagate into condition evaluation."""
    if isinstance(s, pd.DataFrame):
        return s.replace([np.inf, -np.inf], np.nan)
    return s.replace([np.inf, -np.inf], np.nan)


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return _clean(100 - (100 / (1 + rs)))


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """MACD indicator returning line, signal, histogram."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return pd.DataFrame(
        {
            "macd_line": macd_line,
            "macd_signal": signal_line,
            "macd_histogram": histogram,
        },
        index=series.index,
    )


def bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    """Bollinger Bands: upper, middle, lower."""
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    width = (upper - lower) / middle.replace(0, np.nan) * 100
    return _clean(
        pd.DataFrame(
            {
                "bb_upper": upper,
                "bb_middle": middle,
                "bb_lower": lower,
                "bb_width": width,
            },
            index=series.index,
        )
    )


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Average Directional Index (ADX) with +DI and -DI."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    # True Range
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    # Smoothed with EMA
    atr_smooth = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    atr_safe = atr_smooth.replace(0, np.nan)
    plus_di = 100 * (plus_dm.ewm(alpha=1.0 / period, adjust=False).mean() / atr_safe)
    minus_di = 100 * (minus_dm.ewm(alpha=1.0 / period, adjust=False).mean() / atr_safe)

    # ADX
    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * ((plus_di - minus_di).abs() / di_sum)
    adx_line = dx.ewm(alpha=1.0 / period, adjust=False).mean()

    return _clean(
        pd.DataFrame(
            {
                "adx": adx_line,
                "adx_plus": plus_di,
                "adx_minus": minus_di,
            },
            index=df.index,
        )
    )


def supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    """SuperTrend indicator using numpy for performance."""
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    close = df["close"].values.astype(float)
    n = len(close)

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))

    alpha = 1.0 / period
    atr_arr = np.zeros(n)
    atr_arr[0] = tr[0]
    for i in range(1, n):
        atr_arr[i] = alpha * tr[i] + (1 - alpha) * atr_arr[i - 1]

    hl2 = (high + low) / 2.0
    upper_raw = hl2 + multiplier * atr_arr
    lower_raw = hl2 - multiplier * atr_arr

    upper = upper_raw.copy()
    lower = lower_raw.copy()
    st = np.zeros(n)
    st[0] = lower[0]

    for i in range(1, n):
        lower[i] = lower_raw[i] if (lower_raw[i] > lower[i - 1] or close[i - 1] < lower[i - 1]) else lower[i - 1]
        upper[i] = upper_raw[i] if (upper_raw[i] < upper[i - 1] or close[i - 1] > upper[i - 1]) else upper[i - 1]

        if st[i - 1] == upper[i - 1]:
            if close[i] > upper[i]:
                st[i] = lower[i]
            else:
                st[i] = upper[i]
        else:
            if close[i] < lower[i]:
                st[i] = upper[i]
            else:
                st[i] = lower[i]

    result = df.copy()
    result["supertrend"] = st
    return result


def vwap(df: pd.DataFrame) -> pd.Series:
    """Volume Weighted Average Price — resets daily (UTC midnight) for crypto."""
    typical = (df["high"] + df["low"] + df["close"]) / 3
    tp_vol = typical * df["volume"]

    # If index is datetime, reset accumulation daily
    if hasattr(df.index, "date"):
        dates = pd.Series(df.index.date, index=df.index)
        cum_tp_vol = tp_vol.groupby(dates).cumsum()
        cum_vol = df["volume"].groupby(dates).cumsum()
    else:
        # Fallback: simple cumulative (no date info)
        cum_tp_vol = tp_vol.cumsum()
        cum_vol = df["volume"].cumsum()

    return _clean(cum_tp_vol / cum_vol.replace(0, np.nan))


def stochastic_rsi(
    series: pd.Series, rsi_period: int = 14, stoch_period: int = 14, k_smooth: int = 3, d_smooth: int = 3
) -> pd.DataFrame:
    """Stochastic RSI."""
    rsi_val = rsi(series, rsi_period)
    min_rsi = rsi_val.rolling(window=stoch_period).min()
    max_rsi = rsi_val.rolling(window=stoch_period).max()
    denom = (max_rsi - min_rsi).replace(0, np.nan)
    stoch_k = 100 * (rsi_val - min_rsi) / denom
    stoch_k = stoch_k.rolling(window=k_smooth).mean()
    stoch_d = stoch_k.rolling(window=d_smooth).mean()
    return _clean(pd.DataFrame({"stoch_rsi_k": stoch_k, "stoch_rsi_d": stoch_d}, index=series.index))


def cpr(df: pd.DataFrame, timeframe: str = "Day", narrow_pct: float = 0.2, moderate_pct: float = 0.5) -> pd.DataFrame:
    """Central Pivot Range (CPR) with support/resistance levels and width classification.
    Supports 2H, 4H, Day, Week, and Month timeframes.
    Uses previous period's high/low/close to calculate pivot levels.
    """
    d = df.copy()
    tf = timeframe.lower() if timeframe else "day"

    # Determine if data is intraday
    if len(d) > 1:
        diff = (d.index[1] - d.index[0]) if hasattr(d.index, "__getitem__") else pd.Timedelta(hours=1)
        if hasattr(diff, "total_seconds"):
            is_intraday = diff.total_seconds() < 86400
        else:
            is_intraday = True
    else:
        is_intraday = False

    def _calc_pivots(agg_df):
        """Calculate pivot levels from aggregated HLC data."""
        agg_df["pivot"] = (agg_df["high"] + agg_df["low"] + agg_df["close"]) / 3
        agg_df["bc"] = (agg_df["high"] + agg_df["low"]) / 2
        agg_df["tc"] = 2 * agg_df["pivot"] - agg_df["bc"]
        agg_df["R1"] = 2 * agg_df["pivot"] - agg_df["low"]
        agg_df["S1"] = 2 * agg_df["pivot"] - agg_df["high"]
        agg_df["R2"] = agg_df["pivot"] + (agg_df["high"] - agg_df["low"])
        agg_df["S2"] = agg_df["pivot"] - (agg_df["high"] - agg_df["low"])
        agg_df["R3"] = agg_df["high"] + 2 * (agg_df["pivot"] - agg_df["low"])
        agg_df["S3"] = agg_df["low"] - 2 * (agg_df["high"] - agg_df["pivot"])
        agg_df["R4"] = agg_df["R3"] + (agg_df["high"] - agg_df["low"])
        agg_df["S4"] = agg_df["S3"] - (agg_df["high"] - agg_df["low"])
        agg_df["R5"] = agg_df["R4"] + (agg_df["high"] - agg_df["low"])
        agg_df["S5"] = agg_df["S4"] - (agg_df["high"] - agg_df["low"])
        # CPR width
        agg_df["cpr_width_pct"] = (agg_df["tc"] - agg_df["bc"]).abs() / agg_df["close"].replace(0, np.nan) * 100
        return agg_df

    pivot_cols = ["pivot", "bc", "tc", "R1", "S1", "R2", "S2", "R3", "S3", "R4", "S4", "R5", "S5", "cpr_width_pct"]

    def _map_period(agg, period_key):
        """Map aggregated period data back to intraday rows."""
        agg = _calc_pivots(agg)
        agg = agg.shift(1)
        for col in pivot_cols:
            mapping = agg[col].to_dict()
            d[f"CPR_{col}"] = period_key.map(mapping).values

    if is_intraday and hasattr(d.index, "date"):
        if tf == "2h":
            # 2-hour CPR: group by 2-hour windows
            period_key = d.index.floor("2h").astype(str)
            period_key = pd.Series(period_key, index=d.index)
            agg = d.groupby(d.index.floor("2h")).agg({"high": "max", "low": "min", "close": "last"})
            agg.index = agg.index.astype(str)
            _map_period(agg, period_key)
        elif tf == "4h":
            # 4-hour CPR: group by 4-hour windows
            period_key = d.index.floor("4h").astype(str)
            period_key = pd.Series(period_key, index=d.index)
            agg = d.groupby(d.index.floor("4h")).agg({"high": "max", "low": "min", "close": "last"})
            agg.index = agg.index.astype(str)
            _map_period(agg, period_key)
        elif tf == "week":
            period_key = d.index.to_series().dt.isocalendar().apply(lambda x: f"{x.year}-W{x.week:02d}", axis=1)
            period_key.index = d.index
            agg = d.groupby(period_key).agg({"high": "max", "low": "min", "close": "last"})
            _map_period(agg, period_key)
        elif tf == "month":
            period_key = pd.Series(d.index.to_period("M").astype(str), index=d.index)
            agg = d.groupby(period_key).agg({"high": "max", "low": "min", "close": "last"})
            _map_period(agg, period_key)
        else:
            # Daily CPR (default)
            daily = d.groupby(d.index.date).agg({"high": "max", "low": "min", "close": "last"})
            daily = _calc_pivots(daily)
            daily = daily.shift(1)
            date_series = pd.Series(d.index.date, index=d.index)
            for col in pivot_cols:
                mapping = daily[col].to_dict()
                d[f"CPR_{col}"] = date_series.map(mapping).values
    else:
        # Daily data — use previous bar's HLC
        prev_h = d["high"].shift(1)
        prev_l = d["low"].shift(1)
        prev_c = d["close"].shift(1)
        d["CPR_pivot"] = (prev_h + prev_l + prev_c) / 3
        d["CPR_bc"] = (prev_h + prev_l) / 2
        d["CPR_tc"] = 2 * d["CPR_pivot"] - d["CPR_bc"]
        d["CPR_R1"] = 2 * d["CPR_pivot"] - prev_l
        d["CPR_S1"] = 2 * d["CPR_pivot"] - prev_h
        d["CPR_R2"] = d["CPR_pivot"] + (prev_h - prev_l)
        d["CPR_S2"] = d["CPR_pivot"] - (prev_h - prev_l)
        d["CPR_R3"] = prev_h + 2 * (d["CPR_pivot"] - prev_l)
        d["CPR_S3"] = prev_l - 2 * (prev_h - d["CPR_pivot"])
        d["CPR_R4"] = d["CPR_R3"] + (prev_h - prev_l)
        d["CPR_S4"] = d["CPR_S3"] - (prev_h - prev_l)
        d["CPR_R5"] = d["CPR_R4"] + (prev_h - prev_l)
        d["CPR_S5"] = d["CPR_S4"] - (prev_h - prev_l)
        d["CPR_cpr_width_pct"] = (d["CPR_tc"] - d["CPR_bc"]).abs() / d["close"].replace(0, np.nan) * 100

    return d


def compute_dynamic_indicators(df: pd.DataFrame, ui_indicators: list) -> pd.DataFrame:
    """Compute indicators dynamically based on UI selection."""
    df = df.copy()

    # Always expose current candle OHLCV
    df["current_open"] = df["open"]
    df["current_high"] = df["high"]
    df["current_low"] = df["low"]
    df["current_close"] = df["close"]
    df["current_volume"] = df["volume"] if "volume" in df.columns else 0

    for ind_string in ui_indicators:
        try:
            parts = ind_string.split("_")
            name = parts[0]

            if name == "EMA":
                period = int(parts[1])
                df[ind_string] = ema(df["close"], period)

            elif name == "SMA":
                period = int(parts[1])
                df[ind_string] = sma(df["close"], period)

            elif name == "RSI":
                period = int(parts[1])
                df[ind_string] = rsi(df["close"], period)

            elif name == "Supertrend":
                period = int(parts[1])
                mult = float(parts[2])
                st_df = supertrend(df, period=period, multiplier=mult)
                df[ind_string] = st_df["supertrend"]

            elif name == "MACD":
                # Filter out trailing timeframe suffix (e.g. '5m')
                num_parts = [p for p in parts[1:] if not p.endswith("m")]
                fast = int(num_parts[0]) if len(num_parts) > 0 else 12
                slow = int(num_parts[1]) if len(num_parts) > 1 else 26
                sig = int(num_parts[2]) if len(num_parts) > 2 else 9
                macd_df = macd(df["close"], fast, slow, sig)
                # Full-ID prefixed columns (new format: MACD_12_26_9_5m__line)
                df[f"{ind_string}__line"] = macd_df["macd_line"]
                df[f"{ind_string}__signal"] = macd_df["macd_signal"]
                df[f"{ind_string}__histogram"] = macd_df["macd_histogram"]
                # Backward-compat static columns
                df["MACD_line"] = macd_df["macd_line"]
                df["MACD_signal"] = macd_df["macd_signal"]
                df["MACD_histogram"] = macd_df["macd_histogram"]

            elif name == "BB":
                num_parts = [p for p in parts[1:] if not p.endswith("m")]
                period = int(num_parts[0]) if len(num_parts) > 0 else 20
                std = float(num_parts[1]) if len(num_parts) > 1 else 2.0
                bb_df = bollinger_bands(df["close"], period, std)
                # Full-ID prefixed columns
                df[f"{ind_string}__upper"] = bb_df["bb_upper"]
                df[f"{ind_string}__middle"] = bb_df["bb_middle"]
                df[f"{ind_string}__lower"] = bb_df["bb_lower"]
                # Backward-compat static columns
                df["BB_upper"] = bb_df["bb_upper"]
                df["BB_middle"] = bb_df["bb_middle"]
                df["BB_lower"] = bb_df["bb_lower"]
                df["BB_width"] = bb_df["bb_width"]

            elif name == "VWAP":
                val = vwap(df)
                df[ind_string] = val
                df["VWAP"] = val  # backward-compat

            elif name == "ATR":
                period = int(parts[1]) if len(parts) > 1 else 14
                df[ind_string] = atr(df, period)

            elif name == "ADX":
                num_parts = [p for p in parts[1:] if not p.endswith("m")]
                period = int(num_parts[0]) if len(num_parts) > 0 else 14
                adx_df = adx(df, period)
                df[ind_string] = adx_df["adx"]
                df[f"{ind_string}__plus"] = adx_df["adx_plus"]
                df[f"{ind_string}__minus"] = adx_df["adx_minus"]
                # Backward-compat
                df[f"{ind_string}_plus"] = adx_df["adx_plus"]
                df[f"{ind_string}_minus"] = adx_df["adx_minus"]

            elif name == "StochRSI":
                num_parts = [p for p in parts[1:] if not p.endswith("m")]
                period = int(num_parts[0]) if len(num_parts) > 0 else 14
                srsi = stochastic_rsi(df["close"], period)
                # Full-ID prefixed columns
                df[f"{ind_string}__K"] = srsi["stoch_rsi_k"]
                df[f"{ind_string}__D"] = srsi["stoch_rsi_d"]
                # Backward-compat static columns
                df["StochRSI_K"] = srsi["stoch_rsi_k"]
                df["StochRSI_D"] = srsi["stoch_rsi_d"]

            elif name == "CPR":
                # Support CPR_Day_0.2_0.5, CPR_2H, CPR_Week, CPR_Month or plain CPR
                tf = parts[1] if len(parts) > 1 else "Day"
                narrow_pct = float(parts[2]) if len(parts) > 2 else 0.2
                moderate_pct = float(parts[3]) if len(parts) > 3 else 0.5
                cpr_df = cpr(df, timeframe=tf, narrow_pct=narrow_pct, moderate_pct=moderate_pct)
                for col in [
                    "CPR_pivot",
                    "CPR_bc",
                    "CPR_tc",
                    "CPR_R1",
                    "CPR_S1",
                    "CPR_R2",
                    "CPR_S2",
                    "CPR_R3",
                    "CPR_S3",
                    "CPR_R4",
                    "CPR_S4",
                    "CPR_R5",
                    "CPR_S5",
                    "CPR_cpr_width_pct",
                ]:
                    if col in cpr_df.columns:
                        df[col] = cpr_df[col]
                # Capitalized aliases for frontend condition matching
                df["CPR_Pivot"] = df.get("CPR_pivot", np.nan)
                df["CPR_TC"] = df.get("CPR_tc", np.nan)
                df["CPR_BC"] = df.get("CPR_bc", np.nan)
                # Width classification
                df["CPR_width_pct"] = df.get("CPR_cpr_width_pct", np.nan)
                df["CPR_is_narrow"] = df["CPR_width_pct"] <= narrow_pct
                df["CPR_is_moderate"] = (df["CPR_width_pct"] > narrow_pct) & (df["CPR_width_pct"] <= moderate_pct)
                df["CPR_is_wide"] = df["CPR_width_pct"] > moderate_pct

            elif name in ("Current", "Previous"):
                pass

            # Signal Candle — trade-context indicator, populated at runtime by engines
            elif name == "Signal":
                pass

        except (IndexError, ValueError, KeyError) as e:
            print(f"[INDICATORS] Skipping malformed indicator '{ind_string}': {e}")
            continue

    return df
