"""
engine/indicators.py — CryptoForge Technical Indicators
Crypto-optimized: Supertrend, EMA, RSI, MACD, Bollinger Bands, VWAP, ATR
"""

import pandas as pd
import numpy as np


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
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series, fast: int = 12, slow: int = 26,
         signal: int = 9) -> pd.DataFrame:
    """MACD indicator returning line, signal, histogram."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return pd.DataFrame({
        "macd_line": macd_line,
        "macd_signal": signal_line,
        "macd_histogram": histogram,
    }, index=series.index)


def bollinger_bands(series: pd.Series, period: int = 20,
                    std_dev: float = 2.0) -> pd.DataFrame:
    """Bollinger Bands: upper, middle, lower."""
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    width = (upper - lower) / middle * 100
    return pd.DataFrame({
        "bb_upper": upper,
        "bb_middle": middle,
        "bb_lower": lower,
        "bb_width": width,
    }, index=series.index)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def supertrend(df: pd.DataFrame, period: int = 10,
               multiplier: float = 3.0) -> pd.DataFrame:
    """SuperTrend indicator using numpy for performance."""
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    close = df["close"].values.astype(float)
    n = len(close)

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low,
                    np.maximum(np.abs(high - prev_close),
                               np.abs(low - prev_close)))

    alpha = 2.0 / (period + 1)
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
    st_dir = np.zeros(n, dtype=int)
    st[0] = lower[0]
    st_dir[0] = 1

    for i in range(1, n):
        lower[i] = (lower_raw[i] if (lower_raw[i] > lower[i - 1] or
                     close[i - 1] < lower[i - 1]) else lower[i - 1])
        upper[i] = (upper_raw[i] if (upper_raw[i] < upper[i - 1] or
                     close[i - 1] > upper[i - 1]) else upper[i - 1])

        if st[i - 1] == upper[i - 1]:
            if close[i] > upper[i]:
                st[i] = lower[i]; st_dir[i] = 1
            else:
                st[i] = upper[i]; st_dir[i] = -1
        else:
            if close[i] < lower[i]:
                st[i] = upper[i]; st_dir[i] = -1
            else:
                st[i] = lower[i]; st_dir[i] = 1

    result = df.copy()
    result["supertrend"] = st
    result["supertrend_dir"] = st_dir
    return result


def vwap(df: pd.DataFrame) -> pd.Series:
    """Volume Weighted Average Price — resets daily by default for crypto."""
    typical = (df["high"] + df["low"] + df["close"]) / 3
    cum_tp_vol = (typical * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum()
    return cum_tp_vol / cum_vol


def stochastic_rsi(series: pd.Series, rsi_period: int = 14,
                   stoch_period: int = 14, k_smooth: int = 3,
                   d_smooth: int = 3) -> pd.DataFrame:
    """Stochastic RSI."""
    rsi_val = rsi(series, rsi_period)
    min_rsi = rsi_val.rolling(window=stoch_period).min()
    max_rsi = rsi_val.rolling(window=stoch_period).max()
    stoch_k = 100 * (rsi_val - min_rsi) / (max_rsi - min_rsi)
    stoch_k = stoch_k.rolling(window=k_smooth).mean()
    stoch_d = stoch_k.rolling(window=d_smooth).mean()
    return pd.DataFrame({"stoch_rsi_k": stoch_k, "stoch_rsi_d": stoch_d},
                        index=series.index)


def cpr(df: pd.DataFrame) -> pd.DataFrame:
    """Central Pivot Range (CPR) with support/resistance levels.
    Uses previous day's high/low/close to calculate today's pivot levels.
    For intraday data, groups by date and shifts forward.
    """
    d = df.copy()
    # Determine if data is intraday or daily
    if len(d) > 1:
        diff = (d.index[1] - d.index[0]) if hasattr(d.index, '__getitem__') else pd.Timedelta(hours=1)
        if hasattr(diff, 'total_seconds'):
            is_intraday = diff.total_seconds() < 86400
        else:
            is_intraday = True
    else:
        is_intraday = False

    if is_intraday and hasattr(d.index, 'date'):
        # Group by date, use previous day's HLC
        daily = d.groupby(d.index.date).agg({"high": "max", "low": "min", "close": "last"})
        daily["pivot"] = (daily["high"] + daily["low"] + daily["close"]) / 3
        daily["bc"] = (daily["high"] + daily["low"]) / 2
        daily["tc"] = 2 * daily["pivot"] - daily["bc"]
        daily["r1"] = 2 * daily["pivot"] - daily["low"]
        daily["s1"] = 2 * daily["pivot"] - daily["high"]
        daily["r2"] = daily["pivot"] + (daily["high"] - daily["low"])
        daily["s2"] = daily["pivot"] - (daily["high"] - daily["low"])
        daily["r3"] = daily["high"] + 2 * (daily["pivot"] - daily["low"])
        daily["s3"] = daily["low"] - 2 * (daily["high"] - daily["pivot"])
        # Shift by one day — today's CPR uses yesterday's HLC
        daily = daily.shift(1)
        # Map back to intraday rows
        date_series = pd.Series(d.index.date, index=d.index)
        for col in ["pivot", "bc", "tc", "r1", "s1", "r2", "s2", "r3", "s3"]:
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
                df[f"{ind_string}_dir"] = st_df["supertrend_dir"]

            elif name == "MACD":
                fast = int(parts[1]) if len(parts) > 1 else 12
                slow = int(parts[2]) if len(parts) > 2 else 26
                sig = int(parts[3]) if len(parts) > 3 else 9
                macd_df = macd(df["close"], fast, slow, sig)
                df[f"MACD_line"] = macd_df["macd_line"]
                df[f"MACD_signal"] = macd_df["macd_signal"]
                df[f"MACD_histogram"] = macd_df["macd_histogram"]

            elif name == "BB":
                period = int(parts[1]) if len(parts) > 1 else 20
                std = float(parts[2]) if len(parts) > 2 else 2.0
                bb_df = bollinger_bands(df["close"], period, std)
                df["BB_upper"] = bb_df["bb_upper"]
                df["BB_middle"] = bb_df["bb_middle"]
                df["BB_lower"] = bb_df["bb_lower"]
                df["BB_width"] = bb_df["bb_width"]

            elif name == "VWAP":
                df["VWAP"] = vwap(df)

            elif name == "ATR":
                period = int(parts[1]) if len(parts) > 1 else 14
                df[ind_string] = atr(df, period)

            elif name == "StochRSI":
                period = int(parts[1]) if len(parts) > 1 else 14
                srsi = stochastic_rsi(df["close"], period)
                df["StochRSI_K"] = srsi["stoch_rsi_k"]
                df["StochRSI_D"] = srsi["stoch_rsi_d"]

            elif name == "CPR":
                cpr_df = cpr(df)
                for col in ["CPR_pivot", "CPR_bc", "CPR_tc", "CPR_R1", "CPR_S1",
                             "CPR_R2", "CPR_S2", "CPR_R3", "CPR_S3"]:
                    if col in cpr_df.columns:
                        df[col] = cpr_df[col]

            elif name in ("Current", "Previous"):
                pass

        except (IndexError, ValueError, KeyError) as e:
            print(f"[INDICATORS] Skipping malformed indicator '{ind_string}': {e}")
            continue

    return df
