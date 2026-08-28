"""options/adapters.py — audit an archive this package did not write.

The coverage audit is worth running on the Upstox data that already exists, not
only on a fresh Dhan pull. But that archive is contract-keyed — real strikes,
real expiry dates — while this package is moneyness-keyed, because that is how
Dhan serves it. This module bridges the two so `audit.py` can judge either.

The bridge has one honest difficulty. To ask the decisive question — "does
coverage hold *at the money*?" — you need to know where the money was. A
contract-keyed archive only knows that if it also carries spot. So there are
three ways to establish ATM, in descending order of trust, and the one actually
used is always reported:

  EXACT     a spot column on the same rows
  JOINED    a separate underlying series, joined on timestamp
  INFERRED  the most-traded strike of the session, as a proxy

INFERRED is a real estimate and is labelled as one everywhere it appears. It is
good enough to answer "is ATM hollow?" — the question that matters — and not
good enough to price anything.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from options.store import BARS_COLUMNS

# Column names seen in the wild, mapped to ours. Extend freely — a name that is
# missing here costs an explicit --map flag, not a wrong answer.
ALIASES: Dict[str, tuple] = {
    "ts": ("ts", "timestamp", "datetime", "date_time", "time", "candle_time", "start_time", "start_Time", "date"),
    "open": ("open", "o", "open_price"),
    "high": ("high", "h", "high_price"),
    "low": ("low", "l", "low_price"),
    "close": ("close", "c", "close_price", "ltp", "last_price"),
    "volume": ("volume", "v", "vol", "qty", "traded_qty"),
    "oi": ("oi", "OI", "open_interest", "openinterest"),
    "iv": ("iv", "IV", "implied_volatility", "impliedvolatility"),
    "spot": ("spot", "SPOT", "underlying", "underlying_price", "spot_price", "index"),
    "strike": ("strike", "strike_price", "strikeprice", "strike_pr"),
    "option_type": ("option_type", "opt_type", "instrument_type", "cp", "call_put", "right", "drv_option_type"),
    "underlying": ("underlying_symbol", "symbol", "name", "index_name", "ticker"),
    "expiry": ("expiry", "expiry_date", "expirydate", "expiry_dt"),
}


class AtmBasis(str, Enum):
    EXACT = "exact"  # spot on the row
    JOINED = "joined"  # spot joined from a separate series
    INFERRED = "inferred"  # most-traded strike of the session


@dataclass
class AdapterResult:
    frame: pd.DataFrame
    atm_basis: AtmBasis
    rows_in: int
    rows_out: int
    notes: list

    def caveat(self) -> str:
        if self.atm_basis is AtmBasis.INFERRED:
            return (
                "ATM was INFERRED from the most-traded strike per session — no "
                "spot in the archive. Good enough to detect a hollow ATM; not "
                "good enough to price anything."
            )
        return f"ATM basis: {self.atm_basis.value}"


def _read_any(path: str | Path, table: Optional[str] = None) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        files = sorted(list(p.rglob("*.parquet")) + list(p.rglob("*.csv")))
        if not files:
            raise FileNotFoundError(f"no .parquet or .csv under {p}")
        return pd.concat((_read_any(f) for f in files), ignore_index=True)
    if p.suffix == ".parquet":
        return pd.read_parquet(p)
    if p.suffix in (".csv", ".txt"):
        return pd.read_csv(p)
    if p.suffix in (".db", ".sqlite", ".sqlite3"):
        with sqlite3.connect(p) as con:
            names = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)["name"].tolist()
            if not table:
                raise ValueError(f"pass table= for sqlite; tables here: {names}")
            # SQL cannot bind an identifier, so the name is checked against the
            # database's own catalogue and the matching entry -- not the
            # caller's string -- is what gets interpolated.
            match = next((n for n in names if n == table), None)
            if match is None:
                raise ValueError(f"no table {table!r} in {p.name}; tables here: {names}")
            return pd.read_sql_query(f"SELECT * FROM {match}", con)  # nosec B608 - name came from sqlite_master
    raise ValueError(f"unsupported archive format: {p.suffix}")


def _resolve(df: pd.DataFrame, overrides: Dict[str, str]) -> Dict[str, str]:
    lower = {c.lower(): c for c in df.columns}
    found: Dict[str, str] = {}
    for canon, names in ALIASES.items():
        if canon in overrides:
            found[canon] = overrides[canon]
            continue
        for n in names:
            if n.lower() in lower:
                found[canon] = lower[n.lower()]
                break
    return found


def _normalise_option_type(s: pd.Series) -> pd.Series:
    m = s.astype(str).str.upper().str[:1]
    return m.map({"C": "CALL", "P": "PUT"}).fillna("CALL")


def load_external(
    path: str | Path,
    *,
    table: Optional[str] = None,
    column_map: Optional[Dict[str, str]] = None,
    underlying_name: str = "UNKNOWN",
    spot_series: Optional[pd.DataFrame] = None,
    interval: str = "1",
) -> AdapterResult:
    """Load a foreign options archive into the audit's schema."""
    raw = _read_any(path, table=table)
    rows_in = len(raw)
    cols = _resolve(raw, column_map or {})
    notes = []

    if "ts" not in cols:
        raise ValueError(
            f"could not find a timestamp column; got {list(raw.columns)[:15]}. Pass column_map={{'ts': '<name>'}}."
        )

    out = pd.DataFrame()
    out["ts"] = pd.to_datetime(raw[cols["ts"]], errors="coerce")
    out = out[out["ts"].notna()]
    keep = raw.loc[out.index]

    for field in ("open", "high", "low", "close", "volume", "oi", "iv"):
        out[field] = pd.to_numeric(keep[cols[field]], errors="coerce") if field in cols else pd.NA

    out["underlying"] = keep[cols["underlying"]] if "underlying" in cols else underlying_name
    out["option_type"] = _normalise_option_type(keep[cols["option_type"]]) if "option_type" in cols else "CALL"
    out["interval"] = interval
    out["expiry_flag"] = "WEEK"
    out["expiry_code"] = 0

    # ── establish the money ───────────────────────────────────────────────
    basis = AtmBasis.INFERRED
    if "spot" in cols:
        out["spot"] = pd.to_numeric(keep[cols["spot"]], errors="coerce")
        basis = AtmBasis.EXACT
    elif spot_series is not None and not spot_series.empty:
        s = spot_series.copy()
        s["ts"] = pd.to_datetime(s["ts"])
        out = pd.merge_asof(
            out.sort_values("ts"),
            s.sort_values("ts")[["ts", "spot"]],
            on="ts",
            direction="nearest",
        )
        keep = keep.loc[out.index] if len(keep) == len(out) else keep
        basis = AtmBasis.JOINED
    else:
        out["spot"] = pd.NA

    if "strike" not in cols:
        raise ValueError(
            "no strike column — without it the by-moneyness cut cannot be made, "
            "and that cut is the whole reason to run this audit"
        )
    strikes = pd.to_numeric(keep[cols["strike"]], errors="coerce").reset_index(drop=True)
    out = out.reset_index(drop=True)
    out["_strike"] = strikes
    out["_session"] = out["ts"].dt.date

    if basis is AtmBasis.INFERRED:
        # Proxy: the strike carrying the most volume that session sits at or
        # very near the money. Crude, clearly labelled, and sufficient to see
        # whether the middle of the chain is hollow.
        vol = out["volume"].fillna(0)
        atm = (
            out.assign(_v=vol)
            .groupby("_session")
            .apply(lambda g: g.loc[g["_v"].idxmax(), "_strike"] if len(g) else pd.NA, include_groups=False)
        )
        out["_atm"] = out["_session"].map(atm)
        notes.append("ATM inferred from most-traded strike per session")
    else:
        step = _infer_strike_step(out["_strike"])
        out["_atm"] = (out["spot"] / step).round() * step
        notes.append(f"ATM from spot, strike step {step:g}")

    step = _infer_strike_step(out["_strike"])
    out["strike_offset"] = ((out["_strike"] - out["_atm"]) / step).round()
    out["strike_offset"] = out["strike_offset"].fillna(0).astype(int)

    frame = out[BARS_COLUMNS].copy()
    return AdapterResult(frame, basis, rows_in, len(frame), notes)


def _infer_strike_step(strikes: pd.Series) -> float:
    """Strike spacing, from the data. NIFTY is 50, BANKNIFTY 100, but reading it
    off the archive beats hardcoding a number that changes per underlying."""
    uniq = sorted(pd.to_numeric(strikes, errors="coerce").dropna().unique())
    if len(uniq) < 2:
        return 50.0
    diffs = pd.Series(uniq).diff().dropna()
    diffs = diffs[diffs > 0]
    return float(diffs.mode().iloc[0]) if len(diffs) else 50.0
