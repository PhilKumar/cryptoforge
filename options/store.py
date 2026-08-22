"""options/store.py — where the bars land, and the ledger of what never arrived.

Two tables, and the second is the point.

`bars` is the obvious one: OHLC, IV, OI and spot per contract-minute, written
as parquet partitioned by underlying and month.

`coverage` is the table the previous pull did not have. Every request the
backfill makes writes exactly one row here — including the ones that came back
empty. Without it, an archive cannot answer "is this strike missing because it
never traded, or because the vendor never had it?", and that single unanswered
question is what makes a backtest on it uninterpretable. The ledger is written
whether or not any bars were, which is why it is a separate table rather than a
column on `bars`: a fetch with no rows must still leave a trace.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

BARS_COLUMNS = [
    "ts",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "strike_offset",
    "option_type",
    "interval",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "iv",
    "spot",
]

COVERAGE_COLUMNS = [
    "requested_at",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "strike_offset",
    "option_type",
    "interval",
    "from_date",
    "to_date",
    "status",
    "http_status",
    "bars_returned",
    "detail",
]


@dataclass(frozen=True)
class SeriesKey:
    """Identity of one rolling contract series.

    Note what is absent: an absolute strike price and an expiry date. Dhan
    addresses this data by moneyness and expiry rank, so those are the only
    coordinates that exist. Storing a fabricated absolute strike alongside them
    would invent precision the source does not have.
    """

    underlying: str
    expiry_flag: str
    expiry_code: int
    strike_offset: int
    option_type: str
    interval: str = "1"

    def as_dict(self) -> dict:
        return asdict(self)


class OptionStore:
    """Parquet-backed store. Append-only by design — a backfill that overwrites
    is a backfill you cannot audit after the fact."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.bars_dir = self.root / "bars"
        self.coverage_dir = self.root / "coverage"
        self.bars_dir.mkdir(parents=True, exist_ok=True)
        self.coverage_dir.mkdir(parents=True, exist_ok=True)

    # ── writes ───────────────────────────────────────────────────────────
    def write_bars(self, key: SeriesKey, bars: Sequence) -> int:
        """Append bars for one series. Returns rows written."""
        if not bars:
            return 0
        rows = []
        for b in bars:
            rows.append(
                {
                    "ts": b.ts,
                    "underlying": key.underlying,
                    "expiry_flag": key.expiry_flag,
                    "expiry_code": key.expiry_code,
                    "strike_offset": key.strike_offset,
                    "option_type": key.option_type,
                    "interval": key.interval,
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": b.volume,
                    "oi": b.oi,
                    "iv": b.iv,
                    "spot": b.spot,
                }
            )
        df = pd.DataFrame(rows, columns=BARS_COLUMNS)
        df["ts"] = pd.to_datetime(df["ts"])
        month = df["ts"].iloc[0].strftime("%Y-%m")
        out = self.bars_dir / f"underlying={key.underlying}" / f"month={month}"
        out.mkdir(parents=True, exist_ok=True)
        name = (
            f"{key.expiry_flag}_{key.expiry_code}_{key.option_type}"
            f"_{key.strike_offset:+d}_{key.interval}_{df['ts'].iloc[0]:%Y%m%d}.parquet"
        )
        df.to_parquet(out / name, index=False)
        return len(df)

    def write_coverage(self, records: Iterable[dict]) -> int:
        """Record request outcomes — the empties above all."""
        rows = list(records)
        if not rows:
            return 0
        df = pd.DataFrame(rows, columns=COVERAGE_COLUMNS)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        df.to_parquet(self.coverage_dir / f"coverage_{stamp}.parquet", index=False)
        return len(df)

    # ── reads ────────────────────────────────────────────────────────────
    def load_bars(self, underlying: Optional[str] = None) -> pd.DataFrame:
        pattern = f"underlying={underlying}/**/*.parquet" if underlying else "**/*.parquet"
        files = sorted(self.bars_dir.glob(pattern))
        if not files:
            return pd.DataFrame(columns=BARS_COLUMNS)
        df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
        return df.sort_values("ts").reset_index(drop=True)

    def load_coverage(self) -> pd.DataFrame:
        files = sorted(self.coverage_dir.glob("*.parquet"))
        if not files:
            return pd.DataFrame(columns=COVERAGE_COLUMNS)
        return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)

    def completed_windows(self) -> set:
        """Windows already attempted, so a killed backfill resumes rather than
        restarts. Includes NO_DATA windows — re-asking a source that has
        nothing just burns rate limit."""
        cov = self.load_coverage()
        if cov.empty:
            return set()
        return {
            (
                r.underlying,
                r.expiry_flag,
                int(r.expiry_code),
                int(r.strike_offset),
                r.option_type,
                r.interval,
                str(r.from_date),
                str(r.to_date),
            )
            for r in cov.itertuples()
        }
