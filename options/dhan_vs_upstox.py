"""Measure Dhan's rebuilt series against the Upstox archive where both exist.

The two sources overlap from 2024-09 onward. That overlap is the only chance to
find out what a Dhan-sourced backtest is worth, because Upstox is contract-keyed
and therefore known-good: exact strike, exact expiry, no moneyness in the way.

So: rebuild a fixed-strike series out of Dhan for days the archive also covers,
put the two side by side minute for minute, and report three things --

  matched      minutes both sources priced, and how far apart they were
  dhan_missing minutes Upstox priced and Dhan did not (the band drifted)
  upstox_missing  the reverse

A backfill is worth running only if the matched prices agree closely AND
dhan_missing is small at the strikes the strategy actually trades. Wide
agreement on a thin sample proves nothing.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class Comparison:
    matched: int = 0
    dhan_missing: int = 0
    upstox_missing: int = 0
    # Of dhan_missing, the ones whose strike simply sat outside the ATM band
    # the vendor sells. Those are a known edge of the product, not a hole in
    # it, and counting them as gaps condemns data that is in fact complete.
    out_of_band: int = 0
    abs_diffs: list = field(default_factory=list)
    rel_diffs: list = field(default_factory=list)
    worst: Optional[tuple] = None

    def add(self, when: datetime, strike: int, side: str, ups: float, dhan: float) -> None:
        self.matched += 1
        diff = abs(ups - dhan)
        self.abs_diffs.append(diff)
        if ups:
            self.rel_diffs.append(diff / ups)
        if self.worst is None or diff > self.worst[0]:
            self.worst = (diff, when, strike, side, ups, dhan)

    def report(self) -> str:
        if not self.matched:
            return (
                "NO OVERLAP AT ALL -- Dhan priced none of the minutes Upstox did. "
                f"(upstox-only {self.upstox_missing:,}, dhan-only {self.dhan_missing:,}). "
                "Do not run a backfill on this."
            )
        med = statistics.median(self.abs_diffs)
        p95 = sorted(self.abs_diffs)[int(len(self.abs_diffs) * 0.95) - 1]
        relmed = statistics.median(self.rel_diffs) if self.rel_diffs else float("nan")
        asked = self.matched + self.dhan_missing
        lines = [
            f"minutes Upstox priced      : {asked:,}",
            f"  Dhan also priced         : {self.matched:,} ({self.matched / asked:.1%})",
            f"  Dhan had nothing         : {self.dhan_missing:,} ({self.dhan_missing / asked:.1%})",
            f"  of which outside ATM band: {self.out_of_band:,}",
            f"  coverage inside the band : {self.in_band_served:.1%}",
            f"minutes only Dhan priced   : {self.upstox_missing:,}",
            "",
            f"median absolute difference : Rs {med:.2f}",
            f"95th percentile difference : Rs {p95:.2f}",
            f"median relative difference : {relmed:.2%}",
        ]
        if self.worst:
            d, when, strike, side, u, dh = self.worst
            lines.append(
                f"worst bar                  : Rs {d:.2f} at {when:%Y-%m-%d %H:%M} "
                f"{strike}{side}  upstox {u:.2f} vs dhan {dh:.2f}"
            )
        return "\n".join(lines)

    @property
    def in_band_served(self) -> float:
        """Coverage judged only where the vendor claims to sell data."""
        asked = self.matched + self.dhan_missing - self.out_of_band
        return (self.matched / asked) if asked > 0 else 0.0

    @property
    def verdict(self) -> str:
        if not self.matched:
            return "UNUSABLE"
        served = self.in_band_served
        relmed = statistics.median(self.rel_diffs) if self.rel_diffs else 1.0
        if served >= 0.95 and relmed <= 0.01:
            return "GOOD -- Dhan reproduces the archive inside its band"
        if served >= 0.80 and relmed <= 0.05:
            return "USABLE WITH CAVEAT -- agrees, but with holes; report coverage beside every number"
        return "NOT TRUSTWORTHY -- do not publish anything built on this"


def load_upstox_contract(
    archive_root: Path, underlying: str, expiry: str, strike: int, side: str
) -> dict[datetime, float]:
    """Read one contract file from PhilForge's archive into minute -> close."""
    path = archive_root / "upstox" / underlying.lower() / expiry / f"{strike}_{side.upper()}.json"
    if not path.exists():
        return {}
    blob = json.loads(path.read_text())
    out = {}
    for bar in blob.get("bars", []):
        out[datetime.fromisoformat(bar["timestamp"])] = float(bar["close"])
    return out


def compare(
    upstox_series: dict[datetime, float], dhan_source, strike: int, side: str, comparison: Optional[Comparison] = None
) -> Comparison:
    """Put one contract's two versions side by side."""
    cmp_ = comparison or Comparison()
    for when, ups in upstox_series.items():
        got = dhan_source.premium(when, strike, side)
        if got is None:
            cmp_.dhan_missing += 1
        else:
            cmp_.add(when, strike, side, ups, got)
    return cmp_
