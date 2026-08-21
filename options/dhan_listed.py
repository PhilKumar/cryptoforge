"""Serve PhilForge's premium-source interface out of the Dhan parquet stores.

PhilForge's offline runners ask a source two things: which expiries exist, and
what a given contract cost at a given minute. Dhan cannot answer the second
directly -- it sells moneyness, and its stores are keyed by *which* expiry was
nearest rather than by the expiry itself.

The bridge is the calendar. For any minute we know which expiry was nearest,
which was second, and the same for monthlies; so a contract can be routed to
the one store that actually holds it. A contract further out than the stores
reach is reported missing rather than served from the wrong instrument, which
is the whole failure this class exists to prevent.
"""

from __future__ import annotations

import os
from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Any, Optional

import pandas as pd


def monthly_expiries(weeklies: list[date]) -> list[date]:
    """The monthly is simply the last weekly of its calendar month."""
    last: dict[tuple, date] = {}
    for e in weeklies:
        last[(e.year, e.month)] = max(last.get((e.year, e.month), e), e)
    return sorted(last.values())


class _Store:
    """One parquet directory, read a month at a time."""

    def __init__(self, root: str, underlying: str = "NIFTY", keep: int = 2):
        self.root, self.underlying, self.keep = root, underlying, keep
        self._cache: dict[str, dict] = {}
        self._order: list[str] = []

    def month(self, key: str) -> dict:
        if key in self._cache:
            return self._cache[key]
        path = os.path.join(self.root, f"{self.underlying}_{key}.parquet")
        table: dict = {}
        if os.path.exists(path):
            df = pd.read_parquet(path, columns=["ts", "strike", "side", "open"])
            table = {
                (t.to_pydatetime(), int(s), sd): float(o)
                for t, s, sd, o in zip(df["ts"], df["strike"], df["side"], df["open"])
            }
        self._cache[key] = table
        self._order.append(key)
        while len(self._order) > self.keep:
            self._cache.pop(self._order.pop(0), None)
        return table

    def at(self, when: datetime, strike: int, side: str) -> Optional[float]:
        m = when.replace(second=0, microsecond=0, tzinfo=None)
        return self.month(f"{m:%Y-%m}").get((m, int(strike), side.upper()))


class DhanListedSource:
    """``expiries()`` and ``lookup()``, the two calls PhilForge's runners make."""

    def __init__(
        self, weeklies: list[date], stores: dict[str, str], underlying: str = "NIFTY", nearest_within: int = 15
    ):
        self.weeklies = sorted(weeklies)
        self.monthlies = monthly_expiries(self.weeklies)
        self.stores = {k: _Store(v, underlying) for k, v in stores.items() if os.path.isdir(v)}
        self.nearest_within = int(nearest_within)
        # Why a lookup came back empty, counted so a run can be judged.
        self.misses = {"out_of_reach": 0, "no_bar": 0}
        self.served = 0

    # -- calendar ---------------------------------------------------------
    @staticmethod
    def _nth_after(days: list[date], day: date, n: int) -> Optional[date]:
        i = bisect_left(days, day) + n - 1
        return days[i] if 0 <= i < len(days) else None

    def _store_for(self, day: date, expiry: date) -> Optional[str]:
        """Which store, if any, holds this contract on this day."""
        if expiry == self._nth_after(self.weeklies, day, 1):
            return "e1"
        if expiry == self._nth_after(self.weeklies, day, 2):
            return "e2"
        if expiry == self._nth_after(self.monthlies, day, 1):
            return "m1"
        if expiry == self._nth_after(self.monthlies, day, 2):
            return "m2"
        return None

    # -- the interface ----------------------------------------------------
    def expiries(self) -> list[date]:
        return list(self.weeklies)

    def lookup(self, when: datetime, contract: Any) -> Optional[float]:
        stamp = when.replace(tzinfo=None) if when.tzinfo is not None else when
        expiry = contract.expiry
        if isinstance(expiry, datetime):
            expiry = expiry.date()
        if not isinstance(expiry, date):
            expiry = date.fromisoformat(str(expiry)[:10])

        which = self._store_for(stamp.date(), expiry)
        store = self.stores.get(which) if which else None
        if store is None:
            self.misses["out_of_reach"] += 1
            return None

        strike, side = int(contract.strike), str(contract.option_type).upper()
        price = store.at(stamp, strike, side)
        if price is None:
            # The nearest real print the same day, the way the app's own hybrid
            # lookup does it -- forward first, an order resting at the level
            # fills at the option's next trade.
            for step in range(1, self.nearest_within + 1):
                for cand in (stamp + timedelta(minutes=step), stamp - timedelta(minutes=step)):
                    if cand.date() != stamp.date():
                        continue
                    price = store.at(cand, strike, side)
                    if price is not None:
                        break
                if price is not None:
                    break
        if price is None or price <= 0:
            self.misses["no_bar"] += 1
            return None
        self.served += 1
        return price

    def report(self) -> str:
        asked = self.served + sum(self.misses.values())
        if not asked:
            return "no lookups"
        return (
            f"{self.served:,}/{asked:,} lookups served ({self.served / asked:.1%}); "
            f"{self.misses['out_of_reach']:,} beyond the stores' expiries, "
            f"{self.misses['no_bar']:,} with no bar"
        )
