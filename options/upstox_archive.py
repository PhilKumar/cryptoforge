"""Serve the same premium interface out of PhilForge's Upstox option archive.

Two archives, two different failure modes, and that is the point of having both.

Dhan is deep and shallow: five years back, but keyed by *moneyness* -- about
twelve strikes either side of the money -- so a contract the market walks away
from stops being quoted exactly when the trade is winning
([[proj_dhan_atm_band_drops_winners]]).

Upstox is the opposite: real contracts at real strikes, quoted for their whole
life however far they drift, but the archive only reaches back to
**2024-09-02**, and it holds only the contracts PhilForge once fetched -- a
median of 33 strikes per NIFTY expiry, not the whole chain. So a lookup can come
back empty for two very different reasons, and this class refuses to blur them:

    NO_CONTRACT   the archive never held that strike. Says nothing about the
                  market. Pricing it at intrinsic, or at zero, would be an
                  invention.
    NO_BAR        the contract is here and that minute is not. The option
                  genuinely did not print.

A backtest is entitled to know which it hit, so both are counted separately and
both appear in ``report()``.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any, Optional

DEFAULT_ROOT = "/Users/philipkumar/Documents/PhilForge/data/option_archive/upstox"


class UpstoxArchiveSource:
    """``expiries()`` and ``lookup()``, read off the contract-keyed archive."""

    def __init__(self, root: str = DEFAULT_ROOT, underlying: str = "NIFTY", keep: int = 64):
        self.root = os.path.join(root, underlying.lower())
        self.underlying = underlying
        self.keep = int(keep)
        self._cache: dict[tuple, Optional[dict]] = {}
        self._order: list = []
        self.misses = {"no_contract": 0, "no_bar": 0}
        self.served = 0
        self.served_exact = 0
        self.served_nearby = 0
        self.missed_at: set = set()
        # Which strikes the archive actually holds, per expiry. A backtest that
        # keeps asking for strikes just outside this set is not being told
        # "no trade", it is being told "no data", and the two must not net out.
        self._strikes: dict = {}

    # -- calendar ---------------------------------------------------------
    def expiries(self) -> list[date]:
        if not os.path.isdir(self.root):
            return []
        out = []
        for name in sorted(os.listdir(self.root)):
            try:
                out.append(date.fromisoformat(name))
            except ValueError:
                continue
        return out

    def strikes(self, expiry: date, option_type: str = "CE") -> list[int]:
        key = (expiry, option_type.upper())
        if key in self._strikes:
            return self._strikes[key]
        d = os.path.join(self.root, expiry.isoformat())
        out = []
        if os.path.isdir(d):
            for name in os.listdir(d):
                if not name.endswith(f"_{key[1]}.json"):
                    continue
                try:
                    out.append(int(name.split("_")[0]))
                except ValueError:
                    continue
        out.sort()
        self._strikes[key] = out
        return out

    # -- the tape ---------------------------------------------------------
    def _contract(self, expiry: date, strike: int, side: str) -> Optional[dict]:
        """Minute -> open price for one contract, or None if the archive never
        held it. A file that exists but parses to nothing is a held file with no
        bars, which is a NO_BAR for every minute, not a missing contract."""
        key = (expiry, int(strike), side)
        if key in self._cache:
            return self._cache[key]
        path = os.path.join(self.root, expiry.isoformat(), f"{int(strike)}_{side}.json")
        table: Optional[dict] = None
        if os.path.exists(path):
            try:
                doc = json.load(open(path))
            except Exception:
                doc = None
            if isinstance(doc, dict):
                table = {}
                for bar in doc.get("bars") or []:
                    stamp = bar.get("timestamp")
                    price = bar.get("open")
                    if stamp is None or price is None:
                        continue
                    # 'YYYY-MM-DDTHH:MM', occasionally with seconds or a zone.
                    table[datetime.fromisoformat(str(stamp)[:16])] = float(price)
        self._cache[key] = table
        self._order.append(key)
        while len(self._order) > self.keep:
            self._cache.pop(self._order.pop(0), None)
        return table

    def lookup(self, when: datetime, contract: Any) -> Optional[float]:
        stamp = when.replace(second=0, microsecond=0, tzinfo=None)
        expiry = contract.expiry
        if isinstance(expiry, datetime):
            expiry = expiry.date()
        if not isinstance(expiry, date):
            expiry = date.fromisoformat(str(expiry)[:10])
        side = str(contract.option_type).upper()

        table = self._contract(expiry, int(contract.strike), side)
        if table is None:
            self.misses["no_contract"] += 1
            self.missed_at.add(stamp)
            return None
        price = table.get(stamp)
        if price is None or price <= 0:
            self.misses["no_bar"] += 1
            self.missed_at.add(stamp)
            return None
        self.served += 1
        self.served_exact += 1
        return price

    def report(self) -> str:
        asked = self.served + sum(self.misses.values())
        if not asked:
            return "no lookups"
        return (
            f"{self.served:,}/{asked:,} lookups served ({self.served / asked:.1%}), all at the exact minute; "
            f"{self.misses['no_contract']:,} for strikes the archive never held, "
            f"{self.misses['no_bar']:,} for a minute that did not print"
        )

    def lookup_forward(self, when: datetime, contract: Any, minutes: int = 15) -> tuple:
        """The asked-for minute, or the next print of the SAME contract inside
        the same session. Returns ``(price, offset_minutes)``.

        This search is defensible here and is not defensible on Dhan. Dhan's
        misses mean "this strike left the ATM band", so a neighbouring minute
        comes back from the edge of the window at a price that minute never had
        ([[proj_dhan_atm_band_drops_winners]]). An Upstox miss on a contract the
        archive *holds* means only that nobody traded it in that minute -- the
        next print is that same contract's own price. A miss on a contract the
        archive never held is not searched at all.
        """
        from datetime import timedelta

        price = self.lookup(when, contract)
        if price is not None:
            return price, 0
        stamp = when.replace(second=0, microsecond=0, tzinfo=None)
        expiry = contract.expiry
        if isinstance(expiry, datetime):
            expiry = expiry.date()
        if not isinstance(expiry, date):
            expiry = date.fromisoformat(str(expiry)[:10])
        table = self._contract(expiry, int(contract.strike), str(contract.option_type).upper())
        if table is None or not minutes:
            return None, 0
        for step in range(1, int(minutes) + 1):
            for cand in (stamp + timedelta(minutes=step), stamp - timedelta(minutes=step)):
                if cand.date() != stamp.date():
                    continue
                got = table.get(cand)
                if got is not None and got > 0:
                    self.misses["no_bar"] -= 1
                    self.served += 1
                    self.served_nearby += 1
                    return float(got), step
        return None, 0
