"""A fixed-strike premium lookup built on Dhan's moneyness-keyed option series.

Dhan does not sell contracts. It sells *moneyness* -- "current expiry, ATM+1" --
and the contract behind that label changes as spot moves and again at every
rollover. A backtest that reads the label as a contract compares yesterday's
expiring option against today's fresh one and prints nonsense.

The way out is that every bar carries its own real ``strike``. So the contract
series can be rebuilt: pull the whole ATM +/-N band, then keep only the bars
whose strike is the one being asked for. What comes back is a genuine
fixed-strike series with honest holes -- the minutes when that strike had
drifted outside the band Dhan serves.

A hole is returned as ``None``. It is never filled with a neighbouring strike,
never carried forward from an earlier minute, and never confused with a strike
that traded at zero. Callers can ask :meth:`coverage` afterwards to find out how
much of a run stood on real bars, which is the only number that says whether the
run means anything.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import requests

BASE_URL = "https://api.dhan.co/v2/charts/rollingoption"

# Dhan serves index options at ATM +/-10 and stock options at ATM +/-3.
INDEX_BAND = 10
STOCK_BAND = 3

# The vendor caps a single call at 30 days.
MAX_WINDOW_DAYS = 30

# Data APIs allow 5 requests a second. A backfill is not latency-sensitive and a
# throttled client is a client that finishes, so we sit well under the cap.
REQUESTS_PER_SECOND = 2.0

VALID_INTERVALS = {"1", "5", "15", "25", "60"}


class DhanDataError(RuntimeError):
    """The vendor said no. Distinct from 'the vendor had nothing'."""


@dataclass
class Gap:
    """One request that returned no rows. Recorded, never silently dropped."""

    from_date: str
    to_date: str
    strike_alias: str
    option_type: str


@dataclass
class Coverage:
    """What a completed pull actually contains."""

    bars: int = 0
    requests: int = 0
    empty_requests: int = 0
    retries: int = 0
    gaps: list[Gap] = field(default_factory=list)
    minutes: set = field(default_factory=set)
    strikes: set = field(default_factory=set)
    first_minute: Optional[datetime] = None
    last_minute: Optional[datetime] = None

    # Lookups served, split by outcome -- the honest denominator for any result
    # computed on top of this data.
    hits: int = 0
    misses: int = 0

    @property
    def hit_rate(self) -> float:
        asked = self.hits + self.misses
        return (self.hits / asked) if asked else 0.0

    def summary(self) -> str:
        span = "-"
        if self.first_minute and self.last_minute:
            span = f"{self.first_minute:%Y-%m-%d} .. {self.last_minute:%Y-%m-%d}"
        return (
            f"{self.bars:,} bars over {span}; "
            f"{len(self.strikes)} distinct strikes; "
            f"{self.requests} requests ({self.empty_requests} empty, {self.retries} retried); "
            f"lookups {self.hits:,} hit / {self.misses:,} miss "
            f"({self.hit_rate:.1%} served)"
        )


def _aliases(band: int) -> list[str]:
    out = ["ATM"]
    for n in range(1, band + 1):
        out.append(f"ATM+{n}")
        out.append(f"ATM-{n}")
    return out


def _windows(start: date, end: date, days: int = MAX_WINDOW_DAYS):
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=days - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


class DhanRollingPremiums:
    """Pull an ATM band once, then answer fixed-strike questions from memory.

    Usage::

        src = DhanRollingPremiums(client_id, token, security_id="13")
        src.load("2023-01-01", "2023-03-31", option_type="CE")
        lookup = src.as_premium_lookup()      # (ts, strike, side) -> price|None
        print(src.coverage.summary())
    """

    def __init__(
        self,
        client_id: str,
        access_token: str,
        *,
        security_id: str = "13",
        exchange_segment: str = "NSE_FNO",
        instrument: str = "OPTIDX",
        expiry_flag: str = "WEEK",
        expiry_code: int = 1,
        interval: str = "1",
        band: int = INDEX_BAND,
        sleep_between: float = 1.0 / REQUESTS_PER_SECOND,
    ) -> None:
        if str(interval) not in VALID_INTERVALS:
            raise ValueError(f"interval must be one of {sorted(VALID_INTERVALS)}")
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": access_token,
            "client-id": client_id,
        }
        self.security_id = str(security_id)
        self.exchange_segment = exchange_segment
        self.instrument = instrument
        self.expiry_flag = expiry_flag.upper()
        self.expiry_code = int(expiry_code)
        self.interval = str(interval)
        self.band = int(band)
        self.sleep_between = float(sleep_between)

        # (minute, strike, side) -> close.  The whole point of the class.
        self._bars: dict[tuple[datetime, int, str], float] = {}
        # minute -> spot, kept so ATM can be re-derived without a second pull.
        self._spot: dict[datetime, float] = {}
        # minute -> the strikes that were actually served, so a miss can be told
        # apart from a minute the vendor never covered at all.
        self._served: dict[datetime, set] = defaultdict(set)
        self.coverage = Coverage()

    # ---------------------------------------------------------------- network

    # A five-year backfill is thousands of calls over hours. The vendor will
    # drop one somewhere -- a 504 from its gateway, a reset connection -- and a
    # pull that dies on the first of those has to be restarted by hand. Retry
    # the transient failures; never retry a refusal, because a bad token or a
    # malformed request will refuse just as firmly the tenth time.
    RETRY_ON = (408, 429, 500, 502, 503, 504)
    MAX_ATTEMPTS = 6

    def _post(self, body: dict) -> dict:
        last = ""
        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(BASE_URL, json=body, headers=self._headers, timeout=90)
            except requests.RequestException as exc:
                last = f"request failed: {exc!r}"
            else:
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError as exc:
                        raise DhanDataError(f"Dhan sent non-JSON: {resp.text[:200]}") from exc
                if resp.status_code not in self.RETRY_ON:
                    raise DhanDataError(f"Dhan HTTP {resp.status_code}: {resp.text[:300]}")
                last = f"HTTP {resp.status_code}: {resp.text[:150]}"
            if attempt < self.MAX_ATTEMPTS:
                backoff = min(60.0, 2.0**attempt)
                self.coverage.retries += 1
                print(f"    [retry {attempt}/{self.MAX_ATTEMPTS - 1} in {backoff:.0f}s] {last[:110]}", flush=True)
                time.sleep(backoff)
        raise DhanDataError(f"Dhan failed {self.MAX_ATTEMPTS} times, last: {last}")

    def _fetch(self, frm: str, to: str, alias: str, option_type: str) -> int:
        side = "CALL" if option_type.upper() in ("CALL", "CE") else "PUT"
        body = {
            "securityId": self.security_id,
            "exchangeSegment": self.exchange_segment,
            "instrument": self.instrument,
            "expiryFlag": self.expiry_flag,
            "expiryCode": self.expiry_code,
            "strike": alias,
            "drvOptionType": side,
            "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot", "iv", "oi"],
            "fromDate": frm,
            "toDate": to,
            "interval": self.interval,
        }
        payload = self._post(body)
        self.coverage.requests += 1

        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        leg = data.get("ce" if side == "CALL" else "pe") or {}
        stamps = leg.get("timestamp") or []
        if not stamps:
            # An empty 200 is the vendor having nothing. It is NOT a quiet
            # market and must never be read as one.
            self.coverage.empty_requests += 1
            self.coverage.gaps.append(Gap(frm, to, alias, side))
            return 0

        strikes = leg.get("strike") or []
        closes = leg.get("close") or []
        spots = leg.get("spot") or []
        unit = 1000.0 if stamps[0] > 1e12 else 1.0
        tag = "CE" if side == "CALL" else "PE"

        added = 0
        for i, raw in enumerate(stamps):
            if i >= len(strikes) or i >= len(closes):
                break
            if strikes[i] is None or closes[i] is None:
                continue
            # Dhan stamps are UTC epoch; the market is IST.
            minute = datetime.utcfromtimestamp(raw / unit) + timedelta(hours=5, minutes=30)
            strike = int(round(float(strikes[i])))
            self._bars[(minute, strike, tag)] = float(closes[i])
            self._served[minute].add(strike)
            if i < len(spots) and spots[i] is not None:
                self._spot[minute] = float(spots[i])
            self.coverage.strikes.add(strike)
            self.coverage.minutes.add(minute)
            if self.coverage.first_minute is None or minute < self.coverage.first_minute:
                self.coverage.first_minute = minute
            if self.coverage.last_minute is None or minute > self.coverage.last_minute:
                self.coverage.last_minute = minute
            added += 1

        self.coverage.bars += added
        return added

    # ------------------------------------------------------------------- load

    def load(
        self,
        from_date: str,
        to_date: str,
        *,
        option_type: str | Iterable[str] = ("CE", "PE"),
        progress: bool = True,
    ) -> Coverage:
        """Pull the whole ATM band across the window, one 30-day chunk at a time."""
        sides = [option_type] if isinstance(option_type, str) else list(option_type)
        start = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date)
        aliases = _aliases(self.band)
        chunks = list(_windows(start, end))
        total = len(chunks) * len(aliases) * len(sides)
        done = 0

        for frm, to in chunks:
            for side in sides:
                for alias in aliases:
                    self._fetch(frm.isoformat(), to.isoformat(), alias, side)
                    done += 1
                    if progress and done % 25 == 0:
                        print(
                            f"  [{done}/{total}] {frm} {side} {alias:<6} "
                            f"bars={self.coverage.bars:,} empty={self.coverage.empty_requests}",
                            flush=True,
                        )
                    if self.sleep_between:
                        time.sleep(self.sleep_between)
        return self.coverage

    # ----------------------------------------------------------------- lookup

    def premium(self, when: datetime, strike: int, option_type: str) -> Optional[float]:
        """The close of that exact contract at that exact minute, or None.

        None means one of two things, and :meth:`explain` tells them apart: the
        strike was outside the band Dhan serves, or the vendor had no bar for
        that minute at all. Neither is ever papered over with a nearby price.
        """
        key = (when.replace(second=0, microsecond=0), int(strike), option_type.upper())
        hit = self._bars.get(key)
        if hit is None:
            self.coverage.misses += 1
        else:
            self.coverage.hits += 1
        return hit

    def explain(self, when: datetime, strike: int) -> str:
        minute = when.replace(second=0, microsecond=0)
        served = self._served.get(minute)
        if not served:
            return "no_vendor_data_for_minute"
        if int(strike) not in served:
            lo, hi = min(served), max(served)
            return f"strike_outside_band(served {lo}..{hi})"
        return "served"

    def as_premium_lookup(self):
        """Adapt to PhilForge's ``PremiumLookup`` -- (ts, strike, side) -> price|None."""
        return lambda when, strike, option_type: self.premium(when, strike, option_type)

    def spot(self, when: datetime) -> Optional[float]:
        return self._spot.get(when.replace(second=0, microsecond=0))


# ---------------------------------------------------------------------------
# Streaming: a pull too big to hold
# ---------------------------------------------------------------------------
#
# Three years of ATM+/-10 at minute resolution is roughly 12 million bars per
# index. Kept in a dict that is about 2.5 GB of live objects, which is fine on a
# laptop and fatal on the 1 GB box PhilForge runs on. So the backfill writes
# each chunk straight out and keeps nothing, and the lookup reads back one month
# at a time.


class StreamingPull(DhanRollingPremiums):
    """Same fetch, but every chunk lands on disk instead of on the heap."""

    def __init__(self, *args, out_dir: str | Path, underlying: str = "NIFTY", **kw):
        super().__init__(*args, **kw)
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.underlying = underlying.upper()
        self._pending: list[dict] = []

    def _fetch(self, frm: str, to: str, alias: str, option_type: str) -> int:
        side = "CALL" if option_type.upper() in ("CALL", "CE") else "PUT"
        tag = "CE" if side == "CALL" else "PE"
        body = {
            "securityId": self.security_id,
            "exchangeSegment": self.exchange_segment,
            "instrument": self.instrument,
            "expiryFlag": self.expiry_flag,
            "expiryCode": self.expiry_code,
            "strike": alias,
            "drvOptionType": side,
            "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot", "iv", "oi"],
            "fromDate": frm,
            "toDate": to,
            "interval": self.interval,
        }
        payload = self._post(body)
        self.coverage.requests += 1

        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        leg = data.get("ce" if side == "CALL" else "pe") or {}
        stamps = leg.get("timestamp") or []
        if not stamps:
            self.coverage.empty_requests += 1
            self.coverage.gaps.append(Gap(frm, to, alias, side))
            return 0

        strikes = leg.get("strike") or []
        closes = leg.get("close") or []
        opens = leg.get("open") or []
        highs = leg.get("high") or []
        lows = leg.get("low") or []
        spots = leg.get("spot") or []
        ois = leg.get("oi") or []
        ivs = leg.get("iv") or []
        unit = 1000.0 if stamps[0] > 1e12 else 1.0

        def at(seq, i):
            return seq[i] if i < len(seq) and seq[i] is not None else None

        added = 0
        for i, raw in enumerate(stamps):
            if at(strikes, i) is None or at(closes, i) is None:
                continue
            minute = datetime.utcfromtimestamp(raw / unit) + timedelta(hours=5, minutes=30)
            self._pending.append(
                {
                    "ts": minute,
                    "strike": int(round(float(strikes[i]))),
                    "side": tag,
                    "open": at(opens, i),
                    "high": at(highs, i),
                    "low": at(lows, i),
                    "close": float(closes[i]),
                    "oi": at(ois, i),
                    "iv": at(ivs, i),
                    "spot": at(spots, i),
                }
            )
            added += 1

        self.coverage.bars += added
        if self.coverage.first_minute is None and self._pending:
            self.coverage.first_minute = self._pending[0]["ts"]
        if self._pending:
            self.coverage.last_minute = self._pending[-1]["ts"]
        if len(self._pending) > 250_000:
            self.flush()
        return added

    def flush(self) -> int:
        """Write what is buffered, one file per calendar month, then forget it."""
        if not self._pending:
            return 0
        import pandas as pd

        df = pd.DataFrame(self._pending)
        self._pending = []
        written = 0
        for month, part in df.groupby(df["ts"].dt.strftime("%Y-%m")):
            path = self.out_dir / f"{self.underlying}_{month}.parquet"
            if path.exists():
                part = pd.concat([pd.read_parquet(path), part], ignore_index=True)
            part = part.drop_duplicates(subset=["ts", "strike", "side"], keep="last")
            part.to_parquet(path, index=False)
            written += len(part)
        return written

    def load(self, *args, **kw) -> Coverage:
        try:
            return super().load(*args, **kw)
        finally:
            self.flush()


class ParquetPremiums:
    """A PremiumLookup over a streamed pull, holding one month at a time."""

    def __init__(self, out_dir: str | Path, underlying: str = "NIFTY", keep_months: int = 2):
        self.out_dir = Path(out_dir)
        self.underlying = underlying.upper()
        self.keep_months = int(keep_months)
        self._cache: dict[str, dict] = {}
        self._order: list[str] = []
        self.coverage = Coverage()

    def _month(self, key: str) -> dict:
        if key in self._cache:
            return self._cache[key]
        import pandas as pd

        path = self.out_dir / f"{self.underlying}_{key}.parquet"
        table: dict = {}
        if path.exists():
            df = pd.read_parquet(path, columns=["ts", "strike", "side", "close"])
            table = {
                (t.to_pydatetime(), int(s), sd): float(c)
                for t, s, sd, c in zip(df["ts"], df["strike"], df["side"], df["close"])
            }
        self._cache[key] = table
        self._order.append(key)
        while len(self._order) > self.keep_months:
            self._cache.pop(self._order.pop(0), None)
        return table

    def premium(self, when: datetime, strike: int, option_type: str) -> Optional[float]:
        minute = when.replace(second=0, microsecond=0)
        table = self._month(f"{minute:%Y-%m}")
        hit = table.get((minute, int(strike), option_type.upper()))
        if hit is None:
            self.coverage.misses += 1
        else:
            self.coverage.hits += 1
        return hit

    def as_premium_lookup(self):
        return lambda when, strike, option_type: self.premium(when, strike, option_type)
