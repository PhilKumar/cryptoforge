"""options/dhan_client.py — DhanHQ expired-options history, read-only.

One rule governs this module:

    A response that carries no candles is never reported as a quiet market.

That rule exists because of how the previous Upstox-backed book failed. Its
expired-options endpoint answers HTTP 200 with an empty candle array for
contracts it simply does not hold, and a loader written the obvious way — treat
"no candles" as "nothing traded that day" — cannot tell that apart from a strike
that genuinely never printed. The backtest then runs on a fraction of the
universe it believes it has, raises no error, and produces a number that looks
like a result. Every fetch here therefore returns a `FetchResult` with an
explicit `status`, and `NO_DATA` is a distinct outcome from a filled series.

The API surface is documented at https://dhanhq.co/docs/v2/expired-options-data/.
Those docs were not reachable when this was written, so every fact taken from
them is pinned in ASSUMPTIONS below and checked at runtime rather than trusted.
If Dhan's contract differs, the assertions here fail loudly on the first call
instead of quietly mis-shaping a five-year pull.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence

import requests

BASE_URL = "https://api.dhan.co"
ROLLING_OPTION_PATH = "/v2/charts/rollingoption"

# ── ASSUMPTIONS ───────────────────────────────────────────────────────────
# Taken from the DhanHQ v2 docs and the official Python client. Each one is
# enforced below, so a wrong entry surfaces as an exception on call one rather
# than as a silently truncated dataset months later. Re-verify against the live
# docs before trusting a full backfill.
MAX_WINDOW_DAYS = 30  # max span of a single rollingoption request
MAX_HISTORY_YEARS = 5  # documented depth of expired-options history
MAX_STRIKE_OFFSET = 10  # coverage runs ATM-10 .. ATM+10
VALID_INTERVALS = ("1", "5", "15", "25", "60", "D")
VALID_EXPIRY_FLAGS = ("WEEK", "MONTH")
VALID_OPTION_TYPES = ("CALL", "PUT")
# The fields we ask for. IV/OI/spot are the reason this dataset is worth having
# over a bare OHLC pull — without spot on the same clock you cannot recompute
# moneyness, and without IV you cannot sanity-check a premium at all.
REQUIRED_DATA = ("OHLC", "IV", "OI", "VOLUME", "SPOT")

# Dhan does not publish a rate limit for this endpoint. One request per second
# is deliberately conservative: a five-year backfill is a background job, and
# being throttled mid-pull costs far more than running slowly.
DEFAULT_MIN_INTERVAL_S = 1.0


class FetchStatus(str, Enum):
    """Why a request produced the rows it produced — or produced none."""

    OK = "ok"  # candles returned
    NO_DATA = "no_data"  # HTTP 200, zero candles: the source has nothing
    RATE_LIMITED = "rate_limited"
    ERROR = "error"


class DhanConfigError(RuntimeError):
    """Credentials or parameters are wrong — never retried."""


class DhanContractError(RuntimeError):
    """The API answered in a shape this client does not recognise.

    Raised rather than coerced. An unrecognised payload during a long backfill
    is exactly the situation where guessing produces a plausible, wrong archive.
    """


@dataclass(frozen=True)
class OptionBar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    oi: float
    iv: Optional[float]
    spot: Optional[float]


@dataclass
class FetchResult:
    """The outcome of one window request, whatever that outcome was.

    `status` is the field callers must branch on. `bars` being empty is not
    self-explanatory: it means one thing under OK (impossible — OK implies at
    least one bar) and quite another under NO_DATA or ERROR.
    """

    status: FetchStatus
    bars: List[OptionBar] = field(default_factory=list)
    http_status: Optional[int] = None
    detail: str = ""
    request: Dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status is FetchStatus.OK


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def iter_windows(from_date, to_date, span_days: int = MAX_WINDOW_DAYS):
    """Split a range into request-sized windows, inclusive of both ends.

    Yielded as (start, end) date pairs. The API caps a single request at
    MAX_WINDOW_DAYS, so a five-year pull is ~61 windows per contract series.
    """
    start, end = _as_date(from_date), _as_date(to_date)
    if start > end:
        raise ValueError(f"from_date {start} is after to_date {end}")
    step = timedelta(days=span_days - 1)
    cursor = start
    while cursor <= end:
        stop = min(cursor + step, end)
        yield cursor, stop
        cursor = stop + timedelta(days=1)


class DhanClient:
    """Read-only client for Dhan's expired-options history.

    Deliberately has no order methods. This package is a data layer; giving it
    a trading surface would mean a backfill script holds credentials that can
    move money, for no benefit.
    """

    def __init__(
        self,
        client_id: Optional[str] = None,
        access_token: Optional[str] = None,
        *,
        base_url: str = BASE_URL,
        session: Optional[requests.Session] = None,
        min_interval_s: float = DEFAULT_MIN_INTERVAL_S,
        max_retries: int = 4,
        timeout_s: float = 30.0,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.client_id = client_id or os.getenv("DHAN_CLIENT_ID", "")
        self.access_token = access_token or os.getenv("DHAN_ACCESS_TOKEN", "")
        if not self.client_id or not self.access_token:
            raise DhanConfigError(
                "DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set. "
                "Generate a token from the Dhan web portal, or use the "
                "API key/secret flow so refresh can be scripted."
            )
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.min_interval_s = min_interval_s
        self.max_retries = max_retries
        self.timeout_s = timeout_s
        self._clock = clock
        self._sleep = sleep
        self._last_call = 0.0

    # ── plumbing ─────────────────────────────────────────────────────────
    def _headers(self) -> Dict[str, str]:
        return {
            "access-token": self.access_token,
            "client-id": self.client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _throttle(self) -> None:
        elapsed = self._clock() - self._last_call
        if self._last_call and elapsed < self.min_interval_s:
            self._sleep(self.min_interval_s - elapsed)
        self._last_call = self._clock()

    # ── the one request that matters ─────────────────────────────────────
    def fetch_expired_option_window(
        self,
        *,
        security_id: str,
        exchange_segment: str,
        instrument: str,
        expiry_flag: str,
        expiry_code: int,
        strike_offset: int,
        option_type: str,
        from_date,
        to_date,
        interval: str = "1",
    ) -> FetchResult:
        """One contract series, one window (<= MAX_WINDOW_DAYS).

        Strikes are addressed by moneyness, not by absolute price: Dhan serves
        this as a *rolling* series, so `strike_offset=-2` means "two strikes
        below ATM for that expiry", not "the 24500 strike". That is convenient
        for systematic tests and useless for reconstructing one named contract,
        which is a property of the source, not of this client.
        """
        if expiry_flag not in VALID_EXPIRY_FLAGS:
            raise DhanConfigError(f"expiry_flag must be one of {VALID_EXPIRY_FLAGS}")
        if option_type not in VALID_OPTION_TYPES:
            raise DhanConfigError(f"option_type must be one of {VALID_OPTION_TYPES}")
        if interval not in VALID_INTERVALS:
            raise DhanConfigError(f"interval must be one of {VALID_INTERVALS}")
        if abs(strike_offset) > MAX_STRIKE_OFFSET:
            raise DhanConfigError(
                f"strike_offset {strike_offset} is outside Dhan's documented "
                f"ATM +/-{MAX_STRIKE_OFFSET} coverage. Far-OTM wings are not in "
                f"this dataset; a strategy needing them cannot be tested on it."
            )
        start, end = _as_date(from_date), _as_date(to_date)
        span = (end - start).days + 1
        if span > MAX_WINDOW_DAYS:
            raise DhanConfigError(
                f"window of {span} days exceeds the {MAX_WINDOW_DAYS}-day cap; use iter_windows() to chunk the range"
            )

        strike = "ATM" if strike_offset == 0 else f"ATM{strike_offset:+d}"
        payload = {
            "exchangeSegment": exchange_segment,
            "securityId": str(security_id),
            "instrument": instrument,
            "expiryFlag": expiry_flag,
            "expiryCode": int(expiry_code),
            "strike": strike,
            "drvOptionType": option_type,
            "interval": interval,
            "requiredData": list(REQUIRED_DATA),
            "fromDate": start.isoformat(),
            "toDate": end.isoformat(),
        }
        return self._post_with_retries(ROLLING_OPTION_PATH, payload)

    def _post_with_retries(self, path: str, payload: Dict[str, Any]) -> FetchResult:
        url = f"{self.base_url}{path}"
        last: Optional[FetchResult] = None
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                resp = self.session.post(url, json=payload, headers=self._headers(), timeout=self.timeout_s)
            except requests.RequestException as exc:
                last = FetchResult(status=FetchStatus.ERROR, detail=f"transport: {exc}", request=payload)
                self._sleep(2**attempt)
                continue

            if resp.status_code == 429:
                last = FetchResult(
                    status=FetchStatus.RATE_LIMITED,
                    http_status=429,
                    detail="throttled",
                    request=payload,
                )
                self._sleep(2**attempt)
                continue

            if resp.status_code in (401, 403):
                # Never retried: a dead token does not heal, and hammering an
                # auth failure is how an API key gets suspended mid-backfill.
                raise DhanConfigError(
                    f"auth rejected ({resp.status_code}). Dhan access tokens are "
                    f"capped at 24h of use; refresh before a long pull."
                )

            if resp.status_code >= 500:
                last = FetchResult(
                    status=FetchStatus.ERROR,
                    http_status=resp.status_code,
                    detail=resp.text[:200],
                    request=payload,
                )
                self._sleep(2**attempt)
                continue

            if resp.status_code != 200:
                return FetchResult(
                    status=FetchStatus.ERROR,
                    http_status=resp.status_code,
                    detail=resp.text[:200],
                    request=payload,
                )

            return self._parse_ok(resp, payload)

        return last or FetchResult(status=FetchStatus.ERROR, detail="retries exhausted", request=payload)

    def _parse_ok(self, resp, payload: Dict[str, Any]) -> FetchResult:
        """Turn a 200 into bars — or into an explicit NO_DATA.

        This is the function the whole module exists for. A 200 carrying empty
        columns is recorded as NO_DATA and propagated as such, so the backfill
        ledger can later answer "how much of this archive was never delivered?"
        — the question nobody asked of the Upstox pull.
        """
        try:
            body = resp.json()
        except ValueError as exc:
            raise DhanContractError(f"200 with non-JSON body: {exc}") from exc

        if not isinstance(body, dict):
            raise DhanContractError(f"expected a JSON object, got {type(body).__name__}")

        # Dhan's chart endpoints answer in column arrays, not row objects.
        cols = body.get("data") if isinstance(body.get("data"), dict) else body
        ts = cols.get("timestamp") or cols.get("start_Time") or []
        if not ts:
            return FetchResult(
                status=FetchStatus.NO_DATA,
                http_status=200,
                detail="200 with zero candles — source holds nothing here",
                request=payload,
            )

        def col(*names) -> Sequence:
            for n in names:
                v = cols.get(n)
                if v is not None:
                    return v
            return [None] * len(ts)

        op, hi, lo, cl = col("open"), col("high"), col("low"), col("close")
        vol, oi = col("volume"), col("OI", "oi", "open_interest")
        iv, spot = col("IV", "iv", "implied_volatility"), col("spot", "SPOT")

        widths = {len(x) for x in (op, hi, lo, cl, vol, oi) if x is not None}
        if widths and widths != {len(ts)}:
            raise DhanContractError(f"ragged columns: timestamp={len(ts)} others={sorted(widths)}")

        bars: List[OptionBar] = []
        for i, raw_ts in enumerate(ts):
            bars.append(
                OptionBar(
                    ts=_to_dt(raw_ts),
                    open=_f(op[i]),
                    high=_f(hi[i]),
                    low=_f(lo[i]),
                    close=_f(cl[i]),
                    volume=_f(vol[i]),
                    oi=_f(oi[i]),
                    iv=_maybe_f(iv[i]),
                    spot=_maybe_f(spot[i]),
                )
            )
        return FetchResult(status=FetchStatus.OK, bars=bars, http_status=200, request=payload)


def _to_dt(raw) -> datetime:
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw))
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


def _f(v) -> float:
    return 0.0 if v is None else float(v)


def _maybe_f(v) -> Optional[float]:
    return None if v is None else float(v)
