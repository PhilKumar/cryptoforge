"""options/backfill.py — pull five years of expired options, resumably.

The shape of the job: for each underlying, for each expiry type (weekly and
monthly), for each expiry rank, for each strike offset in ATM +/-N, for calls
and puts, walk the date range in 30-day windows. That is a large product —
roughly 61 windows per series per five years — so the driver is built around
two properties rather than speed:

**It resumes.** Every attempted window is in the coverage ledger, so a killed
run picks up where it stopped. Re-asking for a window the source already
answered with nothing is pure waste, so NO_DATA counts as attempted.

**It never infers.** A window that comes back empty writes a ledger row and no
bars. It does not write zero-volume placeholder rows, because a placeholder is
indistinguishable from an observation once it is in the file, and that
indistinguishability is exactly what made the last dataset untrustworthy.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterator, List, Optional, Sequence

from options.dhan_client import (
    MAX_STRIKE_OFFSET,
    DhanClient,
    FetchStatus,
    iter_windows,
)
from options.store import OptionStore, SeriesKey

log = logging.getLogger(__name__)


@dataclass
class BackfillPlan:
    """What to pull. Kept explicit rather than defaulted to 'everything',
    because 'everything' is thousands of requests and the first run should be
    a probe you can verify by hand."""

    underlying: str
    security_id: str
    exchange_segment: str = "NSE_FNO"
    instrument: str = "OPTIDX"
    from_date: date = date(2021, 1, 1)
    to_date: date = date.today()
    expiry_flags: Sequence[str] = ("WEEK", "MONTH")
    expiry_codes: Sequence[int] = (0,)
    strike_offsets: Sequence[int] = tuple(range(-MAX_STRIKE_OFFSET, MAX_STRIKE_OFFSET + 1))
    option_types: Sequence[str] = ("CALL", "PUT")
    interval: str = "1"

    def series(self) -> Iterator[SeriesKey]:
        for flag in self.expiry_flags:
            for code in self.expiry_codes:
                for off in self.strike_offsets:
                    for opt in self.option_types:
                        yield SeriesKey(
                            underlying=self.underlying,
                            expiry_flag=flag,
                            expiry_code=code,
                            strike_offset=off,
                            option_type=opt,
                            interval=self.interval,
                        )


@dataclass
class BackfillReport:
    requested: int = 0
    ok: int = 0
    no_data: int = 0
    errors: int = 0
    skipped: int = 0
    bars_written: int = 0

    def summary(self) -> str:
        delivered = f"{self.ok}/{self.requested - self.skipped}" if self.requested else "0/0"
        pct = 100.0 * self.no_data / (self.requested - self.skipped) if self.requested - self.skipped else 0.0
        return (
            f"requested={self.requested} skipped(resumed)={self.skipped} "
            f"delivered={delivered} empty={self.no_data} ({pct:.1f}%) "
            f"errors={self.errors} bars={self.bars_written:,}"
        )


def run_backfill(
    client: DhanClient,
    store: OptionStore,
    plan: BackfillPlan,
    *,
    resume: bool = True,
    max_requests: Optional[int] = None,
) -> BackfillReport:
    """Execute a plan. `max_requests` exists so the first run can be a 20-call
    probe — pull a month you can check against a chart before spending a night
    on five years."""
    report = BackfillReport()
    done = store.completed_windows() if resume else set()
    pending_coverage: List[dict] = []

    for key in plan.series():
        for start, end in iter_windows(plan.from_date, plan.to_date):
            if max_requests is not None and report.requested >= max_requests:
                store.write_coverage(pending_coverage)
                return report

            fingerprint = (
                key.underlying,
                key.expiry_flag,
                key.expiry_code,
                key.strike_offset,
                key.option_type,
                key.interval,
                start.isoformat(),
                end.isoformat(),
            )
            report.requested += 1
            if fingerprint in done:
                report.skipped += 1
                continue

            result = client.fetch_expired_option_window(
                security_id=plan.security_id,
                exchange_segment=plan.exchange_segment,
                instrument=plan.instrument,
                expiry_flag=key.expiry_flag,
                expiry_code=key.expiry_code,
                strike_offset=key.strike_offset,
                option_type=key.option_type,
                from_date=start,
                to_date=end,
                interval=key.interval,
            )

            pending_coverage.append(
                {
                    "requested_at": datetime.now(),
                    "underlying": key.underlying,
                    "expiry_flag": key.expiry_flag,
                    "expiry_code": key.expiry_code,
                    "strike_offset": key.strike_offset,
                    "option_type": key.option_type,
                    "interval": key.interval,
                    "from_date": start.isoformat(),
                    "to_date": end.isoformat(),
                    "status": result.status.value,
                    "http_status": result.http_status,
                    "bars_returned": len(result.bars),
                    "detail": result.detail[:200],
                }
            )

            if result.status is FetchStatus.OK:
                report.ok += 1
                report.bars_written += store.write_bars(key, result.bars)
            elif result.status is FetchStatus.NO_DATA:
                report.no_data += 1
            else:
                report.errors += 1
                log.warning(
                    "fetch failed %s %s %s..%s: %s",
                    key.underlying,
                    key.option_type,
                    start,
                    end,
                    result.detail,
                )

            if len(pending_coverage) >= 200:
                store.write_coverage(pending_coverage)
                pending_coverage = []

    store.write_coverage(pending_coverage)
    return report
