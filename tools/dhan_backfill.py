#!/usr/bin/env python3
"""tools/dhan_backfill.py — pull expired options history, then audit it.

Run the probe before the full pull. Always.

    # 20 requests, one month, so you can eyeball a known session against a chart
    python3 tools/dhan_backfill.py --underlying NIFTY --security-id 13 \
        --from 2025-01-01 --to 2025-01-31 --strikes 2 --max-requests 20

    # audit what came back, before believing any of it
    python3 tools/dhan_backfill.py --audit-only --store data/options

    # the real thing, once the probe checks out
    python3 tools/dhan_backfill.py --underlying NIFTY --security-id 13 \
        --from 2021-08-01 --to 2026-08-01

Credentials come from DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN.
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options.audit import format_report  # noqa: E402
from options.backfill import BackfillPlan, run_backfill  # noqa: E402
from options.dhan_client import MAX_STRIKE_OFFSET, DhanClient  # noqa: E402
from options.store import OptionStore  # noqa: E402


def _d(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--store", default="data/options")
    p.add_argument("--underlying", default="NIFTY")
    p.add_argument("--security-id", default="13")
    p.add_argument("--segment", default="NSE_FNO")
    p.add_argument("--instrument", default="OPTIDX")
    p.add_argument("--from", dest="from_date", type=_d, default=date(2021, 8, 1))
    p.add_argument("--to", dest="to_date", type=_d, default=date.today())
    p.add_argument("--interval", default="1")
    p.add_argument(
        "--strikes",
        type=int,
        default=MAX_STRIKE_OFFSET,
        help=f"strike offsets each side of ATM (max {MAX_STRIKE_OFFSET})",
    )
    p.add_argument("--expiry-flags", nargs="+", default=["WEEK"])
    p.add_argument("--max-requests", type=int, default=None, help="cap the run — use for the first probe")
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--audit-only", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    store = OptionStore(args.store)

    if args.audit_only:
        print(format_report(store.load_bars(), store.load_coverage()))
        return 0

    client = DhanClient()
    plan = BackfillPlan(
        underlying=args.underlying,
        security_id=args.security_id,
        exchange_segment=args.segment,
        instrument=args.instrument,
        from_date=args.from_date,
        to_date=args.to_date,
        expiry_flags=tuple(args.expiry_flags),
        strike_offsets=tuple(range(-args.strikes, args.strikes + 1)),
        interval=args.interval,
    )
    report = run_backfill(client, store, plan, resume=not args.no_resume, max_requests=args.max_requests)
    print(f"\nbackfill: {report.summary()}\n")
    print(format_report(store.load_bars(), store.load_coverage()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
