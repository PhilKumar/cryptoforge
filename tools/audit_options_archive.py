#!/usr/bin/env python3
"""tools/audit_options_archive.py — is this archive safe to backtest on?

Runs against any options archive, not just one this package wrote. Point it at
the existing Upstox data and it answers the question that was never asked of it:
how much of that book actually had data underneath it.

    # a directory of parquet/csv, or a single file
    python3 tools/audit_options_archive.py /path/to/upstox --underlying NIFTY

    # sqlite: it will list the tables if you omit --table
    python3 tools/audit_options_archive.py archive.db --table candles

    # when column names are not auto-detected
    python3 tools/audit_options_archive.py data.csv \
        --map ts=bar_time,strike=strike_pr,option_type=right

Reads only. Nothing is written, moved or modified.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from options.adapters import load_external  # noqa: E402
from options.audit import format_report  # noqa: E402


def _parse_map(pairs):
    out = {}
    for item in pairs or []:
        if "=" not in item:
            raise SystemExit(f"--map expects canon=column, got {item!r}")
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("path", help="file or directory holding the archive")
    p.add_argument("--table", default=None, help="sqlite table name")
    p.add_argument("--underlying", default="UNKNOWN")
    p.add_argument("--interval", default="1", help="bar interval: 1, 5, 15, 25, 60")
    p.add_argument("--map", nargs="*", default=None, help="column overrides, e.g. ts=bar_time strike=strike_pr")
    p.add_argument(
        "--spot-file",
        default=None,
        help="optional underlying series (csv/parquet with ts,spot) to join, when the archive carries no spot column",
    )
    args = p.parse_args(argv)

    spot = None
    if args.spot_file:
        spot = pd.read_parquet(args.spot_file) if args.spot_file.endswith(".parquet") else pd.read_csv(args.spot_file)

    res = load_external(
        args.path,
        table=args.table,
        column_map=_parse_map(args.map),
        underlying_name=args.underlying,
        spot_series=spot,
        interval=args.interval,
    )
    print(f"loaded {res.rows_out:,} of {res.rows_in:,} rows from {args.path}")
    print(res.caveat())
    for n in res.notes:
        print(f"  note: {n}")
    print()
    print(format_report(res.frame, pd.DataFrame()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
