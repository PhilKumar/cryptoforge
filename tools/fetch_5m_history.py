"""
tools/fetch_5m_history.py — pull whole months of 5m klines from Binance's
public archive into one compact cache file per symbol.

The REST klines endpoint pages 1000 bars at a time; two years of 5m is ~210,000
bars, or 210 round trips per symbol. data.binance.vision publishes the same
bars as one zip per month, so 24 requests do the whole job and the result is
byte-identical to what the API returns.

    .venv/bin/python tools/fetch_5m_history.py BTCUSDT SOLUSDT --months 24

Writes tools/.history_cache/<SYMBOL>_5m.json: a list of
[open_ts_sec, open, high, low, close], ascending, de-duplicated.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import zipfile
from datetime import date, datetime, timezone
from typing import List, Tuple

import requests

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".history_cache")
BASE = "https://data.binance.vision/data/spot/monthly/klines/{sym}/5m/{sym}-5m-{ym}.zip"


def months_back(count: int, end: date) -> List[str]:
    """The `count` complete months ending with the one before `end`'s month."""
    out: List[str] = []
    year, month = end.year, end.month
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        out.append(f"{year:04d}-{month:02d}")
    return list(reversed(out))


def _to_seconds(raw: str) -> int:
    """Binance switched kline timestamps from ms to microseconds in 2025."""
    value = int(float(raw))
    return value // 1_000_000 if value > 10**14 else value // 1000


def fetch_month(symbol: str, ym: str) -> List[Tuple[int, float, float, float, float]]:
    url = BASE.format(sym=symbol, ym=ym)
    resp = requests.get(url, timeout=120)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    rows: List[Tuple[int, float, float, float, float]] = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        name = archive.namelist()[0]
        with archive.open(name) as handle:
            for line in io.TextIOWrapper(handle, encoding="utf-8"):
                parts = line.strip().split(",")
                if not parts or not parts[0] or parts[0][0].isalpha():
                    continue  # 2025+ files carry a header row
                rows.append((_to_seconds(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])))
    return rows


def load(symbol: str, months: int = 24, end: date | None = None, refetch: bool = False) -> List[tuple]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{symbol}_5m.json")
    wanted = months_back(months, end or date.today())
    # The cache is one file however deep it is, so a caller asking for 24
    # months must get 24 even when 36 are stored — and a cache shallower than
    # the ask falls through to a fetch instead of silently short-changing it.
    first_year, first_month = (int(x) for x in wanted[0].split("-"))
    start_ts = int(datetime(first_year, first_month, 1, tzinfo=timezone.utc).timestamp())
    if os.path.exists(path) and not refetch:
        with open(path, "r", encoding="utf-8") as handle:
            rows = [tuple(row) for row in json.load(handle)]
        if rows and rows[0][0] <= start_ts + 86400:
            return [row for row in rows if row[0] >= start_ts]
    seen: dict = {}
    for ym in wanted:
        rows = fetch_month(symbol, ym)
        print(f"  {symbol} {ym}: {len(rows):>6,} bars")
        for row in rows:
            seen[row[0]] = row
    ordered = [seen[ts] for ts in sorted(seen)]
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(ordered, handle)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="+")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--refetch", action="store_true")
    args = parser.parse_args()
    for symbol in args.symbols:
        rows = load(symbol.upper(), args.months, refetch=args.refetch)
        if rows:
            first, last = rows[0][0], rows[-1][0]
            print(f"{symbol.upper()}: {len(rows):,} bars, {first} .. {last}")
        else:
            print(f"{symbol.upper()}: nothing fetched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
