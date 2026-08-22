# options/ — Indian options data layer

Acquires, stores and **audits** expired options history from DhanHQ. Nothing
here places an order.

## Why this exists

The previous options book was priced on Upstox's expired-options endpoint. That
endpoint answers `HTTP 200` with an empty candle array for contracts it does not
hold, and its expired-options history does not reach back the full period the
book covers. A loader that reads "no candles" as "nothing traded" cannot tell an
absent contract from a quiet one — so a backtest runs on a fraction of its
intended universe, raises nothing, and returns a number that looks like a result.

Every design choice here follows from that:

| Choice | Reason |
|---|---|
| `FetchResult.status` with an explicit `NO_DATA` | an absence is never a quiet market |
| a `coverage` ledger recording **every** request | you can ask, afterwards, what was never delivered |
| no zero-volume placeholder rows, ever | a placeholder is indistinguishable from an observation once written |
| ragged payloads raise | guessing during a long backfill builds a plausible, wrong archive |
| dated charge table | STT on options sales changed mid-window (0.0625% → 0.1%, 01-Oct-2024) |
| `audit.py`, run before any backtest | the archive proves itself; the vendor's claim doesn't |

## Usage

```bash
export DHAN_CLIENT_ID=...   DHAN_ACCESS_TOKEN=...

# 1. probe — one month, few strikes, so a known session can be eyeballed
python3 tools/dhan_backfill.py --underlying NIFTY --security-id 13 \
    --from 2025-01-01 --to 2025-01-31 --strikes 2 --max-requests 20

# 2. audit before believing any of it
python3 tools/dhan_backfill.py --audit-only --store data/options

# 3. full pull, only once the probe checks out (resumable; safe to kill)
python3 tools/dhan_backfill.py --underlying NIFTY --security-id 13 \
    --from 2021-08-01 --to 2026-08-01
```

### Auditing an archive this package did not write

Point it at the existing Upstox data — reads only, writes nothing:

```bash
python3 tools/audit_options_archive.py /path/to/upstox --underlying NIFTY
python3 tools/audit_options_archive.py archive.db --table candles
python3 tools/audit_options_archive.py data.csv --map ts=bar_time strike=strike_pr
```

Column names are auto-detected from a table of aliases; `--map` covers the rest.
Because a contract-keyed archive only knows where the money was if it carries
spot, the ATM basis is always reported: `EXACT` (spot on the row), `JOINED`
(via `--spot-file`), or `INFERRED` (most-traded strike per session). INFERRED is
enough to detect a hollow ATM and not enough to price anything, and says so.

## Unverified assumptions

`dhanhq.co` was unreachable from the build environment, so these came from the
official Python client and secondary sources. Each is asserted at runtime, so a
mismatch fails on call one rather than after a night of pulling. **Verify before
trusting a full backfill:**

- 5-year depth, minute granularity, ATM ±10 strikes, 30-day request window
- `POST /v2/charts/rollingoption` payload keys and the column-array response shape
- **How ATM is pinned** — per-day at open, or re-evaluated intraday as spot
  moves. This decides whether a rolling series is continuous or jumps
  mid-session, and no test here can settle it. Check it first.
- Charge rates in `charges.py` marked `VERIFY`

## Known limits of this dataset

- **No bid/ask, no depth.** OHLC only, so slippage is modelled, never measured.
  For options, where the spread is often the dominant cost, that is a real
  ceiling on what a backtest here can prove.
- **ATM ±10 only.** Far-OTM wings are absent. A strategy that ladders into
  distant strikes cannot be fully tested on this, and `dhan_client` refuses such
  requests rather than silently returning a gap.
- **Moneyness-keyed, not contract-keyed.** You address `ATM+2` of an expiry
  rank, never "the 24500 CE of 12-Jun-2025".
