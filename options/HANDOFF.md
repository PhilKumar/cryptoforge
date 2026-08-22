# Handoff — options data work

Plain-English summary of a cloud session, for picking up locally.

## The question we started with

"How many months of options data does Zerodha have?"

**Answer: none for expired contracts.** Zerodha deletes an option contract's
identity when it expires, so its history becomes unreachable. You can only get
data for contracts that have not expired yet — about 3 months at most.

## What we found along the way

**1. Dhan is different — it has 5 years.** DhanHQ sells expired options history:
5 years, minute-by-minute, ATM ±10 strikes, including IV, open interest and the
spot price. Costs ₹499+tax/month for their Data API. This is the best available
option and is why the new code targets Dhan.

**2. Upstox — which the old book was built on — has serious gaps.** Its expired
options data appears to start only around **October 2024**, not years back. Worse,
when it has nothing it replies "success" with an empty result. Any loader that
reads "empty" as "nothing traded that day" will silently run a backtest on a
fraction of the intended data and produce a number that looks real. This is the
most likely reason the earlier tests failed or gave odd results.

**3. A question about the published ledger.** The site
(`media/landing/DOJIMA_LANDING.html`) publishes a 5-year book, Jan 2021 → Aug 2026,
891 trading days, ₹11,60,571 net. Its numbers all reconcile. The page says the
trades were "re-priced against real Upstox option premiums."

But Upstox appears not to hold data before ~Oct 2024, and the book's own regime
break sits at 2024-11-01:

| | Jan 2021 – Oct 2024 | Nov 2024 – Aug 2026 |
|---|---|---|
| days | 602 | 289 |
| net | ₹3,81,192 | ₹7,79,398 |
| average per day | ₹633 | ₹2,697 |
| days above ₹30k | 0 | 7 |

The line between "Upstox has data" and "Upstox has none" falls almost exactly on
the line between the two regimes — and the era with no obvious data source is the
flat one. **This is a question, not an accusation.** The premiums may have come
from a source the page does not name. Full write-up in `PROVENANCE_REVIEW.md`.

## What was built (all on branch `claude/dhan-options-backfill`)

The branch name says "zerodha" only because it was auto-generated from the first
question. All the code is Dhan.

| File | What it does |
|---|---|
| `options/dhan_client.py` | Talks to Dhan's expired-options API. Its core rule: an empty reply is reported as `NO_DATA`, never as a quiet market. |
| `options/store.py` | Saves bars as parquet, **plus a coverage ledger recording every request including empty ones** — so you can later ask "what did the vendor never give us?" |
| `options/backfill.py` | Pulls 5 years in 30-day chunks. Resumable. Never writes fake placeholder rows. |
| `options/audit.py` | Checks whether an archive is trustworthy: completeness by month, and by distance from ATM. |
| `options/adapters.py` | Lets the audit read **someone else's** archive (csv/parquet/sqlite, any column names) — so it works on the existing Upstox data. |
| `options/charges.py` | Indian options charges with date-aware rates (options STT rose 0.0625% → 0.1% on 01-Oct-2024). |
| `tools/dhan_backfill.py` | Command line: pull data from Dhan. |
| `tools/audit_options_archive.py` | Command line: audit any archive. Reads only. |
| `tests/test_options_data_layer.py` | 20 tests, all passing. |

**Why the audit matters:** sparse data far from the money is normal (nothing
traded). Sparse data *at* the money is a hole in the vendor's archive — and that
is exactly where the profit and loss lives. The audit separates the two.

## What could not be done in the cloud session

1. `api.dhan.co` is blocked by the cloud sandbox's firewall (confirmed: 403).
2. No Dhan credentials.
3. The Dōjima strategy rules are not in this repo, so there was nothing to re-run.

None of these block a local machine.

## Next steps, in order

**Step 1 — audit the existing Upstox data.** No subscription, no credentials,
nothing uploaded. This tells you how much of the published book had real data
underneath it.

```bash
pip install pandas pyarrow
python3 tools/audit_options_archive.py /path/to/upstox/data --underlying NIFTY
```

If columns are not auto-detected: `--map ts=your_time_col strike=your_strike_col`

**Step 2 — decide what the 5-year re-run means.** If the audit shows the early
years had no data, the re-run is a rebuild, not a restatement — and the live
site's provenance sentence needs correcting before new numbers go up.

**Step 3 — Dhan probe.** Subscribe (₹499/mo), set credentials, pull one month
first and check it against a chart by hand:

```bash
export DHAN_CLIENT_ID=...  DHAN_ACCESS_TOKEN=...
python3 tools/dhan_backfill.py --underlying NIFTY --security-id 13 \
    --from 2025-01-01 --to 2025-01-31 --strikes 2 --max-requests 20
python3 tools/dhan_backfill.py --audit-only --store data/options
```

**Step 4 — full 5-year pull**, only if the probe looks right. Resumable.

**Step 5 — re-run the strategy** on audited data.

## Things to verify (could not be checked — Dhan's docs were firewalled)

- 5-year depth, ATM ±10, 30-day window, minute granularity
- The exact request/response shape of `POST /v2/charts/rollingoption`
- **How Dhan defines ATM** — fixed each morning, or moving during the day? This
  decides whether the series is continuous or jumps mid-session. Check first.
- Charge rates marked `VERIFY` in `charges.py`

All of these are asserted in code, so a wrong one fails on the first call rather
than after a night of downloading.

## Two known limits of Dhan's data

- **No bid/ask prices** — only OHLC. So trading costs from the spread have to be
  estimated, not measured. For options that is a real limitation.
- **ATM ±10 strikes only** — far-out strikes are not included. If the strategy
  ladders into distant strikes, it cannot be fully tested on this data.

## Housekeeping

- Branch is 3 commits behind `origin/main`; merge main in before continuing.
- Branch name is misleading (says zerodha, is dhan) — safe to rename.
- **Never commit** `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN`. `.env` is gitignored.
- Don't commit data archives either — `*.csv` is gitignored and a 5-year minute
  archive is tens of GB.
