# Provenance review — the published five-year ledger

Written while building `options/`. Factual notes, for a decision.

## What the site publishes

`media/landing/DOJIMA_LANDING.html` carries a day-by-day series (`SER`) of 891
trading days, Jan 2021 → Aug 2026. Its figures reconcile exactly with the prose
claims on the same page:

| Claim on page | Series | Match |
|---|---|---|
| 891 trading days | 891 rows | ✓ |
| first 46 months: ₹3,81,178 | cum. at 2024-10-31 = ₹3,81,178 | ✓ |
| last 21 months: ₹7,79,393 | ₹11,60,571 − ₹3,81,178 | ✓ |
| net ₹13,29,567 gross − ₹1,68,996 charges | final cum. ₹11,60,571 | ✓ |

So the series *is* the published book. The page states these trades were
"re-priced against real Upstox option premiums."

## The issue

Upstox's expired-options history does not reach back over most of that window.
Its `Get Expiries` endpoint returns roughly six months of historical expiries,
and developers report the expired-instruments data returning nothing before
**October 2024** — with September 2024, June 2024 and January 2024 all empty.

The book's own regime break sits at **2024-11-01** (`RG` in the page source).
The two eras behave very differently:

| | Jan 2021 – Oct 2024 | Nov 2024 – Aug 2026 |
|---|---|---|
| days | 602 | 289 |
| net | ₹3,81,192 | ₹7,79,398 |
| mean/day | ₹633 | ₹2,697 |
| stdev | ₹7,956 | ₹13,989 |
| best day | ₹29,701 | ₹95,254 |
| days > ₹30k | 0 | 7 |

The boundary between "data Upstox appears to hold" and "data it appears not to
hold" falls within days of the boundary between the two statistical regimes,
and the era lacking a plausible source is the flat one.

## What this does not establish

That the pre-Nov-2024 figures are wrong, or that any source was misrepresented.
The data may have come from a vendor or archive not named on the page. This note
records a discrepancy between a stated provenance and a source's documented
coverage — nothing more.

## What to resolve, in order

1. **Where did the Jan 2021 – Oct 2024 premiums come from?** If not Upstox, the
   provenance sentence on the live page needs correcting. It is shown to
   prospective buyers and investors.
2. **Re-derive that era from a source that demonstrably holds it**, and compare.
   `options/` exists to make that possible; `options/audit.py` proves coverage
   before the comparison is run.
3. **Only then restate the ledger.** A new five-year number published on top of
   an unresolved provenance question inherits the question.
