# Weekly CPR + EMA20, in bi-weekly NIFTY puts

What the three new tools do, what they were checked against, and where the
result can still be wrong.

## The rule under test

Weekly CPR drawn on the NIFTY 15m chart. While the market is below the CPR,
wait for it to come back up and tag the band, then buy a PE two weekly expiries
out. EMA20 vetoes the trade if price is above it. Profit trails down the
half-pivot ladder S0.5, S1, S1.5, S2 ...; a 15m candle closing back above the
trailing rung ends the trade, and a 15m close above the top of the CPR band
kills it before any rung is reached.

Levels are TradingView's Traditional pivots, computed from the **previous**
calendar week's high, low and close:

```
P  = (H + L + C) / 3            BC = (H + L) / 2          TC = 2P - BC
S1 = 2P - H                     S2 = P - (H - L)          S3 = 2P - (2H - L)
S4 = 3P - (3H - L)              S5 = 4P - (4H - L)
S0.5 = midpoint(P, S1)          S1.5 = midpoint(S1, S2)   ... and so on
```

## The three tools

| File | What it does |
|---|---|
| `tools/nifty_index_from_dhan.py` | Lifts NIFTY's own candles out of the `spot` stamped on every option row, 2021-01 → 2026-08. PhilForge's index cache only starts 2024-10. |
| `tools/nifty_expiry_calendar.py` | Weekly expiry dates and lot sizes back to 2021, and a check of the calendar against the option tape. |
| `tools/cpr_options_backtest.py` | The replay. |

```bash
python3 tools/cpr_options_backtest.py --trail-lag 1 --csv book.csv
python3 tools/cpr_options_backtest.py --slippage-pct 1.0     # spread sensitivity
python3 tools/cpr_options_backtest.py --strike-offset -2     # 100 points out of the money
```

## What was checked, and against what

**The index.** Compared bar for bar against PhilForge's own 15m cache over
11,254 overlapping bars: the close matches exactly, the wicks to about three
points median. The gap is sampling — `spot` only updates when some option
trades, so an extreme touched between prints is not recorded. The 09:15 bar is
the one systematic divergence: NSE's opening bar carries the pre-open
equilibrium print, which the option tape never sees.

**The expiry calendar.** Not trusted, measured. On a real expiry day the
nearest-expiry ATM contract is worth its intrinsic value at the close and
nothing more. Across all 294 computed expiries the median time value at 15:20+
is **Rs 0.08**; across the other 1,103 sessions it is **Rs 85.08**. The
calendar is right, holiday shifts included.

**Lot sizes** are keyed by the contract's expiry, with the steps settled
elsewhere: 75 through July 2021, 50, then 25 from 2024-04-26, 75 from
2025-01-02, 65 from 2026-01-06.

## Where it can still be wrong

- **Dhan carries about twelve strikes either side of the money.** A PE held
  while NIFTY falls more than ~600 points walks off the edge of the archive —
  and that is the winning trade. Those exits are floored at intrinsic value,
  which an in-the-money put is worth at minimum, and counted separately in the
  report. A floor is not a price; it understates.
- **There is no bid/ask in Dhan's data**, so the spread cannot be measured, only
  assumed. `--slippage-pct` is the assumption, and it is 0 by default so the
  headline is explicitly gross of spread.
- **One position at a time.** The rule fires about 30 times a year; the lock
  takes roughly 20 of them.
