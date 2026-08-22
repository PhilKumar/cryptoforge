# EMA20 on NIFTY, in weekly options

Phil's rule: *a candle closes above the EMA20 while the EMA rises at 30 degrees
— buy a call, hold it until a candle closes back below. Mirror it for puts.*

```bash
python3 tools/ema_options_backtest.py --entry armed --intraday --min-angle 20 --strike-offset 4
```

**Verdict in one line: the rule as literally stated cannot fire, but the instinct
under it is right — and once the contract carries enough delta, it makes money.**

| | |
|---|---|
| **358 trades**, 2021-01-11 → 2026-08-04 | **net ₹112,953** |
| win 46% · profit factor **1.21** | max drawdown ₹47,572 |
| peak premium ₹30,596 | honest capital ~₹78,200 |
| **25.9%/yr simple · 17.4% CAGR** | ₹62,747 after 0.5% spread a leg |

---

## 1. What "30 degrees" had to become

An angle on a chart is not a property of the market — stretch the window and the
same EMA reads 15° or 60°. So the slope is divided by one bar's ATR, which is a
chart drawn so **one bar of width is one ATR of height**:

    tan(angle) = (EMA points gained per bar) / ATR(14)

Over 522,205 minutes of NIFTY (2021-01 → 2026-08) that scale is very nearly
**timeframe-free**, which is the test a normalisation has to pass before a
threshold on it means anything:

| \|slope\|/ATR | 5m | 15m | 30m | 1h | degrees |
|---|---|---|---|---|---|
| median | 0.146 | 0.147 | 0.146 | 0.147 | **8.4** |
| p90 | 0.357 | 0.344 | 0.343 | 0.340 | **19.0** |
| p99 | 0.573 | 0.553 | 0.512 | 0.516 | **28.5** |
| steepest | — | — | 0.970 | 0.844 | **~42** |

## 2. The rule as literally stated returns an empty book

At the moment price crosses an EMA20, **the EMA is flat by construction** — it
has just been dragged along by the very closes now crossing it.

| 15m, slope measured over | median angle at a cross | p99 | steepest ever |
|---|---|---|---|
| 1 bar | +2.2° | 22.4° | 39.5° |
| 3 bars | **−0.6°** | 6.8° | **15.3°** |

Asking for 30° *on the crossing candle* refuses **3,866 of 3,869 crosses**. The
screenshot Phil sent shows the same thing: the candle closes through a flat EMA,
and the 30-degree rise happens several candles later.

`--entry armed` is that second reading — the cross puts the trade on watch, and
it opens when the EMA actually reaches the angle. Everything below uses it.

## 3. Steepness is the whole filter

| 15m, ATM weekly, intraday | trades | net |
|---|---|---|
| plain cross, EMA merely not falling against you | 1,405 | **−₹152,130** (PF 0.85) |
| the same entry, angle ≥ 20° | 358 | **+₹45,777** (PF 1.12) |

Phil's instinct is correct and it is worth ₹198,000 across the window. The
number 30 is unreachable; the *idea* behind it is the edge.

## 4. Why ATM was never going to be enough

The signal captures real index movement. ATM options only hand back half of it:

| | |
|---|---|
| index points captured | **+4,206** (avg 11.7, median 0.6) |
| premium points captured | **+1,605** (avg 4.5, median −8.2) |
| realised delta | 0.49 on winners, 0.53 on losers — no convexity at this horizon |

**The hurdle.** What the index has to do before an ATM option breaks even:

| index moved | trades | premium captured | net |
|---|---|---|---|
| < −40 pts | 112 | −44.1 pts | −₹289,647 |
| −40 → −20 | 32 | −23.1 pts | −₹42,026 |
| −20 → 0 | 33 | −14.1 pts | −₹28,283 |
| **0 → +20 (moved YOUR way)** | 27 | **−5.8 pts** | **−₹12,118** |
| +20 → +40 | 34 | +1.6 pts | +₹4,480 |
| > +40 | 120 | +65.5 pts | +₹413,371 |

An ATM option needs **~20 index points just to stop losing**, and the signal's
average trade is 11.7. That is a diagnosis with a prediction attached: *buy more
delta.*

## 5. The prediction holds — depth is monotonic and saturating

15m, angle ≥ 20°, intraday, nearest weekly. Same 358 entries throughout; only
the strike changes.

| strike | net | win | PF | max DD | peak premium |
|---|---|---|---|---|---|
| ATM | ₹45,777 | 40% | 1.12 | −₹44,354 | ₹21,075 |
| ATM+2 ITM | ₹88,179 | 44% | 1.18 | −₹45,109 | ₹25,650 |
| **ATM+4 ITM** | **₹112,953** | 46% | **1.21** | −₹47,572 | ₹30,596 |
| ATM+6 ITM | ₹121,678 | 46% | 1.21 | −₹54,814 | ₹36,105 |

Monotonic, and it saturates between +4 and +6 exactly where delta does. **ATM+4
is the pick** — +6 buys ₹8,725 more for ₹5,500 more premium and ₹7,242 more
drawdown.

The *expiry* axis is not so clean. The second weekly helps at ATM (₹82,878, PF
1.25) and at ATM+4 (₹115,832, PF 1.29) but collapses at ATM+2 (₹18,555, PF 1.04,
drawdown ₹74,362). One bad cell in an otherwise coherent surface is a warning:
**take the depth effect, do not take the expiry effect.**

## 6. Does it survive contact?

| test | result | reading |
|---|---|---|
| spread 0.25% a leg | ₹87,850 (PF 1.16) | survives |
| spread 0.50% a leg | ₹62,747 (PF 1.11) | ~13%/yr — still a strategy |
| **spread 1.00% a leg** | ₹12,541 (PF 1.02) | **dead** |
| first half 2021-01 → 2023-10 | ₹57,341 (PF 1.22) | |
| second half 2023-11 → 2026-08 | ₹55,612 (PF 1.19) | **near-identical, no refitting** |
| drop the top 3 trades | still **+₹54,574** | 52% of net, but not load-bearing |
| by year | +55.7k, +9.1k, **−2.7k**, +18.1k, +17.0k, +15.7k | one losing year in six |

**Angle is a plateau at ATM+4, not a ridge:**

| angle | 16° | 18° | 19° | 20° | 21° | 22° | 24° |
|---|---|---|---|---|---|---|---|
| net (₹k) | 73.8 | 75.7 | 117.8 | **113.0** | 83.9 | 55.4 | 83.3 |
| PF | 1.08 | 1.10 | 1.19 | **1.21** | 1.19 | 1.14 | 1.34 |

Every setting from 16° to 24° is positive. At ATM this was a narrow ridge (18°
gave PF 1.02); the delta is what turns the ridge into a plateau.

## 7. The exit is still the biggest thing left on the table

| exit | trades | win | net |
|---|---|---|---|
| squared off 15:15 | 211 | **72%** | **+₹553,154** |
| **closed through the EMA** | 147 | **9%** | **−₹440,201** |

A 9% win rate on the rule's own exit. The EMA is a lagging average, so waiting
for a close through it means waiting out the whole round trip of the move. **The
measurable next question is not the angle — it is whether the exit can come off
the EMA while the entry stays exactly as it is.**

## 8. Roads taken, all measured and worse

| variation | result |
|---|---|
| 5-minute candles | loses at every angle (−₹193k at 10°, −₹107k at 20°) |
| 30-minute candles | flat to losing (−₹2,029 at 20°) |
| 60-minute candles | +₹17,500 at 20° on 104 trades — too few to judge |
| held overnight | headline +₹105k, but its **priced** net is −₹66,702 — all of it is exits floored at intrinsic, off Dhan's strike band |
| angle 30° (armed) | +₹17,440, PF 1.34, but 43 trades in 5.6 years decides nothing |

## 9. What each archive can and cannot say

| | Dhan | Upstox |
|---|---|---|
| reach | 2021-01 → 2026-08 | **2024-10 → 2026-08 only** |
| keyed by | moneyness, ATM±12 | real contracts, real strikes |
| the flaw | stops quoting a contract exactly when the trade is winning | holds only the strikes PhilForge once fetched |
| served here | **356 of 358 exits priced** | **136 of 456 lookups — 29.8%** |

**The Upstox cross-check is weaker than it looks.** It could price only 68 of
roughly 2,000 signals — 320 entries refused because the archive never held that
strike, 1,602 outside its expiries. That book loses (−₹21,033), which *agrees*
with the ATM Dhan reading, but its sample is selected by which contracts the
Dōjima book once traded, not at random. Corroboration, not proof. See
[[proj_upstox_archive_coverage_audit]].

Squaring off at 15:15 is what makes the Dhan book honest — a position closed the
same day cannot walk off the ATM±12 band.

## 10. Honesty rules kept

* A contract is priced from the minute asked for, never a neighbouring one — on
  Dhan a miss means the strike left the ATM band, and an adjacent minute returns
  a price that minute never had. On Upstox a miss on a contract the archive
  *holds* is a minute that did not print, so the same contract's next print
  inside the session is allowed, and counted.
* An exit off the edge of the archive is floored at intrinsic and reported
  separately. A floor is not a price.
* An out-of-the-money exit with no quote is **dropped**, not booked at zero.

### Verified against the bars

`0 of 358` entries and `0 of 147` EMA exits mis-fire; no two positions overlap;
lot sizes track NIFTY's real history (75 → 50 → 25 → 75); brokerage lands at
₹55–67 a round trip. That check caught two bugs of mine — an entry whose fill
landed on the square-off minute itself, and a strike offset applied *with* the
trade's direction instead of against it, which bought the cheap out-of-the-money
wing and called it delta. The second one inverted an entire finding.

### Caveats to carry

* **2021 is ₹55,708 of the ₹112,953** — half the money is the first year.
* Deep ITM has wider real quotes than ATM, and the 1%-a-leg column is where an
  ITM book actually lives. There it is PF 1.02.
* 358 trades in 5.58 years is 64 a year.
