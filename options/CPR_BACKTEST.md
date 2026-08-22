# CPR + EMA20, in bi-weekly NIFTY puts

## The rule that survived

```bash
python3 tools/cpr_options_backtest.py
```

No arguments. The defaults **are** the rule:

| | |
|---|---|
| Chart | NIFTY 5m |
| Levels | today's daily CPR, drawn from yesterday's high, low and close |
| Entry | a candle wicks above **R1 or R2** and closes back under it, while closing under **EMA20** |
| Buy | ATM+2 PE (100 points in the money), second-nearest weekly expiry, 1 lot |
| Stop | a close above the **quarter rung** (~20 points above entry) |
| Target | trails down the half rungs — R0.5, TC, BC, S0.5, S1 … — exit on a close back above the deepest rung reached |
| Square off | 15:15, always. Nothing is carried overnight |
| Skip | any entry costing more than ₹25,000 |

**154 trades, 2021-01-04 → 2026-07-31. Net ₹59,768. Win rate 51.9%. Profit
factor 2.17. Worst drawdown ₹4,788.** Peak premium ₹24,938, so about ₹30,000 of
capital — roughly **35.7% a year before spread**.

| Spread assumed | Net | PF | Max DD | %/yr | %/yr since 2023 |
|---|---|---|---|---|---|
| none | ₹59,768 | 2.17 | −₹4,788 | 35.7% | 21.0% |
| 0.25% a leg | ₹50,932 | 1.93 | −₹5,728 | 29.4% | 15.8% |
| 0.5% a leg | ₹42,097 | 1.71 | −₹7,556 | 22.9% | 10.6% |
| 1% a leg | ₹24,426 | 1.36 | −₹11,221 | 11.9% | 1.8% |

Every exit is priced — **154 of 154** — because a position squared off at 15:15
cannot walk off the edge of Dhan's ATM±12 band. And the profit is not one trade:
the best is 12% of the net, the top three are 30%, and without them it still
makes ₹41,885.

Honest caveats: 2021 is ₹29,036 of the ₹59,768, the run rate since 2023 is about
21% before spread, and 154 trades in 5.6 years is 27 a year.

## Roads not taken

Each is reachable through a flag, and each was measured and was worse.

| Change | Flag | Result |
|---|---|---|
| Weekly CPR, 15m, held overnight | `--pivots weekly --bar-minutes 15 --no-intraday` | +₹54,347 headline, but the 88 priced exits **lose ₹79,132** — the profit was five trades Dhan cannot price |
| Enter at the CPR instead of R1/R2 | `--entry-rungs BC` | −₹4,030 |
| 5m on the weekly CPR | `--bar-minutes 5 --pivots weekly` | −₹20,642 |
| Stop on the rung above, not the quarter | `--stop-fraction 1.0` | ₹60,894 but a bigger drawdown, 34.9%/yr |
| Stop on the entry candle's high | `--stop entry-high` | 81 stop-outs instead of 47; 25.7%/yr |
| Trail every quarter rung | `--ladder-step 0.25` | less money, smallest drawdown (₹3,521), best recent run rate |
| Out-of-the-money strikes | `--strike-offset -2` | 9.4%/yr, and dies on spread |
| Supertrend(10, 1.7), 2nd touch of the flat line | `--entry-mode supertrend` | −₹47,552. The touch **precedes an upward flip** 70% of the time within six bars |
| The same, buying calls | `--entry-mode supertrend --side CE` | −₹7,034, PF 0.92 — better, because the direction was the problem, but the exits still point down |
| No EMA filter | `--ema 0` | at R1/R2 it is the whole trade: −₹16,970 without it |

## The two data tools underneath

| File | What it does |
|---|---|
| `tools/nifty_index_from_dhan.py` | Lifts NIFTY's candles out of the `spot` on every option row, 2021-01 → 2026-08. Matches PhilForge's cache exactly on the close, ~3 points on wicks. The 09:15 bar is the one divergence: NSE's opening bar carries a pre-open print the option tape never sees. |
| `tools/nifty_expiry_calendar.py` | Weekly expiries and lot sizes back to 2021, **proved against the tape**: median time value at the close is ₹0.08 across all 294 expiry days against ₹85.08 across the other 1,103. |

## What is still unmeasured

- **There is no bid/ask in Dhan's data**, so spread is assumed, not observed. The
  headline is gross of it; the table above is the sensitivity, and it matters.
- The backtest **refuses to run** unless `options/dhan_listed.py` carries the
  wrong-instrument filter, because without it a NIFTY book is priced partly off
  another index. That filter is still uncommitted in the working tree.
