# Executor recovery: the sleeping laptop

Companion to `CASCADE_SIGNAL_FORMAT.md`. How a buyer's local executor survives
sleep, shutdown, crash, and network loss without leaving money unmanaged.

Designed for a personal machine — laptop or desktop — with no always-on server
anywhere in the picture.

## Why this is smaller than it looks

Three properties of Cascade bound the damage before any code is written.

**1. It's spot, not futures.** `BinanceSpotClient`, `supports_funding = False`.
No leverage, no margin call, no liquidation. An unmanaged Cascade position
cannot be wiped out — it can only be held longer than intended. The worst case
of a sleeping laptop is a missed exit and a longer hold, not a blowup.

**2. A stopped engine places no new orders.** The pot only accumulates inside
`_collect_crossed_levels`, which runs in the tick loop. No loop, no new orders.
A sleeping executor cannot deploy more capital — only already-resting orders can
fill.

**3. There is at most one accumulated entry order at a time.**
`campaign.pending_order_id` is singular. Crossed levels merge into one buy stop
rather than resting as a ladder.

Together: **maximum unmanaged exposure equals the notional of the currently
resting entry order(s), and that number is known before the machine sleeps.**

## The twin sleep invariants

Everything else in this document serves these two rules. Both are checkable
before sleep, both are cheap, and together they mean that whatever happens while
the machine is off is something the buyer already wanted.

> **1. Never sleep with an entry order resting.** Cancel them.
>
> **2. Never sleep holding coin without an exit order resting.** Place it.

The first is unintuitive in one direction and the second in the other, so both
are worth stating plainly:

| Order type | On shutdown | Why |
|---|---|---|
| Entry (buy stop, level-8 limit) | **Cancel** | A fill with nothing watching creates a position with no TP against it |
| Exit (TP limit sell) | **Leave resting — and place one if missing** | It can only close a position at a price we already chose, and it needs nothing watching it |

A resting take-profit is the one order that is *safer* left alone. If price
rallies through target at 3am, it takes the exit and the buyer wakes up flat and
profitable. Cancelling it converts a good outcome into a missed one.

Entries are the reverse. Cost of cancelling them is a missed entry if price
crosses the trigger while down — opportunity cost only, re-placed on the next
tick after wake.

Invariant 2 has a live-engine precedent: a campaign can be holding coin with no
TP resting (min-notional notice, or a TP cancelled mid-re-place). Sleeping in
that state is the one genuinely bad configuration, and it is entirely
preventable.

## What each platform actually gives you

This is the load-bearing part, and the three platforms are not equal.

| | Prevent idle sleep | Warning before forced sleep | Usable? |
|---|---|---|---|
| macOS | `IOPMAssertionCreateWithName` (`kIOPMAssertionTypePreventUserIdleSystemSleep`) | `kIOMessageSystemWillSleep` via `IORegisterForSystemPower`, ack within ~30s | Yes — comfortable |
| Linux | `systemd-inhibit --what=sleep --mode=delay` | `InhibitDelayMaxSec`, default 5s, configurable higher | Yes — adequate |
| Windows | `SetThreadExecutionState(ES_CONTINUOUS \| ES_SYSTEM_REQUIRED)` | `PBT_APMSUSPEND`, ~2s on Windows 8+ | **Barely** |

Two consequences.

**Lid close cannot be prevented on any of them.** An idle-sleep assertion stops
the machine dozing off; it does not stop a user shutting the lid. What you get
is a *window*, not a veto — so the design is "do the cancels inside the window",
never "keep it awake."

**Windows cannot reliably cancel on suspend.** Two seconds is not enough for a
round trip to Binance plus confirmation. So on Windows the invariants are
enforced *continuously* rather than at suspend time:

- Hold the execution-state lock whenever any entry order rests, so idle sleep
  never fires while armed.
- Treat forced sleep as equivalent to a crash — the wake ladder handles it.
- Surface the armed exposure more prominently than on the other platforms,
  because there the number is what the buyer is actually relying on.

This asymmetry should be visible in the UI, not hidden. A Windows buyer is
running with less protection than a Mac buyer and deserves to know.

## When the cancel fails

The common real-world case, and the one easiest to skip: the lid closes, wifi
tears down, and the cancel request fails or times out. The machine now sleeps
with entries resting.

Do not retry into a closing window. Instead:

1. Attempt the cancels with a hard 2-second budget each.
2. Record the outcome durably either way — `slept_armed: true`, the order ids,
   and the exposure at sleep time.
3. On wake, that flag drives an urgent reconcile ahead of everything else.

An executor that knows it slept armed behaves very differently from one that
assumes it slept clean. Recording the failure is worth more than preventing it.

## Scheduled wake — the middle ground

Worth building, because it directly buys back what no-VPS gives up.

Both macOS (`pmset schedule` / `IOPMSchedulePowerEvent`) and Windows (Task
Scheduler wake timers) can wake a sleeping machine on a timer. The executor can
therefore wake periodically, tick, enforce the invariants, and sleep again.

This is not as good as staying awake, and it is not equally useful across
timeframes:

| Campaign timeframe | Wake interval | Quality |
|---|---|---|
| 5m | 60–90s | Degraded — misses intra-candle crossings |
| 15m | 3–5 min | Good |
| 1h / 4h | 10–15 min | Nearly equivalent to continuous |

Higher-timeframe campaigns barely need continuous attention, so a buyer running
1h ladders can leave the laptop closed overnight and lose very little. That is a
real feature, not a consolation prize.

Both platforms require the user to permit wake timers, and both will refuse on
battery in some power profiles. Detect and report rather than assume.

## Four ways the executor goes away

They are not the same failure and must not share a code path.

**Clean shutdown** — buyer quits, or the OS asks politely. Fully solvable:
enforce both invariants, snapshot with a `shutdown_at` stamp, exit.

**Sleep / suspend** — enforce the invariants inside the platform window above;
record whether they succeeded. On resume, wait for connectivity before firing
API calls, then run the wake ladder.

**Crash / power loss** — no chance to do anything. Entry orders stay resting and
can fill. Recovery leans on client-order-id idempotency, which the engine
already has (`_recover_order_by_client_id`,
[engine/cascade.py:5366](engine/cascade.py:5366)): on restart it can ask the
exchange about an order it may or may not have successfully placed.

**Network loss while running** — the engine is alive and stepping but blind, and
will keep reasoning from stale state unless it notices. Detect via consecutive
API failures or missed feed heartbeats, then enter an explicit degraded state.

## The wake ladder

| Gap | Action |
|---|---|
| < 2 min | Normal `reconcile()` — the existing deploy-gap path |
| 2 min – 6 h | Full recovery, below |
| > 6 h | **Do not auto-resume.** Present the divergence, require confirmation |

Any gap flagged `slept_armed` skips straight to full recovery regardless of
length.

These thresholds are about **money** — how far the buyer's own positions may
have drifted before a human should look at them. They sit alongside, and do not
conflict with, the **data** thresholds in `CASCADE_SIGNAL_FORMAT.md`: replay
from cursor under 2 minutes, re-snapshot past that, cursor refused entirely past
24 hours. A seven-hour gap needs the buyer's confirmation but still has a valid
cursor. A thirty-hour gap needs both.

### Full recovery, in order

1. **Ask the exchange first, believe it over local state.** Open orders,
   `myTrades` since `shutdown_at`, current balances. Local state is a
   hypothesis; the exchange is the fact.
2. **Ingest fills with their real timestamps** — see the bugs below.
3. **Enforce invariant 2 immediately.** If coin is held with no exit resting,
   place the TP before doing anything else. This comes before geometry work:
   an unprotected position is more urgent than a correct chart.
4. **Replay missed candles**, then apply signal-feed events missed in the gap.
5. **TP catch-up.** If price is already past target, exit at market now rather
   than waiting for a retest that may not come.
6. **Re-place entries** cancelled at shutdown — but only once geometry is
   current. Re-placing against stale levels is worse than not re-placing.
7. **Reconcile `reuse_below`** from the executor's own round history, never from
   the feed.

The 6-hour threshold is a starting number, not a measured one.

## What long gaps break in the server engine

**Fixed.** Recovered fills were stamped `time.time()` rather than the exchange's
fill time, in both branches of `_sync_live_orders`. Immaterial for a 30-second
deploy; wrong for an 8-hour sleep, which recorded a 3am fill as having happened
at 11am. Now read from the order row via `exchange_fill_ts`.

No live trading decision reads a fill timestamp — the live TP is a resting order
on the exchange — so this was a record-keeping fault, not a money fault. What it
corrupted: chart entry markers, the fills snapshotted into a closed round, and
journal charts frozen at a round's exit, which drop a fill that appears to have
happened after the exit.

**Not a bug, retracted.** An earlier draft claimed this also tripped the TP
guard in `_paper_tp_check`. It cannot: `_sync_live_orders` runs only for live
campaigns and `_paper_tp_check` only for paper ones, so the two never meet.
Paper fills are stamped with their own candle's timestamp, and recalc clears
`all_fills` before replaying, so the guard sees only a fill from the candle it
is currently judging — which is exactly its intent.

**Fixed, and misattributed in an earlier draft.** `MAX_REPLAY_BARS` was never
the truncation — it guards how far back a mother may be anchored and already
fails loudly at campaign creation. The silent truncation was in two other
places:

- `_chart_candles` made a single klines call, which returns the most *recent*
  1000 bars and nothing older. A replay asks for the whole campaign, so any
  campaign older than one page — ~3.5 days on 5m — was rebuilt from a window
  that started in the middle of it, without the mother candle in view. It now
  pages when the span needs it, and a view-sized request still takes one call.
- `_fetch_closed_candles` returned quietly when it ran out of pages, so "ran
  out of budget" and "there is no more data" produced the same short list.
  Exhaustion now logs at error level and raises an alert naming how far behind
  it stopped. The page budget also went from 30 to 60, because 90 days of 5m is
  ~26k bars and a page only advances by the part of it after the cursor.

The general shape is worth remembering for the executor: a truncated replay is
not a smaller replay. The geometry machine reads structure out of whatever
candles it is handed, so a short window produces confident, wrong fibs with
nothing on screen to suggest anything was missing.

## What the buyer sees

**Decided: every timeframe stays available on a laptop. The risk is disclosed,
not enforced.** No timeframe is blocked — but the app must make the cost of the
choice visible at the moment it is made, not in documentation.

### At campaign start

Each timeframe carries a plain attention label, because "5m" does not tell a
buyer anything about how much babysitting it needs:

| Timeframe | Label | Wording |
|---|---|---|
| 5m | high attention | Needs the machine awake nearly all the time |
| 15m | some attention | Copes with short breaks |
| 1h, 4h | hands off | Fine to leave alone overnight |

Picking a high-attention timeframe on a laptop shows an inline warning and a
single acknowledgement checkbox. Once per campaign, not a recurring nag — a
buyer who has accepted it does not need telling again.

### While running

An always-visible line: **"if this machine stops now, at most $X can fill
unwatched."** This is the single most useful number in the product. It is
knowable (one resting entry order of known notional), it changes as the ladder
moves, and it turns an abstract worry into a figure the buyer can judge.

Below it, what is actually out there: how many buy orders rest, and what the
sell order is protecting.

### On sleep

A confirmation that the invariants were enforced — buys cancelled, sell left in
place, nothing can be bought while away.

When the cancels did **not** land (network died first), say so plainly and name
the amount left exposed. This is the `slept_armed` case; the buyer should learn
about it at sleep time, not discover it on wake.

### Other states

- **Staleness banner** after two missed feed heartbeats: managing what it holds,
  opening nothing new.
- **Wake report** after any gap over 2 minutes: what filled, what changed, what
  it did about it.
- On Windows, the armed exposure sits more prominently, since the 2-second
  suspend window means that number is what the buyer is actually relying on.

## What stays irreducible

Power loss with an entry resting will occasionally produce an unmanaged fill.
Bounded to one order of known size, with no liquidation possible and a TP placed
within seconds of wake — but not eliminated.

State it plainly in whatever a buyer agrees to. The honest version is short:
this software trades from your machine; if your machine stops, a resting order
may still fill, and the position waits for your machine to come back.

---

*If the laptop constraint is ever lifted, running the executor on a small VPS
removes the clean-shutdown, sleep, and power-loss surface entirely and leaves
only network loss and crashes — both already covered by the wake ladder. Nothing
in this design blocks that; it is the same executor on a machine that does not
sleep.*
