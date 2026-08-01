# Cascade signal format v1

The wire contract between the geometry engine (our server) and a buyer's local
executor. Draft — nothing implements this yet.

## The one rule

**A field may be published only if it is derivable from public candle data.**

Everything else — capital, balances, orders, fills, positions — is the
follower's own business and never crosses the wire in either direction. This is
not a privacy nicety. It is what keeps us from holding anyone's credentials, and
it is also what makes the format correct: a follower's account state genuinely
differs from ours, so publishing ours would be publishing a lie.

The rule is easy to apply because of how the engine already funds a ladder
([engine/cascade.py:1397](engine/cascade.py:1397)):

```python
leg.pool_usd = allocation_pct * campaign.capital_unit_per_pct   # capital / 100
```

Capital enters at exactly one multiplication, at the very end. Everything
upstream of it is capital-independent, and that "everything upstream" is
precisely the payload below.

## Field classification

Taken field by field from `Campaign.to_dict()` and `Leg.to_dict()`.

### Published — candle-derived

| Field | Note |
|---|---|
| `campaign_id`, `seq`, `symbol` | identity |
| `mother_high`, `mother_low`, `mother_timestamp` | the mother candle |
| `mc_kind`, `left_mother_range` | major/minor, and whether the MC was read from the left |
| `timeframe`, `start_timeframe`, `escalates` | ladder position |
| `state`, `active_trendline_id` | lifecycle |
| `parent_campaign_id`, `generation`, `barren_chain` | restart chain |
| `model_version`, `created_at` | compatibility, join gating |
| `trendlines[]` | `anchor1_price/timestamp`, `anchor2_price/timestamp`, `bears_fib` |
| `legs[].leg_id`, `.trendline_id` | identity |
| `legs[].low`, `.touch_high`, `.touch_timestamp` | fib anchors |
| `legs[].created_via_break`, `.finalized`, `.escalated` | leg status |
| `legs[].fib.high_anchor`, `.low_anchor` | the drawn ladder |
| `legs[].leg_pct_from_mother` | depth |
| `min_fib_range_pct`, `median_bar_pct` | the size gate, candle-derived |
| `mother_break_candle`, `mother_break_top_candle`, `mother_break_wait_remaining` | break adjudication |

### Never published — account-specific

| Field | Why |
|---|---|
| `capital_usd`, `pool_usd`, `pool_total_usd` | theirs, not ours |
| `legs[].pending_orders{}` | every field: status, order_id, quantity, notional |
| `all_fills[]`, `rounds[]`, `avg_entry_price` | came from the exchange, not a calculation |
| `tp_price`, `tp_order_id`, `tp_rev`, `tp_filled` | derived from their fills |
| `filled_base_qty`, `residual_base_qty`, `exchange_qty` | their position |
| `collected`, `pending_usd`, `cumulative_used_pct` | their deployment |
| `funded_bands`, `funded_floor_price`, `legs[].netted_pct` | **see below** |
| `reuse_below` | **see below** |

Two of these deserve their own explanation, because publishing them would look
harmless and would in fact be a bug.

**`reuse_below` is set when a round closes**
([engine/cascade.py:4260](engine/cascade.py:4260)), from the follower's own exit.
It is the new-low floor: no new structure is drawn until price prints below it
([engine/cascade.py:3903](engine/cascade.py:3903)). Our round and theirs close at
different moments — theirs may not close at all — so our floor is not their
floor. Each executor maintains its own.

**`netted_pct` and `funded_bands` depend on which other campaigns the follower
is running.** Cross-campaign netting charges a leg only for the stretch of price
no sibling has already funded. A follower running three of our six symbols has
different siblings than we do, so their netting differs. The feed therefore
publishes the **gross** allocation and the executor nets locally against its own
band ledger.

## Transport

A signed, append-only event stream per symbol. The executor holds a durable
cursor and replays from it on reconnect.

- **Snapshot on subscribe** — full current geometry for every running campaign,
  so a cold executor doesn't have to replay history.
- **Events thereafter** — one message per geometry change.
- **Heartbeat every 30s** — silence must be distinguishable from "nothing
  happened", or a dead feed reads as a calm market.

## Envelope

Every message:

```json
{
  "v": 1,
  "model_version": 21,
  "seq": 148820,
  "emitted_at": 1785490000,
  "type": "leg.opened",
  "symbol": "SOLUSDT",
  "campaign_id": "casc_SOLUSDT_1785401234",
  "payload": { },
  "sig": "ed25519:base64..."
}
```

- `seq` is a per-symbol monotonic counter. A gap means the executor must
  re-snapshot rather than guess.
- `model_version` mirrors `MODEL_VERSION` (currently 21). An executor that does
  not recognise the version **must refuse to open new campaigns** and alert,
  rather than trading geometry it may interpret differently. Existing campaigns
  continue under the version they opened with.
- `sig` signs the canonical JSON of everything except `sig` itself. Without it
  anyone who learns the endpoint can push fabricated geometry into a buyer's
  executor, which is the single highest-value attack against this design.
- `emitted_at` is epoch seconds. All timestamps in this format are epoch
  seconds; display conversion to IST is the executor's job.

## Message types

### `campaign.opened`

The only message that may start a follower campaign. Join-at-start is enforced
here: an executor that receives this with `created_at` older than its
`max_join_age_sec` (default 300) **must skip the campaign entirely** rather than
join late.

```json
{
  "type": "campaign.opened",
  "payload": {
    "campaign_id": "casc_SOLUSDT_1785401234",
    "symbol": "SOLUSDT",
    "created_at": 1785401234,
    "mother_high": 178.42,
    "mother_low": 174.10,
    "mother_timestamp": 1785400800,
    "mc_kind": "major",
    "left_mother_range": false,
    "timeframe": "5m",
    "start_timeframe": "5m",
    "escalates": true,
    "state": "WAITING_FIRST_DEPTH",
    "parent_campaign_id": null,
    "generation": 0,
    "barren_chain": 0,
    "min_fib_range_pct": 0.0008,
    "median_bar_pct": 0.0011,
    "advisory": { "tick_size": 0.01, "min_notional_usd": 5.0 }
  }
}
```

`advisory` is a convenience copy of public exchange filters. The executor
**must** re-fetch these from its own `exchangeInfo` call and prefer its own
values — filters change, and an order rejected on a stale tick size is the
executor's problem to prevent.

A restart after a mother break arrives as a fresh `campaign.opened` with
`parent_campaign_id` set and `generation` incremented. Followers may join it:
it is a genuine campaign start.

### `trendline.set`

```json
{
  "type": "trendline.set",
  "payload": {
    "trendline_id": 3,
    "anchor1_price": 178.42,
    "anchor1_timestamp": 1785400800,
    "anchor2_price": 177.06,
    "anchor2_timestamp": 1785403500,
    "bears_fib": true,
    "supersedes": 2
  }
}
```

`supersedes` carries the standing-line rule: a new line replaces the previous
one only on a close above it, and may never sit below the standing line. The
executor asserts this rather than trusting it — if the new line sits below the
one it is replacing, that is a feed bug and the executor should halt on that
campaign, not follow it.

### `leg.opened`

The load-bearing message. This is what earns money the right to deploy.

```json
{
  "type": "leg.opened",
  "payload": {
    "leg_id": 4,
    "trendline_id": 3,
    "low": 172.88,
    "touch_high": 176.40,
    "touch_timestamp": 1785404100,
    "created_via_break": true,
    "fib": { "high_anchor": 176.40, "low_anchor": 172.88 },
    "leg_pct_from_mother": 3.11,
    "allocation_anchor": 174.95,
    "allocation_pct_gross": 1.183,
    "escalated": true,
    "derived": {
      "level_2": 169.36,
      "level_4": 162.32,
      "level_8": 148.24
    }
  }
}
```

`allocation_anchor` is the prior leg's low (or `mother_high` for the first leg),
and `allocation_pct_gross` is `(anchor - low) / anchor * 100` **before** netting.
The executor recomputes both from `low` and `allocation_anchor` and asserts
agreement to 1e-6. Mismatch means the two sides disagree about the model — halt,
don't trade.

`derived` is a checksum, not an instruction. Levels are
`high_anchor - level * (high_anchor - low_anchor)`, matching `FibLadder.level_price`.
The executor computes its own and compares.

### `leg.finalized`

The swing completed — its low broke again. No new orders for this leg.

```json
{ "type": "leg.finalized", "payload": { "leg_id": 4 } }
```

### `campaign.state`

State transitions the executor must mirror in its own machine:
`WAITING_FIRST_DEPTH`, `TRENDLINE_ACTIVE`, `MOTHER_BREAK_PENDING`.

```json
{
  "type": "campaign.state",
  "payload": {
    "state": "MOTHER_BREAK_PENDING",
    "mother_break_candle": 1785412200,
    "mother_break_wait_remaining": 2
  }
}
```

### `campaign.closed`

```json
{
  "type": "campaign.closed",
  "payload": { "state": "MOTHER_BROKEN", "reason": "mother_broken", "closed_at": 1785413400 }
}
```

`state` is one of `COMPLETED`, `MOTHER_BROKEN`, `STOPPED`. The executor stops
drawing new structure — but **must not** blindly flatten. What it holds and what
we hold are different positions; unwinding is its own decision against its own
fills.

### `heartbeat`

```json
{ "type": "heartbeat", "payload": { "running_campaigns": 3 } }
```

Two missed heartbeats (90s) should surface a visible "signal stale" state in the
executor UI. It keeps managing what it already holds — an open position still
needs its TP — but opens nothing new.

## What the executor derives locally

Everything with money in it:

1. **Netting.** Subtract its own `funded_bands` from `allocation_pct_gross` to
   get its own `allocation_pct`.
2. **Pool.** `allocation_pct * (capital_usd / 100)` — its capital, entered by
   the buyer per campaign.
3. **Rung split.** 20/30/50 across levels 2/4/8, per `LEVEL_ALLOCATION`.
4. **Pot accumulation.** As price crosses levels, add to the pot; place when it
   clears one rung (`min_notional * 1.10`, from its own exchange filters).
5. **Entry style.** Levels 2 and 4 as buy stops above a falling market, level 8
   as a resting limit, per `STOP_ENTRY_LEVELS`.
6. **TP.** Fib 0.25 off its own average entry.
7. **`reuse_below`.** Its own, from its own round closes.
8. **Fidelity mode.** At campaign open, compute whether the smallest rung
   (level 2, 20% of pool) clears one rung. If not, warn the buyer that this
   campaign will run coarsened — fewer, deeper entries than published.

## Capital gating

The executor decides this alone; the feed carries no capital.

- Below **$1,000** — refuse to open. Hard floor.
- **$1,000–$3,000** — open, but flag coarsened mode at campaign start.
- **$3,000+** — full fidelity on legs down to ~1%.

The threshold is not a constant. It is `5.50 / 0.20 = $27.50` of pool for the
smallest rung, and pool scales with the leg's own depth — so a deep leg is
faithful at lower capital than a shallow one. Compute per campaign, per leg.

## Open questions

1. **Feed authentication.** Signing keys need rotation and revocation, so a
   lapsed subscription actually stops working. Not designed yet.
2. **Stale-executor policy.** Worked separately in
   `CASCADE_EXECUTOR_RECOVERY.md` — cancel entries on shutdown, keep exits
   resting, and gate auto-resume on gap length.
3. **Replay depth.** How far back the stream is retained decides how long an
   executor may be offline before it must re-snapshot and skip campaigns.
