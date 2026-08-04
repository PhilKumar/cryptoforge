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

A signed, append-only event stream per symbol, served over a WebSocket at
`/ws/cascade-feed`. The executor holds a durable cursor and replays from it on
reconnect.

- **Handshake first** — the executor signs `{buyer_id, nonce, timestamp}` with
  its own key. Ten-second budget, then the socket closes.
- **Snapshot on subscribe** — full current geometry for every running campaign,
  so a cold executor doesn't have to replay history. Snapshot frames carry the
  symbol's current head as their `seq`; they are a rendering of the present,
  not entries in the log, so they consume no sequence number and the executor
  sets its cursor straight from them.
- **Events thereafter** — one message per geometry change.
- **Heartbeat every 30s** — silence must be distinguishable from "nothing
  happened", or a dead feed reads as a calm market. Entitlement is re-checked
  on every beat.

The `welcome` frame reports the measured clock skew, and warns when it is over
a minute. That is not politeness: `max_join_age_sec` is judged against the
executor's own clock, so a machine a few minutes fast decides every campaign is
already too old and joins nothing — silently, indefinitely, on a stream that
looks perfectly healthy. A buyer has no way to tell that from a quiet market.

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
  "payload": { }
}
```

- `seq` is a per-symbol monotonic counter. A gap means the executor must
  re-snapshot rather than guess.
- `model_version` mirrors `MODEL_VERSION` (currently 21). An executor that does
  not recognise the version **must refuse to open new campaigns** and alert,
  rather than trading geometry it may interpret differently. Existing campaigns
  continue under the version they opened with.
- `emitted_at` is epoch seconds. All timestamps in this format are epoch
  seconds; display conversion to IST is the executor's job.

### The signed frame

The envelope above is never transmitted as a bare object. It travels inside a
frame that carries its own signature:

```json
{
  "msg": "{\"v\":1,\"model_version\":21,\"seq\":148820,...}",
  "sig": "ed25519:cf-feed-2026a:BASE64..."
}
```

`msg` is the envelope serialized to a JSON **string**, and the signature covers
exactly those bytes. The executor verifies the string it received and only then
parses it — it never re-serializes the envelope to check a signature.

That is deliberate, and it is worth one paragraph because the obvious design is
wrong. Signing "the canonical JSON of the object" requires both sides to agree
on a canonicalization, and the part that bites is float formatting: `178.42`,
`178.420`, and `1.7842e2` are the same number and different bytes, and Python,
JavaScript, and Go do not all print them the same way. A mismatch there is a
signature failure on a message that is perfectly valid, which in this system
means an executor that halts a live campaign for no reason. Signing the
transmitted bytes removes the entire class of problem: there is nothing to agree
on. The cost is about 15% frame size from JSON escaping, on a stream that emits
a few hundred real messages a day.

Without a signature, anyone who learns the endpoint can push fabricated geometry
into a buyer's executor. That is the single highest-value attack against this
design, and everything in **Feed authentication** below exists to close it.

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

## Feed authentication

Two questions get asked of every connection, and they are not the same question:

1. **Is this geometry really ours?** — message authenticity.
2. **Is this buyer paid up?** — subscription entitlement.

Answering both with one shared secret is the usual mistake. They rotate on
different clocks, they fail in different directions, and one of them must keep
working after the other stops.

### Authenticity: two-tier keys

```
root key (offline, generated once, never rotates)
  └─ signs → key set  {kid, alg, public, not_before, not_after, revoked[]}
       └─ feed key (on the server) signs → every message
```

- **Root key.** Generated once. The private half never touches the server — it
  lives off-box. It signs nothing but key sets. Its public half is compiled into
  the executor, which is what lets a buyer's machine tell one of our keys from
  an attacker's.
- **Feed key.** Lives on the server, signs every message, carries a `kid`.
  Rotates every 90 days on schedule, or immediately on suspicion.
- **Key set.** `GET /api/cascade/feed/keys` returns the current keys, their
  validity windows, and an explicit `revoked` list, all signed by the root key.
  The executor caches it with a hard 24-hour `expires_at`.

The 24-hour cache expiry is the load-bearing number. It means a revocation
reaches **every** executor in the field within a day, including ones that were
switched off when we published it — because a stale key set is refused on its
own terms, not because we managed to reach the machine. An executor whose cached
key set has expired opens no new campaigns and says why.

What this buys: if the box is ever compromised, we revoke the `kid` and every
executor drops it within 24 hours. The single-key alternative — where the
current key signs its own successor — has no recovery at all from that state:
an attacker who holds the key mints the next one too, and every executor in the
field follows them until each buyer installs a new build.

### Entitlement: the buyer's own key, checked continuously

The executor generates an ed25519 pair **locally at install** and registers only
the public half. We therefore never hold a secret that can leak from our side —
the same principle as the exchange credentials we deliberately don't hold.

- **Connect** — the executor signs `{buyer_id, nonce, timestamp}`; we verify it
  against the registered public key, then check the subscription.
- **Re-check on every heartbeat (30s).** This is the part that makes a lapsed
  subscription actually stop: a long-lived stream must not become a way to keep
  receiving signals after the card expires. A lapse gets a terminal
  `feed.revoked` and the socket closes within 30 seconds. No token TTL, no grace
  window anyone has to reason about.
- **One live stream per buyer key.** A second connection displaces the first,
  and both sides are told. Sharing a key is not prevented — it can't be — but it
  is made useless and visible.
- **Offline grace, 24h.** An executor that hasn't successfully validated
  entitlement in 24 hours opens no new campaigns. Otherwise "pull the network
  cable and it keeps trading yesterday's cached geometry forever" is an open
  door.

### What entitlement must never gate

**A revoked, expired, or unpaid executor keeps managing what it already holds.**
Exits stay resting, a missing TP still gets placed, the wake ladder still runs.
Entitlement gates *new* structure only — new campaigns, new legs, new entries.

This is not generosity. Cutting off exit management to enforce a bill would
leave someone holding spot coin with no target against it, on a machine we told
to stop working. The money is theirs and the position is real; a billing state
is not a reason to put it at risk. If a subscription lapses with positions open,
the right behaviour is to keep the exits alive and say clearly that no new
campaigns will start.

## Retention and replay

The question was how far back the stream is retained. The answer starts
somewhere else: **the snapshot is sufficient for correctness, and replay is an
optimization and a diagnostic.**

Nothing the executor *does* is path-dependent on intermediate history. It trades
current geometry, and everything with money in it — `reuse_below`, funded bands,
fills, average entry — is local and never came from the feed in the first place.
A leg that opened and finalized while the machine was asleep needs to be known
as finalized; how it got there changes no decision. So replay never has to be
deep enough to reconstruct anything, which is why the answer is not simply a
bigger number.

Then `max_join_age_sec` (300s) finishes the argument. Any campaign that opened
during a gap longer than five minutes is unjoinable regardless of how perfect
the replay is. **Past five minutes, deeper replay buys nothing tradeable.**

Two numbers, deliberately different, for two different jobs:

| | Value | Job |
|---|---|---|
| **Server retention** | 7 days | Support and forensics — "show me every event on SOL last Tuesday" |
| **Executor cursor validity** | 24 hours | Trust — past this the cursor is refused and the executor cold-snapshots |

**Heartbeats are not retained.** They are liveness, not history: 2,880 a day per
symbol, which would be ~99% of the volume and none of the value. A heartbeat
missing from a replay means nothing. Excluding them is what makes 7 days cost
kilobytes — a busy day is a few hundred real events across every symbol.

The wake ladder maps onto it:

| Gap | Source of truth | What replay is for |
|---|---|---|
| ≤ 2 min | replay from cursor | the state itself |
| 2 min – 24 h | **re-snapshot** | the wake report only — "here's what you missed" |
| > 24 h | snapshot; cursor refused | nothing beyond "you were away N hours" |

This does not conflict with the 6-hour no-auto-resume rule in
`CASCADE_EXECUTOR_RECOVERY.md`; they measure different axes. Six hours is about
**money** — how far the buyer's own positions may have diverged before a human
should look. Twenty-four hours is about **data** — how long we promise a cursor
still means something. A seven-hour gap needs confirmation but has a valid
cursor; a thirty-hour gap needs both.

**A `seq` gap re-snapshots even when the missing rows are still in retention.**
Filling the hole would work and would be faster, and it is not worth it:
re-snapshotting is cheap and always correct, while hole-filling is an
optimization that can be subtly wrong. Take the boring one.

## Still open

- **Snapshot delivery under load.** A cold executor asks for full geometry on
  every running campaign. Fine at our scale; needs a bound before it isn't.
- **The executor itself.** Everything above is the server half. Nothing has
  been built that connects to it and trades.

## Which venue the geometry came from

`campaign.opened` carries `exchange`. It sounds account-specific and is not: it
names a public data source, which is exactly as public as the candles. Binance
SOLUSDT and CoinDCX SOLUSDT are not the same series, and without this field
`symbol` silently implies "yours" — an executor cross-checking our levels
against its own candles would find small mismatches with nothing to explain
them.

The engine stores `""` for "the venue this engine was started with", so the
publisher resolves it before it goes out; a bare `""` on the wire would tell a
buyer nothing. An executor trading a different venue from the one named should
say so plainly in its UI rather than leave the buyer to infer it.
