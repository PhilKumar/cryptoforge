# CryptoForge — Production Readiness Audit

**Date:** 22 Jul 2026 · **Scope:** whole repo, read-only · **Code changed:** none

Baseline verified before starting:

| Check | Result |
|---|---|
| `pytest -q` | **307 passed, 1 xfailed**, 17.7 s |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 28 files already formatted |
| `git status` | clean |

Numbers in this document are measured on an Apple-silicon Mac with NVMe. The
Lightsail box has a burstable vCPU and network-backed storage — assume **4×–8×
these figures there**, and that is the machine that matters.

---

## 1. Findings that will bite in production

### 1.1 `/api/health` reads the entire state database — twice — per call

`_ops_state_summary()` → `_runtime_recovery_summary()` → `_engine_recovery_candidates()`,
and that last one calls `SQLiteJSONStore.export_snapshot()`, which is:

```sql
SELECT bucket, doc_key, payload, created_at, updated_at FROM documents
```

Every row of every bucket, `json.loads` on every payload. It is called **twice**
(once for `engine_live_state`, once for `engine_paper_state`) — to answer a
question that only needs two buckets.

Measured on a DB seeded with your real `runs.json` plus a full cascade history
(500 events, 100 closed campaigns × 200-entry event logs):

```
db size: 5.05 MB
export_snapshot():                24.8 ms
per _ops_state_summary() call:    49.6 ms   ← event loop blocked, synchronously
```

This is on the hot path of `/api/health`, `/api/ready`, `/api/ops/state/summary`
and `/api/admin/health`.

**Why this is the most urgent item:** `deploy/cd-deploy.sh` line 28 is

```bash
curl -sf --max-time 3 "http://127.0.0.1:${port}/api/health"
```

The cost scales linearly with the database, and the database only grows — every
backtest run appends ~0.5 MB to the `runs` bucket permanently. When
`export_snapshot()` × 2 crosses 3 seconds on the Lightsail box, **the deploy
health check times out and `cd-deploy.sh` rolls back a perfectly good build**,
with nothing in the logs to explain it. You will read that as "the deploy is
broken."

Second-order: this is also the endpoint you or an uptime monitor hit *when the
site seems slow*. Checking health makes it worse.

**Fix shape:** `_engine_recovery_candidates(bucket)` needs one bucket, and
`store.get_mapping(bucket)` already returns exactly that. Swapping
`export_snapshot()` for `get_mapping()` is a two-line change with identical
output and no behaviour change.

### 1.2 Cascade P&L ignores exchange fees — every other engine models them

`engine/backtest.py:388` takes `fee_pct` per side and subtracts entry and exit
fees from every trade (`line 667`). `engine/paper_trading.py:310` does the same.

`engine/cascade.py` — the engine actually placing live orders — has **no fee,
spread or slippage term anywhere**. `_close_round` books:

```python
pnl=round((exit_price - avg) * qty, 8)
```

That is gross. At Binance spot standard 0.1% per side, a round-trip on a ~$22
round costs about **$0.044**.

Your SOL #10 closed **+$0.08**. Net of fees that is roughly **+$0.036** — the
panel is reporting about **twice** the money that actually landed in the
account. On rungs this small the fee is not a rounding detail, it is a large
fraction of the edge.

Two consequences:

1. Every Cascade P&L number on screen — per round, per campaign, realised
   total — is optimistic by the round-trip fee.
2. Comparing a Cascade result against a `backtest.py` result compares **net
   against gross**. The backtest is the honest one.

Check your actual fee tier before sizing the correction (BNB discount, VIP
level). The structural point stands either way.

### 1.3 Carried LOT_SIZE dust is booked against the wrong round

`_sync_tp_order` (cascade.py:3006-3016):

```python
offered  = campaign.filled_base_qty + campaign.residual_base_qty
executed = _coerce_float(status_row.get("executedQty"), offered)
campaign.residual_base_qty = max(round(offered - executed, 12), 0.0)
self._close_round(campaign, exit_price, sold_qty=executed)
```

When a residual carried from round *N* is sold in round *N+1*, `sold_qty`
exceeds `filled_base_qty`, and `_close_round` values the whole lot at round
*N+1*'s average entry:

- **Round N**: `invested` (summed from `all_fills`) includes the dust's cost;
  `pnl` excludes its proceeds → round N is **understated**.
- **Round N+1**: `pnl` includes the dust's proceeds at N+1's average entry;
  `invested` never included its cost → round N+1 is **overstated**.

Across many rounds it roughly washes out only if average entries are similar.
Per round it is wrong in both directions, and the error grows with the price gap
between rounds.

This is **live-only**. Paper never generates a residual — `_fill_pending` buys
`usd / price` with no LOT_SIZE step. So paper and live compute per-round P&L by
genuinely different rules, which is exactly the live-versus-paper divergence
worth knowing about before scaling up.

Per RUNBOOK §7, one BTC lot step is **12% of a minimum $5.50 order**. On BTC
this error is material; on SOL (1.4%) and XRP (2.1%) it is small. Another reason
the first live campaigns belong on SOL or XRP.

### 1.4 Fire-and-forget asyncio tasks are not referenced

Three places create a task and keep no reference to it:

| Location | What it does |
|---|---|
| `alerter.py:108` | `loop.create_task(_dispatch(...))` — sends the Telegram alert |
| `app.py:6608` | `create_task(_push())` — broadcasts cascade status to WS clients |
| `engine/cascade.py:2657` | `asyncio.ensure_future(coro)` — **cancels live orders on a mother break** |

CPython's docs are explicit that the event loop keeps only a *weak* reference to
a running task, and that the caller must hold a strong one or the task may be
garbage-collected mid-execution. In practice this fires rarely and
unreproducibly — which is what makes it worth fixing rather than watching for.

`_schedule()` is the one that matters. It is how `_mother_broken` and
`_mother_retested` cancel resting live orders. A task collected there leaves
**real buy stops working on the exchange for a campaign that has ended**.

**Fix shape:** a module-level `set()` of pending tasks, `add_done_callback` to
discard. No behaviour change; three call sites.

### 1.5 nginx lets a slow backend become an unreachable site

`deploy/nginx.conf`, the `/api/` block:

```nginx
proxy_read_timeout 600s;
proxy_send_timeout 600s;
```

Ten minutes. The Cascade page polls `/api/cascade/status` **every 3 seconds**.
If the backend stalls, requests do not fail — they queue. 600 ÷ 3 = up to **200
in-flight requests per open tab**, each holding an nginx connection and a
backend slot, none of them ever giving up.

That is the amplifier that converts "the box is briefly slow" into "the site is
down", and it fits the *Client closed request before response completed* lines
you saw. The blocking-call fixes from the previous session removed the trigger;
this leaves the amplifier in place.

`/api/scalp/` already uses 10 s. Nothing on `/api/` legitimately needs 600 s
except a backtest run — worth giving that its own location block and dropping
the rest to something like 30 s.

---

## 2. Load and robustness

### 2.1 Closed-campaign history is truncated to two different limits

| Location | Cap |
|---|---|
| `_archive_campaign` (cascade.py:2732) | **50** |
| `_adopt_ended_campaigns` (cascade.py:1664) | 100 |
| `load_closed_campaigns` (cascade.py:1313) | 100 |
| `_cascade_persist_closed` (app.py:6575) | 100 |

Every archive silently discards 50 campaigns of in-memory history that every
other path is trying to keep. The DB still holds 100, so it is a display and
restart-consistency inconsistency rather than data loss — but the two numbers
should not disagree.

### 2.2 Every engine log line rewrites a 91 KB blob

`_cascade_persist_event` (app.py:6550) reads the whole 500-event list, parses it,
appends one entry, re-serialises all 500, and writes it back — per event, on the
event loop.

```
cascade_events blob:        91.3 KB
_cascade_persist_event():   3.59 ms per event
```

At the normal one-or-two events per candle step this is invisible. It is only
worth knowing because the cost is per-event and paid in the loop.

For calibration, I benchmarked a full Recalc with real persistence wired in
(1150 synthetic 5m candles, downtrend, mother candle held unbroken throughout):

```
total Recalc wall time :  73.2 ms   ← engine compute 54.2, events 16.0, snapshot 3.0
```

**Recalc is not an outage cause.** Tens of milliseconds, not seconds. I had
suspected it and it does not hold up.

### 2.3 A broker call can hold a worker thread for 93 seconds

`broker/binance.py:_request_with_retry` — `timeout=30`, `max_retries=3`, backoff
1 s then 2 s. Worst case **30 + 1 + 30 + 2 + 30 = 93 s** for one call.

It runs inside `asyncio.to_thread`, so it does not block the loop — but it does
occupy a thread from the default executor, which is `min(32, cpu_count + 4)`:
about **6 threads on a 2-vCPU Lightsail box**. Six stuck broker calls exhaust
the pool, and every subsequent `to_thread` queues behind them — including the
Cascade monitor loop's own price and order-sync calls.

Symptom: the site stops responding but never crashes, and comes back on its own
about 90 seconds later.

### 2.4 systemd has no memory ceiling

`deploy/cryptoforge.service` sets `LimitNOFILE=65536` and a good hardening
block, but no `MemoryMax=` / `MemoryAccounting=`. On a small Lightsail instance
a runaway process takes **nginx down with it**, so the failure presents as
"the whole site is gone" rather than a service restart. `Restart=on-failure` is
already there and would handle it cleanly if the ceiling existed.

### 2.5 `location /health` proxies to a route that does not exist

`deploy/nginx.conf` has `location /health { proxy_pass ... }`. The app defines
`/api/health` only — there is no `/health` route and no catch-all. Any uptime
monitor pointed at `https://crypto.philforge.in/health` gets a **404** and
reports the site down while it is perfectly healthy.

Also in that file: `upstream cryptoforge_backend_default { server 127.0.0.1:8001; }`.
Port **8001**, when the real ports are 9000/9001, and the name never matches the
`proxy_pass http://cryptoforge_backend` used everywhere below — the live
upstream comes from `/etc/nginx/conf.d/cryptoforge-upstream.conf`. Dead config
carrying a wrong port is exactly the trap that cost us time earlier this week.

### 2.6 Background tabs poll all night

There is no `visibilitychange` or `document.hidden` gating anywhere in
`static/cryptoforge-app.js`. These keep running with the phone in your pocket:

| Timer | Interval |
|---|---|
| `pollLiveStatus` | 10 s |
| `loadLiveMonitor` | 5 s |
| `refreshTopbarTicker` | 30 s |
| scalp activity | 15 s |
| portfolio | 60 s |

Roughly **1,200 requests an hour** from one idle tab, each carrying a session
validation and whatever the endpoint does.

The Cascade 3 s poll is the exception and is handled correctly — `showPage`
clears `_cfCascadePollTimer` on leaving the page.

### 2.7 Smaller load items

- **`/api/cascade/status` with no campaigns** re-runs `_restore_cascade_runtime`
  (two SQLite reads) on every 3 s poll, forever.
- **Paper campaigns poll the live ticker** every ~5 s per symbol (`_campaign_tick`
  → `_get_price`, 4 s cache) purely to display Last Price. `_paper_tp_check` now
  closes rounds on candle data, so the ticker is largely cosmetic for paper.
- **`/api/cascade/status` duplicates the WebSocket.** The WS handler already
  processes `cascade_status` pushes (`cryptoforge-app.js:3292`) and
  `_broadcast_cascade_update` already sends them. While the page is open you are
  paying for both paths.
- **Rate limits are effectively global.** Nine call sites pass
  `client_ip=request.client.host`, which behind nginx is always `127.0.0.1`.
  `_client_ip()` (app.py:760) reads `X-Forwarded-For` correctly but is only used
  by the login limiter. Harmless for a single user; wrong if the site is ever
  shared.

---

## 3. Correctness nits

- **`prior_low` is dead** — computed at `cascade.py:1986`, never read. Already in
  RUNBOOK §8.
- **`alerter._TELEGRAM_OK` is frozen at import.** Editing Telegram credentials
  through the admin console writes `.env` but the alerter keeps the boot-time
  value until a restart.
- **`_mother_retested` can drop a paper position.** The paper branch
  (cascade.py:2607) only books a round `if tp and candle.high >= tp`; otherwise
  the campaign is archived still holding `filled_base_qty` with nothing booked.
  The live branch leaves the TP resting so it can still fill. Reachable only
  when the retest candle tops out below TP, which is rare by construction —
  but it is a real live/paper asymmetry.
- **Paper always fills on a stop touch.** `_paper_fill_check` fills whenever
  `candle.high >= pending_stop_price`. A real stop-limit can trigger and miss if
  price gaps past the limit cap. Paper is pessimistic on *price* (it fills at
  the limit) and optimistic on *probability* (it always fills).
- **WS sessions are validated once, at connect.** With
  `proxy_read_timeout 86400s` a socket can outlive its session by nearly a day.

---

## 4. Suspects investigated and cleared

Recording these so they do not get re-investigated:

| Suspect | Verdict |
|---|---|
| Blocking broker calls on the event loop | **Clean.** AST + call-graph scan of all 85 routes found no unwrapped network call. The three flagged lines (`app.py:3204`, `3214`, `3485`) are Delta-only paths, unreachable on Binance. |
| `runs.json` parsed per request | **No.** It is a one-time legacy seed into SQLite; `_load_runs()` reads the store. Costs 8.3 ms and is not on a 3 s poll. |
| Session validation per request | **Not the problem.** 0.62 ms per read with 53 sessions; ~1.2 ms on a POST (auth + CSRF middleware both validate). No `Depends(require_auth)` in use, so it is never a third time. |
| Recalc stalling the loop | **No.** 73 ms for 1150 candles with real persistence attached. |
| `_candles_5m` unbounded growth | **Capped at 20,000** in `_candle_step`. The per-candle O(n) window scans are sub-millisecond at that size. The Recalc path is uncapped in code but bounded in practice by what Binance returns (~1000 klines). |
| Chart library instance leaks | **N/A.** All charts are hand-rolled inline SVG. No library, nothing to `destroy()`. |
| Unauthenticated WebSocket | **Authenticated** — session cookie checked at `app.py:6424`, closes 4001 on failure. |
| Asset cache-bust re-hashing per page load | **Correctly cached** on `(mtime_ns, size)` — `_asset_version`, app.py. |
| Bare `except:` swallowing errors | **Zero** in `app.py` and `engine/cascade.py`. |
| Secrets or cruft in git | **Clean.** `.gitignore` covers `.env`, `*.pem`, `*.bak`, `*.csv`, `*.log`, the state DB and WAL. `git ls-files` confirms none are tracked. |
| Rate-limiter memory growth | Bounded — prunes above 10,000 keys. |
| `ws_clients` leak | Removed in `finally` on disconnect and on send failure. |

---

## 4a. Findings added after the first pass

These came out of the follow-up review and are all now fixed.

### 4a.1 The login lockout could be bypassed with one header — **fixed**

`_client_ip()` read the **first** entry of `X-Forwarded-For`. nginx sets that
header with `$proxy_add_x_forwarded_for`, which *appends* the real peer to
whatever the client sent — so entry zero was a value the caller chose.

That function keys the login lockout. Sending a different `X-Forwarded-For` on
each request made every guess look like a first-time visitor, so the
5-attempts-per-5-minutes limit on a 6-digit PIN **never accumulated at all**.

Now reads `X-Real-IP` (nginx sets it from `$remote_addr`; a client cannot
influence it), falling back to the *last* `X-Forwarded-For` hop. The nine
rate-limit call sites that passed `request.client.host` — always `127.0.0.1`
behind the proxy — now go through the same helper.

### 4a.2 `/api/ticker` reported an estimate as fact — **fixed**

The route started from `t["open"]`, which the bulk endpoints never return, then
fell through to a high/low **midpoint** estimate. Binance's real
`priceChangePercent` was already being carried as `price_change_percent_24h` and
was never read.

Measured against live Binance data at the time of the audit:

| symbol | real % | midpoint estimate | error |
|---|---:|---:|---:|
| BTCUSDT | −1.33 | −0.45 | 0.88 pp |
| ETHUSDT | +0.37 | +0.55 | 0.18 pp |
| **SOLUSDT** | **−0.51** | **+0.25** | **0.76 pp — wrong sign** |
| XRPUSDT | +0.22 | −0.02 | 0.24 pp |
| **DOGEUSDT** | **−0.63** | **+0.01** | **0.63 pp — wrong sign** |
| PAXGUSDT | +2.41 | +1.14 | 1.27 pp |

Not just imprecise — it showed two of six coins **green when they were red**.
The real figure is now preferred, Delta's `open`-based path is kept for Delta,
and a `change_estimated` flag marks the midpoint fallback when it is genuinely
all that is available.

### 4a.3 "Only 6 of 25 symbols resolve" — **not a bug**

`config.TOP_25_CRYPTOS` contains **six** entries. The name is a leftover from a
longer Delta perps list. All six resolve on Binance spot exactly as written,
verified against `/api/v3/ticker/24hr`. There is no symbol-normalisation gap.
Renamed to `TRADABLE_SYMBOLS`, with the old name kept as an alias.

### 4a.4 `SESSION_SECRET` does not log anyone out — **not a bug**

`SESSION_SECRET` is defined at `app.py:463` and reassigned on config reload, and
**is never read anywhere else**. Sessions are random 32-byte tokens stored in
SQLite, so they survive restarts on their own. A fresh random default changes
nothing. It is dead code, not a logout switch. (It is set in `.env` anyway.)

### 4a.5 API keys in plaintext `.env` — **still true, by design**

Worth being straight about: this is a real exposure, and the realistic
mitigations on a single Lightsail box are file permissions and blast radius, not
encryption — anything the app can decrypt unattended at boot, an attacker with
the same file access can decrypt too. What actually helps is already partly in
place: keys scoped to spot trading with withdrawals **off**, an IP allowlist, and
the symbol whitelist from RUNBOOK §7. `UMask=0077` in the systemd unit keeps the
file owner-only. A secrets manager is the real answer if this ever leaves one box.

---

## 5. Suggested order of work

Nothing here is urgent enough to interrupt the two open pre-live checklist items
(three unattended rounds; stop-while-holding). Ranked by risk per unit of
change:

| # | Item | Status |
|---|---|---|
| 1 | `/api/health` full-DB scan (§1.1) | **fixed** — 45.1 ms → 0.64 ms, identical output |
| 2 | Task references (§1.4) | **fixed** — 4 sites, incl. the restart-recovery one |
| 3 | nginx `/api/` timeout (§1.5) | **fixed** — 30 s; backtests keep 600 s in their own block |
| 4 | XFF lockout bypass (§4a.1) | **fixed** — trusts `X-Real-IP` |
| 5 | Login lockout escalation | **added** — 5 min → 15 min → 1 h → 6 h → 24 h |
| 6 | TOTP second factor | **added** — opt-in, stdlib RFC 6238 |
| 7 | `/api/ticker` 24h change (§4a.2) | **fixed** — real figure, was wrong-signed on 2 of 6 |
| 8 | `MemoryMax=` (§2.4) | **fixed** — `MemoryHigh=280M`, `MemoryMax=340M` |
| 9 | `/health` 404 + stale port (§2.5) | **fixed** — real liveness route; port note corrected |
| 10 | Closed-campaign cap 50 vs 100 (§2.1) | **fixed** — one `CLOSED_HISTORY_LIMIT` |
| 11 | Restart-safety coverage | **added** — 6 tests, mutation-checked |
| 12 | Fee accounting (§1.2) | **open** — deliberate decision, see below |
| 13 | Residual attribution (§1.3) | **open** — needs care |
| 14 | `visibilitychange` gating (§2.6) | **open** — steady background load only |

Everything marked fixed is behaviour-preserving except the two security
additions, which are opt-in: with `CRYPTOFORGE_TOTP_SECRET` unset the login flow
is byte-for-byte what it was.

**Item 12 is the one still worth a decision.** It changes displayed numbers on
purpose, so it should not be slipped in quietly — most likely a configurable
`fee_pct` defaulting to your real Binance rate, with the gross figure kept
alongside so historical rounds stay comparable. Until then, read every Cascade
P&L as gross: on a ~$22 round at 0.1%/side the true figure is about $0.044
lower, which on SOL #10's +$0.08 is more than half of it.

---

## 6. Enabling the second factor

Entirely optional, and off until you set the variable. Nothing about login
changes while `CRYPTOFORGE_TOTP_SECRET` is unset.

```bash
venv/bin/python tools/totp_setup.py
```

It prints a secret and an `otpauth://` URI on your terminal and sends them
nowhere — no network call, nothing written to disk, nothing logged. Paste the
secret into `.env`, scan the URI with any authenticator, restart, then confirm
the two agree **before** you log out:

```bash
venv/bin/python tools/totp_setup.py --verify 123456
```

The login keypad collects the PIN, then the code, in two passes. Implementation
notes: codes are checked against RFC 6238's published test vectors in
`tests/test_auth_hardening.py`, one step of clock drift either way is tolerated,
and a used code cannot be replayed inside its own 30-second window.
