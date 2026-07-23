# CryptoForge Runbook

Everything needed to operate the deployed app, diagnose it when it breaks, and
understand what changed on 2026-07-22 — the day the live order path executed
for the first time.

Written after a day in which the site went down three times and eight real bugs
were found in the order path. Every one was found by *running* it, not by
testing it; the test suite was green through all of them.

---

## 1. The facts you need before anything else

| | |
|---|---|
| Server | AWS **Lightsail**, `13.205.229.208`, ap-south-1 (Mumbai) |
| Login user | `ec2-user` |
| App directory | `/home/ec2-user/cryptoforge` |
| Python | **`venv/bin/python`** — NOT `.venv`, that is the local name only |
| Ports | Blue-green on **9000 / 9001**. Never 8000. |
| Active port | `cat ~/.cryptoforge-active-port` |
| nginx upstream | `/etc/nginx/conf.d/cryptoforge-upstream.conf` |
| Domain | crypto.philforge.in → 13.205.229.208 |

**A `cryptoforge@8000` unit exists and crash-loops on "address already in use".
It is not the site.** Restarting it during an outage teaches you nothing. It was
stopped and disabled on 2026-07-22.

### Getting a shell

The Lightsail console has a browser terminal that needs no key:
**lightsail.aws.amazon.com → Instances → orange terminal icon**.

`~/.ssh/Control.pem` is **not** authorised on this instance — it is offered and
rejected. The working key is Lightsail's own: **Account → SSH keys → download
the ap-south-1 key**.

---

## 2. When the site is down

Run these in order. Stop at the first one that explains it.

### Step 1 — is the app answering at all?

```bash
cat ~/.cryptoforge-active-port
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:$(cat ~/.cryptoforge-active-port)/health
```

- **200** → the backend is fine; the problem is nginx or your browser
- **000** → nothing is listening. The process is stopped or dead
- **hangs** → the app is wedged; go to step 3

`/health` is the cheap one — it touches no database, no broker and no disk, so
it still answers when the box is struggling. That is the point of it: use this
to ask "is the process alive", not `/api/health`.

`/api/health` is the *readiness* check — broker configured, state store
writable, recovery needed — and it costs more. It is what `cd-deploy.sh` polls.
Use it once you know the process is up and you want to know whether it can
trade.

Point any external uptime monitor at `https://crypto.philforge.in/health`. It
used to 404 (nginx proxied the path, the app never defined it), so a monitor
aimed there reported the site down while it was perfectly healthy.

### Step 2 — just fix it

```bash
cd /home/ec2-user/cryptoforge && bash deploy/cd-deploy.sh
```

This starts a clean instance on the standby port, health-checks it *before*
switching, and repoints nginx. It fixes both "the process died" and "nginx is
pointing at a dead port" without needing to know which it was. It aborts safely
and leaves the current instance alone if the new one is unhealthy.

**This is what actually fixed the 2026-07-22 outage.** nginx was pointing at
9001, which had been stopped and never restarted.

### Step 3 — read the log

```bash
sudo journalctl -u cryptoforge@9000 -u cryptoforge@9001 --since "2 hours ago" --no-pager | tail -150
```

**Two traps, both of which have cost time:**

- **The journal is in UTC; everything in the UI and these notes is IST**
  (UTC + 5:30). `--since "13:00"` asks for 18:30 IST. **Always use relative
  time** — `--since "2 hours ago"` — and never a clock time.
- **Query BOTH units.** Every deploy switches 9000 ↔ 9001, so an incident that
  spans a deploy has its history split. `$(cat ~/.cryptoforge-active-port)`
  only reads the instance running *now*, which may have started minutes ago.

What to look for, and what it means:

| in the log | meaning |
|---|---|
| `Client closed request before response completed` | **the app is slow, not broken.** Something is blocking. See §3 |
| `tick failed for <id>: <error>` | the cascade engine threw; the tick was swallowed |
| `Ticker error for ZARUSDT / 456USDT / 这是测试币` | junk testnet assets being priced — should be fixed, see §4 `001f571` |
| `-1003` / `429` / `Too much request weight` | rate limit — we are flooding Binance |
| `came back CANCELED/EXPIRED/REJECTED` | the exchange rejected a resting buy stop. **Send me this line** |
| `address already in use` | you are reading `cryptoforge@8000`. Wrong unit |

### Step 4 — is it reachable from outside?

nginx answers over plain HTTP even when the app behind it is dead, so this
proves the box and nginx are alive but says nothing about the app:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -H "Host: crypto.philforge.in" http://13.205.229.208/
```

A `301` is healthy nginx redirecting to HTTPS.

### Step 5 — is the box swapping?

Everything can be "up" and still crawl. This box has 916 MB of RAM and runs
**two** apps — CryptoForge and PhilForge — plus a second CryptoForge instance
during every blue-green deploy. When RAM runs out the kernel pages to disk, and
on Lightsail that disk is over the network.

```bash
free -m
```

`Swap used` in the hundreds of MB means the box is thrashing. Find the owner:

```bash
for p in $(pgrep -f 'uvicorn app:app'); do echo "PID $p  swap=$(awk '/VmSwap/{print $2}' /proc/$p/status)KB  rss=$(awk '/VmRSS/{print $2}' /proc/$p/status)KB  up=$(ps -o etime= -p $p)  $(tr '\0' ' ' < /proc/$p/cmdline | cut -c1-60)"; done
```

**Seen on 2026-07-22:** PhilForge (port 8000) had been up 63 days holding
**1.22 GB** of swap — roughly 20 MB/day — against an RSS of only 88 MB. Almost
everything it had allocated was paged out. CryptoForge was clean at 0.
`sudo systemctl restart philforge@8000` took swap from 1287 MB to 27 MB.

Restarting the leaking process is the fix; **do not run `swapoff`** without
knowing what is in there, as it forces every page back into RAM at once.

PhilForge lives in the algoforge repo, not this one. If its swap climbs again,
it needs its own `MemoryMax` or a weekly restart timer.

---

## 3. The two outage patterns seen so far

Both looked identical from the browser — page loads, every field shows `--` —
and both were slowness, not failure. The API was returning **200** the whole
time.

**Pattern A — blocking the event loop.** Broker calls are synchronous
`requests` I/O. Called directly from an `async def` handler they block *every*
request, not just their own. Fixed in `f40b066`; two AST tests now fail the
build if any handler does it again.

**Pattern B — deriving symbols from wallet balances.** The testnet credits
junk assets (`ZAR`, `IDR`, `456`, `这是测试币`). Any code that builds
`{asset}USDT` and calls the exchange with it makes one doomed network round
trip per junk symbol, every refresh. Fixed twice — `855e5f5` for order history,
`001f571` for balance pricing.

> **Rule: anything that turns a wallet balance into a symbol must be filtered
> against `_tradable_pairs()` first.** This mistake has been made twice.

---

## 4. Everything fixed on 2026-07-22

Chronological. Each entry: what broke, and why it mattered.

### Broker environment and credentials

- **`b531334`** Testnet keys were separated from live keys. Enabling the testnet
  used to reuse the mainnet key pair.
- **`cf3bbcf`** The panel now states which environment orders actually reach —
  TESTNET or LIVE — instead of leaving it to be inferred.
- **`e2a025a`** Cache-bust tokens are content hashes. Hand-typed `?v=` tokens
  meant edited JS shipped under an unchanged URL, so the browser and the service
  worker both kept serving days-old code. *A UI fix can appear to do nothing for
  this reason.*
- **`0b6df5b`** Candles and tickers now come from `data-api.binance.vision` —
  the real market — even when trading the testnet. The testnet's own book drifts
  from reality, so a mother candle read off a real chart did not exist in its
  candles. Signed calls and `exchangeInfo` still go to the venue being traded.

### The live order path — none of this had ever executed

- **`16d5478`** **The buy stop had never once reached Binance.**
  `_place_pending_stop` read the cached price with `.get("price")`, but the
  cache holds `(price, monotonic)` **tuples**. Every sync after the first tick
  raised `AttributeError`, swallowed by the tick's try/except. This is the
  "keeps collecting, never executes" symptom. It also sat at step 2 of 3, so it
  took **take-profit management down with it**.
- **`0615f16`** **Several sell orders could rest against one position.**
  `str(result.get("orderId") or result.get("id") or "")` stored `""` when a
  reply carried neither key. `""` is falsy, so the next sync believed nothing
  was resting and placed another — logging "placed" every time. Now the
  exchange is the authority: any order carrying our `cf-csc-…-tp-` prefix is
  adopted rather than duplicated.
- **`7e820ce`** Buy stops churned — placed, cancelled by the exchange,
  re-placed, every 10–15s. `-2010 Duplicate order sent` now adopts the resting
  order instead of failing. A brake stops the same trigger going out more than
  three times. The log names *which* terminal state came back, because
  collapsing CANCELED/EXPIRED/REJECTED into "cancelled" made it undiagnosable.
- **`ed5cba2`** Stopping a live campaign left its buy stop working on the
  exchange. It cancelled by marker status, which the accumulated stop never
  has. It could fill after the campaign was archived.

> **Stop cancels orders. It never sells.** Coin already bought stays in the
> account with **no TP resting**. You must sell it manually.

### Engine correctness

- **`54f62b5`** Recalc reset only a hand-written list of fields, so `collected`
  and `pending_usd` survived. Each press re-added the same levels: a $7.60 pot
  read **$46.62** after six presses, and once inflated past the rung minimum it
  armed a real buy stop for money no level had collected. The reset now walks
  the dataclass, so a field added later cannot be forgotten.
- **`4f5a888`** A candle replay could open a paper position but never sell it —
  the TP was only checked against the live price. **Recalc is a replay, so
  pressing it erased closed rounds** and handed back an open position. Also
  corrected the backtest, which had been understating the strategy badly:
  realised $0.25 → $8.28 over the 21 hand-traded days.
- **`855e5f5` / `001f571`** See §3 pattern B.
- **`f40b066`** See §3 pattern A.

### Geometry

- **`baf211b`** The same-shelf rule compared only the touch high, so a swing
  starting from the same high but falling much further was discarded as a
  duplicate. `ladders_overlap()` now asks whether the two ladders share any
  price at all. Recovered a fib on BTC #36 whose anchors Phil had drawn by hand.
- **`a287d4c`** The trendline anchor rejected any close a **single cent** above
  the line. On SOL #10 that froze the anchor for a whole day, so no fifth
  trendline could ever be drawn. A close may now exceed the line by
  `ANCHOR_CLOSE_TOLERANCE_PCT`. **The value is a measured band, not a knob** —
  only 0.04%–0.05% satisfies every anchor Phil has confirmed. It also *fixed*
  PAXG TL2, which the old code got wrong.

### UI

- **`f52c942`** Closed rounds keep the individual buys that made up the average.
- **`45b794c`** A standalone closed-round ledger, and fixed column widths that
  were squeezing three columns off the rounds table entirely.
- **`9de228e`** The ledger sits directly under Open Trades.
- **`43f38f5`** Test Active writes a persistent result instead of a toast.

---

## 5. Useful commands

```bash
# check for duplicate orders on the exchange (dry run — cancels nothing)
cd /home/ec2-user/cryptoforge && venv/bin/python tools/cancel_duplicate_orders.py

# actually cancel them
venv/bin/python tools/cancel_duplicate_orders.py --apply

# measure a geometry change against the 21 hand-traded days
venv/bin/python tools/cascade_backtest.py

# full test suite
venv/bin/python -m pytest tests/ -q

# set up the login second factor (prints locally, sends nothing anywhere)
venv/bin/python tools/totp_setup.py
venv/bin/python tools/totp_setup.py --verify 123456   # after setting .env

# locked out? clear the escalating lockout for one address
redis-cli DEL "cryptoforge:loginlock:<ip>" "cryptoforge:login:<ip>"
# no Redis: the lockout is in memory, so a restart clears it
sudo systemctl restart cryptoforge@$(cat ~/.cryptoforge-active-port)

# what is listening
sudo ss -ltnp | grep -E ':(9000|9001)'

# recent deploys
gh run list --limit 5
```

---

## 6. Geometry ground truth

The only hard reference is Phil's own finalised TradingView drawings. The engine
has been rewritten repeatedly against inferred rules, each fit breaking other
days. **Add a confirmed anchor as an assertion before changing geometry, and
treat a conflict with one as a reason to stop and ask.**

| campaign | confirmed |
|---|---|
| BTCUSDT 5m, mother 65,799 @ 07-20 23:40 | fib 0 = 65,196.00, fib 1 = 65,082.81 (same 00:45 candle) |
| PAXGUSDT 5m, mother 4,076.33 @ 07-21 12:15 | TL2 anchor2 = 4,064.83 @ 16:10 |
| BTCUSDT 5m, mother **66,928.49 @ 07-21 20:15** (#36) | fib 1 = 66,907.34 / 66,809.98 · fib 2 = 66,746.68 / 66,678.00 · fib 3 = 66,739.89 / 66,052.63 |
| SOLUSDT 5m, mother 78.88 @ 07-21 12:05 (#10) | F3 = 78.51 / 78.15 · F4 = 78.64 / 78.10 |

> **When the engine and the chart disagree, check the MOTHER CANDLE first.**
> Campaign #36 was started on the 20:25 candle instead of 20:15. One candle out
> changes every fib downstream and looks exactly like a rule bug.

Measure every geometry change with `tools/cascade_backtest.py`, diffing against
a `git stash` baseline. Expect the entry / deployed / P&L columns to hold still
unless the change is meant to move them.

---

## 7. Before real money

Ordered. Items 2 and 3 mostly happen by leaving it alone.

1. ~~No duplicate orders resting~~ — **verified clean 2026-07-22**
2. **Three complete rounds, unattended**, at least one overnight. One round
   proves the path exists; three prove it repeats. *(**2 of 3 done** — SOL #10
   and a BTC round on 2026-07-22, both unattended.)* Remember the P&L shown is
   **gross** — the Cascade engine models no fees, so subtract ~0.2% round-trip
   before judging any of these. See §8.
3. ~~**Deploy while holding a position.**~~ — **passed 2026-07-22 12:30 UTC.**
   Deployed with BTCUSDT #36 holding 0.00011 BTC and its TP resting. The new
   engine started, declined the lock, waited 35s for the old one to exit, then
   took over. No order activity at all during the handover, and the TP came
   through untouched — same id `19545717`, same revision `tp-2`. Repeat this
   after any change to the engine's lifecycle or the deploy script.

   ```
   engine started
   another instance holds the write lock — not placing orders until it exits
   took the write lock — this instance now owns order placement
   ```

   **Fail looks like:** two different PIDs on any `Buy stop placed` or
   `TP limit sell placed` line, or a second order appearing in
   `tools/cancel_duplicate_orders.py`.
4. ~~**Lock the mainnet key down.**~~ — **done 2026-07-22.** Key "Cryptoforge"
   (HMAC): Reading on, Spot Trading on, **Withdrawals OFF**, Universal Transfer
   off, Margin Loan off, and IP-restricted to `13.205.229.208`. Withdrawals-off
   is what turns a worst-case bug into a bad trade rather than a drained
   account. *Still to consider: the Symbol Whitelist, which would stop a wrong
   symbol being traded at all — not hypothetical, since deriving bad symbols
   from wallet balances took the site down twice.*
5. **Stop a campaign while holding**, and confirm for yourself that you are
   left holding coin with no TP. Note the TP order id, press Stop, then
   `cancel_duplicate_orders.py` (expect no cascade orders) and
   `show_fills.py --hours 1` (expect NO sell). Do this LAST — it deliberately
   abandons a position.

   **8. ~~Second factor on the login.~~ — done 2026-07-22.** A 6-digit PIN was
   the only thing between a stranger and an app that places real orders.
   `CRYPTOFORGE_TOTP_SECRET` is set on the server and the login asks for PIN
   then code. The lockout also escalates now (5m → 15m → 1h → 6h → 24h).
   Rollback if a phone is ever lost:

   ```bash
   sudo sed -i 's/^CRYPTOFORGE_TOTP_SECRET=/#&/' /home/ec2-user/cryptoforge/.env
   sudo systemctl restart cryptoforge@$(cat ~/.cryptoforge-active-port)
   ```

   Re-issue with `venv/bin/python tools/totp_setup.py`.
6. ~~**Confirm Telegram alerts arrive.**~~ — **verified 2026-07-22.** The
   `-2010 Duplicate order sent` failure fired "Cascade order FAILED" and it was
   received. Telegram returned HTTP 200 in the same second.
7. ~~**Check mainnet symbol filters.**~~ — **verified 2026-07-22** against the
   public `exchangeInfo`. Every pair's `minNotional` is $5 ($1 DOGE), so the
   $5.50 rung minimum clears everywhere.

   **But LOT_SIZE granularity varies enormously at small order sizes.** One
   lot step, as a share of a minimum $5.50 order:

   | symbol | 1 step | share of a $5.50 order |
   |---|---|---|
   | BTCUSDT | $0.66 | **12%** |
   | PAXGUSDT | $0.41 | 7.5% |
   | ETHUSDT | $0.19 | 3.5% |
   | XRPUSDT | $0.11 | 2.1% |
   | SOLUSDT | $0.077 | **1.4%** |
   | DOGEUSDT | $0.072 | 1.3% |

   `d8b0ffb` carries the remainder forward rather than stranding it, but the
   coarseness is real. **Run the first live campaigns on SOL or XRP, not BTC.**

**Day one sizing: $100–200 campaign capital, one symbol, one campaign.** A fib
risks about 1% of capital, so $200 puts ~$2 on a fib. Enough to prove the
plumbing with real fills and fees; cheap enough that another bug costs a coffee.

---

## 8. Still open

- **Cascade P&L is gross — no fees.** `engine/backtest.py` and
  `engine/paper_trading.py` both model `fee_pct`; `engine/cascade.py` does not.
  At Binance spot 0.1%/side a ~$22 round trip costs about **$0.044**, so SOL
  #10's reported **+$0.08** was really about **+$0.036**. Every Cascade number
  on screen is optimistic by that much, and comparing a Cascade result to a
  backtest result compares gross against net. See `AUDIT.md` §1.2 — it needs a
  decision, not a quiet patch.
- **Carried LOT_SIZE dust is booked to the wrong round.** Residual sold in round
  N+1 is valued at N+1's average entry while its cost sat in N's `invested`.
  Live-only; paper never generates a residual, so the two modes compute
  per-round P&L by different rules. `AUDIT.md` §1.3.
- **Buy-stop -2010 "would trigger immediately" — resolved 2026-07-23.** The
  cause was not mysterious: on a thin book (PAXG especially) the price reaches
  the trigger before a resting buy stop can be placed, and Binance rejects a
  buy stop whose trigger is at or below the market. The engine now reads a ≤1s
  fresh price and, when price has **already reached the trigger**, takes the
  entry as a **limit buy capped at the limit price** instead of resting a stop
  that would be rejected — the same thing paper mode and a manual trade already
  do. Below the trigger it still rests a stop. A -2010 that slips through the
  race is a silent wait, not an alert. This makes live match paper: paper fills
  when a candle reaches the stop, and live had been rejecting at that exact
  moment. **Still needs a live paper→live proving round before trusting it with
  real orders on a thin market.**
- **Partial fills** have never run against a real exchange. A fill *during
  downtime* is now covered by `CascadeRestartSafetyTests` (six tests, both
  mutation-checked), but that is a fake broker, not Binance.
- **`prior_low`** is computed in `_evaluate_cut` and never used — a documented
  rule that was never wired up (`engine/cascade.py`, search `prior_low`).
- **`test_matches_the_user_chart_exactly`** is `@unittest.expectedFailure`: the
  engine produces 65,246.00 / 65,160.00 where the confirmed values are
  65,196.00 / 65,082.81.
- **4 of 21 backtest days take no entry.**
- **"430 open positions"** on the Portfolio page are wallet balances, not
  positions — mostly testnet junk. Needs filtering and a rename.
