# Cascade Executor — install and run

This program trades on your own exchange account, from your own machine.

CryptoForge computes the geometry — the mother candle, the trendline, the fib
ladder, the target — and publishes it as a signal. This program listens to that
signal, sizes it to *your* capital, and places *your* orders with *your* API
keys. Those keys are read from your machine's environment and used only to talk
to your exchange. They are never sent to us, and neither is anything they
return: not your balance, not your orders, not your fills, not your P&L. The
only thing we hold about you is a public key and whether you have paid.

Spot only. No leverage, no futures, no margin. Nothing here can be liquidated.

---

## Before you start

| | |
|---|---|
| A machine | Anything that runs Python 3.11 or newer. A laptop is fine — see [Sleep, lids and shutting down](#sleep-lids-and-shutting-down). |
| An exchange account | Binance or CoinDCX, spot, funded in USDT. |
| Capital | **$1,000 minimum.** Below $3,000 campaigns still run but coarsened — the shallow rungs on smaller legs cannot clear the exchange's minimum order size, so those campaigns trade with fewer, deeper entries than the signal describes. |
| An API key | **Trade permission only.** Never enable withdrawal. Never enable futures or margin. This program uses neither, and a key that cannot withdraw cannot lose you your balance no matter what goes wrong here. |
| A subscription | Your public key has to be registered before the feed will talk to you. That is step 2 below, and it comes before money. |

---

## 1. Install

Put this `executor/` folder wherever you keep things, then, from the folder
that *contains* it:

```bash
pip install -r executor/requirements.txt
```

Four packages, listed with a note each in `requirements.txt`. Everything else
the executor uses is Python's standard library.

Every command below is run from that same parent folder — `python -m executor`
needs to see `executor/` as a package.

## 2. Generate your key and register it

```bash
python -m executor --register
```

This makes a key on your machine (at `~/.cascade-executor/buyer_key.pem`, mode
`0600`) and prints the **public half** — a line of base64. Send us that line and
the name you want to be known by.

The private half never leaves your machine, and there is nothing on our side
that could ever become you. If you lose it — new laptop, wiped disk — run
`--register` again on the new machine and send the new line; we re-key you.

You can run this before you have paid for anything, and before the config file
in step 3 exists. Registering is deliberately the first step: it means every
later billing event lands on a subscriber record that already exists, and the
"paid but no key" mess never happens.

We will send you back two things: your **buyer id**, and the **root public
key** — the base64 string your executor uses to check that a signal really came
from us. Both go in the config next.

## 3. Write your config

```bash
mkdir -p ~/.cascade-executor
python -m executor --sample-config > ~/.cascade-executor/config.json
```

Then edit it:

| Field | What to put |
|---|---|
| `server_url` | `https://crypto.philforge.in` |
| `buyer_id` | The id we sent you. |
| `root_public_key` | The base64 root key we sent you. Every signal is checked against this; a signal that does not verify is discarded, not traded. |
| `exchange` | `binance` or `coindcx`. |
| `capital_usd` | What you are putting behind this. **Every ladder is sized from this number** — it is not a limit you happen not to hit, it is the input. |
| `symbols` | `[]` follows everything published. Or list the ones you want: `["BTCUSDT", "SOLUSDT"]`. |
| `tick_seconds` | Leave at `20`. Under 5 is refused: it rate-limits your exchange without trading any better. |

**Your exchange credentials do not go in this file.** They are read from the
environment:

```bash
export CASCADE_API_KEY="…"
export CASCADE_API_SECRET="…"
```

The config file is the thing people paste into a support conversation when
something is wrong. Keeping secrets out of it by default means that paste is
always safe. (Every field above can also be set by environment variable —
`CASCADE_SERVER_URL`, `CASCADE_CAPITAL_USD`, and so on — which is what you want
if you run this from a launch agent or a container. The environment wins over
the file.)

## 4. Check it before it trades

```bash
python -m executor --check
```

This places nothing. It prints your config with the secrets redacted, your
public key, and then three lines:

```
PASS  Key set: verified against your root key, 1 active signing key(s)
PASS  Exchange: reachable, 3,142.88 USDT free
PASS  Feed: connected and snapshotted, 2 campaign(s) following
```

Every leg is tried even after one fails, so one run tells you everything that
is wrong rather than one thing at a time.

- **Key set FAIL** — usually a wrong `root_public_key`, or a `server_url` typo.
  `No feed key set is installed` is different: that one is our side, not yours.
- **Exchange FAIL** — wrong API key, wrong secret, or the key lacks trade
  permission. The exchange's own message is passed through.
- **Feed FAIL** — `not entitled` means your key is not registered or your
  subscription has lapsed. Anything else is a network or server problem.

## 5. Run it

```bash
python -m executor
```

It prints what it is doing to the terminal, and serves a page at
**http://127.0.0.1:7757** — Home, Console, Campaigns, Rounds, Setup. That page
is bound to localhost and refuses connections from anywhere else; nobody on
your network or the internet can reach it.

Options: `--no-ui` to skip the page, `--ui-port 7758` to move it, `--verbose`
for debug logging, `--config path/to/config.json` for a config somewhere else.

---

## The one number to look at

At the top of the Console:

> **If this machine stops now, at most $412.50 can fill unwatched.**

That is the whole risk of running this on a laptop, as a figure rather than a
worry. There is at most one buy order resting at any moment, of a size the
executor knows, so the number is exact. When it says **nothing can fill while
this machine is away**, you can close the lid without thinking about it.

Two buttons sit under it:

- **Stand down** — cancels every buy order now, leaves every sell order
  protecting what you hold, and stops opening anything new. This is the button
  for "I need to go and I don't want to think about it."
- **Pause / Resume opening** — stops new campaigns being joined while leaving
  everything in flight alone.

Neither ever abandons a position. Nothing in this program will cancel a sell
order that is protecting coin you hold.

## Sleep, lids and shutting down

Stopping cleanly — Ctrl-C, or `systemctl stop` — cancels resting buys, makes
sure anything you hold has a sell order against it, and writes down what it did
so the next start knows. That takes a few seconds; let it finish.

Sleep is not equally survivable on every platform, and pretending otherwise
would be the bug:

| | What you get |
|---|---|
| **macOS** | About 30 seconds' warning before sleep — plenty to cancel entries. The machine is held awake automatically while, and only while, a buy order is resting. |
| **Linux** | systemd gives 5 seconds by default, which is enough, and can be configured higher. Same automatic hold-awake. |
| **Windows** | About 2 seconds — **not** enough to cancel an order and confirm it. So a forced sleep is treated as a crash when you come back, and the armed-exposure number above is what you are actually relying on. |

A lid close cannot be prevented on any operating system. What the executor gets
is a window to act in, never a veto, so the design is "do the cancels inside
the window" rather than "keep the machine awake".

**After a crash or a forced sleep**, the executor asks your exchange what
actually happened while it was gone, places a target on anything you turned out
to be holding, and shows you what changed. It will not open anything new until
you click **I've reviewed — resume trading**. That gate is the point: a machine
that was away and came back does not get to guess.

## What stops it, and what doesn't

| | What happens |
|---|---|
| Subscription lapses | The feed closes and stays closed — retrying will not change it. **Positions you already hold keep being managed to their target.** Entitlement is re-checked every 30 seconds, so this lands within a minute of the status changing. |
| You start a second copy | The newest one wins; the older steps aside and says so. Two copies both reconnecting would displace each other forever and neither would manage anything properly. |
| Wifi drops, or we deploy | Reconnects with backoff. Disconnected is not flat — it keeps its picture and keeps managing what it holds. |
| A campaign's published geometry contradicts itself | That campaign is dropped and says so. Positions already open in it are still managed. Nothing is traded against numbers that do not add up. |
| A campaign started more than 5 minutes ago | Skipped. The ladder only means anything from its mother candle, so joining a fall halfway down is not the strategy — it is a different, worse one. |
| One bad tick | Logged, and the next tick re-reads the exchange. A single failure never stops positions being managed. |

## Troubleshooting

| It says | It means |
|---|---|
| `Config problem: Missing: …` | A required field is blank. API credentials come from `CASCADE_API_KEY` / `CASCADE_API_SECRET` unless you put them in the file. |
| `capital_usd must be set` | It is the input every ladder is sized from, so there is no sensible default. |
| `<campaign>: $500 is under the $1,000 minimum` | Your `capital_usd` is below the floor, so that campaign was not joined. This is checked per campaign as one starts, not at launch — the executor runs and connects fine, it just opens nothing. |
| `Unknown exchange 'kraken'` | Supported today: `binance`, `coindcx`. |
| `not entitled` | Key not registered, or subscription lapsed. Talk to us — a retry loop will not fix it and may get you rate-limited. |
| `Away for 4.2 hours` on a brand-new install | Shouldn't happen anymore; if it does, tell us, because it means the first-start marker did not get written. |
| `Not following <campaign>: …` | It is telling you exactly why — usually the join window or your capital floor. |

Send us the output of `python -m executor --check`. It is redacted by design
and it is almost always enough.

## What we can see

Your public key. When you connect and disconnect. Whether you have paid.

That is all of it. We do not have your API keys, so we cannot see your balance
or your trades; we do not receive your fills, so we do not know your P&L; and
there is no field in the signal protocol that could carry any of it, which is
enforced in code on our side rather than promised here.

## The risk, stated plainly

This software trades from your machine. If your machine stops, a resting order
may still fill, and that position waits for your machine to come back. It
cannot be liquidated — this is spot, not leverage — and the most that can fill
unwatched is the one order shown on your Console.
