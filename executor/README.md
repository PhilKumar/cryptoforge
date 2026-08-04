# Cascade — setting it up on your computer

Read this once, all the way through, before you type anything. Setting up takes
about twenty minutes and you only ever do it once.

---

## What this actually is

We watch the market and work out where to buy and where to sell. That's our
side.

This little program is your side. It sits on your computer, listens to what
we've worked out, and places the buy and sell orders on **your own exchange
account** — in your name, with your money, under your control.

Your money never comes to us. We can't move it, and we can't see it. If you
stop paying, the program stops getting our signal, and that's the whole of what
we can do to you.

A few things that are worth knowing up front:

- **No borrowing.** This buys coins with money you already have. There's no
  such thing as being "wiped out" or "margin called" here. The worst case is
  that you're holding a coin that's worth less than you paid.
- **It runs on your computer, not ours.** So while it's off, it isn't doing
  anything. There's a section below on exactly what that means.
- **You can stop it at any time**, with one button, and it will never abandon a
  position when you do.

---

## What you'll need

- **A computer that can stay switched on** while you're trading. A laptop is
  fine — there's a section on lids and sleeping below.
- **An account on Binance or CoinDCX**, with at least **$1,000** in it (as
  USDT). Below that, the strategy can't really be followed properly — you can
  read why in "Why $1,000" near the end.
- **About twenty minutes**, once.

You do not need to know how to program. You'll be copying lines from this page
and pressing Enter. That's all it is.

---

## First, a word about exchange keys

At one point you'll create something on your exchange called an **API key**.
It sounds alarming. It isn't, if you set it up the way described here.

Think of it as a spare car key that starts the engine but doesn't open the
boot. It lets this program buy and sell on your account — and nothing else.

When your exchange asks what the key is allowed to do:

- ✅ **Tick: trading (spot)**
- ❌ **Do not tick: withdrawals.** Ever. Not for this, not for anything.
- ❌ **Do not tick: futures, margin, or lending.** This program doesn't use them.

A key that can't withdraw can't take your money out of your account. Not by us,
not by a thief, not by a bug. That one unticked box is most of your safety, and
it costs you nothing.

---

## Step 1 — Open the black window

Everything below happens in a program that's already on your computer, where
you type a line and press Enter.

- **On a Mac:** press `Cmd + Space`, type `Terminal`, press Enter.
- **On Windows:** click Start, type `PowerShell`, press Enter.

A window opens with a blinking cursor. This is where the lines below go. You
paste a line, press Enter, wait for it to finish, then do the next one. If
something goes wrong it will tell you in words — nothing here can break your
computer.

## Step 2 — Check you have Python

Python is the language this program is written in. Macs usually have it.
Paste this and press Enter:

```
python3 --version
```

If it prints something like `Python 3.11.0` (any number 3.11 or higher), you're
set — carry on to step 3.

If it says it can't find it, go to **python.org/downloads**, download the big
yellow button, install it, then close and reopen the black window and try again.

> **Windows note:** everywhere below says `python3`. On Windows, type `python`
> instead — just that one word changes.

## Step 3 — Go to the folder we sent you

We'll have sent you a folder called `executor`. Put it somewhere you'll
remember — your Desktop is perfectly fine.

Now you need to tell the black window where it is. Type `cd ` (that's c, d, and
a space) — then **drag the folder that contains `executor` into the window**.
It'll fill in the location for you. Press Enter.

On a Mac, if the `executor` folder is on your Desktop, this also works:

```
cd ~/Desktop
```

## Step 4 — Install the four pieces it needs

```
python3 -m pip install -r executor/requirements.txt
```

This downloads four small, standard, well-known bits of software that the
program uses. It'll print a lot of text. Wait for the cursor to come back.

## Step 5 — Create your ID and send it to us

```
python3 -m executor --register
```

This prints one line that looks like nonsense:

```
XCmGM01hy/L0EN8qqmpQ+GID4IMG9p2Upu67jXIR4ZQ=
```

**Copy that line and email it to us**, along with the name you'd like to be
known by.

What just happened: your computer made itself a pair of matching keys, like a
lock and its key. It kept the private one and printed the public one. The
public one is what you just sent us — it's how we recognise you, and it's
useless to anyone else. The private one never leaves your computer, and we
never have it. There's nothing on our side that could be stolen and used to
pretend to be you.

If you ever get a new computer, just do this step again there and send us the
new line.

**We'll email you back two things:** a short name (your *buyer ID*), and
another line of nonsense (the *root key*). You need both for the next step, so
wait for that email before carrying on.

## Step 6 — Fill in your details

Make yourself a settings file:

```
mkdir -p ~/.cascade-executor
python3 -m executor --sample-config > ~/.cascade-executor/config.json
```

Now open it so you can edit it:

- **Mac:** `open -e ~/.cascade-executor/config.json`
- **Windows:** `notepad $HOME\.cascade-executor\config.json`

You'll see something like this. Change the parts on the right of each colon,
keeping the quote marks exactly where they are:

```json
{
  "server_url": "https://crypto.philforge.in",
  "buyer_id": "buyer-your-name",
  "root_public_key": "(the base64 root key from your subscription email)",
  "exchange": "binance",
  "capital_usd": 3000,
  "symbols": [],
  "tick_seconds": 20
}
```

| Line | What to put |
|---|---|
| `server_url` | Leave it exactly as it is. |
| `buyer_id` | The short name we emailed you. |
| `root_public_key` | The long line of nonsense we emailed you. This is how your computer checks a signal really came from us and not from someone pretending. |
| `exchange` | `binance` or `coindcx` — whichever you use. |
| `capital_usd` | **The money you're putting behind this**, in dollars. Read the note below — this one matters more than it looks. |
| `symbols` | Leave as `[]`, which means "follow everything". |
| `tick_seconds` | Leave at `20`. |

**About `capital_usd`.** This isn't a safety limit you probably won't reach. It
is the number every order size is worked out from. Put 3000 and it will trade
as though it has $3,000 to work with. Put your real figure.

Save the file and close it.

## Step 7 — Give it your exchange keys

Go to your exchange, create an API key with **trading ticked and withdrawals
NOT ticked** (see the section above), and copy the two long strings it gives
you — usually called the *key* and the *secret*.

Open your settings file again and add them, as two extra lines just before the
closing `}`. Don't forget the comma at the end of the line above:

```json
  "tick_seconds": 20,
  "api_key": "paste the key here",
  "api_secret": "paste the secret here"
}
```

Save and close.

> **From now on, that file has your exchange keys in it.** Don't email it,
> don't put it in a chat, don't screenshot it. If you ever need to send us
> something to help sort out a problem, use step 8 — it prints the same
> information with the secret parts blanked out, safe to send to anyone.

## Step 8 — Test it, without trading anything

```
python3 -m executor --check
```

This buys nothing and sells nothing. It just checks the three things that have
to be right, and tells you about all of them at once, so you're not fixing one
problem at a time.

You want three lines saying PASS:

```
PASS  Key set: verified against your root key, 1 active signing key(s)
PASS  Exchange: reachable, 3,142.88 USDT free
PASS  Feed: connected and snapshotted, 2 campaign(s) following
```

If one says FAIL:

| It says | Almost always means |
|---|---|
| FAIL Key set | The root key in your settings file got copied wrong — a missing character, or a space at the end. If it says `could not fetch` rather than anything about your key, that one's our end, not yours; tell us. |
| FAIL Exchange | The exchange key or secret got copied wrong, or you didn't tick trading when you created it. The exchange's own words are printed after it. |
| FAIL Feed, `not entitled` | We haven't registered you yet, or your subscription has run out. Email us — trying again won't fix it. |

Don't go past this step until all three say PASS.

## Step 9 — Start it

```
python3 -m executor
```

It's now running. Leave that window open — closing it stops the program.

Now open your web browser and go to:

**http://127.0.0.1:7757**

That's your dashboard. It's coming from your own computer, not from the
internet, and nobody else can open it — not us, not someone on your wifi.

---

## The one number to look at

At the top of your dashboard, in big text:

> **If this machine stops now, at most $412.50 can fill unwatched.**

That single line is the entire risk of running this on your own computer, and
it's the reason it's the biggest thing on the screen.

Here's what it means. At any moment there is at most **one** buy order sitting
out at the exchange waiting for the price to come to it. If your computer dies
right now — power cut, cat on the keyboard — that one order could still get
filled while nobody's watching, and you'd own that coin until you start the
program again. The number tells you exactly how much that is.

When it says **"Nothing can fill while this machine is away"**, there's no
order waiting, and you can shut the lid without a second thought.

Two buttons sit underneath it:

- **Stand down** — cancels every buy order right now, and leaves every sell
  order in place protecting what you already own. This is the "I have to go and
  I don't want to think about it" button.
- **Pause opening** — stops it starting anything new, while letting everything
  already running finish normally.

Neither of them ever walks away from something you own. Nothing in this program
will cancel a sell order that's protecting a coin you're holding.

## Closing the lid, and going away

**If you shut it down properly** — click into the black window and press
`Ctrl + C`, or use Stand down first — it cancels the waiting buy orders, makes
sure everything you own has a sell order protecting it, and writes down where
it got to. Give it a few seconds to finish.

**If you just close the laptop lid**, your computer gives the program a few
seconds' warning before it sleeps, and it uses them to do the same tidy-up. How
many seconds depends on the computer, and this is worth knowing:

| | |
|---|---|
| **Mac** | About 30 seconds' warning. Plenty. It also quietly stops your Mac dozing off on its own while an order is waiting — and stops doing that the moment nothing is. |
| **Linux** | About 5 seconds. Enough. |
| **Windows** | About 2 seconds — **not** enough to cancel an order and be sure it worked. So on Windows, treat that big number on your dashboard as the real thing, and use **Stand down** before you close the lid. |

Nobody's software can stop a laptop lid from closing. What this program gets is
a few seconds to react, never a veto — so it's built around using those seconds
well, not around keeping your computer awake.

**When you come back**, it asks your exchange what actually happened while it
was away, puts a sell order on anything you turn out to be holding, and shows
you what changed. It won't start anything new until you click **"I've reviewed
— resume trading"**. That's deliberate. A program that's been away doesn't get
to guess.

## When it stops trading on its own

| What happened | What it does |
|---|---|
| Your subscription runs out | It stops receiving new signals. **Anything you already own is still looked after and still sold at its target.** It won't leave you holding something. |
| You start a second copy by mistake | The newest one carries on and the older one steps aside and says so. |
| Your wifi drops, or we restart something | It reconnects by itself. Being disconnected doesn't mean it's forgotten what you own. |
| A signal doesn't add up | It drops that one trade and tells you. Real money doesn't go anywhere near numbers that don't check out. |
| A move started more than 5 minutes ago | It skips it. These trades only make sense from their beginning; joining halfway down is a different and worse idea. |

## Why $1,000

The strategy works by buying in steps on the way down — a little, then a bit
more, then more — so your average price improves as the price falls.

Exchanges refuse orders below a minimum size (often about $5–10). Below $1,000,
the early, smaller steps come out under that minimum and get rejected, so
you'd only ever get the big one at the bottom. That's a completely different
strategy from the one you're paying for, and not one we'd recommend.

Between $1,000 and $3,000 it works, but on smaller moves some of the early
steps still get skipped, so it buys in fewer and deeper chunks than the signal
describes. Your dashboard says so when it happens. Above $3,000, every step
lands as intended.

## If something looks wrong

Run this and send us what it prints:

```
python3 -m executor --check
```

It blanks out your exchange secret automatically, so it's safe to send to
anyone, and it's nearly always enough for us to tell you what's wrong.

## What we can see about you

Your public line of nonsense from step 5. When your program connects and
disconnects. Whether you've paid.

That's the entire list. We don't have your exchange keys, so we can't see your
balance or your trades. We never receive what you bought or sold, so we don't
know whether you're up or down. There is no place in the signal we send you for
any of that to travel, which is enforced by our software rather than just
promised on this page.

## The honest version of the risk

This program trades from your computer. If your computer stops, an order that
was waiting may still go through, and you'd own that coin until your computer
comes back.

It cannot be sold out from under you — you're buying with your own money, not
borrowed — and the most that can happen unwatched is the one order shown on
your dashboard. That risk is small, and bounded, and it is not zero. Anyone who
tells you their trading software has no such gap is either not thinking about
it or not telling you.
