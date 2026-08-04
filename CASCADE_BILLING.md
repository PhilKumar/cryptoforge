# Cascade billing v1 — Razorpay

How a buyer's money becomes, and stops being, an entitlement. Design — nothing
implements this yet.

## The one rule

**Billing's only job is to keep the subscriber record truthful.** The record
already exists (`cascade_feed_subscribers`: status + `expires_at`), the
entitlement check already reads it on every heartbeat, and a lapse already
reaches a connected executor within 30 seconds while its open positions keep
their exits. Billing adds no second entitlement system, no token, no license
file. It writes the same record `entitled()` reads, and everything downstream
is already built and tested.

What billing must never touch, stated once: signing keys (compromise is not a
billing state), the feed log, and anything that manages an open position.
`revoked` stays a manual, Phil-only status for bans and compromised keys —
billing only ever writes `active` and `lapsed`, so a webhook can never undo a
ban.

## Why Razorpay

Buyers are Indian, pricing is INR, and recurring INR collection means UPI
Autopay / e-mandates — which is Razorpay's home ground. Their Subscriptions
product does the retry ladder, mandate management and hosted checkout, so we
build none of it. If international buyers ever matter, that is a different
processor (or Razorpay International activation) and a different document;
nothing below assumes only-India except the payment rail itself.

## The order of onboarding: key first, money second

1. Buyer installs the executor, runs `--register`, sends Phil the printed
   public key.
2. Phil registers it (`POST /api/cascade/feed/subscribers`). The buyer can now
   connect and see the feed working — but `expires_at` is set to now, so
   nothing new opens. A free look at a live product, expiring immediately.
3. Phil creates the subscription in the Razorpay dashboard with
   `notes.buyer_id` set, and sends the buyer the payment link.
4. The buyer pays; webhooks keep `expires_at` ahead of the calendar from then
   on.

Key-first means billing's writes always land on an existing record, which
removes the whole class of "paid but no key yet" state. The failure mode of
the opposite order — money arrives, key doesn't, entitlement dangles — never
exists.

No plan/subscription creation API on our side. At a handful of buyers the
Razorpay dashboard *is* the admin UI, and every endpoint not built is an
endpoint not attackable.

## The load-bearing rule: webhooks extend life, silence ends it

The subscriber record's `expires_at` is stamped as:

```
expires_at = subscription.current_end + GRACE_DAYS (3)
```

on every successful charge. Nothing ever extends it without money.

This makes the system fail **closed** in the same way the key-set cache does:
if our webhook endpoint is down for a week, or Razorpay stops calling, or the
mapping breaks, a paying buyer keeps trading until `current_end + 3d` and then
stops — and support hears about it. A buyer who stopped paying is never kept
alive by our outage, because nothing was written. The dangerous failure
(silence = trades forever) is structurally impossible; the annoying failure
(outage = paying buyer stops at period end) is bounded and visible.

Three days of grace covers UPI Autopay's real-world flakiness: mandates fail
transiently and Razorpay retries on its own schedule. Grace is absorbed into
`expires_at` so the executor needs no new concept — it already honours expiry.

## Webhook handling

`POST /api/billing/razorpay/webhook` — public (no session; Razorpay is the
caller), and hardened the same three ways as everything else on this wire:

1. **Signature over the raw body bytes.** `X-Razorpay-Signature` is
   HMAC-SHA256 of the exact payload with the webhook secret. Verify the bytes
   received, never a re-serialization — the same discipline as the feed frames
   and the CoinDCX client, for the same reason.
2. **Idempotent by event id.** Razorpay redelivers; a `billing_events` bucket
   records processed ids and a repeat is a 200 no-op. Never 4xx a duplicate —
   that just makes them redeliver harder.
3. **The event is a doorbell, not the truth.** On any subscription event, we
   fetch the subscription from Razorpay's API and act on *that* — the same
   "ask the exchange, believe it over local state" rule the executor lives by.
   A forged-but-signed replay or an out-of-order delivery then cannot move
   state backwards, because state is always recomputed from the source.

Event → action (after the authoritative fetch):

| Razorpay state | We write | Why |
|---|---|---|
| `active` / charged | `active`, `expires_at = current_end + 3d` | money arrived |
| `pending` | nothing | Razorpay is mid-retry; grace already covers it |
| `halted` | `lapsed` | retries exhausted — the mandate is dead |
| `cancelled` / `completed` / `expired` | nothing | already paid through `current_end`; expiry ends it on time |
| `paused` | `lapsed` | deliberate stop, effective now |

`cancelled` deliberately writes nothing: the buyer paid for the period, so
cutting them at the cancel click would be taking paid-for service back.
Expiry handles it to the day.

Unknown `buyer_id` in `notes` → log loudly, alert Phil, 200. It means a
dashboard typo, and the fix is human.

## What we store

- `billing_events` bucket: processed event ids (idempotency), pruned with the
  same retention as the feed log.
- On the subscriber record: `razorpay_subscription_id`, `last_charge_at`,
  `expires_at` (already exists). Nothing card-shaped ever touches our box —
  Razorpay holds the instrument, we hold a status. The same shape as the
  exchange-credentials rule: there is no secret here worth stealing.

Admin surface: the existing subscriber list grows `razorpay_subscription_id`
and `expires_at` columns; lapse/revoke buttons already exist as routes.

## What can go wrong, and what it costs

| Failure | Effect | Bounded by |
|---|---|---|
| Webhook endpoint down | paying buyer stops at `current_end + 3d` | fail-closed rule; support hears it |
| Webhook secret leaks | forged events → doorbell only | authoritative fetch decides |
| Razorpay API down during fetch | event retried later; state unchanged | Razorpay's redelivery + our idempotency |
| Buyer's mandate silently dies | trades until `expires_at`, then lapses | grace = 3d, no infinite tail |
| Dashboard typo in `notes.buyer_id` | loud alert, no state change | human fixes, replays event |

The irreducible one, stated plainly like the executor's: a buyer whose final
period is refunded by Phil manually keeps entitlement until `expires_at`
unless he also clicks lapse. Refunds are manual and rare; the lapse button is
right there.

## Built — 2026-08-04

Steps 1–3 are in: `engine/billing.py` (signature, decision table, write
limits, idempotency, the fetch client) and `POST
/api/billing/razorpay/webhook`, with the subscriber list carrying
`razorpay_subscription_id` and `days_left`. Every gate is verified by
disabling it and watching a test fail.

**Step 4 has not happened.** Nothing has spoken to Razorpay — the client is
exercised against an injected stub, so the request shape is assumed correct
until one real test-mode call proves it. Do that before the first paying
buyer, not after.

## Needed from Phil before build

- Razorpay account with Subscriptions enabled (KYC done), a Plan (price is
  his call), and the webhook secret into the server env as
  `RAZORPAY_WEBHOOK_SECRET` (key id/secret as `RAZORPAY_KEY_ID` /
  `RAZORPAY_KEY_SECRET`).
- A decision only he can make: monthly vs quarterly billing. Everything above
  is period-agnostic.
