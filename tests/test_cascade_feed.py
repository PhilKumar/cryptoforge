"""What may and may not cross the wire to a buyer's executor.

The signal feed publishes geometry and withholds money. That split is the whole
product: it is why we never hold a buyer's exchange credentials, and it is why
the format is honest — a follower's account state genuinely differs from ours,
so publishing ours would be publishing a lie.

The load-bearing test here is `test_a_fully_loaded_campaign_leaks_nothing`. It
takes a campaign carrying capital, fills, rounds, resting orders and a live
take-profit — the real shape, not a fixture built to pass — and runs every
builder over it, asserting that no account-specific field appears anywhere in
the output at any depth.
"""

import unittest
from datetime import datetime, timezone

from cryptography.exceptions import InvalidSignature

from engine.cascade import (
    MODEL_VERSION,
    Campaign,
    FibLadder,
    Fill,
    Leg,
    PendingOrder,
    Round,
    Trendline,
    build_fib_ladder_and_pool,
)
from engine.cascade_feed import (
    FEED_VERSION,
    LOGGED_TYPES,
    NEVER_PUBLISH,
    ROOT_KID,
    CascadeFeedPublisher,
    FeedLeak,
    FeedLog,
    FeedSigner,
    FeedSubscribers,
    NotEntitled,
    active_public_keys,
    build_envelope,
    build_key_set,
    build_snapshot,
    campaign_closed_payload,
    campaign_opened_payload,
    campaign_state_payload,
    epoch_from_ist,
    gross_allocation_pct,
    key_set_expiry_warning,
    leg_finalized_payload,
    leg_opened_payload,
    sign_handshake,
    sign_key_set,
    trendline_set_payload,
    verify_frame,
    verify_key_set,
    verify_subscriber_handshake,
)


def _loaded_campaign() -> Campaign:
    """A campaign with real money in it, in every place money can be."""
    campaign = Campaign(
        campaign_id="casc_SOLUSDT_1785401234",
        symbol="SOLUSDT",
        capital_usd=2000.0,
        mother_high=178.42,
        mother_low=174.10,
        mother_timestamp=1785400800,
        min_notional_usd=5.0,
    )
    campaign.seq = 41
    campaign.mode = "live"
    campaign.created_at = "2026-08-03 19:47:00"  # _ist_now_str(): IST, unlabelled
    campaign.state = "TRENDLINE_ACTIVE"
    campaign.mc_kind = "major"
    campaign.tick_size = 0.01
    campaign.min_fib_range_pct = 0.0008
    campaign.median_bar_pct = 0.0011

    campaign.trendlines = [
        Trendline(
            trendline_id=3,
            anchor1_price=178.42,
            anchor1_timestamp=1785400800,
            anchor2_price=177.06,
            anchor2_timestamp=1785403500,
        )
    ]
    campaign.active_trendline_id = 3

    leg = Leg(
        leg_id=4,
        trendline_id=3,
        low=172.88,
        touch_high=176.40,
        touch_timestamp=1785404100,
        created_via_break=True,
    )
    leg.fib = FibLadder(high_anchor=176.40, low_anchor=172.88)
    leg.leg_pct_from_mother = 3.11
    leg.allocation_pct = 1.02
    leg.netted_pct = 0.163
    leg.pool_usd = 20.4
    leg.pool_total_usd = 20.4
    leg.pending_orders[2] = PendingOrder(
        level=2,
        price=169.36,
        usd_notional=5.50,
        quantity=0.0324,
        leg_id=4,
        status="PLACED",
        order_id="BINANCE-99812",
        client_order_id="cf-casc-41-4-2",
        entry_style="stop",
        stop_price=169.51,
        limit_price=169.62,
    )
    campaign.legs = [leg]

    campaign.all_fills = [
        Fill(
            price=169.40,
            quantity=0.0324,
            level=2,
            leg_id=4,
            timestamp=1785404400,
            order_id="BINANCE-99812",
            fee_usd=0.0055,
        )
    ]
    campaign.rounds = [
        Round(
            round_id=1,
            leg_id=3,
            avg_entry=174.02,
            quantity=0.0287,
            invested_usd=4.99,
            exit_price=175.10,
            pnl=0.021,
            pnl_gross=0.031,
            fees_usd=0.010,
        )
    ]
    campaign.avg_entry_price = 169.40
    campaign.filled_base_qty = 0.0324
    campaign.tp_price = 171.15
    campaign.tp_order_id = "BINANCE-99999"
    campaign.reuse_below = 172.51  # our round's floor, not theirs
    campaign.funded_bands = [(172.88, 176.40)]
    campaign.cumulative_used_pct = 1.02
    campaign.collected = 5.50
    campaign.realized_pnl = 0.021
    campaign.event_log = [{"level": "fill", "message": "Bought $5.50 at 169.40"}]
    return campaign


class FakeStore:
    """The subset of SQLiteJSONStore that FeedLog touches."""

    def __init__(self):
        self.rows = {}

    def get(self, bucket, key, default=None):
        return self.rows.get((bucket, str(key)), default)

    def put(self, bucket, key, payload):
        self.rows[(bucket, str(key))] = payload

    def delete(self, bucket, key):
        self.rows.pop((bucket, str(key)), None)

    def get_mapping(self, bucket):
        return {key: value for (row_bucket, key), value in self.rows.items() if row_bucket == bucket}


class PublishedFieldTests(unittest.TestCase):
    def test_a_fully_loaded_campaign_leaks_nothing(self):
        """The one that matters. Every builder, over a campaign holding money."""
        campaign = _loaded_campaign()
        data = campaign.to_dict()
        leg = data["legs"][0]

        payloads = [
            campaign_opened_payload(data),
            trendline_set_payload(data["trendlines"][0], supersedes=2),
            leg_opened_payload(leg, allocation_anchor=data["mother_high"]),
            leg_finalized_payload(leg["leg_id"]),
            campaign_state_payload(data),
            campaign_closed_payload(data),
        ]

        # Not "no banned key at the top level" — no banned key at ANY depth, and
        # no banned VALUE either. A leak that renamed the field on the way out
        # would still be a leak.
        banned_values = {
            campaign.capital_usd,
            campaign.tp_price,
            campaign.avg_entry_price,
            campaign.reuse_below,
            campaign.legs[0].pool_usd,
            campaign.legs[0].netted_pct,
            campaign.legs[0].allocation_pct,
        }
        for payload in payloads:
            for key in _walk_keys(payload):
                self.assertNotIn(key, NEVER_PUBLISH, f"{key} reached the wire")
            for value in _walk_values(payload):
                self.assertNotIn(value, banned_values, f"the value {value} reached the wire")

    def test_the_guard_catches_a_naive_copy(self):
        """The failure mode this exists for: reaching for to_dict() because it is shorter."""
        campaign = _loaded_campaign()
        with self.assertRaises(FeedLeak):
            build_envelope(
                msg_type="campaign.opened",
                symbol="SOLUSDT",
                campaign_id=campaign.campaign_id,
                payload=campaign.to_dict(),
                seq=1,
                model_version=MODEL_VERSION,
            )

    def test_the_guard_reaches_into_nested_structures(self):
        with self.assertRaises(FeedLeak) as caught:
            build_envelope(
                msg_type="leg.opened",
                symbol="SOLUSDT",
                campaign_id="c1",
                payload={"legs": [{"fib": {"high_anchor": 1.0, "pool_usd": 20.4}}]},
                seq=1,
                model_version=MODEL_VERSION,
            )
        self.assertIn("pool_usd", str(caught.exception))

    def test_gross_allocation_is_published_and_netted_is_not(self):
        """The executor nets locally: its siblings are not our siblings."""
        campaign = _loaded_campaign()
        leg = campaign.to_dict()["legs"][0]
        payload = leg_opened_payload(leg, allocation_anchor=178.42)
        self.assertIn("allocation_pct_gross", payload)
        self.assertNotIn("allocation_pct", payload)
        self.assertNotIn("netted_pct", payload)
        # gross, i.e. before the 0.163 a sibling had already funded
        self.assertAlmostEqual(payload["allocation_pct_gross"], (178.42 - 172.88) / 178.42 * 100, places=9)

    def test_derived_levels_match_the_fib_ladder(self):
        """`derived` is a checksum the executor recomputes — it has to be right."""
        campaign = _loaded_campaign()
        leg = campaign.to_dict()["legs"][0]
        ladder = FibLadder(high_anchor=176.40, low_anchor=172.88)
        payload = leg_opened_payload(leg, allocation_anchor=178.42)
        for level in (2, 4, 8):
            self.assertAlmostEqual(payload["derived"][f"level_{level}"], ladder.level_price(level), places=9)

    def test_exchange_filters_are_advisory_and_public(self):
        campaign = _loaded_campaign()
        payload = campaign_opened_payload(campaign.to_dict())
        self.assertEqual(payload["advisory"]["tick_size"], 0.01)
        self.assertEqual(payload["advisory"]["min_notional_usd"], 5.0)

    def test_our_paper_or_live_mode_is_not_the_buyers_business(self):
        campaign = _loaded_campaign()
        payload = campaign_opened_payload(campaign.to_dict())
        self.assertNotIn("mode", payload)
        self.assertIn("mode", NEVER_PUBLISH)

    def test_the_break_candle_goes_out_as_a_timestamp_not_our_ohlc(self):
        """The executor reads the same market; it needs the bar, not our copy."""
        campaign = _loaded_campaign()
        campaign.state = "MOTHER_BREAK_PENDING"
        campaign.mother_break_candle = {
            "timestamp": 1785412200,
            "open": 178.0,
            "high": 178.9,
            "low": 177.6,
            "close": 178.7,
            "timeframe": "5m",
        }
        campaign.mother_break_wait_remaining = 2
        payload = campaign_state_payload(campaign.to_dict())
        self.assertEqual(payload["mother_break_candle"], 1785412200)
        self.assertIsNone(payload["mother_break_top_candle"])
        self.assertEqual(payload["mother_break_wait_remaining"], 2)


class TimestampTests(unittest.TestCase):
    """`created_at` is what join-at-start measures. It has to mean one thing."""

    def test_an_ist_string_becomes_epoch_seconds(self):
        # 2026-08-03 19:47:00 IST == 14:17:00 UTC
        self.assertEqual(
            epoch_from_ist("2026-08-03 19:47:00"),
            int(datetime(2026, 8, 3, 14, 17, 0, tzinfo=timezone.utc).timestamp()),
        )

    def test_the_wire_never_carries_the_unlabelled_local_string(self):
        """A follower outside IST would read every campaign as 5½ hours old.

        `max_join_age_sec` is 300 seconds. An executor comparing its own clock
        against a naive IST string would skip every campaign we ever published,
        and would do it silently — nothing errors, nothing joins.
        """
        payload = campaign_opened_payload(_loaded_campaign().to_dict())
        self.assertIsInstance(payload["created_at"], int)
        self.assertEqual(payload["created_at"], epoch_from_ist("2026-08-03 19:47:00"))

    def test_junk_and_blanks_do_not_raise(self):
        for value in ("", "   ", None, "not a date", [], {}):
            self.assertIsNone(epoch_from_ist(value))

    def test_an_epoch_passes_through_untouched(self):
        self.assertEqual(epoch_from_ist(1785401234), 1785401234)


class SignatureTests(unittest.TestCase):
    def setUp(self):
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.envelope = build_envelope(
            msg_type="leg.finalized",
            symbol="SOLUSDT",
            campaign_id="casc_SOLUSDT_1785401234",
            payload=leg_finalized_payload(4),
            seq=148820,
            model_version=MODEL_VERSION,
        )

    def test_a_signed_frame_round_trips(self):
        frame = self.signer.frame(self.envelope)
        self.assertEqual(verify_frame(frame, self.keys), self.envelope)

    def test_a_tampered_message_fails(self):
        frame = self.signer.frame(self.envelope)
        frame["msg"] = frame["msg"].replace('"seq":148820', '"seq":148821')
        with self.assertRaises(InvalidSignature):
            verify_frame(frame, self.keys)

    def test_an_unknown_kid_fails(self):
        """Revocation is exactly this: drop the kid from the key set."""
        frame = self.signer.frame(self.envelope)
        with self.assertRaises(InvalidSignature):
            verify_frame(frame, {})

    def test_another_key_cannot_forge_our_geometry(self):
        """The attack the signature exists for: fabricated geometry, right kid."""
        attacker = FeedSigner.generate(self.signer.kid)
        frame = attacker.frame(self.envelope)
        with self.assertRaises(InvalidSignature):
            verify_frame(frame, self.keys)

    def test_verification_reads_the_bytes_it_was_sent(self):
        """No re-serialization anywhere in the path — that is the float defence.

        A verifier that rebuilt the JSON from the parsed object would be at the
        mercy of float formatting; 178.42 and 178.420 are one number and two
        byte strings. Here the signed text is the transmitted text, so a
        round-tripped envelope that re-encodes differently changes nothing.
        """
        frame = self.signer.frame(self.envelope)
        self.assertIn('"seq":148820', frame["msg"])
        parsed = verify_frame(frame, self.keys)
        self.assertEqual(parsed["v"], FEED_VERSION)
        self.assertEqual(parsed["model_version"], MODEL_VERSION)


class FeedLogTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.clock = [1785400000.0]
        self.log = FeedLog(self.store, now_fn=lambda: self.clock[0])

    def _append(self, symbol="SOLUSDT", leg_id=4, emitted_at=None):
        return self.log.append(
            envelope_fields={
                "msg_type": "leg.finalized",
                "symbol": symbol,
                "campaign_id": f"casc_{symbol}_1",
                "payload": leg_finalized_payload(leg_id),
                "model_version": MODEL_VERSION,
                "emitted_at": emitted_at if emitted_at is not None else int(self.clock[0]),
            },
            signer=self.signer,
        )

    def test_seq_is_monotonic_per_symbol(self):
        seqs = [verify_frame(self._append(leg_id=n), self.keys)["seq"] for n in range(1, 4)]
        self.assertEqual(seqs, [1, 2, 3])
        # A second symbol counts on its own.
        other = verify_frame(self._append(symbol="BTCUSDT"), self.keys)
        self.assertEqual(other["seq"], 1)

    def test_seq_never_restarts_after_retention_prunes_a_quiet_symbol(self):
        """The bug this is here to prevent: reissuing a seq an executor accepted.

        A symbol that goes quiet has its events aged out. Rebuilding the head
        from surviving rows alone would then restart at 1, and the executor
        would receive a seq it had already seen carrying different geometry —
        which is not a gap it can detect. The watermark is never pruned.
        """
        for n in range(1, 4):
            self._append(leg_id=n)
        self.clock[0] += 8 * 86400
        self.assertEqual(self.log.prune(), 3)
        fresh = FeedLog(self.store, now_fn=lambda: self.clock[0])
        self.assertEqual(verify_frame(self._append(), self.keys)["seq"], 4)
        self.assertEqual(fresh.head("SOLUSDT"), 4)

    def test_a_crash_between_the_two_writes_leaves_a_hole_not_a_reuse(self):
        """Allocate-then-write: the failure mode is a gap, which is recoverable."""
        self._append()
        # Simulate the crash: the watermark advanced, the event never landed.
        self.store.put("cascade_feed", "head|SOLUSDT", 2)
        fresh = FeedLog(self.store, now_fn=lambda: self.clock[0])
        frame = fresh.append(
            envelope_fields={
                "msg_type": "leg.finalized",
                "symbol": "SOLUSDT",
                "campaign_id": "casc_SOLUSDT_1",
                "payload": leg_finalized_payload(9),
                "model_version": MODEL_VERSION,
            },
            signer=self.signer,
        )
        self.assertEqual(verify_frame(frame, self.keys)["seq"], 3)
        # 2 was never used by anything — a hole the executor re-snapshots on.
        self.assertEqual([verify_frame(f, self.keys)["seq"] for f in fresh.since("SOLUSDT", 0)], [1, 3])

    def test_replay_returns_only_what_is_past_the_cursor(self):
        for n in range(1, 5):
            self._append(leg_id=n)
        replayed = [verify_frame(f, self.keys)["seq"] for f in self.log.since("SOLUSDT", 2)]
        self.assertEqual(replayed, [3, 4])

    def test_replay_does_not_bleed_between_symbols(self):
        self._append(symbol="SOLUSDT")
        self._append(symbol="BTCUSDT")
        self._append(symbol="SOLUSDT")
        sol = [verify_frame(f, self.keys)["symbol"] for f in self.log.since("SOLUSDT", 0)]
        self.assertEqual(sol, ["SOLUSDT", "SOLUSDT"])

    def test_heartbeats_are_never_stored(self):
        """They are liveness, not history — 2,880 a day of nothing to replay."""
        self._append()
        self.log.heartbeat(symbol="SOLUSDT", signer=self.signer, running_campaigns=3, model_version=MODEL_VERSION)
        self.log.heartbeat(symbol="SOLUSDT", signer=self.signer, running_campaigns=3, model_version=MODEL_VERSION)
        self.assertEqual(len(self.log.since("SOLUSDT", 0)), 1)
        self.assertNotIn("heartbeat", LOGGED_TYPES)

    def test_a_heartbeat_carries_the_head_so_a_silent_gap_is_found_in_30s(self):
        """Without this, a dropped event on a quiet symbol hides until the next one."""
        for n in range(1, 4):
            self._append(leg_id=n)
        beat = verify_frame(
            self.log.heartbeat(symbol="SOLUSDT", signer=self.signer, running_campaigns=1, model_version=MODEL_VERSION),
            self.keys,
        )
        self.assertEqual(beat["seq"], 3)
        self.assertEqual(beat["payload"]["running_campaigns"], 1)

    def test_prune_drops_old_events_and_keeps_the_watermark(self):
        self._append(emitted_at=int(self.clock[0]))
        self.clock[0] += 8 * 86400
        self._append(emitted_at=int(self.clock[0]))
        self.assertEqual(self.log.prune(), 1)
        self.assertEqual(self.log.head("SOLUSDT"), 2)
        self.assertEqual([verify_frame(f, self.keys)["seq"] for f in self.log.since("SOLUSDT", 0)], [2])

    def test_it_lists_every_symbol_it_holds_events_for(self):
        """Not the same as "symbols with a live campaign", and the difference
        was a real bug: a campaign's last message is campaign.closed, emitted
        when the engine has already dropped it. A stream that only looked at
        live symbols wrote that message and never delivered it — so an executor
        never learned the campaign had ended."""
        self._append(symbol="SOLUSDT")
        self._append(symbol="BTCUSDT")
        self.assertEqual(self.log.symbols(), ["BTCUSDT", "SOLUSDT"])

    def test_the_symbol_list_ignores_the_head_watermarks(self):
        self._append(symbol="SOLUSDT")
        self.assertNotIn("head", self.log.symbols())

    def test_an_unlogged_type_is_refused(self):
        with self.assertRaises(ValueError):
            self.log.append(
                envelope_fields={
                    "msg_type": "heartbeat",
                    "symbol": "SOLUSDT",
                    "campaign_id": "",
                    "payload": {"running_campaigns": 1},
                    "model_version": MODEL_VERSION,
                },
                signer=self.signer,
            )


class PublisherTests(unittest.TestCase):
    """The differ. It sees engine status snapshots and emits what changed."""

    def setUp(self):
        self.store = FakeStore()
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.errors = []
        self.log = FeedLog(self.store)
        self.publisher = CascadeFeedPublisher(
            self.log,
            self.signer,
            model_version=MODEL_VERSION,
            on_error=lambda what, exc: self.errors.append((what, exc)),
        )

    def _status(self, campaign):
        return {"campaigns": [campaign.to_dict()], "closed_campaigns": []}

    def _types(self, frames):
        return [verify_frame(frame, self.keys)["type"] for frame in frames]

    def test_a_new_campaign_announces_itself_and_its_geometry(self):
        campaign = _loaded_campaign()
        self.assertEqual(
            self._types(self.publisher.publish(self._status(campaign))),
            ["campaign.opened", "trendline.set", "leg.opened", "campaign.state"],
        )

    def test_an_unchanged_campaign_says_nothing(self):
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        self.assertEqual(self.publisher.publish(self._status(campaign)), [])

    def test_finalizing_a_leg_is_announced_once(self):
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        campaign.legs[0].finalized = True
        self.assertEqual(self._types(self.publisher.publish(self._status(campaign))), ["leg.finalized"])
        self.assertEqual(self.publisher.publish(self._status(campaign)), [])

    def test_a_new_trendline_never_claims_to_supersede(self):
        """A new id means the old line was SPENT by a close above, not
        replaced — the successor legitimately fans lower from the same mother
        high. Chaining ids here told the executor's standing-line guard to
        halt every normal multi-line campaign."""
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        campaign.trendlines.append(
            Trendline(
                trendline_id=4,
                anchor1_price=178.42,
                anchor1_timestamp=1785400800,
                anchor2_price=176.10,  # below line 3's anchor2 — the normal case
                anchor2_timestamp=1785406000,
            )
        )
        frames = self.publisher.publish(self._status(campaign))
        message = verify_frame(frames[0], self.keys)
        self.assertEqual(message["type"], "trendline.set")
        self.assertIsNone(message["payload"]["supersedes"])

    def test_a_state_change_is_announced(self):
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        campaign.state = "MOTHER_BREAK_PENDING"
        self.assertEqual(self._types(self.publisher.publish(self._status(campaign))), ["campaign.state"])

    def test_paper_campaigns_are_not_published(self):
        """Real geometry, but not money we stand behind."""
        campaign = _loaded_campaign()
        campaign.mode = "paper"
        self.assertEqual(self.publisher.publish(self._status(campaign)), [])

    def test_a_close_is_announced_once_and_only_if_we_announced_the_open(self):
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        campaign.state = "MOTHER_BROKEN"
        campaign.close_reason = "mother_broken"
        campaign.closed_at = "2026-08-03 21:10:00"
        closed = {"campaigns": [], "closed_campaigns": [campaign.to_dict()]}
        self.assertEqual(self._types(self.publisher.publish(closed)), [])  # returned frames exclude closures
        stream = [verify_frame(f, self.keys)["type"] for f in self.log.since("SOLUSDT", 0)]
        self.assertEqual(stream[-1], "campaign.closed")
        before = len(self.log.since("SOLUSDT", 0))
        self.publisher.publish(closed)
        self.assertEqual(len(self.log.since("SOLUSDT", 0)), before)

    def test_a_campaign_that_ended_before_the_feed_was_on_is_not_news(self):
        campaign = _loaded_campaign()
        campaign.state = "MOTHER_BROKEN"
        self.publisher.publish({"campaigns": [], "closed_campaigns": [campaign.to_dict()]})
        self.assertEqual(self.log.since("SOLUSDT", 0), [])

    def test_a_broken_feed_never_stops_the_engine(self):
        """The whole reason this hangs off on_update instead of the tick loop."""

        class Exploding:
            def append(self, **kwargs):
                raise RuntimeError("disk is on fire")

            def prune(self):
                return 0

        publisher = CascadeFeedPublisher(
            Exploding(),
            self.signer,
            model_version=MODEL_VERSION,
            on_error=lambda what, exc: self.errors.append((what, exc)),
        )
        self.assertEqual(publisher.publish(self._status(_loaded_campaign())), [])
        self.assertTrue(self.errors)

    def test_a_leak_is_the_one_thing_that_is_not_swallowed(self):
        """Everything else degrades quietly. Nearly publishing a position does not."""

        class Leaky:
            def append(self, **kwargs):
                raise FeedLeak("payload.capital_usd")

            def prune(self):
                return 0

        publisher = CascadeFeedPublisher(Leaky(), self.signer, model_version=MODEL_VERSION)
        with self.assertRaises(FeedLeak):
            publisher._emit("campaign.opened", "SOLUSDT", "c1", {})

    def test_the_published_anchor_matches_what_the_engine_computed(self):
        """Not a restatement of my own formula — checked against the engine's.

        The engine funds a leg from the PREVIOUS leg's low, falling back to the
        mother high, with a truthiness check on that low that a plain index
        test would miss. We publish the anchor and the gross percent; the
        executor nets locally. If these two drift the executor halts, so they
        are worth pinning to the real function.
        """
        campaign = Campaign(
            campaign_id="c1",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=1785400800,
        )
        campaign.mode = "live"
        first = Leg(leg_id=1, trendline_id=1, low=175.00, touch_high=177.90, touch_timestamp=1785401000)
        campaign.legs.append(first)
        build_fib_ladder_and_pool(campaign, first)
        second = Leg(leg_id=2, trendline_id=1, low=172.88, touch_high=176.40, touch_timestamp=1785404100)
        campaign.legs.append(second)
        build_fib_ladder_and_pool(campaign, second)

        legs = campaign.to_dict()["legs"]
        data = campaign.to_dict()
        self.assertEqual(CascadeFeedPublisher._allocation_anchor(data, legs, 0), 178.42)
        self.assertEqual(CascadeFeedPublisher._allocation_anchor(data, legs, 1), 175.00)
        for index, leg in enumerate(legs):
            anchor = CascadeFeedPublisher._allocation_anchor(data, legs, index)
            published = leg_opened_payload(leg, allocation_anchor=anchor)["allocation_pct_gross"]
            engine_gross = leg["allocation_pct"] + leg["netted_pct"]
            self.assertAlmostEqual(published, engine_gross, places=9)


class KeySetTests(unittest.TestCase):
    """The trust chain: root signs the set, the set decides which feed keys count."""

    def setUp(self):
        self.root = FeedSigner.generate(ROOT_KID)
        self.feed = FeedSigner.generate("cf-feed-2026a")
        self.now = 1785770000
        self.document = build_key_set(
            [
                {
                    "kid": self.feed.kid,
                    "public": self.feed.public_key_b64(),
                    "not_before": self.now,
                    "not_after": self.now + 90 * 86400,
                }
            ],
            issued_at=self.now,
        )
        self.frame = sign_key_set(self.document, self.root)

    def test_the_executor_trusts_a_root_signed_set(self):
        verified = verify_key_set(self.frame, self.root.public_key_b64(), now=self.now + 60)
        self.assertEqual(active_public_keys(verified, now=self.now + 60), {self.feed.kid: self.feed.public_key_b64()})

    def test_a_set_signed_by_anything_but_the_root_is_refused(self):
        """The attack the offline root exists for: a compromised server minting its own successor."""
        attacker = FeedSigner.generate(ROOT_KID)
        forged = sign_key_set(self.document, attacker)
        with self.assertRaises(InvalidSignature):
            verify_key_set(forged, self.root.public_key_b64(), now=self.now + 60)

    def test_a_feed_key_may_not_sign_the_set_that_authorises_it(self):
        with self.assertRaises(ValueError):
            sign_key_set(self.document, self.feed)

    def test_revoking_a_kid_drops_it_from_the_trusted_map(self):
        revoked = build_key_set(self.document["keys"], revoked=[self.feed.kid], issued_at=self.now)
        self.assertEqual(active_public_keys(revoked, now=self.now + 60), {})

    def test_a_revoked_key_can_no_longer_verify_a_real_message(self):
        """End of the chain — revocation has to reach actual messages, not just a list."""
        envelope = build_envelope(
            msg_type="leg.finalized",
            symbol="SOLUSDT",
            campaign_id="c1",
            payload=leg_finalized_payload(4),
            seq=1,
            model_version=MODEL_VERSION,
        )
        message = self.feed.frame(envelope)
        self.assertEqual(verify_frame(message, active_public_keys(self.document, now=self.now + 60))["seq"], 1)
        revoked = build_key_set(self.document["keys"], revoked=[self.feed.kid], issued_at=self.now)
        with self.assertRaises(InvalidSignature):
            verify_frame(message, active_public_keys(revoked, now=self.now + 60))

    def test_an_expired_set_fails_closed(self):
        """A lapsed document must stop new campaigns, not keep trusting what it saw."""
        with self.assertRaises(InvalidSignature):
            verify_key_set(self.frame, self.root.public_key_b64(), now=self.now + 31 * 86400)

    def test_expiry_is_what_makes_a_replayed_old_set_stop_working(self):
        """Serving last month's set is how an attacker un-revokes a key."""
        stale = sign_key_set(build_key_set(self.document["keys"], issued_at=self.now - 40 * 86400), self.root)
        with self.assertRaises(InvalidSignature):
            verify_key_set(stale, self.root.public_key_b64(), now=self.now)

    def test_a_key_outside_its_own_window_is_not_trusted(self):
        self.assertEqual(active_public_keys(self.document, now=self.now - 1), {})
        self.assertEqual(active_public_keys(self.document, now=self.now + 91 * 86400), {})

    def test_the_expiry_warning_arrives_with_time_to_act(self):
        self.assertIsNone(key_set_expiry_warning(self.document, now=self.now))
        self.assertIn("3 day", key_set_expiry_warning(self.document, now=self.now + 27 * 86400))
        self.assertIn("EXPIRED", key_set_expiry_warning(self.document, now=self.now + 31 * 86400))


class EnvelopeTests(unittest.TestCase):
    def test_the_envelope_carries_the_model_version_it_was_drawn_under(self):
        """An executor that does not know the version must not open campaigns."""
        envelope = build_envelope(
            msg_type="campaign.opened",
            symbol="SOLUSDT",
            campaign_id="c1",
            payload=campaign_opened_payload(_loaded_campaign().to_dict()),
            seq=1,
            model_version=MODEL_VERSION,
        )
        self.assertEqual(envelope["model_version"], MODEL_VERSION)
        self.assertEqual(envelope["v"], FEED_VERSION)

    def test_gross_allocation_handles_a_missing_anchor(self):
        self.assertIsNone(gross_allocation_pct(None, 172.88))
        self.assertIsNone(gross_allocation_pct(0, 172.88))
        self.assertIsNone(gross_allocation_pct(178.42, None))


def _walk_keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_keys(item)


def _walk_values(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk_values(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_values(item)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        yield value


if __name__ == "__main__":
    unittest.main()


class SubscriberTests(unittest.TestCase):
    """Who may listen, and how they prove it."""

    def setUp(self):
        self.store = FakeStore()
        self.now = [1785770000.0]
        self.subs = FeedSubscribers(self.store, now_fn=lambda: self.now[0])
        self.buyer = FeedSigner.generate("buyer-7")
        self.subs.add("buyer-7", self.buyer.public_key_b64(), label="Phil's laptop")

    def _handshake(self, **overrides):
        payload = sign_handshake("buyer-7", self.buyer, nonce="n1", timestamp=self.now[0])
        payload.update(overrides)
        return payload

    def test_we_store_only_the_public_half(self):
        """The same principle as not holding their exchange keys."""
        record = self.subs.get("buyer-7")
        self.assertEqual(record["public_key"], self.buyer.public_key_b64())
        self.assertNotIn("private_key", record)

    def test_a_good_handshake_is_accepted(self):
        result = verify_subscriber_handshake(self._handshake(), self.subs, now=self.now[0])
        self.assertEqual(result["subscriber"]["buyer_id"], "buyer-7")
        self.assertFalse(result["join_risk"])

    def test_an_unregistered_machine_is_refused(self):
        stranger = FeedSigner.generate("buyer-99")
        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(
                sign_handshake("buyer-99", stranger, nonce="n1", timestamp=self.now[0]),
                self.subs,
                now=self.now[0],
            )

    def test_a_forged_signature_is_refused(self):
        """Someone who knows a buyer id but not their key."""
        impostor = FeedSigner.generate("buyer-7")
        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(
                sign_handshake("buyer-7", impostor, nonce="n1", timestamp=self.now[0]),
                self.subs,
                now=self.now[0],
            )

    def test_a_deleted_buyer_cannot_connect(self):
        self.subs.remove("buyer-7")
        self.assertIsNone(self.subs.get("buyer-7"))
        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(self._handshake(), self.subs, now=self.now[0])

    def test_deleting_lets_the_same_machine_register_from_scratch(self):
        """`add()` on a live id re-keys it and carries the old status forward.
        Deleting first is what makes a genuinely fresh registration possible."""
        self.subs.set_status("buyer-7", "lapsed")
        self.subs.remove("buyer-7")
        record = self.subs.add("buyer-7", self.buyer.public_key_b64())
        self.assertEqual(record["status"], "active")
        self.assertIsNone(record["expires_at"])

    def test_deleting_an_unknown_buyer_is_an_error_not_a_silent_pass(self):
        with self.assertRaises(KeyError):
            self.subs.remove("nobody")

    def test_a_lapsed_subscription_cannot_connect(self):
        self.subs.set_status("buyer-7", "lapsed")
        with self.assertRaises(NotEntitled) as caught:
            verify_subscriber_handshake(self._handshake(), self.subs, now=self.now[0])
        self.assertIn("lapsed", str(caught.exception))

    def test_an_expired_subscription_cannot_connect(self):
        self.subs.add("buyer-7", self.buyer.public_key_b64(), expires_at=int(self.now[0]) - 1)
        ok, reason = self.subs.entitled("buyer-7", now=self.now[0])
        self.assertFalse(ok)
        self.assertIn("expired", reason)

    def test_a_captured_handshake_cannot_be_replayed(self):
        seen = {}
        handshake = self._handshake()
        verify_subscriber_handshake(handshake, self.subs, now=self.now[0], seen_nonces=seen)
        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(handshake, self.subs, now=self.now[0], seen_nonces=seen)

    def test_a_stale_handshake_is_refused_even_with_a_fresh_nonce(self):
        old = sign_handshake("buyer-7", self.buyer, nonce="n2", timestamp=self.now[0] - 600)
        with self.assertRaises(NotEntitled):
            verify_subscriber_handshake(old, self.subs, now=self.now[0])

    def test_a_skewed_clock_is_measured_not_just_tolerated(self):
        """The silent failure: a fast clock makes join-at-start skip everything.

        max_join_age_sec is 300 seconds, judged against the executor's own
        clock. A machine 90 seconds fast still connects fine and still looks
        healthy — it just quietly decides every campaign is too old. The buyer
        has no way to tell that from a market where nothing is happening.
        """
        skewed = sign_handshake("buyer-7", self.buyer, nonce="n3", timestamp=self.now[0] + 90)
        result = verify_subscriber_handshake(skewed, self.subs, now=self.now[0])
        self.assertTrue(result["join_risk"])
        self.assertAlmostEqual(result["clock_skew_sec"], 90, places=3)

    def test_the_handshake_identity_cannot_disagree_with_its_signature(self):
        with self.assertRaises(ValueError):
            sign_handshake("someone-else", self.buyer, nonce="n1")


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}

    def test_a_cold_executor_gets_the_whole_current_picture(self):
        campaign = _loaded_campaign()
        frames = build_snapshot(
            [campaign.to_dict()], self.signer, model_version=MODEL_VERSION, head_by_symbol={"SOLUSDT": 42}
        )
        messages = [verify_frame(frame, self.keys) for frame in frames]
        self.assertEqual(
            [m["type"] for m in messages],
            ["campaign.opened", "trendline.set", "leg.opened", "campaign.state"],
        )

    def test_snapshot_frames_carry_the_head_so_the_cursor_lands_right(self):
        campaign = _loaded_campaign()
        frames = build_snapshot(
            [campaign.to_dict()], self.signer, model_version=MODEL_VERSION, head_by_symbol={"SOLUSDT": 42}
        )
        self.assertTrue(all(verify_frame(f, self.keys)["seq"] == 42 for f in frames))

    def test_a_snapshot_leaks_nothing_either(self):
        campaign = _loaded_campaign()
        frames = build_snapshot(
            [campaign.to_dict()], self.signer, model_version=MODEL_VERSION, head_by_symbol={"SOLUSDT": 1}
        )
        for frame in frames:
            for key in _walk_keys(verify_frame(frame, self.keys)["payload"]):
                self.assertNotIn(key, NEVER_PUBLISH)

    def test_paper_campaigns_are_absent_from_a_snapshot_too(self):
        campaign = _loaded_campaign()
        campaign.mode = "paper"
        self.assertEqual(
            build_snapshot([campaign.to_dict()], self.signer, model_version=MODEL_VERSION, head_by_symbol={}), []
        )


class VenueTests(unittest.TestCase):
    """Whose candles a fib was drawn on. Binance SOLUSDT is not CoinDCX SOLUSDT."""

    def test_the_campaigns_own_venue_is_published(self):
        campaign = _loaded_campaign()
        campaign.exchange = "coindcx"
        payload = campaign_opened_payload(campaign.to_dict(), default_exchange="binance")
        self.assertEqual(payload["exchange"], "coindcx")

    def test_a_blank_venue_resolves_to_the_engines_default(self):
        """The engine stores "" for "wherever this engine points". A bare ""
        on the wire would tell a buyer nothing at all."""
        campaign = _loaded_campaign()
        self.assertEqual(campaign.to_dict()["exchange"], "")
        payload = campaign_opened_payload(campaign.to_dict(), default_exchange="binance")
        self.assertEqual(payload["exchange"], "binance")

    def test_the_snapshot_names_the_venue_too(self):
        signer = FeedSigner.generate("cf-feed-2026a")
        campaign = _loaded_campaign()
        frames = build_snapshot(
            [campaign.to_dict()],
            signer,
            model_version=MODEL_VERSION,
            head_by_symbol={"SOLUSDT": 1},
            default_exchange="binance",
        )
        opened = verify_frame(frames[0], {signer.kid: signer.public_key_b64()})
        self.assertEqual(opened["payload"]["exchange"], "binance")

    def test_the_publisher_stamps_its_engines_venue(self):
        store = FakeStore()
        signer = FeedSigner.generate("cf-feed-2026a")
        log = FeedLog(store)
        publisher = CascadeFeedPublisher(log, signer, model_version=MODEL_VERSION, default_exchange="binance")
        frames = publisher.publish({"campaigns": [_loaded_campaign().to_dict()], "closed_campaigns": []})
        opened = verify_frame(frames[0], {signer.kid: signer.public_key_b64()})
        self.assertEqual(opened["payload"]["exchange"], "binance")


class PublishCatalogueTests(unittest.TestCase):
    """Phil follows BTC for himself; buyers get SOL and ETH.

    The gate bites at the ANNOUNCEMENT and never mid-flight: a campaign that
    was published finishes its life on the feed, because buyers may be holding
    positions laddered from it, and a feed that goes silent under a held
    position is the failure every executor treats as an emergency.
    """

    def setUp(self):
        self.store = FakeStore()
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.blocked = set()
        self.log = FeedLog(self.store)
        self.publisher = CascadeFeedPublisher(
            self.log,
            self.signer,
            model_version=MODEL_VERSION,
            symbol_gate=lambda symbol: symbol not in self.blocked,
        )

    def _status(self, campaign):
        return {"campaigns": [campaign.to_dict()], "closed_campaigns": []}

    def _types(self, frames):
        return [verify_frame(frame, self.keys)["type"] for frame in frames]

    def test_an_unchecked_symbol_is_never_announced(self):
        self.blocked.add("SOLUSDT")
        campaign = _loaded_campaign()
        self.assertEqual(self.publisher.publish(self._status(campaign)), [])
        self.assertEqual(self.log.since("SOLUSDT", 0), [])

    def test_a_campaign_announced_before_the_uncheck_finishes_its_life(self):
        """The kindness rule, and the whole reason the gate checks _seen."""
        campaign = _loaded_campaign()
        self.publisher.publish(self._status(campaign))
        self.blocked.add("SOLUSDT")
        campaign.state = "MOTHER_BROKEN"
        campaign.close_reason = "mother_broken"
        campaign.closed_at = "2026-08-03 21:10:00"
        self.publisher.publish({"campaigns": [], "closed_campaigns": [campaign.to_dict()]})
        stream = [verify_frame(f, self.keys)["type"] for f in self.log.since("SOLUSDT", 0)]
        self.assertEqual(stream[-1], "campaign.closed", "buyers in it must hear how it ended")

    def test_rechecking_lets_the_next_campaign_through(self):
        self.blocked.add("SOLUSDT")
        campaign = _loaded_campaign()
        self.assertEqual(self.publisher.publish(self._status(campaign)), [])
        self.blocked.clear()
        types = self._types(self.publisher.publish(self._status(campaign)))
        self.assertIn("campaign.opened", types)

    def test_the_snapshot_honours_the_same_catalogue(self):
        """A cold executor connecting must not see what the stream withholds."""
        campaign = _loaded_campaign().to_dict()
        frames = build_snapshot(
            [campaign],
            self.signer,
            model_version=MODEL_VERSION,
            head_by_symbol={"SOLUSDT": 7},
            symbol_gate=lambda symbol: symbol != "SOLUSDT",
        )
        self.assertEqual(frames, [])

    def test_the_snapshot_still_serves_a_campaign_announced_before_the_uncheck(self):
        campaign = _loaded_campaign().to_dict()
        frames = build_snapshot(
            [campaign],
            self.signer,
            model_version=MODEL_VERSION,
            head_by_symbol={"SOLUSDT": 7},
            symbol_gate=lambda symbol: symbol != "SOLUSDT",
            announced_ids={campaign["campaign_id"]},
        )
        self.assertTrue(frames, "buyers holding it still need its geometry on reconnect")
