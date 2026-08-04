"""The buyer's end of the wire.

The client decides what is TRUE; `executor/model.py` decides what to do about
it. So the tests here are about trust and refusal — what it declines to
follow, what it halts on, and what it keeps doing anyway when the feed goes
quiet — rather than about arithmetic, which lives in test_executor_model.py.

`verify_frame_local` is duplicated from `engine/cascade_feed.py` on purpose:
the executor ships without our engine, and this is the half of the contract
where a subtle difference from the writer would be catastrophic. The first
test drives real server-signed frames through the executor's own reader.
"""

import unittest

from cryptography.exceptions import InvalidSignature

from engine.cascade import MODEL_VERSION
from engine.cascade_feed import (
    ROOT_KID,
    FeedSigner,
    build_envelope,
    build_key_set,
    leg_finalized_payload,
    leg_opened_payload,
    sign_key_set,
    trendline_set_payload,
    verify_key_set,
)
from executor import model
from executor.feed_client import (
    KEYSET_CACHE_TTL_SEC,
    STALE_AFTER_SEC,
    FeedClient,
    active_keys_from_keyset,
    verify_frame_local,
)

NOW = 1785770000.0


def _campaign_payload(**overrides):
    payload = {
        "campaign_id": "casc_SOLUSDT_1",
        "symbol": "SOLUSDT",
        "exchange": "binance",
        "created_at": int(NOW) - 30,
        "mother_high": 178.42,
        "mother_low": 174.10,
        "mother_timestamp": 1785400800,
        "mc_kind": "major",
        "left_mother_range": False,
        "timeframe": "5m",
        "start_timeframe": "5m",
        "escalates": True,
        "state": "TRENDLINE_ACTIVE",
        "parent_campaign_id": None,
        "generation": 1,
        "barren_chain": 0,
        "min_fib_range_pct": 0.0008,
        "median_bar_pct": 0.0011,
        "advisory": {"tick_size": 0.01, "min_notional_usd": 5.0},
    }
    payload.update(overrides)
    return payload


def _leg_payload(**overrides):
    leg = {
        "leg_id": 4,
        "trendline_id": 3,
        "low": 172.88,
        "touch_high": 176.40,
        "touch_timestamp": 1785404100,
        "created_via_break": True,
        "escalated": True,
        "fib": {"high_anchor": 176.40, "low_anchor": 172.88},
        "leg_pct_from_mother": 3.11,
    }
    leg.update(overrides)
    return leg_opened_payload(leg, allocation_anchor=overrides.pop("allocation_anchor", 178.42))


class ClientHarness(unittest.TestCase):
    def setUp(self):
        self.clock = [NOW]
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.events = []
        self.client = FeedClient(
            public_keys=self.keys,
            keyset_fetched_at=NOW,
            now_fn=lambda: self.clock[0],
            on_event=lambda kind, detail: self.events.append((kind, detail)),
        )
        self.seq = 0

    def send(self, msg_type, payload, *, campaign_id="casc_SOLUSDT_1", symbol="SOLUSDT", seq=None, signer=None):
        if seq is None:
            self.seq += 1
            seq = self.seq
        envelope = build_envelope(
            msg_type=msg_type,
            symbol=symbol,
            campaign_id=campaign_id,
            payload=payload,
            seq=seq,
            model_version=MODEL_VERSION,
            emitted_at=int(self.clock[0]),
        )
        return self.client.handle_frame((signer or self.signer).frame(envelope))

    def open_campaign(self, **overrides):
        self.send("campaign.opened", _campaign_payload(**overrides))
        return self.client.campaigns["casc_SOLUSDT_1"]


class WireAgreementTests(ClientHarness):
    """The executor's reader against the server's writer, on real frames."""

    def test_the_duplicated_verifier_reads_what_the_server_signs(self):
        envelope = build_envelope(
            msg_type="leg.finalized",
            symbol="SOLUSDT",
            campaign_id="c1",
            payload=leg_finalized_payload(4),
            seq=1,
            model_version=MODEL_VERSION,
        )
        frame = self.signer.frame(envelope)
        self.assertEqual(verify_frame_local(frame, self.keys), envelope)

    def test_a_tampered_frame_is_refused_by_the_executors_reader_too(self):
        frame = self.signer.frame(
            build_envelope(
                msg_type="leg.finalized",
                symbol="SOLUSDT",
                campaign_id="c1",
                payload=leg_finalized_payload(4),
                seq=1,
                model_version=MODEL_VERSION,
            )
        )
        frame["msg"] = frame["msg"].replace('"seq":1', '"seq":2')
        with self.assertRaises(InvalidSignature):
            verify_frame_local(frame, self.keys)

    def test_an_unverifiable_frame_raises_rather_than_being_skipped(self):
        """Dropping it quietly would leave the ladder missing a rung."""
        stranger = FeedSigner.generate("cf-feed-2026a")
        with self.assertRaises(InvalidSignature):
            self.send("campaign.opened", _campaign_payload(), signer=stranger)
        self.assertEqual(self.events[-1][0], "bad_signature")

    def test_the_key_set_reader_agrees_with_the_servers(self):
        root = FeedSigner.generate(ROOT_KID)
        document = build_key_set(
            [
                {
                    "kid": self.signer.kid,
                    "public": self.signer.public_key_b64(),
                    "not_before": int(NOW),
                    "not_after": int(NOW) + 90 * 86400,
                }
            ],
            issued_at=int(NOW),
        )
        frame = sign_key_set(document, root)
        verified = verify_key_set(frame, root.public_key_b64(), now=NOW + 60)
        self.assertEqual(active_keys_from_keyset(verified, now=NOW + 60), self.keys)

    def test_a_revoked_kid_is_dropped_by_the_executors_reader(self):
        document = build_key_set(
            [
                {
                    "kid": self.signer.kid,
                    "public": self.signer.public_key_b64(),
                    "not_before": int(NOW),
                    "not_after": int(NOW) + 86400,
                }
            ],
            revoked=[self.signer.kid],
            issued_at=int(NOW),
        )
        self.assertEqual(active_keys_from_keyset(document, now=NOW + 60), {})


class JoinTests(ClientHarness):
    def test_a_fresh_campaign_is_joined(self):
        campaign = self.open_campaign()
        self.assertTrue(campaign.joined)
        self.assertEqual(campaign.skip_reason, "")

    def test_a_campaign_that_started_too_long_ago_is_skipped(self):
        """Join-at-start. A ladder only makes sense from its mother."""
        campaign = self.open_campaign(created_at=int(NOW) - 900)
        self.assertFalse(campaign.joined)
        self.assertIn("join window", campaign.skip_reason)

    def test_a_campaign_drawn_under_another_model_version_is_declined(self):
        client = FeedClient(public_keys=self.keys, keyset_fetched_at=NOW, now_fn=lambda: self.clock[0])
        envelope = build_envelope(
            msg_type="campaign.opened",
            symbol="SOLUSDT",
            campaign_id="casc_SOLUSDT_1",
            payload=_campaign_payload(),
            seq=1,
            model_version=MODEL_VERSION + 1,
            emitted_at=int(NOW),
        )
        client.handle_frame(self.signer.frame(envelope))
        campaign = client.campaigns["casc_SOLUSDT_1"]
        self.assertFalse(campaign.joined)
        self.assertIn("model v", campaign.skip_reason)

    def test_a_repeat_announcement_is_a_no_op(self):
        """Their restart re-announces; snapshot and stream overlap anyway."""
        self.open_campaign()
        self.send("leg.opened", _leg_payload())
        self.send("campaign.opened", _campaign_payload())
        self.assertEqual(len(self.client.campaigns["casc_SOLUSDT_1"].legs), 1)


class HaltTests(ClientHarness):
    """Published geometry that contradicts itself stops THAT campaign only."""

    def test_a_drifted_level_checksum_halts_the_campaign(self):
        self.open_campaign()
        payload = _leg_payload()
        payload["derived"]["level_4"] += 0.01
        self.send("leg.opened", payload)
        campaign = self.client.campaigns["casc_SOLUSDT_1"]
        self.assertIn("do not match", campaign.halted)
        self.assertFalse(campaign.active)

    def test_a_mismatched_allocation_halts_the_campaign(self):
        self.open_campaign()
        payload = _leg_payload()
        payload["allocation_pct_gross"] = 9.99
        self.send("leg.opened", payload)
        self.assertIn("allocation", self.client.campaigns["casc_SOLUSDT_1"].halted)

    def test_a_trendline_below_the_one_it_supersedes_halts(self):
        """A line may never sit below the standing one. Asserted, not trusted."""
        self.open_campaign()
        self.send(
            "trendline.set",
            trendline_set_payload(
                {
                    "trendline_id": 3,
                    "anchor1_price": 178.42,
                    "anchor1_timestamp": 1785400800,
                    "anchor2_price": 177.06,
                    "anchor2_timestamp": 1785403500,
                }
            ),
        )
        self.send(
            "trendline.set",
            trendline_set_payload(
                {
                    "trendline_id": 4,
                    "anchor1_price": 178.42,
                    "anchor1_timestamp": 1785400800,
                    "anchor2_price": 176.00,  # below the line it replaces
                    "anchor2_timestamp": 1785406000,
                },
                supersedes=3,
            ),
        )
        self.assertIn("below the line", self.client.campaigns["casc_SOLUSDT_1"].halted)

    def test_a_legitimate_new_trendline_is_accepted(self):
        self.open_campaign()
        for tl_id, anchor2 in ((3, 177.06), (4, 177.50)):
            self.send(
                "trendline.set",
                trendline_set_payload(
                    {
                        "trendline_id": tl_id,
                        "anchor1_price": 178.42,
                        "anchor1_timestamp": 1785400800,
                        "anchor2_price": anchor2,
                        "anchor2_timestamp": 1785403500 + tl_id,
                    },
                    supersedes=3 if tl_id == 4 else None,
                ),
            )
        campaign = self.client.campaigns["casc_SOLUSDT_1"]
        self.assertEqual(campaign.halted, "")
        self.assertEqual(campaign.standing_trendline_id, 4)

    def test_a_halt_does_not_touch_other_campaigns(self):
        self.open_campaign()
        self.send(
            "campaign.opened",
            _campaign_payload(campaign_id="casc_BTCUSDT_1", symbol="BTCUSDT"),
            campaign_id="casc_BTCUSDT_1",
            symbol="BTCUSDT",
        )
        payload = _leg_payload()
        payload["derived"]["level_2"] += 0.5
        self.send("leg.opened", payload)
        self.assertTrue(self.client.campaigns["casc_SOLUSDT_1"].halted)
        self.assertEqual(self.client.campaigns["casc_BTCUSDT_1"].halted, "")


class PostureTests(ClientHarness):
    """Stale trades less. It never stops caring about what is already held."""

    def test_two_missed_heartbeats_make_it_stale(self):
        self.send("heartbeat", {"running_campaigns": 1})
        self.assertFalse(self.client.stale)
        self.clock[0] += STALE_AFTER_SEC + 1
        self.assertTrue(self.client.stale)

    def test_a_stale_feed_opens_nothing_new(self):
        self.send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += STALE_AFTER_SEC + 1
        may_open, reason = self.client.may_open_new
        self.assertFalse(may_open)
        self.assertIn("stale", reason)

    def test_an_expired_key_set_opens_nothing_new(self):
        """A revocation reaches a switched-off machine by the cache lapsing."""
        self.clock[0] += KEYSET_CACHE_TTL_SEC + 1
        may_open, reason = self.client.may_open_new
        self.assertFalse(may_open)
        self.assertIn("key set", reason)

    def test_unconfirmed_entitlement_opens_nothing_new(self):
        """Otherwise pulling the network cable trades forever."""
        self.clock[0] += 24 * 3600 + 1
        self.assertFalse(self.client.may_open_new[0])

    def test_a_heartbeat_head_ahead_of_our_cursor_asks_for_a_resnapshot(self):
        """The gap detector on a silent symbol."""
        self.assertFalse(self.client.needs_resnapshot)
        self.send("heartbeat", {"running_campaigns": 1}, seq=99)
        self.assertTrue(self.client.needs_resnapshot)

    def test_a_seq_gap_asks_for_a_resnapshot_rather_than_guessing(self):
        self.open_campaign()
        self.send("leg.finalized", leg_finalized_payload(4), seq=50)
        self.assertTrue(self.client.needs_resnapshot)

    def test_a_closed_campaign_is_never_flattened_for_them(self):
        """What they hold and what we hold are different positions."""
        self.open_campaign()
        self.send("campaign.closed", {"state": "MOTHER_BROKEN", "reason": "mother_broken", "closed_at": int(NOW)})
        closed = [detail for kind, detail in self.events if kind == "closed"]
        self.assertEqual(closed[-1]["flatten"], False)
        self.assertFalse(self.client.campaigns["casc_SOLUSDT_1"].active)


class PlanTests(ClientHarness):
    """The buyer's own ladder: their netting, their pool, their rungs."""

    def test_it_plans_a_ladder_from_the_buyers_capital(self):
        self.open_campaign()
        self.send("leg.opened", _leg_payload())
        plan = self.client.plan("casc_SOLUSDT_1", capital_usd=5000.0)
        leg = plan["legs"][0]
        gross = (178.42 - 172.88) / 178.42 * 100
        self.assertAlmostEqual(leg["allocation_pct_net"], gross, places=9)
        self.assertAlmostEqual(leg["pool_usd"], model.leg_pool_usd(gross, 5000.0), places=9)
        self.assertEqual([rung["level"] for rung in leg["rungs"]], [2, 4, 8])
        self.assertEqual([rung["entry_style"] for rung in leg["rungs"]], ["stop", "stop", "limit"])

    def test_the_buyers_own_bands_net_the_charge_not_ours(self):
        self.open_campaign()
        self.send("leg.opened", _leg_payload())
        full = self.client.plan("casc_SOLUSDT_1", capital_usd=5000.0)["legs"][0]
        netted = self.client.plan("casc_SOLUSDT_1", capital_usd=5000.0, funded_bands=[(175.0, 178.42)])["legs"][0]
        self.assertLess(netted["allocation_pct_net"], full["allocation_pct_net"])

    def test_capital_under_the_floor_refuses_rather_than_shrinks(self):
        self.open_campaign()
        self.send("leg.opened", _leg_payload())
        plan = self.client.plan("casc_SOLUSDT_1", capital_usd=500.0)
        self.assertIn("minimum", plan["refused"])
        self.assertEqual(plan["legs"], [])

    def test_a_finalized_leg_gets_no_new_rungs(self):
        self.open_campaign()
        self.send("leg.opened", _leg_payload())
        self.send("leg.finalized", leg_finalized_payload(4))
        self.assertEqual(self.client.plan("casc_SOLUSDT_1", capital_usd=5000.0)["legs"], [])

    def test_a_skipped_campaign_is_not_planned(self):
        self.open_campaign(created_at=int(NOW) - 900)
        self.assertIsNone(self.client.plan("casc_SOLUSDT_1", capital_usd=5000.0))

    def test_the_target_is_priced_off_the_buyers_own_venue(self):
        """A CoinDCX buyer pays twice a Binance buyer's commission."""
        self.open_campaign(exchange="coindcx")
        cheap = model.take_profit_price(100.0, 100.10, exchange="binance")
        theirs = self.client.target_price("casc_SOLUSDT_1", 100.0)
        self.assertGreater(theirs, cheap)

    def test_the_target_uses_their_average_entry_not_ours(self):
        self.open_campaign()
        self.assertAlmostEqual(
            self.client.target_price("casc_SOLUSDT_1", 172.00),
            model.take_profit_price(172.00, 178.42, exchange="binance"),
            places=9,
        )


class VenueFeeTests(unittest.TestCase):
    """Pinned against the brokers', for the same reason the model constants are."""

    def test_the_venue_rates_match_the_brokers(self):
        from broker.base import BaseBroker
        from broker.coindcx_spot import CoinDCXSpotClient

        self.assertEqual(model.EXCHANGE_FEE_PCT["binance"], BaseBroker.fee_pct_per_side)
        self.assertEqual(model.EXCHANGE_FEE_PCT["coindcx"], CoinDCXSpotClient.fee_pct_per_side)

    def test_an_unknown_venue_takes_the_higher_rate(self):
        """Guessing low sets a target under the commission. Guessing high
        merely leaves a little on the table."""
        self.assertEqual(model.fee_pct_for("some-new-exchange"), max(model.EXCHANGE_FEE_PCT.values()))

    def test_a_coindcx_target_clears_a_coindcx_commission(self):
        entry = 100.0
        target = model.take_profit_price(entry, 100.10, exchange="coindcx")
        self.assertGreater(target, model.tp_breakeven_price(entry, model.EXCHANGE_FEE_PCT["coindcx"]))

    def test_the_binance_rate_would_have_underpriced_it(self):
        """The bug this exists to prevent, stated as a test."""
        entry = 100.0
        at_binance_rate = model.take_profit_price(entry, 100.10, exchange="binance")
        coindcx_breakeven = model.tp_breakeven_price(entry, model.EXCHANGE_FEE_PCT["coindcx"])
        self.assertLess(at_binance_rate, coindcx_breakeven)


if __name__ == "__main__":
    unittest.main()
