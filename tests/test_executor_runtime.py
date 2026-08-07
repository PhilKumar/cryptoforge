"""The loop, end to end.

Everything below is an integration property — something no single module can
state on its own, which is exactly why the wiring deserves its own tests rather
than being assumed correct because its parts are.

The two that matter most:

**Staleness reduces trading; it never stops caring.** A quiet feed, an expired
key set and a lapsed subscription each stop NEW structure and leave exit
management running, because a buyer whose subscription expired still has coin
on an exchange and it still needs a target.

**A halted campaign is quarantined, not abandoned.** Geometry that contradicts
itself stops that campaign opening anything more, while its position keeps its
exit and its siblings carry on.
"""

import unittest

from engine.cascade import MODEL_VERSION
from engine.cascade_feed import (
    FeedSigner,
    build_envelope,
    campaign_opened_payload,
    leg_opened_payload,
)
from executor.exchange import ExchangeError, OrderRecord, SymbolRules
from executor.feed_client import FeedClient
from executor.orders import Candle, Fill
from executor.runtime import ExecutorRuntime, RuntimeConfig

NOW = 1785770000.0


class FakeExchange:
    def __init__(self, balances=None):
        self.rules = SymbolRules(tick_size=0.01, step_size=0.001, min_notional_usd=5.0, base_asset="SOL")
        self.balances = dict(balances or {"USDT": 5000.0, "SOL": 1.0})
        self.orders = {}
        self.placed = []

    def symbol_rules(self, symbol):
        return self.rules

    def free_balance(self, asset):
        return float(self.balances.get(asset, 0.0))

    def place(self, *, symbol, side, order_type, quantity, price, stop_price, client_order_id):
        record = OrderRecord(
            client_order_id=client_order_id,
            exchange_order_id=f"X{len(self.placed) + 1}",
            status="NEW",
            side=side,
            price=price,
            stop_price=stop_price,
            quantity=quantity,
        )
        self.placed.append(record)
        self.orders[client_order_id] = record
        return record

    def cancel(self, *, symbol, client_order_id):
        record = self.orders.pop(client_order_id, None)
        if record:
            record.status = "CANCELLED"
        return record

    def get_order(self, *, symbol, client_order_id):
        return self.orders.get(client_order_id)

    def open_orders(self, symbol):
        return [r for r in self.orders.values() if r.is_open]

    @property
    def sides(self):
        return [record.side for record in self.placed]


class FakeMarket:
    def __init__(self, price=163.0, candles=None):
        self.price = price
        self.candles = list(candles or [])

    def last_price(self, symbol):
        return self.price

    def closed_candles_since(self, symbol, timeframe, since_ts):
        return [candle for candle in self.candles if candle.timestamp > since_ts]


class RuntimeHarness(unittest.TestCase):
    def setUp(self):
        self.clock = [NOW]
        self.signer = FeedSigner.generate("cf-feed-2026a")
        self.keys = {self.signer.kid: self.signer.public_key_b64()}
        self.client = FeedClient(public_keys=self.keys, keyset_fetched_at=NOW, now_fn=lambda: self.clock[0])
        self.exchange = FakeExchange()
        self.market = FakeMarket()
        self.seq = 0

    def _runtime(self, capital=5000.0, **kwargs):
        return ExecutorRuntime(
            client=self.client,
            adapter=self.exchange,
            market=self.market,
            config=RuntimeConfig(capital_usd=capital, **kwargs),
            now_fn=lambda: self.clock[0],
        )

    def _send(self, msg_type, payload, campaign_id="casc_SOLUSDT_1", symbol="SOLUSDT"):
        self.seq += 1
        self.client.handle_frame(
            self.signer.frame(
                build_envelope(
                    msg_type=msg_type,
                    symbol=symbol,
                    campaign_id=campaign_id,
                    payload=payload,
                    seq=self.seq,
                    model_version=MODEL_VERSION,
                    emitted_at=int(self.clock[0]),
                )
            )
        )

    def _open(self, campaign_id="casc_SOLUSDT_1", symbol="SOLUSDT", **overrides):
        payload = {
            "campaign_id": campaign_id,
            "symbol": symbol,
            "exchange": "binance",
            "created_at": int(self.clock[0]) - 30,
            "mother_high": 178.42,
            "mother_low": 174.10,
            "mother_timestamp": 1785400800,
            "state": "TRENDLINE_ACTIVE",
            "timeframe": "5m",
            "tick_size": 0.01,
            "min_notional_usd": 5.0,
            "median_bar_pct": 0.002,
        }
        payload.update(overrides)
        self._send("campaign.opened", campaign_opened_payload(payload), campaign_id=campaign_id, symbol=symbol)

    def _leg(self, campaign_id="casc_SOLUSDT_1", symbol="SOLUSDT", leg_id=4, low=172.88, touch_high=176.40, **kw):
        leg = {
            "leg_id": leg_id,
            "trendline_id": 3,
            "low": low,
            "touch_high": touch_high,
            "touch_timestamp": 1785404100,
            "created_via_break": True,
            "escalated": True,
            "fib": {"high_anchor": touch_high, "low_anchor": low},
            "leg_pct_from_mother": 3.11,
        }
        leg.update(kw)
        self._send(
            "leg.opened",
            leg_opened_payload(leg, allocation_anchor=178.42),
            campaign_id=campaign_id,
            symbol=symbol,
        )

    @staticmethod
    def _red(ts, close):
        return Candle(timestamp=ts, open=close + 1.0, high=close + 1.0, low=close, close=close)


class SyncTests(RuntimeHarness):
    def test_a_joined_campaign_gets_a_book(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        self.assertIn("casc_SOLUSDT_1", runtime.book.campaigns)

    def test_a_skipped_campaign_never_gets_one(self):
        """Join-at-start already declined it; the runtime must not undo that."""
        self._open(created_at=int(NOW) - 900)
        runtime = self._runtime()
        runtime.sync()
        self.assertEqual(runtime.book.campaigns, {})

    def test_capital_under_the_floor_opens_nothing_at_all(self):
        self._open()
        runtime = self._runtime(capital=500.0)
        notes = runtime.sync()
        self.assertEqual(runtime.book.campaigns, {})
        self.assertIn("minimum", notes[0])

    def test_the_buyer_can_follow_a_subset_of_symbols(self):
        self._open(campaign_id="sol", symbol="SOLUSDT")
        self._open(campaign_id="btc", symbol="BTCUSDT")
        runtime = self._runtime(symbols=["SOLUSDT"])
        runtime.sync()
        self.assertEqual(list(runtime.book.campaigns), ["sol"])

    def test_a_symbol_this_venue_does_not_list_is_declined_at_sync(self):
        """The geometry may be drawn on an exchange carrying coins ours does
        not. Finding that out at order time leaves a campaign that looks
        followed and can never place."""

        def unlisted(symbol):
            raise ExchangeError(f"{symbol} is not listed on CoinDCX")

        self.exchange.symbol_rules = unlisted
        self._open()
        runtime = self._runtime(exchange="coindcx")
        notes = runtime.sync()
        self.assertEqual(runtime.book.campaigns, {})
        self.assertIn("not tradeable on coindcx", notes[0])

    def test_the_fee_venue_is_where_the_buyer_pays_not_where_it_was_drawn(self):
        """A CoinDCX buyer following a Binance-drawn campaign pays CoinDCX's
        commission, so their target's fee floor must be priced there."""
        self._open(exchange="binance")
        runtime = self._runtime(exchange="coindcx")
        runtime.sync()
        self.assertEqual(runtime.book.campaigns["casc_SOLUSDT_1"].exchange, "coindcx")

    def test_the_venues_own_filters_beat_the_feeds_advisory(self):
        self.exchange.rules = SymbolRules(tick_size=0.5, step_size=0.001, min_notional_usd=17.0, base_asset="SOL")
        self._open(tick_size=0.01, min_notional_usd=5.0)
        runtime = self._runtime()
        runtime.sync()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        self.assertEqual(orders.tick_size, 0.5)
        self.assertEqual(orders.min_notional_usd, 17.0)

    def test_the_published_bar_size_reaches_the_stop_allowance(self):
        """A fabricated stand-in would be a different filter on every market."""
        self._open(median_bar_pct=0.002)
        runtime = self._runtime()
        runtime.sync()
        self.assertAlmostEqual(runtime.book.get("casc_SOLUSDT_1").median_bar_pct, 0.002)


class CandleFailureTests(RuntimeHarness):
    """One venue refusing one symbol's candles must not stop the pass.

    Found live: a CoinDCX machine following 5m geometry got 422 from
    /market_data/candles every tick, and the raise escaped the campaign loop.
    """

    def _market_that_fails(self, bad_symbol):
        market = self.market

        class Selective:
            def last_price(self, symbol):
                return market.last_price(symbol)

            def closed_candles_since(self, symbol, timeframe, since_ts):
                if symbol == bad_symbol:
                    raise RuntimeError(f"422 Unprocessable Entity for {symbol} {timeframe}")
                return market.closed_candles_since(symbol, timeframe, since_ts)

        return Selective()

    def test_a_candle_failure_does_not_abandon_the_other_campaigns(self):
        self._open(campaign_id="sol", symbol="SOLUSDT")
        self._open(campaign_id="btc", symbol="BTCUSDT")
        self.market.candles = [self._red(1785400900, 160.0)]
        runtime = self._runtime()
        runtime._market = self._market_that_fails("SOLUSDT")
        runtime.sync()

        report = runtime.tick()  # must not raise
        self.assertTrue(any("could not read SOLUSDT" in note for note in report.notes))
        # SOLUSDT is tracked first, so before the fix the loop never reached
        # BTCUSDT at all. Its price being read is the proof that it did.
        self.assertIn("BTCUSDT", runtime.last_prices)

    def test_the_failing_campaign_still_gets_its_exit_placed(self):
        """The whole point. It holds coin; the candles are only needed to open
        more, never to protect what is already there."""
        self._open(campaign_id="sol", symbol="SOLUSDT")
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("sol", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime._market = self._market_that_fails("SOLUSDT")

        report = runtime.tick()
        self.assertTrue(any("exit is still managed" in note for note in report.notes))
        sells = [call for call in self.exchange.placed if call.side == "sell"]
        self.assertEqual(len(sells), 1)

    def test_no_entry_is_placed_from_candles_we_could_not_read(self):
        """Without candles we cannot know which rungs were crossed, and
        guessing is how money lands at the wrong price."""
        self._open(campaign_id="sol", symbol="SOLUSDT")
        self._leg(campaign_id="sol", symbol="SOLUSDT")
        self.market.candles = [self._red(1785400900, 160.0)]
        runtime = self._runtime()
        runtime._market = self._market_that_fails("SOLUSDT")
        runtime.sync()

        runtime.tick()
        buys = [call for call in self.exchange.placed if call.side == "buy"]
        self.assertEqual(buys, [])


class SettingsTests(RuntimeHarness):
    """The buyer changing their own mind, from their own console."""

    def test_narrowing_the_subscription_takes_effect_on_the_next_campaign(self):
        runtime = self._runtime()
        runtime.set_subscription(timeframes=["15m"], source_exchanges=[], symbols=[])
        self._open(campaign_id="fast", timeframe="5m", start_timeframe="5m")
        runtime.sync()
        self.assertEqual(runtime.book.campaigns, {})
        self.assertIn("15m", self.client.campaigns["fast"].skip_reason)

    def test_narrowing_never_abandons_a_campaign_already_running(self):
        """A position does not stop needing its exit because the buyer
        narrowed what they want to hear about next."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        self.assertIn("casc_SOLUSDT_1", runtime.book.campaigns)
        runtime.set_subscription(timeframes=["1h"], source_exchanges=["coindcx"], symbols=["BTCUSDT"])
        self.assertIn("casc_SOLUSDT_1", runtime.book.campaigns)

    def test_the_subscription_line_follows_the_change(self):
        runtime = self._runtime()
        message = runtime.set_subscription(timeframes=["15m"], source_exchanges=["coindcx"], symbols=["solusdt"])
        self.assertIn("15m · drawn on coindcx · SOLUSDT", message)

    def test_a_flat_machine_may_change_venue(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        self.assertEqual(runtime.venue_change_blockers(), [])

    def test_holding_coin_blocks_a_venue_change(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        self.assertEqual(runtime.venue_change_blockers(), ["casc_SOLUSDT_1"])

    def test_a_resting_order_blocks_a_venue_change_too(self):
        """Nothing held, but an order on the old exchange that nothing would
        ever come back to cancel."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.book.get("casc_SOLUSDT_1").entry_resting = True
        self.assertEqual(runtime.venue_change_blockers(), ["casc_SOLUSDT_1"])


class TickTests(RuntimeHarness):
    def test_a_fall_through_a_level_arms_and_places_a_buy(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        # Level 4 of this fib sits at 176.40 - 4*(176.40-172.88) = 162.32.
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        report = runtime.tick()
        self.assertEqual(report.placed, 1)
        self.assertEqual(self.exchange.sides, ["buy"])

    def test_a_far_bounced_market_holds_rather_than_buying(self):
        """The late-start case: arming here buys over value on no new low."""
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 172.0  # bounced miles above the trigger
        report = runtime.tick()
        self.assertEqual(report.placed, 0)

    def test_a_fill_gets_a_target_placed_against_it(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.tick()
        self.assertIn("sell", self.exchange.sides)


class PostureTests(RuntimeHarness):
    """Staleness reduces trading. It never stops caring."""

    def _holding(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        return runtime

    def test_a_stale_feed_still_places_the_exit(self):
        runtime = self._holding()
        self._send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += 91  # two missed beats
        self.assertFalse(self.client.may_open_new[0])
        runtime.tick()
        self.assertIn("sell", self.exchange.sides)
        self.assertNotIn("buy", self.exchange.sides)

    def test_a_stale_feed_opens_no_new_entries(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        self._send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += 91
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        report = runtime.tick()
        self.assertEqual(report.placed, 0)
        self.assertIn("stale", report.notes[0])

    def test_a_blocked_posture_is_logged_once_not_once_per_tick(self):
        """The activity log records the change, not the weather.

        This wrote one line per campaign per tick, so three campaigns on a
        twenty-second tick repeated the same sentence nine times a minute and
        buried the fills underneath it.
        """
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        self._send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += 91  # feed goes stale

        first = runtime.tick()
        stale_lines = [n for n in first.notes if "stale" in n]
        self.assertEqual(len(stale_lines), 1, first.notes)

        for _ in range(3):
            again = runtime.tick()
            self.assertEqual([n for n in again.notes if "stale" in n], [], again.notes)

    def test_opening_resuming_is_logged_once(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        self._send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += 91
        runtime.tick()  # goes stale, logs once
        runtime.tick()  # silent

        self._send("heartbeat", {"running_campaigns": 1})  # feed is live again
        back = runtime.tick()
        self.assertEqual(len([n for n in back.notes if "resumed" in n]), 1, back.notes)
        quiet = runtime.tick()
        self.assertEqual([n for n in quiet.notes if "resumed" in n], [], quiet.notes)

    def test_an_expired_key_set_still_places_the_exit(self):
        """A revocation must never strand somebody's coin without a target."""
        runtime = self._holding()
        self.clock[0] += 25 * 3600
        self.assertFalse(self.client.may_open_new[0])
        runtime.tick()
        self.assertIn("sell", self.exchange.sides)

    def test_a_halted_campaign_keeps_its_exit_and_spares_its_siblings(self):
        self._open(campaign_id="bad", symbol="SOLUSDT")
        self._open(campaign_id="good", symbol="BTCUSDT")
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("bad", Fill(price=162.0, quantity=0.05, timestamp=40))
        self.client.campaigns["bad"].halted = "levels do not match"

        runtime.tick()
        self.assertIn("sell", self.exchange.sides)
        self.assertEqual(runtime.status()["halted"], ["bad"])
        self.assertNotIn("good", runtime.status()["halted"])


class NettingTests(RuntimeHarness):
    """Their siblings, not ours. That is why the feed publishes gross."""

    def test_a_second_campaign_on_the_same_symbol_is_netted_at_birth(self):
        self._open(campaign_id="first")
        self._leg(campaign_id="first")
        runtime = self._runtime()
        runtime.sync()

        self._open(campaign_id="second")
        self._leg(campaign_id="second")
        runtime.sync()
        self.assertTrue(runtime._birth_bands["second"])
        self.assertEqual(runtime._birth_bands["first"], [])

    def test_a_first_campaign_pays_the_full_gross(self):
        self._open(campaign_id="only")
        self._leg(campaign_id="only")
        runtime = self._runtime()
        runtime.sync()
        plan = self.client.plan("only", capital_usd=5000.0, funded_bands=runtime._birth_bands["only"])
        gross = (178.42 - 172.88) / 178.42 * 100
        self.assertAlmostEqual(plan["legs"][0]["allocation_pct_net"], gross, places=9)


class SleepAndWakeTests(RuntimeHarness):
    def test_sleep_cancels_the_entry_and_reports_a_clean_stop(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        runtime.tick()
        self.assertEqual(self.exchange.sides, ["buy"])

        result = runtime.prepare_for_sleep()
        self.assertFalse(result["record"]["slept_armed"])
        self.assertIn("Nothing can be bought", result["message"])

    def test_sleep_places_a_missing_exit_on_a_held_position(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.prepare_for_sleep()
        self.assertIn("sell", self.exchange.sides)

    def test_waking_protects_a_position_that_lost_its_exit(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        report = runtime.on_wake({"shutdown_at": NOW - 3600, "slept_armed": False})
        self.assertEqual(report["protected"], ["casc_SOLUSDT_1"])
        self.assertIn("sell", self.exchange.sides)

    def test_a_missing_record_is_treated_as_a_crash_not_a_clean_stop(self):
        """No record means there was no chance to cancel anything."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        report = runtime.on_wake(None)
        self.assertTrue(report["requires_confirmation"])

    def test_a_first_ever_run_is_not_a_crash(self):
        """A fresh install has no record because there has never been a
        shutdown. Reading that as a crash told a brand-new buyer they had been
        "away for 24.0 hours" and that entries were being held back — the first
        false, the second describing a gate that is not actually applied."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        report = runtime.on_wake(None, first_run=True)
        self.assertEqual(report["band"], "first_run")
        self.assertFalse(report["requires_confirmation"])
        self.assertEqual(report["steps"], [])
        self.assertNotIn("Away for", report["message"])
        self.assertIn("First run", report["message"])

    def test_first_run_never_overrides_a_real_shutdown_record(self):
        """The flag answers "has this machine started before", not "is this
        safe" — a record present means a real gap, and it still governs."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        report = runtime.on_wake({"shutdown_at": NOW - 8 * 3600, "slept_armed": False}, first_run=True)
        self.assertNotEqual(report["band"], "first_run")
        self.assertTrue(report["requires_confirmation"])

    def test_a_crash_after_the_first_run_is_still_a_crash(self):
        """The marker exists from run two onward, so a missing record then is
        the real thing and must stay loud."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        report = runtime.on_wake(None, first_run=False)
        self.assertTrue(report["requires_confirmation"])
        self.assertIn("no new entries go out", report["message"])
        # An unmeasured gap must not be reported as a measured one. The 24h
        # stand-in exists to pick the cautious band, not to be read aloud.
        self.assertNotIn("24.0h", report["message"])
        self.assertIn("unknown", report["message"])
        self.assertEqual(report["message"].count("Away for"), 0)

    def test_a_measured_gap_is_still_stated_once(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        report = runtime.on_wake({"shutdown_at": NOW - 8 * 3600, "slept_armed": False})
        self.assertEqual(report["message"].count("Away for"), 1)
        self.assertIn("8.0h", report["message"])

    def test_a_dead_price_on_wake_does_not_strand_the_other_positions(self):
        """Wake is when the most is at stake: one symbol's price failing must
        not stop the others catching their targets up, or produce no report."""
        self._open(campaign_id="sol", symbol="SOLUSDT")
        self._open(campaign_id="btc", symbol="BTCUSDT")
        runtime = self._runtime()
        runtime.sync()
        for campaign_id in ("sol", "btc"):
            runtime.on_fill(campaign_id, Fill(price=162.0, quantity=0.05, timestamp=40))
            runtime.book.get(campaign_id).exit_price = 170.0

        class DeadPrice:
            def last_price(self, symbol):
                raise RuntimeError("venue down")

            def closed_candles_since(self, symbol, timeframe, since_ts):
                return []

        runtime._market = DeadPrice()
        report = runtime.on_wake({"shutdown_at": NOW - 3600, "slept_armed": False})
        self.assertIn("message", report)
        self.assertEqual(sorted(report["protected"]), ["btc", "sol"])

    def test_a_long_gap_protects_the_position_but_asks_before_trading(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        report = runtime.on_wake({"shutdown_at": NOW - 8 * 3600, "slept_armed": False})
        self.assertTrue(report["requires_confirmation"])
        self.assertEqual(report["protected"], ["casc_SOLUSDT_1"])
        self.assertNotIn("replace_entries", report["steps"])

    def test_price_past_target_on_wake_is_taken_at_market(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.tick()  # places the resting exit and records its price
        self.market.price = 500.0  # ran miles past target while away
        report = runtime.on_wake({"shutdown_at": NOW - 3600, "slept_armed": False})
        self.assertEqual(report["tp_caught_up"], ["casc_SOLUSDT_1"])


class StatusTests(RuntimeHarness):
    def test_it_reports_the_armed_exposure(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        runtime.tick()
        self.assertGreater(runtime.status()["armed_exposure_usd"], 0)

    def test_it_names_a_position_with_no_exit_against_it(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        self.assertEqual(runtime.status()["unprotected"], ["casc_SOLUSDT_1"])

    def test_it_explains_why_it_is_not_opening_anything(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        self._send("heartbeat", {"running_campaigns": 1})
        self.clock[0] += 91
        status = runtime.status()
        self.assertFalse(status["opening_new"])
        self.assertIn("stale", status["posture_reason"])


if __name__ == "__main__":
    unittest.main()


class BuyerGateTests(RuntimeHarness):
    """The buyer's own switches gate the TICK, not just the status readout."""

    def _falling_market(self, runtime):
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        return runtime

    def test_pause_stops_the_tick_placing_entries(self):
        self._open()
        self._leg()
        runtime = self._falling_market(self._runtime())
        runtime.pause_opening()
        report = runtime.tick()
        self.assertEqual(report.placed, 0)
        self.assertTrue(any("Paused by you" in note for note in report.notes))

    def test_resume_lets_the_same_tick_place_again(self):
        self._open()
        self._leg()
        runtime = self._falling_market(self._runtime())
        runtime.pause_opening()
        runtime.tick()
        runtime.resume_opening()
        report = runtime.tick()
        self.assertEqual(report.placed, 1)

    def test_an_unconfirmed_wake_stops_the_tick_placing_entries(self):
        self._open()
        self._leg()
        runtime = self._falling_market(self._runtime())
        runtime.on_wake({"shutdown_at": NOW - 8 * 3600, "slept_armed": False})
        report = runtime.tick()
        self.assertEqual(report.placed, 0)
        runtime.confirm_wake()
        self.assertEqual(runtime.tick().placed, 1)

    def test_pause_still_places_the_exit_on_a_held_position(self):
        """Pause is "stop opening", never "stop caring"."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.pause_opening()
        runtime.tick()
        self.assertIn("sell", self.exchange.sides)


class BookPersistenceTests(RuntimeHarness):
    """The pot must survive a restart.

    Nothing else can rebuild it. The feed publishes geometry, not what this
    buyer collected against it; the exchange knows the coin but not which rungs
    paid for it, nor where the floor from their last round sits. Losing the file
    means a fall that was half paid for has to be earned again from wherever
    price happens to be — and the levels already crossed are not coming back.
    """

    def _collected(self):
        """A runtime whose one campaign has money in the pot."""
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [Candle(10, 175, 175, 162.0, 163.0)]
        runtime.tick()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        self.assertGreater(orders.pot_usd, 0, "the fixture must actually collect something")
        return runtime, orders

    def test_the_pot_survives_a_restart(self):
        runtime, before = self._collected()
        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        after = revived.book.campaigns["casc_SOLUSDT_1"]
        self.assertEqual(after.pot_usd, before.pot_usd)
        self.assertEqual(after.collected_levels, before.collected_levels)

    def test_a_restored_rung_is_not_collected_a_second_time(self):
        """The pot coming back is only half of it. If the crossed levels did
        not come back with it, the same fall would be paid for twice."""
        runtime, before = self._collected()
        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        revived.tick()  # the same candles are still in the market
        self.assertEqual(revived.book.campaigns["casc_SOLUSDT_1"].pot_usd, before.pot_usd)

    def test_a_position_and_its_round_history_survive(self):
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=161.0, quantity=0.05, timestamp=50), side="sell")

        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        orders = revived.book.campaigns["casc_SOLUSDT_1"]
        self.assertEqual(len(orders.closed_rounds), 1)
        self.assertEqual(orders.reuse_below, 161.0)
        self.assertEqual(len(revived.rounds_view()), 1)

    def test_the_floor_survives_so_the_next_round_cannot_re_enter_above_it(self):
        """`reuse_below` is the new-low rule. Losing it lets the next round
        arm at or above the price the last one exited at, which is the one
        thing this strategy must never do."""
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        runtime.book.campaigns["casc_SOLUSDT_1"].reuse_below = 160.0
        runtime.book.campaigns["casc_SOLUSDT_1"].pot_usd = 0.01  # so it is worth writing down

        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        self.market.candles = [Candle(10, 175, 175, 162.0, 163.0)]
        revived.tick()
        self.assertEqual(
            revived.book.campaigns["casc_SOLUSDT_1"].pot_usd,
            0.01,
            "a low of 162 is above the 160 floor and must collect nothing",
        )

    def test_a_held_position_is_protected_on_wake(self):
        """The point of restoring BEFORE on_wake. Against an empty book the
        protect pass has nothing to iterate, so coin held through a crash came
        back with no target on it."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.book.campaigns["casc_SOLUSDT_1"].exit_resting = False

        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        report = revived.on_wake(None)
        self.assertEqual(report["protected"], ["casc_SOLUSDT_1"])
        self.assertIn("sell", self.exchange.sides)

    def test_the_birth_bands_survive_so_the_ladder_keeps_its_shape(self):
        """Fixed at birth and not recomputable later: by the next start the
        siblings they were measured against have moved on. Rebuilding them
        would re-net different bands and quietly resize every rung."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.book.campaigns["casc_SOLUSDT_1"].pot_usd = 3.0
        runtime._birth_bands["casc_SOLUSDT_1"] = [(150.0, 178.0)]

        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        self.assertEqual(revived._birth_bands["casc_SOLUSDT_1"], [(150.0, 178.0)])

    def test_sync_does_not_overwrite_a_restored_campaign(self):
        """It rebuilds what it does not recognise. A restored campaign it does
        recognise must be left alone, pot and all."""
        runtime, before = self._collected()
        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        revived.sync()
        self.assertEqual(revived.book.campaigns["casc_SOLUSDT_1"].pot_usd, before.pot_usd)

    def test_an_untouched_campaign_is_not_written_down(self):
        """It is fully described by the feed, so keeping it grows the file for
        nothing."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        self.assertEqual(runtime.book_snapshot()["campaigns"], [])

    def test_a_book_from_another_version_is_ignored_rather_than_half_read(self):
        runtime, _ = self._collected()
        saved = runtime.book_snapshot()
        saved["v"] = 999
        revived = self._runtime()
        notes = revived.restore_book(saved)
        self.assertEqual(revived.book.campaigns, {})
        self.assertIn("different version", notes[0])

    def test_one_unreadable_campaign_does_not_cost_the_others_theirs(self):
        self._open(campaign_id="a", symbol="SOLUSDT")
        self._open(campaign_id="b", symbol="SOLUSDT")
        runtime = self._runtime()
        runtime.sync()
        runtime.book.campaigns["a"].pot_usd = 2.0
        runtime.book.campaigns["b"].pot_usd = 3.0
        saved = runtime.book_snapshot()
        del saved["campaigns"][0]["campaign_id"]

        revived = self._runtime()
        revived.restore_book(saved)
        self.assertEqual(list(revived.book.campaigns), ["b"])

    def test_a_resting_order_comes_back_resting_so_its_fill_is_noticed(self):
        """Restoring the flag as False would silently drop a fill that landed
        while the process was down, and re-place over a live order."""
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        runtime.tick()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        self.assertTrue(orders.entry_resting)

        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        restored = revived.book.campaigns["casc_SOLUSDT_1"]
        self.assertTrue(restored.entry_resting)
        self.assertEqual(restored.entry_client_order_id(), orders.entry_client_order_id())

        self.exchange.orders[orders.entry_client_order_id()].status = "FILLED"
        self.exchange.orders[orders.entry_client_order_id()].filled_qty = 0.05
        self.exchange.orders[orders.entry_client_order_id()].avg_fill_price = 162.0
        noticed = revived.poll_fills()
        self.assertTrue(any("entry filled" in note for note in noticed))
        self.assertGreater(revived.book.campaigns["casc_SOLUSDT_1"].base_qty, 0)


class UnpublishedCampaignTests(RuntimeHarness):
    """A campaign the feed has stopped publishing still holds the buyer's coin.

    The tick used to skip it whole — `if not followed: continue` — which took
    its exit with it. The geometry ending is not the position ending: the coin
    was bought with their money and stays theirs, so it keeps its target. Only
    entries need the feed, because only the ladder cannot be drawn without it.

    Reachable because the book is restored from disk: a campaign that ended on
    the server while this machine was down comes back in the book and is not in
    the snapshot.
    """

    def _held_but_unpublished(self):
        """A book campaign holding coin, with no feed entry at all."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        runtime.book.campaigns["casc_SOLUSDT_1"].exit_resting = False
        runtime._client.campaigns.pop("casc_SOLUSDT_1")
        return runtime

    def test_its_exit_is_still_placed(self):
        runtime = self._held_but_unpublished()
        runtime.tick()
        self.assertIn("sell", self.exchange.sides)

    def test_it_opens_nothing_new(self):
        """Entries need geometry, and there is none to read."""
        self._open()
        self._leg()
        runtime = self._runtime()
        runtime.sync()
        runtime._client.campaigns.pop("casc_SOLUSDT_1")
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        report = runtime.tick()
        self.assertEqual(report.placed, 0)
        self.assertEqual(runtime.book.campaigns["casc_SOLUSDT_1"].pot_usd, 0.0)

    def test_a_later_sibling_is_not_abandoned_with_it(self):
        """The `continue` skipped one campaign; a raise would have skipped the
        rest of the pass. Neither may cost a sibling its exit."""
        self._open(campaign_id="gone", symbol="SOLUSDT")
        self._open(campaign_id="live", symbol="SOLUSDT")
        runtime = self._runtime()
        runtime.sync()
        for campaign_id in ("gone", "live"):
            runtime.on_fill(campaign_id, Fill(price=162.0, quantity=0.05, timestamp=40))
            runtime.book.campaigns[campaign_id].exit_resting = False
        runtime._client.campaigns.pop("gone")
        runtime.tick()
        self.assertEqual(self.exchange.sides.count("sell"), 2)

    def test_the_buyer_is_told_once_not_every_tick(self):
        runtime = self._held_but_unpublished()
        first = [n for n in runtime.tick().notes if "no longer published" in n]
        self.assertEqual(len(first), 1)
        self.assertIn("keeps its target", first[0])
        for _ in range(3):
            self.assertEqual([n for n in runtime.tick().notes if "no longer published" in n], [])

    def test_a_campaign_holding_nothing_is_not_announced_at_all(self):
        """Nothing is at stake, so the line would be noise."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        runtime._client.campaigns.pop("casc_SOLUSDT_1")
        self.assertEqual([n for n in runtime.tick().notes if "no longer published" in n], [])

    def test_it_does_not_inflate_the_posture_line(self):
        """`opened_blocked` counts campaigns the FEED is holding back. One
        blocked for its own reason would overstate what the posture costs."""
        self._open()
        runtime = self._runtime()
        runtime.sync()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        orders.pot_usd = 5.0
        runtime._client.campaigns.pop("casc_SOLUSDT_1")
        runtime.pause_opening()
        self.assertEqual(runtime.tick().opened_blocked, [])

    def test_the_console_says_ended_rather_than_a_question_mark(self):
        from executor.ui import campaigns_view

        runtime = self._held_but_unpublished()
        runtime.tick()
        row = next(r for r in campaigns_view(runtime) if r["campaign_id"] == "casc_SOLUSDT_1")
        self.assertEqual(row["state"], "ENDED")
        self.assertGreater(row["position_qty"], 0)
        self.assertIsNotNone(row["target"])


class AbandonedEntryTests(RuntimeHarness):
    """A buy resting on a campaign the feed has stopped publishing is waiting
    to spend money on geometry that no longer exists. It comes off.

    The opposite of the sleep cancel, which means "not now" and re-places on
    wake. This means "not ever", so the pot goes with the order.
    """

    def _resting_then_unpublished(self):
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        runtime.tick()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        self.assertTrue(orders.entry_resting, "the fixture must leave a buy resting")
        self.assertGreater(orders.pot_usd, 0)
        self.market.candles = []
        runtime._client.campaigns.pop("casc_SOLUSDT_1")
        return runtime, orders

    def test_the_resting_buy_is_cancelled(self):
        runtime, orders = self._resting_then_unpublished()
        entry_id = orders.entry_client_order_id()
        report = runtime.tick()
        self.assertEqual(report.cancelled, 1)
        self.assertNotIn(entry_id, self.exchange.orders)
        self.assertFalse(orders.entry_resting)

    def test_the_pot_and_the_armed_stop_go_with_it(self):
        """Capital pending on a campaign that cannot spend it, and a trigger
        that can never fire, are both fiction on the buyer's page."""
        runtime, orders = self._resting_then_unpublished()
        runtime.tick()
        self.assertEqual(orders.pot_usd, 0.0)
        self.assertIsNone(orders.stop_price)
        self.assertIsNone(orders.pot_line)
        self.assertEqual(runtime.status()["armed_exposure_usd"], 0.0)

    def test_the_buyer_is_told_what_was_released(self):
        runtime, orders = self._resting_then_unpublished()
        spent = orders.pot_usd
        notes = [n for n in runtime.tick().notes if "cancelled the resting buy" in n]
        self.assertEqual(len(notes), 1)
        self.assertIn(f"${spent:,.2f}", notes[0])

    def test_it_is_not_retried_every_tick(self):
        runtime, _ = self._resting_then_unpublished()
        runtime.tick()
        for _ in range(3):
            report = runtime.tick()
            self.assertEqual(report.cancelled, 0)
            self.assertEqual([n for n in report.notes if "cancelled the resting buy" in n], [])

    def test_a_held_position_keeps_its_target_through_the_cancel(self):
        """This ends the buying, not the holding."""
        runtime, orders = self._resting_then_unpublished()
        runtime.on_fill("casc_SOLUSDT_1", Fill(price=162.0, quantity=0.05, timestamp=40))
        orders.entry_resting = True  # a second rung was still resting when it ended
        orders.pot_usd = 6.0
        runtime.tick()
        self.assertEqual(orders.base_qty, 0.05)
        self.assertTrue(orders.exit_resting)
        self.assertIn("sell", self.exchange.sides)

    def test_a_published_campaign_keeps_its_resting_buy(self):
        """The guard is the feed entry, not the posture. A paused or stale
        campaign is coming back; an ended one is not."""
        self._open()
        self._leg()
        runtime = self._runtime()
        self.market.candles = [
            Candle(10, 175, 175, 162.0, 163),
            self._red(20, 162.0),
            self._red(30, 161.5),
        ]
        self.market.price = 161.55
        runtime.tick()
        orders = runtime.book.campaigns["casc_SOLUSDT_1"]
        runtime.pause_opening()
        report = runtime.tick()
        self.assertEqual(report.cancelled, 0)
        self.assertTrue(orders.entry_resting)
        self.assertGreater(orders.pot_usd, 0)

    def test_the_cleared_entry_survives_a_restart(self):
        """The abandonment must be written down, or the next start restores a
        pot and a trigger for a campaign that is over."""
        runtime, _ = self._resting_then_unpublished()
        runtime.tick()
        revived = self._runtime()
        revived.restore_book(runtime.book_snapshot())
        restored = revived.book.campaigns.get("casc_SOLUSDT_1")
        if restored is not None:  # kept only if something else is still at stake
            self.assertEqual(restored.pot_usd, 0.0)
            self.assertIsNone(restored.stop_price)
            self.assertFalse(restored.entry_resting)
