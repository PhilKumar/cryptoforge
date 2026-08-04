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
from executor.exchange import OrderRecord, SymbolRules
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

    def test_the_published_bar_size_reaches_the_stop_allowance(self):
        """A fabricated stand-in would be a different filter on every market."""
        self._open(median_bar_pct=0.002)
        runtime = self._runtime()
        runtime.sync()
        self.assertAlmostEqual(runtime.book.get("casc_SOLUSDT_1").median_bar_pct, 0.002)


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
