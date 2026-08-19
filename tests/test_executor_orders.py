"""What the buyer's machine actually places.

This is the only part of the executor with money on the other side of it, so
it is built as a decider and tested as one: no network, no account, every rule
reachable from a candle and a price.

Two things get the most attention here. The buy-stop walk, because the whole
edge is that the order chases a falling market down and only fills on the turn
back up — a bug there buys the knife. And the twin sleep invariants, because
they run in OPPOSITE directions for the two order types and the intuition is
wrong for one of them in each direction.
"""

import unittest

from engine import cascade as engine
from executor import model
from executor.orders import (
    MAX_STOP_RAISE_BAR_RATIO,
    MAX_STOP_RAISE_FLOOR_TICKS,
    STOP_LIMIT_GAP_BAR_RATIO,
    STOP_LIMIT_GAP_USD,
    STOP_LIMIT_OFFSET_TICKS,
    CampaignOrders,
    Candle,
    Fill,
    OrderBook,
    max_stop_raise_usd,
    stop_limit_gap_usd,
)


def _orders(**overrides) -> CampaignOrders:
    kwargs = {
        "campaign_id": "casc_SOLUSDT_1",
        "symbol": "SOLUSDT",
        "mother_high": 178.42,
        "exchange": "binance",
        "tick_size": 0.01,
        "min_notional_usd": 5.0,
        "median_bar_pct": 0.002,
    }
    kwargs.update(overrides)
    return CampaignOrders(**kwargs)


def _rungs(*specs):
    """(leg_id, level, price, usd) tuples as the planner would hand them over."""
    return [{"leg_id": leg, "level": lvl, "price": price, "usd": usd} for leg, lvl, price, usd in specs]


def _red(ts, close, open_=None):
    return Candle(
        timestamp=ts, open=open_ if open_ is not None else close + 1.0, high=close + 1.0, low=close, close=close
    )


def _green(ts, close):
    return Candle(timestamp=ts, open=close - 1.0, high=close, low=close - 1.0, close=close)


class EntryContractTests(unittest.TestCase):
    """The entry constants are copies. Pinned, like the rest of the model."""

    def test_the_stop_raise_cap_matches_the_engine(self):
        self.assertEqual(MAX_STOP_RAISE_BAR_RATIO, engine.MAX_STOP_RAISE_BAR_RATIO)
        self.assertEqual(MAX_STOP_RAISE_FLOOR_TICKS, engine.MAX_STOP_RAISE_FLOOR_TICKS)

    def test_the_stop_limit_gap_matches_the_engine(self):
        self.assertEqual(STOP_LIMIT_OFFSET_TICKS, engine.STOP_LIMIT_OFFSET_TICKS)
        self.assertEqual(STOP_LIMIT_GAP_USD, engine.STOP_LIMIT_GAP_USD)
        self.assertEqual(STOP_LIMIT_GAP_BAR_RATIO, engine.STOP_LIMIT_GAP_BAR_RATIO)

    def test_the_fill_window_matches_the_engines_function(self):
        """Half a median bar, floored at the symbol's own gap or five ticks —
        the SAME number on both sides, or a buyer's stop expires where the
        desk's fills (BTC 2026-08-19: five ticks lost to a one-millisecond
        sweep from 64,180.00 to 64,180.10)."""
        for sym, tick, price, bar in (
            ("BTCUSDT", 0.01, 64180.0, 0.00024),  # measured bar: half of it, $7.70
            ("BTCUSDT", 0.01, 64180.0, 0.0),  # unmeasured: five ticks
            ("PAXGUSDT", 0.01, 4357.63, 0.00006),  # $2 floor beats a quiet bar
            ("SOLUSDT", 0.01, 76.9, 0.00106),  # 4c bar beats the 2c floor
        ):
            self.assertAlmostEqual(
                stop_limit_gap_usd(sym, tick, price, bar), engine.stop_limit_gap_usd(sym, tick, price, bar), places=9
            )
        self.assertAlmostEqual(stop_limit_gap_usd("BTCUSDT", 0.01, 64180.0, 0.00024), 7.7016, places=4)
        self.assertAlmostEqual(stop_limit_gap_usd("BTCUSDT", 0.01, 64180.0, 0.0), 0.05, places=9)
        self.assertAlmostEqual(stop_limit_gap_usd("PAXGUSDT", 0.01, 4357.63, 0.00006), 2.00, places=9)
        self.assertGreater(stop_limit_gap_usd("SOLUSDT", 0.01, 76.9, 0.00106), 0.02)

    def test_the_allowance_matches_the_engines_function(self):
        for trigger, bar, tick in ((72.90, 0.0019, 0.01), (66000.0, 0.0011, 0.01), (4000.0, 0.0, 0.01)):
            self.assertAlmostEqual(
                max_stop_raise_usd(trigger, bar, tick),
                engine.max_stop_raise_usd(trigger, bar, tick),
                places=9,
            )

    def test_an_unmeasured_bar_gives_the_strict_allowance_not_a_wide_one(self):
        """A failed measurement must never widen a real filter."""
        self.assertAlmostEqual(max_stop_raise_usd(100.0, 0.0, 0.01), MAX_STOP_RAISE_FLOOR_TICKS * 0.01, places=9)

    def test_the_executor_does_not_import_the_engine(self):
        from executor import orders

        self.assertNotIn("from engine", open(orders.__file__, encoding="utf-8").read())


class CollectionTests(unittest.TestCase):
    """A level is a marker, not an order. Touching it puts money in play."""

    def test_a_crossed_level_adds_its_money_to_the_pot(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 169.0, 170), _rungs((4, 2, 169.36, 3.0)))
        self.assertAlmostEqual(orders.pot_usd, 3.0)
        self.assertIsNone(orders.pot_line)  # $3 does not clear $5.50 yet

    def test_the_level_that_tips_the_pot_becomes_the_line(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 160.0, 162), _rungs((4, 2, 169.36, 3.0), (4, 4, 162.32, 4.5)))
        self.assertAlmostEqual(orders.pot_usd, 7.5)
        self.assertEqual(orders.pot_line, 162.32)

    def test_levels_are_collected_shallowest_first(self):
        """The total has to build in the order price actually met them."""
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 148.0, 150), _rungs((4, 8, 148.24, 6.0), (4, 2, 169.36, 6.0)))
        # The shallow one tipped it first, so IT is the line.
        self.assertEqual(orders.pot_line, 169.36)

    def test_a_level_is_never_collected_twice(self):
        orders = _orders()
        rungs = _rungs((4, 2, 169.36, 6.0))
        orders.collect(Candle(1, 175, 175, 169.0, 170), rungs)
        orders.collect(Candle(2, 170, 170, 168.0, 169), rungs)
        self.assertAlmostEqual(orders.pot_usd, 6.0)

    def test_nothing_collects_at_or_above_the_previous_rounds_exit(self):
        """The new-low rule. Re-entering at or above the last exit is the one
        thing this strategy must never do."""
        orders = _orders()
        orders.reuse_below = 170.0
        self.assertEqual(orders.collect(Candle(1, 175, 175, 171.0, 172), _rungs((4, 2, 172.0, 6.0))), [])
        self.assertAlmostEqual(orders.pot_usd, 0.0)

    def test_it_collects_again_once_price_prints_below_that_floor(self):
        orders = _orders()
        orders.reuse_below = 170.0
        orders.collect(Candle(2, 172, 172, 169.0, 169.5), _rungs((4, 2, 169.36, 6.0)))
        self.assertAlmostEqual(orders.pot_usd, 6.0)


class StopWalkTests(unittest.TestCase):
    """The edge: the order chases price down and only fills on the turn up."""

    def setUp(self):
        self.orders = _orders()
        self.orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        self.assertEqual(self.orders.pot_line, 162.32)

    def test_two_reds_below_the_line_set_the_stop(self):
        self.assertIsNotNone(self.orders.advance_stop(_red(2, 162.00)))
        self.assertIsNone(self.orders.stop_price)  # first red only breaks the line
        self.orders.advance_stop(_red(3, 161.50))
        self.assertAlmostEqual(self.orders.stop_price, 162.00)  # the PREVIOUS red close

    def test_the_trigger_sits_above_the_market_which_is_what_makes_it_a_stop(self):
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 161.50))
        self.assertGreater(self.orders.stop_price, 161.50)

    def test_the_limit_caps_half_a_bar_over_the_trigger(self):
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 161.50))
        # The fixture's bar is 0.2%: half of it at 162.00 is 16.2c, which beats
        # SOLUSDT's own 2c floor. The window is a race against the sweep that
        # triggers it, so it is sized in bars, not ticks.
        self.assertAlmostEqual(self.orders.limit_price, 162.00 + 162.00 * 0.002 * 0.5)

    def test_a_symbol_without_its_own_gap_uses_half_a_bar_floored_at_five_ticks(self):
        orders = _orders(symbol="BTCUSDT", tick_size=0.01)
        orders.collect(Candle(1, 70000, 70000, 66000.0, 66100), _rungs((4, 4, 66052.63, 6.0)))
        orders.advance_stop(_red(2, 66050.00))
        orders.advance_stop(_red(3, 66040.00))
        self.assertAlmostEqual(orders.limit_price, 66050.00 + 66050.00 * 0.002 * 0.5)
        # Unmeasured bar: the five-tick floor is the whole window.
        quiet = _orders(symbol="BTCUSDT", tick_size=0.01, median_bar_pct=0.0)
        quiet.collect(Candle(1, 70000, 70000, 66000.0, 66100), _rungs((4, 4, 66052.63, 6.0)))
        quiet.advance_stop(_red(2, 66050.00))
        quiet.advance_stop(_red(3, 66040.00))
        self.assertAlmostEqual(quiet.limit_price, 66050.00 + 5 * 0.01)

    def test_green_candles_are_ignored(self):
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_green(3, 161.00))
        self.assertIsNone(self.orders.stop_price)

    def test_a_red_that_closes_higher_does_not_count(self):
        """Price must keep falling for the walk to continue."""
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 162.10))
        self.assertIsNone(self.orders.stop_price)

    def test_the_stop_walks_down_with_each_new_low(self):
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 161.50))
        first = self.orders.stop_price
        self.orders.advance_stop(_red(4, 161.00))
        self.assertLess(self.orders.stop_price, first)

    def test_walking_the_stop_invalidates_the_resting_order(self):
        """A resting order at the old trigger is now the wrong order."""
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 161.50))
        self.orders.entry_resting = True
        rev_before = self.orders.entry_rev
        self.orders.advance_stop(_red(4, 161.00))
        self.assertFalse(self.orders.entry_resting)
        self.assertGreater(self.orders.entry_rev, rev_before)

    def test_the_same_candle_twice_does_nothing(self):
        self.orders.advance_stop(_red(2, 162.00))
        self.orders.advance_stop(_red(3, 161.50))
        stop = self.orders.stop_price
        self.orders.advance_stop(_red(3, 161.50))
        self.assertEqual(self.orders.stop_price, stop)


class HeldEntryTests(unittest.TestCase):
    """A fall that already bottomed must not be bought at today's price."""

    def _armed(self, **overrides):
        orders = _orders(**overrides)
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        return orders

    def test_a_market_just_over_the_trigger_arms(self):
        orders = self._armed()
        allowed, _ = orders.entry_allowed(162.05)
        self.assertTrue(allowed)

    def test_a_market_far_above_the_trigger_is_held(self):
        orders = self._armed()
        allowed, reason = orders.entry_allowed(170.0)
        self.assertFalse(allowed)
        self.assertIn("no new low", reason)

    def test_holding_places_nothing(self):
        orders = self._armed()
        self.assertEqual(orders.intents(market_price=170.0), [])
        self.assertIn("Held", orders.held_reason)

    def test_the_allowance_is_measured_in_this_instruments_own_bars(self):
        """A flat percent is a 65x different filter across markets.

        Same trigger (162.00), same market (162.50), opposite answers: a jumpy
        instrument's quarter-bar is 0.81 and swallows the 0.50 raise, while a
        calm one's is only its 3-tick floor of 0.03 and refuses it.
        """
        wide = self._armed(median_bar_pct=0.02)
        tight = self._armed(median_bar_pct=0.0002)
        self.assertTrue(wide.entry_allowed(162.50)[0])
        self.assertFalse(tight.entry_allowed(162.50)[0])


class ExitTests(unittest.TestCase):
    def setUp(self):
        self.orders = _orders()
        self.orders.on_entry_filled(Fill(price=172.00, quantity=0.03, timestamp=10))

    def test_a_fill_places_a_target_off_their_own_average(self):
        intents = self.orders.intents(market_price=172.0)
        exits = [intent for intent in intents if intent.kind == "exit"]
        self.assertEqual(len(exits), 1)
        self.assertAlmostEqual(exits[0].price, model.take_profit_price(172.00, 178.42, exchange="binance"), places=6)
        self.assertAlmostEqual(exits[0].quantity, 0.03)

    def test_a_second_fill_moves_the_target_and_replaces_the_order(self):
        self.orders.intents(market_price=172.0)
        self.orders.on_entry_filled(Fill(price=168.00, quantity=0.03, timestamp=20))
        intents = self.orders.intents(market_price=168.0)
        self.assertEqual([intent.action for intent in intents], ["cancel", "place"])
        self.assertAlmostEqual(
            intents[-1].price,
            model.take_profit_price(self.orders.avg_entry, 178.42, exchange="binance"),
            places=6,
        )

    def test_an_unmoved_target_is_not_churned(self):
        self.orders.intents(market_price=172.0)
        self.assertEqual(self.orders.intents(market_price=172.5), [])

    def test_a_coindcx_target_is_priced_off_a_coindcx_commission(self):
        orders = _orders(exchange="coindcx", mother_high=100.10)
        orders.on_entry_filled(Fill(price=100.0, quantity=1.0, timestamp=10))
        target = [i for i in orders.intents(market_price=100.0) if i.kind == "exit"][0].price
        self.assertGreater(target, model.tp_breakeven_price(100.0, model.EXCHANGE_FEE_PCT["coindcx"]))

    def test_a_fill_clears_the_pot_so_it_cannot_be_bought_twice(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.on_entry_filled(Fill(price=162.0, quantity=0.03, timestamp=10))
        self.assertAlmostEqual(orders.pot_usd, 0.0)
        self.assertIsNone(orders.stop_price)

    def test_closing_a_round_sets_their_own_floor_not_ours(self):
        self.orders.intents(market_price=172.0)
        self.orders.on_exit_filled(exit_price=175.10)
        self.assertEqual(self.orders.reuse_below, 175.10)
        self.assertEqual(self.orders.base_qty, 0)
        self.assertEqual(self.orders.collected_levels, set())


class SleepInvariantTests(unittest.TestCase):
    """The two rules run in opposite directions. Both are worth stating."""

    def test_a_resting_entry_is_cancelled_before_sleep(self):
        """A fill with nothing watching creates a position with no target."""
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        orders.entry_resting = True
        intents = orders.sleep_intents()
        self.assertEqual([(i.action, i.kind) for i in intents], [("cancel", "entry")])

    def test_coin_held_without_an_exit_gets_one_placed_before_sleep(self):
        """The one genuinely bad configuration, and entirely preventable."""
        orders = _orders()
        orders.on_entry_filled(Fill(price=172.0, quantity=0.03, timestamp=10))
        intents = orders.sleep_intents()
        self.assertEqual([(i.action, i.kind) for i in intents], [("place", "exit")])

    def test_a_resting_exit_is_left_alone(self):
        """Cancelling it turns a 3am rally into a missed outcome."""
        orders = _orders()
        orders.on_entry_filled(Fill(price=172.0, quantity=0.03, timestamp=10))
        orders.intents(market_price=172.0)  # places the exit
        self.assertEqual(orders.sleep_intents(), [])

    def test_both_invariants_at_once(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        orders.entry_resting = True
        orders.fills.append(Fill(price=172.0, quantity=0.03, timestamp=10))
        self.assertEqual([(i.action, i.kind) for i in orders.sleep_intents()], [("cancel", "entry"), ("place", "exit")])


class ExposureTests(unittest.TestCase):
    """ "If this machine stops now, at most $X can fill unwatched." """

    def test_nothing_resting_is_no_exposure(self):
        self.assertEqual(_orders().armed_exposure_usd(), 0.0)

    def test_a_resting_entry_is_exactly_its_own_notional(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 7.25)))
        orders.entry_resting = True
        self.assertEqual(orders.armed_exposure_usd(), 7.25)

    def test_a_collected_but_unarmed_pot_is_not_exposure(self):
        """A stopped machine places no new orders — only resting ones can fill."""
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 7.25)))
        self.assertEqual(orders.armed_exposure_usd(), 0.0)

    def test_the_book_totals_across_campaigns(self):
        book = OrderBook()
        for index, usd in enumerate((6.0, 9.0)):
            orders = _orders(campaign_id=f"c{index}")
            orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, usd)))
            orders.entry_resting = True
            book.track(orders)
        self.assertEqual(book.armed_exposure_usd(), 15.0)

    def test_the_book_names_anything_holding_coin_without_an_exit(self):
        book = OrderBook()
        safe = book.track(_orders(campaign_id="safe"))
        safe.on_entry_filled(Fill(price=172.0, quantity=0.03, timestamp=1))
        safe.intents(market_price=172.0)
        naked = book.track(_orders(campaign_id="naked"))
        naked.on_entry_filled(Fill(price=172.0, quantity=0.03, timestamp=1))
        self.assertEqual(book.unprotected(), ["naked"])


class IdempotencyTests(unittest.TestCase):
    """A crash between deciding and placing is the ordinary case."""

    def test_the_same_decision_derives_the_same_order_id(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        first = orders.intents(market_price=162.05)[0].client_order_id
        orders.entry_resting = False
        self.assertEqual(orders.intents(market_price=162.05)[0].client_order_id, first)

    def test_walking_the_stop_gives_the_new_order_a_new_id(self):
        """It is a different order at a different trigger, not a retry."""
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        first = orders.intents(market_price=162.05)[0].client_order_id
        orders.entry_resting = True
        orders.advance_stop(_red(4, 161.00))
        self.assertNotEqual(orders.intents(market_price=161.55)[0].client_order_id, first)


class SerialisationTests(unittest.TestCase):
    """to_dict/from_dict must be lossless for everything a restart cannot ask
    someone else for."""

    def test_a_full_campaign_round_trips(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        orders.entry_resting = True
        orders.held_reason = "held for a reason"
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=99))
        orders.exit_intents()
        orders.on_exit_filled(165.0, ts=120)
        orders.collect(Candle(5, 170, 170, 160.0, 161), _rungs((5, 2, 160.5, 7.0)))

        revived = CampaignOrders.from_dict(orders.to_dict())
        self.assertEqual(revived.to_dict(), orders.to_dict())
        self.assertEqual(revived.collected_levels, orders.collected_levels)
        self.assertEqual(revived.reuse_below, orders.reuse_below)
        self.assertEqual(len(revived.closed_rounds), 1)

    def test_an_unset_price_comes_back_unset_not_zero(self):
        """Every price here means "not set" by absence. Coercing a missing one
        to 0.0 would read as a real level at zero — and `pot_line = 0.0` arms a
        stop that should not exist."""
        revived = CampaignOrders.from_dict(_orders().to_dict())
        self.assertIsNone(revived.pot_line)
        self.assertIsNone(revived.stop_price)
        self.assertIsNone(revived.reuse_below)

    def test_leg_ids_of_mixed_types_do_not_lose_the_pot(self):
        """Leg ids arrive as JSON from the feed. Sorting a mixed batch on the
        id itself raises, and losing the pot over a tidy-ordering detail is not
        a trade worth making."""
        orders = _orders()
        orders.collected_levels = {(4, 2), ("x", 8)}
        orders.pot_usd = 9.0
        self.assertEqual(CampaignOrders.from_dict(orders.to_dict()).collected_levels, orders.collected_levels)

    def test_an_untouched_campaign_is_not_worth_keeping(self):
        self.assertFalse(_orders().worth_keeping())

    def test_a_campaign_with_a_pot_is_worth_keeping(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        self.assertTrue(orders.worth_keeping())

    def test_a_flat_campaign_that_closed_a_round_is_still_worth_keeping(self):
        """Its floor and its round history are the whole record of what this
        buyer did here, and neither is on the exchange."""
        orders = _orders()
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=99))
        orders.on_exit_filled(165.0, ts=120)
        self.assertEqual(orders.pot_usd, 0.0)
        self.assertEqual(orders.base_qty, 0.0)
        self.assertTrue(orders.worth_keeping())


class AbandonEntryTests(unittest.TestCase):
    """Ending the buying, not the holding."""

    def test_nothing_to_cancel_when_no_buy_is_resting(self):
        self.assertEqual(_orders().abandon_entry_intents("over"), [])

    def test_the_cancel_names_the_resting_order(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.advance_stop(_red(2, 162.00))
        orders.advance_stop(_red(3, 161.50))
        orders.entry_resting = True
        intent = orders.abandon_entry_intents("over")[0]
        self.assertEqual(intent.action, "cancel")
        self.assertEqual(intent.kind, "entry")
        self.assertEqual(intent.client_order_id, orders.entry_client_order_id())

    def test_the_position_and_the_journal_are_untouched(self):
        orders = _orders()
        orders.collect(Candle(1, 175, 175, 162.0, 163), _rungs((4, 4, 162.32, 6.0)))
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=99))
        orders.on_exit_filled(165.0, ts=120)
        orders.on_entry_filled(Fill(price=161.0, quantity=0.04, timestamp=130))
        orders.pot_usd, orders.entry_resting = 6.0, True

        orders.abandon_entry()
        self.assertEqual(orders.base_qty, 0.04)
        self.assertEqual(len(orders.closed_rounds), 1)
        self.assertEqual(orders.reuse_below, 165.0)
        self.assertEqual(orders.pot_usd, 0.0)
        self.assertFalse(orders.entry_resting)


if __name__ == "__main__":
    unittest.main()
