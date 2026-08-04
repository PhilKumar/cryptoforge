"""The buyer's local model — the half of the product that handles their money.

Two jobs here.

The first is the drift guard. `executor/model.py` ships to buyers' machines and
deliberately does not import `engine.cascade`, so its constants are copies. A
copy that quietly diverges is the worst bug this system can have: both sides
would keep working, agree about nothing, and place orders at levels the other
never drew. `ModelContractTests` pins every one of them to the engine's value,
and it fails HERE, in our repo, which is the only place it can be caught before
it reaches somebody's account.

The second is that the derivations are right — netting, pool, rungs, target,
and the capital gate — checked against the engine's own functions wherever the
engine has one to check against.
"""

import unittest

from engine import cascade as engine
from executor import model


class ModelContractTests(unittest.TestCase):
    """Every constant the executor copies, pinned to the engine's value."""

    def test_the_model_version_matches(self):
        """If this fails, either bump the executor or you have a real drift.

        model_version is what an executor checks before opening anything. Ours
        claiming 21 while the engine draws 22 means it would trade geometry it
        may interpret differently, which is the exact thing the version exists
        to prevent.
        """
        self.assertEqual(model.MODEL_VERSION, engine.MODEL_VERSION)

    def test_the_ladder_shape_matches(self):
        self.assertEqual(model.CASCADE_LEVELS, engine.CASCADE_LEVELS)
        self.assertEqual(model.LEVEL_ALLOCATION, engine.LEVEL_ALLOCATION)
        self.assertEqual(model.STOP_ENTRY_LEVELS, engine.STOP_ENTRY_LEVELS)

    def test_the_target_rules_match(self):
        self.assertEqual(model.TP_FIB_LEVEL, engine.TP_FIB_LEVEL)
        self.assertEqual(model.TP_MUST_CLEAR_FEES, engine.TP_MUST_CLEAR_FEES)
        self.assertEqual(model.TP_MIN_NET_PCT, engine.TP_MIN_NET_PCT)

    def test_the_sizing_rules_match(self):
        self.assertEqual(model.FEE_PCT_PER_SIDE, engine.FEE_PCT_PER_SIDE)
        self.assertEqual(model.RUNG_BUFFER_PCT, engine.RUNG_BUFFER_PCT)

    def test_the_executor_does_not_import_the_engine(self):
        """It ships to buyers' machines. The geometry engine does not."""
        source = open(model.__file__, encoding="utf-8").read()
        self.assertNotIn("engine.cascade", source.replace("`engine.cascade`", ""))
        self.assertNotIn("from engine", source)


class BandLedgerTests(unittest.TestCase):
    """The executor keeps its own. Its siblings are not our siblings."""

    def test_it_matches_the_engines_ledger_maths(self):
        span = (100.0, 130.0)
        taken = [(105.0, 110.0), (120.0, 125.0)]
        self.assertEqual(model.subtract_bands(span, taken), engine.subtract_bands(span, taken))
        self.assertAlmostEqual(model.free_span_of(span, taken), engine.free_span_of(span, taken), places=9)

    def test_ground_taken_in_the_middle_frees_both_sides(self):
        """The whole reason this is a ledger and not a floor."""
        self.assertEqual(model.subtract_bands((100.0, 130.0), [(110.0, 120.0)]), [(100.0, 110.0), (120.0, 130.0)])

    def test_touching_bands_merge(self):
        self.assertEqual(model.merge_bands([(90.0, 95.0), (95.0, 100.0)]), [(90.0, 100.0)])

    def test_a_buyer_with_no_siblings_pays_the_full_gross(self):
        self.assertAlmostEqual(
            model.net_allocation_pct(
                3.105, allocation_anchor=178.42, leg_low=172.88, mother_high=178.42, funded_bands=[]
            ),
            3.105,
            places=9,
        )

    def test_a_sibling_that_funded_half_the_stretch_halves_the_charge(self):
        netted = model.net_allocation_pct(
            4.0, allocation_anchor=180.0, leg_low=170.0, mother_high=180.0, funded_bands=[(175.0, 180.0)]
        )
        self.assertAlmostEqual(netted, 2.0, places=9)

    def test_netting_matches_the_engine_on_a_real_leg(self):
        """Same inputs through the engine's own funding path."""
        campaign = engine.Campaign(
            campaign_id="c1",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=0,
        )
        campaign.funded_bands = [(174.0, 176.5)]
        leg = engine.Leg(leg_id=1, trendline_id=1, low=172.88, touch_high=177.90, touch_timestamp=1)
        campaign.legs.append(leg)
        engine.build_fib_ladder_and_pool(campaign, leg)

        gross = (178.42 - 172.88) / 178.42 * 100.0
        ours = model.net_allocation_pct(
            gross,
            allocation_anchor=178.42,
            leg_low=172.88,
            mother_high=178.42,
            funded_bands=campaign.funded_bands,
        )
        self.assertAlmostEqual(ours, leg.allocation_pct, places=9)


class SizingTests(unittest.TestCase):
    def test_capital_enters_at_exactly_one_multiplication(self):
        self.assertAlmostEqual(model.leg_pool_usd(3.105, 2000.0), 62.10, places=9)

    def test_the_rungs_split_20_30_50(self):
        self.assertEqual(model.rung_split(100.0), {2: 20.0, 4: 30.0, 8: 50.0})

    def test_level_prices_match_the_engines_ladder(self):
        ladder = engine.FibLadder(high_anchor=176.40, low_anchor=172.88)
        for level in model.CASCADE_LEVELS:
            self.assertAlmostEqual(model.level_price(176.40, 172.88, level), ladder.level_price(level), places=9)

    def test_shallow_levels_are_stops_and_the_deep_one_rests(self):
        self.assertEqual(model.entry_style(2), "stop")
        self.assertEqual(model.entry_style(4), "stop")
        self.assertEqual(model.entry_style(8), "limit")

    def test_a_rung_carries_its_cushion(self):
        self.assertAlmostEqual(model.min_rung_usd(5.0), 5.50, places=9)


class TargetTests(unittest.TestCase):
    def test_the_target_matches_the_engines(self):
        campaign = engine.Campaign(
            campaign_id="c1",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=0,
        )
        campaign.avg_entry_price = 172.00
        self.assertAlmostEqual(model.take_profit_price(172.00, 178.42), engine.compute_tp_price(campaign), places=9)

    def test_the_fee_floor_is_dormant_on_a_real_fall(self):
        """Measured falls run 2.8-4.6%. The floor is not meant to move a target."""
        geometric = 172.00 + model.TP_FIB_LEVEL * (178.42 - 172.00)
        self.assertAlmostEqual(model.take_profit_price(172.00, 178.42), geometric, places=9)

    def test_a_shallow_round_is_floored_above_its_own_commission(self):
        entry, mother = 100.0, 100.10  # a 0.1% fall: geometry alone loses to fees
        target = model.take_profit_price(entry, mother)
        self.assertGreater(target, model.tp_breakeven_price(entry))


class FidelityTests(unittest.TestCase):
    """What the buyer is told about a ladder their capital cannot lay in full."""

    def test_a_pool_that_affords_every_rung_is_full(self):
        # smallest rung is 20% of pool, and must clear $5.50
        self.assertEqual(model.fidelity(pool_usd=30.0, min_notional_usd=5.0), "full")

    def test_a_pool_that_cannot_afford_its_shallow_rung_is_coarse(self):
        self.assertEqual(model.fidelity(pool_usd=20.0, min_notional_usd=5.0), "coarse")

    def test_a_pool_that_cannot_place_anything_at_all(self):
        self.assertEqual(model.fidelity(pool_usd=4.0, min_notional_usd=5.0), "none")

    def test_fidelity_is_per_leg_not_per_campaign(self):
        """A deep leg is faithful at capital where a shallow one is not.

        Pool scales with the leg's own depth, so a single 'this much capital is
        enough' figure would be wrong in both directions.
        """
        capital = 2000.0
        deep = model.leg_pool_usd(3.0, capital)  # a 3% leg
        shallow = model.leg_pool_usd(0.5, capital)  # a 0.5% leg
        self.assertEqual(model.fidelity(deep, 5.0), "full")
        self.assertEqual(model.fidelity(shallow, 5.0), "coarse")


class CapitalGateTests(unittest.TestCase):
    def test_under_the_floor_it_refuses_to_open(self):
        may_open, tier, warning = model.capital_gate(800)
        self.assertFalse(may_open)
        self.assertEqual(tier, "below_floor")
        self.assertIn("1,000", warning)

    def test_between_the_floor_and_full_fidelity_it_opens_and_warns(self):
        may_open, tier, warning = model.capital_gate(1500)
        self.assertTrue(may_open)
        self.assertEqual(tier, "coarsened")
        self.assertIn("fewer, deeper entries", warning)

    def test_above_full_fidelity_it_opens_quietly(self):
        may_open, tier, warning = model.capital_gate(5000)
        self.assertTrue(may_open)
        self.assertEqual(tier, "full")
        self.assertIsNone(warning)


class ChecksumTests(unittest.TestCase):
    """`derived` and the gross percent are checksums, not instructions."""

    def test_a_matching_checksum_passes(self):
        derived = {f"level_{lvl}": model.level_price(176.40, 172.88, lvl) for lvl in model.CASCADE_LEVELS}
        self.assertTrue(model.verify_derived_levels(derived, 176.40, 172.88))

    def test_a_drifted_level_is_caught(self):
        derived = {f"level_{lvl}": model.level_price(176.40, 172.88, lvl) for lvl in model.CASCADE_LEVELS}
        derived["level_4"] += 0.01
        self.assertFalse(model.verify_derived_levels(derived, 176.40, 172.88))

    def test_a_missing_level_is_caught_rather_than_skipped(self):
        self.assertFalse(model.verify_derived_levels({"level_2": 169.36}, 176.40, 172.88))

    def test_the_gross_percent_is_recomputed_not_trusted(self):
        self.assertTrue(model.verify_allocation((178.42 - 172.88) / 178.42 * 100, 178.42, 172.88))
        self.assertFalse(model.verify_allocation(3.5, 178.42, 172.88))


if __name__ == "__main__":
    unittest.main()
