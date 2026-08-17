"""Warm-up ladders never spend the purse.

Phil, 2026-08-17: "Nothing works in this strategy for the past few days...
No trade taken even after market moves many stages down and up." BTC had
walked through nine armed orders since 12 Aug. Every one was refused by the
budget gate: seven WARM-UP ladders -- fills the replay made on the 30 days of
history BEFORE the paper clock, which the console itself says "never count"
-- held $96.81 of the $100 cap, and the next $5.50 buy would have breached it.
Nobody bought those ladders; the money was never spent, so it cannot be tied
up. The sim now takes the paper clock and counts only fills at or after it.
"""

import unittest
import unittest.mock

import pandas as pd

import tools.rule3070_sim as sim
from engine import rule3070_paper


def _fill(when: str, usd: float = 5.5) -> sim.Fill:
    return sim.Fill(pd.Timestamp(when, tz="UTC"), 63_000.0, usd, "30% b1")


class WarmupBudgetTests(unittest.TestCase):
    def tearDown(self):
        sim.BUDGET_FROM_TS = None

    def test_without_a_clock_every_fill_spends(self):
        """The backtest and the CLI have no paper clock; nothing changes for them."""
        sim.BUDGET_FROM_TS = None
        self.assertTrue(sim._spent(_fill("2026-07-23 09:20")))

    def test_a_fill_before_the_clock_is_phantom_and_one_after_it_is_real(self):
        sim.BUDGET_FROM_TS = pd.Timestamp("2026-08-11 15:33:54", tz="UTC")
        self.assertFalse(sim._spent(_fill("2026-07-23 09:20")), "warm-up: never bought")
        self.assertFalse(sim._spent(_fill("2026-08-11 15:33:53")), "one second before the clock")
        self.assertTrue(sim._spent(_fill("2026-08-11 15:33:54")), "on the clock: paper money")
        self.assertTrue(sim._spent(_fill("2026-08-14 15:45")))

    def test_harvest_hands_the_sim_the_paper_clock_and_takes_it_back(self):
        """The service is the only place a clock exists; it must set it for the
        replay and clear it after, so a CLI run in the same process is untouched."""
        seen = {}

        def fake_run_ladder(df, minors=False):
            seen["clock"] = sim.BUDGET_FROM_TS
            return []

        df = pd.DataFrame(
            {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2026-08-17", tz="UTC")], name="datetime"),
        )
        with unittest.mock.patch.object(sim, "run_ladder", fake_run_ladder):
            rule3070_paper.harvest(df, 1786462434, set())
        self.assertEqual(seen["clock"], pd.Timestamp(1786462434, unit="s", tz="UTC"))
        self.assertIsNone(sim.BUDGET_FROM_TS, "cleared after the replay")


if __name__ == "__main__":
    unittest.main()
