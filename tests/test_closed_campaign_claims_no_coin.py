"""A closed campaign's books must not lay claim to today's balance.

Phil, 2026-08-31, live: "Why this current live buy happened and why some other
entry said market sell and entry not in exchange... I closed an existing
campaign and started a new one."

The reconciliation sweep asks "is my coin still on the exchange?" by taking the
account balance and subtracting what OTHER campaigns claim. That subtraction
counted every campaign the engine had ever loaded, closed ones included.
`filled_base_qty` is never zeroed on close, so the claims pile up forever: by
that morning sixteen BTCUSDT campaigns closed between 26 July and 31 August
still claimed 0.00040274 between them, against a real balance of 0.00025.
Every live campaign therefore netted to zero and the sweep announced that
present, locked, perfectly safe coin was "gone ... settle this one by hand".

Stopping one campaign and starting another is what moved the numbers enough to
re-arm the once-per-change notice, which is why it looked like the new
campaign caused it.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cascade import FINAL_STATES  # noqa: E402


class FakeCampaign:
    def __init__(self, cid, qty, *, mode="live", state="TRENDLINE_ACTIVE", closed_at="", residual=0.0):
        self.campaign_id = cid
        self.symbol = "BTCUSDT"
        self.filled_base_qty = qty
        self.residual_base_qty = residual
        self.mode = mode
        self.state = state
        self.closed_at = closed_at


def claimed_by_others(campaigns, me):
    """The production expression, kept in step with engine/cascade.py."""
    return sum(
        float(c.filled_base_qty or 0.0) + float(c.residual_base_qty or 0.0)
        for c in campaigns
        if c.campaign_id != me.campaign_id
        and c.symbol == me.symbol
        and c.filled_base_qty > 0
        and str(getattr(c, "mode", "") or "") == "live"
        and not c.closed_at
        and str(c.state or "") not in FINAL_STATES
    )


# The real books that morning, trimmed to the ones that mattered.
TODAY = [
    FakeCampaign("c3", 0.00014426, state="STOPPED", closed_at="2026-07-26 22:56:49"),
    FakeCampaign("c10", 0.00008909, state="STOPPED", closed_at="2026-07-27 18:19:17"),
    FakeCampaign("c424", 0.00000775, state="MOTHER_BROKEN", closed_at="2026-08-24 20:05:04"),
    FakeCampaign("c492", 0.00005116, state="STOPPED", closed_at="2026-08-31 11:26:47"),
    FakeCampaign("c493", 0.00025106, state="TRENDLINE_ACTIVE", closed_at=""),
]
EXCHANGE_HOLDS = 0.00025


class ClosedCampaignsClaimNothingTests(unittest.TestCase):
    def test_the_open_campaign_sees_its_own_coin(self):
        me = next(c for c in TODAY if c.campaign_id == "c493")
        mine = max(EXCHANGE_HOLDS - claimed_by_others(TODAY, me), 0.0)
        self.assertAlmostEqual(mine, EXCHANGE_HOLDS, places=8)
        # This is what the sweep tests: present coin must read as present.
        self.assertGreaterEqual(mine, me.filled_base_qty * 0.99)

    def test_a_campaign_stopped_this_morning_does_not_eat_the_balance(self):
        """#492 was stopped at 11:26 and its books never zeroed; the new
        campaign's coin must not disappear because of it."""
        me = next(c for c in TODAY if c.campaign_id == "c493")
        self.assertEqual(claimed_by_others(TODAY, me), 0.0)

    def test_the_old_expression_is_what_produced_the_false_alarm(self):
        """Kept as the counter-example: without the filters, #493's own coin
        reads as entirely someone else's."""
        me = next(c for c in TODAY if c.campaign_id == "c493")
        unfiltered = sum(
            float(c.filled_base_qty or 0.0) + float(c.residual_base_qty or 0.0)
            for c in TODAY
            if c.campaign_id != me.campaign_id and c.symbol == me.symbol and c.filled_base_qty > 0
        )
        self.assertAlmostEqual(unfiltered, 0.00029226, places=8)
        self.assertEqual(max(EXCHANGE_HOLDS - unfiltered, 0.0), 0.0)

    def test_two_live_open_campaigns_still_net_off_each_other(self):
        """The netting must keep working — this is not a licence to ignore
        siblings, only the dead ones."""
        a = FakeCampaign("a", 0.0004)
        b = FakeCampaign("b", 0.0006)
        self.assertAlmostEqual(claimed_by_others([a, b], a), 0.0006, places=8)

    def test_paper_coin_claims_nothing(self):
        """Matches app.py's _live_claim_of, which already filtered this way —
        the two sums disagreeing is what let imaginary coin count once."""
        live = FakeCampaign("live", 0.0004)
        paper = FakeCampaign("paper", 0.9, mode="paper")
        self.assertEqual(claimed_by_others([live, paper], live), 0.0)

    def test_residual_on_an_open_campaign_still_counts(self):
        a = FakeCampaign("a", 0.0004)
        b = FakeCampaign("b", 0.0001, residual=0.0002)
        self.assertAlmostEqual(claimed_by_others([a, b], a), 0.0003, places=8)

    def test_every_final_state_is_excluded(self):
        me = FakeCampaign("me", 0.0004)
        for state in FINAL_STATES:
            dead = FakeCampaign("dead", 0.5, state=state)
            self.assertEqual(claimed_by_others([me, dead], me), 0.0, state)


class ProductionExpressionMatchesTests(unittest.TestCase):
    """The copy above is only useful while it matches the real one."""

    def test_the_engine_filters_on_all_four_conditions(self):
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        src = open(os.path.join(here, "engine", "cascade.py"), encoding="utf-8").read()
        start = src.index("claimed_by_others = sum(")
        block = src[start : start + 600]
        self.assertIn('str(getattr(c, "mode", "") or "") == "live"', block)
        self.assertIn("not c.closed_at", block)
        self.assertIn('str(c.state or "") not in FINAL_STATES', block)
        self.assertIn("c.filled_base_qty > 0", block)


class FillLineNamesALevelAsALevelTests(unittest.TestCase):
    """The fill line must not describe a ladder level as a price the market hit.

    2026-08-31: "4 level(s) collected down to 77,255.41" alongside a buy at
    77,990 reads as a fill 735 above the low. The market's low that morning was
    77,864 and 77,255 was a rung nobody traded — the entry was 0.16% off the
    real low. Phil read it the only way the sentence allowed.
    """

    def setUp(self):
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.src = open(os.path.join(here, "engine", "cascade.py"), encoding="utf-8").read()

    def test_the_misleading_phrase_is_gone_from_every_message(self):
        # Only the comment explaining the change may still quote it.
        for line in self.src.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            self.assertNotIn("collected down to", line)

    def test_both_the_log_line_and_the_alert_say_level(self):
        self.assertEqual(self.src.count("ladder level(s) collected"), 2)
        self.assertEqual(self.src.count("deepest level {deepest:,.2f}"), 2)
