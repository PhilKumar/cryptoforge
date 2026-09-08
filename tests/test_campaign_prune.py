"""Ended campaigns that never traded must not accumulate forever.

This is why the site got slow "suddenly" without the code changing.
_adopt_ended_campaigns copied a finished campaign into closed_campaigns but
left it in self.campaigns, so the live set only grew. The V-Rule driver opens a
campaign on every confirmed V and most never reach their entry: from
2026-09-02 it added ~130 a day, and by 2026-09-08 was carrying 738 of which 660
had never placed a trade — 85% of its payload. Cascade held 420 of 465,
Cascade-Auto 246 of 277.

Everything that hurt scaled with that count: get_status building a dict per
campaign, FastAPI encoding them, the runtime snapshot serializing them on every
geometry change.

The line that must never move: a campaign that EVER TRADED stays, whatever its
age. closed_campaigns keeps only the last hundred and the ledger reads rounds
from both pools, so dropping a traded campaign deletes booked P&L.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cascade import KEEP_RECENT_DEAD_CAMPAIGNS, CascadeEngine  # noqa: E402


class FakeCampaign:
    def __init__(self, cid, state="COMPLETED", closed_at="2026-09-08", rounds=None, fills=None, qty=0.0, pnl=0.0):
        self.campaign_id = cid
        self.state = state
        self.closed_at = closed_at
        self.rounds = rounds or []
        self.all_fills = fills or []
        self.filled_base_qty = qty
        self._pnl = pnl

    @property
    def realized_pnl_total(self):
        return self._pnl


def engine_with(campaigns):
    eng = CascadeEngine.__new__(CascadeEngine)
    eng.campaigns = {c.campaign_id: c for c in campaigns}
    return eng


def dead_batch(n, prefix="d"):
    """Ended, never traded, oldest first by closed_at."""
    return [FakeCampaign(f"{prefix}{i}", closed_at="2026-09-%02d" % (1 + i % 28)) for i in range(n)]


def test_the_backlog_is_dropped_but_a_recent_tail_survives():
    """A campaign just stopped by hand still needs its Delete button to work."""
    eng = engine_with(dead_batch(KEEP_RECENT_DEAD_CAMPAIGNS + 200))
    assert eng.prune_dead_campaigns() == 200
    assert len(eng.campaigns) == KEEP_RECENT_DEAD_CAMPAIGNS


def test_a_small_book_is_left_entirely_alone():
    eng = engine_with(dead_batch(3))
    assert eng.prune_dead_campaigns() == 0
    assert len(eng.campaigns) == 3


def test_a_campaign_with_rounds_is_kept():
    eng = engine_with([FakeCampaign("traded", rounds=[{"pnl": 1.0}])])
    assert eng.prune_dead_campaigns() == 0
    assert "traded" in eng.campaigns


def test_a_campaign_with_fills_is_kept():
    eng = engine_with([FakeCampaign("filled", fills=[{"price": 1.0}])])
    assert eng.prune_dead_campaigns() == 0
    assert "filled" in eng.campaigns


def test_a_campaign_still_holding_coin_is_kept():
    eng = engine_with([FakeCampaign("holding", qty=0.5)])
    assert eng.prune_dead_campaigns() == 0
    assert "holding" in eng.campaigns


def test_a_campaign_with_realised_pnl_is_kept():
    eng = engine_with([FakeCampaign("booked", pnl=4.25)])
    assert eng.prune_dead_campaigns() == 0
    assert "booked" in eng.campaigns


def test_a_running_campaign_is_never_touched():
    eng = engine_with([FakeCampaign("live", state="TRENDLINE_ACTIVE", closed_at="")])
    assert eng.prune_dead_campaigns() == 0
    assert "live" in eng.campaigns


def test_the_real_shape_of_the_leak():
    """660 dead against 55 traded — the V-Rule book on 2026-09-08."""
    dead = dead_batch(660)
    traded = [FakeCampaign(f"t{i}", rounds=[{"pnl": 1.0}]) for i in range(55)]
    live = [FakeCampaign(f"w{i}", state="TRENDLINE_ACTIVE", closed_at="") for i in range(23)]
    eng = engine_with(dead + traded + live)

    assert eng.prune_dead_campaigns() == 660 - KEEP_RECENT_DEAD_CAMPAIGNS
    # 55 traded + 23 working + the kept tail. Nothing with money is touched.
    assert len(eng.campaigns) == 55 + 23 + KEEP_RECENT_DEAD_CAMPAIGNS
    assert all(c.campaign_id.startswith("t") for c in eng.campaigns.values() if c.rounds)
    assert sum(1 for c in eng.campaigns.values() if c.rounds) == 55


def test_pruning_twice_changes_nothing():
    eng = engine_with(dead_batch(KEEP_RECENT_DEAD_CAMPAIGNS + 10) + [FakeCampaign("traded", rounds=[{"pnl": 1.0}])])
    assert eng.prune_dead_campaigns() == 10
    assert eng.prune_dead_campaigns() == 0
    assert "traded" in eng.campaigns
