"""The V-Rule live status sends the ladders you can act on, not every V it ever saw.

The driver opens a campaign on each confirmed V, and most never come back to
their entry. In two days the list reached 260 campaigns of which five were
armed or holding; the page drew a card for every one and the poll carried
796 KB (Phil, 2026-09-04: "Why so much campaigns are shown here... Can we put
only those were armed or in the trade?").

Nothing is lost by leaving them out: a campaign that ended is in
`closed_campaigns`, and the ones still waiting are counted in `watching`.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

working = app_module._cascade_campaign_working


def campaign(**over):
    row = {
        "campaign_id": "c1",
        "state": "WAITING_FIRST_DEPTH",
        "closed_at": "",
        "filled_base_qty": 0.0,
        "pending_usd": 0.0,
        "pending_stop_price": None,
        "pending_order_id": None,
    }
    row.update(over)
    return row


# ── what counts as working ────────────────────────────────────────────────


def test_a_ladder_holding_coin_is_working():
    assert working(campaign(filled_base_qty=0.0004)) is True


def test_a_ladder_with_money_on_a_resting_buy_is_working():
    assert working(campaign(pending_usd=5.5)) is True


def test_a_ladder_with_a_stop_resting_on_the_exchange_is_working():
    assert working(campaign(pending_stop_price=77990.0)) is True
    assert working(campaign(pending_order_id="cf-csc-abc-1")) is True


def test_a_v_that_never_armed_is_not_working():
    """24 of these on 2026-09-04. Real history, but nothing to act on."""
    assert working(campaign()) is False


def test_a_finished_ladder_is_not_working_however_it_ended():
    for state in ("COMPLETED", "MOTHER_BROKEN", "STOPPED"):
        assert working(campaign(state=state)) is False, state


def test_a_closed_ladder_still_holding_is_not_listed_as_working():
    """It ended; its coin is a settlement question, not a live ladder. This is
    also what stops the 229 completed campaigns coming back through the
    holding branch."""
    assert working(campaign(state="STOPPED", closed_at="2026-09-04 10:00", filled_base_qty=0.5)) is False


def test_a_bad_number_does_not_promote_a_dead_ladder():
    assert working(campaign(filled_base_qty="not a number")) is False
    assert working(campaign(pending_usd=None)) is False


# ── the shape the page receives ───────────────────────────────────────────


class FakeEngine:
    def __init__(self, campaigns):
        self._campaigns = campaigns
        self.closed_campaigns = []

    def get_status(self):
        return {"campaigns": self._campaigns, "closed_campaigns": [], "instruments": {}}


class FakeDriver:
    def status(self):
        return {"books": []}


def _status(monkeypatch, campaigns):
    import asyncio

    monkeypatch.setattr(app_module, "_get_vrule", lambda: FakeDriver())
    monkeypatch.setattr(app_module, "_get_vrule_engine", lambda: FakeEngine(campaigns))
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(app_module.vrule_live_status())


def test_every_campaign_still_travels_so_the_ledger_keeps_its_rounds(monkeypatch):
    rows = [
        campaign(campaign_id="held", filled_base_qty=0.01),
        campaign(campaign_id="armed", pending_usd=5.5),
        campaign(campaign_id="waiting"),
        campaign(campaign_id="done", state="COMPLETED"),
        campaign(campaign_id="broke", state="MOTHER_BROKEN"),
    ]
    out = _status(monkeypatch, rows)
    # All five, because the Closed Rounds table reads its rounds off ended
    # campaigns that closed_campaigns does not keep. The CARDS are filtered on
    # the client; the payload is not.
    assert [c["campaign_id"] for c in out["campaigns"]] == ["held", "armed", "waiting", "done", "broke"]
    assert out["watching"] == 3


def test_the_ones_left_out_are_counted_not_silently_dropped(monkeypatch):
    rows = [campaign(campaign_id="armed", pending_usd=5.5)] + [campaign(campaign_id=f"w{i}") for i in range(24)]
    out = _status(monkeypatch, rows)
    assert out["watching"] == 24
    assert len(out["campaigns"]) == 25


def test_nothing_working_still_reports_the_watchers(monkeypatch):
    out = _status(monkeypatch, [campaign(campaign_id=f"w{i}") for i in range(7)])
    assert len(out["campaigns"]) == 7
    assert out["watching"] == 7


def test_an_empty_book_reports_none_watching(monkeypatch):
    out = _status(monkeypatch, [])
    assert out["campaigns"] == []
    assert out["watching"] == 0
