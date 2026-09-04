"""Cascade-Auto's status sheds ladder geometry it no longer draws — and NOTHING else.

242 campaigns made a 1.3 MB payload on a poll that repeats every few seconds,
and `legs` alone was 454 KB of it. But this page must NOT get the V-Rule's
treatment of leaving ended campaigns out: 191 of its 231 ended campaigns are
not in `closed_campaigns` (that list keeps only the last CLOSED_HISTORY_LIMIT),
and the Paper Journal builds its rounds from BOTH pools. Filtering would have
deleted 15 booked rounds from the realised P&L without a word.

So every campaign still travels; a finished one simply arrives without the
geometry only a card or a chart reads.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

slim = app_module._slim_ended_campaign
working = app_module._cascade_campaign_working


def campaign(**over):
    row = {
        "campaign_id": "c1",
        "symbol": "BTCUSDT",
        "seq": 12,
        "mode": "paper",
        "state": "TRENDLINE_ACTIVE",
        "closed_at": "",
        "filled_base_qty": 0.0,
        "pending_usd": 0.0,
        "pending_stop_price": None,
        "pending_order_id": None,
        "legs": [{"leg_id": 1, "low": 100.0}],
        "trendlines": [{"id": 1}],
        "mother_break_candle": {"t": 1},
        "mother_break_top_candle": {"t": 2},
        "pending_fibs": [{"x": 1}],
        "rounds": [{"round_id": 1, "pnl": 0.42}],
        "event_log": [{"timestamp": "2026-09-04 10:00", "message": "started"}],
        "all_fills": [{"price": 100.0, "qty": 0.1}],
        "realized_pnl": 0.42,
        "avg_entry_price": 100.0,
        "tp_price": 101.0,
        "exchange_qty": 0.0,
    }
    row.update(over)
    return row


# ── what a RUNNING campaign keeps ─────────────────────────────────────────


def test_a_running_campaign_is_untouched():
    c = campaign()
    assert slim(c) == c
    assert slim(c)["legs"] == c["legs"]


# ── what an ENDED campaign loses, and what it must not ────────────────────


ENDED = [
    {"state": "COMPLETED"},
    {"state": "MOTHER_BROKEN"},
    {"state": "STOPPED"},
    {"state": "TRENDLINE_ACTIVE", "closed_at": "2026-09-04 10:00"},
]


def test_an_ended_campaign_loses_only_the_geometry():
    for over in ENDED:
        out = slim(campaign(**over))
        for gone in ("legs", "trendlines", "mother_break_candle", "mother_break_top_candle", "pending_fibs"):
            assert gone not in out, (over, gone)


def test_the_ledger_still_gets_its_rounds():
    """The 15 rounds a naive filter would have deleted live here."""
    for over in ENDED:
        out = slim(campaign(**over))
        assert out["rounds"] == [{"round_id": 1, "pnl": 0.42}], over
        assert out["realized_pnl"] == 0.42


def test_the_event_log_arrives_whole():
    """Paged, not truncated, by an earlier deliberate decision — so it must
    travel complete however old the campaign is."""
    log = [{"timestamp": f"2026-09-04 10:{i:02d}", "message": str(i)} for i in range(200)]
    out = slim(campaign(state="COMPLETED", event_log=log))
    assert out["event_log"] == log


def test_open_trades_still_has_what_it_reads():
    out = slim(campaign(state="STOPPED", filled_base_qty=0.5))
    for kept in ("filled_base_qty", "all_fills", "avg_entry_price", "tp_price", "exchange_qty", "symbol", "seq"):
        assert kept in out, kept


def test_identity_survives_so_the_ledger_can_dedupe_by_it():
    out = slim(campaign(state="COMPLETED"))
    assert out["campaign_id"] == "c1"
    assert out["mode"] == "paper"


def test_a_non_dict_is_handed_back_unchanged():
    assert slim(None) is None
    assert slim("nonsense") == "nonsense"


# ── the shared "working" definition ───────────────────────────────────────


def test_working_matches_the_client_side_rule():
    assert working(campaign(filled_base_qty=0.1)) is True
    assert working(campaign(pending_usd=5.5)) is True
    assert working(campaign(pending_stop_price=100.0)) is True
    assert working(campaign()) is False
    assert working(campaign(state="COMPLETED", filled_base_qty=0.1)) is False
    assert working(campaign(closed_at="2026-09-04 10:00", pending_usd=5.5)) is False
