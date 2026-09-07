"""The Cascade page's own status sheds ended geometry AND ended event logs.

`/api/cascade/status` is polled every 3 seconds by every open tab and was
shipping 3.75 MB each time: 463 campaigns of which 459 had already ended, each
still carrying its ladder geometry and its full event log. 5,567 of the 5,721
event lines belonged to finished campaigns.

Dropping `event_log` is safe HERE and nowhere else. The Cascade page has a
second copy — `_BUCKET_CASCADE_EVENTS` is the live engine's own persisted log,
which the payload now carries whole — while `_strategy_event` deliberately does
not write to that bucket, so for V-Rule and Cascade-Auto the campaign's own
`event_log` is the only copy there is.

What must NOT change: ended campaigns still travel. `closed_campaigns` keeps
only the last 40, and the ledger builds its rounds from both pools, so
filtering them out would delete booked rounds from the realised P&L.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

slim = app_module._slim_ended_campaign


def campaign(**over):
    row = {
        "campaign_id": "c1",
        "symbol": "BTCUSDT",
        "mode": "live",
        "state": "TRENDLINE_ACTIVE",
        "closed_at": "",
        "legs": [{"leg_id": "L1"}],
        "trendlines": [{"a": 1}],
        "mother_break_candle": {"o": 1},
        "mother_break_top_candle": {"o": 2},
        "pending_fibs": [{"f": 1}],
        "event_log": [{"timestamp": "t1", "message": "started"}],
        "rounds": [{"pnl": 4.25}],
        "all_fills": [{"price": 10.0}],
        "realized_pnl_total": 4.25,
    }
    row.update(over)
    return row


ENDED = ("COMPLETED", "MOTHER_BROKEN", "STOPPED")


def test_working_campaign_keeps_its_event_log():
    row = campaign()
    assert slim(row, drop_events=True) == row


def test_ended_campaign_loses_its_event_log_when_asked():
    for state in ENDED:
        out = slim(campaign(state=state), drop_events=True)
        assert "event_log" not in out, state


def test_ended_campaign_keeps_its_event_log_by_default():
    """The V-Rule and Cascade-Auto path: that log is their only copy."""
    for state in ENDED:
        out = slim(campaign(state=state))
        assert out["event_log"] == [{"timestamp": "t1", "message": "started"}], state


def test_closed_at_alone_ends_a_campaign():
    out = slim(campaign(closed_at="2026-09-08 11:00:00"), drop_events=True)
    assert "event_log" not in out
    assert "legs" not in out


def test_ended_campaign_keeps_its_rounds_and_money():
    """The ledger reads these from ended campaigns. Losing them loses P&L."""
    out = slim(campaign(state="COMPLETED"), drop_events=True)
    assert out["rounds"] == [{"pnl": 4.25}]
    assert out["all_fills"] == [{"price": 10.0}]
    assert out["realized_pnl_total"] == 4.25
    assert out["campaign_id"] == "c1"


def test_geometry_still_goes_with_the_event_log():
    out = slim(campaign(state="COMPLETED"), drop_events=True)
    for key in app_module._ENDED_CAMPAIGN_DROP:
        assert key not in out, key


def test_drop_events_does_not_mutate_the_caller():
    row = campaign(state="COMPLETED")
    slim(row, drop_events=True)
    assert "event_log" in row and "legs" in row


def test_non_dict_passes_through():
    assert slim(None, drop_events=True) is None
    assert slim("x", drop_events=True) == "x"
