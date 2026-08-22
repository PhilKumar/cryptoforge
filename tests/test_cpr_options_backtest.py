"""The parts of the CPR options backtest that would fail silently if wrong."""

from datetime import date

import pandas as pd
import pytest

from tools.cpr_options_backtest import RUNG_ORDER, weekly_levels
from tools.nifty_expiry_calendar import era_for, lot_size, weekly_expiries


def _daily(rows):
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame(
        {
            "open": [r[1] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[3] for r in rows],
            "close": [r[4] for r in rows],
        },
        index=idx,
    )


def test_weekly_levels_use_the_previous_week_only():
    """A level a trade acts on this week may not know a single price from it."""
    rows = [("2024-01-01", 100, 110, 90, 105), ("2024-01-02", 105, 120, 100, 118), ("2024-01-08", 118, 200, 118, 190)]
    lv = weekly_levels(_daily(rows))
    week = lv[date(2024, 1, 8)]
    # built from 1-2 Jan: H=120 L=90 C=118 -> P=(120+90+118)/3
    assert week.pivot == pytest.approx((120 + 90 + 118) / 3)
    assert week.bc == pytest.approx((120 + 90) / 2)
    # nothing from 8 Jan (high 200) may leak in
    assert week.rungs["S1"] == pytest.approx(2 * week.pivot - 120)
    assert week.rungs["R1"] == pytest.approx(2 * week.pivot - 90)


def test_cpr_band_is_ordered_top_above_bottom():
    rows = [("2024-01-01", 100, 110, 90, 91), ("2024-01-08", 100, 110, 90, 95)]
    lv = weekly_levels(_daily(rows))
    assert lv[date(2024, 1, 8)].tc >= lv[date(2024, 1, 8)].bc


def test_half_rungs_sit_between_their_neighbours():
    rows = [("2024-01-01", 100, 130, 70, 110), ("2024-01-08", 100, 110, 90, 95)]
    lv = weekly_levels(_daily(rows))[date(2024, 1, 8)]
    r = lv.rungs
    assert r["S1.5"] == pytest.approx((r["S1"] + r["S2"]) / 2)
    assert r["R1.5"] == pytest.approx((r["R1"] + r["R2"]) / 2)
    assert list(r) == RUNG_ORDER


def test_the_ladder_only_ever_descends():
    """Every rule below reads this list as ordered; an inversion would make a
    stop sit under its own targets."""
    rows = [("2024-01-01", 100, 130, 70, 110), ("2024-01-08", 100, 110, 90, 95)]
    prices = list(weekly_levels(_daily(rows))[date(2024, 1, 8)].rungs.values())
    assert all(a > b for a, b in zip(prices, prices[1:]))


def test_a_rung_knows_its_stop_and_its_targets():
    rows = [("2024-01-01", 100, 130, 70, 110), ("2024-01-08", 100, 110, 90, 95)]
    lv = weekly_levels(_daily(rows))[date(2024, 1, 8)]
    assert lv.above("R1")[0] == "R1.5"
    assert lv.above("BC")[0] == "TC"  # the original rule, unchanged
    assert [k for k, _ in lv.below("R1")][:3] == ["R0.5", "TC", "BC"]
    assert lv.above("R5") is None


def test_expiry_walks_back_off_a_holiday_never_forward():
    """26 Jan 2023 was a Thursday and a market holiday; the contract settled the 25th."""
    sessions = [date(2023, 1, d) for d in (23, 24, 25, 27, 30, 31)]
    exp = weekly_expiries(sessions)
    assert date(2023, 1, 25) in exp
    assert date(2023, 1, 27) not in exp


def test_lot_size_steps_are_keyed_by_expiry():
    assert lot_size(date(2021, 3, 25)) == 75  # before the Aug-2021 cut
    assert lot_size(date(2022, 6, 30)) == 50
    assert lot_size(date(2024, 11, 28)) == 25  # the step that used to read 50
    assert lot_size(date(2025, 6, 26)) == 75
    assert lot_size(date(2026, 3, 10)) == 65


def test_expiry_weekday_moves_to_tuesday_in_september_2025():
    assert era_for(date(2025, 8, 20))["expiry_weekday"] == 3
    assert era_for(date(2025, 9, 3))["expiry_weekday"] == 1


def test_a_calls_targets_run_upward_and_nearest_first():
    """The bug this catches produced zero trailing exits on every call run: the
    ladder came out farthest-rung-first, so the trail never engaged."""
    rows = [("2024-01-01", 100, 130, 70, 110), ("2024-01-08", 100, 110, 90, 95)]
    lv = weekly_levels(_daily(rows))[date(2024, 1, 8)]

    put = lv.below("R1")
    assert [k for k, _ in put][:3] == ["R0.5", "TC", "BC"]
    assert put[0][1] > put[1][1], "a put's targets must descend"

    call = list(reversed(lv.rungs_above("S1")))
    assert [k for k, _ in call][:3] == ["S0.5", "BC", "TC"]
    assert call[0][1] < call[1][1], "a call's targets must ascend, nearest first"
