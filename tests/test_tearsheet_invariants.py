"""The published books must add up, on BOTH harnesses.

The two strategies do not share a backtest. The V-Rule goes through
rule3070_sim and the cascades through cascade_depth_sweep, and the rows they
produce are read side by side as though they were the same measurement — which
is only safe while the arithmetic behind each field agrees.

It did not. `final_capital` on the cascade path was `result.final_capital =
self.capital`, the sweep's own pot; these runs are deliberately
non-compounding, so that pot never moves and every cascade row published "final
capital = the money you started with", profit and all. It was corrected once by
hand in the data files on 2026-09-08 and came back on the very next run,
because the data was patched and the code was not.

So it is derived in `_shape` now, for both paths, and these invariants hold the
line. They are cheap and they are the difference between a number that is wrong
and a number that is wrong AND believed.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

from tools.tearsheet import run_backtests as rb  # noqa: E402


def shaped(**over):
    """A row as a harness hands it over, before _shape derives anything."""
    row = {
        "capital": 500.0,
        "net_pnl": 1000.0,  # closed rounds
        "open_pnl": -200.0,  # the bag
        "total_pnl": 800.0,
        "stranded_cost": 300.0,
        "stranded_value": 100.0,
        "peak_deployed": 650.0,
        "first_ts": 1502942400,
        "last_ts": 1785542100,
        "span_days": 3270,
        # what the SWEEP wrongly supplies: its own un-compounded pot
        "final_capital": 500.0,
    }
    row.update(over)
    return rb._shape(row, "BTCUSDT", "auto")


def test_final_capital_counts_the_profit():
    """The bug: the sweep's pot was published verbatim."""
    row = shaped()
    assert row["final_capital"] == 1300.0, "capital + total_pnl, not the starting pot"


def test_final_capital_counts_the_bag_against_you():
    """A round only closes AT TARGET, so the whole loss lives in the bag."""
    row = shaped(net_pnl=1000.0, open_pnl=-900.0, total_pnl=100.0)
    assert row["final_capital"] == 600.0


def test_a_harness_supplied_value_never_wins():
    """Whatever either harness says, _shape decides — that is the point."""
    row = shaped(final_capital=99999.0)
    assert row["final_capital"] == 1300.0


def test_every_rate_reads_the_purse_off_the_row():
    """return_pct and per_year_pct took the MODULE global, final_capital the row.

    They agreed only because main() sets the global before the row is built.
    With per-strategy purses that is one reordering away from quoting a rate
    against a purse the run never used, so all three read the same field now.
    """
    row = shaped()
    assert row["return_pct"] == pytest.approx(800 / 500 * 100, rel=1e-3)
    assert row["final_capital"] == 1300.0


def test_the_peak_rate_is_the_smaller_one_when_peak_exceeds_the_purse():
    """Peak 650 against a 500 purse: money at risk is MORE, so the rate is less."""
    row = shaped()
    assert row["per_year_on_peak_pct"] < row["per_year_pct"]


def test_peak_of_zero_does_not_explode():
    row = shaped(peak_deployed=0.0)
    assert row["per_year_on_peak_pct"] == 0.0


@pytest.mark.parametrize("strategy", ["hybrid", "auto", "vrule"])
def test_every_published_book_adds_up(strategy):
    """The committed sheets are the provenance; they must satisfy it too."""
    import json

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(here, "tools", "tearsheet", "data", f"{strategy}_report_data.json")
    book = json.load(open(path, encoding="utf-8"))
    coins = book["coins"]
    coins = coins if isinstance(coins, dict) else {c["symbol"]: c for c in coins}
    assert coins, f"{strategy} has no coins"
    for symbol, row in coins.items():
        where = f"{strategy}/{symbol}"
        assert abs(row["total_pnl"] - (row["net_pnl"] + row["open_pnl"])) < 0.02, where
        assert abs(row["open_pnl"] - (row["stranded_value"] - row["stranded_cost"])) < 0.02, where
        assert abs(row["final_capital"] - (row["capital"] + row["total_pnl"])) < 0.02, where
        assert row["wins"] + row["losses"] == row["rounds"], where
        # The monthly series is CLOSED money; the bag is not in it.
        assert abs(sum(row["monthly"].values()) - row["net_pnl"]) < 0.5, where
