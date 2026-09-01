"""The public curve's last point must equal the headline net.

They are the same money shown twice on one screen. Before the fix the daily
value was rounded to cents and the rounded value accumulated, so the error
compounded one day at a time; the live page showed $7.74 on the curve and
$7.72 on the tile.
"""

import unittest

import app


def _t(i, day, pnl):
    return {
        "trade_id": f"t{i}",
        "date": day,
        "closed_ts": 1700000000 + i * 86400,
        "coin": "AAA",
        "status": "Closed",
        "invested_usd": 100.0,
        "pnl_usd": pnl,
        "roi_pct": pnl,
    }


class CurveMatchesHeadline(unittest.TestCase):
    def test_endpoint_equals_realized(self):
        # Values chosen so each day rounds DOWN a fraction of a cent: 40 days
        # of x.xx4 accumulate visibly once each is rounded before summing.
        trades = [_t(i, f"2026-07-{(i % 28) + 1:02d}", 0.1049) for i in range(40)]
        s = app._journal_summary(trades, 200.0)
        curve = s["equity_curve"] if "equity_curve" in s else s.get("daily") or s.get("equity")
        self.assertTrue(curve, f"no curve in summary keys: {sorted(s)}")
        last = curve[-1]["cumulative_pnl"]
        self.assertAlmostEqual(
            round(last, 2),
            s["realized_pnl_usd"],
            places=2,
            msg=f"curve endpoint {last} != headline {s['realized_pnl_usd']}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
