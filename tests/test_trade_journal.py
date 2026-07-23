"""
Pairing exchange fills into journal trades.

The journal was a static Google Sheets export, so trades placed by hand on
Binance never showed up at all. These cover the pairing that fixes that —
buys accumulating a position, sells closing it, and the awkward cases that a
real spot account actually produces.
"""

import unittest

from engine.trade_journal import merge_with_sheet, pair_fills_into_trades

_DAY = 86_400_000
_BASE = 1_784_000_000_000  # a fixed ms epoch so dates in assertions are stable


def fill(symbol, side, price, qty, *, ms, fee=0.0):
    """A fill shaped the way broker/binance.py normalises /api/v3/myTrades."""
    return {
        "symbol": symbol,
        "isBuyer": side == "buy",
        "side": side,
        "price": str(price),
        "qty": str(qty),
        "quoteQty": str(round(price * qty, 8)),
        "time": ms,
        "paid_commission": fee,
        "commissionAsset": "USDT",
    }


class PairingTests(unittest.TestCase):
    def test_a_ladder_of_buys_and_one_sell_is_one_trade(self):
        """The shape these trades are actually placed in."""
        fills = [
            fill("SOLUSDT", "buy", 81.45, 0.1, ms=_BASE),
            fill("SOLUSDT", "buy", 80.52, 0.153, ms=_BASE + 60_000),
            fill("SOLUSDT", "sell", 81.65, 0.253, ms=_BASE + 120_000),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t["coin"], "SOLUSDT")
        self.assertEqual(t["status"], "Closed")
        self.assertEqual(t["buy_count"], 2)
        self.assertAlmostEqual(t["total_qty"], 0.253, places=6)
        cost = 81.45 * 0.1 + 80.52 * 0.153
        self.assertAlmostEqual(t["invested_usd"], cost, places=3)
        self.assertAlmostEqual(t["avg_buy_price"], cost / 0.253, places=4)
        self.assertAlmostEqual(t["sell_price"], 81.65, places=4)

    def test_pnl_is_net_of_fees(self):
        """The whole point of reading the exchange rather than a spreadsheet."""
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE, fee=0.10),
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 60_000, fee=0.101),
        ]
        t = pair_fills_into_trades(fills)[0]
        self.assertAlmostEqual(t["pnl_gross_usd"], 1.0, places=4)
        self.assertAlmostEqual(t["fees_usd"], 0.201, places=4)
        self.assertAlmostEqual(t["pnl_usd"], 0.799, places=4)
        self.assertLess(t["pnl_usd"], t["pnl_gross_usd"], "net must be below gross")

    def test_roi_is_computed_on_net(self):
        fills = [
            fill("BTCUSDT", "buy", 100.0, 1.0, ms=_BASE, fee=0.1),
            fill("BTCUSDT", "sell", 110.0, 1.0, ms=_BASE + 1000, fee=0.11),
        ]
        t = pair_fills_into_trades(fills)[0]
        self.assertAlmostEqual(t["roi_pct"], (10.0 - 0.21) / 100.0 * 100, places=3)

    def test_two_round_trips_are_two_trades(self):
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 1000),
            fill("SOLUSDT", "buy", 99.0, 2.0, ms=_BASE + _DAY),
            fill("SOLUSDT", "sell", 100.0, 2.0, ms=_BASE + _DAY + 1000),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual(len(trades), 2)
        self.assertEqual([t["status"] for t in trades], ["Closed", "Closed"])
        self.assertEqual([t["buy_count"] for t in trades], [1, 1])

    def test_selling_in_pieces_still_closes_one_trade(self):
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("SOLUSDT", "sell", 101.0, 0.4, ms=_BASE + 1000),
            fill("SOLUSDT", "sell", 102.0, 0.6, ms=_BASE + 2000),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["status"], "Closed")
        # Average of the two sells, weighted by size.
        self.assertAlmostEqual(trades[0]["sell_price"], (101.0 * 0.4 + 102.0 * 0.6) / 1.0, places=4)

    def test_lot_size_dust_still_counts_as_closed(self):
        """BTC's lot step routinely strands a sliver. Waiting for an exact zero
        would hold a finished trade open forever."""
        fills = [
            fill("BTCUSDT", "buy", 66000.0, 0.00011, ms=_BASE),
            fill("BTCUSDT", "sell", 66300.0, 0.00010, ms=_BASE + 1000),
        ]
        t = pair_fills_into_trades(fills)[0]
        self.assertEqual(t["status"], "Closed")
        self.assertGreater(t["residual_qty"], 0.0, "the unsold sliver is reported, not hidden")

    def test_dust_is_not_valued_at_the_sell_price(self):
        """Only what actually sold has a realised result."""
        fills = [
            fill("BTCUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("BTCUSDT", "sell", 110.0, 0.99, ms=_BASE + 1000),
        ]
        t = pair_fills_into_trades(fills)[0]
        # 0.99 sold at 110 against a cost basis of 0.99 x 100 = 99.
        self.assertAlmostEqual(t["pnl_gross_usd"], 0.99 * 110 - 99.0, places=4)

    def test_an_unsold_position_is_reported_open(self):
        fills = [fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE)]
        t = pair_fills_into_trades(fills)[0]
        self.assertEqual(t["status"], "Open")
        self.assertEqual(t["pnl_usd"], 0.0)
        self.assertEqual(t["sell_price"], 0.0)

    def test_open_positions_can_be_excluded(self):
        fills = [fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE)]
        self.assertEqual(pair_fills_into_trades(fills, include_open=False), [])

    def test_symbols_are_tracked_separately(self):
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("BTCUSDT", "buy", 60000.0, 0.001, ms=_BASE + 100),
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 200),
            fill("BTCUSDT", "sell", 61000.0, 0.001, ms=_BASE + 300),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual({t["coin"] for t in trades}, {"SOLUSDT", "BTCUSDT"})
        self.assertEqual(len(trades), 2)

    def test_a_sell_with_no_position_is_ignored(self):
        """Happens whenever the history window opens mid-position. It must not
        invent a short on a spot account."""
        fills = [
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE),
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE + 1000),
            fill("SOLUSDT", "sell", 102.0, 1.0, ms=_BASE + 2000),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual(len(trades), 1)
        self.assertAlmostEqual(trades[0]["sell_price"], 102.0, places=4)

    def test_fills_out_of_order_are_sorted_first(self):
        fills = [
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 2000),
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
        ]
        trades = pair_fills_into_trades(fills)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["status"], "Closed")

    def test_buys_carry_how_far_under_the_first_they_landed(self):
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("SOLUSDT", "buy", 98.0, 1.0, ms=_BASE + 1000),
        ]
        buys = pair_fills_into_trades(fills)[0]["buys"]
        self.assertEqual(buys[0]["market_down_pct"], 0.0)
        self.assertAlmostEqual(buys[1]["market_down_pct"], 2.0, places=3)

    def test_dates_are_ist(self):
        """Everything user-facing in this app is IST; a UTC date would put an
        evening trade on the wrong day."""
        # 2026-07-22 19:00 UTC == 2026-07-23 00:30 IST
        ms = int(1784_000_000) * 0 + 1784746800000
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=ms),
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=ms + 1000),
        ]
        t = pair_fills_into_trades(fills)[0]
        from datetime import datetime, timedelta, timezone

        expected = datetime.fromtimestamp((ms + 1000) / 1000, timezone(timedelta(hours=5, minutes=30)))
        self.assertEqual(t["date"], expected.strftime("%Y-%m-%d"))

    def test_garbage_in_does_not_raise(self):
        self.assertEqual(pair_fills_into_trades([]), [])
        self.assertEqual(pair_fills_into_trades(None), [])
        self.assertEqual(pair_fills_into_trades([None, "x", {}, {"symbol": ""}]), [])
        # a fill with no usable price or quantity contributes nothing
        self.assertEqual(pair_fills_into_trades([{"symbol": "SOLUSDT", "isBuyer": True, "price": 0, "qty": 0}]), [])

    def test_bnb_paid_commission_is_not_counted_as_dollars(self):
        """commission is in BNB here; only paid_commission is quote-denominated."""
        raw = fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE)
        raw.pop("paid_commission")
        raw["commission"] = "0.0012"
        raw["commissionAsset"] = "BNB"
        sell = fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 1000)
        sell.pop("paid_commission")
        sell["commission"] = "0.0012"
        sell["commissionAsset"] = "BNB"
        t = pair_fills_into_trades([raw, sell])[0]
        self.assertEqual(t["fees_usd"], 0.0, "0.0012 BNB must not be booked as $0.0012")


class MergeTests(unittest.TestCase):
    SHEET = [
        {"date": "2026-07-05", "coin": "SOLUSDT", "pnl_usd": 0.19},
        {"date": "2026-07-16", "coin": "BTCUSDT", "pnl_usd": 0.04},
    ]

    def test_sheet_is_used_when_the_broker_has_nothing(self):
        merged = merge_with_sheet(self.SHEET, [])
        self.assertEqual(len(merged), 2)
        self.assertTrue(all(r["source"] == "sheet" for r in merged))

    def test_broker_wins_from_its_earliest_fill_onward(self):
        broker = [{"date": "2026-07-16", "coin": "BTCUSDT", "pnl_usd": 0.03, "source": "binance"}]
        merged = merge_with_sheet(self.SHEET, broker)
        self.assertEqual(len(merged), 2, "the 07-16 sheet row is superseded, not duplicated")
        self.assertEqual([r["source"] for r in merged], ["sheet", "binance"])
        self.assertEqual(merged[1]["pnl_usd"], 0.03, "the exchange figure, which knows the fees")

    def test_older_sheet_history_survives(self):
        broker = [{"date": "2026-07-20", "coin": "SOLUSDT", "source": "binance"}]
        merged = merge_with_sheet(self.SHEET, broker)
        self.assertEqual(len(merged), 3)
        self.assertEqual([r["date"] for r in merged], ["2026-07-05", "2026-07-16", "2026-07-20"])

    def test_result_is_date_ordered(self):
        broker = [
            {"date": "2026-07-22", "coin": "SOLUSDT", "source": "binance"},
            {"date": "2026-07-18", "coin": "BTCUSDT", "source": "binance"},
        ]
        merged = merge_with_sheet(self.SHEET, broker)
        self.assertEqual([r["date"] for r in merged], sorted(r["date"] for r in merged))


if __name__ == "__main__":
    unittest.main()


class DustValueRuleTests(unittest.TestCase):
    """Leftover coin is judged by what it is WORTH, not by what share of the
    position it is. One BTC lot step is ~12% of a minimum order, so a fraction
    rule left finished BTC trades open forever."""

    def test_a_remainder_too_small_to_sell_closes_the_trade(self):
        # 0.00001 BTC at 66,300 is $0.66 — under Binance's ~$5 minNotional, so
        # it can never be sold and the trade is over.
        fills = [
            fill("BTCUSDT", "buy", 66000.0, 0.00011, ms=_BASE),
            fill("BTCUSDT", "sell", 66300.0, 0.00010, ms=_BASE + 1000),
        ]
        self.assertEqual(pair_fills_into_trades(fills)[0]["status"], "Closed")

    def test_a_remainder_worth_selling_keeps_the_trade_open(self):
        # Same 9% remainder by quantity, but here it is worth $900 — a real
        # position still held, not dust.
        fills = [
            fill("BTCUSDT", "buy", 66000.0, 0.15, ms=_BASE),
            fill("BTCUSDT", "sell", 66300.0, 0.1364, ms=_BASE + 1000),
        ]
        t = pair_fills_into_trades(fills)[0]
        self.assertEqual(t["status"], "Open", "a $900 remainder is not dust")

    def test_the_boundary_is_the_minimum_notional(self):
        for remaining, expected in ((4.0 / 100.0, "Closed"), (6.0 / 100.0, "Open")):
            fills = [
                fill("XRPUSDT", "buy", 100.0, 1.0 + remaining, ms=_BASE),
                fill("XRPUSDT", "sell", 100.0, 1.0, ms=_BASE + 1000),
            ]
            self.assertEqual(pair_fills_into_trades(fills)[0]["status"], expected, f"remainder {remaining}")

    def test_an_exact_zero_remainder_is_closed(self):
        fills = [
            fill("SOLUSDT", "buy", 100.0, 1.0, ms=_BASE),
            fill("SOLUSDT", "sell", 101.0, 1.0, ms=_BASE + 1000),
        ]
        t = pair_fills_into_trades(fills)[0]
        self.assertEqual(t["status"], "Closed")
        self.assertEqual(t["residual_qty"], 0.0)


class JournalSummaryTests(unittest.TestCase):
    """The Journal KPI summary, from app._journal_summary — realised stats on
    CLOSED trades only, with fees and gross surfaced."""

    def setUp(self):
        import app

        self.summary = app._journal_summary

    def _closed(self, coin, invested, net, gross, fees, roi):
        return {
            "coin": coin,
            "status": "Closed",
            "date": "2026-07-20",
            "invested_usd": invested,
            "pnl_usd": net,
            "pnl_gross_usd": gross,
            "fees_usd": fees,
            "roi_pct": roi,
            "source": "binance",
        }

    def test_fees_and_gross_are_totalled(self):
        trades = [
            self._closed("SOLUSDT", 22.25, 0.1965, 0.2412, 0.0447, 0.883),
            self._closed("BTCUSDT", 7.24, 0.0128, 0.0266, 0.0138, 0.194),
        ]
        s = self.summary(trades, 2000.0)
        self.assertAlmostEqual(s["fees_usd"], 0.0585, places=4)
        self.assertAlmostEqual(s["gross_pnl_usd"], 0.27, places=2)
        self.assertAlmostEqual(s["realized_pnl_usd"], 0.21, places=2)
        self.assertGreater(s["gross_pnl_usd"], s["realized_pnl_usd"], "gross must exceed net")

    def test_open_trades_do_not_count_in_realised_stats(self):
        trades = [
            self._closed("BTCUSDT", 7.24, 0.0128, 0.0266, 0.0138, 0.194),
            {"coin": "SOLUSDT", "status": "Open", "invested_usd": 35.46, "pnl_usd": 0.0, "date": "2026-07-23"},
        ]
        s = self.summary(trades, 2000.0)
        self.assertEqual(s["trade_count"], 1, "the open trade is not a closed trade")
        self.assertEqual(s["open_trade_count"], 1)
        self.assertAlmostEqual(s["open_invested_usd"], 35.46, places=2)
        self.assertAlmostEqual(s["invested_usd"], 7.24, places=2, msg="open capital is not 'deployed' realised")
        self.assertEqual(s["win_rate_pct"], 100.0, "one closed win, not one-of-two")

    def test_a_sheet_row_contributes_no_fees(self):
        """A sheet row has no fee field; its gross must fall back to its own P&L
        so the totals do not silently drop it."""
        trades = [
            {
                "coin": "SOLUSDT",
                "status": "Closed",
                "invested_usd": 20.46,
                "pnl_usd": 0.19,
                "roi_pct": 0.93,
                "source": "sheet",
                "date": "2026-07-05",
            }
        ]
        s = self.summary(trades, 2000.0)
        self.assertEqual(s["fees_usd"], 0.0)
        self.assertAlmostEqual(s["gross_pnl_usd"], 0.19, places=2)
        self.assertAlmostEqual(s["realized_pnl_usd"], 0.19, places=2)

    def test_empty_is_safe(self):
        s = self.summary([], 2000.0)
        self.assertEqual(s["trade_count"], 0)
        self.assertEqual(s["fees_usd"], 0.0)
        self.assertEqual(s["win_rate_pct"], 0.0)
