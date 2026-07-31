"""Commissions charged in BNB.

With "pay fees with BNB" switched on, Binance takes the commission in BNB —
neither side of the pair traded. Every fee reader here understood only the base
and the quote asset, so it scored those commissions as ZERO: fees vanished from
the books the moment the discount was enabled, and net P&L silently became
gross P&L. These cover the conversion that fixes that, and the reporting that
makes the discount's state visible without opening Binance.
"""

import unittest
from unittest.mock import patch

from broker.binance import BinanceSpotClient
from engine.trade_journal import pair_fills_into_trades

_BASE = 1_784_000_000_000


def _client() -> BinanceSpotClient:
    client = BinanceSpotClient.__new__(BinanceSpotClient)
    client.quote_asset = "USDT"
    client._fee_price_cache = {}
    client._FEE_PRICE_TTL = 300
    return client


class CommissionConversionTests(unittest.TestCase):
    def test_bnb_commission_is_priced_not_dropped(self):
        client = _client()
        with patch.object(BinanceSpotClient, "_market_get", return_value={"price": "600.0"}):
            fee = client._commission_in_quote(0.00002, "BNB", "BTC", "USDT", 61000.0)
        self.assertAlmostEqual(fee, 0.012, places=6)

    def test_quote_and_base_commissions_still_work(self):
        client = _client()
        self.assertEqual(client._commission_in_quote(0.019, "USDT", "BTC", "USDT", 61000.0), 0.019)
        # Charged in the coin: worth its fill price.
        self.assertAlmostEqual(client._commission_in_quote(0.0000003, "BTC", "BTC", "USDT", 61000.0), 0.0183, places=6)

    def test_a_stablecoin_fee_needs_no_ticker(self):
        client = _client()
        with patch.object(BinanceSpotClient, "_market_get", side_effect=AssertionError("must not price a stable")):
            self.assertEqual(client._commission_in_quote(0.02, "FDUSD", "BTC", "USDT", 61000.0), 0.02)

    def test_a_stale_price_beats_reporting_no_fee_at_all(self):
        client = _client()
        with patch.object(BinanceSpotClient, "_market_get", return_value={"price": "600.0"}):
            client._fee_asset_price_usd("BNB")
        client._FEE_PRICE_TTL = -1  # force the cache to look expired
        with patch.object(BinanceSpotClient, "_market_get", side_effect=RuntimeError("ticker down")):
            fee = client._commission_in_quote(0.00002, "BNB", "BTC", "USDT", 61000.0)
        self.assertAlmostEqual(fee, 0.012, places=6, msg="a wrong-by-cents fee beats a zero")

    def test_an_unpriceable_asset_reports_zero_rather_than_nonsense(self):
        client = _client()
        with patch.object(BinanceSpotClient, "_market_get", return_value={"price": "0"}):
            self.assertEqual(client._commission_in_quote(5.0, "MYSTERY", "BTC", "USDT", 61000.0), 0.0)

    def test_the_price_is_fetched_once_and_cached(self):
        client = _client()
        with patch.object(BinanceSpotClient, "_market_get", return_value={"price": "600.0"}) as market:
            for _ in range(5):
                client._commission_in_quote(0.00002, "BNB", "BTC", "USDT", 61000.0)
        self.assertEqual(market.call_count, 1, "a history scan must not price BNB once per fill")

    def test_order_fills_convert_the_same_way(self):
        client = _client()
        result = {
            "executedQty": "0.0002",
            "cummulativeQuoteQty": "12.20",
            "fills": [{"price": "61000", "qty": "0.0002", "commission": "0.00002", "commissionAsset": "BNB"}],
        }
        with (
            patch.object(
                BinanceSpotClient, "get_product_by_symbol", return_value={"base_asset": "BTC", "quote_asset": "USDT"}
            ),
            patch.object(BinanceSpotClient, "_market_get", return_value={"price": "600.0"}),
        ):
            out = client._attach_spot_order_fill_fields(result, "BTCUSDT")
        self.assertAlmostEqual(float(out["paid_commission"]), 0.012, places=6)


def _fill(symbol, side, price, qty, *, ms, fee, asset):
    return {
        "symbol": symbol,
        "isBuyer": side == "buy",
        "side": side,
        "price": str(price),
        "qty": str(qty),
        "quoteQty": str(round(price * qty, 8)),
        "time": ms,
        "paid_commission": fee,
        "commissionAsset": asset,
    }


class JournalFeeAssetTests(unittest.TestCase):
    def test_the_journal_reports_which_asset_paid_the_fee(self):
        fills = [
            _fill("SOLUSDT", "buy", 73.0, 0.2, ms=_BASE, fee=0.011, asset="BNB"),
            _fill("SOLUSDT", "sell", 74.0, 0.2, ms=_BASE + 1000, fee=0.011, asset="BNB"),
        ]
        trade = pair_fills_into_trades(fills)[0]
        self.assertEqual(trade["status"], "Closed")
        self.assertEqual(trade["fee_assets"], {"BNB": 0.022})
        self.assertAlmostEqual(trade["fees_usd"], 0.022, places=4)
        # Net is gross minus the fee — the whole point of not losing it.
        self.assertAlmostEqual(trade["pnl_gross_usd"] - trade["fees_usd"], trade["pnl_usd"], places=4)

    def test_a_mixed_run_records_both_assets(self):
        # What a BNB balance running dry mid-run actually looks like.
        fills = [
            _fill("SOLUSDT", "buy", 73.0, 0.2, ms=_BASE, fee=0.011, asset="BNB"),
            _fill("SOLUSDT", "sell", 74.0, 0.2, ms=_BASE + 1000, fee=0.0148, asset="SOL"),
        ]
        trade = pair_fills_into_trades(fills)[0]
        self.assertEqual(sorted(trade["fee_assets"]), ["BNB", "SOL"])

    def test_a_zero_fee_records_no_asset(self):
        fills = [
            _fill("SOLUSDT", "buy", 73.0, 0.2, ms=_BASE, fee=0.0, asset="BNB"),
            _fill("SOLUSDT", "sell", 74.0, 0.2, ms=_BASE + 1000, fee=0.0, asset="BNB"),
        ]
        self.assertEqual(pair_fills_into_trades(fills)[0]["fee_assets"], {})


if __name__ == "__main__":
    unittest.main()
