"""The buyer's own venue, read for candles and for the four quotes on top.

The strip is the only thing on the page that is not about their money, which
is exactly why it gets tests: decoration that can raise from inside the
trading tick is not decoration, it is a way to stop managing positions.
"""

import unittest

from executor.market import STRIP_SYMBOLS, ExchangeMarketData, MarketStrip


class BinanceTickerTests(unittest.TestCase):
    def setUp(self):
        self.calls = []

    def _http(self, url, params):
        self.calls.append((url, params))
        return [
            {"symbol": "BTCUSDT", "lastPrice": "121000.50", "priceChangePercent": "1.25"},
            {"symbol": "SOLUSDT", "lastPrice": "180.20", "priceChangePercent": "-3.40"},
            {"symbol": "DOGEUSDT", "lastPrice": "0.12", "priceChangePercent": "9.00"},
        ]

    def test_every_symbol_comes_back_in_one_request(self):
        market = ExchangeMarketData(None, "binance", http=self._http)
        rows = market.ticker_24h(["BTCUSDT", "SOLUSDT"])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(rows["BTCUSDT"]["price"], 121000.50)
        self.assertEqual(rows["SOLUSDT"]["change_pct"], -3.40)

    def test_a_symbol_that_was_not_asked_for_is_not_reported(self):
        """The venue answers what it likes; the strip draws what it asked."""
        market = ExchangeMarketData(None, "binance", http=self._http)
        self.assertNotIn("DOGEUSDT", market.ticker_24h(["BTCUSDT"]))


class CoinDCXTickerTests(unittest.TestCase):
    def _http(self, url, params):
        return [
            {"market": "BTCUSDT", "last_price": "120500", "change_24_hour": "0.80"},
            {"market": "ETHUSDT", "last_price": "4100", "change_24_hour": "-1.10"},
        ]

    def test_it_quotes_the_buyers_venue_not_ours(self):
        market = ExchangeMarketData(None, "coindcx", http=self._http)
        rows = market.ticker_24h(["BTCUSDT", "ETHUSDT"])
        self.assertEqual(rows["BTCUSDT"]["price"], 120500.0)
        self.assertEqual(rows["ETHUSDT"]["change_pct"], -1.10)

    def test_a_coin_the_venue_does_not_list_is_absent_not_zero(self):
        """An empty cell is honest. A zero is a price."""
        market = ExchangeMarketData(None, "coindcx", http=self._http)
        self.assertNotIn("PAXGUSDT", market.ticker_24h(STRIP_SYMBOLS))


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeTickerMarket:
    def __init__(self):
        self.calls = 0
        self.rows = {"BTCUSDT": {"price": 100.0, "change_pct": 1.0}}
        self.raises = None

    def ticker_24h(self, symbols):
        self.calls += 1
        if self.raises:
            raise self.raises
        return dict(self.rows)


class MarketStripTests(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.market = FakeTickerMarket()
        self.strip = MarketStrip(self.market, refresh_sec=30, clock=self.clock)

    def test_the_four_are_fixed_and_ordered(self):
        """A strip that changes shape with the book is not a reference."""
        self.assertEqual(STRIP_SYMBOLS, ("BTCUSDT", "ETHUSDT", "PAXGUSDT", "SOLUSDT"))
        self.assertEqual(self.strip.snapshot()["symbols"], list(STRIP_SYMBOLS))

    def test_it_runs_on_its_own_clock_not_the_ticks(self):
        for _ in range(20):
            self.strip.snapshot()
        self.assertEqual(self.market.calls, 1)
        self.clock.now += 31
        self.strip.snapshot()
        self.assertEqual(self.market.calls, 2)

    def test_a_venue_that_fails_never_reaches_the_caller(self):
        """It is called from the tick. The class of bug that has bitten this
        codebase three times is a venue call that abandons the work after it."""
        self.strip.snapshot()
        self.market.raises = RuntimeError("venue down")
        self.clock.now += 31
        self.assertEqual(self.strip.snapshot()["rows"]["BTCUSDT"]["price"], 100.0)

    def test_a_failed_refresh_waits_out_the_interval_like_a_good_one(self):
        self.market.raises = RuntimeError("venue down")
        self.strip.snapshot()
        self.strip.snapshot()
        self.assertEqual(self.market.calls, 1)

    def test_an_empty_answer_does_not_blank_the_last_known_prices(self):
        self.strip.snapshot()
        self.market.rows = {}
        self.clock.now += 31
        self.assertEqual(self.strip.snapshot()["rows"]["BTCUSDT"]["price"], 100.0)


if __name__ == "__main__":
    unittest.main()
