"""The Scalp desk's chart and its price alarms (engine/scalp_chart.py + routes).

Phil, 2026-11-04: "Please create the same kinda scalp page for cryptoforge as
well same as that of philforge". The chart beside the order form needs candles
from the Scalp broker, a 20-EMA and each day's CPR; and right-click alarms the
server keeps and rings once.

The tests that matter most:

* `test_each_day_carries_the_levels_of_the_daily_bar_before_it` -- crypto has
  no session, so a "day" is the exchange's UTC daily candle, and 1000 one-minute
  bars do not reach back to yesterday: the levels must come from DAILY bars.
* `test_an_alarm_rings_once` and `test_a_status_push_rings_the_alarm` -- the
  server rings from the engine's own status pushes (no loop of its own, which
  would die at every deploy), and a rung alarm never rings twice.
"""

import asyncio
import os
import tempfile
import unittest
from importlib import import_module

import pandas as pd
from state_quiesce import quiesce_state_writers

from engine import scalp_chart as sc

DAY = 86_400


def _frame(rows):
    """rows: [(epoch_sec, o, h, l, c)] -> a broker-shaped OHLC frame."""
    index = pd.to_datetime([r[0] for r in rows], unit="s", utc=True)
    return pd.DataFrame(
        {
            "open": [r[1] for r in rows],
            "high": [r[2] for r in rows],
            "low": [r[3] for r in rows],
            "close": [r[4] for r in rows],
            "volume": [1.0] * len(rows),
        },
        index=index,
    )


class CandleTests(unittest.TestCase):
    def test_frame_becomes_epoch_second_candles_oldest_first(self):
        frame = _frame([(1_800_000_300, 2, 3, 1, 2.5), (1_800_000_000, 1, 2, 0.5, 1.5)])
        candles = sc.frame_to_candles(frame)
        self.assertEqual([c["t"] for c in candles], [1_800_000_000, 1_800_000_300])
        self.assertEqual(candles[0], {"t": 1_800_000_000, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5})

    def test_bad_rows_are_dropped_and_the_tail_kept(self):
        rows = [(1_800_000_000 + i * 60, 1, 2, 0.5, 1.5) for i in range(10)]
        rows.append((1_800_000_000 + 10 * 60, float("nan"), 2, 0.5, 1.5))
        candles = sc.frame_to_candles(_frame(rows), limit=4)
        self.assertEqual(len(candles), 4)
        self.assertEqual(candles[-1]["t"], 1_800_000_000 + 9 * 60)

    def test_empty_frame_is_no_candles(self):
        self.assertEqual(sc.frame_to_candles(pd.DataFrame()), [])
        self.assertEqual(sc.frame_to_candles(None), [])


class StudyTests(unittest.TestCase):
    def test_ema_needs_its_period_of_bars(self):
        bars = [{"t": i * 60, "o": 1, "h": 1, "l": 1, "c": float(i)} for i in range(20)]
        self.assertIsNone(sc.ema_overlay(bars))
        ov = sc.ema_overlay(bars + [{"t": 20 * 60, "o": 1, "h": 1, "l": 1, "c": 20.0}])
        self.assertEqual(ov["group"], "ema")
        self.assertEqual(ov["points"][0]["t"], 19 * 60)

    def test_cpr_levels_are_the_floor_pivots(self):
        levels = dict((name, price) for name, price, _ in sc.cpr_levels(110.0, 90.0, 100.0))
        self.assertAlmostEqual(levels["CPR P"], 100.0)
        self.assertAlmostEqual(levels["CPR BC"], 100.0)
        self.assertAlmostEqual(levels["R1"], 110.0)
        self.assertAlmostEqual(levels["S1"], 90.0)
        self.assertAlmostEqual(levels["R2"], 120.0)
        self.assertLessEqual(levels["CPR BC"], levels["CPR TC"])

    def test_each_day_carries_the_levels_of_the_daily_bar_before_it(self):
        d0 = 20_000 * DAY  # a UTC midnight
        daily = [
            {"t": d0, "o": 100, "h": 110, "l": 90, "c": 100},
            {"t": d0 + DAY, "o": 100, "h": 220, "l": 180, "c": 200},
        ]
        # Intraday bars on day 1 and day 2 only: day 1's levels come from the
        # daily bar of day 0, which the intraday series never saw.
        candles = [{"t": d0 + DAY + i * 3600, "o": 1, "h": 1, "l": 1, "c": 1} for i in range(3)]
        candles += [{"t": d0 + 2 * DAY + i * 3600, "o": 1, "h": 1, "l": 1, "c": 1} for i in range(3)]
        out = sc.cpr_overlays(candles, daily, 3600)
        pivots = [o for o in out if o["color"] == sc.CPR_PIVOT]
        self.assertEqual(len(pivots), 2)
        self.assertAlmostEqual(pivots[0]["points"][0]["price"], 100.0)
        self.assertAlmostEqual(pivots[1]["points"][0]["price"], 200.0)
        # The steps land on the UTC day turn.
        self.assertEqual(pivots[1]["points"][0]["t"], d0 + 2 * DAY)
        # Only the newest day labels its lines.
        self.assertEqual(pivots[0]["label"], "")
        self.assertTrue(pivots[1]["label"].startswith("CPR P (200.00)"))
        # ...and runs a little past the last bar, never past the day.
        self.assertGreater(pivots[1]["points"][-1]["t"], candles[-1]["t"])
        self.assertLess(pivots[1]["points"][-1]["t"], d0 + 3 * DAY)

    def test_a_day_without_a_daily_bar_before_it_gets_no_levels(self):
        d0 = 20_000 * DAY
        candles = [{"t": d0 + i * 60, "o": 1, "h": 1, "l": 1, "c": 1} for i in range(3)]
        self.assertEqual(sc.cpr_overlays(candles, [{"t": d0, "o": 1, "h": 2, "l": 0.5, "c": 1}], 60), [])

    def test_prices_keep_the_decimals_a_coin_needs(self):
        self.assertEqual(sc.fmt_price(78123.456), "78,123.46")
        self.assertEqual(sc.fmt_price(2.34567), "2.3457")
        self.assertEqual(sc.fmt_price(0.123456789), "0.123457")


class AlarmBookTests(unittest.TestCase):
    def setUp(self):
        self.saved = {}
        self.now = [1_800_000_000.0]
        self.book = sc.AlarmBook(
            load=lambda: dict(self.saved), save=lambda d: self.saved.update(d), now_fn=lambda: self.now[0]
        )

    def test_direction_is_fixed_when_set(self):
        self.assertEqual(self.book.add("BTCUSDT", 70_000, now_price=75_000)["dir"], "down")
        self.assertEqual(self.book.add("BTCUSDT", 80_000, now_price=75_000)["dir"], "up")

    def test_an_alarm_rings_once(self):
        alarm = self.book.add("BTCUSDT", 80_000, now_price=75_000)
        self.assertEqual(self.book.check({"BTCUSD": 79_999}), [])
        rung = self.book.check({"BTCUSD": 80_001})
        self.assertEqual([r["id"] for r in rung], [alarm["id"]])
        self.assertEqual(self.book.check({"BTCUSD": 81_000}), [])
        self.assertIsNone(self.book.fire(alarm["id"], 81_000))
        listing = self.book.listing()
        self.assertEqual(listing["alarms"], [])
        self.assertEqual([f["id"] for f in listing["fired"]], [alarm["id"]])

    def test_a_down_alarm_rings_at_or_under_its_level(self):
        self.book.add("ETHUSDT", 2000, now_price=2100)
        self.assertEqual(self.book.check({"ETHUSD": 2000.5}), [])
        self.assertEqual(len(self.book.check({"ETHUSD": 2000})), 1)

    def test_a_coin_without_a_price_is_not_judged(self):
        self.book.add("SOLUSDT", 1, now_price=100)
        self.assertEqual(self.book.check({"BTCUSD": 1}), [])
        self.assertEqual(len(self.book.listing()["alarms"]), 1)

    def test_remove_also_silences_a_rung_alarm(self):
        alarm = self.book.add("BTCUSDT", 1, now_price=0)
        self.book.check({"BTCUSD": 2})
        self.assertEqual(self.book.remove(alarm["id"]), 1)
        self.assertEqual(self.book.listing()["fired"], [])

    def test_old_rings_leave_the_listing(self):
        self.book.add("BTCUSDT", 1, now_price=0)
        self.book.check({"BTCUSD": 2})
        self.now[0] += sc.FIRED_SHOW_SEC + 1
        self.assertEqual(self.book.listing()["fired"], [])

    def test_bad_prices_are_refused(self):
        with self.assertRaises(ValueError):
            self.book.add("BTCUSDT", 0)
        with self.assertRaises(ValueError):
            self.book.add("BTCUSDT", float("nan"))

    def test_status_prices_are_read_by_coin_across_brokers(self):
        status = {
            "entry_controls": {"symbol": "BTCUSDT", "price": 80_000},
            "open_trades": [{"symbol": "ETHUSD", "current_price": 2000}, {"symbol": "BTCUSD", "current_price": 1}],
        }
        prices = sc.prices_from_status(status)
        self.assertEqual(prices["BTCUSD"], 80_000)  # the watched price wins
        self.assertEqual(prices["ETHUSD"], 2000)
        self.assertEqual(sc.symbol_key("GOLD"), "PAXGUSD")


class _FakeClient:
    broker_name = "delta"

    def __init__(self, frame, daily):
        self.frame, self.daily, self.calls = frame, daily, []

    def get_candles(self, symbol, resolution="5m", start=None, end=None):
        self.calls.append((symbol, resolution, start))
        return self.daily if resolution == "1d" else self.frame


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.app = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig = (self.app._STATE_DB_FILE, self.app._scalp_broker_client, self.app.alerter.alert)
        self.app._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(quiesce_state_writers, self.app._STATE_DB_FILE)
        self.addCleanup(self._restore)
        self.app._SCALP_CHART_CACHE.clear()
        self.app._scalp_alarm_books.clear()
        self.alerts = []
        self.app.alerter.alert = lambda title, body, level="info": self.alerts.append((title, body))

    def _restore(self):
        self.app._STATE_DB_FILE, self.app._scalp_broker_client, self.app.alerter.alert = self._orig
        self.app._SCALP_CHART_CACHE.clear()
        self.app._scalp_alarm_books.clear()

    def test_chart_route_draws_candles_ema_and_cpr(self):
        d0 = 20_000 * DAY
        rows = [(d0 + DAY + i * 300, 100, 101, 99, 100 + i * 0.01) for i in range(40)]
        client = _FakeClient(_frame(rows), _frame([(d0, 100, 110, 90, 100), (d0 + DAY, 100, 101, 99, 100)]))
        self.app._scalp_broker_client = lambda: client
        payload = asyncio.run(self.app.scalp_chart(symbol="BTCUSDT", timeframe="5m"))
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(len(payload["candles"]), 40)
        groups = {o.get("group") for o in payload["overlays"]}
        self.assertEqual(groups, {"ema", "cpr"})
        self.assertEqual(payload["broker"], "delta")
        # Cached: a second ask within the TTL does not go to the broker.
        asyncio.run(self.app.scalp_chart(symbol="BTCUSDT", timeframe="5m"))
        self.assertEqual(sum(1 for c in client.calls if c[1] == "5m"), 1)

    def test_chart_route_refuses_an_unknown_timeframe(self):
        with self.assertRaises(self.app.HTTPException) as caught:
            asyncio.run(self.app.scalp_chart(symbol="BTCUSDT", timeframe="7m"))
        self.assertEqual(caught.exception.status_code, 422)

    def test_a_status_push_rings_the_alarm(self):
        self.app._scalp_alarms().add("BTCUSDT", 80_000, now_price=75_000)
        self.app._scalp_check_alarms({"entry_controls": {"symbol": "BTCUSDT", "price": 79_000}})
        self.assertEqual(self.alerts, [])
        self.app._scalp_check_alarms({"entry_controls": {"symbol": "BTCUSDT", "price": 80_050}})
        self.app._scalp_check_alarms({"entry_controls": {"symbol": "BTCUSDT", "price": 80_100}})
        self.assertEqual(len(self.alerts), 1)
        self.assertIn("BTCUSDT", self.alerts[0][0])
        self.assertIn("rose to $80,000.00", self.alerts[0][1])

    def test_alarms_survive_a_restart(self):
        alarm = self.app._scalp_alarms().add("ETHUSDT", 2000, now_price=2100)
        self.app._scalp_alarm_books.clear()  # a new process reads the state DB
        ids = [a["id"] for a in self.app._scalp_alarms().listing()["alarms"]]
        self.assertEqual(ids, [alarm["id"]])


if __name__ == "__main__":
    unittest.main()
