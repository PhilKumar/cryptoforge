"""
The snapshot the public landing reads, written by _write_landing_ledger().

What this guards is a bug that reached production on 2026-08-17. A parameter
carrying the closed rounds was added to _write_landing_ledger() and named
`trades` — but the function body already binds a local `trades` to an int:

    trades = int(summary.get("trade_count") or 0)

so by the time the rounds were used, the name held a count. Every call to
/api/journal/trades raised `TypeError: 'int' object is not iterable` and the
private journal page came up empty.

The helper had been unit-tested on its own and passed; nothing exercised the
writer that calls it. So these tests call _write_landing_ledger() the way
journal_trades() does — positionally, with the real round dicts — rather than
testing _landing_recent_rounds() in isolation.
"""

import importlib
import json
import os
import tempfile
import unittest

app = importlib.import_module("app")

_MS = 1755450000000  # a fixed IST instant; the clock must not enter these tests


def _round(coin, *, opened_min, closed_min, invested, fees, roi, fee_asset="USDT", **extra):
    row = {
        "status": "Closed",
        "coin": coin,
        "opened_ts": _MS + opened_min * 60000,
        "closed_ts": _MS + closed_min * 60000,
        "invested_usd": invested,
        "fees_usd": fees,
        "roi_pct": roi,
        "fee_assets": {fee_asset: fees or 0.0004},
    }
    row.update(extra)
    return row


_SUMMARY = {
    "equity_curve": [{"date": "2026-08-17", "pnl": 1.2, "cumulative_pnl": 1.2}],
    "trade_count": 3,
    "win_count": 2,
    "loss_count": 1,
    "realized_pnl_usd": 1.65,
    "fees_usd": 0.045,
}


class LandingLedgerWriterTests(unittest.TestCase):
    def setUp(self):
        self._real_path = app._LANDING_LEDGER_FILE
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(self.path)
        app._LANDING_LEDGER_FILE = self.path
        self.addCleanup(setattr, app, "_LANDING_LEDGER_FILE", self._real_path)
        self.addCleanup(lambda: os.path.exists(self.path) and os.unlink(self.path))

    def write(self, rounds, summary=None, capital=200.0):
        app._write_landing_ledger(dict(summary or _SUMMARY), capital, rounds)
        with open(self.path, encoding="utf-8") as handle:
            return json.load(handle)

    def test_the_count_and_the_rounds_are_both_published(self):
        """The regression itself: `trades` stays the int, `recent` is the list."""
        payload = self.write([_round("BTCUSDT", opened_min=-48, closed_min=0, invested=20.0, fees=0.02, roi=-0.31)])
        self.assertEqual(payload["trades"], 3)
        self.assertIsInstance(payload["trades"], int)
        self.assertEqual(len(payload["recent"]), 1)

    def test_no_round_carries_the_coin_it_traded(self):
        """The record is public; the coin selection is not."""
        payload = self.write(
            [
                _round("BTCUSDT", opened_min=-48, closed_min=0, invested=20.0, fees=0.02, roi=-0.31),
                _round("ETHUSDT", opened_min=60, closed_min=280, invested=25.0, fees=0.025, roi=0.84),
            ]
        )
        blob = json.dumps(payload["recent"])
        for leak in ("BTC", "ETH", "USDT", "SOL", "BNB"):
            self.assertNotIn(leak, blob)

    def test_one_coin_keeps_one_label(self):
        payload = self.write(
            [
                _round("ETHUSDT", opened_min=60, closed_min=280, invested=25.0, fees=0.025, roi=0.84),
                _round("ETHUSDT", opened_min=400, closed_min=465, invested=22.0, fees=0.016, roi=-0.42),
                _round("BTCUSDT", opened_min=-48, closed_min=0, invested=20.0, fees=0.02, roi=-0.31),
            ]
        )
        by_result = {r["result_pct"]: r["pair"] for r in payload["recent"]}
        self.assertEqual(by_result[0.84], by_result[-0.42])
        self.assertNotEqual(by_result[0.84], by_result[-0.31])

    def test_a_fee_paid_in_bnb_is_null_and_never_zero(self):
        """0.00% under "fees are shown as what they were" would be the one lie."""
        payload = self.write(
            [_round("BNBUSDT", opened_min=10, closed_min=382, invested=18.0, fees=0.0, roi=1.12, fee_asset="BNB")]
        )
        self.assertIsNone(payload["recent"][0]["fee_pct"])

    def test_open_rounds_and_fee_floats_are_left_out(self):
        payload = self.write(
            [
                _round("BTCUSDT", opened_min=-48, closed_min=0, invested=20.0, fees=0.02, roi=-0.31),
                _round("SOLUSDT", opened_min=0, closed_min=5, invested=10.0, fees=0.01, roi=0.0, status="Open"),
                _round("BNBUSDT", opened_min=0, closed_min=1, invested=5.0, fees=0.0, roi=0.0, kind="fee_float"),
            ]
        )
        self.assertEqual(len(payload["recent"]), 1)

    def test_newest_round_first(self):
        payload = self.write(
            [
                _round("BTCUSDT", opened_min=-48, closed_min=0, invested=20.0, fees=0.02, roi=-0.31),
                _round("ETHUSDT", opened_min=400, closed_min=465, invested=22.0, fees=0.016, roi=-0.42),
            ]
        )
        self.assertEqual(payload["recent"][0]["result_pct"], -0.42)

    def test_the_rounds_are_optional(self):
        """journal_trades is not the only shape this may ever be called in."""
        app._write_landing_ledger(dict(_SUMMARY), 200.0)
        with open(self.path, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle)["recent"], [])

    def test_nothing_is_written_when_there_is_nothing_real(self):
        """The panel stays off rather than publishing zeros."""
        app._write_landing_ledger({"equity_curve": [], "trade_count": 0}, 200.0, [])
        self.assertFalse(os.path.exists(self.path))


if __name__ == "__main__":
    unittest.main()
