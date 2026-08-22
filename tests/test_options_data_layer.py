"""The failure this data layer exists to prevent.

The previous options book was built on an archive whose vendor answered HTTP
200 with an empty candle array for contracts it did not hold. Read the obvious
way, that is indistinguishable from a session where nothing traded — so the
backtest ran on a fraction of its intended universe, raised nothing, and
produced a number that looked like a result.

The load-bearing test here is `test_empty_200_is_never_read_as_a_quiet_market`.
Everything else supports it.
"""

import importlib.util
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from options.audit import SESSION_COMPLETE_THRESHOLD, audit, format_report
from options.charges import RATE_TABLE, rates_for, round_trip_charges
from options.dhan_client import (
    DhanClient,
    DhanConfigError,
    DhanContractError,
    FetchStatus,
    iter_windows,
)
from options.store import OptionStore, SeriesKey

# The store writes parquet, which pandas cannot do without pyarrow. That is a
# test-time dependency only -- the app never imports this package -- so it is
# not in requirements.txt. Without it these skip and say so, rather than
# failing with an ImportError that looks like a broken store.
HAS_PARQUET = importlib.util.find_spec("pyarrow") is not None
NEEDS_PARQUET = unittest.skipUnless(HAS_PARQUET, "pyarrow not installed")


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeSession:
    """Replays queued responses and records what was asked for."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.requests.append(json)
        return self._responses.pop(0)


def make_client(responses):
    return DhanClient(
        client_id="X",
        access_token="Y",
        session=FakeSession(responses),
        min_interval_s=0.0,
        sleep=lambda s: None,
    )


def candle_payload(n=3, start="2025-01-02T09:15:00"):
    base = datetime.fromisoformat(start)
    return {
        "timestamp": [(base + timedelta(minutes=i)).isoformat() for i in range(n)],
        "open": [100.0 + i for i in range(n)],
        "high": [101.0 + i for i in range(n)],
        "low": [99.0 + i for i in range(n)],
        "close": [100.5 + i for i in range(n)],
        "volume": [1000 + i for i in range(n)],
        "OI": [50000 + i for i in range(n)],
        "IV": [14.2 + i for i in range(n)],
        "spot": [23500.0 + i for i in range(n)],
    }


class TestEmptyResponseHandling(unittest.TestCase):
    def test_empty_200_is_never_read_as_a_quiet_market(self):
        """The whole point. A 200 with no candles must surface as NO_DATA —
        a distinct, inspectable outcome — and must yield no bars at all."""
        client = make_client([FakeResponse(200, {"timestamp": [], "open": []})])
        result = client.fetch_expired_option_window(
            security_id="13",
            exchange_segment="NSE_FNO",
            instrument="OPTIDX",
            expiry_flag="WEEK",
            expiry_code=0,
            strike_offset=0,
            option_type="CALL",
            from_date="2025-01-01",
            to_date="2025-01-20",
        )
        self.assertIs(result.status, FetchStatus.NO_DATA)
        self.assertEqual(result.bars, [])
        self.assertFalse(result.ok)
        # and it must not be mistakable for a real, quiet series
        self.assertNotEqual(result.status, FetchStatus.OK)

    def test_populated_response_parses_every_field(self):
        client = make_client([FakeResponse(200, candle_payload(3))])
        result = client.fetch_expired_option_window(
            security_id="13",
            exchange_segment="NSE_FNO",
            instrument="OPTIDX",
            expiry_flag="WEEK",
            expiry_code=0,
            strike_offset=-2,
            option_type="PUT",
            from_date="2025-01-01",
            to_date="2025-01-20",
        )
        self.assertIs(result.status, FetchStatus.OK)
        self.assertEqual(len(result.bars), 3)
        bar = result.bars[0]
        self.assertEqual(bar.close, 100.5)
        self.assertEqual(bar.oi, 50000)
        self.assertAlmostEqual(bar.iv, 14.2)
        self.assertEqual(bar.spot, 23500.0)

    def test_ragged_columns_raise_rather_than_coerce(self):
        """A misaligned payload during a long backfill is precisely when
        guessing produces a plausible, wrong archive."""
        bad = candle_payload(3)
        bad["close"] = [1.0, 2.0]
        client = make_client([FakeResponse(200, bad)])
        with self.assertRaises(DhanContractError):
            client.fetch_expired_option_window(
                security_id="13",
                exchange_segment="NSE_FNO",
                instrument="OPTIDX",
                expiry_flag="WEEK",
                expiry_code=0,
                strike_offset=0,
                option_type="CALL",
                from_date="2025-01-01",
                to_date="2025-01-20",
            )

    def test_auth_failure_is_not_retried(self):
        session = FakeSession([FakeResponse(401, text="bad token")])
        client = DhanClient("X", "Y", session=session, min_interval_s=0.0, sleep=lambda s: None)
        with self.assertRaises(DhanConfigError):
            client.fetch_expired_option_window(
                security_id="13",
                exchange_segment="NSE_FNO",
                instrument="OPTIDX",
                expiry_flag="WEEK",
                expiry_code=0,
                strike_offset=0,
                option_type="CALL",
                from_date="2025-01-01",
                to_date="2025-01-20",
            )
        self.assertEqual(len(session.requests), 1)


class TestDocumentedLimits(unittest.TestCase):
    def test_strike_beyond_atm_10_is_refused(self):
        """Dhan covers ATM +/-10. A strategy needing wings cannot be tested
        here, and should be told so rather than handed a silent gap."""
        client = make_client([])
        with self.assertRaises(DhanConfigError):
            client.fetch_expired_option_window(
                security_id="13",
                exchange_segment="NSE_FNO",
                instrument="OPTIDX",
                expiry_flag="WEEK",
                expiry_code=0,
                strike_offset=15,
                option_type="CALL",
                from_date="2025-01-01",
                to_date="2025-01-20",
            )

    def test_window_longer_than_30_days_is_refused(self):
        client = make_client([])
        with self.assertRaises(DhanConfigError):
            client.fetch_expired_option_window(
                security_id="13",
                exchange_segment="NSE_FNO",
                instrument="OPTIDX",
                expiry_flag="WEEK",
                expiry_code=0,
                strike_offset=0,
                option_type="CALL",
                from_date="2025-01-01",
                to_date="2025-03-01",
            )

    def test_windows_tile_the_range_without_gap_or_overlap(self):
        windows = list(iter_windows("2025-01-01", "2025-03-31"))
        self.assertEqual(windows[0][0], date(2025, 1, 1))
        self.assertEqual(windows[-1][1], date(2025, 3, 31))
        for (_, prev_end), (next_start, _) in zip(windows, windows[1:]):
            self.assertEqual(next_start - prev_end, timedelta(days=1))
        for start, end in windows:
            self.assertLessEqual((end - start).days + 1, 30)


@NEEDS_PARQUET
class TestStoreAndAudit(unittest.TestCase):
    def _store_with(self, tmp, sessions_spec):
        """sessions_spec: list of (strike_offset, day, bars_present)."""
        store = OptionStore(tmp)
        for offset, day, n in sessions_spec:
            if n == 0:
                continue
            key = SeriesKey("NIFTY", "WEEK", 0, offset, "CALL", "1")
            base = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=15)

            class B:
                pass

            bars = []
            for i in range(n):
                b = B()
                b.ts = base + timedelta(minutes=i)
                b.open = b.high = b.low = b.close = 100.0
                b.volume, b.oi, b.iv, b.spot = 10.0, 100.0, 12.0, 23000.0
                bars.append(b)
            store.write_bars(key, bars)
        return store

    def test_full_sessions_pass_the_audit(self):
        with TemporaryDirectory() as tmp:
            store = self._store_with(tmp, [(0, date(2025, 1, 2), 375), (1, date(2025, 1, 2), 375)])
            verdict = audit(store.load_bars(), store.load_coverage())
            self.assertTrue(verdict.ok())
            self.assertEqual(verdict.usable_from, "2025-01")
            self.assertGreaterEqual(verdict.atm_completeness, SESSION_COMPLETE_THRESHOLD)

    def test_hollow_atm_fails_the_audit(self):
        """The Upstox signature: rows exist, but the strikes that carry the
        P&L are nearly empty. This must not read as a usable archive."""
        with TemporaryDirectory() as tmp:
            store = self._store_with(
                tmp,
                [(0, date(2025, 1, 2), 12), (1, date(2025, 1, 2), 9), (8, date(2025, 1, 2), 375)],
            )
            verdict = audit(store.load_bars(), store.load_coverage())
            self.assertFalse(verdict.ok())
            self.assertLess(verdict.atm_completeness, SESSION_COMPLETE_THRESHOLD)
            self.assertTrue(any("near-ATM" in n for n in verdict.notes))

    def test_coverage_ledger_records_the_absences(self):
        with TemporaryDirectory() as tmp:
            store = OptionStore(tmp)
            store.write_coverage(
                [
                    {
                        "requested_at": datetime.now(),
                        "underlying": "NIFTY",
                        "expiry_flag": "WEEK",
                        "expiry_code": 0,
                        "strike_offset": 0,
                        "option_type": "CALL",
                        "interval": "1",
                        "from_date": "2022-01-01",
                        "to_date": "2022-01-30",
                        "status": "no_data",
                        "http_status": 200,
                        "bars_returned": 0,
                        "detail": "empty",
                    }
                ]
            )
            verdict = audit(store.load_bars(), store.load_coverage())
            self.assertEqual(verdict.silent_empty_requests, 1)
            self.assertFalse(verdict.ok())

    def test_backfill_resumes_from_the_ledger(self):
        with TemporaryDirectory() as tmp:
            store = OptionStore(tmp)
            store.write_coverage(
                [
                    {
                        "requested_at": datetime.now(),
                        "underlying": "NIFTY",
                        "expiry_flag": "WEEK",
                        "expiry_code": 0,
                        "strike_offset": 0,
                        "option_type": "CALL",
                        "interval": "1",
                        "from_date": "2025-01-01",
                        "to_date": "2025-01-30",
                        "status": "ok",
                        "http_status": 200,
                        "bars_returned": 375,
                        "detail": "",
                    }
                ]
            )
            done = store.completed_windows()
            self.assertIn(("NIFTY", "WEEK", 0, 0, "CALL", "1", "2025-01-01", "2025-01-30"), done)

    def test_report_renders_on_an_empty_store(self):
        with TemporaryDirectory() as tmp:
            store = OptionStore(tmp)
            text = format_report(store.load_bars(), store.load_coverage())
            self.assertIn("NOT SAFE TO BACKTEST ON", text)


class TestCharges(unittest.TestCase):
    def test_stt_rate_changes_across_the_backtest_window(self):
        """A five-year book priced at one rate is wrong, and wrong in a way
        that scales with trade count."""
        before = rates_for(date(2024, 9, 30))
        after = rates_for(date(2024, 10, 1))
        self.assertLess(before.stt_sell_pct, after.stt_sell_pct)

    def test_charges_are_on_premium_turnover_not_notional(self):
        c = round_trip_charges(
            trade_date=date(2025, 1, 2),
            buy_premium=100.0,
            sell_premium=120.0,
            quantity=75,
        )
        # STT is sell-side only: 0.1% of 120 * 75
        self.assertAlmostEqual(c.stt, 120.0 * 75 * 0.001, places=4)
        # stamp duty is buy-side only
        self.assertAlmostEqual(c.stamp_duty, 100.0 * 75 * 0.00003, places=4)
        self.assertGreater(c.total, 0)

    def test_undated_era_stops_the_backtest(self):
        with self.assertRaises(ValueError):
            rates_for(date(2019, 1, 1))

    def test_rate_table_is_ordered(self):
        dates = [r.effective_from for r in RATE_TABLE]
        self.assertEqual(dates, sorted(dates))


if __name__ == "__main__":
    unittest.main()


class TestExternalArchiveAdapter(unittest.TestCase):
    """The audit has to judge the archive that already exists, not only the one
    this package writes — otherwise the Upstox question stays unanswered."""

    def _upstox_shaped_csv(self, tmp, *, with_spot, atm_bars, wing_bars):
        """An archive in someone else's shape: real strikes, their column names."""
        rows = []
        base = datetime(2025, 1, 2, 9, 15)
        for strike, n in ((23500, atm_bars), (23550, atm_bars), (24000, wing_bars)):
            for i in range(n):
                row = {
                    "candle_time": (base + timedelta(minutes=i)).isoformat(),
                    "strike_price": strike,
                    "instrument_type": "CE",
                    "open_price": 100.0,
                    "high_price": 101.0,
                    "low_price": 99.0,
                    "close_price": 100.5,
                    "traded_qty": 5000 if strike in (23500, 23550) else 10,
                    "open_interest": 1000,
                }
                if with_spot:
                    row["underlying_price"] = 23520.0
                rows.append(row)
        path = Path(tmp) / "upstox.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_foreign_column_names_are_recognised(self):
        from options.adapters import AtmBasis, load_external

        with TemporaryDirectory() as tmp:
            path = self._upstox_shaped_csv(tmp, with_spot=True, atm_bars=375, wing_bars=20)
            res = load_external(path, underlying_name="NIFTY")
            self.assertEqual(res.atm_basis, AtmBasis.EXACT)
            self.assertGreater(res.rows_out, 0)
            self.assertEqual(res.frame["underlying"].iloc[0], "NIFTY")
            self.assertEqual(res.frame["option_type"].iloc[0], "CALL")

    def test_hollow_atm_in_a_foreign_archive_is_caught(self):
        """The whole point of the adapter: run the ATM cut on their data and
        have it fail when the middle of the chain is empty."""
        from options.adapters import load_external

        with TemporaryDirectory() as tmp:
            path = self._upstox_shaped_csv(tmp, with_spot=True, atm_bars=8, wing_bars=375)
            res = load_external(path, underlying_name="NIFTY")
            verdict = audit(res.frame, pd.DataFrame())
            self.assertFalse(verdict.ok())
            self.assertTrue(any("near-ATM" in n for n in verdict.notes))

    def test_missing_spot_falls_back_to_a_labelled_estimate(self):
        from options.adapters import AtmBasis, load_external

        with TemporaryDirectory() as tmp:
            path = self._upstox_shaped_csv(tmp, with_spot=False, atm_bars=375, wing_bars=20)
            res = load_external(path, underlying_name="NIFTY")
            self.assertEqual(res.atm_basis, AtmBasis.INFERRED)
            self.assertIn("INFERRED", res.caveat())

    def test_missing_strike_column_refuses_rather_than_guesses(self):
        from options.adapters import load_external

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "nostrike.csv"
            pd.DataFrame([{"candle_time": "2025-01-02T09:15:00", "close_price": 1.0}]).to_csv(path, index=False)
            with self.assertRaises(ValueError):
                load_external(path)
