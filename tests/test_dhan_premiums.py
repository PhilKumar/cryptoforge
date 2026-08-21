"""Tests for the fixed-strike rebuild on top of Dhan's moneyness-keyed series.

Every test here guards one way the rebuild could quietly lie: filling a hole
with a neighbouring strike, carrying a stale price forward, reading an empty
vendor reply as a zero-volume market, or losing the IST offset.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime

import pytest

from options.dhan_premiums import (
    DhanDataError,
    DhanRollingPremiums,
    _aliases,
    _windows,
)
from options.dhan_vs_upstox import Comparison, compare


def epoch_ist(y, m, d, hh, mm):
    """Dhan stamps UTC; the loader adds 5h30m. Build the UTC stamp for an IST time."""
    utc = datetime(y, m, d, hh, mm) - __import__("datetime").timedelta(hours=5, minutes=30)
    return calendar.timegm(utc.timetuple())


def leg(stamps, strikes, closes, spots=None):
    return {
        "timestamp": list(stamps),
        "strike": list(strikes),
        "close": list(closes),
        "spot": list(spots or [0] * len(stamps)),
    }


def source(replies, **kw):
    """A DhanRollingPremiums whose transport is a scripted list of payloads."""
    src = DhanRollingPremiums("cid", "tok", sleep_between=0, **kw)
    calls = []

    def fake_post(body):
        calls.append(body)
        return replies.pop(0) if replies else {"data": {"ce": {}, "pe": {}}}

    src._post = fake_post
    src.calls = calls
    return src


# ----------------------------------------------------------------- band shape


def test_index_band_is_twenty_one_strikes():
    assert len(_aliases(10)) == 21
    assert _aliases(10)[0] == "ATM"
    assert "ATM+10" in _aliases(10) and "ATM-10" in _aliases(10)


def test_stock_band_is_seven_strikes():
    # Dhan serves stock options at ATM+/-3, a third of the index width.
    assert len(_aliases(3)) == 7


def test_windows_never_exceed_the_vendor_cap():
    spans = list(_windows(date(2021, 1, 1), date(2021, 6, 30)))
    assert all((b - a).days < 30 for a, b in spans)
    assert spans[0][0] == date(2021, 1, 1)
    assert spans[-1][1] == date(2021, 6, 30)
    # contiguous, no overlap and no skipped day
    for (_, prev_end), (next_start, _) in zip(spans, spans[1:]):
        assert (next_start - prev_end).days == 1


# --------------------------------------------------------- the empty-200 trap


def test_empty_reply_is_recorded_as_a_gap_not_a_quiet_market():
    src = source([{"data": {"ce": {}}}])
    src.load("2023-01-01", "2023-01-05", option_type="CE", progress=False)
    assert src.coverage.bars == 0
    assert src.coverage.empty_requests == src.coverage.requests
    assert src.coverage.gaps, "an empty reply must leave a gap record behind"
    assert src.coverage.gaps[0].from_date == "2023-01-01"


def test_http_error_raises_rather_than_returning_empty():
    src = DhanRollingPremiums("cid", "tok", sleep_between=0)

    def boom(body):
        raise DhanDataError("Dhan HTTP 401: token expired")

    src._post = boom
    with pytest.raises(DhanDataError):
        src.load("2023-01-01", "2023-01-05", option_type="CE", progress=False)


# ------------------------------------------------------ fixed-strike rebuild


def test_bars_are_keyed_by_their_own_strike_not_by_the_alias():
    # One ATM request whose underlying strike MOVED mid-window -- the whole
    # reason the rebuild exists.
    stamps = [epoch_ist(2023, 5, 2, 9, 15), epoch_ist(2023, 5, 2, 9, 16)]
    src = source([{"data": {"ce": leg(stamps, [18000, 18050], [120.5, 98.25])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)

    at = datetime(2023, 5, 2, 9, 15)
    nxt = datetime(2023, 5, 2, 9, 16)
    assert src.premium(at, 18000, "CE") == 120.5
    assert src.premium(nxt, 18050, "CE") == 98.25
    # and crucially NOT smeared across both minutes
    assert src.premium(nxt, 18000, "CE") is None
    assert src.premium(at, 18050, "CE") is None


def test_a_strike_outside_the_band_returns_none_and_says_why():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"ce": leg(stamps, [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)

    at = datetime(2023, 5, 2, 9, 15)
    assert src.premium(at, 19500, "CE") is None
    assert src.explain(at, 19500).startswith("strike_outside_band")
    assert src.explain(at, 18000) == "served"


def test_an_unserved_minute_is_distinguishable_from_a_missing_strike():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"ce": leg(stamps, [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    assert src.explain(datetime(2023, 5, 2, 14, 0), 18000) == "no_vendor_data_for_minute"


def test_no_price_is_ever_carried_forward_into_a_hole():
    stamps = [epoch_ist(2023, 5, 2, 9, 15), epoch_ist(2023, 5, 2, 9, 17)]
    src = source([{"data": {"ce": leg(stamps, [18000, 18000], [120.5, 118.0])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    # 09:16 was never served; it must stay empty rather than inherit 09:15
    assert src.premium(datetime(2023, 5, 2, 9, 16), 18000, "CE") is None


def test_a_zero_premium_is_kept_and_not_read_as_missing():
    stamps = [epoch_ist(2023, 5, 2, 15, 29)]
    src = source([{"data": {"ce": leg(stamps, [18000], [0.0])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    assert src.premium(datetime(2023, 5, 2, 15, 29), 18000, "CE") == 0.0


def test_null_rows_are_skipped_rather_than_coerced_to_zero():
    stamps = [epoch_ist(2023, 5, 2, 9, 15), epoch_ist(2023, 5, 2, 9, 16)]
    src = source([{"data": {"ce": leg(stamps, [18000, None], [120.5, None])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    assert src.coverage.bars == 1
    assert src.premium(datetime(2023, 5, 2, 9, 16), 18000, "CE") is None


# ------------------------------------------------------------------ IST clock


def test_stamps_land_on_ist_market_minutes():
    src = source([{"data": {"ce": leg([epoch_ist(2023, 5, 2, 9, 15)], [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    only = next(iter(src.coverage.minutes))
    assert (only.hour, only.minute) == (9, 15), "market opens 09:15 IST, not 03:45 UTC"


def test_millisecond_stamps_are_handled():
    ms = epoch_ist(2023, 5, 2, 9, 15) * 1000
    src = source([{"data": {"ce": leg([ms], [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    assert src.premium(datetime(2023, 5, 2, 9, 15), 18000, "CE") == 120.5


# ------------------------------------------------------------ request shaping


def test_put_requests_read_the_pe_leg():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"pe": leg(stamps, [18000], [88.0])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="PE", progress=False)
    assert src.calls[0]["drvOptionType"] == "PUT"
    assert src.premium(datetime(2023, 5, 2, 9, 15), 18000, "PE") == 88.0


def test_interval_must_be_one_the_vendor_supports():
    with pytest.raises(ValueError):
        DhanRollingPremiums("cid", "tok", interval="3")


def test_lookup_adapter_matches_philforge_signature():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"ce": leg(stamps, [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    lookup = src.as_premium_lookup()
    assert lookup(datetime(2023, 5, 2, 9, 15), 18000, "CE") == 120.5


def test_coverage_counts_hits_and_misses_so_a_run_can_be_judged():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"ce": leg(stamps, [18000], [120.5])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    src.premium(datetime(2023, 5, 2, 9, 15), 18000, "CE")  # hit
    src.premium(datetime(2023, 5, 2, 9, 16), 18000, "CE")  # miss
    assert src.coverage.hits == 1 and src.coverage.misses == 1
    assert src.coverage.hit_rate == 0.5


# ------------------------------------------------------------- the comparison


def test_comparison_refuses_to_bless_a_zero_overlap():
    cmp_ = Comparison()
    cmp_.dhan_missing = 500
    assert cmp_.verdict == "UNUSABLE"
    assert "Do not run a backfill" in cmp_.report()


def test_comparison_blesses_close_agreement():
    cmp_ = Comparison()
    for i in range(100):
        cmp_.add(datetime(2025, 1, 2, 9, 15), 23000, "CE", 100.0, 100.5)
    assert cmp_.verdict.startswith("GOOD")


def test_comparison_flags_holes_even_when_prices_agree():
    cmp_ = Comparison()
    for i in range(100):
        cmp_.add(datetime(2025, 1, 2, 9, 15), 23000, "CE", 100.0, 100.0)
    cmp_.dhan_missing = 60  # agrees perfectly on the third it served
    assert cmp_.verdict.startswith("NOT TRUSTWORTHY")


def test_compare_walks_an_upstox_series_against_a_dhan_source():
    stamps = [epoch_ist(2023, 5, 2, 9, 15)]
    src = source([{"data": {"ce": leg(stamps, [18000], [120.0])}}], band=0)
    src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    ups = {datetime(2023, 5, 2, 9, 15): 121.0, datetime(2023, 5, 2, 9, 16): 119.0}
    cmp_ = compare(ups, src, 18000, "CE")
    assert cmp_.matched == 1 and cmp_.dhan_missing == 1


def test_out_of_band_misses_do_not_condemn_complete_data():
    """Dhan sells ATM+/-10. A strike 12 out is absent by design, and counting it
    as a hole marks an archive untrustworthy when it is in fact complete."""
    cmp_ = Comparison()
    for _ in range(100):
        cmp_.add(datetime(2025, 1, 2, 9, 15), 23000, "CE", 100.0, 100.0)
    cmp_.dhan_missing = 60
    cmp_.out_of_band = 60  # every miss was outside the band
    assert cmp_.in_band_served == 1.0
    assert cmp_.verdict.startswith("GOOD")


def test_holes_inside_the_band_still_condemn():
    cmp_ = Comparison()
    for _ in range(100):
        cmp_.add(datetime(2025, 1, 2, 9, 15), 23000, "CE", 100.0, 100.0)
    cmp_.dhan_missing = 60
    cmp_.out_of_band = 10  # 50 real holes at strikes Dhan should serve
    assert cmp_.verdict.startswith("NOT TRUSTWORTHY")


def test_a_gateway_timeout_is_retried_not_fatal():
    """A five-year pull is thousands of calls; one 504 must not end it."""
    import options.dhan_premiums as dp

    src = DhanRollingPremiums("cid", "tok", sleep_between=0, band=0)
    src.MAX_ATTEMPTS = 3
    calls = []
    good = {"data": {"ce": leg([epoch_ist(2023, 5, 2, 9, 15)], [18000], [120.5])}}

    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._b, self.text = code, body, "gateway timeout"

        def json(self):
            return self._b

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append(1)
        return Resp(504) if len(calls) < 3 else Resp(200, good)

    real_post, real_sleep = dp.requests.post, dp.time.sleep
    dp.requests.post, dp.time.sleep = fake_post, lambda *_: None
    try:
        src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    finally:
        dp.requests.post, dp.time.sleep = real_post, real_sleep

    assert len(calls) == 3, "should have retried twice then succeeded"
    assert src.premium(datetime(2023, 5, 2, 9, 15), 18000, "CE") == 120.5
    assert src.coverage.retries == 2


def test_a_refusal_is_not_retried():
    """A dead token refuses just as firmly the tenth time; retrying it only
    delays the error and burns the rate budget."""
    import options.dhan_premiums as dp

    src = DhanRollingPremiums("cid", "tok", sleep_between=0, band=0)
    calls = []

    class Resp:
        status_code, text = 401, '{"errorCode":"DH-901"}'

        def json(self):
            return {}

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append(1)
        return Resp()

    real_post = dp.requests.post
    dp.requests.post = fake_post
    try:
        with pytest.raises(DhanDataError):
            src.load("2023-05-02", "2023-05-02", option_type="CE", progress=False)
    finally:
        dp.requests.post = real_post
    assert len(calls) == 1, "a 401 must fail on the first call"


def test_listed_source_does_not_substitute_a_neighbouring_minute_by_default():
    """A price borrowed from a nearby minute measured a median 2.55% from the
    Upstox archive against 0.076% for the minute actually asked for, and the
    caller already does its own forward/back search. Silence is the honest
    answer here."""
    from datetime import date

    from options.dhan_listed import DhanListedSource

    class Contract:
        expiry, strike, option_type = date(2021, 1, 7), 14000, "CE"

    src = DhanListedSource([date(2021, 1, 7)], {})
    assert src.nearest_within == 0
    # no stores at all -> the contract is out of reach, and nothing is invented
    assert src.lookup(datetime(2021, 1, 4, 9, 20), Contract()) is None
    assert src.served == 0
