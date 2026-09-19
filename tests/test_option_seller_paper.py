"""The 4 PM option seller, paper only.

Driven against a fake Delta (public endpoints only) and a fake clock, so these
say what the ENGINE decides: whether it trades, which side, when it stops,
what it books — and that it cannot place an order at all.
"""

import asyncio
import datetime as dt
import math
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

from engine import option_seller_paper as osp  # noqa: E402

DAY = dt.date(2026, 9, 18)
ENTRY = dt.datetime(2026, 9, 18, 10, 30, tzinfo=dt.timezone.utc).timestamp()
SETTLE_ISO = "2026-09-18T12:00:00Z"


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


class FakeDelta:
    """Public Delta: index candles, today's products, tickers, mark candles."""

    def __init__(self, p0=100_000.0, moves=None, marks=None):
        # moves: lookback -> percent move up to 16:00 (positive = BTC rose)
        self.p0 = p0
        self.moves = moves if moves is not None else {lb: 2.0 for lb in osp.LOOKBACKS_MIN}
        self.marks = list(marks or [500.0])  # successive mark prices returned
        self.mark_candles = []
        self.calls = []
        self.fail_index = False
        self.fail_ticker = False

    async def __call__(self, path, params):
        self.calls.append((path, dict(params)))
        if path == "/history/candles" and params["symbol"] == osp.UNDERLYING:
            if self.fail_index:
                raise RuntimeError("index down")
            rows = [{"time": int(ENTRY), "open": self.p0, "high": self.p0, "low": self.p0, "close": self.p0}]
            for lb, mv in self.moves.items():
                ref = self.p0 / (1 + mv / 100.0)
                rows.append({"time": int(ENTRY) - lb * 60, "open": ref, "high": ref, "low": ref, "close": ref})
            return {"result": rows}
        if path == "/history/candles":
            return {"result": self.mark_candles}
        if path == "/products":
            kind = "C" if params["contract_types"] == "call_options" else "P"
            rows = []
            for k in (99_000, 100_000, 101_000):
                rows.append(
                    {
                        "symbol": f"{kind}-BTC-{k}-180926",
                        "strike_price": str(k),
                        "initial_margin": "0.5",
                        "settlement_time": SETTLE_ISO,
                        "underlying_asset": {"symbol": "BTC"},
                    }
                )
            rows.append(
                {
                    "symbol": f"{kind}-BTC-100000-190926",
                    "strike_price": "100000",
                    "settlement_time": "2026-09-19T12:00:00Z",
                    "underlying_asset": {"symbol": "BTC"},
                }
            )
            rows.append(
                {
                    "symbol": f"{kind}-ETH-3000-180926",
                    "strike_price": "3000",
                    "settlement_time": SETTLE_ISO,
                    "underlying_asset": {"symbol": "ETH"},
                }
            )
            return {"result": rows, "meta": {"after": None}}
        if path.startswith("/tickers/"):
            if self.fail_ticker:
                raise RuntimeError("ticker down")
            mark = self.marks.pop(0) if len(self.marks) > 1 else self.marks[0]
            return {
                "result": {
                    "mark_price": str(mark),
                    "spot_price": str(self.p0),
                    "quotes": {"best_bid": str(mark - 5), "best_ask": str(mark + 5)},
                }
            }
        raise AssertionError(f"unexpected call {path}")


def run(coro):
    return asyncio.run(coro)


def seller(delta, t=ENTRY + 30, enabled=True):
    clock = Clock(t)
    s = osp.OptionSellerPaper(fetch_json=delta, clock=clock)
    s.enabled = enabled
    return s, clock


# ── the decision ──────────────────────────────────────────────────


def test_thresholds_grow_with_the_window():
    assert osp.threshold_pct(360) == pytest.approx(0.75)
    assert osp.threshold_pct(120) == pytest.approx(0.75 * math.sqrt(1 / 3))
    assert osp.threshold_pct(720) == pytest.approx(0.75 * math.sqrt(2))


def test_five_of_six_up_sells_a_call():
    d = osp.count_votes(100.0, {lb: 100 / 1.02 for lb in osp.LOOKBACKS_MIN})
    assert d["votes"] == {"C": 6, "P": 0} and d["side"] == "C"


def test_four_votes_is_not_enough():
    refs = {lb: 100 / 1.02 for lb in osp.LOOKBACKS_MIN}
    refs[120] = refs[180] = 100.0  # two windows flat
    d = osp.count_votes(100.0, refs)
    assert d["votes"]["C"] == 4 and d["side"] == ""


def test_a_small_move_on_a_wide_window_does_not_vote():
    """0.9% over 12h is below 0.75 x sqrt(2) = 1.06%."""
    d = osp.count_votes(100.0, {720: 100 / 1.009})
    assert d["votes"] == {"C": 0, "P": 0}


def test_down_moves_sell_a_put():
    d = osp.count_votes(100.0, {lb: 100 / 0.98 for lb in osp.LOOKBACKS_MIN})
    assert d["side"] == "P"


# ── a day ─────────────────────────────────────────────────────────


def test_nothing_happens_before_four_pm():
    delta = FakeDelta()
    s, _ = seller(delta, t=ENTRY - 60)
    assert run(s.tick()) is False
    assert s.days == {} and delta.calls == []


def test_an_off_book_never_trades():
    delta = FakeDelta()
    s, _ = seller(delta, enabled=False)
    assert run(s.tick()) is False
    assert s.days == {} and delta.calls == []


def test_agreement_opens_one_atm_call_of_todays_expiry():
    delta = FakeDelta(marks=[500.0])
    s, _ = seller(delta)
    assert run(s.tick()) is True
    rec = s.days[DAY.isoformat()]
    assert rec["status"] == "open"
    assert rec["symbol"] == "C-BTC-100000-180926", "must be ATM AND today's expiry, never tomorrow's"
    assert rec["entry_mark"] == 500.0 and rec["stop_px"] == 1000.0
    assert rec["entry_bid"] == 495.0
    assert rec["contracts"] == 100 and rec["size_btc"] == 0.1


def test_no_agreement_is_a_recorded_skip():
    moves = {lb: 0.1 for lb in osp.LOOKBACKS_MIN}
    s, _ = seller(FakeDelta(moves=moves))
    assert run(s.tick()) is True
    rec = s.days[DAY.isoformat()]
    assert rec["status"] == "skipped" and rec["side"] == ""
    assert "no agreement" in rec["reason"]


def test_one_decision_per_day():
    delta = FakeDelta(moves={lb: 0.1 for lb in osp.LOOKBACKS_MIN})
    s, clock = seller(delta)
    run(s.tick())
    calls = len(delta.calls)
    clock.t += 60
    assert run(s.tick()) is False
    assert len(delta.calls) == calls, "a decided day must not be decided again"


def test_waits_for_the_four_pm_candle():
    delta = FakeDelta()
    s, _ = seller(delta)

    async def no_entry_bar(path, params):
        out = await delta(path, params)
        if path == "/history/candles":
            out["result"] = [r for r in out["result"] if r["time"] != int(ENTRY)]
        return out

    s._fetch = no_entry_bar
    assert run(s.tick()) is False
    assert s.days == {}, "no decision may be made before the 16:00 price exists"


def test_switched_on_late_is_a_missed_day_not_a_late_trade():
    delta = FakeDelta()
    s, _ = seller(delta, t=ENTRY + osp.ENTRY_GRACE_SEC + 1)
    assert run(s.tick()) is True
    assert s.days[DAY.isoformat()]["status"] == "missed"
    assert not any(p == "/products" for p, _ in delta.calls), "a late start must not trade a decayed premium"


def test_an_unreadable_index_retries_rather_than_deciding():
    delta = FakeDelta()
    delta.fail_index = True
    s, _ = seller(delta)
    assert run(s.tick()) is False
    assert s.days == {}


def test_incomplete_history_is_no_trade():
    moves = {lb: 2.0 for lb in osp.LOOKBACKS_MIN if lb != 720}
    s, _ = seller(FakeDelta(moves=moves))

    async def trimmed(path, params):
        out = await FakeDelta(moves=moves)(path, params)
        if path == "/history/candles":
            # nothing at or before the 12h window start
            out["result"] = [r for r in out["result"] if r["time"] > int(ENTRY) - 720 * 60]
        return out

    s._fetch = trimmed
    run(s.tick())
    assert s.days[DAY.isoformat()]["status"] == "skipped"


# ── managing the position ─────────────────────────────────────────


def test_the_stop_fires_when_the_mark_doubles():
    delta = FakeDelta(marks=[500.0, 700.0, 1001.0])
    s, clock = seller(delta)
    run(s.tick())  # open at 500
    clock.t += 15
    assert run(s.tick()) is True  # 700: still open, and the new price is saved
    assert s.days[DAY.isoformat()]["status"] == "open"
    clock.t += 15
    assert run(s.tick()) is True  # 1001: stopped
    rec = s.days[DAY.isoformat()]
    assert rec["status"] == "closed" and rec["exit_why"] == "stop"
    fees = osp.fee_per_btc(500.0, 100_000.0) + osp.fee_per_btc(1001.0, 100_000.0)
    assert rec["pnl_per_btc_mark"] == pytest.approx(500.0 - 1001.0 - fees)
    assert rec["pnl_usd_mark"] == pytest.approx((500.0 - 1001.0 - fees) * 0.1)


def test_time_exit_at_1725_ist_books_the_decay():
    delta = FakeDelta(marks=[500.0, 120.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t = dt.datetime(2026, 9, 18, 11, 55, tzinfo=dt.timezone.utc).timestamp()
    assert run(s.tick()) is True
    rec = s.days[DAY.isoformat()]
    assert rec["exit_why"] == "time"
    fees = osp.fee_per_btc(500.0, 100_000.0) + osp.fee_per_btc(120.0, 100_000.0)
    assert rec["pnl_per_btc_mark"] == pytest.approx(380.0 - fees)
    # the real-quote twin: sold at the bid 495, bought back at the ask 125
    q_fees = osp.fee_per_btc(495.0, 100_000.0) + osp.fee_per_btc(125.0, 100_000.0)
    assert rec["pnl_per_btc_quote"] == pytest.approx(495.0 - 125.0 - q_fees)
    assert rec["pnl_per_btc_quote"] < rec["pnl_per_btc_mark"], "the spread must cost something"


def test_a_stop_the_poll_missed_is_flagged():
    delta = FakeDelta(marks=[500.0, 120.0])
    delta.mark_candles = [{"time": int(ENTRY) + 600, "high": 1_050.0}]
    s, clock = seller(delta)
    run(s.tick())
    clock.t = dt.datetime(2026, 9, 18, 11, 55, tzinfo=dt.timezone.utc).timestamp()
    run(s.tick())
    rec = s.days[DAY.isoformat()]
    assert rec["exit_why"] == "time" and rec["stop_touched_by_candle"] is True
    assert s.status()["totals"]["poll_missed_stops"] == 1


def test_switching_off_does_not_abandon_an_open_position():
    delta = FakeDelta(marks=[500.0, 1200.0])
    s, clock = seller(delta)
    run(s.tick())
    s.configure(enabled=False)
    clock.t += 15
    assert run(s.tick()) is True
    assert s.days[DAY.isoformat()]["exit_why"] == "stop"


def test_a_position_left_open_across_midnight_is_still_closed():
    delta = FakeDelta(marks=[500.0])
    s, clock = seller(delta)
    run(s.tick())
    delta.fail_ticker = True
    clock.t = dt.datetime(2026, 9, 19, 0, 5, tzinfo=dt.timezone.utc).timestamp()
    assert run(s.tick()) is True
    rec = s.days[DAY.isoformat()]
    assert rec["status"] == "closed" and "last mark" in rec["note"]


def test_a_failed_quote_before_settlement_just_retries():
    delta = FakeDelta(marks=[500.0])
    s, clock = seller(delta)
    run(s.tick())
    delta.fail_ticker = True
    clock.t += 15
    assert run(s.tick()) is False
    assert s.days[DAY.isoformat()]["status"] == "open"


# ── what the page shows about a position (18-Sep-2026) ───────────
# "I want to know open positions, current price or P&L change, how much
# capital used and a chart" — the page showed the 16:00 price all afternoon,
# because a check that did not close the trade was never saved.


def test_every_check_saves_the_live_price_so_the_page_sees_it():
    delta = FakeDelta(marks=[500.0, 430.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t += 15
    assert run(s.tick()) is True, "a new price must be written, or the page never sees it"
    reloaded = osp.OptionSellerPaper(fetch_json=delta, clock=clock)
    reloaded.load(s.dump())
    rec = reloaded.days[DAY.isoformat()]
    assert rec["last_mark"] == 430.0 and rec["last_bid"] == 425.0 and rec["last_ask"] == 435.0
    assert rec["last_seen_ts"] == clock.t


def test_the_open_position_shows_pnl_capital_and_risk():
    delta = FakeDelta(marks=[500.0, 400.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t += 60
    run(s.tick())
    view = s.status()["open"]["view"]
    spot, size = 100_000.0, 0.1
    fees = osp.fee_per_btc(500.0, spot) + osp.fee_per_btc(400.0, spot)
    assert view["pnl_usd_mark_now"] == pytest.approx(round((500.0 - 400.0 - fees) * size, 2))
    q_fees = osp.fee_per_btc(495.0, spot) + osp.fee_per_btc(405.0, spot)
    assert view["pnl_usd_quote_now"] == pytest.approx(round((495.0 - 405.0 - q_fees) * size, 2))
    assert view["mark_change_pct"] == pytest.approx(-20.0)
    assert view["stop_distance_pct"] == pytest.approx(150.0), "stop 1000 is 150% above 400"
    # capital: Delta's listed 0.5% of $10,000 of Bitcoin, plus the $50 premium
    assert view["im_pct"] == 0.5 and view["im_pct_is_default"] is False
    assert view["margin_usd"] == pytest.approx(50.0)
    assert view["capital_usd"] == pytest.approx(100.0)
    stop_fees = osp.fee_per_btc(500.0, spot) + osp.fee_per_btc(1000.0, spot)
    assert view["max_loss_usd"] == pytest.approx(round((500.0 + stop_fees) * size, 2))
    assert view["minutes_left"] == 84
    assert s.status()["today"]["view"]["capital_usd"] == view["capital_usd"]


def test_a_call_seller_is_told_when_bitcoin_is_past_the_strike():
    rec = {
        "date": DAY.isoformat(),
        "status": "open",
        "symbol": "C-BTC-100000-180926",
        "side": "C",
        "strike": 100_000.0,
        "size_btc": 0.1,
        "spot": 100_000.0,
        "entry_mark": 500.0,
        "stop_px": 1000.0,
        "last_mark": 600.0,
        "last_spot": 100_250.0,
        "last_seen_ts": ENTRY + 60,
    }
    assert osp.position_view(rec, ENTRY + 60)["view"]["btc_past_strike_usd"] == 250.0
    put = dict(rec, side="P", symbol="P-BTC-100000-180926")
    assert osp.position_view(put, ENTRY + 60)["view"]["btc_past_strike_usd"] == -250.0


def test_a_trade_recorded_before_the_margin_rate_uses_deltas_listed_default():
    rec = {
        "date": DAY.isoformat(),
        "status": "closed",
        "symbol": "C-BTC-100000-180926",
        "size_btc": 0.1,
        "spot": 100_000.0,
        "entry_mark": 500.0,
        "stop_px": 1000.0,
        "pnl_usd_mark": 20.0,
    }
    view = osp.position_view(rec, ENTRY)["view"]
    assert view["im_pct"] == osp.DEFAULT_IM_PCT and view["im_pct_is_default"] is True
    assert view["return_on_capital_pct"] == pytest.approx(20.0)


def test_a_day_without_a_trade_has_no_money_block():
    assert "view" not in osp.position_view({"date": "2026-09-17", "status": "missed"}, ENTRY)


def test_the_chart_draws_the_open_trade_oldest_first():
    delta = FakeDelta(marks=[500.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t += 300
    delta.mark_candles = [
        {"time": int(ENTRY) + 120, "close": 480.0},
        {"time": int(ENTRY) + 60, "close": 490.0},
        {"time": int(ENTRY), "close": "bad"},
    ]
    out = run(s.chart())
    assert out["date"] == DAY.isoformat() and out["trade"]["status"] == "open"
    assert out["option"] == [[int(ENTRY) + 60, 490.0], [int(ENTRY) + 120, 480.0]]
    assert out["index"], "Bitcoin is drawn under the option"
    asked = [p for path, p in delta.calls if path == "/history/candles" and p["symbol"].startswith("MARK:")]
    assert asked[-1]["end"] <= clock.t + 60, "never asks for candles from the future"


def test_the_chart_of_an_unknown_day_is_empty_not_another_day():
    s, _ = seller(FakeDelta())
    out = run(s.chart("2026-01-01"))
    assert out["trade"] is None and out["option"] == []


def test_a_chart_delta_will_not_serve_is_empty_not_an_error():
    delta = FakeDelta(marks=[500.0])
    s, _ = seller(delta)
    run(s.tick())

    async def down(path, params):
        raise RuntimeError("delta down")

    s._fetch = down
    out = run(s.chart())
    assert out["trade"]["symbol"] and out["option"] == [] and out["index"] == []


# ── calm weekends (19-Sep-2026) ───────────────────────────────────
# "Why not option seller taken on Saturday and sunday... it will be calm". On a
# Saturday or Sunday with NO window voting, it sells in the 2-hour direction.
# Calm weekdays lose in the research, so a calm Friday must still be skipped.

SAT = dt.date(2026, 9, 19)
SUN = dt.date(2026, 9, 20)


class DayDelta(FakeDelta):
    """FakeDelta moved to another date: the index, the listing and its expiry."""

    def __init__(self, day, **kw):
        super().__init__(**kw)
        self.day = day
        self.entry = dt.datetime(day.year, day.month, day.day, 10, 30, tzinfo=dt.timezone.utc).timestamp()

    async def __call__(self, path, params):
        shift = int(self.entry - ENTRY)
        if path == "/history/candles" and params["symbol"] == osp.UNDERLYING:
            out = await super().__call__(path, params)
            for row in out["result"]:
                row["time"] += shift
            return out
        if path == "/products":
            out = await super().__call__(path, params)
            tag = self.day.strftime("%d%m%y")
            for row in out["result"]:
                if row["settlement_time"] == SETTLE_ISO:
                    row["settlement_time"] = f"{self.day.isoformat()}T12:00:00Z"
                    row["symbol"] = row["symbol"].replace("180926", tag)
            return out
        return await super().__call__(path, params)


def calm(two_hour=0.1):
    moves = {lb: 0.05 for lb in osp.LOOKBACKS_MIN}
    moves[120] = two_hour
    return moves


def on(day, delta):
    s = osp.OptionSellerPaper(fetch_json=delta, clock=Clock(delta.entry + 30))
    s.enabled = True
    return s


def test_a_calm_saturday_sells_in_the_two_hour_direction():
    delta = DayDelta(SAT, moves=calm(0.1), marks=[80.0])
    s = on(SAT, delta)
    assert run(s.tick()) is True
    rec = s.days[SAT.isoformat()]
    assert rec["status"] == "open" and rec["leg"] == "weekend-calm"
    assert rec["side"] == "C" and rec["symbol"] == "C-BTC-100000-190926"
    assert rec["votes"] == {"C": 0, "P": 0}
    assert "calm weekend" in s.events[-1]["message"]


def test_a_calm_sunday_after_a_small_dip_sells_the_put():
    delta = DayDelta(SUN, moves=calm(-0.1), marks=[80.0])
    s = on(SUN, delta)
    run(s.tick())
    rec = s.days[SUN.isoformat()]
    assert rec["leg"] == "weekend-calm" and rec["side"] == "P"


def test_a_calm_friday_is_still_skipped():
    """Calm WEEKDAYS lost -2,558 per 1 BTC in the research."""
    s, _ = seller(FakeDelta(moves=calm(0.1)))
    run(s.tick())
    assert s.days[DAY.isoformat()]["status"] == "skipped"


def test_a_weekend_with_some_votes_is_not_calm():
    moves = calm(0.1)
    moves[720] = 5.0  # one window voted up: mixed, not calm
    s = on(SAT, DayDelta(SAT, moves=moves))
    run(s.tick())
    assert s.days[SAT.isoformat()]["status"] == "skipped"


def test_a_strong_weekend_is_the_ordinary_rule():
    s = on(SAT, DayDelta(SAT, marks=[500.0]))
    run(s.tick())
    rec = s.days[SAT.isoformat()]
    assert rec["status"] == "open" and rec["leg"] == "strong" and rec["votes"]["C"] == 6


def test_the_weekend_leg_can_be_switched_off_alone():
    delta = DayDelta(SAT, moves=calm(0.1))
    s = on(SAT, delta)
    s.configure(weekend_calm=False)
    run(s.tick())
    assert s.days[SAT.isoformat()]["status"] == "skipped"
    assert "switched OFF" in s.events[0]["message"]


def test_the_weekend_leg_is_on_for_a_book_saved_before_it_existed():
    s = osp.OptionSellerPaper()
    s.load({"enabled": True, "size_btc": 0.1, "days": {}})
    assert s.weekend_calm is True
    s.configure(weekend_calm=False)
    again = osp.OptionSellerPaper()
    again.load(s.dump())
    assert again.weekend_calm is False and again.status()["weekend_calm"] is False
    with pytest.raises(ValueError):
        s.configure(weekend_calm="yes")


# ── settings, state, safety ───────────────────────────────────────


def test_size_is_whole_contracts_and_bounded():
    s = osp.OptionSellerPaper(fetch_json=FakeDelta())
    s.configure(size_btc=0.1234)
    assert s.size_btc == 0.123
    for bad in (0, -1, 11, "abc", float("nan"), 0.0001):
        with pytest.raises(ValueError):
            s.configure(size_btc=bad)


def test_the_switch_takes_only_true_or_false():
    s = osp.OptionSellerPaper(fetch_json=FakeDelta())
    with pytest.raises(ValueError):
        s.configure(enabled="false")
    s.configure(enabled=True)
    assert s.enabled is True


def test_state_round_trips():
    delta = FakeDelta(marks=[500.0])
    s, _ = seller(delta)
    run(s.tick())
    other = osp.OptionSellerPaper(fetch_json=delta)
    other.load(s.dump())
    assert other.dump() == s.dump()


def test_status_totals():
    delta = FakeDelta(marks=[500.0, 120.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t = dt.datetime(2026, 9, 18, 11, 55, tzinfo=dt.timezone.utc).timestamp()
    run(s.tick())
    st = s.status()
    assert st["paper_only"] is True
    assert st["totals"]["trades"] == 1 and st["totals"]["wins"] == 1
    assert st["next_decision_ist"] == "2026-09-19 16:00 IST"


def test_the_module_cannot_place_an_order():
    """Paper only BY CONSTRUCTION: public GETs, no keys, no order paths."""
    src = open(osp.__file__, encoding="utf-8").read()
    code = re.sub(r'""".*?"""', "", src, flags=re.S)  # ignore the docstrings
    for forbidden in (
        "requests.post",
        "requests.delete",
        "requests.put",
        "/orders",
        "api_key",
        "api-key",
        "signature",
        "place_order",
        "broker",
    ):
        assert forbidden not in code, f"{forbidden!r} appears in the paper engine"
    assert "requests.get(" in code
    assert osp.BASE_URL.startswith("https://api.india.delta.exchange/")


def test_every_request_is_a_public_read():
    delta = FakeDelta(marks=[500.0, 120.0])
    s, clock = seller(delta)
    run(s.tick())
    clock.t = dt.datetime(2026, 9, 18, 11, 55, tzinfo=dt.timezone.utc).timestamp()
    run(s.tick())
    paths = {p.split("/")[1] for p, _ in delta.calls}
    assert paths <= {"history", "products", "tickers"}
