"""The Option Seller's live leg (22-Sep-2026): real orders on Delta, behind locks.

Phil: "yes build the live side with stop order". Driven against a FAKE
executor and a fake Delta transport, so nothing here can reach an account.
What must hold:

  · no live order unless every lock is open (armed, keys, size cap, mode);
  · a sale is followed at once by a RESTING stop on Delta at twice the
    premium, on the mark, reduce-only;
  · a stop that cannot be placed means an immediate buy-back, never a naked
    short;
  · at 17:25 the stop is cancelled and the position bought back;
  · an order POST is never retried blind — it is looked up by its
    client_order_id first.
"""

import asyncio
import datetime as dt
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_option_seller_paper import DAY, ENTRY, Clock, FakeDelta  # noqa: E402

from engine import option_seller_live as osl  # noqa: E402
from engine import option_seller_paper as osp  # noqa: E402

EXIT = dt.datetime(2026, 9, 18, 11, 55, tzinfo=dt.timezone.utc).timestamp()


def run(coro):
    return asyncio.run(coro)


class FakeExecutor:
    """Records every call; each behaviour can be switched per test."""

    def __init__(self, blocked="", fill=100, stop_fails=0, sell_raises=False):
        self.blocked = blocked
        self.fill = fill
        self.stop_fails = stop_fails
        self.sell_raises = sell_raises
        self.calls = []
        self.stop_state = {"state": "open"}
        self.position = 0

    def why_not_live(self, contracts):
        return self.blocked

    async def product(self, symbol):
        return {"id": 77, "tick": 0.5}

    async def sell_open(self, symbol, contracts, bid, day):
        self.calls.append(("sell", symbol, contracts, bid))
        if self.sell_raises:
            raise osl.LiveOrderError("insufficient margin")
        self.position = -self.fill
        return {"order_id": 1, "product_id": 77, "filled": self.fill, "avg_price": 494.5, "limit": 480.5}

    async def place_stop(self, product_id, contracts, stop_px, tick, day):
        self.calls.append(("stop", product_id, contracts, stop_px))
        if self.stop_fails:
            self.stop_fails -= 1
            raise osl.LiveOrderError("stop refused")
        return {"order_id": 2, "stop_price": stop_px}

    async def order(self, order_id):
        return self.stop_state

    async def cancel(self, order_id, product_id):
        self.calls.append(("cancel", order_id))

    async def position_size(self, product_id):
        return self.position

    async def buy_close(self, product_id, contracts, day):
        self.calls.append(("buy", product_id, contracts))
        self.position = 0
        return {"order_id": 3, "filled": contracts, "avg_price": 120.0}


def live_seller(ex, marks=(500.0,)):
    delta = FakeDelta(marks=list(marks))
    alerts = []
    s = osp.OptionSellerPaper(
        fetch_json=delta, clock=Clock(ENTRY + 30), executor=ex, on_alert=lambda t, b, lvl: alerts.append((t, lvl))
    )
    s.enabled = True
    s.configure(mode="live")
    return s, alerts


# ── the locks ─────────────────────────────────────────────────────────


def test_live_is_refused_while_any_lock_is_shut():
    s = osp.OptionSellerPaper(executor=FakeExecutor(blocked="the server is not armed"))
    with pytest.raises(ValueError, match="not armed"):
        s.configure(mode="live")
    assert s.mode == "paper"
    with pytest.raises(ValueError, match="no live executor"):
        osp.OptionSellerPaper().configure(mode="live")


def test_a_reload_without_the_locks_comes_back_as_paper():
    s = osp.OptionSellerPaper(executor=FakeExecutor(blocked="keys missing"))
    s.load({"mode": "live", "size_btc": 0.1})
    assert s.mode == "paper"


def test_the_executor_locks_in_order():
    ex = osl.DeltaOptionExecutor(api_key="", api_secret="", armed=False)
    assert "not armed" in ex.why_not_live(1)
    ex = osl.DeltaOptionExecutor(api_key="", api_secret="", armed=True)
    assert "keys" in ex.why_not_live(1)
    ex = osl.DeltaOptionExecutor(api_key="realkey123", api_secret="realsecret123", armed=True)
    assert ex.why_not_live(1) == ""
    assert "capped" in ex.why_not_live(osl.MAX_LIVE_CONTRACTS + 1)
    assert "at least 1" in ex.why_not_live(0)


def test_dummy_test_keys_never_count_as_configured():
    assert not osl.DeltaOptionExecutor(api_key="e2e-dummy", api_secret="e2e-dummy-secret", armed=True).configured()


# ── a live day ────────────────────────────────────────────────────────


def test_a_live_sale_rests_a_stop_on_delta_at_twice_the_premium():
    ex = FakeExecutor()
    s, alerts = live_seller(ex)
    run(s.tick())
    rec = s.days[DAY.isoformat()]
    assert rec["status"] == "open"
    assert ex.calls[0] == ("sell", rec["symbol"], 100, 495.0)
    assert ex.calls[1] == ("stop", 77, 100, rec["stop_px"]) and rec["stop_px"] == 1000.0
    live = rec["live"]
    assert live["status"] == "open" and live["entry_fill"] == 494.5 and live["stop_order_id"] == 2
    assert any("sold" in t for t, _ in alerts)


def test_delta_filling_the_stop_closes_the_live_trade_at_its_fill():
    ex = FakeExecutor()
    s, _ = live_seller(ex, marks=(500.0, 700.0))
    run(s.tick())
    ex.stop_state = {"state": "closed", "average_fill_price": "1010.0"}
    s._clock.t += 15
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert live["status"] == "closed" and live["exit_why"] == "stop" and live["exit_fill"] == 1010.0
    fees = osp.fee_per_btc(494.5, 100_000.0) + osp.fee_per_btc(1010.0, 100_000.0)
    assert live["pnl_usd"] == pytest.approx(round((494.5 - 1010.0 - fees) * 0.1, 2))


def test_at_1725_the_stop_is_cancelled_and_the_position_bought_back():
    ex = FakeExecutor()
    s, _ = live_seller(ex, marks=(500.0, 120.0))
    run(s.tick())
    s._clock.t = EXIT
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert ("cancel", 2) in ex.calls
    assert ("buy", 77, 100) in ex.calls
    assert live["status"] == "closed" and live["exit_why"] == "time" and live["exit_fill"] == 120.0
    assert s.status()["live_totals"]["trades"] == 1


def test_no_stop_means_an_immediate_buy_back():
    ex = FakeExecutor(stop_fails=2)
    s, alerts = live_seller(ex)
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert [c[0] for c in ex.calls] == ["sell", "stop", "stop", "buy"]
    assert live["status"] == "closed" and live["exit_why"] == "no-stop"
    assert any(lvl == "error" and "NO STOP" in t for t, lvl in alerts)


def test_one_failed_stop_attempt_is_retried_once():
    ex = FakeExecutor(stop_fails=1)
    s, _ = live_seller(ex)
    run(s.tick())
    assert s.days[DAY.isoformat()]["live"]["status"] == "open"
    assert [c[0] for c in ex.calls] == ["sell", "stop", "stop"]


def test_an_unfilled_sale_takes_no_position_and_no_stop():
    ex = FakeExecutor(fill=0)
    s, _ = live_seller(ex)
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert live["status"] == "not_filled"
    assert [c[0] for c in ex.calls] == ["sell"]


def test_a_refused_sale_is_an_error_not_a_position():
    ex = FakeExecutor(sell_raises=True)
    s, alerts = live_seller(ex)
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert live["status"] == "error" and "insufficient margin" in live["reason"]
    assert any(lvl == "error" for _, lvl in alerts)
    assert s.days[DAY.isoformat()]["status"] == "open", "the paper record still runs"


def test_a_paper_book_never_touches_the_executor():
    ex = FakeExecutor()
    delta = FakeDelta(marks=[500.0])
    s = osp.OptionSellerPaper(fetch_json=delta, clock=Clock(ENTRY + 30), executor=ex)
    s.enabled = True
    run(s.tick())
    assert ex.calls == [] and "live" not in s.days[DAY.isoformat()]


def test_the_live_position_survives_a_reload():
    ex = FakeExecutor()
    s, _ = live_seller(ex, marks=(500.0, 120.0))
    run(s.tick())
    again = osp.OptionSellerPaper(fetch_json=FakeDelta(marks=[120.0]), clock=Clock(EXIT), executor=ex)
    again.load(s.dump())
    assert again.mode == "live" and again.status()["live_open"]
    run(again.tick())
    assert again.days[DAY.isoformat()]["live"]["status"] == "closed"


def test_a_stop_cancelled_on_delta_is_put_back_while_still_short():
    ex = FakeExecutor()
    s, alerts = live_seller(ex, marks=(500.0, 600.0))
    run(s.tick())
    ex.stop_state = {"state": "cancelled"}
    s._clock.t += 15
    run(s.tick())
    assert [c[0] for c in ex.calls].count("stop") == 2
    assert s.days[DAY.isoformat()]["live"]["status"] == "open"
    assert any("replaced" in t for t, _ in alerts)


def test_a_stop_cancelled_after_a_manual_close_books_the_close():
    ex = FakeExecutor()
    s, _ = live_seller(ex, marks=(500.0, 600.0))
    run(s.tick())
    ex.stop_state = {"state": "cancelled"}
    ex.position = 0
    s._clock.t += 15
    run(s.tick())
    live = s.days[DAY.isoformat()]["live"]
    assert live["status"] == "closed" and live["exit_why"] == "manual"


# ── the executor's own order discipline ───────────────────────────────


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sent = []

    async def __call__(self, method, url, headers, body, params):
        self.sent.append((method, url.split("/v2", 1)[1], json.loads(body) if body else None, params))
        return self.responses.pop(0)


def executor(responses):
    t = FakeTransport(responses)
    return osl.DeltaOptionExecutor(api_key="realkey123", api_secret="realsecret123", http=t, armed=True), t


def test_the_stop_order_is_a_reduce_only_market_buy_on_the_mark():
    ex, t = executor([(200, {"success": True, "result": {"id": 9}})])
    out = run(ex.place_stop(77, 10, 327.58, 0.5, "2026-09-21"))
    method, path, body, _ = t.sent[0]
    assert (method, path) == ("POST", "/orders")
    assert body["side"] == "buy" and body["order_type"] == "market_order"
    assert body["stop_order_type"] == "stop_loss_order" and body["stop_trigger_method"] == "mark_price"
    assert body["reduce_only"] is True and body["size"] == 10
    assert body["stop_price"] == "327.5" and out["stop_price"] == 327.5, "rounded to the tick, never above the stop"


def test_the_entry_is_an_ioc_limit_just_under_the_bid():
    ex, t = executor(
        [
            (200, {"success": True, "result": {"id": 77, "tick_size": "0.5"}}),
            (200, {"success": True, "result": {"id": 5, "size": 10, "unfilled_size": 0, "average_fill_price": "154"}}),
        ]
    )
    out = run(ex.sell_open("C-BTC-78200-180926", 10, 154.0, "2026-09-18"))
    body = t.sent[1][2]
    assert body["side"] == "sell" and body["order_type"] == "limit_order" and body["time_in_force"] == "ioc"
    assert float(body["limit_price"]) == pytest.approx(149.5), "3% under the bid, on the tick"
    assert out["filled"] == 10 and out["avg_price"] == 154.0


def test_a_failed_order_post_is_looked_up_never_resent():
    ex, t = executor(
        [
            (502, {"success": False, "error": {"code": "bad_gateway"}}),
            (200, {"success": True, "result": [{"id": 11, "client_order_id": "PLACEHOLDER"}]}),
        ]
    )

    async def go():
        real = ex._post_once

        async def once(data):
            t.responses[1][1]["result"][0]["client_order_id"] = data["client_order_id"]
            return await real(data)

        ex._post_once = once
        return await ex.buy_close(77, 10, "2026-09-18")

    out = run(go())
    posts = [s for s in t.sent if s[0] == "POST"]
    assert len(posts) == 1, "an order POST is sent once"
    assert out["order_id"] == 11, "the order that DID reach Delta is found by its client id"


def test_orders_carry_their_own_client_ids():
    ids = {osl.DeltaOptionExecutor._coid("sell", "2026-09-18") for _ in range(20)}
    assert len(ids) == 20 and all(len(i) <= 32 for i in ids)
