"""The V-Rule live driver: its locks, its engine surface, and its parity with
the locked simulator.

The parity test is the one that matters. The driver re-implements the
simulator's ladder over real fills; in paper mode the two must agree on every
fill, target and end over a tape. If that test fails, the live driver is not
the proven rule and must not trade.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import engine.vrule_live as vr  # noqa: E402
import tools.rule3070_sim as sim  # noqa: E402
from engine.cascade import ACTIVE_STATES, CascadeEngine, compute_tp_price  # noqa: E402
from engine.rule3070_paper import REPLAY_LOCK, configure  # noqa: E402
from engine.vrule_live import Book, Ladder, VRuleLive  # noqa: E402
from tools.vrule_live_parity import HarnessBroker, run_parity, synthetic_tape  # noqa: E402


@pytest.fixture(autouse=True)
def armed(monkeypatch):
    monkeypatch.setattr(vr, "DRIVER_ARMED", True)


# ── the numbers are the simulator's numbers ───────────────────────


def test_the_drivers_rule_constants_are_the_simulators():
    """configure() is the locked config. The driver mirrors it; this pins them."""
    with REPLAY_LOCK:
        configure()
        assert vr.SPLIT == sim.SPLIT
        assert vr.MIN_ORDER_USD == sim.MIN_ORDER_USD
        assert vr.MAX_BANDS == sim.MAX_BANDS
        assert vr.MIN_NET_MARGIN == sim.MIN_NET_MARGIN
        assert vr.BUDGET_CAP_FRAC == sim.BUDGET_CAP_FRAC
        assert (vr.FOLD_AT_FRACTION,) == sim.COMPOUND_SCHEDULE
        assert vr.FEE_PER_SIDE == sim.FEE_PER_SIDE
        assert sim.COMPOUND_AT_HALF is True
        assert sim.ENFORCE_BUDGET is True


# ── parity with the locked simulator ──────────────────────────────


@pytest.mark.parametrize("seed", [7, 11, 23])
def test_paper_driver_reproduces_the_simulator_bar_for_bar(seed):
    report = run_parity(synthetic_tape(1800, seed), purse=2000.0)
    assert report["campaigns"] > 0, "the tape formed no V at all — not a test"
    assert report["ok"], "\n".join(report["mismatches"][:20])


# ── the engine surface a driven campaign uses ─────────────────────


def _engine():
    engine = CascadeEngine(HarnessBroker())
    engine.start = lambda: None
    return engine


_MOTHERS = iter(range(1, 10_000))


def _driven(engine, mode="paper"):
    # Each call is a different mother candle: the engine refuses two live
    # campaigns on the same one, and that refusal is not what these test.
    n = next(_MOTHERS)
    result = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0 + n,
            mother_low=108.0 + n,
            mother_timestamp=int(__import__("time").time()) - 3600 - 300 * n,
            mode=mode,
            timeframe="5m",
            mc_kind="major",
            strategy=vr.STRATEGY,
            driven=True,
        )
    )
    assert not result.get("error"), result
    return engine.campaigns[result["campaign"]["campaign_id"]]


def test_a_driven_campaign_never_escalates_and_skips_the_candle_machine():
    engine = _engine()
    campaign = _driven(engine)
    assert campaign.driven is True
    assert campaign.escalates is False
    called = []
    engine._candle_step = lambda c: called.append(c) or asyncio.sleep(0)  # would be awaited
    asyncio.run(engine._campaign_tick(campaign))
    assert called == []  # the driver steps it, not the engine


def test_arm_is_idempotent_and_a_moved_trigger_replaces_the_order():
    from engine.cascade import Candle

    engine = _engine()
    campaign = _driven(engine)
    bar = Candle(1, 100, 101, 99, 100)
    assert engine.arm_driven_entry(campaign, 50.0, 100.0, bar) is True
    rev = campaign.pending_rev
    assert campaign.pending_usd == 50.0 and campaign.pending_stop_price == 100.0
    assert engine.arm_driven_entry(campaign, 50.0, 100.0, bar) is False  # nothing moved
    assert campaign.pending_rev == rev
    campaign.pending_order_id = "resting-123"
    assert engine.arm_driven_entry(campaign, 50.0, 99.0, bar) is True  # walked down
    assert campaign.pending_rev == rev + 1
    assert campaign.pending_order_id is None  # the sweep cancels the old one
    assert engine.disarm_driven_entry(campaign) is True
    assert campaign.pending_usd == 0.0 and campaign.pending_stop_price is None


def test_the_override_target_replaces_the_fib_level_but_keeps_the_fee_floor():
    engine = _engine()
    campaign = _driven(engine)
    campaign.pending_usd = 100.0
    engine._fill_pending(campaign, 100.0, 10)
    assert campaign.avg_entry_price == pytest.approx(100.0)
    campaign.tp_override_price = campaign.mother_high - 1.0
    assert compute_tp_price(campaign) == pytest.approx(campaign.mother_high - 1.0)
    campaign.tp_override_price = 100.01  # under the round-trip fee
    assert compute_tp_price(campaign) > 100.01  # floored: it never sells at a loss


def test_complete_books_the_rules_reason_and_archives():
    engine = _engine()
    campaign = _driven(engine)
    engine.complete_driven_campaign(campaign, "cancelled", "closed above the mother")
    assert campaign.state == "COMPLETED"
    assert campaign.close_reason == "cancelled"
    assert campaign.campaign_id not in {c.campaign_id for c in engine.active_campaigns}
    assert any(r["campaign_id"] == campaign.campaign_id for r in engine.closed_campaigns)


def test_a_hand_started_campaign_is_untouched_by_the_driver_surface():
    engine = _engine()
    result = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0,
            mother_low=108.0,
            mother_timestamp=int(__import__("time").time()) - 3600,
            mode="paper",
            timeframe="5m",
        )
    )
    campaign = engine.campaigns[result["campaign"]["campaign_id"]]
    assert campaign.driven is False and campaign.escalates is True
    from engine.cascade import Candle

    assert engine.arm_driven_entry(campaign, 50.0, 100.0, Candle(1, 1, 1, 1, 1)) is False
    assert engine.disarm_driven_entry(campaign) is False
    assert engine.complete_driven_campaign(campaign, "x").get("error")
    assert campaign.state in ACTIVE_STATES


# ── the locks ─────────────────────────────────────────────────────


class ArmedBroker(HarnessBroker):
    live_armed = True

    def _is_configured(self):
        return True


def test_live_is_refused_unless_armed_and_keyed(monkeypatch):
    driver = VRuleLive(_engine())
    with pytest.raises(ValueError, match="switched off"):
        driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=2000.0)
    engine = CascadeEngine(ArmedBroker())
    engine.start = lambda: None
    monkeypatch.setattr(vr, "LIVE_ARMED", True)
    driver = VRuleLive(engine)
    book = driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=2000.0)
    assert book.mode == "live" and driver.live_available() is True


def test_a_live_purse_over_the_ceiling_is_refused(monkeypatch):
    monkeypatch.setattr(vr, "LIVE_CEILING_USD", 2000.0)
    engine = CascadeEngine(ArmedBroker())
    engine.start = lambda: None
    with pytest.raises(ValueError, match="at most"):
        VRuleLive(engine).set_book("BTCUSDT", enabled=True, mode="live", capital_usd=50_000.0)


def test_a_live_book_restored_while_disarmed_comes_back_as_paper(monkeypatch):
    monkeypatch.setattr(vr, "LIVE_ARMED", False)
    book = Book.from_dict({"symbol": "BTCUSDT", "mode": "live", "enabled": True})
    assert book.mode == "paper"


# ── a second venue ────────────────────────────────────────────────


class DearVenue(HarnessBroker):
    """CoinDCX's shape: twice the fee, a 15m floor, a higher minimum."""

    broker_name = "coindcx"
    display_name = "CoinDCX Spot"
    min_timeframe = "15m"
    fee_pct_per_side = 0.2

    def get_product_by_symbol(self, symbol):
        return {**super().get_product_by_symbol(symbol), "min_notional": "11.0"}


def _two_venue_engine(default=None, dear=None):
    engine = CascadeEngine(default or HarnessBroker(), brokers={"coindcx": dear or DearVenue()})
    engine.start = lambda: None
    return engine


def test_the_default_venue_is_stored_blank_and_an_unknown_one_is_refused():
    driver = VRuleLive(_two_venue_engine())
    first = driver.set_book("BTCUSDT", capital_usd=500.0, exchange="binance")
    second = driver.set_book("BTCUSDT", capital_usd=600.0, exchange="")
    assert first is second and first.exchange == "" and list(driver.books) == ["btcusdt:"]
    with pytest.raises(ValueError, match="no client for 'kraken'"):
        driver.set_book("BTCUSDT", capital_usd=500.0, exchange="kraken")
    assert driver.set_book("BTCUSDT", capital_usd=500.0, exchange="CoinDCX").exchange == "coindcx"


def test_live_is_judged_by_the_named_venues_own_keys(monkeypatch):
    monkeypatch.setattr(vr, "LIVE_ARMED", True)

    class ArmedDear(DearVenue):
        live_armed = True

        def _is_configured(self):
            return False

    driver = VRuleLive(_two_venue_engine(default=ArmedBroker(), dear=ArmedDear()))
    assert driver.live_available() is True and driver.live_available("coindcx") is False
    with pytest.raises(ValueError, match="CoinDCX Spot API keys are not configured"):
        driver.set_book("BTCUSDT", enabled=True, mode="live", capital_usd=500.0, exchange="coindcx")
    venues = {v["name"]: v for v in driver.status()["exchanges"]}
    assert venues["binance"]["live_available"] is True and venues["coindcx"]["live_available"] is False
    assert venues["coindcx"]["fee_pct_per_side"] == 0.2


def test_a_driven_5m_ladder_is_allowed_on_a_15m_floor_venue_and_priced_at_its_fee():
    """The venue floor guards the engine's own target; a driven target is the
    rule's, and it is fee-floored at the venue's rate whatever the rule says."""
    engine = _two_venue_engine()
    refused = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0,
            mother_low=108.0,
            mother_timestamp=int(__import__("time").time()) - 3600,
            mode="paper",
            timeframe="5m",
            mc_kind="major",
            exchange="coindcx",
        )
    )
    assert "15m and slower" in refused.get("error", "")  # the Cascade page's campaign still is
    result = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0,
            mother_low=108.0,
            mother_timestamp=int(__import__("time").time()) - 3600,
            mode="paper",
            timeframe="5m",
            mc_kind="major",
            exchange="coindcx",
            strategy=vr.STRATEGY,
            driven=True,
        )
    )
    assert not result.get("error"), result
    campaign = engine.campaigns[result["campaign"]["campaign_id"]]
    assert campaign.timeframe == "5m" and campaign.exchange == "coindcx"
    assert campaign.fee_pct_per_side == 0.2 and campaign.min_notional_usd == 11.0
    assert engine.broker_for(campaign) is engine.brokers["coindcx"]
    campaign.pending_usd = 100.0
    engine._fill_pending(campaign, 100.0, 10)
    campaign.tp_override_price = 100.3  # clears Binance's round trip, not CoinDCX's
    assert compute_tp_price(campaign) > 100.4


def test_the_fee_gate_is_the_venues_round_trip_plus_the_same_edge():
    """Binance: exactly the locked 0.35%. CoinDCX (0.2% a side): 0.55%. A
    campaign that predates per-venue fees gets the locked number."""
    from types import SimpleNamespace as NS

    assert vr.venue_net_margin(NS(fee_pct_per_side=0.1)) == pytest.approx(vr.MIN_NET_MARGIN)
    assert vr.venue_net_margin(NS(fee_pct_per_side=0.2)) == pytest.approx(0.0055)
    assert vr.venue_net_margin(NS(fee_pct_per_side=None)) == vr.MIN_NET_MARGIN


class TapeVenue(DearVenue):
    """A venue with its own tape and ticker, and a record of who asked."""

    def __init__(self, price=100.0):
        self.price = price
        self.candle_calls = []
        self.ticker_calls = 0

    def get_ticker(self, symbol):
        self.ticker_calls += 1
        return {"last_price": self.price}

    def get_candles(self, symbol, resolution="5m", start=None, end=None):
        import pandas as pd

        self.candle_calls.append((symbol, resolution, start))
        now = int(__import__("time").time()) // 300 * 300
        rows = [(now - 300 * (i + 1), 100.0, 101.0, 99.0, 100.0) for i in range(30, 0, -1)]
        rows.append((now, 100.0, 101.0, 99.0, 100.0))  # the still-forming bar
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
        df.index = pd.to_datetime(df.pop("ts"), unit="s", utc=True)
        return df

    async def async_get_candles(self, symbol, **kwargs):
        return self.get_candles(symbol, **kwargs)


class TapeDefault(HarnessBroker):
    def __init__(self):
        self.ticker_calls = 0
        self.candle_calls = []

    def get_ticker(self, symbol):
        self.ticker_calls += 1
        return {"last_price": 50.0}

    async def async_get_candles(self, symbol, **kwargs):
        self.candle_calls.append(kwargs)
        return await super().async_get_candles(symbol, **kwargs)


def test_a_campaigns_price_and_candles_come_from_its_own_venue():
    """Binance: the default client and the symbol-keyed cache, exactly as
    before. CoinDCX: that venue's ticker and tape, under its own key."""
    default, dear = TapeDefault(), TapeVenue(price=100.0)
    engine = CascadeEngine(default, brokers={"coindcx": dear})
    engine.start = lambda: None
    binance = _driven(engine)
    coindcx = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0,
            mother_low=108.0,
            mother_timestamp=int(__import__("time").time()) - 3600,
            mode="paper",
            timeframe="5m",
            mc_kind="major",
            exchange="coindcx",
            strategy=vr.STRATEGY,
            driven=True,
        )
    )
    coindcx = engine.campaigns[coindcx["campaign"]["campaign_id"]]
    assert engine._price_key(binance) == "BTCUSDT"
    assert engine._price_key(coindcx) == "coindcx:BTCUSDT"
    assert asyncio.run(engine._get_price("BTCUSDT", venue=engine.broker_for(binance))) == 50.0
    assert asyncio.run(engine._get_price("BTCUSDT", venue=engine.broker_for(coindcx))) == 100.0
    assert engine._price_cache["BTCUSDT"][0] == 50.0 and engine._price_cache["coindcx:BTCUSDT"][0] == 100.0
    assert default.ticker_calls == 1 and dear.ticker_calls == 1
    since = int(__import__("time").time()) - 3600
    rows = asyncio.run(engine._fetch_closed_candles("BTCUSDT", since, "5m", venue=engine.broker_for(coindcx)))
    assert rows and dear.candle_calls and dear.candle_calls[-1][1] == "5m"
    asyncio.run(engine._fetch_closed_candles("BTCUSDT", since, "5m", venue=engine.broker_for(binance)))
    assert default.candle_calls  # the default venue still reads the default client
    # and with no venue named at all, the default — the pre-venue call shape
    before = len(default.candle_calls)
    asyncio.run(engine._fetch_closed_candles("BTCUSDT", since, "5m"))
    assert len(default.candle_calls) == before + 1


def test_a_restarted_book_gets_its_new_warm_up_not_the_cached_one():
    """Turning a book off and on moves the warm-up forward. The kept window
    must move with it, or the locked simulator scans a longer window than
    fetch_window would ever hand the same book on Binance."""
    import pandas as pd

    now = int(__import__("time").time()) // 300 * 300

    class LongTape(TapeVenue):
        def get_candles(self, symbol, resolution="5m", start=None, end=None):
            self.candle_calls.append((symbol, resolution, start))
            n = 120 * 288  # 120 days of 5m bars, whatever is asked for
            rows = [(now - 300 * (n - i), 100.0, 101.0, 99.0, 100.0) for i in range(n)]
            df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
            df.index = pd.to_datetime(df.pop("ts"), unit="s", utc=True)
            return df

    driver = VRuleLive(_two_venue_engine(dear=LongTape()))
    book = driver.set_book("BTCUSDT", enabled=True, capital_usd=500.0, exchange="coindcx")
    book.history_start_ts = now - 90 * 86400  # an older, longer warm-up
    wide = driver._load_window(book)
    assert int(wide.index[0].timestamp()) >= book.history_start_ts
    assert (wide.index[-1] - wide.index[0]).total_seconds() > 60 * 86400  # the long warm-up, cached
    driver.set_book("BTCUSDT", enabled=False, exchange="coindcx")
    fresh = driver.set_book("BTCUSDT", enabled=True, exchange="coindcx")
    assert abs((now - fresh.history_start_ts) - vr.WARMUP_DAYS * 86400) < 600  # the clock moved on
    window = driver._load_window(fresh)
    # The cached 90 days must NOT survive into the new 30-day warm-up.
    assert int(window.index[0].timestamp()) >= fresh.history_start_ts
    assert (window.index[-1] - window.index[0]).total_seconds() <= vr.WARMUP_DAYS * 86400


def test_a_campaign_on_a_venue_the_engine_lost_is_still_shown():
    """broker_for refuses it, rightly — but get_status and the chart must
    not die with it, or one orphan hides every other campaign."""
    engine = _two_venue_engine()
    result = asyncio.run(
        engine.start_campaign(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=110.0,
            mother_low=108.0,
            mother_timestamp=int(__import__("time").time()) - 3600,
            mode="paper",
            timeframe="5m",
            mc_kind="major",
            exchange="coindcx",
            strategy=vr.STRATEGY,
            driven=True,
        )
    )
    campaign = engine.campaigns[result["campaign"]["campaign_id"]]
    del engine.brokers["coindcx"]  # the client failed to build on this restart
    with pytest.raises(LookupError):
        engine.broker_for(campaign)
    assert engine._price_key(campaign) == "coindcx:BTCUSDT"
    status = engine.get_status()
    assert any(c["campaign_id"] == campaign.campaign_id for c in status["campaigns"])
    # and the chart draws nothing rather than 500ing or drawing Binance's bars
    campaign.mother_timestamp = int(__import__("time").time()) - 90 * 86400  # forces the paged branch
    assert asyncio.run(engine.get_chart_data(campaign.campaign_id)).get("candles") == []


def test_a_coindcx_book_scans_its_own_tape_and_only_tops_it_up_after():
    dear = TapeVenue()
    driver = VRuleLive(_two_venue_engine(dear=dear))
    book = driver.set_book("BTCUSDT", enabled=True, capital_usd=500.0, exchange="coindcx")
    df = driver._load_window(book)
    now_bucket = int(__import__("time").time()) // 300 * 300
    assert len(df) == 30 and int(df.index[-1].timestamp()) < now_bucket  # the forming bar is dropped
    assert dear.candle_calls[0][2] == __import__("datetime").datetime.utcfromtimestamp(book.history_start_ts).strftime(
        "%Y-%m-%d"
    )
    again = driver._load_window(book)
    assert len(again) == 30 and len(dear.candle_calls) == 2
    assert dear.candle_calls[1][2] >= dear.candle_calls[0][2]  # the top-up starts near the last bar, not 30 days back
    # the default venue still goes through the paper book's own fetch
    calls = []
    import engine.rule3070_paper as paper

    original = paper.fetch_window
    paper.fetch_window = lambda symbol, since_ts=0, **kw: calls.append((symbol, since_ts)) or df
    try:
        plain = driver.set_book("BTCUSDT", enabled=True, capital_usd=500.0)
        driver._load_window(plain)
    finally:
        paper.fetch_window = original
    assert calls == [("BTCUSDT", plain.history_start_ts)]


def test_the_paper_only_broker_hands_the_engine_the_venues_fee():
    from engine.auto_cascade_fib import PaperOnlyBroker

    engine = CascadeEngine(PaperOnlyBroker(HarnessBroker()), brokers={"coindcx": PaperOnlyBroker(DearVenue())})
    assert engine.venue_min_timeframe("coindcx") == "15m"
    assert {v["name"]: v["fee_pct_per_side"] for v in engine.available_exchanges()} == {"binance": 0.1, "coindcx": 0.2}


def test_turning_a_book_on_starts_its_clock():
    driver = VRuleLive(_engine())
    book = driver.set_book("BTCUSDT", enabled=True, capital_usd=2000.0)
    assert book.start_ts > 0
    assert book.history_start_ts == book.start_ts - vr.WARMUP_DAYS * 86400


def test_the_driver_declares_only_its_live_coin():
    engine = _engine()
    driver = VRuleLive(engine)
    live = _driven(engine, mode="live") if False else _driven(engine)
    live.mode = "live"
    live.filled_base_qty = 0.5
    paper = _driven(engine)
    paper.filled_base_qty = 99.0
    assert driver.claimed_base_qty("BTCUSDT") == pytest.approx(0.5)


def test_state_survives_a_save_and_load():
    driver = VRuleLive(_engine())
    book = driver.set_book("SOLUSDT", enabled=True, capital_usd=500.0)
    book.ladders["1-2"] = Ladder(
        vid="1-2",
        campaign_id="abc",
        mother_ts=1,
        mother_high=100,
        swing_low_ts=2,
        swing_low=90,
        swing_high_ts=3,
        swing_high=95,
        born_ts=4,
        touched=True,
        lowest_low=88.0,
        pending="70%",
        band=1,
    )
    book.purse_usd = 650.0
    book.pocket_usd = 12.0
    revived = VRuleLive(_engine())
    revived.load(driver.dump())
    back = revived.books["solusdt:"]
    assert back.purse_usd == 650.0 and back.pocket_usd == 12.0 and back.enabled is True
    assert back.ladders["1-2"].lowest_low == 88.0 and back.ladders["1-2"].pending == "70%"
