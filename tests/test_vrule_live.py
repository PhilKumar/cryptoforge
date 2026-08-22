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
    with pytest.raises(ValueError, match="not armed for live"):
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
