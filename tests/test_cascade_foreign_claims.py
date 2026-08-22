"""Two engines, one exchange account: each must net off the other's coin.

Since 2026-08-21 Auto-Cascade_Fib runs in its own CascadeEngine. Both engines
trade the same Binance account, and each one's `self.campaigns` sees only half
the claims on a symbol's balance. Without netting, a campaign reads the other
engine's holding as unclaimed and concludes its own coin is still there — the
wrong answer in the SILENT direction, which is worse than a false alarm.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cascade import Campaign, CascadeEngine  # noqa: E402
from tests.test_cascade_engine import FakeCascadeBroker  # noqa: E402


def _engine_with_holding(free_btc: float):
    broker = FakeCascadeBroker()
    broker.free_balances = {"BTC": free_btc}
    broker.locked_balances = {"BTC": 0.0}
    engine = CascadeEngine(broker)
    return engine


def _ended_campaign(engine, filled: float):
    campaign = Campaign(
        campaign_id="mine",
        symbol="BTCUSDT",
        capital_usd=2000.0,
        mother_high=100.0,
        mother_low=90.0,
        mother_timestamp=0,
        mode="live",
    )
    campaign.filled_base_qty = filled
    campaign.state = "COMPLETED"
    engine.campaigns[campaign.campaign_id] = campaign
    return campaign


def test_without_netting_a_siblings_coin_reads_as_our_own():
    """The bug this hook exists to close, stated as a test."""
    engine = _engine_with_holding(1.0)  # 1 BTC in the account...
    campaign = _ended_campaign(engine, filled=1.0)  # ...and we claim all of it
    # No foreign_claims set: the engine believes the whole balance is its own.
    assert asyncio.run(engine._settle_ended_position(campaign)) is False
    assert campaign.exchange_qty == 1.0


def test_a_siblings_claim_is_subtracted_from_the_shared_balance():
    engine = _engine_with_holding(1.0)
    campaign = _ended_campaign(engine, filled=1.0)
    # The other engine holds 0.9 of that same 1.0 BTC.
    engine.foreign_claims = lambda symbol, venue: 0.9
    assert asyncio.run(engine._settle_ended_position(campaign)) is True
    assert campaign.exchange_qty == 0.1  # only a tenth was ever ours
    assert campaign.position_missing_notice  # and it said so


def test_the_hook_is_asked_for_the_right_symbol_and_venue():
    engine = _engine_with_holding(1.0)
    campaign = _ended_campaign(engine, filled=1.0)
    seen = []
    engine.foreign_claims = lambda symbol, venue: seen.append((symbol, venue)) or 0.0
    asyncio.run(engine._settle_ended_position(campaign))
    assert seen and seen[0][0] == "BTCUSDT"


def test_a_broken_hook_never_breaks_the_balance_check():
    """The sibling engine may be mid-construction, or simply broken."""
    engine = _engine_with_holding(1.0)
    campaign = _ended_campaign(engine, filled=1.0)

    def boom(symbol, venue):
        raise RuntimeError("sibling is down")

    engine.foreign_claims = boom
    assert asyncio.run(engine._settle_ended_position(campaign)) is False
    assert campaign.exchange_qty == 1.0  # fell back to the un-netted view


def test_a_negative_claim_cannot_inflate_our_share():
    engine = _engine_with_holding(1.0)
    campaign = _ended_campaign(engine, filled=1.0)
    engine.foreign_claims = lambda symbol, venue: -5.0
    asyncio.run(engine._settle_ended_position(campaign))
    assert campaign.exchange_qty == 1.0  # clamped at zero, not added back


def test_an_unset_hook_leaves_a_single_engine_exactly_as_it_was():
    engine = _engine_with_holding(0.5)
    campaign = _ended_campaign(engine, filled=0.5)
    assert engine.foreign_claims is None
    assert asyncio.run(engine._settle_ended_position(campaign)) is False
