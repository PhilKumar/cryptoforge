"""The two settings Auto-Cascade_Fib varies, and the promise that they change
NOTHING for a campaign that does not set them.

The live Cascade and Auto-Cascade_Fib share one engine, so the risk worth
testing is not "does the new setting work" but "does an unset campaign still
behave exactly as it did before". Both are checked here.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import engine.cascade as cascade  # noqa: E402


def _campaign(**kwargs) -> cascade.Campaign:
    base = dict(
        campaign_id="t1",
        symbol="BTCUSDT",
        capital_usd=2000.0,
        mother_high=100.0,
        mother_low=90.0,
        mother_timestamp=0,
    )
    base.update(kwargs)
    return cascade.Campaign(**base)


# ── the target ────────────────────────────────────────────────────


def test_unset_campaign_still_sells_at_the_module_level():
    """A campaign that names no level is the live Cascade's 0.25, untouched."""
    campaign = _campaign()
    campaign.avg_entry_price = 80.0
    # 80 + 0.25 x (100 - 80) = 85
    assert cascade.compute_tp_price(campaign) == pytest.approx(85.0)


def test_half_target_sells_half_way_back():
    campaign = _campaign(tp_fib_level=0.5)
    campaign.avg_entry_price = 80.0
    # 80 + 0.5 x (100 - 80) = 90
    assert cascade.compute_tp_price(campaign) == pytest.approx(90.0)


def test_a_zero_level_is_honoured_not_treated_as_unset():
    """0.0 is falsy — the code must test for None, or a zero target silently
    becomes 0.25 and the campaign sells somewhere nobody asked for."""
    campaign = _campaign(tp_fib_level=0.0)
    campaign.avg_entry_price = 80.0
    # The geometric target collapses to the entry, so the fee floor takes over
    # and the answer must be ABOVE the entry but nowhere near the 0.25 line.
    price = cascade.compute_tp_price(campaign)
    assert price is not None
    assert 80.0 < price < 85.0


def test_no_target_before_there_is_a_position():
    assert cascade.compute_tp_price(_campaign(tp_fib_level=0.5)) is None


# ── the ladder cap ────────────────────────────────────────────────


def test_unset_campaign_still_climbs_to_the_top_of_the_ladder():
    for rung in cascade.ESCALATION_LADDER[:-1]:
        assert _campaign(timeframe=rung).can_escalate is True
    assert _campaign(timeframe=cascade.ESCALATION_LADDER[-1]).can_escalate is False


def test_a_capped_campaign_stops_at_its_cap():
    assert _campaign(timeframe="1h", cap_timeframe="4h").can_escalate is True
    assert _campaign(timeframe="4h", cap_timeframe="4h").can_escalate is False


def test_reaching_the_cap_does_not_end_the_campaign():
    """The cap stops the ladder widening; it must not stop the trading. A
    campaign sitting on its cap is still active and still owns its position."""
    campaign = _campaign(timeframe="4h", cap_timeframe="4h")
    assert campaign.can_escalate is False
    assert campaign.state not in cascade.FINAL_STATES


def test_a_nonsense_cap_falls_back_to_the_full_ladder():
    """A typo must not silently freeze a campaign on 5m for life."""
    assert _campaign(timeframe="15m", cap_timeframe="4hours").can_escalate is True


def test_a_campaign_that_does_not_escalate_ignores_the_cap():
    assert _campaign(timeframe="5m", escalates=False, cap_timeframe="4h").can_escalate is False


# ── surviving a restart ───────────────────────────────────────────


def test_both_settings_survive_a_save_and_load():
    campaign = _campaign(tp_fib_level=0.5, cap_timeframe="4h")
    revived = cascade.Campaign.from_dict(campaign.to_dict())
    assert revived.tp_fib_level == 0.5
    assert revived.cap_timeframe == "4h"


def test_a_snapshot_written_before_these_settings_existed_still_loads():
    """Every campaign already saved on prod has neither key. It must load as
    the live Cascade's own behaviour, not as an Auto-Cascade_Fib campaign."""
    payload = _campaign().to_dict()
    payload.pop("tp_fib_level")
    payload.pop("cap_timeframe")
    revived = cascade.Campaign.from_dict(payload)
    assert revived.tp_fib_level is None
    assert revived.cap_timeframe == ""
    revived.avg_entry_price = 80.0
    assert cascade.compute_tp_price(revived) == pytest.approx(85.0)


# ── the wallet cap ────────────────────────────────────────────────
#
# Auto-Cascade_Fib's "never more than half the purse in coin" is not its own
# code — it sets the symbol's capital group and relies on the engine clamping
# funding to it. That is the safety property worth proving here, because the
# start-up alert says the group is "informational only", which reads like the
# cap does nothing.


def _leg(campaign, touch_high: float, low: float) -> cascade.Leg:
    leg = cascade.Leg(
        leg_id=len(campaign.legs) + 1,
        trendline_id=1,
        low=low,
        touch_high=touch_high,
        touch_timestamp=0,
    )
    campaign.legs.append(leg)
    return leg


def test_funding_is_clamped_to_what_the_group_has_left():
    """A campaign nominally sized $2,000 may not fund past a $1,000 group."""
    campaign = _campaign(capital_usd=2000.0, mother_high=200.0, mother_low=190.0)
    budget = 1000.0
    funded = 0.0
    price = 100.0
    # Walk a long fall, funding leg after leg, always telling the engine what
    # the group has left the way CascadeEngine.group_remaining_usd does.
    for _ in range(40):
        price *= 0.90
        leg = _leg(campaign, touch_high=price / 0.90, low=price)
        remaining = max(budget - campaign.cumulative_used_pct * campaign.capital_unit_per_pct, 0.0)
        cascade.build_fib_ladder_and_pool(campaign, leg, remaining)
        funded += leg.pool_usd
    assert funded <= budget + 0.01, f"funded ${funded:,.2f} against a ${budget:,.2f} group"
    assert campaign.cumulative_used_pct * campaign.capital_unit_per_pct <= budget + 0.01


def test_without_a_group_the_campaign_funds_its_whole_capital():
    """The clamp must only bite when a budget is actually set — the live
    Cascade's uncapped campaigns keep funding the way they always did."""
    campaign = _campaign(capital_usd=2000.0, mother_high=200.0, mother_low=190.0)
    price = 100.0
    funded = 0.0
    for _ in range(40):
        price *= 0.90
        leg = _leg(campaign, touch_high=price / 0.90, low=price)
        cascade.build_fib_ladder_and_pool(campaign, leg, None)
        funded += leg.pool_usd
    assert funded > 1000.0, "an uncapped campaign should fund well past a $1,000 group"


def test_a_capped_leg_records_what_it_could_not_fund():
    campaign = _campaign(capital_usd=2000.0, mother_high=200.0, mother_low=190.0)
    leg = _leg(campaign, touch_high=99.0, low=50.0)  # a 50% fall, far past the budget
    cascade.build_fib_ladder_and_pool(campaign, leg, 100.0)
    assert leg.capped_pct > 0
    assert leg.pool_usd <= 100.01
