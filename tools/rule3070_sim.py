"""tools/rule3070_sim.py — the 30-70 Rule: sequential geometry engine.

Phil's spec, as adjudicated across 2026-08-10 (eighth pass — corrected against
his TradingView screenshot of BTCUSDT 5m, 2026-08-07):

- MOTHER = the standing structure top, same law as his trendlines: the
  high-water mark (wick counts) since the last candle that CLOSED above it.
- The V is the small structure RIGHT AFTER the mother candle, not the deep
  bottom: price dips to a SWING LOW, bounces to a SWING HIGH (a lower top on
  a failed V, above the mother on an extra V, level on an equal V). That
  little V is the buyer involvement, and the fibs are drawn from it at once.
- Two fibs share the swing-low anchor, levels 0/1/2/4 only, projected down
  (level n = high - n x leg):
    Fib-S: swing low -> swing high
    Fib-B: swing low -> mother high
  REFERENCE = the upper of the two level-2s. NO size gate of any kind —
  "no small or large; the percent of fall is the one we are working here."
- First touch of the reference starts the measurement; a candle CLOSE below
  it arms the 30% buy.
- Entry: low + 0.25 x (mother - low) — a quarter of the whole fall back up,
  trailing every new low until it fills, never cancelled.
- The ladder: buys alternate 30, 70, 30, 70… downward. Every 30% buy
  re-anchors Fib-S's level 1 to its low (its level 2 = next 30% boundary);
  every 70% buy re-anchors Fib-B's level 1 to its low (its level 2 = next
  70% boundary). Band 1's 70% arms when the low from before the 30% breaks.
  Each buy must fill below the previous fill.
- TARGET = average buy + 0.25 x (mother - lowest low). Never below the
  average buy; deeper ladders earn a bigger target. One target closes all.
- CANCEL: a candle close above max(mother, swing high) before the first fill
  kills the setup (falling-market trade only). After a fill the target is
  always crossed first.
- MONEY: pot = fall% from mother x capital/100 measured at each fill
  (deepens with the fall, cascade-style), split 30/70, every order bumped to
  the Binance minimum of $5.5.
- SEQUENTIAL: when a campaign ends and the mother still stands, scanning
  resumes immediately — the next dip-and-bounce V under the same mother
  starts the next trade. A new wick top becomes the mother for future Vs
  without killing a running campaign (only its own close-above rule can).

Known conventions still awaiting Phil's confirmation: 5-bar swing pivots;
close-above (not wick) as the break; the intrabar rule that a bar both
making the low and reaching the entry must CLOSE through it to fill.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

PIVOT_SPAN = 2  # bars each side -> 5-bar swing, same as the fib-space engine
CAPITAL_USD = 200.0  # display sizing only; Phil's real account scale
SPLIT = (0.30, 0.70)
MIN_ORDER_USD = 5.5  # Binance minimum notional (Phil, 2026-08-10)
# Crash-avoidance experiments (2026-08-10), all OFF by default:
# TARGET_AT_FILL_ONLY — the target moves when a BUY fills, never on a bare
# new low, so it stops chasing a receding finish line down a crash.
TARGET_AT_FILL_ONLY = False
# ENFORCE_BUDGET — no buy may take total committed cost past CAPITAL_USD;
# the order stays armed and fills when money is free again. Without this,
# minors mode borrows infinitely and the numbers are fantasy.
ENFORCE_BUDGET = False
_COMMITTED = 0.0  # run_ladder refreshes this every bar
_MAJOR_COMMITTED = 0.0  # what the big trades hold; minors size off the rest
# BUDGET_FROM_TS — the paper clock. A replay that starts 30 days before the
# clock (engine/rule3070_paper) fills WARM-UP ladders on history nobody
# traded; they are shown, they never count as trades -- and until 2026-08-17
# they COUNTED AGAINST THE PURSE. Seven of them held $96.81 of BTC's $100 cap
# and refused every real buy for five days while price walked through nine
# armed orders (Phil: "Nothing works in this strategy"). A fill before this
# stamp is phantom money: it was never spent, so it cannot be tied up.
# None = every fill spends (the backtest and the CLI, which have no clock).
BUDGET_FROM_TS: Optional[pd.Timestamp] = None
# MAX_BANDS — a campaign stops adding buys past this band and simply holds
# for its target: the crash brake. Phil adjudicated 2026-08-11: "lets have
# 4 buys deeper" — 2 bands = 4 buys, then hold (0 = unlimited).
MAX_BANDS = 2
# MIN_NET_MARGIN — the fee gate: a buy waits until the expected win
# (a quarter of the fall, measured at the entry) is at least this fraction
# of price. Binance costs 0.2% round trip; below that a "win" is a donation.
# 0 = off. A deeper low raises the margin, so waiting is self-correcting.
MIN_NET_MARGIN = 0.0
# MAX_ACTIVE_MINORS — Phil's cascade orchestration (2026-08-11): ONE working
# line at a time. A campaign occupies the slot while hunting or freshly
# filled; once it has held past GRAD_HOLD (cascade's 200-bar 5m + 15m clock
# to 1h) it is a background MAJOR and the slot frees for the next line.
# 0 = unlimited (the original minors free-for-all).
MAX_ACTIVE_MINORS = 0
GRAD_HOLD = pd.Timedelta(hours=67)
# COMPOUND_AT_HALF — Phil's reinvestment rule (2026-08-11): closed profit
# banks up, and the moment the bank reaches 50% of the capital it folds IN —
# $2,000 earns $1,000 and the purse becomes $3,000 (next fold at $1,500).
# The growing purse is what lets minors keep working while bags hold the
# original capital. Fee-aware: profit banks NET of 0.1%/side.
COMPOUND_AT_HALF = False
# The fold schedule: the Nth fold happens when the bank reaches schedule[N]
# of capital; the last entry repeats forever. Phil's staged rule (2026-08-11):
# first fold at 50%, second at 100% (the purse doubles), then 25% thereafter.
COMPOUND_SCHEDULE = (0.5,)
# COMPOUND_PUMP — Phil's full money rule (2026-08-11): profit folds at 25%,
# and each time profit has grown the purse 50% past the cycle's start, HE
# PUMPS IN fresh money equal to 100% of the purse (doubling it). Pumps are
# DEPOSITS, not profit — _PUMPED_TOTAL keeps them honest in the report.
COMPOUND_PUMP = False
FEE_PER_SIDE = 0.001
_PROFIT_BANK = 0.0
_FOLD_IDX = 0
_CYCLE_BASE = 0.0
_PUMPED_TOTAL = 0.0
_PUMP_EVENTS: list = []
# Phil, 2026-08-11 evening, after the $1,053 audit: "one fall = one budget".
# Every ladder inside one fall (the major and all minors born under it) spends
# from the SAME envelope — fall% of capital measured from the major's mother to
# the family's lowest low — instead of each minor minting a fresh pot. This is
# the same accounting his live Cascade already uses: percent of the fall is
# funded once, never re-funded at the same levels; only a NEW low grows it.
FAMILY_BUDGET = False
# Fraction of the purse the whole book may deploy at once (1.0 = the old
# behaviour). Phil, 2026-08-11: "how much is 50% budget gate?" — at 0.5 the
# engine keeps half the purse in reserve no matter how deep the crash goes.
BUDGET_CAP_FRAC = 1.0
# And a new minor may only be born when every ladder of the fall has finished
# its 4 buys and is holding for its target — "only if the 4 buys done and
# target not hit, then only it goes for the next minor mother."
SPAWN_ONLY_EXHAUSTED = False
_FAMILIES: dict = {}
_NEXT_FAMILY = 0


def _new_family(top: float, low: float) -> int:
    global _NEXT_FAMILY
    _NEXT_FAMILY += 1
    _FAMILIES[_NEXT_FAMILY] = {"top": top, "low": low, "spent": 0.0}
    return _NEXT_FAMILY


# Scanner state at the end of the last run_ladder — read-only, for consoles
LAST_SCAN: dict = {}


@dataclass
class Fill:
    ts: pd.Timestamp
    price: float
    usd: float
    label: str


@dataclass
class Campaign:
    mother_ts: pd.Timestamp
    mother_high: float
    swing_low_ts: pd.Timestamp
    swing_low: float
    swing_high_ts: pd.Timestamp
    swing_high: float

    touch_ts: Optional[pd.Timestamp] = None
    trigger_ts: Optional[pd.Timestamp] = None
    lowest_low: float = 0.0
    lowest_low_ts: Optional[pd.Timestamp] = None
    ultimate_low: float = 0.0
    fibB_low_anchor: float = 0.0
    s2_line: float = 0.0
    b2_line: float = 0.0
    band_lines: List[tuple] = field(default_factory=list)  # (kind, band, price, armed_ts)
    fills: List[Fill] = field(default_factory=list)
    target: Optional[float] = None
    target_ts: Optional[pd.Timestamp] = None
    end_ts: Optional[pd.Timestamp] = None
    worst_dd_usd: float = 0.0  # deepest paper loss while the ladder was held
    capital_at_fill: float = 0.0  # the purse when the first buy landed
    is_minor: bool = False  # a bounce-top campaign inside a busy major
    family_id: int = 0  # the fall this ladder belongs to (one fall = one budget)
    # The bar whose first red confirmed the V — the bar this campaign was born
    # on. Stepping begins on the NEXT bar. Recorded so a live driver can pick
    # the campaign up at exactly the bar the rule did; it changes nothing here.
    born_ts: Optional[pd.Timestamp] = None
    status: str = "DETECTED"
    events: List[str] = field(default_factory=list)

    # live state while the ladder runs
    _pending: str = "30%"
    _band: int = 1
    _armed: bool = False
    _line: float = 0.0
    _touched: bool = False
    _exhausted: bool = False  # band cap reached; holding for target only

    @property
    def fibS2(self) -> float:
        return self.swing_high - 2 * (self.swing_high - self.swing_low)

    @property
    def fibB2(self) -> float:
        return self.mother_high - 2 * (self.mother_high - self.swing_low)

    @property
    def reference(self) -> float:
        return max(self.fibS2, self.fibB2)

    @property
    def v_type(self) -> str:
        if abs(self.swing_high - self.mother_high) / self.mother_high < 0.0005:
            return "equal V"
        return "extra V" if self.swing_high > self.mother_high else "failed V"

    def level(self, fib: str, n: float) -> float:
        if fib == "S":
            hi, lo = self.swing_high, self.swing_low
        else:
            hi, lo = self.mother_high, self.fibB_low_anchor or self.swing_low
        return hi - n * (hi - lo)

    @property
    def fall_pct(self) -> float:
        low = min(self.lowest_low or self.swing_low, self.swing_low)
        return (self.mother_high - low) / self.mother_high * 100

    @property
    def pot_usd(self) -> float:
        # Phil, 2026-08-10: past a 50% fall the fund measures at capital/50,
        # not capital/100 — the deep-crash pot funds twice as hard
        unit = CAPITAL_USD / 50.0 if self.fall_pct > 50 else CAPITAL_USD / 100.0
        return self.fall_pct * unit

    @property
    def avg_buy(self) -> float:
        usd = sum(f.usd for f in self.fills)
        return sum(f.price * f.usd for f in self.fills) / usd if usd else 0.0

    def entry_price(self) -> float:
        return self.lowest_low + 0.25 * (self.mother_high - self.lowest_low)

    def target_price(self) -> float:
        return self.avg_buy + 0.25 * (self.mother_high - self.lowest_low)

    def describe(self) -> str:
        def ist(ts):
            return ts.tz_convert("Asia/Kolkata").strftime("%Y-%m-%d %H:%M") if ts is not None else "—"

        lines = [
            f"mother {self.mother_high:.2f} @ {ist(self.mother_ts)}  [{self.v_type}]",
            f"swing low {self.swing_low:.2f} @ {ist(self.swing_low_ts)}   swing high {self.swing_high:.2f} @ {ist(self.swing_high_ts)}",
            f"S2 {self.fibS2:.2f}   B2 {self.fibB2:.2f}   reference {self.reference:.2f}",
            f"touch {ist(self.touch_ts)}   trigger close {ist(self.trigger_ts)}   lowest low {self.lowest_low:.2f}",
            f"fall from mother {self.fall_pct:.2f}%  ->  pot ${self.pot_usd:.2f} (30% ${self.pot_usd * SPLIT[0]:.2f} / 70% ${self.pot_usd * SPLIT[1]:.2f})",
        ]
        for f in self.fills:
            lines.append(f"fill {f.label} {f.price:.2f} @ {ist(f.ts)} (${f.usd:.2f})")
        if self.target and self.status == "TARGET HIT":
            lines.append(f"target {self.target:.2f} -> TARGET HIT @ {ist(self.target_ts)}")
        else:
            lines.append(f"status {self.status}")
        return "\n".join(lines)


def _spent(f: Fill) -> bool:
    """Whether this fill's money is really out of the purse (see BUDGET_FROM_TS)."""
    return BUDGET_FROM_TS is None or f.ts >= BUDGET_FROM_TS


def _step(c: Campaign, ts, o, h, lo, cl) -> bool:
    """Advance one closed bar. Returns False once the campaign has ended."""
    if not c.fills and cl > max(c.mother_high, c.swing_high):
        c.status = "CANCELLED (broke above the mother — wait for the next V)"
        c.end_ts = ts
        return False

    if not c._touched:
        # until the reference is first touched, the bounce may still be
        # growing: the swing high trails every new high (Phil, 2026-08-10:
        # the first red does not end the bounce — the top ~10 candles later
        # is the swing high), and the reference line moves with it
        if h > c.swing_high:
            c.swing_high = h
            c.swing_high_ts = ts
            c._line = c.reference
        if lo <= c.reference:
            c._touched = True
            c.touch_ts = ts
            c.lowest_low = lo
            c.lowest_low_ts = ts
        else:
            return True

    new_low = False
    if lo < c.lowest_low:
        c.lowest_low, c.lowest_low_ts, new_low = lo, ts, True
        fam = _FAMILIES.get(c.family_id)
        if fam is not None and lo < fam["low"]:
            fam["low"] = lo
        if c._band == 1 and c._pending == "70%" and c._armed:
            c.fibB_low_anchor = lo

    if c.fills:
        dd = sum(f.usd / f.price for f in c.fills) * lo - sum(f.usd for f in c.fills)
        if dd < c.worst_dd_usd:
            c.worst_dd_usd = dd

    if c._exhausted:
        # band cap reached: no more arming or buying — hold for the target
        if c.target and h >= c.target:
            c.status = "TARGET HIT"
            c.target_ts = ts
            c.end_ts = ts
            return False
        return True

    if not c._armed:
        if c._band == 1 and c._pending == "70%":
            if lo < c.ultimate_low:
                c._armed = True
                c.fibB_low_anchor = lo
                c.events.append("ultimate low broken — Fib-B stretched")
        elif cl < c._line:
            # the buy order goes in AFTER this close, so no same-bar fill
            c._armed = True
            if c.trigger_ts is None:
                c.trigger_ts = ts
            c.band_lines.append((c._pending, c._band, c._line, ts))
            if not TARGET_AT_FILL_ONLY:
                c.target = c.target_price() if c.fills else None
            return True
        if not c._armed:
            if c.fills and c.target and h >= c.target:
                c.status = "TARGET HIT"
                c.target_ts = ts
                c.end_ts = ts
                return False
            if not TARGET_AT_FILL_ONLY:
                c.target = c.target_price() if c.fills else None
            return True

    entry = c.entry_price()
    deep_enough = not c.fills or entry < c.fills[-1].price
    if MIN_NET_MARGIN and 0.25 * (c.mother_high - c.lowest_low) / entry < MIN_NET_MARGIN:
        deep_enough = False  # the win would not beat the fee — wait for a deeper low

    if deep_enough and h >= entry and (not new_low or cl >= entry):
        split = SPLIT[0] if c._pending == "30%" else SPLIT[1]
        # Funding base (Phil, 2026-08-11): the big trade measures off the
        # whole account; a MINOR measures off what the big trades have NOT
        # spent — "$2000, $500 on the 4 buys, the remaining $1500 for the
        # minor ones."
        base = CAPITAL_USD if not c.is_minor else max(CAPITAL_USD - _MAJOR_COMMITTED, 0.0)
        unit = base / 50.0 if c.fall_pct > 50 else base / 100.0
        usd = max(c.fall_pct * unit * split, MIN_ORDER_USD)
        if FAMILY_BUDGET:
            fam = _FAMILIES.get(c.family_id)
            if fam is not None:
                fam_fall = (fam["top"] - min(fam["low"], c.lowest_low)) / fam["top"] * 100.0
                fam_unit = CAPITAL_USD / 50.0 if fam_fall > 50 else CAPITAL_USD / 100.0
                remaining = fam_fall * fam_unit - fam["spent"]
                if remaining < MIN_ORDER_USD:
                    # this fall's money is fully spent — wait, but keep the
                    # target live (the PAXG deadlock lesson)
                    if not TARGET_AT_FILL_ONLY:
                        c.target = c.target_price() if c.fills else None
                    if c.fills and c.target and h >= c.target:
                        c.status = "TARGET HIT"
                        c.target_ts = ts
                        c.end_ts = ts
                        return False
                    return True
                usd = min(usd, remaining)
        if ENFORCE_BUDGET:
            global _COMMITTED
            if _COMMITTED + usd > CAPITAL_USD * BUDGET_CAP_FRAC:
                # No free money — the order stays armed and waits. But the
                # TARGET must stay live: skipping it here deadlocked a full
                # book (nobody could buy, and the sell that would free the
                # money was the very check being skipped — 56 PAXG trades sat
                # below target through a 100% gold rally, found 2026-08-11).
                if not TARGET_AT_FILL_ONLY:
                    c.target = c.target_price() if c.fills else None
                if c.fills and c.target and h >= c.target:
                    c.status = "TARGET HIT"
                    c.target_ts = ts
                    c.end_ts = ts
                    return False
                return True
            _COMMITTED += usd
        if not c.fills:
            c.capital_at_fill = CAPITAL_USD
        c.fills.append(Fill(ts, entry, usd, f"{c._pending} b{c._band}"))
        fam = _FAMILIES.get(c.family_id)
        if fam is not None:
            fam["spent"] += usd
        c.target = c.target_price()
        buy_low = c.lowest_low
        # The fibs SLIDE down with the buys keeping their ORIGINAL leg size
        # (Phil, 2026-08-10 ninth pass): level 1 moves onto this buy's low,
        # level 2 sits one original leg below it. Stretching the legs instead
        # compounded band over band until the next line lagged 35k under a
        # crash and the ladder went silent — his "no levels made after that".
        if c._pending == "30%":
            c.ultimate_low = buy_low
            c.s2_line = buy_low - (c.swing_high - c.swing_low)
            c._pending = "70%"
            c._line = c.b2_line if c._band > 1 else c.reference
        else:
            c.b2_line = buy_low - (c.mother_high - c.swing_low)
            c._band += 1
            c._pending = "30%"
            c._line = c.s2_line
            if MAX_BANDS and c._band > MAX_BANDS:
                c._exhausted = True  # crash brake: hold what we have, wait for target
        c._armed = False
        return True

    if not TARGET_AT_FILL_ONLY:
        c.target = c.target_price() if c.fills else None
    if c.fills and c.target and h >= c.target:
        c.status = "TARGET HIT"
        c.target_ts = ts
        c.end_ts = ts
        return False
    return True


def run_ladder(df: pd.DataFrame, minors: bool = False) -> List[Campaign]:
    """Walk the tape once: standing mothers, V detection, and every campaign,
    sequentially — a new V arms as soon as the previous trade under the same
    mother ends. Concurrent campaigns under older wick-mothers are allowed.

    `minors=True` (Phil's crash idea, 2026-08-10): while a campaign is busy
    under the standing mother, scanning continues — each further V starts a
    MINOR campaign whose mother is the local top since the last campaign
    began (the bounce top inside the fall), the way Phil live-scalps minor
    MCs in the cascade. Minors fund off their own small fall and cancel on a
    close above their own top.
    """
    o, h, lo, cl = df["open"].values, df["high"].values, df["low"].values, df["close"].values
    n = len(df)

    campaigns: List[Campaign] = []
    active: List[Campaign] = []

    stand_pos, stand_high = 0, h[0]
    scan_from = 1  # V scanning restarts here (new mother or campaign ended)
    # the forming V: a dip, then 2 consecutive green candles (the bounce),
    # confirmed by the first red candle — no pivot ceremony, no size rule
    dip_pos: Optional[int] = None
    dip_mother: Optional[tuple] = None  # (pos, high) mother snapshot at the dip
    green_run = 0
    in_bounce = False
    top_pos: Optional[int] = None  # highest point of the bounce so far
    # minors: the local top since the last campaign began — the bounce top a
    # minor campaign uses as ITS mother while the big one is busy
    local_pos, local_high = 0, h[0]
    dip_local: Optional[tuple] = None

    global _COMMITTED, _MAJOR_COMMITTED, _PROFIT_BANK, CAPITAL_USD, _FOLD_IDX, _CYCLE_BASE, _NEXT_FAMILY
    _FAMILIES.clear()
    _NEXT_FAMILY = 0
    _PROFIT_BANK = 0.0
    _FOLD_IDX = 0
    _CYCLE_BASE = CAPITAL_USD
    _PUMP_EVENTS.clear()
    for pos in range(1, n):
        ts = df.index[pos]

        # 1) step every running campaign. Only money actually spent is tied
        # up: a warm-up fill from before the paper clock never left the purse.
        _COMMITTED = sum(f.usd for c in active for f in c.fills if _spent(f))
        _MAJOR_COMMITTED = sum(f.usd for c in active if not c.is_minor for f in c.fills if _spent(f))
        still = []
        ended_current_mother = False
        for c in active:
            if _step(c, ts, None, h[pos], lo[pos], cl[pos]):
                still.append(c)
            else:
                if c.mother_ts == df.index[stand_pos]:
                    ended_current_mother = True
                if COMPOUND_AT_HALF and c.status == "TARGET HIT" and c.fills:
                    cost = sum(f.usd for f in c.fills)
                    qty = sum(f.usd / f.price for f in c.fills)
                    _PROFIT_BANK += qty * c.target - cost - FEE_PER_SIDE * (cost + qty * c.target)
                    fold = COMPOUND_SCHEDULE[min(_FOLD_IDX, len(COMPOUND_SCHEDULE) - 1)]
                    if _PROFIT_BANK >= fold * CAPITAL_USD:
                        CAPITAL_USD += _PROFIT_BANK
                        _PROFIT_BANK = 0.0
                        _FOLD_IDX += 1
                    if COMPOUND_PUMP and CAPITAL_USD >= 1.5 * _CYCLE_BASE:
                        # profit grew the purse 50% past the cycle start —
                        # Phil doubles it with fresh money and a new cycle begins
                        global _PUMPED_TOTAL
                        _PUMPED_TOTAL += CAPITAL_USD
                        _PUMP_EVENTS.append((ts, CAPITAL_USD))
                        CAPITAL_USD *= 2
                        _CYCLE_BASE = CAPITAL_USD
        active = still
        if ended_current_mother:
            scan_from, dip_pos, dip_mother = pos + 1, None, None
            green_run, in_bounce, top_pos = 0, False, None

        # 2) standing mother: a close above BREAKS it (the forming V dies
        # with it); a wick above only moves the mark for FUTURE structures —
        # the V already forming keeps its left mother, which is how an extra
        # V exists at all (the bounce's high becomes the next mother without
        # stealing the current V)
        if cl[pos] > stand_high:
            stand_pos, stand_high = pos, h[pos]
            local_pos, local_high = pos, h[pos]
            scan_from, dip_pos, dip_mother, dip_local = pos + 1, None, None, None
            green_run, in_bounce, top_pos = 0, False, None
            continue
        if h[pos] > stand_high:
            stand_pos, stand_high = pos, h[pos]
            if dip_pos is None:
                scan_from = pos + 1
        if h[pos] > local_high:
            local_pos, local_high = pos, h[pos]

        # 3) scan for the V (one campaign at a time under one mother):
        # a dip, then 2 green candles, confirmed by the first red
        busy = any(c.mother_ts == df.index[stand_pos] for c in active)
        if busy and not minors:
            continue
        if pos < scan_from:
            continue
        green = cl[pos] > o[pos]
        if in_bounce:
            if h[pos] > h[top_pos]:
                top_pos = pos
            if not green:
                # first red confirms the V — even if this same bar wicked a
                # hair under the dip (Phil keeps the V; 2026-08-07 19:15 IST
                # undercut the dip by $2.81 and his fib stayed on the dip)
                # While the standing mother is busy, this V trades as a MINOR
                # under the local bounce top instead.
                m_pos, m_high = dip_local if busy else dip_mother
                taken = {c.mother_ts for c in active}
                slot_free = True
                if busy and MAX_ACTIVE_MINORS:
                    ts_now = df.index[pos]
                    working = sum(1 for cc in active if not cc.fills or ts_now - cc.fills[0].ts <= GRAD_HOLD)
                    slot_free = working < MAX_ACTIVE_MINORS
                parent = None
                if busy:
                    parent = next((cc for cc in active if cc.mother_ts == df.index[stand_pos]), None)
                    if slot_free and SPAWN_ONLY_EXHAUSTED:
                        fam_id = parent.family_id if parent else None
                        kin = [cc for cc in active if fam_id is None or cc.family_id == fam_id]
                        # one working ladder per fall: the next minor is born
                        # only when every sibling has all 4 buys in and is
                        # holding for its target
                        if any(not cc._exhausted for cc in kin):
                            slot_free = False
                if slot_free and m_pos < dip_pos and df.index[m_pos] not in taken:
                    c = Campaign(
                        mother_ts=df.index[m_pos],
                        mother_high=m_high,
                        swing_low_ts=df.index[dip_pos],
                        swing_low=lo[dip_pos],
                        swing_high_ts=df.index[top_pos],
                        swing_high=h[top_pos],
                    )
                    c._line = c.reference
                    c.born_ts = df.index[pos]
                    c.is_minor = busy
                    if busy and parent is not None:
                        c.family_id = parent.family_id
                    else:
                        c.family_id = _new_family(m_high, lo[dip_pos])
                    campaigns.append(c)
                    active.append(c)
                    local_pos, local_high = pos, h[pos]
                dip_pos, dip_mother, dip_local = None, None, None
                green_run, in_bounce, top_pos = 0, False, None
                continue
        if dip_pos is None or lo[pos] < lo[dip_pos]:
            # a new low restarts the V — the dip deepened; a green dip
            # candle counts as the bounce's first green
            dip_pos = pos
            dip_mother = (stand_pos, stand_high)
            dip_local = (local_pos, local_high)
            green_run, in_bounce, top_pos = (1 if green else 0), False, None
            continue
        if not in_bounce:
            # 2 green candles SINCE THE DIP, not consecutive — Phil's Oct-7
            # bounce went green/red/green (the red even made the swing high)
            # and is a valid V; only a new low resets the count
            if green:
                green_run += 1
            if green_run >= 2:
                in_bounce = True
                top_pos = dip_pos + int(h[dip_pos : pos + 1].argmax())

    for c in active:
        c.status = (
            f"OPEN ({c._pending} of band {c._band} pending)" if c._touched or c.fills else "OPEN (waiting for touch)"
        )
        c.end_ts = df.index[-1]

    # What the scanner is looking at RIGHT NOW — the standing mother and how
    # far the next V has formed. Nothing here changes the run; it exists so a
    # console can show the wait instead of an empty screen.
    if dip_pos is None:
        stage = "watching for a dip under the mother"
    elif in_bounce:
        stage = "bouncing — the first red candle confirms the V"
    elif green_run >= 1:
        stage = f"dip in — {green_run} green so far, needs 2"
    else:
        stage = "dip in — waiting for the first green"
    LAST_SCAN.clear()
    LAST_SCAN.update(
        {
            "mother_ts": df.index[stand_pos],
            "mother_high": float(stand_high),
            "stage": stage,
            "dip_ts": df.index[dip_pos] if dip_pos is not None else None,
            "dip_low": float(lo[dip_pos]) if dip_pos is not None else None,
            "greens": int(green_run),
            "bar_ts": df.index[-1],
            "close": float(cl[-1]),
        }
    )
    return campaigns
