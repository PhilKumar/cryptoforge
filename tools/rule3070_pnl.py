"""tools/rule3070_pnl.py — money verdict for the 30-70 Rule.

Runs the adjudicated geometry over the cached BTC 5m history and prices every
campaign with the same fee the cascade engine models: 0.1% per side. A round
sells its whole ladder at the target, so:

    qty      = sum(usd / price) over fills
    gross    = qty * target - cost
    fees     = 0.1% * (cost + qty * target)
    net      = gross - fees

Open campaigns are marked to the last close and reported separately.
Capital usage = the maximum concurrent cost across overlapping campaigns.
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tools.rule3070_sim as sim  # noqa: E402
from tools.rule3070_sim import run_ladder  # noqa: E402

FEE = 0.001  # 0.1% per side, engine/cascade.py FEE_PCT_PER_SIDE
CACHE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "candles", "BTCUSDT_5m.pkl")


def load_symbol(symbol: str, months: int = 24) -> pd.DataFrame:
    """5m history from tools/.history_cache (fetch_5m_history)."""
    from tools.fetch_5m_history import load

    candles = load(symbol, months)
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close"])
    df.index = pd.to_datetime(df.pop("ts"), unit="s", utc=True)
    df.index.name = "datetime"
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=200.0)
    ap.add_argument("--minors", action="store_true", help="minor Vs under bounce tops while the major is busy")
    ap.add_argument("--target-at-fill", action="store_true", help="target moves only when a buy fills, not on new lows")
    ap.add_argument("--budget", action="store_true", help="no buy past CAPITAL_USD total committed")
    ap.add_argument("--max-bands", type=int, default=2, help="crash brake: stop buying past this band (0 = unlimited)")
    ap.add_argument(
        "--min-margin",
        type=float,
        default=0.0,
        help="fee gate: expected win must be at least this %% of price (e.g. 0.3)",
    )
    ap.add_argument("--symbol", default="", help="run on tools/.history_cache/<SYMBOL>_5m.json instead of the BTC pkl")
    ap.add_argument("--months", type=int, default=24, help="history depth for --symbol runs")
    ap.add_argument(
        "--compound",
        action="store_true",
        help="Phil's reinvestment rule: closed profit folds into the purse each time "
        "the bank reaches --compound-at %% of capital",
    )
    ap.add_argument(
        "--compound-at",
        default="50",
        help="fold schedule as %% of capital, comma-separated; last repeats (e.g. '50,100,25')",
    )
    ap.add_argument(
        "--pump",
        action="store_true",
        help="Phil's deposit rule: each time profit grows the purse 50%% past the cycle "
        "start, fresh money equal to 100%% of the purse is pumped in (doubling it)",
    )
    ap.add_argument(
        "--one-line",
        action="store_true",
        help="Phil's cascade orchestration: one working line at a time; a line held past "
        "the 1h clock is a background major and frees the slot",
    )
    args = ap.parse_args()
    if args.one_line:
        sim.MAX_ACTIVE_MINORS = 1
    sim.COMPOUND_AT_HALF = args.compound
    sim.COMPOUND_SCHEDULE = tuple(float(x) / 100.0 for x in str(args.compound_at).split(",") if x.strip())
    sim.COMPOUND_PUMP = args.pump
    sim._PUMPED_TOTAL = 0.0
    sim.MAX_BANDS = args.max_bands
    sim.MIN_NET_MARGIN = args.min_margin / 100.0
    sim.CAPITAL_USD = args.capital
    sim.TARGET_AT_FILL_ONLY = args.target_at_fill
    sim.ENFORCE_BUDGET = args.budget
    global CAPITAL_USD
    CAPITAL_USD = args.capital

    df = load_symbol(args.symbol, args.months) if args.symbol else pd.read_pickle(CACHE)  # nosec B301 - our own cache file
    campaigns = run_ladder(df, minors=args.minors)
    n_minor = sum(1 for c in campaigns if c.is_minor)
    if args.minors:
        print(f"minors mode: {n_minor} minor campaigns of {len(campaigns)} total")
    last_close = df["close"].iloc[-1]
    span_days = (df.index[-1] - df.index[0]).days

    closed, open_ = [], []
    events = []  # (ts, +cost) on fill, (ts, -cost) at end, for concurrency
    rows = []
    for c in campaigns:
        if not c.fills:
            continue
        cost = sum(f.usd for f in c.fills)
        qty = sum(f.usd / f.price for f in c.fills)
        for f in c.fills:
            events.append((f.ts, f.usd))
        events.append((c.end_ts, -cost))
        if c.status == "TARGET HIT":
            gross = qty * c.target - cost
            fees = FEE * (cost + qty * c.target)
            net = gross - fees
            days = (c.target_ts - c.fills[0].ts).total_seconds() / 86400
            closed.append((c, cost, net, days))
            rows.append((c.mother_ts, cost, gross, fees, net, days, c.worst_dd_usd, len(c.fills)))
        else:
            unreal = qty * last_close - cost
            open_.append((c, cost, unreal))

    total_net = sum(n for _, _, n, _ in closed)
    total_cost_traded = sum(cst for _, cst, _, _ in closed)
    losers = [(c, cst, n) for (c, cst, n, _) in closed if n < 0]
    worst_dd = min((c.worst_dd_usd for c, *_ in closed), default=0.0)

    # max concurrent committed cost
    events.sort(key=lambda e: (e[0], -e[1]))
    cur = peak = 0.0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)

    print(f"period: {df.index[0].date()} -> {df.index[-1].date()}  ({span_days} days)")
    print(
        f"campaigns with fills: {len(closed) + len(open_)}  |  closed at target: {len(closed)}  |  still open: {len(open_)}"
    )
    print(f"NET P&L (closed, after 0.1%/side fees): ${total_net:.2f}  on ${total_cost_traded:.2f} total traded cost")
    print(f"fee-eaten rounds (net < 0 despite target): {len(losers)}")
    for c, cst, n in losers:
        print(
            f"   {c.mother_ts.tz_convert('Asia/Kolkata'):%Y-%m-%d %H:%M} cost ${cst:.2f} net ${n:.2f} (fall {c.fall_pct:.2f}%)"
        )
    print(f"max concurrent capital committed: ${peak:.2f}  (account: ${CAPITAL_USD:.0f})")
    print(f"deepest paper loss while holding (any round): ${worst_dd:.2f}")
    yr = span_days / 365.25
    print(
        f"annualised: net ${total_net / yr:.2f}/yr  =  {total_net / yr / CAPITAL_USD * 100:.2f}% on the ${CAPITAL_USD:.0f} account, "
        f"{(total_net / yr / peak * 100) if peak else 0:.2f}% on peak committed capital"
    )
    for c, cost, unreal in open_:
        print(
            f"OPEN: mother {c.mother_ts.tz_convert('Asia/Kolkata'):%Y-%m-%d %H:%M}  cost ${cost:.2f}  unrealised ${unreal:.2f}  ({c.status})"
        )

    med = sorted(n for _, _, n, _ in closed)[len(closed) // 2] if closed else 0
    hold = sorted(d for _, _, _, d in closed)
    print(
        f"per-round net: median ${med:.2f}  |  hold time median {hold[len(hold) // 2]:.2f} d, max {max(hold):.1f} d"
        if hold
        else ""
    )

    # per-trade fund stats (a trade = one campaign's whole ladder)
    all_costs = [cst for _, cst, _, _ in closed] + [cst for _, cst, _ in open_]
    if all_costs:
        big = max(closed + [(c, cst, 0.0, 0.0) for c, cst, _ in open_], key=lambda r: r[1])
        print(
            f"max invested in a single trade: ${max(all_costs):.2f} "
            f"(mother {big[0].mother_ts.tz_convert('Asia/Kolkata'):%Y-%m-%d %H:%M}, fall {big[0].fall_pct:.1f}%)"
        )
        print(f"average invested per trade: ${sum(all_costs) / len(all_costs):.2f}")
    if closed:
        print(f"average profit per closed trade: ${total_net / len(closed):.4f}")
    # percent view: each trade against the purse it was funded from
    pcts = [(cst / c.capital_at_fill * 100, c) for c, cst, *_ in closed + open_ if c.capital_at_fill > 0]
    if pcts:
        mx, mc = max(pcts, key=lambda t: t[0])
        print(
            f"max % of purse in one trade: {mx:.2f}% "
            f"(mother {mc.mother_ts.tz_convert('Asia/Kolkata'):%Y-%m-%d %H:%M}, fall {mc.fall_pct:.1f}%, "
            f"purse ${mc.capital_at_fill:.0f})"
        )
        print(f"average % of purse per trade: {sum(p for p, _ in pcts) / len(pcts):.3f}%")
    if closed:
        margins = [n / cst * 100 for _, cst, n, _ in closed if cst > 0]
        print(f"average profit % per closed trade (net/cost): {sum(margins) / len(margins):.3f}%")
    deep = [c for c, *_ in closed] + [c for c, *_ in open_]
    n50 = sum(1 for c in deep if c.fall_pct > 50)
    print(f"trades whose fall passed 50% (funded at capital/50): {n50}")
    if args.compound:
        print(
            f"purse after reinvestment: ${sim.CAPITAL_USD:.2f} (started ${args.capital:.0f}, bank holds ${sim._PROFIT_BANK:.2f})"
        )
    if args.pump:
        print(f"PUMPED IN (your deposits, not profit): ${sim._PUMPED_TOTAL:.2f} across {len(sim._PUMP_EVENTS)} pumps")
        for ts, amt in sim._PUMP_EVENTS:
            print(
                f"   pump {ts.tz_convert('Asia/Kolkata'):%Y-%m-%d %H:%M}: +${amt:.2f} (purse doubled to ${amt * 2:.2f})"
            )

    # the law-of-large-numbers view: does every year earn, or one lucky year?
    by_year: dict = {}
    for c, _cst, n, _d in closed:
        year = c.target_ts.tz_convert("Asia/Kolkata").year
        wins, net = by_year.get(year, (0, 0.0))
        by_year[year] = (wins + 1, net + n)
    for year in sorted(by_year):
        wins, net = by_year[year]
        print(f"   {year}: {wins} wins closed, net ${net:.2f}")


if __name__ == "__main__":
    main()
