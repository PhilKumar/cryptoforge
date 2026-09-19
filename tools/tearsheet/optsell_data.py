"""tools/tearsheet/optsell_data.py — the Option Seller tearsheet's numbers.

The other three sheets come out of run_backtests.py, which replays Binance
candles through the Cascade engines. The 4 PM option seller is a different
animal — Delta's daily BTC options, one trade a day at most — and its backtest
lives in the research harness that chose the rule (memory note
proj_delta_momentum_option_backtest). This reads that harness's VERIFIED output
and writes tools/tearsheet/data/optsell_report_data.json; nothing here
re-prices a trade.

    python3 tools/tearsheet/optsell_data.py ~/Documents/delta-momentum-research

Inputs, both written by the research harness:
  mom_sell_weekendcalm_slip0.json  the calm-weekend trades (19-Sep-2026), as
                                 rebuilt and checked by weekend_verify.py
  mom_sell_consensus_slip0.json  the 54 consensus trades, as rebuilt by
                                 momentum_bt.run_day and passed through all five
                                 checks by consensus_verify.py
  daytable.json                  every day's would-be trade at every lookback,
                                 used only to show what the SKIPPED days made

The rule is engine/option_seller_paper.py's, and the file is refused unless
its trades obey it (5 of 6 lookbacks agreeing, stop at twice the premium).
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO))

from engine import option_seller_paper as osp  # noqa: E402

OUT = _HERE / "data" / "optsell_report_data.json"


def _stats(trades):
    eq = peak = dd = 0.0
    for tr in sorted(trades, key=lambda x: x["day"]):
        eq += tr["net"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    n = len(trades)
    net = sum(tr["net"] for tr in trades)
    return {
        "trades": n,
        "net": round(net, 2),
        "win_pct": round(100.0 * sum(tr["net"] > 0 for tr in trades) / n, 1) if n else 0.0,
        "stops": sum(1 for tr in trades if tr["why"] == "stop"),
        "worst_run": round(dd, 2),
        "per_trade": round(net / n, 2) if n else 0.0,
    }


def _votes(rows_by_lb):
    up = sum(1 for lb in osp.LOOKBACKS_MIN if rows_by_lb[lb]["move"] >= osp.threshold_pct(lb))
    down = sum(1 for lb in osp.LOOKBACKS_MIN if rows_by_lb[lb]["move"] <= -osp.threshold_pct(lb))
    return up, down


def build(research: pathlib.Path) -> dict:
    harness = json.load(open(research / "mom_sell_consensus_slip0.json", encoding="utf-8"))
    table = json.load(open(research / "daytable.json", encoding="utf-8"))
    days = table["days"]
    if harness["stop"] != osp.STOP_MULT:
        raise SystemExit(f"harness stop {harness['stop']} is not the engine's {osp.STOP_MULT}")

    # Every day's rows at the engine's stop, keyed by lookback.
    by_day: dict = {}
    for r in table["rows"]:
        if r["stop"] == osp.STOP_MULT:
            by_day.setdefault(r["day"], {})[r["lb"]] = r

    def row(tr, votes, leg):
        spot = float(tr["underlying_at_entry"])
        return {
            "day": tr["day"],
            "leg": leg,
            "side": tr["side"],
            "symbol": tr["symbol"],
            "strike": tr["strike"],
            "btc": round(spot, 2),
            "votes": votes,
            "entry": round(tr["entry"], 4),
            "exit": round(tr["exit"], 4),
            "why": tr["why"],
            "fees": round(tr["fees"], 4),
            "net": round(tr["net"], 4),
            # Capital a live seller would tie up per 1 BTC: Delta's listed
            # 0.5% initial margin on the Bitcoin value, plus the premium.
            "capital": round(spot * osp.DEFAULT_IM_PCT / 100.0 + tr["entry"], 2),
        }

    trades = []
    for tr in sorted(harness["trades"], key=lambda x: x["day"]):
        up, down = _votes(by_day[tr["day"]])
        agreed = up if tr["side"] == "C" else down
        if agreed < osp.MIN_VOTES:
            raise SystemExit(f"{tr['day']}: only {agreed} windows agree — not the engine's rule")
        trades.append(row(tr, agreed, "strong"))
    strong_days = {x["day"] for x in trades}

    weekend = []
    wpath = research / "mom_sell_weekendcalm_slip0.json"
    if wpath.exists():
        for tr in sorted(json.load(open(wpath, encoding="utf-8"))["trades"], key=lambda x: x["day"]):
            up, down = _votes(by_day[tr["day"]])
            if dt.date.fromisoformat(tr["day"]).weekday() < 5 or up + down or tr["day"] in strong_days:
                raise SystemExit(f"{tr['day']}: not a calm weekend day — not the engine's weekend rule")
            weekend.append(row(tr, 0, "weekend-calm"))

    strong = trades
    trades = sorted(strong + weekend, key=lambda x: x["day"])
    cut = days[len(days) // 2]
    third, two_thirds = days[len(days) // 3], days[2 * len(days) // 3]
    splits = []
    for label_en, label_ta, at in (
        ("Chosen on the first half, scored on the second", "முதல் பாதியில் தேர்வு, இரண்டாம் பாதியில் சோதனை", cut),
        ("Split at one third instead", "மூன்றில் ஒரு பங்கில் பிரித்தால்", third),
        ("Split at two thirds instead", "மூன்றில் இரண்டு பங்கில் பிரித்தால்", two_thirds),
    ):
        splits.append(
            {
                "label_en": label_en,
                "label_ta": label_ta,
                "cut": at,
                "before": _stats([x for x in strong if x["day"] < at]),
                "after": _stats([x for x in strong if x["day"] >= at]),
            }
        )

    weekend_splits = [
        {
            "label_en": "Calm weekends, split at the half",
            "label_ta": "அமைதியான வார இறுதிகள், பாதியில் பிரித்தால்",
            "cut": cut,
            "before": _stats([x for x in weekend if x["day"] < cut]),
            "after": _stats([x for x in weekend if x["day"] >= cut]),
        }
    ]

    # Every kind of day at 4 PM, and what selling the same way made on it.
    groups = {"mixed": [], "calm_weekday": []}
    for day, rows in by_day.items():
        if not all(lb in rows for lb in osp.LOOKBACKS_MIN):
            continue
        up, down = _votes(rows)
        if up >= osp.MIN_VOTES or down >= osp.MIN_VOTES:
            continue  # traded by the strong rule
        if up + down == 0:
            if dt.date.fromisoformat(day).weekday() >= 5:
                continue  # traded by the weekend rule
            groups["calm_weekday"].append(rows[120])
        else:
            groups["mixed"].append(rows[120])

    def prem(rows):
        return round(sum(x["entry"] for x in rows) / max(1, len(rows)), 1)

    day_kinds = {
        "strong": _stats(strong),
        "weekend_calm": _stats(weekend),
        "mixed": _stats(groups["mixed"]),
        "calm_weekday": _stats(groups["calm_weekday"]),
        "avg_premium": {
            "strong": prem(strong),
            "weekend_calm": prem(weekend),
            "mixed": prem(groups["mixed"]),
            "calm_weekday": prem(groups["calm_weekday"]),
        },
    }

    monthly: dict = {}
    for x in trades:
        monthly[x["day"][:7]] = round(monthly.get(x["day"][:7], 0.0) + x["net"], 4)

    return {
        "generated": dt.datetime.now(osp.IST).strftime("%Y-%m-%d"),
        "first_day": days[0],
        "last_day": days[-1],
        "days": len(days),
        "rule": {
            "lookbacks_min": list(osp.LOOKBACKS_MIN),
            "thresholds_pct": {str(lb): round(osp.threshold_pct(lb), 3) for lb in osp.LOOKBACKS_MIN},
            "min_votes": osp.MIN_VOTES,
            "stop_mult": osp.STOP_MULT,
            "weekend_calm": True,
            "im_pct": osp.DEFAULT_IM_PCT,
        },
        "totals": _stats(trades),
        "avg_capital": round(sum(x["capital"] for x in trades) / len(trades), 2),
        "fees": round(sum(x["fees"] for x in trades), 2),
        "splits": splits,
        "weekend_splits": weekend_splits,
        "strong_totals": _stats(strong),
        "weekend_totals": _stats(weekend),
        "day_kinds": day_kinds,
        "monthly": monthly,
        "trades": trades,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    book = build(pathlib.Path(os.path.expanduser(sys.argv[1])))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(book, indent=1) + "\n", encoding="utf-8")
    t = book["totals"]
    print(f"{t['trades']} trades, net {t['net']:+,.2f} per 1 BTC, worst run {t['worst_run']:,.2f} → {OUT}")
    if not math.isclose(sum(book["monthly"].values()), t["net"], abs_tol=0.05):
        print("monthly book does not sum to the total")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
