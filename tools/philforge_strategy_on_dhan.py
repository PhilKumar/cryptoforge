"""PE_NoTarget over the full Dhan archive, priced on real listed premiums."""

import json
import os
import sys

CF = "/Users/philipkumar/Documents/CryptoForge"
PF = os.environ.get("PF_ROOT", "/Users/philipkumar/Documents/PhilForge")
# ORDER MATTERS. Both repos ship an  and a  package; whichever
# comes first wins, and CryptoForge's engine.backtest is a different engine
# entirely. PhilForge must precede it.  has no __init__ in either, so it
# is a namespace package and merges -- which is how the Dhan calendar and index
# loader stay reachable from CryptoForge.
sys.path.insert(0, CF)
sys.path.insert(0, PF)

from engine.backtest import run_backtest  # noqa: E402
from tools.nifty_expiry_calendar import weekly_expiries  # noqa: E402
from tools.nifty_index_from_dhan import load_minutes, sessions  # noqa: E402
from tools.philforge_dhan_selector import DhanHistoricalPremiumSelector  # noqa: E402

STORES = {
    "e1": os.path.join(CF, "data", "dhan_options"),
    "e2": os.path.join(CF, "data", "dhan_options_e2"),
    "m1": os.path.join(CF, "data", "dhan_options_m1"),
    "m2": os.path.join(CF, "data", "dhan_options_m2"),
}


def main() -> None:
    cfg = dict(json.load(open(sys.argv[1])))
    out_path = sys.argv[2]
    from_date = sys.argv[3] if len(sys.argv) > 3 else "2021-01-01"
    to_date = sys.argv[4] if len(sys.argv) > 4 else "2026-08-31"

    minute = load_minutes()
    minute = minute.loc[from_date : to_date + " 23:59"]
    if minute.empty:
        raise SystemExit("no NIFTY minutes in that range")

    weeklies = weekly_expiries(sessions(minute))
    cfg["mode"] = "backtest"
    cfg["from_date"], cfg["to_date"] = from_date, to_date
    selector = DhanHistoricalPremiumSelector("26000", STORES, weeklies)
    cfg["_upstox_premium_selector"] = selector  # the key the engine reads

    result = run_backtest(minute, cfg["entry_conditions"], cfg["exit_conditions"], cfg)
    trades = result.get("trades", []) or []
    net = sum(float(t.get("pnl", 0) or 0) for t in trades)
    wins = sum(1 for t in trades if float(t.get("pnl", 0) or 0) > 0)
    reasons: dict = {}
    for t in trades:
        reasons[str(t.get("exit_reason", "?"))] = reasons.get(str(t.get("exit_reason", "?")), 0) + 1

    summary = {
        "range": [from_date, to_date],
        "bars": int(len(minute)),
        "trades": len(trades),
        "wins": wins,
        "net": round(net, 2),
        "exit_reasons": reasons,
        "selector": selector.report(),
        "trade_keys": [
            f"{str(t.get('entry_time'))[:16]}|{t.get('strike')}|{str(t.get('exit_time'))[:16]}|{round(float(t.get('pnl', 0) or 0), 2)}"
            for t in trades
        ],
    }
    json.dump(summary, open(out_path, "w"), indent=1)
    print(f"bars={summary['bars']} trades={summary['trades']} wins={wins} net={summary['net']:,.2f}")
    print("exit reasons:", reasons)
    print("selector:", selector.report())


if __name__ == "__main__":
    main()
