"""tools/rule3070_paper.py — CLI for the 30-70 paper trader.

The logic lives in engine/rule3070_paper.py — the same service the site
console runs — so the CLI and the console can never disagree, and a pid
lockfile stops them writing the same journal at once.

    python tools/rule3070_paper.py --once     # one tick, then exit
    nohup python tools/rule3070_paper.py >> out/rule3070/paper.log 2>&1 &
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.rule3070_paper import Rule3070PaperService  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="one tick then exit")
    ap.add_argument("--symbol", default="BTCUSDT")
    args = ap.parse_args()
    Rule3070PaperService(symbol=args.symbol).run_foreground(once=args.once)
    return 0


if __name__ == "__main__":
    sys.exit(main())
