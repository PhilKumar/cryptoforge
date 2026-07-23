#!/usr/bin/env python3
"""
tools/cascade_reset.py — wipe persisted Cascade campaign state.

One-time housekeeping for an environment flip (testnet -> mainnet). The Open
Trades panel, the Closed Rounds table and the campaign event log are all rebuilt
on boot from three buckets in the state DB:

    cascade_runtime / current    -> active + restored campaigns (Open Trades)
    cascade_closed  / campaigns  -> closed history (Closed Rounds)
    cascade_events  / log        -> the campaign event log

After switching accounts those hold the OLD account's campaigns, which are
meaningless against the new one (and, if restored active, unsafe — testnet
positions and order ids reconciled against a real account). This clears exactly
those three buckets and NOTHING else: no orders are placed or cancelled, the
trade journal is untouched, and no other engine is affected. It reads and writes
the same state DB the running service uses.

Run it while the Cascade engine is Idle, then restart the service so it reloads
the now-empty state:

    venv/bin/python tools/cascade_reset.py           # dry run — shows what it would clear
    venv/bin/python tools/cascade_reset.py --yes     # actually clears the three buckets
    sudo systemctl restart cryptoforge@$(cat ~/.cryptoforge-active-port)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importing app resolves the SAME state DB path the service uses (env override
# included). It defines routes only — no server starts, no engine runs, no
# broker call is made at import — so this is a safe, read-then-write utility.
import app  # noqa: E402

_BUCKETS = [
    (app._BUCKET_CASCADE_RUNTIME, "current", "active / restored campaigns (Open Trades)"),
    (app._BUCKET_CASCADE_CLOSED, "campaigns", "closed history (Closed Rounds)"),
    (app._BUCKET_CASCADE_EVENTS, "log", "campaign event log"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="actually clear (default is a dry run)")
    args = parser.parse_args()

    store = app._get_state_store()
    print(f"State DB: {app._current_state_db_file()}\n")

    runtime = store.get(app._BUCKET_CASCADE_RUNTIME, "current", default={}) or {}
    closed = store.get(app._BUCKET_CASCADE_CLOSED, "campaigns", default=[]) or []
    events = store.get(app._BUCKET_CASCADE_EVENTS, "log", default=[]) or []
    active = runtime.get("campaigns") or []

    print("Currently persisted:")
    print(f"  active / restored campaigns : {len(active)}")
    print(f"  closed campaigns            : {len(closed)}")
    print(f"  event-log entries           : {len(events)}")
    for camp in active:
        print(f"    - #{camp.get('seq')} {camp.get('symbol')} {camp.get('state')} ({camp.get('mode')})")

    if not (active or closed or events):
        print("\nNothing persisted — already clean.")
        return 0

    if not args.yes:
        print("\nDry run. Re-run with --yes to clear all three buckets, then restart the service.")
        return 0

    for bucket, key, label in _BUCKETS:
        store.delete(bucket, key)
        print(f"cleared: {label}")

    print("\nDone. Restart the service so it reloads the now-empty state:")
    print("  sudo systemctl restart cryptoforge@$(cat ~/.cryptoforge-active-port)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
