"""
The closed-round ledger is pure JavaScript, so it is tested by running the real
function out of static/cryptoforge-app.js under Node rather than by
reimplementing its logic here — a reimplementation would have happily agreed
with the bug.

What it guards: archiving a campaign appends it to closed_campaigns WITHOUT
removing it from campaigns, so an ended campaign appears in both pools. The
ledger read both and listed every round twice. SOL #10 showed as two identical
$14.61 rows — one tagged "running", one "ended" — and the summary counted both:
$36.82 deployed / +$0.19 realised, when the truth was $22.21 / +$0.11.
"""

import json
import os
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")

# The two functions under test, lifted verbatim from the bundle.
_HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');

function extract(name) {
  const start = src.indexOf('function ' + name + '(');
  if (start === -1) throw new Error('not found: ' + name);
  let i = src.indexOf('{', start), depth = 0;
  for (;; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}') { depth--; if (depth === 0) break; }
  }
  return src.slice(start, i + 1);
}

eval(extract('_cfCascadeCampaignHasEnded'));
eval(extract('_cfCascadeCollectRounds'));

const status = JSON.parse(process.argv[2]);
const rows = _cfCascadeCollectRounds(status);
console.log(JSON.stringify(rows.map(r => ({
  campaign_id: r.campaign.campaign_id, round_id: r.round.round_id,
  symbol: r.symbol, ended: r.ended, invested: r.round.invested_usd, pnl: r.round.pnl
}))));
"""


def _round(round_id=1, invested=14.61, pnl=0.08):
    return {"round_id": round_id, "invested_usd": invested, "pnl": pnl, "closed_ts": 1000 + round_id}


@unittest.skipIf(_NODE is None, "node is not installed")
class CascadeLedgerDedupeTests(unittest.TestCase):
    def collect(self, status):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _APP_JS, json.dumps(status)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            self.fail(f"node failed: {proc.stderr.strip()}")
        return json.loads(proc.stdout)

    def test_an_ended_campaign_in_both_pools_is_listed_once(self):
        """The reported bug, as the payload that produced it."""
        sol = {"campaign_id": "sol10", "symbol": "SOLUSDT", "seq": 10, "state": "STOPPED", "rounds": [_round()]}
        rows = self.collect({"campaigns": [sol], "closed_campaigns": [dict(sol)]})
        self.assertEqual(len(rows), 1, f"one round, listed once — got {rows}")
        self.assertEqual(rows[0]["campaign_id"], "sol10")

    def test_totals_are_not_doubled(self):
        """What the user actually saw: $36.82 deployed and +$0.19 realised."""
        sol = {"campaign_id": "sol10", "symbol": "SOLUSDT", "state": "STOPPED", "rounds": [_round()]}
        btc = {
            "campaign_id": "btc36",
            "symbol": "BTCUSDT",
            "state": "TRENDLINE_ACTIVE",
            "rounds": [_round(1, 7.60, 0.03)],
        }
        rows = self.collect({"campaigns": [btc, sol], "closed_campaigns": [dict(sol)]})
        self.assertAlmostEqual(sum(r["invested"] for r in rows), 22.21, places=2)
        self.assertAlmostEqual(sum(r["pnl"] for r in rows), 0.11, places=2)

    def test_ended_flag_follows_state_not_which_pool(self):
        """A finished campaign found in `campaigns` still read "running"."""
        sol = {"campaign_id": "sol10", "symbol": "SOLUSDT", "state": "STOPPED", "rounds": [_round()]}
        rows = self.collect({"campaigns": [sol], "closed_campaigns": []})
        self.assertTrue(rows[0]["ended"], "STOPPED is ended wherever it was found")

        for state in ("MOTHER_BROKEN", "COMPLETED", "STOPPED"):
            got = self.collect({"campaigns": [{**sol, "state": state}], "closed_campaigns": []})
            self.assertTrue(got[0]["ended"], state)

    def test_a_running_campaigns_banked_rounds_still_show(self):
        """Dedupe must not become "only ended campaigns count" — a live campaign
        that has already banked rounds is history too."""
        btc = {
            "campaign_id": "btc36",
            "symbol": "BTCUSDT",
            "state": "TRENDLINE_ACTIVE",
            "rounds": [_round(1, 7.60, 0.03), _round(2, 8.10, 0.05)],
        }
        rows = self.collect({"campaigns": [btc], "closed_campaigns": []})
        self.assertEqual(len(rows), 2)
        self.assertFalse(any(r["ended"] for r in rows))

    def test_a_campaign_only_in_the_closed_pool_still_shows(self):
        """Old campaigns evicted from `campaigns` but kept in history."""
        old = {"campaign_id": "old1", "symbol": "SOLUSDT", "state": "COMPLETED", "rounds": [_round()]}
        rows = self.collect({"campaigns": [], "closed_campaigns": [old]})
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["ended"])

    def test_empty_and_malformed_input_does_not_throw(self):
        self.assertEqual(self.collect({}), [])
        self.assertEqual(self.collect({"campaigns": [], "closed_campaigns": []}), [])
        # a campaign with no id cannot be keyed, and must not crash the ledger
        self.assertEqual(self.collect({"campaigns": [{"symbol": "X", "rounds": [_round()]}]}), [])

    def test_rounds_stay_newest_first(self):
        a = {"campaign_id": "a", "symbol": "SOLUSDT", "state": "STOPPED", "rounds": [_round(1)]}
        b = {"campaign_id": "b", "symbol": "BTCUSDT", "state": "STOPPED", "rounds": [_round(5)]}
        rows = self.collect({"campaigns": [a, b], "closed_campaigns": []})
        self.assertEqual([r["round_id"] for r in rows], [5, 1])


if __name__ == "__main__":
    unittest.main()
