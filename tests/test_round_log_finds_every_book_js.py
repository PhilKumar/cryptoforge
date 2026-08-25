"""The Log button has to find the round, whichever book drew it.

Phil, 2026-08-25: "The log button is not working on the paper journal".

A Log button carries a campaign_id and a round number and nothing else, and
_cfCascadeFindRound looked only in _cfCascadeLastStatus — the LIVE Cascade's
payload. Cascade-Auto's campaigns live in the sandbox engine and are never in
it, so every Log button on that page, in the new Paper Journal and on the
campaign cards alike, fell through to "That round is no longer in the current
status payload" and opened an empty box.

Exactly the shape of the earlier bug where Remove on that page reported success
and deleted nothing: the live engine has never heard of a strategy's campaign.
The fix is a registry every renderer writes to, so a third book cannot bring
the bug back by being forgotten.
"""

import json
import os
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")

_HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');
function grab(start, stop) {
  const s = src.indexOf(start);
  if (s < 0) throw new Error('MISSING: ' + start);
  const e = src.indexOf(stop, s + start.length);
  if (e < 0) throw new Error('MISSING STOP: ' + stop);
  eval.call(globalThis, src.slice(s, e));
}
grab('var _cfCascadeStatusPools = {};', 'var _cfCascadeOrigShowPage');
grab('function _cfCascadeFindRound(', 'function cfCascadeShowRoundLog');

const q = JSON.parse(process.argv[2]);
globalThis._cfCascadeLastStatus = q.live;
_cfCascadeRememberStatus('cascade', q.live);
const before = !!_cfCascadeFindRound(q.id, q.round);
if (q.registerSandbox) _cfCascadeRememberStatus('auto-fib', q.sandbox);
const found = _cfCascadeFindRound(q.id, q.round);
process.stdout.write(JSON.stringify({
  before: before,
  found: !!found,
  fills: found ? (found.round.fills || []).length : 0,
  symbol: found ? found.campaign.symbol : null,
}));
"""

LIVE = {
    "campaigns": [{"campaign_id": "live1", "symbol": "BTCUSDT", "rounds": [{"round_id": 1, "fills": [{"price": 1}]}]}],
    "closed_campaigns": [],
}
SANDBOX = {
    "campaigns": [
        {"campaign_id": "af9", "symbol": "SOLUSDT", "rounds": [{"round_id": 3, "fills": [{"price": 2}, {"price": 3}]}]}
    ],
    "closed_campaigns": [
        {"campaign_id": "af4", "symbol": "ETHUSDT", "rounds": [{"round_id": 1, "fills": [{"price": 9}]}]}
    ],
}


@unittest.skipUnless(_NODE, "node is not installed")
class RoundLogFindsEveryBookTests(unittest.TestCase):
    def _find(self, cid, round_id, register_sandbox=True):
        payload = {
            "live": LIVE,
            "sandbox": SANDBOX,
            "id": cid,
            "round": round_id,
            "registerSandbox": register_sandbox,
        }
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _JS, json.dumps(payload)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_a_sandbox_round_is_found_once_its_page_has_drawn(self):
        out = self._find("af9", 3)
        self.assertFalse(out["before"], "guard: unfindable until the sandbox registers — this WAS the bug")
        self.assertTrue(out["found"])
        self.assertEqual(out["fills"], 2)
        self.assertEqual(out["symbol"], "SOLUSDT")

    def test_a_closed_sandbox_campaign_is_found_too(self):
        """The Paper Journal lists rounds from ended lines as well as running ones."""
        out = self._find("af4", 1)
        self.assertTrue(out["found"])
        self.assertEqual(out["symbol"], "ETHUSDT")

    def test_the_live_cascade_still_resolves_exactly_as_before(self):
        out = self._find("live1", 1)
        self.assertTrue(out["before"], "the live book must never have needed the registry")
        self.assertTrue(out["found"])
        self.assertEqual(out["symbol"], "BTCUSDT")

    def test_an_unknown_campaign_still_finds_nothing(self):
        self.assertFalse(self._find("nope", 1)["found"])

    def test_the_round_number_still_has_to_match(self):
        """Scanning more pools must not turn into matching any round of that id."""
        self.assertFalse(self._find("af9", 99)["found"])


class EveryRendererRegistersItsStatusTests(unittest.TestCase):
    """A book that draws rounds and forgets to register brings the bug back."""

    def setUp(self):
        with open(_JS, encoding="utf-8") as fh:
            self.js = fh.read()

    def test_both_books_register(self):
        self.assertIn("_cfCascadeRememberStatus('cascade'", self.js)
        self.assertIn("_cfCascadeRememberStatus('auto-fib'", self.js)

    def test_the_sandbox_registers_before_it_draws(self):
        at_register = self.js.index("_cfCascadeRememberStatus('auto-fib'")
        at_draw = self.js.index("cfRenderCascadeLedger(data || {}, 'cf-af-ledger'")
        self.assertLess(at_register, at_draw, "register first, or the first paint's buttons are dead")
