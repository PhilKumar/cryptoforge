"""Only one /api/cascade/status may be in flight at a time.

The poll fires every 3s and the payload is ~87 KB. When the box is busy a
response outlasts the interval, so on 09-Sep-2026 the prod access log showed
FIVE of them landing inside the same second. Chrome allows six connections per
host, so those five starved the page: a "Got it" POST never got a socket and
died on its own 10s deadline — /api/notifications/ack appears nowhere in the
server log for that period, while Phil watched three "The server did not
answer" toasts stack up.

The real function is run under Node against stubs, because a reimplementation
would have agreed with the bug.
"""

import json
import os
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")

_HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');

// Handles `async function name(` as well as a bare one.
function extract(name) {
  const at = src.indexOf('function ' + name + '(');
  if (at === -1) throw new Error('not found: ' + name);
  const start = src.slice(0, at).endsWith('async ') ? at - 'async '.length : at;
  let i = src.indexOf('{', at), depth = 0;
  for (;; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}') { depth--; if (depth === 0) break; }
  }
  return src.slice(start, i + 1);
}

const HOLD_MS = 40;          // a response slower than the poll interval
let fetches = 0;
let toasts = [];
let renders = 0;

var _cfCascadeStatusInFlight = null;
var _cfCascadeLastStatus = null;

function cfApiFetch() {
  fetches++;
  return new Promise((resolve) => setTimeout(() => resolve({ ok: true }), HOLD_MS));
}
async function cfReadApiPayload() { return { status: 'ok', campaigns: [] }; }
function cfApiErrorDetail(_payload, fallback) { return fallback; }
function cfToast(message, tone) { toasts.push(tone + ':' + message); }
function _cfCascadeRememberStatus() {}
function cfRenderCascadeStatus() { renders++; }
async function _cfCascadeRefreshOpenCanvasChartFromPoll() {}

eval(extract('_cfLoadCascadeStatusOnce'));
eval(extract('cfLoadCascadeStatus'));

(async () => {
  // Five pollers firing while the first response is still on the wire.
  const first = await Promise.all([
    cfLoadCascadeStatus(false),
    cfLoadCascadeStatus(false),
    cfLoadCascadeStatus(false),
    cfLoadCascadeStatus(true),
    cfLoadCascadeStatus(false),
  ]);
  const duringBurst = fetches;
  // Once the burst has settled, the next poll must go out for real.
  await cfLoadCascadeStatus(false);
  console.log(JSON.stringify({
    duringBurst: duringBurst,
    afterBurst: fetches,
    results: first,
    renders: renders,
    toasts: toasts,
    settled: _cfCascadeStatusInFlight === null,
  }));
})();
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class CascadeStatusSingleFlightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            [_NODE, "-e", _HARNESS, _APP_JS],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        cls.out = json.loads(result.stdout.strip().splitlines()[-1])

    def test_five_simultaneous_callers_open_one_request(self):
        self.assertEqual(
            self.out["duringBurst"],
            1,
            "the poll opened a second request while one was still in flight",
        )

    def test_every_caller_still_gets_an_answer(self):
        self.assertEqual(self.out["results"], [True, True, True, True, True])

    def test_a_hand_refresh_riding_along_is_still_told(self):
        self.assertIn("success:Cascade status refreshed", self.out["toasts"])

    def test_the_guard_clears_so_the_next_poll_goes_out(self):
        self.assertTrue(self.out["settled"], "the in-flight guard was left set")
        self.assertEqual(self.out["afterBurst"], 2, "the poll stopped fetching after the burst")

    def test_the_panel_is_painted_once_per_request_not_once_per_caller(self):
        self.assertEqual(self.out["renders"], 2)


if __name__ == "__main__":
    unittest.main()
