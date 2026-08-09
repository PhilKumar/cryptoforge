"""The Emergency Stop button has to appear for every runtime it can stop.

It stops paper, live, SCALP and Cascade — but its visibility only ever asked
about paper and live RUNS. On 2026-08-09 the scalp engine was the only thing
running, so the button was hidden and there was no way to stop scalp from the
UI at all: there is no scalp-only endpoint, and Emergency Stop is the only
control that halts it.

That also froze deployment. `deploy/cd-deploy.sh` refuses to restart the
process while any runtime is live, so a hidden button meant every push failed
at the deploy step with "Active trading runtime detected on port 9000".

Tested by running the real function out of static/cryptoforge-app.js under
Node, against a stub DOM — a reimplementation would have agreed with the bug.
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

// Just enough DOM for the one element the function touches.
let hidden = true;
const button = {
  classList: { toggle: (_cls, on) => { hidden = !!on; }, contains: () => hidden },
};
global.document = { getElementById: (id) => (id === 'kill-switch-btn' ? button : null) };

var _cfRuntimeLive = { paper: false, live: false, scalp: false };
eval(extract('cfUpdateKillSwitch'));

const out = JSON.parse(process.argv[2]).map((patch) => {
  cfUpdateKillSwitch(patch);
  return !hidden;  // true = the button is visible
});
console.log(JSON.stringify(out));
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class KillSwitchVisibilityTests(unittest.TestCase):
    def visible_after(self, patches):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _APP_JS, json.dumps(patches)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            self.fail(f"node failed: {proc.stderr.strip()}")
        return json.loads(proc.stdout)

    def test_scalp_alone_shows_the_button(self):
        """The reported bug: scalp running, nothing else, no way to stop it."""
        clear, scalp = self.visible_after([{"paper": False, "live": False, "scalp": False}, {"scalp": True}])
        self.assertFalse(clear, "nothing running — the button stays out of the way")
        self.assertTrue(scalp, "scalp is running and Emergency Stop is the only thing that halts it")

    def test_cascade_alone_does_NOT_show_the_button(self):
        """Emergency Stop would halt cascade, but every campaign already has its
        own Stop on its strip — and cascade runs near-continuously. Keying off
        it parks a fixed, pulsing, z-index 9999 button over the top-right
        corner forever, which swallows clicks meant for the page beneath. Two
        strip tests failed on exactly that before this was narrowed."""
        clear, cascade = self.visible_after([{}, {"cascade": True}])
        self.assertFalse(clear)
        self.assertFalse(cascade, "cascade must not summon a permanent overlay")

    def test_paper_and_live_still_show_it(self):
        """The behaviour that already worked must keep working."""
        paper, off, live = self.visible_after([{"paper": True}, {"paper": False}, {"live": True}])
        self.assertTrue(paper)
        self.assertFalse(off)
        self.assertTrue(live)

    def test_each_poll_reports_only_its_own_engine(self):
        """The live poll knows nothing about scalp. Before this was a shared
        picture, whichever poll ran last decided the answer for everything —
        so the live poll saying "no runs" would re-hide a running scalp."""
        _, scalp_on, live_poll_says_nothing = self.visible_after(
            [{"scalp": False}, {"scalp": True}, {"paper": False, "live": False}]
        )
        self.assertTrue(scalp_on)
        self.assertTrue(live_poll_says_nothing, "a poll that cannot see scalp must not hide the button")

    def test_the_button_hides_only_when_everything_is_down(self):
        states = self.visible_after(
            [
                {"paper": True, "live": True, "scalp": True},
                {"paper": False},
                {"live": False},
                {"scalp": False},
            ]
        )
        self.assertEqual(states, [True, True, True, False])


if __name__ == "__main__":
    unittest.main()
