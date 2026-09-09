"""A campaign action repaints the page it was taken on.

The campaign controls — stop, delete, recalculate, Market Sell — are shared by
all three strategy pages, but _cfCascadeAction only ever refreshed the LIVE
Cascade's status. So on 09-Sep-2026 a Market Sell on Cascade-Auto sold the
position, toasted success, and left the row on screen until Phil reloaded the
page by hand: "It is not going off after selling it but it stays."

Each strategy keeps its own engine and its own status endpoint, so the refresh
has to follow the page rather than assume the live one.
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

const active = process.argv[2];
const called = [];

const document = {
  getElementById: (id) => ({ classList: { contains: (c) => c === 'active-page' && id === active } }),
};
function cfAfRefresh() { called.push('auto'); }
function cfVrRefresh() { called.push('vrule'); }
function cfLoadCascadeStatus() { called.push('cascade'); }

eval(extract('_cfRefreshStrategyInView'));
_cfRefreshStrategyInView();
console.log(JSON.stringify(called));
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class ActionRefreshesItsOwnPageTests(unittest.TestCase):
    def _on(self, page_id):
        result = subprocess.run(
            [_NODE, "-e", _HARNESS, _APP_JS, page_id],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_on_the_auto_page_the_sandbox_is_refreshed(self):
        self.assertEqual(self._on("autofib-page"), ["auto"])

    def test_on_the_vrule_page_the_vrule_status_is_refreshed(self):
        self.assertEqual(self._on("rule3070-page"), ["vrule"])

    def test_on_the_cascade_page_nothing_changes(self):
        self.assertEqual(self._on("cascade-page"), ["cascade"])

    def test_anywhere_else_it_falls_back_to_the_live_cascade(self):
        self.assertEqual(self._on("journal-page"), ["cascade"])

    def test_the_shared_action_helper_uses_it(self):
        with open(_APP_JS, encoding="utf-8") as handle:
            js = handle.read()
        body = js[js.index("async function _cfCascadeAction(") :]
        body = body[: body.index("\n}\n")]
        self.assertIn("_cfRefreshStrategyInView();", body)
        self.assertNotIn("cfLoadCascadeStatus(false);", body, "it still assumes the live page")


if __name__ == "__main__":
    unittest.main()
