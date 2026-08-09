"""The deploy gate decides whether it is safe to restart the live trading box.

It lives as a Python snippet embedded in `deploy/cd-deploy.sh`, which nothing
else covers — a shell heredoc is exactly where a safety rule rots unnoticed. So
these tests extract that snippet verbatim and run it against real
`/api/ready` shapes.

What it is protecting: a source deploy restarts the process and rebinds broker
clients, so it must never land on top of an open position or a resting order.
It is about POSITIONS AND ORDERS — not about which loops happen to be spinning.

On 2026-08-09 it also counted `scalp_running`, and since the scalp engine stays
up long after its last trade closes, it blocked every deploy for hours with
zero trades and zero pending entries to protect — while the only UI control
that could stop it was hidden. Four consecutive pushes went red on a green test
run. Two things came out of that: the gate ignores an idle scalp engine, and a
refusal now exits 75 so the workflow can report SKIPPED instead of failure.
"""

import json
import os
import re
import subprocess
import sys
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPT = os.path.join(_HERE, "deploy", "cd-deploy.sh")


def _extract_gate() -> str:
    """The decision snippet, exactly as the deploy runs it."""
    src = open(_SCRIPT, encoding="utf-8").read()
    match = re.search(r'"\$VENV/bin/python" -c \'\n(.*?)\n\'\n', src, re.DOTALL)
    if not match:
        raise AssertionError("could not find the runtime_is_active python snippet in cd-deploy.sh")
    return match.group(1)


def _blocks(runtime: dict) -> bool:
    """True when the gate refuses the deploy (snippet exits 0)."""
    proc = subprocess.run(
        [sys.executable, "-c", _extract_gate()],
        input=json.dumps({"runtime": runtime}),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode not in (0, 1):
        raise AssertionError(f"gate crashed ({proc.returncode}): {proc.stderr.strip()}")
    return proc.returncode == 0


class DeployGateTests(unittest.TestCase):
    def test_an_idle_scalp_engine_does_not_block(self):
        """The bug that cost an afternoon: running, but nothing to protect."""
        self.assertFalse(
            _blocks(
                {
                    "scalp_running": True,
                    "scalp_open_trades": 0,
                    "scalp_pending_entries": 0,
                    "cascade_active_campaigns": 0,
                    "live_running_runs": [],
                    "paper_running_runs": [],
                }
            ),
            "a scalp engine with no trades and no pending entries has nothing a restart could harm",
        )

    def test_real_exposure_still_blocks(self):
        for label, runtime in (
            ("scalp holding a trade", {"scalp_running": True, "scalp_open_trades": 1}),
            ("scalp pending entry", {"scalp_running": True, "scalp_pending_entries": 1}),
            ("live cascade campaigns", {"cascade_active_campaigns": 3}),
            ("a live engine run", {"live_running_runs": ["r1"]}),
            ("a paper engine run", {"paper_running_runs": ["r1"]}),
        ):
            with self.subTest(label):
                self.assertTrue(_blocks(runtime), f"{label} must still block a restart")

    def test_a_flat_book_deploys(self):
        self.assertFalse(_blocks({"scalp_running": False}))
        self.assertFalse(_blocks({}))

    def test_a_refusal_exits_75_not_1(self):
        """A deploy correctly declined is not a broken build. Exiting 1 painted
        four green test runs red, which teaches you to ignore the colour."""
        src = open(_SCRIPT, encoding="utf-8").read()
        self.assertIn("EXIT_DEPLOY_SKIPPED=75", src)
        self.assertRegex(src, r"skip\(\)\s*{[^}]*exit \"\$EXIT_DEPLOY_SKIPPED\"")
        # The trading-in-flight branch must use skip(), never die().
        branch = src.split('if [[ "$runtime_status" -eq 0 ]]; then', 1)[1].split("fi", 1)[0]
        self.assertIn("skip ", branch)
        self.assertNotIn("die ", branch)

    def test_a_genuine_deploy_fault_still_fails(self):
        """Only the trading-in-flight case is a skip. A worker that cannot be
        read at all is still blocked closed, and still red."""
        src = open(_SCRIPT, encoding="utf-8").read()
        branch = src.split('if [[ "$runtime_status" -eq 2 ]]', 1)[1].split("fi", 1)[0]
        self.assertIn("die ", branch)
        self.assertIn("exit 1", src.split("die()", 1)[1].split("\n", 1)[0])

    def test_both_workflows_treat_75_as_a_skip(self):
        """deploy.yml and playwright.yml both run the script; a skip must not
        be red in either, and neither may then claim the deploy was verified."""
        for name in ("deploy.yml", "playwright.yml"):
            with self.subTest(name):
                flow = open(os.path.join(_HERE, ".github", "workflows", name), encoding="utf-8").read()
                self.assertIn('"$status" -eq 75', flow, f"{name} does not special-case the skip code")
                self.assertIn("skipped=true", flow)
                self.assertIn("steps.deploy.outputs.skipped != 'true'", flow)


if __name__ == "__main__":
    unittest.main()
