"""The live engine must wake on boot, not on the first page view.

_get_cascade_engine builds the engine lazily, and nothing on the boot path
touched it: /api/health and /api/ready both read the global rather than
construct it. So after the 2026-08-11 restart the three live campaigns sat
unwatched for 45 seconds until a browser opened the Cascade page — and with
nobody looking, indefinitely. A buy stop filling in that window leaves coin in
the account with no take-profit resting against it.
"""

import asyncio
import unittest
from importlib import import_module


class CascadeBootWakeTests(unittest.TestCase):
    def setUp(self):
        self.app = import_module("app")

    def test_lifespan_wakes_the_cascade_engine_without_any_request(self):
        calls = []

        async def drive():
            original = self.app._get_cascade_engine
            self.app._get_cascade_engine = lambda: calls.append("built")
            try:
                async with self.app._app_lifespan(None):
                    # Boot is deliberately a background task so a slow broker
                    # cannot hold up serving; give it a turn to run.
                    for _ in range(10):
                        if calls:
                            break
                        await asyncio.sleep(0)
            finally:
                self.app._get_cascade_engine = original

        asyncio.run(drive())
        self.assertEqual(calls, ["built"], "startup never constructed the cascade engine")

    def test_a_broker_failure_at_boot_cannot_stop_the_app_serving(self):
        """Fail closed on trading, never on the HTTP server."""
        served = []

        async def drive():
            original = self.app._get_cascade_engine

            def boom():
                raise RuntimeError("broker unreachable")

            self.app._get_cascade_engine = boom
            try:
                async with self.app._app_lifespan(None):
                    served.append("yes")
                    for _ in range(10):
                        await asyncio.sleep(0)
            finally:
                self.app._get_cascade_engine = original

        asyncio.run(drive())
        self.assertEqual(served, ["yes"])


class ScalpBootResumeTests(unittest.TestCase):
    """Scalp's monitor is what honours a target and a stop.

    It is built lazily like the cascade engine, so an open trade carried
    across a deploy was unwatched until somebody opened the Scalp tab. On
    2026-09-08 there were four deploys in a day.
    """

    def setUp(self):
        self.app = import_module("app")

    def _isolate_boot(self):
        """Stub every OTHER engine the boot path builds.

        Left real, they construct broker clients and leave live globals behind,
        and the next file's ops-restore test then refuses with "Active runtime
        detected". Only the scalp branch is under test here.
        """
        names = ["_get_cascade_engine", "_get_auto_fib_engine", "_get_vrule_engine"]
        saved = {name: getattr(self.app, name) for name in names}
        saved["_resume_rule3070_on_boot"] = self.app._resume_rule3070_on_boot

        async def noop():
            return None

        for name in names:
            setattr(self.app, name, lambda: None)
        self.app._resume_rule3070_on_boot = noop
        return saved

    def _restore_boot(self, saved):
        for name, value in saved.items():
            setattr(self.app, name, value)

    def _drive(self, runtime):
        built = []
        served = []

        async def drive():
            saved = self._isolate_boot()
            original_get = self.app._get_scalp_engine
            original_load = self.app._load_scalp_runtime
            self.app._load_scalp_runtime = lambda: runtime
            self.app._get_scalp_engine = lambda: built.append("built")
            try:
                async with self.app._app_lifespan(None):
                    served.append("yes")
                    for _ in range(10):
                        if built:
                            break
                        await asyncio.sleep(0)
            finally:
                self.app._get_scalp_engine = original_get
                self.app._load_scalp_runtime = original_load
                self._restore_boot(saved)

        asyncio.run(drive())
        return built, served

    def test_an_open_trade_is_restored_without_any_request(self):
        built, _ = self._drive({"open_trades": [{"trade_id": 1}], "pending_entries": []})
        self.assertEqual(built, ["built"], "boot never restored the open scalp book")

    def test_a_pending_entry_is_restored_too(self):
        built, _ = self._drive({"open_trades": [], "pending_entries": [{"entry_id": 7}]})
        self.assertEqual(built, ["built"])

    def test_an_idle_scalp_does_not_build_a_broker_client_on_every_boot(self):
        built, _ = self._drive({"open_trades": [], "pending_entries": []})
        self.assertEqual(built, [], "an empty scalp book must not construct the engine")

    def test_a_scalp_failure_at_boot_cannot_stop_the_app_serving(self):
        served = []

        async def drive():
            saved = self._isolate_boot()
            original = self.app._load_scalp_runtime

            def boom():
                raise RuntimeError("state store unreadable")

            self.app._load_scalp_runtime = boom
            try:
                async with self.app._app_lifespan(None):
                    served.append("yes")
                    for _ in range(10):
                        await asyncio.sleep(0)
            finally:
                self.app._load_scalp_runtime = original
                self._restore_boot(saved)

        asyncio.run(drive())
        self.assertEqual(served, ["yes"])


if __name__ == "__main__":
    unittest.main()
