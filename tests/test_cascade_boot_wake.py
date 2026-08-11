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


if __name__ == "__main__":
    unittest.main()
