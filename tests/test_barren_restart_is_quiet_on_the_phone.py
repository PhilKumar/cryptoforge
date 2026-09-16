"""A suppressed restart must be quiet on the PHONE, not just in the alert stack.

17-Sep-2026, Phil: "Now I am getting loads of telegram alerts and site
alerts... Please resolve it immediately". Every Telegram sent in the hour
before he wrote came from one path: 11 "Auto-started from the break" lines and
8 "Restart N of a barren chain ... alert suppressed" lines, 19 events against
18 sends.

The suppression had been in place since 08-Aug and it worked — on `_alert`.
But a campaign's event log travels to the phone by a SECOND route: engine
`on_event` -> app._cascade_notify, which forwards any event whose level is
"error", "stop" or "start". Both lines above were logged at level "start", so
each suppressed restart still pushed two notifications, and `dedupe_key`
(which includes the message text) could never collapse them because the
campaign number and the restart count made every message unique.

CascadeRestartAlertNoiseTests in test_cascade_engine.py asserts on `on_alert`
only, which is why six tests were green while the phone was being flooded.
These tests assert on the channel that actually rang.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402
from engine.cascade import Campaign, Candle, CascadeEngine  # noqa: E402
from tests.test_cascade_engine import FakeCascadeBroker  # noqa: E402


class BarrenRestartIsQuietOnThePhoneTests(unittest.TestCase):
    def setUp(self):
        self.alerts = []
        self.events = []
        self.engine = CascadeEngine(
            FakeCascadeBroker(),
            on_alert=lambda title, body, level: self.alerts.append((title, body)),
            on_event=self.events.append,
        )
        self.parent = Campaign(
            campaign_id="p1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            seq=1,
            mode="paper",
            min_notional_usd=5.0,
            tick_size=0.01,
        )
        self.engine.campaigns["p1"] = self.parent

    def _chain(self, links):
        parent = self.parent
        for i in range(links):
            parent.close_reason = "mother_broken"
            child = self.engine._auto_restart(parent, Candle(3000 + i * 300, 1.0, 2.0, 0.5, 1.5))
            if child is None:
                return parent
            parent = child
        return parent

    def _pushed(self):
        """The events app.py would actually forward to Telegram and the stack."""
        return [e for e in self.events if str(e.get("level") or "").lower() in app_module._CASCADE_NOTIFY_LEVELS]

    def test_a_nine_link_barren_chain_pushes_only_the_head(self):
        self._chain(9)
        pushed = self._pushed()
        self.assertLessEqual(
            len(pushed),
            1,
            "a barren chain must ring the phone once, not per link; got "
            + repr([(e["level"], e["message"][:60]) for e in pushed]),
        )

    def test_the_suppression_notice_does_not_itself_notify(self):
        """The line that says "alert suppressed" must not be an alert."""
        self._chain(5)
        notices = [e for e in self.events if "alert suppressed" in str(e.get("message") or "")]
        self.assertTrue(notices, "expected the suppressed-restart notices to still be logged")
        for e in notices:
            self.assertNotIn(
                str(e.get("level") or "").lower(),
                app_module._CASCADE_NOTIFY_LEVELS,
                f"the suppression notice is being forwarded to the phone at level {e['level']!r}",
            )

    def test_a_suppressed_restart_is_still_written_to_the_event_log(self):
        """Silent on the phone, never invisible on the page."""
        self._chain(5)
        started = [e for e in self.events if "Auto-started from the break" in str(e.get("message") or "")]
        self.assertGreaterEqual(len(started), 4, "the restarts must still be logged for the Cascade page")

    def test_the_head_of_the_chain_still_reaches_the_phone(self):
        """Suppressing the tail must not suppress the announcement."""
        self._chain(3)
        self.assertEqual(
            len(self._pushed()), 1, "the first restart of a chain is the one event Phil does want to hear about"
        )

    def test_a_restart_that_drew_structure_rings_again(self):
        """barren resets to 0 when a link draws a fib, so the next one speaks."""
        self._chain(4)
        before = len(self._pushed())
        parent = [c for c in self.engine.campaigns.values() if c.campaign_id != "p1"][-1]
        parent.barren_chain = 0  # this link finally drew a fib
        parent.close_reason = "mother_broken"
        self.engine._auto_restart(parent, Candle(9000, 1.0, 2.0, 0.5, 1.5))
        self.assertEqual(len(self._pushed()), before + 1, "a restart after real structure must be announced")


if __name__ == "__main__":
    unittest.main()
