"""What the runtime snapshot has to carry across an engine restart.

A deploy restarts the live engine, so anything the snapshot drops is gone for
good. Journal rows find their campaign by matching buy order ids against the
fills the engine still holds, which makes closed-campaign retention the thing
standing between a finished trade and its chart button.
"""

import unittest
from importlib import import_module

from engine.cascade import CLOSED_HISTORY_LIMIT


class CascadeRuntimeSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.app = import_module("app")

    def test_snapshot_keeps_everything_the_engine_keeps(self):
        """The snapshot used to keep 20 while the engine kept 100.

        Every restart therefore threw away 80 closed campaigns, and the journal
        rows that pointed at them lost their chart for good.
        """
        closed = [{"campaign_id": f"c{i}"} for i in range(CLOSED_HISTORY_LIMIT + 50)]
        snapshot = self.app._snapshot_cascade_runtime({"closed_campaigns": closed})
        self.assertEqual(len(snapshot["closed_campaigns"]), CLOSED_HISTORY_LIMIT)
        # And it keeps the NEWEST ones, not the first that happened to be seen.
        self.assertEqual(snapshot["closed_campaigns"][-1]["campaign_id"], closed[-1]["campaign_id"])

    def test_a_short_history_is_not_padded_or_truncated(self):
        closed = [{"campaign_id": "c1"}, {"campaign_id": "c2"}]
        snapshot = self.app._snapshot_cascade_runtime({"closed_campaigns": closed})
        self.assertEqual([row["campaign_id"] for row in snapshot["closed_campaigns"]], ["c1", "c2"])

    def test_fills_survive_the_snapshot_so_the_journal_can_still_link(self):
        """The chart button depends on these order ids being here after a restart."""
        closed = [
            {
                "campaign_id": "c-77",
                "seq": 77,
                "all_fills": [
                    {"order_id": "OID-1", "price": 71.19, "quantity": 0.4, "level": 2, "leg_id": 1, "timestamp": 1}
                ],
                "rounds": [
                    {
                        "fills": [
                            {
                                "order_id": "OID-2",
                                "price": 70.0,
                                "quantity": 0.2,
                                "level": 4,
                                "leg_id": 1,
                                "timestamp": 2,
                            }
                        ]
                    }
                ],
            }
        ]
        snapshot = self.app._snapshot_cascade_runtime({"closed_campaigns": closed})
        row = snapshot["closed_campaigns"][0]
        self.assertEqual(row["all_fills"][0]["order_id"], "OID-1")
        self.assertEqual(row["rounds"][0]["fills"][0]["order_id"], "OID-2")


if __name__ == "__main__":
    unittest.main()
