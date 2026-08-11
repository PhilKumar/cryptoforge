import json
import os
import tempfile
import unittest
from unittest.mock import patch

from engine import rule3070_paper


class _RunningThread:
    def is_alive(self):
        return True


class Rule3070InstrumentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out_patch = patch.object(rule3070_paper, "OUT", self.tmp.name)
        self.out_patch.start()

    def tearDown(self):
        self.out_patch.stop()
        self.tmp.cleanup()

    def test_six_supported_instruments_are_exposed(self):
        self.assertEqual(
            rule3070_paper.SUPPORTED_SYMBOLS,
            ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSDT"),
        )
        with self.assertRaises(ValueError):
            rule3070_paper.Rule3070PaperService("BNBUSDT")

    def test_each_instrument_uses_an_isolated_book_and_btc_keeps_legacy_paths(self):
        service = rule3070_paper.Rule3070PaperService()
        self.assertEqual(service.state_path, os.path.join(self.tmp.name, "paper_state.json"))
        self.assertEqual(service.journal_path, os.path.join(self.tmp.name, "paper_journal.jsonl"))

        btc_state = {"start_ts": 101, "seen": ["btc-fill"]}
        with open(service.state_path, "w") as fh:
            json.dump(btc_state, fh)

        eth = service.select_symbol("ethusdt")
        self.assertEqual(eth["symbol"], "ETHUSDT")
        self.assertEqual(service.state_path, os.path.join(self.tmp.name, "paper_state_ETHUSDT.json"))
        self.assertEqual(service.journal_path, os.path.join(self.tmp.name, "paper_journal_ETHUSDT.jsonl"))
        self.assertFalse(os.path.exists(service.state_path))

        with open(service.journal_path, "w") as fh:
            fh.write(json.dumps({"kind": "TARGET", "net": 4.25}) + "\n")
        service.select_symbol("BTCUSDT")
        service.select_symbol("ETHUSDT")
        self.assertEqual(service.status()["closed"], {"count": 1, "net": 4.25})

        with open(os.path.join(self.tmp.name, "paper_state.json")) as fh:
            self.assertEqual(json.load(fh), btc_state)

    def test_running_writer_cannot_be_retargeted(self):
        service = rule3070_paper.Rule3070PaperService("BTCUSDT")
        service._thread = _RunningThread()
        with self.assertRaisesRegex(RuntimeError, "Stop the BTCUSDT"):
            service.select_symbol("SOLUSDT")
        self.assertEqual(service.symbol, "BTCUSDT")

    def test_reset_archives_only_the_selected_instrument(self):
        service = rule3070_paper.Rule3070PaperService("ETHUSDT")
        btc_path = os.path.join(self.tmp.name, "paper_state.json")
        with open(btc_path, "w") as fh:
            json.dump({"start_ts": 11}, fh)
        with open(service.state_path, "w") as fh:
            json.dump({"start_ts": 22}, fh)

        result = service.reset()

        self.assertTrue(result["reset"])
        self.assertTrue(os.path.exists(btc_path))
        self.assertFalse(os.path.exists(service.state_path))
        self.assertTrue(os.path.exists(service.state_path + "." + result["archived_as"]))


if __name__ == "__main__":
    unittest.main()
