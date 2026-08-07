"""The first run, without a terminal.

Two properties carry this file. **Nothing is written until everything has been
checked** — a half-saved config is a machine that looks set up and is not, and
sends the buyer back to the text editor this page exists to replace. And **the
secret goes to one place only**: a key written to the credential store AND to
the config file has been put somewhere the buyer was never told about, next to
a sentence assuring them it is in the Keychain.
"""

import json
import os
import re
import tempfile
import unittest

from executor import secrets, setup
from executor.config import ExecutorConfig, load

GOOD = {
    "server_url": "https://crypto.philforge.in/",
    "buyer_id": "buyer-phil",
    "root_public_key": "tEWSMxMq9QgRd9FIHu26NLra+KbL0XF7uT11zpYQDTU=",
    "exchange": "binance",
    "capital_usd": "3000",
    "api_key": "AKAKAKAKAK",
    "api_secret": "SESESESESE",
}


class FieldTests(unittest.TestCase):
    def test_a_good_form_has_nothing_wrong_with_it(self):
        self.assertEqual(setup.check_fields(GOOD), [])

    def test_every_problem_is_reported_at_once(self):
        """Not the first. A buyer who fixes one field, saves, and is told about
        the next has been made to do the work four times."""
        fields = [field for field, _ in setup.check_fields({})]
        self.assertEqual(
            fields,
            ["server_url", "buyer_id", "root_public_key", "exchange", "capital_usd", "api_key", "api_secret"],
        )

    def test_capital_under_the_floor_is_refused_in_the_gates_own_words(self):
        """One rule, one sentence, wherever a buyer meets it."""
        problems = dict(setup.check_fields({**GOOD, "capital_usd": "200"}))
        self.assertIn("$1,000 minimum", problems["capital_usd"])

    def test_a_url_without_a_scheme_is_refused(self):
        problems = dict(setup.check_fields({**GOOD, "server_url": "crypto.philforge.in"}))
        self.assertIn("https://", problems["server_url"])

    def test_an_unknown_exchange_is_refused(self):
        problems = dict(setup.check_fields({**GOOD, "exchange": "kraken"}))
        self.assertIn("Binance or CoinDCX", problems["exchange"])


class WriteTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = os.path.join(self._dir.name, "config.json")
        self.stored = []

    def _store(self, buyer_id, key, secret):
        self.stored.append((buyer_id, key, secret))
        return "your macOS Keychain"

    def _ok(self, _config):
        return ""

    def test_a_good_form_writes_the_config_and_stores_the_key(self):
        result = setup.write_setup(GOOD, path=self.path, store=self._store, verify=self._ok)
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.stored, [("buyer-phil", "AKAKAKAKAK", "SESESESESE")])
        self.assertTrue(os.path.exists(self.path))

    def test_the_secret_is_never_in_the_config_file(self):
        setup.write_setup(GOOD, path=self.path, store=self._store, verify=self._ok)
        written = open(self.path, encoding="utf-8").read()
        self.assertNotIn("AKAKAKAKAK", written)
        self.assertNotIn("SESESESESE", written)
        self.assertNotIn("api_key", json.loads(written))

    def test_the_config_file_is_readable_by_this_user_only(self):
        setup.write_setup(GOOD, path=self.path, store=self._store, verify=self._ok)
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_a_bad_field_writes_nothing_at_all(self):
        result = setup.write_setup({**GOOD, "capital_usd": "10"}, path=self.path, store=self._store, verify=self._ok)
        self.assertFalse(result["ok"])
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual(self.stored, [])

    def test_credentials_the_exchange_refuses_write_nothing_at_all(self):
        """Checked against the venue BEFORE the disk is touched. A key with a
        missing character otherwise fails at the first tick, hours later, on a
        page the buyer has already walked away from."""
        result = setup.write_setup(
            GOOD, path=self.path, store=self._store, verify=lambda _c: "Your exchange refused these credentials: -2014"
        )
        self.assertFalse(result["ok"])
        self.assertIn("-2014", result["problems"][0]["message"])
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual(self.stored, [], "nothing may be stored for credentials the venue rejected")

    def test_no_credential_store_refuses_rather_than_writing_a_plaintext_key(self):
        """Telling a buyer their key is in the Keychain while putting it in a
        JSON file gives them a false idea of where their money's front door is."""

        def unavailable(*_args):
            raise secrets.SecretsUnavailable("nowhere safe to keep it")

        result = setup.write_setup(GOOD, path=self.path, store=unavailable, verify=self._ok)
        self.assertFalse(result["ok"])
        self.assertIn("nowhere safe", result["problems"][0]["message"])
        self.assertFalse(os.path.exists(self.path))

    def test_what_was_written_loads_back_and_is_valid(self):
        """The point of the page: the machine can start afterwards."""
        setup.write_setup(GOOD, path=self.path, store=self._store, verify=self._ok)
        config = load(self.path, environ={}, secrets_lookup=lambda _b: ("AKAKAKAKAK", "SESESESESE"))
        config.validate()
        self.assertEqual(config.buyer_id, "buyer-phil")
        self.assertEqual(config.capital_usd, 3000.0)
        self.assertFalse(setup.needs_setup(config))

    def test_the_trailing_slash_on_the_server_url_is_dropped(self):
        setup.write_setup(GOOD, path=self.path, store=self._store, verify=self._ok)
        self.assertEqual(json.load(open(self.path))["server_url"], "https://crypto.philforge.in")

    def test_the_signal_venue_follows_the_trading_venue(self):
        """Never a free choice — they fill at their own venue's prices."""
        setup.write_setup({**GOOD, "exchange": "coindcx"}, path=self.path, store=self._store, verify=self._ok)
        self.assertEqual(json.load(open(self.path))["signal_exchanges"], ["coindcx"])

    def test_a_timeframe_the_venue_cannot_carry_is_dropped(self):
        """CoinDCX serves nothing under 15m, so a 5m subscription there is a
        machine that 422s on every tick."""
        setup.write_setup(
            {**GOOD, "exchange": "coindcx", "timeframes": "5m,15m"}, path=self.path, store=self._store, verify=self._ok
        )
        self.assertEqual(json.load(open(self.path))["timeframes"], ["15m"])


class NeedsSetupTests(unittest.TestCase):
    """The question is "can this machine run", not "is there a file"."""

    def test_an_empty_config_needs_setup(self):
        self.assertTrue(setup.needs_setup(ExecutorConfig(server_url="", buyer_id="", root_public_key="")))

    def test_a_config_missing_only_its_key_still_needs_setup(self):
        config = ExecutorConfig(
            server_url="https://x", buyer_id="b", root_public_key="k", capital_usd=3000, api_secret="s"
        )
        self.assertTrue(setup.needs_setup(config))

    def test_a_complete_config_does_not(self):
        config = ExecutorConfig(
            server_url="https://x",
            buyer_id="b",
            root_public_key="k",
            capital_usd=3000,
            api_key="k",
            api_secret="s",
        )
        self.assertFalse(setup.needs_setup(config))


class CredentialPrecedenceTests(unittest.TestCase):
    """Environment, then the file, then the store — and both halves together."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = os.path.join(self._dir.name, "config.json")
        with open(self.path, "w", encoding="utf-8") as handle:
            json.dump({"server_url": "https://x", "buyer_id": "b", "root_public_key": "k"}, handle)

    def _load(self, environ, stored=("stored-key", "stored-secret")):
        return load(self.path, environ=environ, secrets_lookup=lambda _b: stored)

    def test_the_store_is_used_when_the_environment_is_silent(self):
        config = self._load({})
        self.assertEqual((config.api_key, config.api_secret), ("stored-key", "stored-secret"))

    def test_the_environment_wins_over_the_store(self):
        config = self._load({"CASCADE_API_KEY": "env-key", "CASCADE_API_SECRET": "env-secret"})
        self.assertEqual((config.api_key, config.api_secret), ("env-key", "env-secret"))

    def test_half_an_environment_pair_is_not_mixed_with_the_store(self):
        """A key from one place and a secret from another is a signature that
        will not verify, and the exchange's error for that says nothing."""
        config = self._load({"CASCADE_API_KEY": "env-key"})
        self.assertEqual((config.api_key, config.api_secret), ("stored-key", "stored-secret"))


class KeyringBackendTests(unittest.TestCase):
    """Which backend counts as a real one.

    The fail backend's class is called `Keyring`. So is the macOS one —
    `keyring.backends.macOS.Keyring`. A check written on the class NAME
    therefore rejects the real Keychain on every Mac, which is the machine this
    was written for, and the app reports "no password store" while sitting on
    one. Identity, never the name.
    """

    def setUp(self):
        import sys
        import types

        self._saved = {name: sys.modules.get(name) for name in list(sys.modules) if name.startswith("keyring")}
        self.addCleanup(self._restore)

        fail_mod = types.ModuleType("keyring.backends.fail")

        class Keyring:  # the FAIL backend, whose name collides with the real one
            priority = 0

        fail_mod.Keyring = Keyring

        backends = types.ModuleType("keyring.backends")
        backends.fail = fail_mod
        errors = types.ModuleType("keyring.errors")

        class NoKeyringError(Exception):
            pass

        errors.NoKeyringError = NoKeyringError

        self.keyring = types.ModuleType("keyring")
        self.keyring.backends = backends
        self.keyring.errors = errors
        self.fail_cls = Keyring

        sys.modules["keyring"] = self.keyring
        sys.modules["keyring.backends"] = backends
        sys.modules["keyring.backends.fail"] = fail_mod
        sys.modules["keyring.errors"] = errors
        os.environ.pop("CASCADE_NO_KEYRING", None)

    def _restore(self):
        import sys

        for name in [n for n in list(sys.modules) if n.startswith("keyring")]:
            del sys.modules[name]
        for name, module in self._saved.items():
            if module is not None:
                sys.modules[name] = module
        os.environ["CASCADE_NO_KEYRING"] = "1"

    def test_a_real_backend_named_keyring_is_accepted(self):
        """This is the macOS Keychain, and rejecting it was the bug."""

        class Keyring:  # same name as the fail backend, different class
            priority = 5

        self.keyring.get_keyring = lambda: Keyring()
        self.assertTrue(secrets.available())

    def test_the_fail_backend_is_rejected(self):
        """Writing into it succeeds and loses the value."""
        self.keyring.get_keyring = lambda: self.fail_cls()
        self.assertFalse(secrets.available())

    def test_a_chainer_with_nothing_behind_it_is_rejected(self):
        class Chainer:
            priority = 0

        self.keyring.get_keyring = lambda: Chainer()
        self.assertFalse(secrets.available())

    def test_the_kill_switch_wins_over_a_working_backend(self):
        """Tests must never reach into the login keychain of whoever runs them."""

        class Keyring:
            priority = 5

        self.keyring.get_keyring = lambda: Keyring()
        os.environ["CASCADE_NO_KEYRING"] = "1"
        self.assertFalse(secrets.available())


class PageScriptTests(unittest.TestCase):
    """The setup page's script must parse.

    It is one inline <script>, so a single syntax error kills the whole block —
    the key never appears, the copy button does nothing, and Save does nothing
    either. Nothing raises server-side, so the page looks fine from Python and
    is inert in the browser.

    The bug this was written for: `PAGE` is a normal triple-quoted string, not a
    raw one, so a `\\n` written for JavaScript is turned into a real newline by
    Python at import — which lands in the middle of a JS string literal.
    """

    def _script(self) -> str:
        body = setup.PAGE.split("<script>", 1)[1]
        return body.split("</script>", 1)[0]

    def test_no_string_literal_is_left_open_at_the_end_of_a_line(self):
        for number, line in enumerate(self._script().splitlines(), 1):
            for quote in ('"', "'"):
                unescaped = len(re.findall(r"(?<!\\)" + quote, line))
                self.assertEqual(
                    unescaped % 2,
                    0,
                    f"line {number} leaves a {quote} string open, so the whole script fails to parse: {line.strip()}",
                )

    def test_the_mail_body_carries_an_escaped_newline_not_a_real_one(self):
        self.assertIn("\\n", self._script(), "the JS must receive a backslash-n, not a line break")

    def test_the_brackets_balance(self):
        script = self._script()
        for opener, closer in (("(", ")"), ("{", "}"), ("[", "]")):
            self.assertEqual(script.count(opener), script.count(closer), f"unbalanced {opener}{closer}")

    def test_every_element_the_script_reaches_for_exists_in_the_markup(self):
        """A renamed id is the other way this page goes quietly inert."""
        for element_id in re.findall(r'getElementById\("([^"]+)"\)', self._script()):
            self.assertIn(f'id="{element_id}"', setup.PAGE, f"the script looks for #{element_id}, which is not there")


if __name__ == "__main__":
    unittest.main()
