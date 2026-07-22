"""Shared test setup.

Keeps the suite independent of whatever is in the developer's .env.
"""

import pytest

import app as app_module


@pytest.fixture(autouse=True)
def _totp_disabled_by_default(request):
    """Turn the second factor OFF unless a test explicitly asks for it.

    config.py calls load_dotenv() at import, so app.TOTP_SECRET is whatever the
    developer happens to have in .env. The moment a real CRYPTOFORGE_TOTP_SECRET
    was added there, 33 tests started failing — every one that logs in with
    `{"password": AUTH_PIN}` and no code, which is almost all of them. Nothing
    was wrong with the app; the suite was just reading the developer's
    environment.

    Tests that DO exercise the second factor patch TOTP_SECRET themselves in
    setUp, which runs after this fixture and therefore wins. They opt out by
    name so this fixture never fights them.
    """
    if request.cls is not None and getattr(request.cls, "wants_totp", False):
        yield
        return
    original = app_module.TOTP_SECRET
    app_module.TOTP_SECRET = ""
    try:
        yield
    finally:
        app_module.TOTP_SECRET = original
        app_module._totp_used_counters.clear()
