#!/usr/bin/env python3
"""
tools/totp_setup.py — generate the second-factor secret for CryptoForge login.

A 6-digit PIN is 10^6 combinations. The escalating lockout makes brute forcing
it impractical, but the PIN is still one secret standing between a stranger and
an app that places real orders. This adds a second one that changes every 30
seconds.

Run it yourself. The secret is printed on YOUR terminal and sent nowhere — no
network call, no file written, nothing logged. Paste it into .env as
CRYPTOFORGE_TOTP_SECRET and restart, then scan the URI with Google
Authenticator, Authy, 1Password, or any RFC 6238 app.

    venv/bin/python tools/totp_setup.py
    venv/bin/python tools/totp_setup.py --verify 123456   # after setting .env

Until CRYPTOFORGE_TOTP_SECRET is set, login behaves exactly as it does today.
"""

import argparse
import base64
import hashlib
import hmac
import os
import secrets
import struct
import time
from urllib.parse import quote

STEP_SEC = 30
DIGITS = 6


def code_at(secret_b32: str, counter: int) -> str:
    cleaned = secret_b32.strip().replace(" ", "").upper()
    key = base64.b32decode(cleaned + "=" * (-len(cleaned) % 8), casefold=True)
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    truncated = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(truncated % (10**DIGITS)).zfill(DIGITS)


def generate() -> int:
    # 160 bits, the RFC 4226 recommendation, base32 with no padding.
    secret = base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")
    label = quote("CryptoForge:crypto.philforge.in")
    uri = f"otpauth://totp/{label}?secret={secret}&issuer=CryptoForge&algorithm=SHA1&digits={DIGITS}&period={STEP_SEC}"

    print("\n  Second-factor secret — keep this off screen shares and chat logs.\n")
    print("  1. Add this line to .env:\n")
    print(f"       CRYPTOFORGE_TOTP_SECRET={secret}\n")
    print("  2. Add this account to your authenticator app, by URI or manual entry:\n")
    print(f"       {uri}\n")
    print(f"       manual entry — secret: {secret}")
    print(f"                      type:   time-based, {DIGITS} digits, {STEP_SEC}s\n")
    print("  3. Restart CryptoForge, then check the app agrees before you log out:\n")
    print("       venv/bin/python tools/totp_setup.py --verify <code-from-app>\n")
    print(f"  Right now the code should be:  {code_at(secret, int(time.time() // STEP_SEC))}")
    print("  (that one is for this secret — if you regenerate, it changes)\n")
    return 0


def verify(code: str) -> int:
    secret = (os.getenv("CRYPTOFORGE_TOTP_SECRET") or "").strip()
    if not secret:
        # Read .env directly: this tool runs outside the app, which is what
        # loads the file normally.
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        try:
            with open(env_path, encoding="utf-8") as handle:
                for line in handle:
                    if line.strip().startswith("CRYPTOFORGE_TOTP_SECRET="):
                        secret = line.split("=", 1)[1].strip()
                        break
        except OSError:
            pass
    if not secret:
        print("CRYPTOFORGE_TOTP_SECRET is not set in the environment or .env.")
        return 1

    digits = "".join(ch for ch in str(code) if ch.isdigit())
    if len(digits) != DIGITS:
        print(f"Expected a {DIGITS}-digit code, got {digits!r}.")
        return 1

    current = int(time.time() // STEP_SEC)
    for delta in (-1, 0, 1):
        if secrets.compare_digest(digits, code_at(secret, current + delta)):
            drift = {-1: " (one step slow — fine)", 0: "", 1: " (one step fast — fine)"}[delta]
            print(f"Code accepted{drift}. Your authenticator and the server agree.")
            return 0
    print("Code rejected. Check the phone's clock is set automatically, then try the next code.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--verify", metavar="CODE", help="check a code against the configured secret")
    args = parser.parse_args()
    return verify(args.verify) if args.verify else generate()


if __name__ == "__main__":
    raise SystemExit(main())
