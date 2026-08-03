#!/usr/bin/env python3
"""
Generate and inspect the Cascade feed's offline ROOT key.

The root key signs one thing: the key set that says which feed keys are valid.
The feed key on the server signs the actual messages. That split is the whole
point — if the server is ever compromised, the attacker holds a key we can
revoke, not the key that decides what "revoked" means.

So this key must never live on the server, never be committed, and never be
copied into a deploy. Two guards enforce that here: the tool refuses to write
anywhere inside the repo, and it refuses to overwrite an existing key — a
replaced root orphans every executor in the field, which is not a thing to do
by accident.

    Generate:  python3 tools/cascade_feed_rootkey.py generate
    Read back: python3 tools/cascade_feed_rootkey.py public

The public half it prints is what gets compiled into a buyer's executor.
"""

from __future__ import annotations

import argparse
import base64
import os
import stat
import sys
from getpass import getpass

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

DEFAULT_PATH = "~/.cryptoforge/feed_root_key.pem"
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve(path: str) -> str:
    resolved = os.path.abspath(os.path.expanduser(path))
    if os.path.commonpath([resolved, _REPO]) == _REPO:
        sys.exit(
            f"Refusing to write inside the repo ({resolved}).\n"
            "The root key must not be committable or deployable. Put it under "
            "your home directory, or straight into a password manager."
        )
    return resolved


def generate(path: str) -> int:
    resolved = _resolve(path)
    if os.path.exists(resolved):
        sys.exit(
            f"{resolved} already exists.\n"
            "A new root key orphans every executor already in the field — they "
            "trust the old public half and nothing will tell them otherwise. "
            "Move the old file aside deliberately if you really mean to."
        )

    passphrase = getpass("Passphrase for the root key (you will need it at every rotation): ")
    if len(passphrase) < 12:
        sys.exit("Use at least 12 characters. This key is the trust anchor for every buyer.")
    if passphrase != getpass("Again: "):
        sys.exit("Passphrases did not match.")

    key = Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(passphrase.encode("utf-8")),
    )

    os.makedirs(os.path.dirname(resolved), exist_ok=True)
    # Create 0600 from the start rather than chmod after — a world-readable
    # window, however short, is a window.
    fd = os.open(resolved, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(fd, "wb") as handle:
        handle.write(pem)

    print(f"\nRoot key written to {resolved} (0600, encrypted).")
    print(f"Public half:\n\n  {_public_b64(key)}\n")
    print("Next: put the file in your password manager as well, and keep the")
    print("passphrase somewhere separate from it. Nothing on the server ever")
    print("needs either one.")
    return 0


def public(path: str) -> int:
    resolved = _resolve(path)
    if not os.path.exists(resolved):
        sys.exit(f"No root key at {resolved}. Run `generate` first.")
    passphrase = getpass("Passphrase: ").encode("utf-8")
    with open(resolved, "rb") as handle:
        key = serialization.load_pem_private_key(handle.read(), password=passphrase)
    if not isinstance(key, Ed25519PrivateKey):
        sys.exit("That file is not an ed25519 root key.")
    print(_public_b64(key))
    return 0


def _public_b64(key: Ed25519PrivateKey) -> str:
    raw = key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(raw).decode("ascii")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("generate", "public"))
    parser.add_argument("--path", default=DEFAULT_PATH, help=f"where the key lives (default {DEFAULT_PATH})")
    args = parser.parse_args()
    return generate(args.path) if args.command == "generate" else public(args.path)


if __name__ == "__main__":
    raise SystemExit(main())
