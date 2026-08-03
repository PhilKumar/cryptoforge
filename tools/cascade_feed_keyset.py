#!/usr/bin/env python3
"""
Mint and revoke the Cascade feed's signing keys. Runs on your machine, never
on the server — it needs the root key, which is the one thing the server must
not have.

    Mint a feed key:  python3 tools/cascade_feed_keyset.py mint
    Revoke one:       python3 tools/cascade_feed_keyset.py revoke cf-feed-2026a
    Re-sign as-is:    python3 tools/cascade_feed_keyset.py renew

Each command writes two artefacts into ./out/ by default:

  feed_key_<kid>.pem   the server's signing key — scp to the box, 0600
  feed_keyset.json     the root-signed public document — scp to the state dir

Only the second one is public. The first is a secret the server needs and
nobody else does; the root key itself never leaves this machine.

`renew` exists because the signed document expires (30 days) even when the
keys inside it have not changed. That expiry is what makes a revocation stick
against someone replaying an older key set — see KEYSET_VALID_DAYS.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
from getpass import getpass

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cascade_feed import (  # noqa: E402
    ROOT_KID,
    FeedSigner,
    build_key_set,
    key_set_expiry_warning,
    sign_key_set,
    verify_key_set,
)

DEFAULT_ROOT = "~/.cryptoforge/feed_root_key.pem"
DEFAULT_OUT = "./out"
FEED_KEY_DAYS = 90


def _load_root(path: str) -> FeedSigner:
    resolved = os.path.abspath(os.path.expanduser(path))
    if not os.path.exists(resolved):
        sys.exit(f"No root key at {resolved}. Run tools/cascade_feed_rootkey.py generate first.")
    with open(resolved, "rb") as handle:
        pem = handle.read()
    key = serialization.load_pem_private_key(pem, password=getpass("Root key passphrase: ").encode("utf-8"))
    if not isinstance(key, Ed25519PrivateKey):
        sys.exit("That file is not an ed25519 root key.")
    return FeedSigner(ROOT_KID, key)


def _existing_keys(out_dir: str) -> tuple[list, list]:
    """Read the current key set so mint/revoke extend it rather than replace it."""
    path = os.path.join(out_dir, "feed_keyset.json")
    if not os.path.exists(path):
        return [], []
    with open(path, encoding="utf-8") as handle:
        document = json.loads(json.load(handle)["msg"])
    return list(document.get("keys") or []), list(document.get("revoked") or [])


def _write(out_dir: str, root: FeedSigner, keys: list, revoked: list) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    document = build_key_set(keys, revoked=revoked)
    frame = sign_key_set(document, root)
    # Verify what we are about to ship, with the same code the executor runs.
    # A key set that does not check out here would fail silently in the field.
    verify_key_set(frame, root.public_key_b64())
    path = os.path.join(out_dir, "feed_keyset.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(frame, handle, indent=2)
    print(f"\nSigned key set  -> {path}")
    print(f"  valid until   {time.strftime('%Y-%m-%d %H:%M IST', time.localtime(document['expires_at']))}")
    print(f"  active kids   {[k['kid'] for k in keys] or '(none)'}")
    print(f"  revoked       {revoked or '(none)'}")
    return document


def mint(args) -> int:
    root = _load_root(args.root)
    kid = args.kid or f"cf-feed-{time.strftime('%Y%m%d')}"
    keys, revoked = _existing_keys(args.out)
    if any(key.get("kid") == kid for key in keys):
        sys.exit(f"{kid} is already in the key set. Pass --kid with a different name.")

    feed_key = Ed25519PrivateKey.generate()
    signer = FeedSigner(kid, feed_key)
    now = int(time.time())
    keys.append(
        {
            "kid": kid,
            "public": signer.public_key_b64(),
            "not_before": now,
            "not_after": now + FEED_KEY_DAYS * 86400,
        }
    )

    os.makedirs(args.out, exist_ok=True)
    key_path = os.path.join(args.out, f"feed_key_{kid}.pem")
    if os.path.exists(key_path):
        sys.exit(f"{key_path} already exists — refusing to overwrite a signing key.")
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(fd, "wb") as handle:
        handle.write(
            feed_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    print(f"Feed signing key -> {key_path}  (0600, UNENCRYPTED — the server has to read it unattended)")
    _write(args.out, root, keys, revoked)
    print("\nNext: copy both files to the server, then restart. Keep neither on this machine.")
    return 0


def revoke(args) -> int:
    root = _load_root(args.root)
    keys, revoked = _existing_keys(args.out)
    if not keys:
        sys.exit(f"No key set in {args.out}. Nothing to revoke.")
    if args.kid not in {key.get("kid") for key in keys}:
        sys.exit(f"{args.kid} is not in the key set.")
    if args.kid in revoked:
        print(f"{args.kid} is already revoked. Re-signing anyway to refresh the expiry.")
    else:
        revoked.append(args.kid)
    _write(args.out, root, keys, revoked)
    print("\nDeploy this now. Executors pick it up within 24h of their next fetch;")
    print("until they do, the revoked key still verifies on machines holding the")
    print("older document. There is no way to make that faster than their cache.")
    return 0


def renew(args) -> int:
    root = _load_root(args.root)
    keys, revoked = _existing_keys(args.out)
    if not keys:
        sys.exit(f"No key set in {args.out}. Run `mint` first.")
    document = _write(args.out, root, keys, revoked)
    warning = key_set_expiry_warning(document)
    print(f"\n{warning}" if warning else "\nFresh for another 30 days.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=DEFAULT_ROOT, help=f"root key path (default {DEFAULT_ROOT})")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"where artefacts land (default {DEFAULT_OUT})")
    sub = parser.add_subparsers(dest="command", required=True)
    minter = sub.add_parser("mint", help="generate a new feed signing key and add it to the set")
    minter.add_argument("--kid", help="key id (default cf-feed-YYYYMMDD)")
    minter.set_defaults(func=mint)
    revoker = sub.add_parser("revoke", help="mark a kid revoked and re-sign")
    revoker.add_argument("kid")
    revoker.set_defaults(func=revoke)
    sub.add_parser("renew", help="re-sign the current set to refresh its expiry").set_defaults(func=renew)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
