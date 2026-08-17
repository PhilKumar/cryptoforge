"""
webauthn_auth.py — passkeys (Face ID / fingerprint). Shared verbatim between PhilForge and CryptoForge.

WHY THIS IS HAND-WRITTEN RATHER THAN A LIBRARY
The obvious choice is the `webauthn` package, but it requires cryptography>=49
and the two Forge apps share one virtualenv on the box, with CryptoForge live
trading real money on a pinned cryptography. Forcing that upgrade to add a
login convenience is a bad trade. This file is IDENTICAL in both repos — change
it in one, copy it to the other. Everything needed here is a small, fully specified subset of
WebAuthn Level 2, and `cryptography` — already a dependency — does the actual
signature checking. Nothing security-critical is improvised: the parsing is
plain structure-walking and the verification is delegated.

WHAT IS AND IS NOT VERIFIED
Attestation is deliberately NOT verified. Attestation answers "which brand of
authenticator is this", which matters when an enterprise must exclude certain
hardware. Here the enrolling user is already logged in, so the device is
whoever they registered from, and every browser's platform authenticator is
acceptable. Everything that protects the LOGIN is checked in full: challenge,
origin, RP ID hash, user-presence and user-verification flags, the signature,
and the signature counter.

THE BIOMETRIC ITSELF NEVER LEAVES THE PHONE. Face ID / fingerprint unlocks a
private key held in the device's secure hardware; the server only ever sees a
public key and signatures. There is no fingerprint to leak here.
"""

import hashlib
import json
import logging
import secrets
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa

_logger = logging.getLogger(__name__)

# Flags in the authenticator-data byte.
FLAG_USER_PRESENT = 0x01
FLAG_USER_VERIFIED = 0x04
FLAG_ATTESTED_CREDENTIAL_DATA = 0x40

COSE_ES256 = -7
COSE_RS256 = -257
SUPPORTED_ALGORITHMS = (COSE_ES256, COSE_RS256)


class WebAuthnError(Exception):
    """A passkey ceremony that must be refused."""


# ── base64url, without the padding the spec omits ────────────────
def b64url_decode(value: str) -> bytes:
    padded = str(value or "") + "=" * (-len(str(value or "")) % 4)
    return urlsafe_b64decode(padded.encode("ascii"))


def b64url_encode(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


# ── A CBOR reader covering exactly what WebAuthn emits ───────────
# WebAuthn's attestation objects and COSE keys use unsigned ints, negative
# ints, byte strings, text strings, arrays, maps and the simple values. There
# are no tags, no indefinite lengths and no floats in this input, so anything
# outside that set is treated as malformed rather than guessed at.
def _cbor_read(data: bytes, offset: int) -> tuple[Any, int]:
    if offset >= len(data):
        raise WebAuthnError("CBOR ended early")
    initial = data[offset]
    major, minor = initial >> 5, initial & 0x1F
    offset += 1

    if minor < 24:
        value = minor
    elif minor == 24:
        value, offset = data[offset], offset + 1
    elif minor == 25:
        value, offset = int.from_bytes(data[offset : offset + 2], "big"), offset + 2
    elif minor == 26:
        value, offset = int.from_bytes(data[offset : offset + 4], "big"), offset + 4
    elif minor == 27:
        value, offset = int.from_bytes(data[offset : offset + 8], "big"), offset + 8
    else:
        raise WebAuthnError(f"unsupported CBOR length encoding {minor}")

    if major == 0:
        return value, offset
    if major == 1:
        return -1 - value, offset
    if major == 2:
        return data[offset : offset + value], offset + value
    if major == 3:
        return data[offset : offset + value].decode("utf-8", "replace"), offset + value
    if major == 4:
        items = []
        for _ in range(value):
            item, offset = _cbor_read(data, offset)
            items.append(item)
        return items, offset
    if major == 5:
        mapping: dict = {}
        for _ in range(value):
            key, offset = _cbor_read(data, offset)
            item, offset = _cbor_read(data, offset)
            mapping[key] = item
        return mapping, offset
    if major == 7:
        if minor == 20:
            return False, offset
        if minor == 21:
            return True, offset
        if minor == 22:
            return None, offset
        raise WebAuthnError(f"unsupported CBOR simple value {minor}")
    raise WebAuthnError(f"unsupported CBOR major type {major}")


def cbor_decode(data: bytes) -> Any:
    value, _ = _cbor_read(data, 0)
    return value


# ── COSE public key → a key `cryptography` can verify with ───────
def _load_cose_key(cose: dict):
    kty = cose.get(1)
    alg = cose.get(3)
    if alg not in SUPPORTED_ALGORITHMS:
        raise WebAuthnError(f"unsupported credential algorithm {alg}")
    if kty == 2:  # Elliptic curve — what every phone produces
        if cose.get(-1) != 1:
            raise WebAuthnError("only the P-256 curve is accepted")
        x = int.from_bytes(cose[-2], "big")
        y = int.from_bytes(cose[-3], "big")
        return ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1()).public_key(), alg
    if kty == 3:  # RSA — some Windows Hello configurations
        n = int.from_bytes(cose[-1], "big")
        e = int.from_bytes(cose[-2], "big")
        return rsa.RSAPublicNumbers(e, n).public_key(), alg
    raise WebAuthnError(f"unsupported key type {kty}")


def _verify_signature(public_key, alg: int, message: bytes, signature: bytes) -> None:
    try:
        if alg == COSE_ES256:
            public_key.verify(signature, message, ec.ECDSA(hashes.SHA256()))
        else:
            public_key.verify(signature, message, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise WebAuthnError("signature does not match this passkey") from exc


# ── Authenticator data ───────────────────────────────────────────
class AuthenticatorData:
    __slots__ = ("rp_id_hash", "flags", "sign_count", "credential_id", "cose_key")

    def __init__(self, raw: bytes, *, expect_credential: bool):
        if len(raw) < 37:
            raise WebAuthnError("authenticator data is too short")
        self.rp_id_hash = raw[0:32]
        self.flags = raw[32]
        self.sign_count = int.from_bytes(raw[33:37], "big")
        self.credential_id = b""
        self.cose_key: dict = {}
        if not expect_credential:
            return
        if not self.flags & FLAG_ATTESTED_CREDENTIAL_DATA:
            raise WebAuthnError("registration carried no credential data")
        if len(raw) < 55:
            raise WebAuthnError("credential data is truncated")
        id_length = int.from_bytes(raw[53:55], "big")
        self.credential_id = raw[55 : 55 + id_length]
        if len(self.credential_id) != id_length:
            raise WebAuthnError("credential id is truncated")
        self.cose_key = cbor_decode(raw[55 + id_length :])


def _check_client_data(
    client_data_json: bytes, *, expected_type: str, expected_challenge: bytes, expected_origin: str
) -> None:
    try:
        client_data = json.loads(client_data_json.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WebAuthnError("client data is not readable JSON") from exc
    if client_data.get("type") != expected_type:
        raise WebAuthnError(f"expected a {expected_type} ceremony")
    # secrets.compare_digest: the challenge is the replay defence, so it is
    # compared without leaking where two values first differ.
    if not secrets.compare_digest(b64url_decode(client_data.get("challenge", "")), expected_challenge):
        raise WebAuthnError("this challenge is not the one we issued")
    if str(client_data.get("origin", "")).rstrip("/") != expected_origin.rstrip("/"):
        raise WebAuthnError("this passkey was used from the wrong site")


def _check_authenticator(auth_data: AuthenticatorData, *, rp_id: str, require_user_verification: bool) -> None:
    if not secrets.compare_digest(auth_data.rp_id_hash, hashlib.sha256(rp_id.encode("utf-8")).digest()):
        raise WebAuthnError("this passkey belongs to a different site")
    if not auth_data.flags & FLAG_USER_PRESENT:
        raise WebAuthnError("the authenticator reported nobody present")
    # This is the bit that makes it BIOMETRIC rather than merely "a device you
    # hold". Without it a passkey would unlock on possession alone.
    if require_user_verification and not auth_data.flags & FLAG_USER_VERIFIED:
        raise WebAuthnError("unlock with your fingerprint, face or device PIN")


def new_challenge() -> bytes:
    return secrets.token_bytes(32)


def registration_options(*, rp_id: str, rp_name: str, user_id: int, username: str, existing_ids: list[str]) -> dict:
    """Options for navigator.credentials.create(), minus the challenge."""
    return {
        "rp": {"id": rp_id, "name": rp_name},
        "user": {
            # An opaque handle, never the username: it is stored on the device
            # and can be shown on other people's account pickers.
            "id": b64url_encode(str(user_id).encode("utf-8")),
            "name": username,
            "displayName": username,
        },
        "pubKeyCredParams": [{"type": "public-key", "alg": alg} for alg in SUPPORTED_ALGORITHMS],
        "timeout": 120000,
        "attestation": "none",
        "authenticatorSelection": {
            # "platform" = the fingerprint/face reader built into this phone or
            # laptop, not a USB key. residentKey so the phone can offer the
            # account without anyone typing a username first.
            "authenticatorAttachment": "platform",
            "residentKey": "preferred",
            "userVerification": "required",
        },
        "excludeCredentials": [{"type": "public-key", "id": cid} for cid in existing_ids],
    }


def verify_registration(*, credential: dict, expected_challenge: bytes, rp_id: str, origin: str) -> dict:
    """Check a navigator.credentials.create() result. Returns what to store."""
    response = credential.get("response") or {}
    _check_client_data(
        b64url_decode(response.get("clientDataJSON", "")),
        expected_type="webauthn.create",
        expected_challenge=expected_challenge,
        expected_origin=origin,
    )
    attestation = cbor_decode(b64url_decode(response.get("attestationObject", "")))
    auth_data = AuthenticatorData(attestation["authData"], expect_credential=True)
    _check_authenticator(auth_data, rp_id=rp_id, require_user_verification=True)
    _load_cose_key(auth_data.cose_key)  # reject a key we could never verify with later
    return {
        "credential_id": b64url_encode(auth_data.credential_id),
        "public_key": b64url_encode(_cose_to_bytes(auth_data.cose_key)),
        "sign_count": auth_data.sign_count,
    }


def verify_authentication(
    *, credential: dict, expected_challenge: bytes, rp_id: str, origin: str, public_key_b64: str, stored_sign_count: int
) -> int:
    """Check a navigator.credentials.get() result. Returns the new sign count."""
    response = credential.get("response") or {}
    client_data_json = b64url_decode(response.get("clientDataJSON", ""))
    _check_client_data(
        client_data_json,
        expected_type="webauthn.get",
        expected_challenge=expected_challenge,
        expected_origin=origin,
    )
    raw_auth_data = b64url_decode(response.get("authenticatorData", ""))
    auth_data = AuthenticatorData(raw_auth_data, expect_credential=False)
    _check_authenticator(auth_data, rp_id=rp_id, require_user_verification=True)

    public_key, alg = _load_cose_key(cbor_decode(b64url_decode(public_key_b64)))
    _verify_signature(
        public_key,
        alg,
        raw_auth_data + hashlib.sha256(client_data_json).digest(),
        b64url_decode(response.get("signature", "")),
    )

    # A counter that goes backwards means this passkey has been cloned. Many
    # platform authenticators (Apple, Google) never increment it and always
    # send 0 — that is legal, and only a NON-ZERO count that failed to advance
    # is evidence of a clone.
    if auth_data.sign_count and auth_data.sign_count <= stored_sign_count:
        raise WebAuthnError("this passkey looks cloned and has been refused")
    return auth_data.sign_count


def _cose_to_bytes(cose: dict) -> bytes:
    """Re-encode a COSE key for storage.

    The key is kept as canonical CBOR rather than the raw tail of authData so
    that what is stored is exactly what will later be parsed — storing the
    slice would preserve any trailing extension bytes that came with it.
    """
    parts = [bytes([0xA0 | len(cose)])]
    for key in sorted(cose, key=lambda k: (k < 0, abs(k))):
        parts.append(_cbor_write_int(key))
        value = cose[key]
        if isinstance(value, int):
            parts.append(_cbor_write_int(value))
        elif isinstance(value, bytes):
            parts.append(_cbor_write_head(2, len(value)) + value)
        else:
            raise WebAuthnError("unexpected COSE key member")
    return b"".join(parts)


def _cbor_write_head(major: int, value: int) -> bytes:
    if value < 24:
        return bytes([(major << 5) | value])
    if value < 256:
        return bytes([(major << 5) | 24, value])
    if value < 65536:
        return bytes([(major << 5) | 25]) + value.to_bytes(2, "big")
    return bytes([(major << 5) | 26]) + value.to_bytes(4, "big")


def _cbor_write_int(value: int) -> bytes:
    if value >= 0:
        return _cbor_write_head(0, value)
    return _cbor_write_head(1, -1 - value)
