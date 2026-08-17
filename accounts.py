"""
accounts.py — user accounts for CryptoForge.

Phil, 2026-08-17: "I need the same kinda authentication for cryptoforge as well
with username and password and authentication... Also I want to add user from
admin console... Also with biometrics on cryptoforge as well."

This is PhilForge's account model (auth.py + the users/passkeys tables) carried
over to CryptoForge's storage, which is a JSON document store rather than
SQLite tables: users, passkeys and passkey challenges each live in a mapping
bucket of the state store. Same rules, same shapes, same words in the UI.

WHAT LIVES HERE
- password hashing (bcrypt, cost 12) and the password policy
- per-account TOTP: enrolment payload (secret + QR), verification with a
  replay counter so a code cannot be used twice
- the user store: create / get / list / update / disable / delete
- the passkey store (PUBLIC keys only) and single-use WebAuthn challenges
- roles and what each may do — admin runs the desk, user trades, viewer looks

WHAT DOES NOT
- sessions, the login route, rate limiting and the middleware stay in app.py,
  which already owns them; this module never touches a Request.
"""

from __future__ import annotations

import base64
import io
import logging
import re
import secrets
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import bcrypt
import pyotp

_logger = logging.getLogger(__name__)

# ── Roles ─────────────────────────────────────────────────────────
ROLE_ADMIN = "admin"
ROLE_USER = "user"
ROLE_VIEWER = "viewer"
USER_ROLES = (ROLE_ADMIN, ROLE_USER, ROLE_VIEWER)

BUCKET_USERS = "users"
BUCKET_PASSKEYS = "passkeys"
BUCKET_WEBAUTHN_CHALLENGES = "webauthn_challenges"

TOTP_ISSUER = "CryptoForge"

# ── Password policy ───────────────────────────────────────────────
PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 128
PASSWORD_POLICY_HINT = "At least 8 characters, with a letter and a number."


def password_policy_error(password: str, label: str = "Password") -> str | None:
    """The sentence to show for a password that will not do, or None."""
    value = str(password or "")
    if len(value) < PASSWORD_MIN_LENGTH:
        return f"{label} must be at least {PASSWORD_MIN_LENGTH} characters"
    if len(value) > PASSWORD_MAX_LENGTH:
        return f"{label} is too long"
    if not re.search(r"[A-Za-z]", value) or not re.search(r"\d", value):
        return f"{label} needs at least one letter and one number"
    return None


def hash_password(password: str) -> str:
    return bcrypt.hashpw(str(password).encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(str(plain).encode("utf-8"), str(hashed).encode("utf-8"))
    except Exception:
        return False


def normalize_role(value) -> str:
    role = str(value or "").strip().lower()
    return role if role in USER_ROLES else ROLE_USER


def normalize_username(value) -> str:
    return str(value or "").strip()


_USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,31}$")


def username_error(username: str) -> str | None:
    if not _USERNAME_RE.match(username or ""):
        return "Username must be 2-32 characters: letters, digits, dot, dash or underscore"
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── TOTP ──────────────────────────────────────────────────────────
def totp_enrollment(username: str) -> dict:
    """A fresh secret, its otpauth URI, and a QR (SVG data URI) when segno is present."""
    secret = pyotp.random_base32()
    uri = pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name=TOTP_ISSUER)
    qr_data_uri = ""
    try:
        import segno

        buf = io.BytesIO()
        segno.make(uri, micro=False, error="m").save(buf, kind="svg", scale=5, border=2)
        qr_data_uri = "data:image/svg+xml;base64," + base64.b64encode(buf.getvalue()).decode("ascii")
    except ImportError:
        pass  # manual key entry still works; the QR is a convenience
    return {"secret": secret, "otpauth_uri": uri, "qr_data_uri": qr_data_uri}


def matching_totp_counter(secret: str, code: str, *, now: float | None = None, valid_window: int = 1) -> int | None:
    """The 30-second counter this code belongs to, or None. Does not consume."""
    candidate = re.sub(r"\s+", "", str(code or ""))
    if not re.fullmatch(r"\d{6}", candidate):
        return None
    cleaned = str(secret or "").strip().replace(" ", "").upper()
    if not cleaned:
        return None
    try:
        totp = pyotp.TOTP(cleaned)
        timestamp = float(time.time() if now is None else now)
        current = int(timestamp // totp.interval)
        for counter in range(current - valid_window, current + valid_window + 1):
            if counter < 0:
                continue
            if pyotp.utils.strings_equal(totp.at(counter * totp.interval), candidate):
                return counter
    except Exception as exc:  # a secret that is not base32
        _logger.error("TOTP secret is not usable: %s", exc)
    return None


# ── The store ─────────────────────────────────────────────────────
class AccountStore:
    """Users, passkeys and challenges over the app's JSON document store.

    `store_fn` returns the live SQLiteJSONStore, resolved on every call, so a
    test that swaps the state DB path mid-run is honoured — the same reason
    app.py never caches _get_state_store().
    """

    def __init__(self, store_fn: Callable[[], object]):
        self._store_fn = store_fn

    @property
    def _store(self):
        return self._store_fn()

    # ── users ─────────────────────────────────────────────────
    def _users(self) -> dict:
        return self._store.get_mapping(BUCKET_USERS)

    def list_users(self) -> list[dict]:
        rows = [self._public(u) for u in self._users().values()]
        rows.sort(key=lambda u: int(u.get("id") or 0))
        return rows

    def count_users(self) -> int:
        return len(self._users())

    def get_user(self, user_id) -> Optional[dict]:
        if user_id is None:
            return None
        return self._store.get(BUCKET_USERS, str(user_id))

    def get_user_by_username(self, username: str) -> Optional[dict]:
        wanted = normalize_username(username).lower()
        if not wanted:
            return None
        for row in self._users().values():
            if str(row.get("username") or "").lower() == wanted:
                return row
        return None

    def first_admin(self) -> Optional[dict]:
        admins = [u for u in self._users().values() if u.get("role") == ROLE_ADMIN and u.get("is_active", True)]
        admins.sort(key=lambda u: int(u.get("id") or 0))
        return admins[0] if admins else None

    def active_admin_count(self, *, excluding=None) -> int:
        return sum(
            1
            for u in self._users().values()
            if u.get("role") == ROLE_ADMIN and u.get("is_active", True) and str(u.get("id")) != str(excluding)
        )

    def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        role: str = ROLE_USER,
        mfa_totp_secret: str = "",
    ) -> dict:
        users = self._users()
        next_id = max([int(k) for k in users.keys() if str(k).isdigit()] + [0]) + 1
        record = {
            "id": next_id,
            "username": normalize_username(username),
            "password_hash": password_hash,
            "role": normalize_role(role),
            "is_active": True,
            "mfa_totp_secret": str(mfa_totp_secret or ""),
            "mfa_pending_secret": "",
            "mfa_enabled": bool(mfa_totp_secret),
            "mfa_enrolled_at": _now_iso() if mfa_totp_secret else None,
            "mfa_last_counter": -1,
            "created_at": _now_iso(),
            "last_login": None,
        }
        self._store.put(BUCKET_USERS, str(next_id), record)
        return record

    def update_user(self, user_id, **fields) -> Optional[dict]:
        record = self.get_user(user_id)
        if not record:
            return None
        record.update(fields)
        self._store.put(BUCKET_USERS, str(user_id), record)
        return record

    def delete_user(self, user_id) -> bool:
        if not self.get_user(user_id):
            return False
        self._store.delete(BUCKET_USERS, str(user_id))
        for cid, row in self._store.get_mapping(BUCKET_PASSKEYS).items():
            if str(row.get("user_id")) == str(user_id):
                self._store.delete(BUCKET_PASSKEYS, cid)
        return True

    def touch_login(self, user_id) -> None:
        self.update_user(user_id, last_login=_now_iso())

    def claim_mfa_counter(self, user_id, counter: int) -> bool:
        """Spend one TOTP counter. False if it (or a later one) was already spent."""
        record = self.get_user(user_id)
        if not record:
            return False
        if int(counter) <= int(record.get("mfa_last_counter", -1) or -1):
            return False
        record["mfa_last_counter"] = int(counter)
        self._store.put(BUCKET_USERS, str(user_id), record)
        return True

    def verify_user_totp(self, user: dict, code: str, *, now: float | None = None) -> bool:
        """Check an enrolled account's code and spend it."""
        if not user or not bool(user.get("mfa_enabled")):
            return False
        counter = matching_totp_counter(str(user.get("mfa_totp_secret") or ""), code, now=now)
        if counter is None:
            return False
        return self.claim_mfa_counter(user["id"], counter)

    def verify_totp_enrollment(self, user_id, secret: str, code: str, *, now: float | None = None) -> bool:
        counter = matching_totp_counter(secret, code, now=now)
        if counter is None:
            return False
        return self.claim_mfa_counter(user_id, counter)

    @staticmethod
    def _public(user: dict) -> dict:
        """The shape the UI sees — never a hash, never a secret."""
        return {
            "id": user.get("id"),
            "username": user.get("username"),
            "role": user.get("role"),
            "is_active": bool(user.get("is_active", True)),
            "mfa_enabled": bool(user.get("mfa_enabled")),
            "mfa_enrolled_at": user.get("mfa_enrolled_at"),
            "created_at": user.get("created_at"),
            "last_login": user.get("last_login"),
        }

    def public(self, user: dict) -> dict:
        return self._public(user)

    # ── passkeys ──────────────────────────────────────────────
    def add_passkey(self, credential_id: str, user_id, public_key: str, sign_count: int, label: str) -> None:
        self._store.put(
            BUCKET_PASSKEYS,
            credential_id,
            {
                "credential_id": credential_id,
                "user_id": int(user_id),
                "public_key": public_key,
                "sign_count": int(sign_count or 0),
                "label": str(label or "")[:60],
                "created_at": _now_iso(),
                "last_used_at": None,
            },
        )

    def get_passkey(self, credential_id: str) -> Optional[dict]:
        if not credential_id:
            return None
        return self._store.get(BUCKET_PASSKEYS, credential_id)

    def list_passkeys(self, user_id) -> list[dict]:
        rows = [
            {k: v for k, v in row.items() if k != "public_key"}
            for row in self._store.get_mapping(BUCKET_PASSKEYS).values()
            if str(row.get("user_id")) == str(user_id)
        ]
        rows.sort(key=lambda r: str(r.get("created_at") or ""))
        return rows

    def touch_passkey(self, credential_id: str, sign_count: int) -> None:
        row = self.get_passkey(credential_id)
        if not row:
            return
        row["sign_count"] = int(sign_count or 0)
        row["last_used_at"] = _now_iso()
        self._store.put(BUCKET_PASSKEYS, credential_id, row)

    def delete_passkey(self, credential_id: str, user_id) -> bool:
        row = self.get_passkey(credential_id)
        if not row or str(row.get("user_id")) != str(user_id):
            return False
        self._store.delete(BUCKET_PASSKEYS, credential_id)
        return True

    # ── challenges ────────────────────────────────────────────
    # Single-use and short-lived, kept in the store rather than in memory so a
    # restart mid-ceremony fails closed rather than accepting a stale one.
    def store_challenge(self, user_id, purpose: str, challenge_b64: str, ttl_seconds: int = 300) -> str:
        self._sweep_challenges()
        challenge_id = secrets.token_hex(16)
        self._store.put(
            BUCKET_WEBAUTHN_CHALLENGES,
            challenge_id,
            {
                "challenge_id": challenge_id,
                "user_id": int(user_id) if user_id is not None else None,
                "purpose": str(purpose),
                "challenge": challenge_b64,
                "expires_at": time.time() + int(ttl_seconds),
            },
        )
        return challenge_id

    def consume_challenge(self, challenge_id: str, purpose: str) -> Optional[dict]:
        if not challenge_id:
            return None
        row = self._store.get(BUCKET_WEBAUTHN_CHALLENGES, challenge_id)
        if not row:
            return None
        self._store.delete(BUCKET_WEBAUTHN_CHALLENGES, challenge_id)
        if row.get("purpose") != purpose or float(row.get("expires_at") or 0) < time.time():
            return None
        return row

    def _sweep_challenges(self) -> None:
        now = time.time()
        for cid, row in self._store.get_mapping(BUCKET_WEBAUTHN_CHALLENGES).items():
            if float(row.get("expires_at") or 0) < now:
                self._store.delete(BUCKET_WEBAUTHN_CHALLENGES, cid)
