"""
state_store.py — durable local state storage for CryptoForge.

The app previously spread writable runtime state across multiple JSON files.
This module consolidates that state into a single SQLite-backed JSON document
store with lightweight migration helpers.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterable

_STORE_CACHE: dict[str, "SQLiteJSONStore"] = {}
_STORE_CACHE_LOCK = threading.RLock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteJSONStore:
    def __init__(self, db_path: str):
        self.db_path = os.path.abspath(os.path.expanduser(db_path))
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    bucket TEXT NOT NULL,
                    doc_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (bucket, doc_key)
                );

                CREATE INDEX IF NOT EXISTS idx_documents_bucket_updated
                ON documents(bucket, updated_at DESC);
                """
            )

    def health(self) -> dict:
        with self._lock, self._connect() as conn:
            conn.execute("SELECT 1")
        size_bytes = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        return {
            "path": self.db_path,
            "exists": os.path.exists(self.db_path),
            "size_bytes": size_bytes,
            "writable": os.access(os.path.dirname(self.db_path), os.W_OK),
        }

    def count(self, bucket: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM documents WHERE bucket=?", (bucket,)).fetchone()
            return int(row["c"] or 0)

    def get(self, bucket: str, key: str, default=None):
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM documents WHERE bucket=? AND doc_key=?",
                (bucket, str(key)),
            ).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["payload"])
        except json.JSONDecodeError:
            return default

    def get_mapping(self, bucket: str) -> dict:
        mapping = {}
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT doc_key, payload FROM documents WHERE bucket=? ORDER BY doc_key ASC",
                (bucket,),
            ).fetchall()
        for row in rows:
            try:
                mapping[str(row["doc_key"])] = json.loads(row["payload"])
            except json.JSONDecodeError:
                continue
        return mapping

    def list(self, bucket: str, *, order_by: str = "updated_at", reverse: bool = False) -> list[dict]:
        query_map = {
            ("created_at", False): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents "
                "WHERE bucket=? ORDER BY created_at ASC, doc_key ASC"
            ),
            ("created_at", True): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents "
                "WHERE bucket=? ORDER BY created_at DESC, doc_key DESC"
            ),
            ("doc_key", False): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents " "WHERE bucket=? ORDER BY doc_key ASC"
            ),
            ("doc_key", True): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents " "WHERE bucket=? ORDER BY doc_key DESC"
            ),
            ("updated_at", False): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents "
                "WHERE bucket=? ORDER BY updated_at ASC, doc_key ASC"
            ),
            ("updated_at", True): (
                "SELECT doc_key, payload, created_at, updated_at FROM documents "
                "WHERE bucket=? ORDER BY updated_at DESC, doc_key DESC"
            ),
        }
        order_col = "updated_at" if order_by not in {"created_at", "updated_at", "doc_key"} else order_by
        query = query_map[(order_col, bool(reverse))]
        with self._lock, self._connect() as conn:
            rows = conn.execute(query, (bucket,)).fetchall()
        results = []
        for row in rows:
            try:
                payload = json.loads(row["payload"])
            except json.JSONDecodeError:
                continue
            results.append(payload)
        return results

    def put(self, bucket: str, key: str, payload) -> None:
        now = _utc_now_iso()
        encoded = json.dumps(payload, default=str, separators=(",", ":"))
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO documents(bucket, doc_key, payload, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(bucket, doc_key) DO UPDATE SET
                    payload=excluded.payload,
                    updated_at=excluded.updated_at
                """,
                (bucket, str(key), encoded, now, now),
            )

    def delete(self, bucket: str, key: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM documents WHERE bucket=? AND doc_key=?", (bucket, str(key)))

    def replace_mapping(self, bucket: str, mapping: dict) -> None:
        now = _utc_now_iso()
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM documents WHERE bucket=?", (bucket,))
            rows = [
                (
                    bucket,
                    str(key),
                    json.dumps(value, default=str, separators=(",", ":")),
                    now,
                    now,
                )
                for key, value in mapping.items()
            ]
            if rows:
                conn.executemany(
                    """
                    INSERT INTO documents(bucket, doc_key, payload, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def replace_list(self, bucket: str, records: Iterable, *, key_fn: Callable[[object, int], str]) -> None:
        now = _utc_now_iso()
        prepared = []
        for index, record in enumerate(records):
            prepared.append(
                (
                    bucket,
                    str(key_fn(record, index)),
                    json.dumps(record, default=str, separators=(",", ":")),
                    now,
                    now,
                )
            )
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM documents WHERE bucket=?", (bucket,))
            if prepared:
                conn.executemany(
                    """
                    INSERT INTO documents(bucket, doc_key, payload, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    prepared,
                )

    def max_numeric_key(self, bucket: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(CAST(doc_key AS INTEGER)) AS max_key
                FROM documents
                WHERE bucket=? AND doc_key GLOB '[0-9]*'
                """,
                (bucket,),
            ).fetchone()
        return int(row["max_key"] or 0)

    def export_snapshot(self) -> dict:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT bucket, doc_key, payload, created_at, updated_at "
                "FROM documents ORDER BY bucket ASC, doc_key ASC"
            ).fetchall()
        buckets: dict[str, dict] = {}
        for row in rows:
            bucket = str(row["bucket"])
            bucket_rows = buckets.setdefault(bucket, {})
            try:
                payload = json.loads(row["payload"])
            except json.JSONDecodeError:
                payload = None
            bucket_rows[str(row["doc_key"])] = {
                "payload": payload,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        return {
            "format": "cryptoforge-state-snapshot/v1",
            "db_path": self.db_path,
            "exported_at": _utc_now_iso(),
            "buckets": buckets,
        }

    def import_snapshot(self, snapshot: dict, *, replace: bool = True) -> None:
        buckets = dict((snapshot or {}).get("buckets") or {})
        with self._lock, self._connect() as conn:
            if replace:
                conn.execute("DELETE FROM documents")
            rows = []
            for bucket, entries in buckets.items():
                for key, entry in dict(entries or {}).items():
                    payload = (entry or {}).get("payload")
                    rows.append(
                        (
                            str(bucket),
                            str(key),
                            json.dumps(payload, default=str, separators=(",", ":")),
                            str((entry or {}).get("created_at") or _utc_now_iso()),
                            str((entry or {}).get("updated_at") or _utc_now_iso()),
                        )
                    )
            if rows:
                conn.executemany(
                    """
                    INSERT INTO documents(bucket, doc_key, payload, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(bucket, doc_key) DO UPDATE SET
                        payload=excluded.payload,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                )


def get_json_store(db_path: str) -> SQLiteJSONStore:
    resolved = os.path.abspath(os.path.expanduser(db_path))
    with _STORE_CACHE_LOCK:
        store = _STORE_CACHE.get(resolved)
        if store is None:
            store = SQLiteJSONStore(resolved)
            _STORE_CACHE[resolved] = store
        return store
