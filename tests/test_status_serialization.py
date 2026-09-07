"""The polled status endpoints must not pay for jsonable_encoder, or reload runs.

py-spy on the production worker, after three earlier rounds of moving writes off
the event loop had failed to fix the site:

  * jsonable_encoder frames all over the main thread. FastAPI walks the WHOLE
    response with it whenever an endpoint returns a plain dict — measured at
    5.44 s against 0.80 s for json.dumps of the same payload, 6.8x, per request.
    That 5.44 s is the p99 of 6.1 s that survived everything else.
  * _load_runs, reading 239 documents and decoding 3.37 MB — 1.12 s — on EVERY
    /api/paper/status poll, to look at the last run.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402

import app as app_module  # noqa: E402

POLLED = [
    "live_status",
    "paper_status",
    "engines_all",
    "scalp_status",
    "auto_fib_status",
    "vrule_live_status",
    "cascade_status",
]


@pytest.mark.parametrize("name", POLLED)
def test_polled_endpoints_bypass_jsonable_encoder(name):
    """Each must be wrapped, or it silently goes back to costing 6.8x."""
    fn = getattr(app_module, name)
    assert getattr(fn, "__wrapped__", None) is not None, f"{name} is not wrapped"


def test_fast_json_renders_without_jsonable_encoder():
    body = app_module._FastJSON({"a": 1, "b": [1, 2]}).body
    assert body == b'{"a":1,"b":[1,2]}'


def test_fast_json_survives_a_datetime():
    """default=str is what plain-dict returns already relied on."""
    from datetime import datetime

    body = app_module._FastJSON({"t": datetime(2026, 9, 8)}).body
    assert b"2026-09-08" in body


def test_a_response_return_is_passed_through():
    import asyncio

    async def route():
        return JSONResponse({"already": "a response"})

    wrapped = app_module._fast_json_route(route)
    out = asyncio.run(wrapped())
    assert isinstance(out, JSONResponse)
    assert out.body == JSONResponse({"already": "a response"}).body


def test_runs_are_not_reloaded_on_every_call(monkeypatch):
    calls = []

    class Store:
        def list(self, bucket, order_by=None):
            calls.append(bucket)
            return [{"id": 1}]

    monkeypatch.setattr(app_module, "_seed_list_bucket", lambda *a, **k: Store())
    app_module._invalidate_runs_cache()

    first = app_module._load_runs()
    for _ in range(20):
        app_module._load_runs()

    assert first == [{"id": 1}]
    assert len(calls) == 1, f"reloaded {len(calls)} times inside the TTL"


def test_a_write_is_visible_immediately(monkeypatch):
    """Five seconds of staleness is fine for a poll, never for a write."""
    calls = []

    class Store:
        def list(self, bucket, order_by=None):
            calls.append(bucket)
            return [{"id": 1}]

        def replace_list(self, bucket, rows, key_fn=None):
            pass

    monkeypatch.setattr(app_module, "_seed_list_bucket", lambda *a, **k: Store())
    monkeypatch.setattr(app_module, "_get_state_store", lambda: Store())
    app_module._invalidate_runs_cache()

    app_module._load_runs()
    assert len(calls) == 1
    app_module._save_runs([{"id": 2}])
    app_module._load_runs()
    assert len(calls) == 2, "a save must invalidate the cache"


def test_the_cache_expires(monkeypatch):
    calls = []

    class Store:
        def list(self, bucket, order_by=None):
            calls.append(bucket)
            return [{"id": 1}]

    monkeypatch.setattr(app_module, "_seed_list_bucket", lambda *a, **k: Store())
    monkeypatch.setattr(app_module, "_RUNS_CACHE_TTL_SEC", 0.05)
    app_module._invalidate_runs_cache()

    app_module._load_runs()
    time.sleep(0.08)
    app_module._load_runs()
    assert len(calls) == 2, "the TTL must expire so an unknown writer is seen"
