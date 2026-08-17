"""
sync_service/master_candidates/test_backfill_state.py

Unit tests for backfill/state.py's try_claim() -- the single-claimant
compare-and-swap logic. Uses httpx.MockTransport so no real Supabase
access is needed; each test controls exactly what the mocked GET/PATCH
calls return and asserts on both try_claim()'s return value and the
exact requests it made (e.g. that a fresh heartbeat short-circuits
before ever issuing a PATCH at all).

This file exists specifically because of a production incident: the
previous try_claim() implementation used a single conditional PATCH with
an inequality embedded inside a PostgREST `or=(...)` filter string, and
it silently returned False on every attempt for an extended period even
against an unambiguously stale heartbeat, with no way to observe why from
outside Postgres/PostgREST. The replacement moves the staleness decision
into plain Python (covered directly by these tests) and only ever sends
PostgREST an exact-value filter for the actual claim.
"""

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from master_candidates.backfill import state


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class _Recorder:
    """Captures every request the mock transport receives, in order."""
    def __init__(self):
        self.requests: list[httpx.Request] = []

    def record(self, request: httpx.Request) -> None:
        self.requests.append(request)


def _client(recorder: _Recorder, get_rows, patch_rows) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        recorder.record(request)
        if request.method == "GET":
            return httpx.Response(200, json=get_rows)
        if request.method == "PATCH":
            return httpx.Response(200, json=patch_rows)
        raise AssertionError(f"unexpected method in test: {request.method}")
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_try_claim_succeeds_when_heartbeat_is_null():
    recorder = _Recorder()
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": None}]
    patch_rows = [{"process_name": "portal_a"}]  # non-empty = CAS matched
    async with _client(recorder, get_rows, patch_rows) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is True
    assert len(recorder.requests) == 2
    patch_req = recorder.requests[1]
    assert patch_req.method == "PATCH"
    assert patch_req.url.params["worker_heartbeat_at"] == "is.null"
    assert patch_req.url.params["desired_state"] == "eq.running"


async def test_try_claim_succeeds_when_heartbeat_is_stale():
    recorder = _Recorder()
    stale_ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": _iso(stale_ts)}]
    patch_rows = [{"process_name": "portal_a"}]
    async with _client(recorder, get_rows, patch_rows) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is True
    patch_req = recorder.requests[1]
    # exact-value CAS filter, not an inequality
    assert patch_req.url.params["worker_heartbeat_at"] == f"eq.{_iso(stale_ts)}"


async def test_try_claim_fails_and_never_patches_when_heartbeat_is_fresh():
    recorder = _Recorder()
    fresh_ts = datetime.now(timezone.utc) - timedelta(seconds=10)
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": _iso(fresh_ts)}]
    async with _client(recorder, get_rows, patch_rows=None) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is False
    # Must short-circuit BEFORE ever issuing a PATCH -- a fresh heartbeat
    # means another instance legitimately owns the job right now.
    assert len(recorder.requests) == 1
    assert recorder.requests[0].method == "GET"


async def test_try_claim_fails_when_desired_state_not_running():
    recorder = _Recorder()
    get_rows = [{"desired_state": "paused", "worker_heartbeat_at": None}]
    async with _client(recorder, get_rows, patch_rows=None) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is False
    assert len(recorder.requests) == 1  # no PATCH attempted


async def test_try_claim_fails_when_no_control_row_exists():
    recorder = _Recorder()
    async with _client(recorder, get_rows=[], patch_rows=None) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is False
    assert len(recorder.requests) == 1


async def test_try_claim_loses_race_when_cas_patch_matches_nothing():
    """Two instances both read a stale heartbeat and both consider
    themselves eligible; only the first PATCH to actually land should win.
    This simulates the second, losing instance: its exact-value CAS filter
    no longer matches because the first instance already changed the
    heartbeat, so PostgREST correctly returns an empty array."""
    recorder = _Recorder()
    stale_ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": _iso(stale_ts)}]
    async with _client(recorder, get_rows, patch_rows=[]) as client:  # CAS finds 0 rows
        result = await state.try_claim(client, "instance-b")
    assert result is False
    assert len(recorder.requests) == 2  # it DID attempt the PATCH, just lost


async def test_try_claim_just_under_stale_cutoff_is_not_eligible():
    """A heartbeat comfortably (5s) under CLAIM_STALE_AFTER_SEC old must
    not be claimable -- verified with margin rather than at the exact
    boundary, since real wall-clock time elapses between constructing the
    timestamp here and try_claim's own `now` a moment later, making an
    exact-boundary comparison inherently flaky rather than meaningful."""
    recorder = _Recorder()
    from master_candidates.backfill.config import CLAIM_STALE_AFTER_SEC
    almost_stale_ts = datetime.now(timezone.utc) - timedelta(seconds=CLAIM_STALE_AFTER_SEC - 5)
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": _iso(almost_stale_ts)}]
    async with _client(recorder, get_rows, patch_rows=None) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is False
    assert len(recorder.requests) == 1


async def test_try_claim_just_over_stale_cutoff_is_eligible():
    """A heartbeat comfortably (5s) over CLAIM_STALE_AFTER_SEC old must be
    claimable -- same margin rationale as the test above."""
    recorder = _Recorder()
    from master_candidates.backfill.config import CLAIM_STALE_AFTER_SEC
    just_stale_ts = datetime.now(timezone.utc) - timedelta(seconds=CLAIM_STALE_AFTER_SEC + 5)
    get_rows = [{"desired_state": "running", "worker_heartbeat_at": _iso(just_stale_ts)}]
    patch_rows = [{"process_name": "portal_a"}]
    async with _client(recorder, get_rows, patch_rows) as client:
        result = await state.try_claim(client, "instance-a")
    assert result is True
