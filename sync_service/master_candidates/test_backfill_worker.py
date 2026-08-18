"""
sync_service/master_candidates/test_backfill_worker.py

Unit tests for backfill/worker.py::fetch_page() -- the composite
(updated_at, id) cursor page-fetch query.

Production incident (Aug 2026): the original cursor predicate --
    or=(updated_at.gt.X,and(updated_at.eq.X,id.gt.Y))
-- gave PostgREST's query planner nothing sargable to push into an Index
Cond (confirmed via EXPLAIN ANALYZE: it used the updated_at index only for
scan direction, then filtered every row it visited -- 42-116s once the
cursor landed inside a same-timestamp cluster of >150k rows, versus ~54ms
for an equivalent row-comparison predicate). PostgREST's filter grammar
has no way to express a literal `WHERE (updated_at, id) > (X, Y)` row
comparison, and adding an RPC to get one is explicitly out of scope, so
the fix instead adds a redundant-but-sargable `updated_at=gte.X` filter
alongside the existing OR: the OR already implies updated_at >= X in
every case, so this changes nothing about which rows match, but it gives
the planner an obvious range condition to push into the index.

These tests cover two things:
  1. fetch_page() sends the exact expected PostgREST filter params for
     each cursor state (no cursor / timestamp-only fallback / full
     composite cursor).
  2. The combined filter (`updated_at=gte.X` ANDed with the existing OR,
     as PostgREST ANDs all top-level params together) is logically
     equivalent to the intended keyset condition (updated_at, id) > (X, Y)
     -- proven directly against the incident's boundary cases, without a
     live Postgres connection.
"""

import httpx
import pytest

from master_candidates.backfill.worker import fetch_page, SELECT_FIELDS


class _Recorder:
    def __init__(self, response_rows):
        self.requests: list[httpx.Request] = []
        self._response_rows = response_rows

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, json=self._response_rows)


def _client(recorder: "_Recorder") -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(recorder.handler))


async def test_fetch_page_first_page_has_no_cursor_filters():
    recorder = _Recorder([])
    async with _client(recorder) as client:
        await fetch_page(client, after_ts=None, after_id=None, limit=500)
    params = recorder.requests[0].url.params
    assert "updated_at" not in params
    assert "or" not in params
    assert params["order"] == "updated_at.asc,id.asc"
    assert params["limit"] == "500"
    assert params["select"] == SELECT_FIELDS


async def test_fetch_page_timestamp_only_cursor_uses_plain_gt():
    """The after_id=None fallback branch -- a bare `gt` is already fully
    sargable on its own with no tiebreak needed, so no redundant filter is
    added here; this branch is unaffected by the fix."""
    recorder = _Recorder([])
    async with _client(recorder) as client:
        await fetch_page(client, after_ts="2026-07-01T00:00:00+00:00", after_id=None, limit=500)
    params = recorder.requests[0].url.params
    assert params["updated_at"] == "gt.2026-07-01T00:00:00+00:00"
    assert "or" not in params


async def test_fetch_page_composite_cursor_adds_redundant_sargable_filter():
    """The production fix: both the pre-existing OR tiebreak AND a plain
    `gte` filter must be present -- the `gte` is what lets the planner use
    an Index Cond instead of filtering every row (see module docstring and
    worker.py::fetch_page's docstring)."""
    recorder = _Recorder([])
    ts = "2026-07-01T00:00:00+00:00"
    cid = "11111111-1111-1111-1111-111111111111"
    async with _client(recorder) as client:
        await fetch_page(client, after_ts=ts, after_id=cid, limit=500)
    params = recorder.requests[0].url.params
    assert params["updated_at"] == f"gte.{ts}"
    assert params["or"] == f"(updated_at.gt.{ts},and(updated_at.eq.{ts},id.gt.{cid}))"


def _keyset_predicate(row_ts: str, row_id: str, cursor_ts: str, cursor_id: str) -> bool:
    """Pure-Python mirror of the exact combined WHERE clause fetch_page()
    now sends: `updated_at >= cursor_ts AND (updated_at > cursor_ts OR
    (updated_at = cursor_ts AND id > cursor_id))`. Plain string comparison
    matches Postgres's own ordering for ISO-8601 timestamps and UUIDs."""
    gte = row_ts >= cursor_ts
    or_clause = (row_ts > cursor_ts) or (row_ts == cursor_ts and row_id > cursor_id)
    return gte and or_clause


CURSOR_TS = "2026-07-01T00:00:00+00:00"
CURSOR_ID = "50000000-0000-0000-0000-000000000000"


@pytest.mark.parametrize("row_ts,row_id,expected", [
    pytest.param("2026-07-02T00:00:00+00:00", "00000000-0000-0000-0000-000000000000", True,
                 id="later_timestamp_any_id_included"),
    pytest.param(CURSOR_TS, "90000000-0000-0000-0000-000000000000", True,
                 id="same_timestamp_greater_id_included"),
    pytest.param(CURSOR_TS, CURSOR_ID, False,
                 id="same_timestamp_equal_id_excluded"),
    pytest.param(CURSOR_TS, "10000000-0000-0000-0000-000000000000", False,
                 id="same_timestamp_smaller_id_excluded"),
    pytest.param("2026-06-30T00:00:00+00:00", "ffffffff-ffff-ffff-ffff-ffffffffffff", False,
                 id="earlier_timestamp_any_id_excluded"),
])
def test_combined_filter_matches_intended_keyset_semantics(row_ts, row_id, expected):
    assert _keyset_predicate(row_ts, row_id, CURSOR_TS, CURSOR_ID) is expected
