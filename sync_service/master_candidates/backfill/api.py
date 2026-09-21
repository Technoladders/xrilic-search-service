"""
sync_service/master_candidates/backfill/api.py

FastAPI router mounted at /mc/admin/backfill/*. All processing (matching,
merging, orchestration) lives in match.py/worker.py -- this file is only
request parsing, auth, and thin delegation.

Auth model: every endpoint requires a verified Supabase JWT belonging to a
global_superadmin, checked entirely via plain PostgREST calls (no Supabase
RPC) -- see require_global_superadmin. This closes a real production gap:
the Edge Function this module replaces is deployed with --no-verify-jwt and
never reads an Authorization header at all; nothing today actually
restricts who can trigger a mass backfill besides obscurity of the
function's URL.

hr_employees.user_id (NOT hr_employees.id) holds the Supabase Auth user id
-- confirmed directly against the live schema and the existing
backfill_errors_superadmin_all RLS policy / set_mc_process_control RPC,
both of which already use e.user_id = auth.uid().
"""

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from ..config import SB_HEADERS, SUPABASE_REST, SUPABASE_URL, HTTP_TIMEOUT_SUPABASE
from . import worker, state
from .config import (
    DEFAULT_BATCH_SIZE, DEFAULT_CONCURRENCY, MAX_BATCH_SIZE, MAX_CONCURRENCY,
    DEFAULT_DRY_RUN_PAGES, MAX_DRY_RUN_PAGES, SOURCE,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc/admin/backfill", tags=["master_candidates_backfill"])


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
async def require_global_superadmin(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[len("Bearer "):]

    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"apikey": SB_HEADERS["apikey"], "Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT_SUPABASE)
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail="invalid token")
        user_id = (r.json() or {}).get("id")
        if not user_id:
            raise HTTPException(status_code=401, detail="invalid token")

        role_r = await client.get(
            f"{SUPABASE_REST}/hr_employees",
            params={"user_id": f"eq.{user_id}", "select": "role_id,hr_roles(name)"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    if role_r.status_code >= 400:
        raise HTTPException(status_code=role_r.status_code, detail=role_r.text[:400])
    rows = role_r.json()
    role_name = (rows[0].get("hr_roles") or {}).get("name") if rows else None
    if role_name != "global_superadmin":
        raise HTTPException(status_code=403, detail="not_authorized")
    return user_id


def _clamp(value: Optional[int], default: int, maximum: int, minimum: int = 1) -> int:
    if value is None:
        return default
    return max(minimum, min(int(value), maximum))


# ─────────────────────────────────────────────────────────────────────────────
# Job control
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/start")
async def backfill_start(request: Request, user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    body: dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        pass
    batch_size = _clamp(body.get("batch_size"), DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE)
    concurrency = _clamp(body.get("concurrency"), DEFAULT_CONCURRENCY, MAX_CONCURRENCY)

    async with httpx.AsyncClient() as client:
        result = await worker.start(client, batch_size=batch_size, concurrency=concurrency)
    if not result["ok"] and result.get("already_running"):
        raise HTTPException(status_code=409, detail="backfill already running")
    return result


@router.post("/pause")
async def backfill_pause(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        await worker.request_pause(client)
    return {"ok": True, "status": "pausing"}


@router.post("/stop")
async def backfill_stop(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        await worker.request_stop(client)
    return {"ok": True, "status": "stopping"}


@router.get("/status")
async def backfill_status(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        return await worker.get_status(client)


@router.get("/batches")
async def backfill_batches(limit: int = 20, user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    limit = max(1, min(limit, 100))
    async with httpx.AsyncClient() as client:
        rows = await state.get_recent_batches(client, limit)
    return {"batches": rows}


@router.get("/errors")
async def backfill_errors(limit: int = 50, user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    limit = max(1, min(limit, 200))
    async with httpx.AsyncClient() as client:
        rows = await state.get_recent_errors(client, limit)
    return {"errors": rows}


@router.post("/retry-errors")
async def backfill_retry_errors(request: Request, user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    body: dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        pass
    limit = _clamp(body.get("limit"), 500, 1000)

    async with httpx.AsyncClient() as client:
        ids = await state.get_unretried_error_source_ids(client, limit)
        if not ids:
            return {"ok": True, "retried": 0, "inserted": 0, "merged": 0, "errors": 0}

        r = await client.get(
            f"{SUPABASE_REST}/naukri_candidates",
            params={"id": f"in.({','.join(ids)})", "select": worker.SELECT_FIELDS},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        r.raise_for_status()
        rows = r.json()

        from . import match  # local import to avoid a module-load cycle with worker
        import asyncio as _asyncio
        results = []
        for chunk in worker._chunked(rows, DEFAULT_CONCURRENCY):
            results.extend(await _asyncio.gather(*(match.ingest_backfill_row(client, row) for row in chunk)))

        inserted = sum(1 for r_ in results if r_.ok and r_.is_new)
        merged = sum(1 for r_ in results if r_.ok and not r_.is_new)
        failed = [r_ for r_ in results if not r_.ok]
        if failed:
            await state.write_error_rows(
                client, None, [{"source_row_id": r_.source_row_id, "error": r_.error} for r_ in failed])

    return {"ok": True, "retried": len(results), "inserted": inserted,
            "merged": merged, "errors": len(failed)}


# ─────────────────────────────────────────────────────────────────────────────
# Dry run -- structurally separate endpoint, see worker.run_dry_run's docstring
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/dry-run")
async def backfill_dry_run(request: Request, user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    body: dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        pass
    batch_size = _clamp(body.get("batch_size"), DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE)
    concurrency = _clamp(body.get("concurrency"), DEFAULT_CONCURRENCY, MAX_CONCURRENCY)
    max_pages = _clamp(body.get("max_pages"), DEFAULT_DRY_RUN_PAGES, MAX_DRY_RUN_PAGES)

    async with httpx.AsyncClient() as client:
        report = await worker.run_dry_run(client, batch_size, concurrency, max_pages)
    return {"ok": True, **report}


# ─────────────────────────────────────────────────────────────────────────────
# Stats -- exact counts. Deliberately NOT count=estimated: SUPABASE_REST here
# resolves to a read replica, and a standby never runs its own ANALYZE (that
# requires write access to pg_statistic), so pg_class.reltuples -- what
# PostgREST's estimated count reads -- stays frozen at whatever the replica
# inherited and never reflects real growth. Confirmed in production:
# estimated total_source_rows read 2,916,706 against naukri_candidates' real
# count of 2,788,835 -- a 128K-row error that showed up as a phantom
# 84,716-row "still uningested" backlog on a backfill that had actually
# already finished (see worker.get_status for the cheap, frequent-poll-safe
# endpoint that doesn't need any of this).
#
# count=exact has no shortcut on Postgres -- it must visit every matching row
# for MVCC visibility -- so an unfiltered count over a ~2.79M-row table (or a
# filtered count whose filter still matches nearly the whole table, e.g.
# candidate_source_links' single-source portal_a rows) can legitimately take
# longer than the shared HTTP_TIMEOUT_SUPABASE=30s used everywhere else in
# this service. Confirmed in production: total_source_rows alone raised
# httpx.ReadTimeout, which (being unhandled) surfaced to the browser as a
# CORS error -- FastAPI's default 500 for an unhandled exception is built by
# Starlette's ServerErrorMiddleware, which sits OUTSIDE CORSMiddleware, so it
# never gets an Access-Control-Allow-Origin header; "blocked by CORS policy"
# was a red herring for the real failure, the timeout. Given its own longer
# timeout here (STATS_COUNT_TIMEOUT_SEC) rather than raising the shared
# HTTP_TIMEOUT_SUPABASE constant, which would also loosen unrelated,
# latency-sensitive call sites (auth checks, ingest, search). And made
# resilient to a per-count failure: a table this size can occasionally still
# exceed even this longer timeout under load, and the correct response to
# "couldn't get an exact count in time" is null ("unavailable"), never a
# silent 0 or a crash of the other five counts in the same response --
# returning 0 would reintroduce exactly the kind of wrong-but-confident
# number this endpoint was fixed to stop showing.
# ─────────────────────────────────────────────────────────────────────────────
STATS_COUNT_TIMEOUT_SEC = 60.0


async def _get_count(client: httpx.AsyncClient, table: str, params: dict[str, str], estimated: bool = False) -> Optional[int]:
    prefer = f"count={'estimated' if estimated else 'exact'}"
    try:
        r = await client.get(
            f"{SUPABASE_REST}/{table}",
            params={**params, "select": "id", "limit": "1"},
            headers={**SB_HEADERS, "Prefer": prefer},
            timeout=STATS_COUNT_TIMEOUT_SEC)
        r.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"[mc-backfill-stats] count failed for {table} {params}: {e!r}")
        return None
    content_range = r.headers.get("content-range", "")
    total = content_range.rsplit("/", 1)[-1] if "/" in content_range else ""
    try:
        return int(total)
    except ValueError:
        return None


@router.get("/stats")
async def backfill_stats(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        total_source_rows = await _get_count(client, "naukri_candidates", {})
        total_ingested = await _get_count(client, "candidate_source_links", {"source": f"eq.{SOURCE}"})
        total_master_candidates = await _get_count(client, "master_candidates", {})
        total_full_profile = await _get_count(client, "master_candidates", {"has_full_profile": "eq.true"})
        total_with_contact = await _get_count(client, "master_candidates", {"has_contact": "eq.true"})
        total_pending_merge_review = await _get_count(
            client, "candidate_merge_queue", {"status": "eq.pending"})

        latest_source_r = await client.get(
            f"{SUPABASE_REST}/candidate_source_links",
            params={"source": f"eq.{SOURCE}", "select": "last_synced_at",
                    "order": "last_synced_at.desc", "limit": "1"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        latest_source_r.raise_for_status()
        latest_source_rows = latest_source_r.json()
        latest_source_updated_at = latest_source_rows[0].get("last_synced_at") if latest_source_rows else None

        latest_master_r = await client.get(
            f"{SUPABASE_REST}/master_candidates",
            params={"select": "updated_at", "order": "updated_at.desc", "limit": "1"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        latest_master_r.raise_for_status()
        latest_master_rows = latest_master_r.json()
        latest_master_updated_at = latest_master_rows[0].get("updated_at") if latest_master_rows else None

    total_uningested = (
        max(0, total_source_rows - total_ingested)
        if total_source_rows is not None and total_ingested is not None
        else None
    )

    return {
        "total_source_rows": total_source_rows,
        "total_ingested": total_ingested,
        "total_uningested": total_uningested,
        "total_master_candidates": total_master_candidates,
        "total_full_profile": total_full_profile,
        "total_with_contact": total_with_contact,
        "total_pending_merge_review": total_pending_merge_review,
        "latest_source_updated_at": latest_source_updated_at,
        "oldest_source_updated_at": None,
        "latest_master_updated_at": latest_master_updated_at,
    }
