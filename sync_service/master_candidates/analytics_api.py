"""
sync_service/master_candidates/analytics_api.py

GET /mc/admin/analytics/* — full-data analytics for the superadmin dashboard,
sourced directly from the LIVE master_candidates_v1 Typesense index (the same
~1M+ document collection /mc/search_v2 queries), NOT the capped/offline
suggestions collection that GET /mc/search_suggestions reads. That endpoint
is fine for autocomplete but caps at 2,000 values per dimension and only
refreshes on a manual admin trigger -- unsuitable for "full data, no limit"
analytics.

Three endpoints:
  - GET  /facets                     -- live, one Typesense facet query over
                                         the 16 fields already marked
                                         facet:true in the schema. Fast
                                         (sub-second even at 1M+ docs --
                                         Typesense facets are precomputed),
                                         so no caching/snapshot needed here,
                                         unlike the Postgres-backed pieces
                                         elsewhere in this codebase.
  - GET  /numeric_buckets            -- live, one Typesense /multi_search
                                         batching range-filtered count-only
                                         queries for CTC/experience/notice
                                         period. No schema change needed.
  - POST /full_distributions/rebuild -- offline batch pass (can take real
                                         time on 1M+ docs -- admin-triggered,
                                         cooldown-gated, same shape as the
                                         existing Postgres snapshot's
                                         "Refresh now"). Reuses
                                         suggestions_aggregator.py's proven
                                         export+aggregate logic UNCHANGED
                                         (export_documents/run_aggregation),
                                         just with a much higher cap, and
                                         writes to a brand-new, separate
                                         Typesense collection -- the existing
                                         suggestion collection and its 2,000
                                         cap are never touched.
  - GET  /full_distributions         -- reads that new collection, no cap.

Auth: a LOCAL copy of require_global_superadmin (defined independently in
backfill/api.py, not shared anywhere in this codebase -- every router here
defines its own auth dependency locally, e.g. admin_api.py's own weaker
require_admin; duplicating here matches that convention rather than
refactoring an existing file).
"""

import asyncio
import json
import logging
import time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query

from .config import (
    SB_HEADERS, SUPABASE_REST, SUPABASE_URL, HTTP_TIMEOUT_SUPABASE,
    TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION, HTTP_TIMEOUT_TYPESENSE,
)
from . import suggestions_aggregator as sagg
from .shared_writes import require_shared_writes

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc/admin/analytics", tags=["master_candidates_full_analytics"])

# Own collection, never the suggestions collection -- so this rebuild can use
# a far higher cap without touching /mc/search_suggestions' 2,000-value
# behavior or its offline-refresh cadence at all.
TS_FULL_ANALYTICS_COLLECTION = "master_candidates_full_analytics_v1"
FULL_DISTRIBUTIONS_CAP_PER_DIMENSION = 20000  # effectively unbounded for this data
REBUILD_COOLDOWN_SEC = 600  # 10 min, same cooldown shape as the Postgres snapshot refresh


# ─────────────────────────────────────────────────────────────────────────────
# Auth -- copied from backfill/api.py's require_global_superadmin verbatim
# (see that file's own docstring for why: the Edge Function this replaces
# has no auth at all; nothing today restricts backfill/analytics triggers
# besides obscurity of the URL otherwise).
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


# ─────────────────────────────────────────────────────────────────────────────
# /facets -- live, single Typesense facet query
# ─────────────────────────────────────────────────────────────────────────────
FACETED_FIELDS = [
    "has_full_profile", "has_contact", "sources", "primary_source", "seniority",
    "country", "industry", "job_function", "functional_area", "company_industry",
    "languages_filter", "gender", "marital_status", "disability",
    "desired_job_type", "employment_status_pref", "work_auth_countries",
]


@router.get("/facets")
async def get_facets(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    params = {
        "q": "*",
        "per_page": 0,          # facet counts only, no document bodies
        "facet_by": ",".join(FACETED_FIELDS),
        "max_facet_values": 500,  # well beyond any of these fields' real cardinality
    }
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/search",
                headers=TS_HEADERS, params=params, timeout=HTTP_TIMEOUT_TYPESENSE)
        r.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"[mc-analytics] facets query failed: {e!r}")
        raise HTTPException(status_code=502, detail="typesense facet query failed")

    data = r.json()
    facets: dict[str, list[dict[str, Any]]] = {}
    for fc in data.get("facet_counts", []):
        field = fc.get("field_name")
        if not field:
            continue
        facets[field] = [
            {"label": c["value"], "count": c["count"]}
            for c in fc.get("counts", [])
        ]
    return {"total": data.get("found", 0), "facets": facets}


# ─────────────────────────────────────────────────────────────────────────────
# /numeric_buckets -- live, one Typesense /multi_search call
# ─────────────────────────────────────────────────────────────────────────────
CTC_BUCKETS = [
    ("<2 L", None, 2), ("2-5 L", 2, 5), ("5-8 L", 5, 8), ("8-12 L", 8, 12),
    ("12-20 L", 12, 20), ("20-35 L", 20, 35), ("35+ L", 35, None),
]
# expected_ctc_lacs IS indexed in Typesense (index:true, float, sortable) --
# confirmed directly against the live collection schema. Earlier claimed
# absent based on an incomplete source-code summary; that was wrong, and
# this corrects it -- same bucket shape as current_ctc_lacs, no reason for
# this one to go through the Postgres snapshot at all.
EXPECTED_CTC_BUCKETS = CTC_BUCKETS
EXP_BUCKETS_MONTHS = [
    ("Fresher", None, 12), ("1-3 yrs", 12, 36), ("3-5 yrs", 36, 60),
    ("5-10 yrs", 60, 120), ("10+ yrs", 120, None),
]
# No NOTICE_BUCKETS_DAYS -- deliberately removed. notice_period_days is NULL
# for all but 1 of 2,837,322 master_candidates rows (confirmed via direct
# Postgres query), so any bucketing of it is meaningless by construction;
# indexer.py:188's `row.get("notice_period_days") or 0` then bakes every one
# of those nulls into a fake Typesense 0, which was showing up here as a
# near-total "Immediate" spike that reflected nothing real. The actual
# signal lives in the free-text notice_period_display column instead (e.g.
# "1 Month", "15 Days or less") -- see get_master_candidates_analytics()'s
# notice_period_dist in the Postgres snapshot, which the frontend uses.


def _range_filter(field: str, lo: Optional[float], hi: Optional[float]) -> str:
    # Half-open [lo, hi) via two explicit conditions -- NOT Typesense's
    # `[lo..hi]` bracket syntax, which is inclusive on BOTH ends and was
    # double-counting every document sitting exactly on a shared boundary
    # (e.g. exactly 20 lacs counted in both "12-20 L" and "20-35 L") --
    # confirmed in production: experience bucket counts summed to 3,080,487
    # against a true total of 2,786,500.
    if lo is None:
        return f"{field}:<{hi}"
    if hi is None:
        return f"{field}:>={lo}"
    return f"{field}:>={lo} && {field}:<{hi}"


async def _multi_search_counts(
    client: httpx.AsyncClient, field: str, buckets: list[tuple[str, Optional[float], Optional[float]]],
) -> list[dict[str, Any]]:
    body = {
        "searches": [
            {"collection": TS_COLLECTION, "q": "*", "per_page": 0,
             "filter_by": _range_filter(field, lo, hi)}
            for _, lo, hi in buckets
        ]
    }
    r = await client.post(
        f"{TYPESENSE_BASE}/multi_search", headers=TS_HEADERS, json=body,
        timeout=HTTP_TIMEOUT_TYPESENSE)
    r.raise_for_status()
    results = r.json().get("results", [])
    return [
        {"label": label, "count": (results[i].get("found", 0) if i < len(results) else 0)}
        for i, (label, _, _) in enumerate(buckets)
    ]


@router.get("/numeric_buckets")
async def get_numeric_buckets(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient() as client:
            ctc, ectc, exp = await asyncio.gather(
                _multi_search_counts(client, "current_ctc_lacs", CTC_BUCKETS),
                _multi_search_counts(client, "expected_ctc_lacs", EXPECTED_CTC_BUCKETS),
                _multi_search_counts(client, "total_experience_months", EXP_BUCKETS_MONTHS),
            )
    except httpx.HTTPError as e:
        logger.warning(f"[mc-analytics] numeric_buckets query failed: {e!r}")
        raise HTTPException(status_code=502, detail="typesense numeric bucket query failed")

    return {"ctc_buckets": ctc, "expected_ctc_buckets": ectc, "experience_buckets": exp}


# ─────────────────────────────────────────────────────────────────────────────
# /full_distributions -- uncapped long-tail categorical dimensions (skills,
# schools, degrees, fields of study, titles, employers, languages, location)
# via an offline export+aggregate pass into a dedicated new collection.
# ─────────────────────────────────────────────────────────────────────────────
FULL_ANALYTICS_COLLECTION_SCHEMA: dict[str, Any] = {
    "name": TS_FULL_ANALYTICS_COLLECTION,
    "default_sorting_field": "candidate_count",
    "fields": [
        {"name": "id", "type": "string"},                # f"{type}:{normalized_value}"
        {"name": "type", "type": "string", "facet": True},
        {"name": "value", "type": "string"},
        {"name": "candidate_count", "type": "int32"},
    ],
}


async def _ensure_full_analytics_collection(client: httpx.AsyncClient) -> None:
    r = await client.get(
        f"{TYPESENSE_BASE}/collections/{TS_FULL_ANALYTICS_COLLECTION}",
        headers=TS_HEADERS, timeout=HTTP_TIMEOUT_TYPESENSE)
    if r.status_code == 200:
        return
    if r.status_code != 404:
        r.raise_for_status()
    r = await client.post(
        f"{TYPESENSE_BASE}/collections", headers=TS_HEADERS,
        json=FULL_ANALYTICS_COLLECTION_SCHEMA, timeout=HTTP_TIMEOUT_TYPESENSE)
    r.raise_for_status()


async def _upsert_full_analytics_batch(client: httpx.AsyncClient, docs: list[dict]) -> tuple[int, list[str]]:
    if not docs:
        return 0, []
    body = "\n".join(json.dumps(d) for d in docs)
    r = await client.post(
        f"{TYPESENSE_BASE}/collections/{TS_FULL_ANALYTICS_COLLECTION}/documents/import",
        params={"action": "upsert"},
        headers={**TS_HEADERS, "Content-Type": "text/plain"},
        content=body, timeout=HTTP_TIMEOUT_TYPESENSE)
    r.raise_for_status()
    ok, errors = 0, []
    for line in r.text.strip().split("\n"):
        try:
            result = json.loads(line)
            if result.get("success"):
                ok += 1
            else:
                errors.append(str(result)[:200])
        except Exception:
            errors.append(line[:200])
    return ok, errors


_last_rebuild_at: Optional[float] = None  # module-level, single-process app (see main.py's uvicorn CMD)

# Fire-and-forget background task state -- NOT a request/response value.
# Both the Postgres RPC path (57014, PostgREST's statement_timeout) and this
# Typesense path (504, confirmed in production against an intermediate
# reverse proxy -- Nginx Proxy Manager in front of sync.xrilic.ai) failed
# for the same underlying reason: a multi-minute job was run synchronously
# inside a single held-open HTTP request, and something between the browser
# and this service killed that connection before the work finished. No
# timeout tuning fixes that -- the fix is to never hold the request open
# that long in the first place. This mirrors the exact pattern already
# proven at this scale by the portal_a backfill worker
# (backfill/worker.py's asyncio.create_task + a separate /status poll):
# POST returns instantly once the task is scheduled; the frontend polls
# GET .../rebuild/status for progress.
_full_dist_task: Optional[asyncio.Task] = None
_full_dist_status: dict[str, Any] = {"state": "idle", "started_at": None, "finished_at": None, "error": None, "result": None}

# httpx.AsyncClient()'s own default timeout is ~5s -- far too short for
# streaming+aggregating ~2.8M documents. Now moot for proxy purposes (the
# HTTP response returns immediately -- see above) but still needed for the
# background task's own internal calls to Typesense, which do take real time.
REBUILD_HTTP_TIMEOUT = httpx.Timeout(1800.0, connect=30.0)


async def _run_full_distributions_rebuild() -> None:
    global _last_rebuild_at
    try:
        async with httpx.AsyncClient(timeout=REBUILD_HTTP_TIMEOUT) as client:
            await _ensure_full_analytics_collection(client)
            # Reuses suggestions_aggregator.py's export+aggregate logic
            # UNCHANGED -- proven correct against the real field shapes
            # already, just with a much higher cap and written to our own
            # separate collection.
            report = await sagg.run_aggregation(
                sagg.export_documents(client), cap_per_dimension=FULL_DISTRIBUTIONS_CAP_PER_DIMENSION)
            rows = [
                {"id": r["id"], "type": r["type"], "value": r["value"], "candidate_count": r["candidate_count"]}
                for r in report.rows
            ]
            inserted, errors = 0, []
            batch_size = 500
            for i in range(0, len(rows), batch_size):
                ok, errs = await _upsert_full_analytics_batch(client, rows[i:i + batch_size])
                inserted += ok
                errors.extend(errs)
    except Exception as e:
        logger.warning(f"[mc-analytics] full_distributions rebuild failed: {e!r}")
        _full_dist_status.update(state="failed", finished_at=time.time(), error=str(e))
        return

    _last_rebuild_at = time.monotonic()
    result = {
        "documents_processed": report.documents_processed,
        "rows_written": inserted,
        "errors": len(errors),
        "per_dimension_distinct_counts": report.per_dimension_distinct_counts,
    }
    logger.info(f"[mc-analytics] full_distributions rebuild: {result}")
    _full_dist_status.update(state="completed", finished_at=time.time(), error=None, result=result)


@router.post("/full_distributions/rebuild")
async def rebuild_full_distributions(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    global _full_dist_task
    now = time.monotonic()
    if _last_rebuild_at is not None and (now - _last_rebuild_at) < REBUILD_COOLDOWN_SEC:
        return {"skipped": True, "started": False, "reason": f"rebuilt within the last {REBUILD_COOLDOWN_SEC}s"}
    if _full_dist_task is not None and not _full_dist_task.done():
        return {"skipped": True, "started": False, "reason": "a rebuild is already running"}

    _full_dist_status.update(state="running", started_at=time.time(), finished_at=None, error=None, result=None)
    _full_dist_task = asyncio.create_task(_run_full_distributions_rebuild())
    return {"skipped": False, "started": True}


@router.get("/full_distributions/rebuild/status")
async def get_full_distributions_rebuild_status(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    return _full_dist_status


TYPESENSE_MAX_PER_PAGE = 250  # Typesense's own hard cap on /documents/search -- higher values error out


async def _fetch_distribution(client: httpx.AsyncClient, dim_type: str, limit: int) -> list[dict[str, Any]]:
    """Paginates in chunks of TYPESENSE_MAX_PER_PAGE since Typesense rejects
    a per_page above that, however high `limit` (this endpoint's whole point
    is no artificial cap, so a limit above 250 must page rather than truncate)."""
    out: list[dict[str, Any]] = []
    page = 1
    while len(out) < limit:
        page_size = min(TYPESENSE_MAX_PER_PAGE, limit - len(out))
        r = await client.get(
            f"{TYPESENSE_BASE}/collections/{TS_FULL_ANALYTICS_COLLECTION}/documents/search",
            headers=TS_HEADERS,
            params={"q": "*", "query_by": "value", "filter_by": f"type:={dim_type}",
                    "sort_by": "candidate_count:desc", "per_page": page_size, "page": page},
            timeout=HTTP_TIMEOUT_TYPESENSE)
        if r.status_code == 404:
            break
        r.raise_for_status()
        hits = r.json().get("hits") or []
        if not hits:
            break
        out.extend({"label": h["document"]["value"], "count": h["document"]["candidate_count"]} for h in hits)
        if len(hits) < page_size:
            break  # last page
        page += 1
    return out


@router.get("/full_distributions")
async def get_full_distributions(
    types: str = Query(..., description="comma-separated dimension names"),
    limit: int = Query(500, ge=1, le=20000),
    user_id: str = Depends(require_global_superadmin),
) -> dict[str, Any]:
    type_list = [t.strip() for t in types.split(",") if t.strip()]
    if not type_list:
        raise HTTPException(status_code=400, detail="types is required")

    out: dict[str, list[dict[str, Any]]] = {}
    try:
        async with httpx.AsyncClient() as client:
            results = await asyncio.gather(*(_fetch_distribution(client, t, limit) for t in type_list))
        out = dict(zip(type_list, results))
    except httpx.HTTPError as e:
        logger.warning(f"[mc-analytics] full_distributions read failed: {e!r}")
        raise HTTPException(status_code=502, detail="typesense full_distributions query failed")

    return {"distributions": out}


# ─────────────────────────────────────────────────────────────────────────────
# /pg_snapshot/rebuild -- orchestrates the Postgres-only analytics (trend,
# expected CTC, contact-method breakdown, completeness, notice period --
# the handful of things genuinely absent from the Typesense index, see the
# module docstring). request_master_candidates_analytics_refresh() (the
# monolithic version, still used by the sibling MasterCandidatesAnalyticsPage)
# kept hitting 57014 statement timeout when PostgREST ran it synchronously
# for the browser -- SET LOCAL statement_timeout didn't reliably fix it in
# production. Rather than keep tuning against a timeout ceiling this service
# doesn't control, this calls 4 SPLIT, individually-cheap SQL functions
# (mc_analytics_group_totals/numeric/quality/trend -- each just 2-5
# single-pass full-table scans, not ~14 run sequentially) in parallel, each
# with its own generous httpx timeout this service DOES control, merges the
# results here, and writes the snapshot with one trivial insert. Same fix
# shape already proven for full_distributions/rebuild: move orchestration
# of anything that touches the full 2.8M-row table out of a single
# browser-triggered PostgREST call and into this backend.
# ─────────────────────────────────────────────────────────────────────────────
PG_SNAPSHOT_GROUP_FNS = [
    "mc_analytics_group_totals", "mc_analytics_group_numeric",
    "mc_analytics_group_quality", "mc_analytics_group_trend",
]
PG_SNAPSHOT_HTTP_TIMEOUT = httpx.Timeout(120.0, connect=30.0)  # generous headroom; each call is expected to take low single-digit seconds
PG_SNAPSHOT_COOLDOWN_SEC = 600

_last_pg_snapshot_at: Optional[float] = None

# Same fire-and-forget shape as full_distributions above -- the direct
# supabase.rpc() call this replaced hit 504/57014 the same way, confirming
# it's not really about Postgres specifically: any multi-second-plus job run
# synchronously inside one HTTP request is at the mercy of whatever timeout
# the browser, Nginx, or PostgREST enforces on that single connection.
_pg_snapshot_task: Optional[asyncio.Task] = None
_pg_snapshot_status: dict[str, Any] = {"state": "idle", "started_at": None, "finished_at": None, "error": None, "result": None}


async def _call_pg_group_fn(client: httpx.AsyncClient, fn_name: str) -> dict[str, Any]:
    r = await client.post(
        f"{SUPABASE_REST}/rpc/{fn_name}", headers=SB_HEADERS, json={},
        timeout=PG_SNAPSHOT_HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json() or {}


async def _run_pg_snapshot_rebuild() -> None:
    global _last_pg_snapshot_at
    start = time.monotonic()
    try:
        async with httpx.AsyncClient() as client:
            group_results = await asyncio.gather(
                *(_call_pg_group_fn(client, fn) for fn in PG_SNAPSHOT_GROUP_FNS))
            payload: dict[str, Any] = {}
            for group in group_results:
                payload.update(group)

            duration_ms = int((time.monotonic() - start) * 1000)
            write_r = await client.post(
                f"{SUPABASE_REST}/rpc/mc_analytics_write_snapshot", headers=SB_HEADERS,
                json={"p_payload": payload, "p_duration_ms": duration_ms},
                timeout=PG_SNAPSHOT_HTTP_TIMEOUT)
            write_r.raise_for_status()
    except Exception as e:
        logger.warning(f"[mc-analytics] pg_snapshot rebuild failed: {e!r}")
        _pg_snapshot_status.update(state="failed", finished_at=time.time(), error=str(e))
        return

    _last_pg_snapshot_at = time.monotonic()
    logger.info(f"[mc-analytics] pg_snapshot rebuild: duration_ms={duration_ms} keys={list(payload.keys())}")
    _pg_snapshot_status.update(
        state="completed", finished_at=time.time(), error=None,
        result={"duration_ms": duration_ms})


@router.post("/pg_snapshot/rebuild", dependencies=[Depends(require_shared_writes)])
async def rebuild_pg_snapshot(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    global _pg_snapshot_task
    now = time.monotonic()
    if _last_pg_snapshot_at is not None and (now - _last_pg_snapshot_at) < PG_SNAPSHOT_COOLDOWN_SEC:
        return {"skipped": True, "started": False, "reason": f"refreshed within the last {PG_SNAPSHOT_COOLDOWN_SEC}s"}
    if _pg_snapshot_task is not None and not _pg_snapshot_task.done():
        return {"skipped": True, "started": False, "reason": "a refresh is already running"}

    _pg_snapshot_status.update(state="running", started_at=time.time(), finished_at=None, error=None, result=None)
    _pg_snapshot_task = asyncio.create_task(_run_pg_snapshot_rebuild())
    return {"skipped": False, "started": True}


@router.get("/pg_snapshot/rebuild/status")
async def get_pg_snapshot_rebuild_status(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    return _pg_snapshot_status
