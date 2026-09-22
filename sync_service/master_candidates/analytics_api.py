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
EXP_BUCKETS_MONTHS = [
    ("Fresher", None, 12), ("1-3 yrs", 12, 36), ("3-5 yrs", 36, 60),
    ("5-10 yrs", 60, 120), ("10+ yrs", 120, None),
]
NOTICE_BUCKETS_DAYS = [
    ("Immediate", None, 1), ("1-15 days", 1, 15), ("16-30 days", 15, 30),
    ("31-60 days", 30, 60), ("60+ days", 60, None),
]


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
            ctc, exp, notice = await asyncio.gather(
                _multi_search_counts(client, "current_ctc_lacs", CTC_BUCKETS),
                _multi_search_counts(client, "total_experience_months", EXP_BUCKETS_MONTHS),
                _multi_search_counts(client, "notice_period_days", NOTICE_BUCKETS_DAYS),
            )
    except httpx.HTTPError as e:
        logger.warning(f"[mc-analytics] numeric_buckets query failed: {e!r}")
        raise HTTPException(status_code=502, detail="typesense numeric bucket query failed")

    return {"ctc_buckets": ctc, "experience_buckets": exp, "notice_period_buckets": notice}


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


@router.post("/full_distributions/rebuild")
async def rebuild_full_distributions(user_id: str = Depends(require_global_superadmin)) -> dict[str, Any]:
    global _last_rebuild_at
    now = time.monotonic()
    if _last_rebuild_at is not None and (now - _last_rebuild_at) < REBUILD_COOLDOWN_SEC:
        return {"skipped": True, "reason": f"rebuilt within the last {REBUILD_COOLDOWN_SEC}s"}

    async with httpx.AsyncClient() as client:
        await _ensure_full_analytics_collection(client)
        # Reuses suggestions_aggregator.py's export+aggregate logic UNCHANGED
        # -- proven correct against the real field shapes already, just with
        # a much higher cap and written to our own separate collection.
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

    _last_rebuild_at = now
    logger.info(
        f"[mc-analytics] full_distributions rebuild: processed={report.documents_processed} "
        f"rows={len(rows)} inserted={inserted} errors={len(errors)}")
    return {
        "skipped": False,
        "documents_processed": report.documents_processed,
        "rows_written": inserted,
        "errors": len(errors),
        "per_dimension_distinct_counts": report.per_dimension_distinct_counts,
    }


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
