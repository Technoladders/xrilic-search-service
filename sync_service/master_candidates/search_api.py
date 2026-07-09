"""
sync_service/master_candidates/search_api.py

FastAPI router mounted at /mc.
POST /mc/search   — takes the InternalFilters payload from useInternalSearch.ts
                    returns { profiles, total, page, count_capped }
GET  /mc/health   — collection stats
"""

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request

from .config import (
    TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION,
    HTTP_TIMEOUT_TYPESENSE,
)
from .typesense_client import QUERY_BY, QUERY_BY_WEIGHTS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc", tags=["master_candidates"])


def _escape(v: str) -> str:
    """Escape a value for filter_by. Backtick-wrap and escape backticks."""
    return "`" + v.replace("`", "\\`") + "`"


def _filter_any(field: str, values: list[str]) -> str | None:
    if not values:
        return None
    return f"{field}:=[" + ",".join(_escape(v) for v in values) + "]"


def _filter_all_skills(values: list[str]) -> str | None:
    if not values:
        return None
    # ALL: chain with AND — Typesense supports array-contains semantics on
    # string[] fields via `:=` per-value
    return " && ".join([f"skills:={_escape(v)}" for v in values])


def _filter_none_skills(values: list[str]) -> str | None:
    if not values:
        return None
    return " && ".join([f"skills:!={_escape(v)}" for v in values])


def _build_filter_by(f: dict[str, Any]) -> str:
    parts: list[str] = []

    # skills: must-all, nice = at-least-one, exclude = none-of
    skill_chips = f.get("skillChips") or []
    must    = [c["label"] for c in skill_chips if c.get("mode") == "must"]
    nice    = [c["label"] for c in skill_chips if c.get("mode") == "nice"]
    exclude = [c["label"] for c in skill_chips if c.get("mode") == "exclude"]
    for p in [_filter_all_skills(must), _filter_any("skills", nice), _filter_none_skills(exclude)]:
        if p:
            parts.append(p)

    # titles (current only vs also past)
    titles = f.get("titles") or []
    include_past = bool(f.get("includePastTitles"))
    if titles:
        if include_past:
            parts.append(
                f"(title:=[{','.join(_escape(t) for t in titles)}] || "
                f"all_titles:=[{','.join(_escape(t) for t in titles)}])"
            )
        else:
            parts.append(f"title:=[{','.join(_escape(t) for t in titles)}]")

    # employer
    employers = f.get("currentEmployer") or []
    any_emp   = bool(f.get("anyEmployer"))
    if employers:
        if any_emp:
            parts.append(
                f"(current_employer:=[{','.join(_escape(e) for e in employers)}] || "
                f"all_employers:=[{','.join(_escape(e) for e in employers)}])"
            )
        else:
            parts.append(f"current_employer:=[{','.join(_escape(e) for e in employers)}]")

    # locations (OR)
    if p := _filter_any("location", f.get("locations") or []):
        parts.append(p)

    # education
    if p := _filter_any("schools", f.get("school") or []):
        parts.append(p)
    if p := _filter_any("degrees", f.get("degree") or []):
        parts.append(p)

    # years min/max (in months)
    y_min, y_max = f.get("yearsMin"), f.get("yearsMax")
    if y_min not in (None, ""):
        parts.append(f"total_experience_months:>={int(y_min)*12}")
    if y_max not in (None, ""):
        parts.append(f"total_experience_months:<={int(y_max)*12}")

    if f.get("hasContactOnly"):
        parts.append("has_contact:=true")
    if f.get("fullProfileOnly"):
        parts.append("has_full_profile:=true")

    return " && ".join(parts) if parts else ""


def _to_rr_profile(hit: dict[str, Any]) -> dict[str, Any]:
    """Normalize a Typesense hit into the RRProfile shape the frontend expects.
    (Contact fields left as availability-only — actual emails/phones come from
     Supabase on candidate detail view.)"""
    d = hit.get("document", {})
    return {
        "id":               d["id"],
        "status":           "complete",
        "name":             d.get("full_name") or "",
        "current_title":    d.get("title") or "",
        "current_employer": d.get("current_employer") or "",
        "location":         d.get("location") or "",
        "country_code":     d.get("country") or "",
        "linkedin_url":     d.get("linkedin_url"),
        "profile_pic":      d.get("profile_picture_url"),
        "connections":      d.get("followers"),
        "_skills":          d.get("skills") or [],
        "_jobHistory":      [],   # detail view fills this from Supabase
        "_education":       [],
        "_allEmails":       [],
        "_allPhones":       [],
        "_enriched":        bool(d.get("has_contact")),
        "_is_cached":       True,
        "_provider":        "internal",
        "_internal": {
            "masterId":           d["id"],
            "experienceDisplay":  d.get("experience_display"),
            "totalExpMonths":     d.get("total_experience_months"),
            "ctcDisplay":         d.get("current_ctc_display"),
            "noticeDisplay":      d.get("notice_period_display"),
            "hasFullProfile":     bool(d.get("has_full_profile")),
            "preferredLocations": d.get("preferred_locations") or [],
            "seniority":          d.get("seniority"),
            "headline":           d.get("headline"),
            "summary":            d.get("summary_short"),
        },
        "_score": hit.get("text_match"),
    }


@router.post("/search")
async def search(request: Request) -> dict[str, Any]:
    payload = await request.json()
    filters: dict[str, Any] = payload.get("filters") or {}
    page = max(1, int(payload.get("page", 1)))
    per_page = min(50, max(1, int(payload.get("per_page", 25))))

    q = (filters.get("keyword") or "").strip() or "*"

    ts_params: dict[str, Any] = {
        "q":                q,
        "query_by":         QUERY_BY,
        "query_by_weights": QUERY_BY_WEIGHTS,
        "per_page":         per_page,
        "page":             page,
        "num_typos":        "2,1,0,0,0,0,0,0,0,0,0,0",  # only allow typos on name & title
        "prioritize_exact_match": "true",
        "sort_by":          "_text_match:desc,data_freshness_ts:desc" if q != "*" else "data_freshness_ts:desc",
        "facet_by":         "has_full_profile,has_contact,sources,seniority,country,primary_source",
        "max_facet_values": "10",
        "highlight_fields": "full_name,title,skills_text",
    }
    filter_by = _build_filter_by(filters)
    if filter_by:
        ts_params["filter_by"] = filter_by

    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/search",
            headers=TS_HEADERS, params=ts_params, timeout=HTTP_TIMEOUT_TYPESENSE,
        )
    if r.status_code >= 400:
        logger.warning(f"typesense search failed: {r.status_code} {r.text[:400]}")
        raise HTTPException(status_code=502, detail="search backend error")
    data = r.json()

    return {
        "profiles":  [_to_rr_profile(h) for h in (data.get("hits") or [])],
        "total":     data.get("found", 0),
        "page":      page,
        "per_page":  per_page,
        "facets":    data.get("facet_counts", []),
        "took_ms":   data.get("search_time_ms"),
    }


@router.get("/health")
async def health() -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}",
            headers=TS_HEADERS, timeout=HTTP_TIMEOUT_TYPESENSE,
        )
    if r.status_code == 404:
        return {"collection": None, "status": "missing"}
    r.raise_for_status()
    body = r.json()
    return {
        "collection":       TS_COLLECTION,
        "num_documents":    body.get("num_documents"),
        "num_memory_bytes": body.get("num_memory_bytes"),
    }