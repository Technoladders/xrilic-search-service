"""
sync_service/master_candidates/search_api.py

FastAPI router mounted at /mc.
POST /mc/search   — takes the InternalFilters payload from useInternalSearch.ts
                    returns { profiles, total, page, count_capped }
GET  /mc/health   — collection stats

v2 CHANGES (this file — no schema change, no reindex needed;
            last_active_date_ts + data_freshness_ts already exist & populated):
  1. RANKING moved fully to the backend:
       • nice skills leave filter_by and go into q — Typesense _text_match
         then scales with HOW MANY nice skills a profile matches
       • sort chain: _text_match:desc, last_active_date_ts:desc,
         data_freshness_ts:desc  (no q → last_active first, then freshness)
       • drop_tokens_threshold lets any SUBSET of nice tokens match,
         so 6/6 matchers rank above 2/6 instead of 2/6 being filtered out
     → the frontend must NOT re-order results anymore.
  2. IDS LOOKUP: payload {"ids": [...]} short-circuit — powers
     fetchMasterProfilesByIds() (job-sourcing card hydration). Previously this
     payload fell through to q="*" and returned arbitrary profiles.
  3. SECURITY: contacts never leave /mc/search. emails_json/phones_json are
     excluded from Typesense retrieval; _allEmails/_allPhones are always [],
     _enriched always False. Reveal (sourcexr-reveal) is the only contact
     source; contact_availability booleans + teaser still power the
     "email available" hint.
"""

import json as _json
import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from .config import (
    TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION,
    SUPABASE_URL, SB_HEADERS,
    HTTP_TIMEOUT_TYPESENSE,
)
from .typesense_client import QUERY_BY, QUERY_BY_WEIGHTS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc", tags=["master_candidates"])

# Contacts are stripped at retrieval time (defense-in-depth with the
# frontend sanitizeMcProfile belt).
EXCLUDE_FIELDS = "emails_json,phones_json"


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

    # skills: must = all required, exclude = none-of.
    # v2: NICE skills are NOT filtered here anymore — they move into q so the
    # text-match score ranks by how many matched. (To additionally REQUIRE at
    # least one nice skill, re-add: _filter_any("skills", nice) below.)
    skill_chips = f.get("skillChips") or []
    must    = [c["label"] for c in skill_chips if c.get("mode") == "must"]
    exclude = [c["label"] for c in skill_chips if c.get("mode") == "exclude"]
    for p in [_filter_all_skills(must), _filter_none_skills(exclude)]:
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


def _loads(s: Any) -> list:
    if not s:
        return []
    try:
        v = _json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []


def _period(e: dict) -> str:
    sy = e.get("start_date_year") or ""
    if e.get("is_current"):
        return f"{sy} - now" if sy else ""
    ey = e.get("end_date_year") or ""
    return f"{sy} - {ey}" if (sy or ey) else ""


def _to_rr_profile(hit: dict[str, Any]) -> dict[str, Any]:
    d = hit.get("document", {})

    experience = _loads(d.get("experience_json"))
    education  = _loads(d.get("education_json"))
    certs      = _loads(d.get("certifications_json"))
    # emails_json/phones_json are excluded from retrieval (EXCLUDE_FIELDS);
    # these stay as a safety net if the exclude is ever removed.
    raw_emails = _loads(d.get("emails_json"))
    raw_phones = _loads(d.get("phones_json"))

    job_history = [{
        "title":        e.get("title") or "",
        "company_name": e.get("company_name") or "",
        "company":      e.get("company_name") or "",
        "is_current":   bool(e.get("is_current")),
        "summary":      e.get("summary"),
        "period":       _period(e),
    } for e in experience if isinstance(e, dict)]

    education_out = [{
        "institution": e.get("school_name") or "",
        "school":      e.get("school_name") or "",
        "degree":      e.get("degree") or "",
        "field":       e.get("field_of_study") or "",
        "major":       e.get("field_of_study") or "",
        "period":      f"{e.get('start_date_year') or ''} - {e.get('end_date_year') or ''}"
                       if (e.get("start_date_year") or e.get("end_date_year")) else "",
    } for e in education if isinstance(e, dict)]

    contact_avail = {
        "personal_email": bool(d.get("contact_personal_email")) or len(raw_emails) > 0,
        "phone":          bool(d.get("contact_phone")) or len(raw_phones) > 0,
        "work_email":     any(isinstance(e, dict) and e.get("type") == "work" for e in raw_emails),
    }

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

        "experience_display":      d.get("experience_display"),
        "current_ctc_display":     d.get("current_ctc_display"),
        "notice_period_display":   d.get("notice_period_display"),
        "preferred_locations":     d.get("preferred_locations") or [],
        "last_active_date":        d.get("last_active_date"),
        "seniority":               d.get("seniority"),
        "headline":                d.get("headline"),
        "summary":                 d.get("summary_full") or d.get("summary_short"),
        "languages":               d.get("languages") or [],
        "total_experience_months": d.get("total_experience_months"),
        "has_full_profile":        bool(d.get("has_full_profile")),
        "contact_availability":    contact_avail,
        "certifications":          certs,

        "teaser": {
            "emails":              ["available"] if contact_avail["personal_email"] else [],
            "personal_emails":     ["available"] if contact_avail["personal_email"] else [],
            "professional_emails": ["available"] if contact_avail["work_email"] else [],
            "phones":              [{"number": "available", "is_premium": False}]
                                   if contact_avail["phone"] else [],
        },

        "_skills":     d.get("skills") or [],
        "_jobHistory": job_history,
        "_education":  education_out,

        # ── SECURITY: contacts never ride on search/list results ─────────
        # (sourcexr-reveal + org-reveal hydration are the ONLY contact
        #  sources; this was the credit-system bypass.)
        "_allEmails":  [],
        "_allPhones":  [],
        "_enriched":   False,

        "_is_cached":  True,
        "_provider":   "internal",
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
            "summary":            d.get("summary_full") or d.get("summary_short"),
            "lastActiveDate":     d.get("last_active_date"),
            "languages":          d.get("languages") or [],
        },
        "_score": hit.get("text_match"),
    }

async def require_user(authorization: str | None = Header(None)) -> str:
    """Verify the Supabase JWT by calling the auth endpoint. Returns user id."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[len("Bearer "):]
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"apikey": SB_HEADERS["apikey"],
                     "Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
    if r.status_code != 200:
        raise HTTPException(status_code=401, detail="invalid token")
    return (r.json() or {}).get("id", "")


async def _ts_search(ts_params: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/search",
            headers=TS_HEADERS, params=ts_params, timeout=HTTP_TIMEOUT_TYPESENSE,
        )
    if r.status_code >= 400:
        logger.warning(f"typesense search failed: {r.status_code} {r.text[:400]}")
        raise HTTPException(status_code=502, detail="search backend error")
    return r.json()


@router.post("/search")
async def search(request: Request, user_id: str = Depends(require_user)) -> dict[str, Any]:
    payload = await request.json()

    # ── v2: ids lookup — job-sourcing card hydration (fetchMasterProfilesByIds)
    ids = [str(i) for i in (payload.get("ids") or []) if i][:100]
    if ids:
        ts_params = {
            "q":              "*",
            "query_by":       QUERY_BY,
            "filter_by":      "id:=[" + ",".join(_escape(i) for i in ids) + "]",
            "per_page":       len(ids),
            "page":           1,
            "exclude_fields": EXCLUDE_FIELDS,
        }
        data = await _ts_search(ts_params)
        return {
            "profiles":  [_to_rr_profile(h) for h in (data.get("hits") or [])],
            "total":     data.get("found", 0),
            "page":      1,
            "per_page":  len(ids),
            "facets":    [],
            "took_ms":   data.get("search_time_ms"),
        }

    filters: dict[str, Any] = payload.get("filters") or {}
    page = max(1, int(payload.get("page", 1)))
    per_page = min(50, max(1, int(payload.get("per_page", 25))))

    # ── v2 RANKING: keyword + NICE skills form the query. Typesense's
    #    _text_match grows with the number of matched tokens, so a profile
    #    matching 6/6 nice skills outranks one matching 2/6. Tie-breaks:
    #    most recently active, then data freshness — all server-side.
    keyword = (filters.get("keyword") or "").strip()
    nice = [str(c["label"]) for c in (filters.get("skillChips") or [])
            if c.get("mode") == "nice" and c.get("label")]
    q_tokens = ([keyword] if keyword else []) + nice
    q = " ".join(q_tokens) if q_tokens else "*"

    sort_by = (
        "_text_match:desc,last_active_date_ts:desc,data_freshness_ts:desc"
        if q != "*"
        else "last_active_date_ts:desc,data_freshness_ts:desc"
    )

    ts_params: dict[str, Any] = {
        "q":                q,
        "query_by":         QUERY_BY,
        "query_by_weights": QUERY_BY_WEIGHTS,
        "per_page":         per_page,
        "page":             page,
        "num_typos":        "2,1,0,0,0,0,0,0,0,0,0,0",  # only allow typos on name & title
        "prioritize_exact_match": "true",
        "sort_by":          sort_by,
        "facet_by":         "has_full_profile,has_contact,sources,seniority,country,primary_source",
        "max_facet_values": "10",
        "highlight_fields": "full_name,title,skills_text",
        "exclude_fields":   EXCLUDE_FIELDS,
    }
    if q != "*":
        # Nice tokens are OPTIONAL: if the full token set yields fewer results
        # than this threshold, Typesense progressively drops tokens so any
        # SUBSET matches — ranked by how many actually matched.
        ts_params["drop_tokens_threshold"] = "250"

    filter_by = _build_filter_by(filters)
    if filter_by:
        ts_params["filter_by"] = filter_by

    data = await _ts_search(ts_params)

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