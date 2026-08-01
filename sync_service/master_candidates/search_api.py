"""
sync_service/master_candidates/search_api.py

FastAPI router mounted at /mc.
POST /mc/search   — takes the InternalFilters payload from useInternalSearch.ts
                    returns { profiles, total, page, per_page, facets, took_ms,
                    count_capped }
GET  /mc/health   — collection stats

v3 CHANGE (this file only, this turn — everything else identical to the
previous delivered version):

  TOTAL-COUNT HONESTY + GRACEFUL OVERFLOW, in the nice-skill branch of
  search(). Previously, when more profiles matched than
  RERANK_POOL_HARD_CAP, the response reported "total": RERANK_POOL_HARD_CAP
  (e.g. 1000) even when the true count was higher (e.g. 2230) — understating
  how many candidates actually match. Now:
    • "total" is ALWAYS the true Typesense `found` count, never capped.
    • Exact nice-skill match-count ranking still only extends
      RERANK_POOL_HARD_CAP deep (unchanged — that's a real performance
      tradeoff, not something removed).
    • Pages beyond that depth are fetched DIRECTLY from Typesense (still
      respecting the nice-skill OR filter, so every result genuinely
      matches), ordered by the native sort_by (last_active_date_ts desc,
      data_freshness_ts desc) instead of exact match-count. No page a user
      clicks to ever comes back empty because of the pool boundary.
    • "count_capped" now means "beyond this page, ordering is recency-based
      rather than exact nice-skill-count" — still real results, just a
      different (still correct) ordering past that depth.

  v2 CHANGES (carried from before, unchanged):
    RANKING: nice skills rank by exact match count (computed in Python over
    a bounded pool, since Typesense can't count OR-filter matches natively).
    sort_by leads with last_active_date_ts (most recently active first),
    data_freshness_ts as tiebreaker.

  CARRIED FORWARD (also unchanged, same file):
    • ids lookup short-circuit ({"ids": [...]}) — job-sourcing card hydration.
    • SECURITY: exclude_fields=emails_json,phones_json on every Typesense
      call. _to_rr_profile() naturally produces empty/False contacts once
      Typesense never returns those two fields.
"""

import json as _json
import logging
import re as _re
import time as _time
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

# Contacts never leave /mc/search — reveal (sourcexr-reveal) is the only
# contact source. contact_availability booleans + teaser still work since
# those read separate (non-excluded) boolean fields.
EXCLUDE_FIELDS = "emails_json,phones_json"

# Typesense's practical max per single request. We page through multiples of
# this to build the nice-skill rerank pool without ever changing per_page
# mid-loop (changing per_page between calls breaks Typesense's page-offset
# math, since offset = (page-1)*per_page using the CURRENT call's per_page).
TYPESENSE_MAX_PER_PAGE = 250

# How deep EXACT nice-skill match-count ranking goes. This is a performance
# bound, NOT a results-count bound — "total" in the response is always the
# true count regardless of this cap (see the v3 fix above). At the existing
# per_page cap of 50, this covers 20 pages of exactly-ranked results before
# falling back to recency ordering for anything deeper.
RERANK_POOL_HARD_CAP = 1000


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


def _extract_skill_chips(f: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    """(must, nice, exclude) labels from filters.skillChips."""
    chips = f.get("skillChips") or []
    must    = [c["label"] for c in chips if c.get("mode") == "must" and c.get("label")]
    nice    = [c["label"] for c in chips if c.get("mode") == "nice" and c.get("label")]
    exclude = [c["label"] for c in chips if c.get("mode") == "exclude" and c.get("label")]
    return must, nice, exclude


def _parse_years_bucket(v: Any) -> tuple[Optional[int], Optional[int]]:
    """
    'yearsExperience' arrives as the bucket string the sidebar sends
    (e.g. "0_1", "3_5", "10"), matching YEARS_OPTIONS in
    SourceXRSearchSidebar.tsx. Previously this function looked for
    "yearsMin"/"yearsMax" keys that are never sent by the frontend at all,
    so the filter silently did nothing on /mc/search. Returns
    (min_years, max_years); "10" (10+ years) has no upper bound.
    """
    if not v or not isinstance(v, str):
        return None, None
    if v == "10":
        return 10, None
    m = _re.match(r"^(\d+)_(\d+)$", v)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


# Day-range boundaries for the Activity filter, matching ACTIVITY_OPTIONS in
# SourceXRSearchSidebar.tsx exactly (Fresh/Active/Recent/Aging/Stale).
ACTIVITY_DAY_RANGES: dict[str, tuple[int, Optional[int]]] = {
    "Fresh":  (0, 14),
    "Active": (15, 30),
    "Recent": (31, 45),
    "OpenToWork": (0, 45),
    "Aging":  (46, 60),
    "Stale":  (60, None),
}


def _activity_filter(category: Any) -> Optional[str]:
    """
    Real backend predicate for the Activity filter. Filters on
    last_active_date_ts (int64, already live in the Typesense collection —
    confirmed, not assumed). Bounds are inclusive day counts converted to a
    unix-seconds window measured back from "now"; a smaller day count means
    more recent, i.e. a larger timestamp, hence the swapped comparison
    directions vs. the day range itself.
    """
    if not category or category not in ACTIVITY_DAY_RANGES:
        return None
    days_min, days_max = ACTIVITY_DAY_RANGES[category]
    now = int(_time.time())
    ts_upper = now - (days_min * 86400)
    parts = [f"last_active_date_ts:<={ts_upper}"]
    if days_max is not None:
        ts_lower = now - (days_max * 86400)
        parts.append(f"last_active_date_ts:>={ts_lower}")
    return " && ".join(parts)


def _build_filter_by(f: dict[str, Any]) -> str:
    parts: list[str] = []

    # skills: must-all, nice = at-least-one (inclusion only — ranking by
    # match count happens in search(), Typesense filters can't count).
    must, nice, exclude = _extract_skill_chips(f)
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

    # years of experience — FIX: previously read "yearsMin"/"yearsMax",
    # keys the frontend never sends (it sends "yearsExperience" as a
    # bucket string like "3_5" or "10"). That mismatch meant this filter
    # silently did nothing on /mc/search.
    y_min, y_max = _parse_years_bucket(f.get("yearsExperience"))
    if y_min is not None:
        parts.append(f"total_experience_months:>={y_min*12}")
    if y_max is not None:
        parts.append(f"total_experience_months:<={y_max*12}")

    # activity — FIX: was frontend-only (client-side post-filter on the
    # current page only, per SourceXRSearchSidebar.tsx's prior header
    # comment). Now a real predicate on last_active_date_ts so it actually
    # narrows what the server returns and the total count, not just what's
    # shown on the current page.
    if p := _activity_filter(f.get("activityCategory")):
        parts.append(p)

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
    # emails_json/phones_json are excluded from Typesense retrieval
    # (EXCLUDE_FIELDS) — d.get(...) resolves to None here, so _loads(None)
    # returns [], and everything downstream (emails/phones/_allEmails/
    # _allPhones/_enriched) comes out empty/False automatically.
    raw_emails = _loads(d.get("emails_json"))
    raw_phones = _loads(d.get("phones_json"))

    emails = [{
        "email":      e.get("value") or "",
        "type":       "work" if e.get("type") == "work" else "personal",
        "smtp_valid": "valid" if e.get("verified") else None,
        "grade":      None,
        "is_primary": i == 0,
    } for i, e in enumerate(raw_emails) if isinstance(e, dict) and e.get("value")]

    phones = [{
        "number":      p.get("value") or "",
        "type":        p.get("type") or "mobile",
        "validity":    "valid" if p.get("verified") else "unknown",
        "recommended": i == 0,
        "is_primary":  i == 0,
    } for i, p in enumerate(raw_phones) if isinstance(p, dict) and p.get("value")]

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
        "start_date_year": e.get("start_date_year"),
        "end_date_year":   e.get("end_date_year"),
        "period":      f"{e.get('start_date_year') or ''} - {e.get('end_date_year') or ''}"
                       if (e.get("start_date_year") or e.get("end_date_year")) else "",
    } for e in education if isinstance(e, dict)]

    contact_avail = {
        "personal_email": bool(d.get("contact_personal_email")) or len(emails) > 0,
        "phone":          bool(d.get("contact_phone")) or len(phones) > 0,
        "work_email":     any(e["type"] == "work" for e in emails),
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
        "_allEmails":  emails,
        "_allPhones":  phones,
        "_enriched":   len(emails) > 0 or len(phones) > 0,
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


async def _fetch_rerank_pool(
    base_params: dict[str, Any], needed: int,
) -> tuple[list[dict], int, list, Optional[float]]:
    """
    Fetches Typesense pages (fixed per_page=TYPESENSE_MAX_PER_PAGE — must
    stay constant across calls, since Typesense's offset math is
    (page-1)*per_page using the CURRENT call's per_page) until `needed` hits
    are collected, the pool exhausts, or RERANK_POOL_HARD_CAP is reached.
    Returns (hits, total_found, facet_counts, took_ms).
    """
    hits: list[dict] = []
    found = 0
    facets: list = []
    took_ms: Optional[float] = None
    target = min(max(needed, 1), RERANK_POOL_HARD_CAP)

    pool_page = 1
    while len(hits) < target:
        params = {**base_params, "page": pool_page, "per_page": TYPESENSE_MAX_PER_PAGE}
        data = await _ts_search(params)
        page_hits = data.get("hits") or []
        if pool_page == 1:
            found = data.get("found", 0)
            facets = data.get("facet_counts", [])
            took_ms = data.get("search_time_ms")
        hits.extend(page_hits)
        if len(page_hits) < TYPESENSE_MAX_PER_PAGE:
            break  # exhausted every actual match — nothing more to fetch
        pool_page += 1

    return hits, found, facets, took_ms


@router.post("/search")
async def search(request: Request, user_id: str = Depends(require_user)) -> dict[str, Any]:
    payload = await request.json()

    # ── ids lookup short-circuit — job-sourcing card hydration ────────────
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
            "profiles":     [_to_rr_profile(h) for h in (data.get("hits") or [])],
            "total":        data.get("found", 0),
            "page":         1,
            "per_page":     len(ids),
            "facets":       [],
            "took_ms":      data.get("search_time_ms"),
            "count_capped": False,
        }

    filters: dict[str, Any] = payload.get("filters") or {}
    page = max(1, int(payload.get("page", 1)))
    per_page = min(50, max(1, int(payload.get("per_page", 25))))

    q = (filters.get("keyword") or "").strip() or "*"
    _, nice, _ = _extract_skill_chips(filters)

    filter_by = _build_filter_by(filters)

    base_params: dict[str, Any] = {
        "q":                q,
        "query_by":         QUERY_BY,
        "query_by_weights": QUERY_BY_WEIGHTS,
        "num_typos":        "2,1,0,0,0,0,0,0,0,0,0,0",
        "prioritize_exact_match": "true",
        "facet_by":         "has_full_profile,has_contact,sources,seniority,country,primary_source",
        "max_facet_values": "10",
        "highlight_fields": "full_name,title,skills_text",
        "exclude_fields":   EXCLUDE_FIELDS,
        "sort_by": (
            "_text_match:desc,last_active_date_ts:desc,data_freshness_ts:desc"
            if q != "*" else
            "last_active_date_ts:desc,data_freshness_ts:desc"
        ),
    }
    if filter_by:
        base_params["filter_by"] = filter_by

    if not nice:
        # ── Simple path: no nice skills → Typesense's native sort_by is
        # already exactly correct. One call, same cost as before.
        ts_params = {**base_params, "page": page, "per_page": per_page}
        data = await _ts_search(ts_params)
        return {
            "profiles":     [_to_rr_profile(h) for h in (data.get("hits") or [])],
            "total":        data.get("found", 0),
            "page":         page,
            "per_page":     per_page,
            "facets":       data.get("facet_counts", []),
            "took_ms":      data.get("search_time_ms"),
            "count_capped": False,
        }

    # ── Nice-skill path ────────────────────────────────────────────────────
    needed = page * per_page

    if needed <= RERANK_POOL_HARD_CAP:
        # Fully exact-ranked: fetch the pool, count-rank in Python, slice.
        hits, found, facets, took_ms = await _fetch_rerank_pool(base_params, needed)

        nice_lower = {s.lower() for s in nice}

        def _rank_key(hit: dict[str, Any]) -> tuple:
            doc = hit.get("document", {})
            skills_lower = {str(s).lower() for s in (doc.get("skills") or [])}
            match_count = len(skills_lower & nice_lower)
            text_match = hit.get("text_match") or 0
            last_active = doc.get("last_active_date_ts") or 0
            freshness = doc.get("data_freshness_ts") or 0
            return (-match_count, -text_match, -last_active, -freshness)

        hits_sorted = sorted(hits, key=_rank_key)
        page_hits = hits_sorted[(page - 1) * per_page: page * per_page]

        return {
            "profiles":     [_to_rr_profile(h) for h in page_hits],
            # v3 FIX: always the TRUE count — never capped to the pool size.
            "total":        found,
            "page":         page,
            "per_page":     per_page,
            "facets":       facets,
            "took_ms":      took_ms,
            # true only if the true count exceeds what we exact-rank; pages
            # beyond this depth (below) still return real, filter-matching
            # results — just ordered by recency, not exact nice-skill count.
            "count_capped": found > RERANK_POOL_HARD_CAP,
        }

    # ── v3 FIX: page requested is beyond the exact-rank pool. Fetch THIS
    # SPECIFIC page directly from Typesense — still respects the nice-skill
    # OR filter (every result genuinely matches), just ordered by the native
    # sort_by (recency) instead of exact match-count this deep. This is what
    # keeps deep pagination from ever returning an empty/broken page.
    ts_params = {**base_params, "page": page, "per_page": per_page}
    data = await _ts_search(ts_params)
    return {
        "profiles":     [_to_rr_profile(h) for h in (data.get("hits") or [])],
        "total":        data.get("found", 0),
        "page":         page,
        "per_page":     per_page,
        "facets":       data.get("facet_counts", []),
        "took_ms":      data.get("search_time_ms"),
        "count_capped": True,
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