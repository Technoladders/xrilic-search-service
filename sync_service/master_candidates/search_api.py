"""
sync_service/master_candidates/search_api.py

FastAPI router mounted at /mc.
POST /mc/search      — takes the InternalFilters payload from useInternalSearch.ts
                        returns { profiles, total, page, per_page, facets, took_ms,
                        count_capped }. UNCHANGED behavior — profile_pic is
                        still the raw third-party photo URL, kept only for any
                        caller not yet migrated to /mc/search_v2.
POST /mc/search_v2   — identical filter/ranking/pagination to /mc/search (both
                        call _search_impl(); there is no duplicated search
                        logic to drift). The ONLY difference: profile_pic is a
                        same-origin proxy URL (/mc/avatar/<id>) instead of the
                        raw Naukri CDN URL, or null if there's no real photo.
GET  /mc/avatar/{id} — streams the candidate's photo bytes from our own
                        origin. Public (no auth — an <img src> can't carry an
                        Authorization header); the candidate UUID is opaque
                        and only obtainable by first authenticating through
                        one of the two search routes above.
GET  /mc/health      — collection stats

v5 CHANGE (this turn): moved Boolean query semantics into a testable
query-planning layer (./search/) and fixed two real bugs found there:

  1. NICE-skill inclusion was ANDed onto MUST/EXCLUDE (`skills:=must &&
     skills:=[nice-any]`), silently making "nice-to-have" skills a hard
     mandatory-OR filter — a candidate with zero nice-skill matches was
     excluded entirely, the opposite of what the sidebar UI implies.
     Confirmed live: adding a 3rd nice skill (a common one) to a 2-nice
     search changed total 4,805 -> 118,196; promoting it to `must` instead
     collapsed it to 1,604. Fixed to the explicit, confirmed product
     semantics: (ALL must) OR (ANY nice) when both are set — see
     search/skill_logic.py.
  2. `keyword` was passed verbatim into Typesense's `q`, relying on the
     literal words "AND"/"OR"/"NOT" coincidentally overlapping with real
     query tokens — Typesense's `q` has no native AND/OR/parenthesization
     (confirmed against Typesense's docs). Replaced with a real parser
     (search/keyword_query.py) with an explicit EXACT (single Typesense
     call, exact `found`) vs. BOUNDED (Typesense can't express an OR/union
     natively, so evaluated over capped pools with an honest, possibly-
     conservative total and count_capped=True) distinction — never
     presenting a bounded/approximate count as exact.

Neither change alters `_search_impl`'s response shape
({profiles, total, page, per_page, facets, took_ms, count_capped}) — that
contract is frozen because useUnifiedWaterfallSearch.ts's entire internal/
external waterfall fallback decision is driven by `total` alone. See the
implementation plan for the full boundary rationale; nothing in this file
talks to, or is aware of, the external (ContactOut/RocketReach) providers.

v4 CHANGE (carried from before, unchanged): stop leaking the raw Naukri
profile-photo URL to the browser — it was visible in /mc/search's JSON
response body, and rendering it in an <img> made the browser fetch Naukri
directly, picking up a third-party tracking cookie (_t_ds) in the process.
/mc/search is left completely unchanged (still the raw URL) so nothing that
already calls it breaks; /mc/search_v2 and /mc/avatar/{id} are additive.

v3 CHANGE (carried from before, unchanged):

  TOTAL-COUNT HONESTY + GRACEFUL OVERFLOW, in the nice-skill/keyword-Tier-A
  branch of search(). "total" is ALWAYS the true Typesense `found` count
  for that exact query, never capped — only exact match-count/relevance
  ranking DEPTH is bounded (RERANK_POOL_HARD_CAP), and pages beyond that
  depth are fetched directly from Typesense (native sort_by), still
  respecting every filter, just ordered by recency past that depth.

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
import uuid
from typing import Any, Optional
from urllib.parse import urljoin

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import Response

from .config import (
    TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION,
    SUPABASE_URL, SB_HEADERS,
    HTTP_TIMEOUT_TYPESENSE,
    PUBLIC_BASE_URL,
)
from .typesense_client import QUERY_BY, QUERY_BY_WEIGHTS, get_document
from .search import keyword_query, skill_logic
from .search.bucket_pagination import compute_bucket_slice
from .search.query_types import KeywordSyntaxError, SearchPlan
from .search.ranking import fetch_bounded_pool, fetch_direct_slice, rank_key, rank_key_bucket_b

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc", tags=["master_candidates"])

# Contacts never leave /mc/search — reveal (sourcexr-reveal) is the only
# contact source. contact_availability booleans + teaser still work since
# those read separate (non-excluded) boolean fields.
EXCLUDE_FIELDS = "emails_json,phones_json"

# Typesense's practical max per single request. We page through multiples of
# this to build bounded pools (nice-skill rerank AND keyword Tier-B union)
# without ever changing per_page mid-loop (changing per_page between calls
# breaks Typesense's page-offset math, since offset = (page-1)*per_page
# using the CURRENT call's per_page).
TYPESENSE_MAX_PER_PAGE = 250

# How deep EXACT nice-skill match-count ranking / keyword Tier-B union goes.
# This is a performance bound, NOT a results-count bound for Tier A / plain
# nice-skill queries — "total" there is always the true Typesense `found`
# count regardless of this cap. For Tier-B keyword OR/union, this IS a
# real bound on the reported total (see search/keyword_query.py) — that's
# the honest, deliberate tradeoff documented in the implementation plan.
RERANK_POOL_HARD_CAP = 1000


def _escape(v: str) -> str:
    """Escape a value for filter_by. Backtick-wrap and escape backticks."""
    return "`" + v.replace("`", "\\`") + "`"


def _filter_any(field: str, values: list[str]) -> str | None:
    if not values:
        return None
    return f"{field}:=[" + ",".join(_escape(v) for v in values) + "]"


def _parse_years_bucket(v: Any) -> tuple[Optional[int], Optional[int]]:
    """
    'yearsExperience' arrives as the bucket string the sidebar sends
    (e.g. "0_1", "3_5", "10"), matching YEARS_OPTIONS in
    SourceXRSearchSidebar.tsx. Returns (min_years, max_years); "10" (10+
    years) has no upper bound.
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
    "OpenToWork": (0, 90),
    "Aging":  (46, 60),
    "Stale":  (60, None),
}


def _activity_filter(category: Any) -> Optional[str]:
    """
    Real backend predicate for the Activity filter. Filters on
    last_active_date_ts (int64, already live in the Typesense collection).
    Bounds are inclusive day counts converted to a unix-seconds window
    measured back from "now"; a smaller day count means more recent, i.e.
    a larger timestamp, hence the swapped comparison directions vs. the
    day range itself.
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


def _build_other_hard_filters(f: dict[str, Any]) -> str:
    """
    Everything EXCEPT skills (must/nice/exclude, owned by skill_logic.py)
    and keyword (owned by keyword_query.py): titles, employer, location,
    education, experience, activity, contact, full-profile.
    """
    parts: list[str] = []

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

    if p := _filter_any("location", f.get("locations") or []):
        parts.append(p)

    if p := _filter_any("schools", f.get("school") or []):
        parts.append(p)
    if p := _filter_any("degrees", f.get("degree") or []):
        parts.append(p)

    y_min, y_max = _parse_years_bucket(f.get("yearsExperience"))
    if y_min is not None:
        parts.append(f"total_experience_months:>={y_min*12}")
    if y_max is not None:
        parts.append(f"total_experience_months:<={y_max*12}")

    if p := _activity_filter(f.get("activityCategory")):
        parts.append(p)

    if f.get("hasContactOnly"):
        parts.append("has_contact:=true")
    if f.get("fullProfileOnly"):
        parts.append("has_full_profile:=true")

    return " && ".join(parts) if parts else ""


def _build_search_plan(filters: dict[str, Any]) -> SearchPlan:
    """Raises KeywordSyntaxError for a malformed `keyword` Boolean expression."""
    skills = skill_logic.extract_skill_chips(filters)
    keyword_ast = keyword_query.parse(filters.get("keyword") or "")
    return SearchPlan(
        keyword_ast=keyword_ast,
        skills=skills,
        inclusion_filter_by=skill_logic.build_inclusion_filter(skills) or "",
        exclude_filter_by=skill_logic.build_exclude_filter(skills) or "",
        other_hard_filter_by=_build_other_hard_filters(filters),
    )


def _combined_filter_by(plan: SearchPlan) -> str:
    """
    ANDs inclusion/exclude/other-hard-filters together. inclusion_filter_by
    may itself contain a top-level `||` (when both MUST and NICE are set —
    see skill_logic.build_inclusion_filter) — since `&&` binds tighter than
    `||`, joining it unparenthesized with the other AND-ed clauses would
    silently scope EXCLUDE onto only the last OR-branch instead of the
    whole inclusion result (e.g. "A || B && C" parses as "A || (B && C)",
    not "(A || B) && C"). Wrapping it here keeps EXCLUDE applied to the
    outside of whichever inclusion branch a candidate matched through,
    regardless of how skill_logic composed the inner clause.
    """
    inclusion = f"({plan.inclusion_filter_by})" if plan.inclusion_filter_by else ""
    return " && ".join(
        p for p in [inclusion, plan.exclude_filter_by, plan.other_hard_filter_by] if p
    )


def _bucket_filter(inclusion: str | None, plan: SearchPlan) -> str:
    """Same AND-composition as _combined_filter_by, for one bucket's own
    inclusion clause instead of the combined (ALL must) OR (ANY nice) one."""
    parts = [f"({inclusion})"] if inclusion else []
    if plan.exclude_filter_by:
        parts.append(plan.exclude_filter_by)
    if plan.other_hard_filter_by:
        parts.append(plan.other_hard_filter_by)
    return " && ".join(parts)


async def _count_only(params: dict[str, Any]) -> int:
    data = await _ts_search({**params, "page": 1, "per_page": 1})
    return data.get("found", 0)


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


def _is_real_photo(url: str | None) -> bool:
    """Naukri serves a generic grey placeholder silhouette
    (static.naukimg.com/.../defaultAvatar.<hash>.svg) for candidates with no
    real photo. Treat that as "no photo" — same guard as the frontend's
    NaukriCandidatesPage.tsx:92 (!photo_url.includes('defaultAvatar'))."""
    return bool(url) and "defaultAvatar" not in url


def _avatar_url(candidate_id: str) -> str:
    return f"{PUBLIC_BASE_URL}/mc/avatar/{candidate_id}"


def _to_rr_profile(hit: dict[str, Any], avatar_proxy: bool) -> dict[str, Any]:
    """
    avatar_proxy=False reproduces /mc/search's pre-existing output exactly
    (profile_pic = the raw third-party URL). avatar_proxy=True is the
    photo-safe path used by /mc/search_v2. No default value on purpose: every
    call site must state explicitly which behavior it wants.
    """
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
        "profile_pic": (
            _avatar_url(d["id"]) if _is_real_photo(d.get("profile_picture_url")) else None
        ) if avatar_proxy else d.get("profile_picture_url"),
        "connections":      d.get("followers"),

        "experience_display":      d.get("experience_display"),
        "current_ctc_lacs":        d.get("current_ctc_lacs"),
        "expected_ctc_lacs":       d.get("expected_ctc_lacs"),
        "current_ctc_display":     d.get("current_ctc_display"),
        "notice_period_days":      d.get("notice_period_days"),
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
            "currentCtcLacs":     d.get("current_ctc_lacs"),
            "expectedCtcLacs":    d.get("expected_ctc_lacs"),
            "ctcDisplay":         d.get("current_ctc_display"),
            "noticePeriodDays":   d.get("notice_period_days"),
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


def _log_search(plan: SearchPlan, filter_by: str, q: str, tier: str,
                 found: int, took_ms: Optional[float], capped: bool,
                 total_ms: float) -> None:
    """Structured, non-PII debug log. Never logs candidate names/contacts/resumes."""
    logger.info(
        "mc_search "
        f"must_count={len(plan.skills.must)} nice_count={len(plan.skills.nice)} "
        f"exclude_count={len(plan.skills.exclude)} keyword_tier={tier} "
        f"q={q!r} filter_by={filter_by!r} typesense_found={found} "
        f"typesense_search_ms={took_ms} count_capped={capped} "
        f"backend_total_ms={total_ms:.1f}"
    )


async def _search_two_bucket(
    plan: SearchPlan, page: int, per_page: int, base_params: dict[str, Any],
    tier: str, exact_q: Optional[str],
) -> tuple[list[dict], int, bool, Optional[float]]:
    """
    MUST-completeness-first ranking: entered only when BOTH must and nice
    are set (see _search_impl's gating) and keyword tier != "B" (that
    combination is an explicit, documented, tested follow-up — see the
    implementation plan's Edge Cases). Two disjoint, exact Typesense
    filters instead of one combined OR filter:

      Bucket A: ALL must matched (regardless of nice) — ranked by
                nice_match_count (existing rank_key, unchanged).
      Bucket B: NOT all must matched, but NICE-qualified — ranked by
                must_partial_match_count first, then nice_match_count
                (rank_key_bucket_b).

    Bucket routing (which bucket a page's data comes from) is decided
    ONLY by each bucket's EXACT count (countA/countB), never by whether a
    bucket's own ranking pool was exceeded — that's the critical invariant
    from the implementation plan: a bucket exceeding RERANK_POOL_HARD_CAP
    only degrades that bucket's OWN ranking to native order for the
    portion beyond its pool depth (via fetch_direct_slice, which stays
    scoped to that bucket's own filter_by) — it can never cause the other
    bucket's candidates to appear out of order.

    Returns (ordered_hits, total, count_capped, took_ms) — the caller
    builds the final response dict so the frozen contract lives in one place.
    """
    q = exact_q if tier == "A" else "*"
    common_params = {**base_params, "q": q}
    if tier == "A":
        common_params["drop_tokens_threshold"] = 0

    filter_a = _bucket_filter(skill_logic.build_must_complete_filter(plan.skills), plan)
    filter_b = _bucket_filter(skill_logic.build_bucket_b_inclusion_filter(plan.skills), plan)

    params_a = {**common_params, "filter_by": filter_a}
    params_b = {**common_params, "filter_by": filter_b}

    needed_a = page * per_page
    hits_a_pool, found_a, _facets_a, took_ms = await fetch_bounded_pool(
        _ts_search, params_a, needed_a, RERANK_POOL_HARD_CAP, TYPESENSE_MAX_PER_PAGE,
    )

    slice_ = compute_bucket_slice(page, per_page, found_a)
    capped = False

    hits_a: list[dict] = []
    if slice_.needs_a:
        if needed_a <= RERANK_POOL_HARD_CAP:
            ranked_a = sorted(hits_a_pool, key=lambda h: rank_key(h, plan.skills.nice))
            hits_a = ranked_a[slice_.a_offset: slice_.a_offset + slice_.a_limit]
        else:
            hits_a, _ = await fetch_direct_slice(
                _ts_search, params_a, "last_active_date_ts:desc,data_freshness_ts:desc",
                slice_.a_offset, slice_.a_limit,
            )
            capped = True

    # found_b is needed for an accurate `total` regardless of whether this
    # specific page touches Bucket B at all.
    found_b = await _count_only(params_b)

    hits_b: list[dict] = []
    if slice_.needs_b:
        needed_b = slice_.b_offset + slice_.b_limit
        if needed_b <= RERANK_POOL_HARD_CAP:
            hits_b_pool, _found_b_pool, _facets_b, _took_b = await fetch_bounded_pool(
                _ts_search, params_b, needed_b, RERANK_POOL_HARD_CAP, TYPESENSE_MAX_PER_PAGE,
            )
            ranked_b = sorted(
                hits_b_pool,
                key=lambda h: rank_key_bucket_b(h, plan.skills.must, plan.skills.nice),
            )
            hits_b = ranked_b[slice_.b_offset: slice_.b_offset + slice_.b_limit]
        else:
            hits_b, _ = await fetch_direct_slice(
                _ts_search, params_b, "last_active_date_ts:desc,data_freshness_ts:desc",
                slice_.b_offset, slice_.b_limit,
            )
            capped = True

    logger.info(
        f"mc_search_bucket bucket_a_count={found_a} bucket_b_count={found_b} "
        f"page={page} per_page={per_page} count_capped={capped}"
    )
    return hits_a + hits_b, found_a + found_b, capped, took_ms


async def _search_impl(payload: dict[str, Any], avatar_proxy: bool) -> dict[str, Any]:
    """
    Shared by /mc/search (avatar_proxy=False) and /mc/search_v2
    (avatar_proxy=True) — identical filter/ranking/pagination logic, just
    parameterized on `payload` instead of reading it from a Request
    directly, so both routes run the same code path and can never drift
    apart. avatar_proxy only affects what _to_rr_profile() puts in
    "profile_pic".

    Response shape is a FROZEN CONTRACT — every branch below returns
    exactly {profiles, total, page, per_page, facets, took_ms,
    count_capped}. useUnifiedWaterfallSearch.ts's entire internal/external
    waterfall fallback decision is driven by `total` alone; nothing here
    may change that shape or add new top-level fields the frontend doesn't
    already read.
    """
    start_time = _time.monotonic()

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
            "profiles":     [_to_rr_profile(h, avatar_proxy) for h in (data.get("hits") or [])],
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

    try:
        plan = _build_search_plan(filters)
    except KeywordSyntaxError as e:
        raise HTTPException(status_code=400, detail=f"invalid keyword expression: {e}")

    filter_by = _combined_filter_by(plan)
    tier, exact_q = keyword_query.plan_keyword(plan.keyword_ast)

    base_params: dict[str, Any] = {
        "query_by":         QUERY_BY,
        "query_by_weights": QUERY_BY_WEIGHTS,
        "num_typos":        "2,1,0,0,0,0,0,0,0,0,0,0",
        "prioritize_exact_match": "true",
        "facet_by":         "has_full_profile,has_contact,sources,seniority,country,primary_source",
        "max_facet_values": "10",
        "highlight_fields": "full_name,title,skills_text",
        "exclude_fields":   EXCLUDE_FIELDS,
    }

    # MUST-completeness-first ranking: when both MUST and NICE are set, the
    # single combined (ALL must) OR (ANY nice) filter can't express "how
    # MUST-complete is this candidate," so a NICE-only candidate could
    # outrank a MUST-complete one on recency alone. Two-bucket tiering
    # fixes this — see _search_two_bucket. Excluded for keyword tier "B"
    # (OR/parens): combining bucket tiering with the bounded keyword-OR
    # evaluator is an explicit, documented follow-up (implementation plan,
    # Edge Cases) — that combination keeps today's single-filter behavior
    # rather than silently dropping MUST-first ordering or crashing.
    if plan.skills.must and plan.skills.nice and tier != "B":
        hits, found, capped, took_ms = await _search_two_bucket(
            plan, page, per_page, base_params, tier, exact_q,
        )
        _log_search(plan, filter_by, exact_q if tier == "A" else "*", tier, found, took_ms,
                    capped, (_time.monotonic() - start_time) * 1000)
        return {
            "profiles":     [_to_rr_profile(h, avatar_proxy) for h in hits],
            "total":        found,
            "page":         page,
            "per_page":     per_page,
            "facets":       [],
            "took_ms":      took_ms,
            "count_capped": capped,
        }

    # Nice-match-count ranking only differentiates candidates when it can
    # take more than one value among the candidates that pass the inclusion
    # filter. With a single nice skill and no MUST, the inclusion filter
    # (skills:=[x]) already forces every match to have exactly that skill —
    # ranking by "does it have x" is degenerate (always 1) and pooling for
    # it buys nothing but an extra round-trip and a misleading
    # count_capped=True on a query whose total was always exact. Confirmed
    # live: nice=[react] alone returned count_capped=true with no actual
    # ranking ambiguity. With MUST also set, or 2+ nice skills, match-count
    # is a real 0..N range and the pool remains worth it.
    nice_ranking_is_meaningful = bool(plan.skills.nice) and (
        len(plan.skills.nice) > 1 or bool(plan.skills.must)
    )
    needs_pool = nice_ranking_is_meaningful or tier == "B"

    if not needs_pool:
        # ── Simple path: single native Typesense call. Same cost as before
        # this change for the common case (no nice skills, no OR/parens in
        # keyword). ──────────────────────────────────────────────────────
        q = exact_q if tier == "A" else "*"
        ts_params = {
            **base_params, "q": q, "page": page, "per_page": per_page,
            "sort_by": ("_text_match:desc,last_active_date_ts:desc,data_freshness_ts:desc"
                        if q != "*" else "last_active_date_ts:desc,data_freshness_ts:desc"),
        }
        if tier == "A":
            ts_params["drop_tokens_threshold"] = 0
        if filter_by:
            ts_params["filter_by"] = filter_by
        data = await _ts_search(ts_params)
        found = data.get("found", 0)
        _log_search(plan, filter_by, q, tier, found, data.get("search_time_ms"),
                    False, (_time.monotonic() - start_time) * 1000)
        return {
            "profiles":     [_to_rr_profile(h, avatar_proxy) for h in (data.get("hits") or [])],
            "total":        found,
            "page":         page,
            "per_page":     per_page,
            "facets":       data.get("facet_counts", []),
            "took_ms":      data.get("search_time_ms"),
            "count_capped": False,
        }

    if tier == "B":
        # ── Keyword Tier-B: bounded OR/union evaluation, strictly internal
        # to this function — never touches the response shape below. ────
        bound_result = await keyword_query.evaluate_bounded(
            plan.keyword_ast,
            ts_search=_ts_search,
            base_params=base_params,
            structured_filter_by=filter_by,
            pool_cap=RERANK_POOL_HARD_CAP,
            page_size=TYPESENSE_MAX_PER_PAGE,
        )
        hits, found, capped = bound_result.hits, bound_result.total, bound_result.capped
        took_ms = None
    else:
        # tier in ("empty", "A"), but nice skills need ranking — reuse the
        # bounded-pool pattern (same as before this change, just extracted).
        q = exact_q if tier == "A" else "*"
        pool_params = {**base_params, "q": q}
        if tier == "A":
            pool_params["drop_tokens_threshold"] = 0
        if filter_by:
            pool_params["filter_by"] = filter_by

        needed = page * per_page
        if needed <= RERANK_POOL_HARD_CAP:
            hits, found, facets, took_ms = await fetch_bounded_pool(
                _ts_search, pool_params, needed, RERANK_POOL_HARD_CAP, TYPESENSE_MAX_PER_PAGE,
            )
            capped = found > RERANK_POOL_HARD_CAP
        else:
            # Requested page is beyond the exact-rank pool depth — fetch
            # THIS SPECIFIC page directly from Typesense (still respects
            # every filter; just ordered by recency past this depth).
            direct_params = {
                **pool_params, "page": page, "per_page": per_page,
                "sort_by": "last_active_date_ts:desc,data_freshness_ts:desc",
            }
            data = await _ts_search(direct_params)
            found = data.get("found", 0)
            _log_search(plan, filter_by, q, tier, found, data.get("search_time_ms"),
                        True, (_time.monotonic() - start_time) * 1000)
            return {
                "profiles":     [_to_rr_profile(h, avatar_proxy) for h in (data.get("hits") or [])],
                "total":        found,
                "page":         page,
                "per_page":     per_page,
                "facets":       data.get("facet_counts", []),
                "took_ms":      data.get("search_time_ms"),
                "count_capped": True,
            }

    hits_sorted = sorted(hits, key=lambda h: rank_key(h, plan.skills.nice))
    page_hits = hits_sorted[(page - 1) * per_page: page * per_page]

    log_q = exact_q if tier == "A" else (filters.get("keyword") or "*") if tier == "B" else "*"
    _log_search(plan, filter_by, log_q, tier, found, took_ms, capped,
                (_time.monotonic() - start_time) * 1000)
    return {
        "profiles":     [_to_rr_profile(h, avatar_proxy) for h in page_hits],
        "total":        found,
        "page":         page,
        "per_page":     per_page,
        "facets":       [],
        "took_ms":      took_ms,
        "count_capped": capped,
    }


@router.post("/search")
async def search(request: Request, user_id: str = Depends(require_user)) -> dict[str, Any]:
    payload = await request.json()
    return await _search_impl(payload, avatar_proxy=False)


@router.post("/search_v2")
async def search_v2(request: Request, user_id: str = Depends(require_user)) -> dict[str, Any]:
    """
    Identical filter/ranking/pagination behavior to /mc/search — both call
    _search_impl, so there is no duplicated search logic to drift out of sync.
    The ONLY difference is avatar_proxy=True, which changes exactly one field
    inside _to_rr_profile: profile_pic becomes a same-origin proxy URL
    (/mc/avatar/<id>) instead of the raw third-party photo URL.
    """
    payload = await request.json()
    return await _search_impl(payload, avatar_proxy=True)


# ── Avatar proxy ─────────────────────────────────────────────────────────
# GET /mc/avatar/{candidate_id} — streams the candidate's photo bytes from
# our own origin so the browser never sees or requests the raw Naukri CDN
# URL. Public (no Depends(require_user)): an <img src> request can't carry
# an Authorization header, and the candidate UUID is opaque — obtaining one
# requires first authenticating through /mc/search or /mc/search_v2.

_ALLOWED_PHOTO_HOSTS = {"p.naukri.com", "static.naukimg.com"}
_ALLOWED_PHOTO_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}
_MAX_AVATAR_BYTES = 2 * 1024 * 1024          # real Naukri headshots are ~4 KB
_MAX_PHOTO_REDIRECTS = 3
_UPSTREAM_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
    "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


def _is_allowed_photo_host(url: str) -> bool:
    """HTTPS + exact-hostname allowlist — no substring/`in` checks. Only the
    two hosts confirmed live for Naukri's photo CDN."""
    try:
        parsed = httpx.URL(url)
    except Exception:
        return False
    return parsed.scheme == "https" and parsed.host in _ALLOWED_PHOTO_HOSTS


async def _fetch_avatar_bytes(client: httpx.AsyncClient, start_url: str) -> tuple[bytes, str] | None:
    """
    Fetches an image from an allowlisted photo host, following redirects
    MANUALLY so every hop — the starting URL and every subsequent Location
    header — is independently re-validated against the host allowlist BEFORE
    it is ever requested.

    Deliberately NOT `follow_redirects=True`: that validates only the first
    URL and then blindly trusts however many redirects the server issues —
    an allowlisted host could 302 to an arbitrary internal or external target
    and the client would follow it unquestioned. This is the SSRF-via-redirect
    class of bug; the loop below closes it by re-checking the allowlist on
    every hop, capped at _MAX_PHOTO_REDIRECTS.

    Streams the body and aborts as soon as _MAX_AVATAR_BYTES is exceeded,
    rather than buffering an unbounded response before checking its size.

    Returns (body_bytes, content_type) on success, None on ANY failure — every
    failure mode is intentionally collapsed to the same outcome by the caller
    (an identical 204); only server logs distinguish why.
    """
    current = start_url
    for hop in range(_MAX_PHOTO_REDIRECTS + 1):
        if not _is_allowed_photo_host(current):
            logger.warning(f"[mc-avatar] blocked non-allowlisted host at hop {hop}")
            return None

        try:
            async with client.stream(
                "GET", current, headers=_UPSTREAM_HEADERS,
                follow_redirects=False, timeout=10.0,
            ) as resp:
                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location")
                    if not location:
                        return None
                    current = urljoin(current, location)
                    continue

                if resp.status_code != 200:
                    return None

                content_type = resp.headers.get("content-type", "").split(";")[0].strip().lower()
                if content_type not in _ALLOWED_PHOTO_CONTENT_TYPES:
                    return None

                declared_len = resp.headers.get("content-length")
                if declared_len and declared_len.isdigit() and int(declared_len) > _MAX_AVATAR_BYTES:
                    return None

                chunks = bytearray()
                async for chunk in resp.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > _MAX_AVATAR_BYTES:
                        return None     # abort mid-stream — never buffer past the cap
                if not chunks:
                    return None
                return bytes(chunks), content_type
        except httpx.HTTPError as e:
            logger.warning(f"[mc-avatar] upstream fetch failed: {e}")
            return None

    logger.warning("[mc-avatar] too many redirects")
    return None


@router.get("/avatar/{candidate_id}")
async def avatar(candidate_id: str) -> Response:
    """
    Public — no auth dependency. See module note above for why.

    Error strategy: a syntactically invalid id is a 400 (a client input
    error; reveals nothing about any real candidate). EVERY other failure —
    not found, no photo, disallowed host, upstream error, bad content-type,
    oversized, too many redirects — is an IDENTICAL 204 with no body, so the
    response can never be used to distinguish *why* a given id produced no
    image, or to enumerate which ids correspond to real candidates.
    """
    try:
        uuid.UUID(candidate_id)
    except ValueError:
        return Response(status_code=400)

    async with httpx.AsyncClient() as client:
        doc = await get_document(client, candidate_id)
        if not doc:
            return Response(status_code=204)

        photo_url = doc.get("profile_picture_url")
        if not _is_real_photo(photo_url):
            return Response(status_code=204)

        fetched = await _fetch_avatar_bytes(client, photo_url)

    if fetched is None:
        return Response(status_code=204)

    body, content_type = fetched
    return Response(
        content=body,
        media_type=content_type,
        headers={
            "Cache-Control": "public, max-age=604800, immutable",
            "X-Content-Type-Options": "nosniff",
            # A fresh Response is built from scratch — no upstream header
            # (including any Set-Cookie, e.g. Naukri's _t_ds tracking cookie)
            # is ever copied onto it.
        },
    )


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
