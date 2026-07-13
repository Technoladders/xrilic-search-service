"""
sync_service/master_candidates/search_api.py

FastAPI router mounted at /mc.
POST /mc/search   — takes the InternalFilters payload from useInternalSearch.ts
                    returns { profiles, total, page, per_page, facets, took_ms,
                    count_capped }
GET  /mc/health   — collection stats

v2 CHANGES (this file only — no schema change, no reindex; last_active_date_ts
            and data_freshness_ts already exist and are populated):

  RANKING (this turn's fix):
    _build_filter_by() was ALREADY correct for nice-skill inclusion — nice
    skills sit in filter_by as an OR (`skills:=[a,b,c]`, via _filter_any),
    same as before. What was missing: nothing counted HOW MANY nice skills
    each profile matched, so a profile matching all 3 sorted identically to
    one matching just 1. Typesense's sort_by can't do array-intersection-
    count natively, so:
      • when nice skills are present, we fetch a bounded pool (see
        RERANK_POOL_HARD_CAP) and compute the exact match count in Python,
        then sort by (match_count desc, keyword relevance desc,
        last_active_date_ts desc, data_freshness_ts desc).
      • when there are no nice skills, Typesense's native sort_by handles
        everything in one call — just with the field order corrected:
        last_active_date_ts now leads (most recently active first),
        data_freshness_ts (sync recency) is the tiebreaker. Previously
        data_freshness_ts was primary, which is the wrong field for "most
        recently active first."

  CARRIED FORWARD from earlier in this conversation (separate from the
  ranking ask, but same file, so included here — flagged distinctly):
    • ids lookup short-circuit: {"ids": [...]} in the payload returns those
      profiles directly. Powers fetchMasterProfilesByIds() (job-sourcing
      card hydration) — the real file had no support for this at all, so
      that payload fell through to q="*" and returned arbitrary profiles.
    • SECURITY: exclude_fields=emails_json,phones_json on every Typesense
      call. _to_rr_profile() is UNCHANGED — but since Typesense now never
      returns those two fields, _loads(d.get("emails_json")) naturally
      resolves to [], so _allEmails/_allPhones/_enriched come out safe
      automatically. contact_availability/teaser booleans still work
      because they also read the separate contact_personal_email /
      contact_phone boolean fields, which are NOT excluded.

  IMPORTANT — frontend: any client-side .sort() added after receiving
  /mc/search results must be removed. It would re-scramble this ordering,
  and it only ever sees one page at a time so it can't reproduce the
  pool-based nice-skill ranking correctly anyway.
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

# Contacts never leave /mc/search — reveal (sourcexr-reveal) is the only
# contact source. contact_availability booleans + teaser still work since
# those read separate (non-excluded) boolean fields.
EXCLUDE_FIELDS = "emails_json,phones_json"

# Typesense's practical max per single request. We page through multiples of
# this to build the nice-skill rerank pool without ever changing per_page
# mid-loop (changing per_page between calls breaks Typesense's page-offset
# math, since offset = (page-1)*per_page using the CURRENT call's per_page).
TYPESENSE_MAX_PER_PAGE = 250

# Safety ceiling for how deep nice-skill match-count reranking goes. Typical
# loads (page 1-20 at the existing 50/page cap = up to 1000) cost exactly
# ONE Typesense call when total matches are under ~250, growing by one more
# call per extra 250. Beyond this cap, results are still returned but
# "count_capped": true is set on the response so the frontend can surface
# a "narrow your search for more" hint if it wants to.
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
    """(must, nice, exclude) labels from filters.skillChips.
    Pulled out into its own function so search() can reuse the same `nice`
    list that _build_filter_by() uses — pure refactor, identical logic to
    what was previously inlined in _build_filter_by, no behavior change."""
    chips = f.get("skillChips") or []
    must    = [c["label"] for c in chips if c.get("mode") == "must" and c.get("label")]
    nice    = [c["label"] for c in chips if c.get("mode") == "nice" and c.get("label")]
    exclude = [c["label"] for c in chips if c.get("mode") == "exclude" and c.get("label")]
    return must, nice, exclude


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
    # emails_json/phones_json are excluded from Typesense retrieval
    # (EXCLUDE_FIELDS) — d.get(...) resolves to None here, so _loads(None)
    # returns [], and everything downstream (emails/phones/_allEmails/
    # _allPhones/_enriched) comes out empty/False automatically. No other
    # change needed in this function for the contact strip to hold.
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
    """Single GET to Typesense's search endpoint. Same behavior as the
    inline call that used to live directly in search() — pulled into a
    function so both the simple path and the pool-fetch loop can share it."""
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
    (page-1)*per_page using the CURRENT call's per_page; changing it
    mid-loop would skip or repeat rows) until `needed` hits are collected,
    the pool exhausts, or RERANK_POOL_HARD_CAP is reached.

    Returns (hits, total_found, facet_counts, took_ms). facets/took_ms are
    captured from the first page only — recomputing them on every
    pool-extension call would add cost for no real benefit.
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
    # fetchMasterProfilesByIds() POSTs {"ids": [...]}. Returns those exact
    # profiles, contact-stripped the same as a normal search.
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
    _, nice, _ = _extract_skill_chips(filters)   # same list _build_filter_by used for the OR filter

    filter_by = _build_filter_by(filters)

    base_params: dict[str, Any] = {
        "q":                q,
        "query_by":         QUERY_BY,
        "query_by_weights": QUERY_BY_WEIGHTS,
        "num_typos":        "2,1,0,0,0,0,0,0,0,0,0,0",  # only allow typos on name & title
        "prioritize_exact_match": "true",
        "facet_by":         "has_full_profile,has_contact,sources,seniority,country,primary_source",
        "max_facet_values": "10",
        "highlight_fields": "full_name,title,skills_text",
        "exclude_fields":   EXCLUDE_FIELDS,
        # v2 SORT: most recently active first, sync freshness as tiebreak.
        # (Previously data_freshness_ts was primary — wrong field for "most
        # recently active first.") This sort_by also determines which
        # profiles enter the rerank pool below, when nice skills are used.
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

    # ── Nice-skill path: filter_by already restricts to "at least one nice
    # skill" (OR, via _build_filter_by/_filter_any) — that part was already
    # correct. What Typesense can't do is sort by HOW MANY of those matched.
    # Fetch a bounded pool, then rank in Python:
    #   1. most nice skills matched (desc)               ← the actual fix
    #   2. keyword relevance, if a keyword was given too (desc)
    #   3. most recently active (desc)
    #   4. sync freshness (desc)
    needed = page * per_page
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

    pool_capped = found > RERANK_POOL_HARD_CAP
    return {
        "profiles":     [_to_rr_profile(h) for h in page_hits],
        "total":        (RERANK_POOL_HARD_CAP if pool_capped else found),
        "page":         page,
        "per_page":     per_page,
        "facets":       facets,
        "took_ms":      took_ms,
        "count_capped": pool_capped,
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