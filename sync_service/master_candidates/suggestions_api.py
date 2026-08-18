"""
sync_service/master_candidates/suggestions_api.py

GET /mc/search_suggestions — lightweight autocomplete, backed ONLY by the
dedicated master_candidate_suggestions_v1 collection (never
master_candidates_v1 — see suggestions_aggregator.py for how that
collection gets populated). This is what makes "don't run /mc/search_v2 on
every keystroke" true by construction: this endpoint has no code path that
can reach the 1M+ candidate collection.

Auth: same Depends(require_user) as /mc/search_v2 — this is candidate-
intelligence data (counts by value), not something to expose unauthenticated.
"""
import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from .config import TYPESENSE_BASE, TS_HEADERS, TS_SUGGESTIONS_COLLECTION, HTTP_TIMEOUT_TYPESENSE
from .search_api import require_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc", tags=["master_candidates"])

# The fixed enum of live suggestion dimensions (implementation plan, Part 2).
# The 7 future-analytics-only fields are deliberately not queryable here.
VALID_TYPES = frozenset({
    "skill", "current_title", "previous_title", "current_employer", "previous_employer",
    "school", "degree", "field_of_study", "language", "location",
    "industry", "job_function", "functional_area", "company_industry", "seniority",
})


def _escape(v: str) -> str:
    return "`" + v.replace("`", "\\`") + "`"


def _tiered_num_typos(query_len: int) -> int:
    """1-2 chars -> prefix only; 3-4 -> limited typo; 5+ -> full typo.
    Keeps suggestions useful instead of noisy for very short queries."""
    if query_len <= 2:
        return 0
    if query_len <= 4:
        return 1
    return 2


async def _ts_suggestions_search(ts_params: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{TYPESENSE_BASE}/collections/{TS_SUGGESTIONS_COLLECTION}/documents/search",
            headers=TS_HEADERS, params=ts_params, timeout=HTTP_TIMEOUT_TYPESENSE,
        )
    if r.status_code >= 400:
        logger.warning(f"typesense suggestions search failed: {r.status_code} {r.text[:400]}")
        raise HTTPException(status_code=502, detail="suggestions backend error")
    return r.json()


@router.get("/search_suggestions")
async def search_suggestions(
    type: str = Query(..., description="Dimension name, or comma-separated list of dimensions"),
    q: str = Query("", description="User's in-progress input"),
    limit: int = Query(8, ge=1, le=50),
    # Accepted now, ignored for now — reserved for a future personalization/
    # analytics layer so that layer doesn't need a contract change later
    # (implementation plan, Part 2 §7). Never touches business logic here.
    context: Optional[str] = Query(None),
    selected: Optional[str] = Query(None),
    user_id: str = Depends(require_user),
) -> dict[str, Any]:
    types = [t.strip() for t in type.split(",") if t.strip()]
    if not types:
        raise HTTPException(status_code=400, detail="type is required")
    unknown = [t for t in types if t not in VALID_TYPES]
    if unknown:
        raise HTTPException(status_code=400, detail=f"unknown suggestion type(s): {unknown}")

    q_clean = (q or "").strip()
    ts_params: dict[str, Any] = {
        "q":          q_clean or "*",
        "query_by":   "value",
        "filter_by":  "type:=[" + ",".join(_escape(t) for t in types) + "]",
        "sort_by":    "candidate_count:desc",
        "per_page":   limit,
        "page":       1,
        "prefix":     "true",
    }
    if q_clean:
        ts_params["num_typos"] = _tiered_num_typos(len(q_clean))

    data = await _ts_suggestions_search(ts_params)
    suggestions = [
        {
            "value": h["document"]["value"],
            "label": h["document"]["value"],
            "count": h["document"]["candidate_count"],
            "type":  h["document"]["type"],
        }
        for h in (data.get("hits") or [])
    ]
    return {"suggestions": suggestions}
