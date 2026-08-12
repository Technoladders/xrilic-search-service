"""
sync_service/master_candidates/search/ranking.py

Two things, both unchanged in *logic* from the pre-existing search_api.py,
just extracted for testability:

1. rank_key() — nice-skill rerank ordering: exact nice-match-count desc,
   then Typesense text_match desc, then last_active_date_ts desc, then
   data_freshness_ts desc.
2. fetch_bounded_pool() — the paginated "keep fetching until `target` hits
   or the pool is exhausted, capped at a hard maximum" loop, previously
   named `_fetch_rerank_pool` in search_api.py. It's reused as-is by
   keyword_query.py's Tier-B (bounded OR) evaluator, since both are the
   same "cheap-but-honest bounded pool" pattern — deliberately not
   reinvented for keyword search.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

from .skill_logic import nice_match_count

TsSearchFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def rank_key(hit: dict[str, Any], nice: list[str]) -> tuple:
    doc = hit.get("document", {})
    match_count = nice_match_count(doc.get("skills"), nice)
    text_match = hit.get("text_match") or 0
    last_active = doc.get("last_active_date_ts") or 0
    freshness = doc.get("data_freshness_ts") or 0
    return (-match_count, -text_match, -last_active, -freshness)


async def fetch_bounded_pool(
    ts_search: TsSearchFn,
    base_params: dict[str, Any],
    needed: int,
    hard_cap: int,
    page_size: int,
) -> tuple[list[dict], int, list, Optional[float]]:
    """
    Fetches Typesense pages (fixed per_page=page_size — must stay constant
    across calls, since Typesense's offset math is (page-1)*per_page using
    the CURRENT call's per_page) until `needed` hits are collected, the
    pool exhausts, or `hard_cap` is reached. Returns (hits, total_found,
    facet_counts, took_ms). `total_found` is Typesense's own exact `found`
    for base_params's q/filter_by — always exact, never capped; only the
    ranking DEPTH (how many hits get exact nice-match-count/relevance
    ordering before falling back to native sort_by) is bounded by hard_cap.
    """
    hits: list[dict] = []
    found = 0
    facets: list = []
    took_ms: Optional[float] = None
    target = min(max(needed, 1), hard_cap)

    pool_page = 1
    while len(hits) < target:
        params = {**base_params, "page": pool_page, "per_page": page_size}
        data = await ts_search(params)
        page_hits = data.get("hits") or []
        if pool_page == 1:
            found = data.get("found", 0)
            facets = data.get("facet_counts", [])
            took_ms = data.get("search_time_ms")
        hits.extend(page_hits)
        if len(page_hits) < page_size:
            break  # exhausted every actual match — nothing more to fetch
        pool_page += 1

    return hits, found, facets, took_ms
