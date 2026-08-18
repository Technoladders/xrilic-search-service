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

from .skill_logic import must_partial_match_count, nice_match_count

TsSearchFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def rank_key(hit: dict[str, Any], nice: list[str]) -> tuple:
    """Bucket A's rank key (also used for the single-bucket nice-ranking
    path when MUST isn't set) — nice-match-count desc, then relevance/
    activity/freshness. MUST is either absent or already fully satisfied
    by construction of Bucket A's filter, so it isn't a ranking dimension
    here."""
    doc = hit.get("document", {})
    match_count = nice_match_count(doc.get("skills"), nice)
    text_match = hit.get("text_match") or 0
    last_active = doc.get("last_active_date_ts") or 0
    freshness = doc.get("data_freshness_ts") or 0
    return (-match_count, -text_match, -last_active, -freshness)


def rank_key_bucket_b(hit: dict[str, Any], must: list[str], nice: list[str]) -> tuple:
    """
    Bucket B's rank key: candidates here never satisfy every MUST skill (by
    construction of Bucket B's filter), so "how many MUST skills they still
    happen to have" is a real, non-degenerate ranking dimension and takes
    priority over nice-match-count, per the confirmed ranking hierarchy:
    must-partial-match-count desc -> nice-match-count desc -> relevance ->
    activity -> freshness.
    """
    doc = hit.get("document", {})
    must_count = must_partial_match_count(doc.get("skills"), must)
    nice_count = nice_match_count(doc.get("skills"), nice)
    text_match = hit.get("text_match") or 0
    last_active = doc.get("last_active_date_ts") or 0
    freshness = doc.get("data_freshness_ts") or 0
    return (-must_count, -nice_count, -text_match, -last_active, -freshness)


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


async def fetch_direct_slice(
    ts_search: TsSearchFn,
    base_params: dict[str, Any],
    sort_by: str,
    offset: int,
    limit: int,
) -> tuple[list[dict], Optional[float]]:
    """
    Fetch exactly `limit` hits starting at `offset`, in native Typesense
    order — used when a requested slice falls beyond a bucket's exact-rank
    pool depth. Costs AT MOST 2 Typesense calls regardless of how deep
    `offset` is: Typesense's own `page`/`per_page` pagination is a single
    O(1)-ish server-side jump, not a client-side walk from the start, but
    `offset = (page-1)*per_page` is a product of two integers we choose —
    so hitting an arbitrary `offset` in one call requires `limit` to evenly
    divide it, which isn't guaranteed. Instead this windows around the
    target: fetch the `per_page=limit`-sized page containing `offset`
    (`page = offset // limit + 1`), and if that window doesn't fully cover
    through `offset + limit`, fetch the very next page too, then slice the
    exact range out of the concatenated result — never walks from page 1.
    """
    if limit <= 0:
        return [], None
    window_start = (offset // limit) * limit
    params = {**base_params, "page": offset // limit + 1, "per_page": limit, "sort_by": sort_by}
    data = await ts_search(params)
    hits = list(data.get("hits") or [])
    took_ms = data.get("search_time_ms")
    local_offset = offset - window_start
    if len(hits) == limit and local_offset + limit > len(hits):
        next_data = await ts_search({**params, "page": params["page"] + 1})
        hits.extend(next_data.get("hits") or [])
    return hits[local_offset: local_offset + limit], took_ms
