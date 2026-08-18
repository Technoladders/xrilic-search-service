"""
sync_service/master_candidates/search/bucket_pagination.py

Pure page-slicing math for the two-bucket MUST+NICE tiering (Bucket A =
MUST-complete, Bucket B = NICE-qualified but not MUST-complete). No network,
no side effects — fully unit-testable, deliberately mirroring the shape of
this codebase's own frontend `computePageSlice()`
(src/components/rocketreach/unified/waterfallMath.ts), which already proves
this exact "split a page across two ordered, disjoint sources by cumulative
count" pattern in production for the internal/external waterfall.

CRITICAL INVARIANT (see the implementation plan's "Critical invariant"
callout): which bucket a page's data comes from is decided HERE, using only
the exact `count_a` — never by whether a bucket's own ranking pool has been
exhausted. A bucket's RERANK_POOL_HARD_CAP only ever affects how deep into
that bucket's own results exact ranking goes before falling back to native
Typesense order; it must never change which bucket a given offset routes to.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BucketSlice:
    a_offset: int
    a_limit: int
    b_offset: int
    b_limit: int
    needs_a: bool
    needs_b: bool
    is_boundary: bool


def compute_bucket_slice(page: int, per_page: int, count_a: int) -> BucketSlice:
    """
    Given a 1-indexed UI `page`/`per_page` and Bucket A's EXACT total count,
    determine how the requested [start, end) window splits across Bucket A
    (first) and Bucket B (only once Bucket A is exhausted).

    Example: count_a=5, per_page=13
      page 1 -> a_offset=0,  a_limit=5,  b_offset=0, b_limit=8   (boundary)
      page 2 -> a_offset=13, a_limit=0,  b_offset=8, b_limit=13
    """
    start = (page - 1) * per_page
    end = page * per_page

    a_remaining = max(0, count_a - start)
    a_limit = min(per_page, a_remaining)

    b_offset = max(0, start - count_a)
    b_limit = per_page - a_limit

    return BucketSlice(
        a_offset=start,
        a_limit=a_limit,
        b_offset=b_offset,
        b_limit=b_limit,
        needs_a=a_limit > 0,
        needs_b=b_limit > 0,
        is_boundary=a_limit > 0 and b_limit > 0,
    )
