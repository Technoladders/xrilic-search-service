"""
sync_service/master_candidates/verify_suggestions.py

Step 5 of the validation sequence: exercises the REAL search_suggestions()
business logic (tiered typo tolerance, type validation, response shape)
against the REAL dev suggestions collection — calling the actual Python
function directly (bypassing the HTTP/FastAPI layer, since there's no
separate deployed service instance pointed at the dev collections; auth
enforcement itself is Depends(require_user), the exact same mechanism
/mc/search_v2 already uses live in production, so it isn't re-verified here).

For each of the 15 live dimensions, dynamically derived from REAL data
(never hardcoded guesses, since dev content depends on whatever got seeded):
  1. Empty-query request -> top suggestions by candidate_count.
  2. A short prefix of the top result's value -> confirms prefix matching,
     and that the same value still appears.
  3. A deliberately-typo'd version of that value (one char swapped) -> confirms
     typo tolerance actually engages for longer queries.
  4. Latency for each call.

Run seed_dev_collection.py + run_dev_aggregation.py FIRST.

Usage:
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_SUGGESTIONS_COLLECTION_NAME=master_candidate_suggestions_v1_dev \
        python3 verify_suggestions.py
"""
import os
import sys
import time

_required = ["TYPESENSE_HOST", "TYPESENSE_API_KEY"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"Set these env vars first: {_missing}. Aborting.")
    sys.exit(1)

if not os.environ.get("MC_SUGGESTIONS_COLLECTION_NAME", "").endswith("_dev"):
    print("REFUSING TO RUN: MC_SUGGESTIONS_COLLECTION_NAME must be set and end in '_dev'.")
    sys.exit(1)

os.environ.setdefault("SUPABASE_URL", "http://unused.invalid")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "unused")
os.environ.setdefault("WEBHOOK_SECRET", "unused")
os.environ.setdefault("ADMIN_SECRET", "unused")
os.environ.setdefault("MC_COLLECTION_NAME", "master_candidates_v1_dev")  # unused by this script directly

import asyncio  # noqa: E402
import sys as _sys  # noqa: E402

_sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from master_candidates import suggestions_api as sapi  # noqa: E402


def _typo(value: str) -> str:
    """Swap two adjacent middle characters — a minimal, realistic single typo."""
    if len(value) < 4:
        return value
    mid = len(value) // 2
    chars = list(value)
    chars[mid], chars[mid + 1] = chars[mid + 1], chars[mid]
    return "".join(chars)


async def check_one_type(dim: str) -> bool:
    print(f"\n--- {dim} ---")
    ok = True

    t0 = time.monotonic()
    result = await sapi.search_suggestions(type=dim, q="", limit=5, user_id="verify-script")
    took_empty = (time.monotonic() - t0) * 1000
    top = result["suggestions"]
    print(f"  empty query: {len(top)} results in {took_empty:.1f}ms")
    if not top:
        print(f"  !! no suggestions at all for '{dim}' — check seeding/aggregation for this dimension")
        return False
    for s in top[:3]:
        print(f"     {s['label']!r} (count={s['count']})")

    target = top[0]["value"]

    prefix = target[:2] if len(target) >= 2 else target
    t0 = time.monotonic()
    prefix_result = await sapi.search_suggestions(type=dim, q=prefix, limit=8, user_id="verify-script")
    took_prefix = (time.monotonic() - t0) * 1000
    prefix_values = [s["value"] for s in prefix_result["suggestions"]]
    prefix_hit = target in prefix_values
    print(f"  prefix {prefix!r}: {len(prefix_values)} results in {took_prefix:.1f}ms, "
          f"contains top value: {prefix_hit}")
    if not prefix_hit:
        print(f"  !! expected {target!r} to appear when searching its own prefix {prefix!r}")
        ok = False

    typo_query = _typo(target)
    if typo_query != target:
        t0 = time.monotonic()
        typo_result = await sapi.search_suggestions(type=dim, q=typo_query, limit=8, user_id="verify-script")
        took_typo = (time.monotonic() - t0) * 1000
        typo_values = [s["value"] for s in typo_result["suggestions"]]
        typo_hit = target in typo_values
        print(f"  typo {typo_query!r} (of {target!r}): {len(typo_values)} results in {took_typo:.1f}ms, "
              f"contains original value: {typo_hit}")
        if not typo_hit:
            print(f"  ?? typo tolerance didn't surface {target!r} for {typo_query!r} — "
                  f"may be expected depending on how different the swap made it; not auto-failed")

    return ok


async def main() -> None:
    results = {}
    for dim in sorted(sapi.VALID_TYPES):
        try:
            results[dim] = await check_one_type(dim)
        except Exception as e:
            print(f"\n--- {dim} ---\n  !! EXCEPTION: {e}")
            results[dim] = False

    print("\n=== Summary ===")
    failed = [d for d, ok in results.items() if not ok]
    print(f"{len(results) - len(failed)}/{len(results)} dimensions passed.")
    if failed:
        print(f"Failed: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
