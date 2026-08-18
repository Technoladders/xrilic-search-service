"""
sync_service/master_candidates/run_dev_aggregation.py

Step 4 of the validation sequence: populates master_candidate_suggestions_v1_dev
from master_candidates_v1_dev (run seed_dev_collection.py FIRST, or this will
just report 0 documents processed). Prints per-dimension distinct-value
counts for all 15 live suggestion dimensions so you can confirm real values
came through, not just an empty run.

SAFETY: same hard guard as seed_dev_collection.py — refuses to run unless
both MC_COLLECTION_NAME and MC_SUGGESTIONS_COLLECTION_NAME end in "_dev".
This only reads from the candidate collection (via Typesense's bulk export)
and writes only to the suggestions collection — it never touches
master_candidates_v1 itself, dev or prod, except to read from whichever
name MC_COLLECTION_NAME points at (guarded to "_dev" only).

Usage:
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_COLLECTION_NAME=master_candidates_v1_dev \
    MC_SUGGESTIONS_COLLECTION_NAME=master_candidate_suggestions_v1_dev \
        python3 run_dev_aggregation.py
"""
import os
import sys

_required = ["TYPESENSE_HOST", "TYPESENSE_API_KEY"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"Set these env vars first: {_missing}. Aborting.")
    sys.exit(1)

for _var in ("MC_COLLECTION_NAME", "MC_SUGGESTIONS_COLLECTION_NAME"):
    _val = os.environ.get(_var, "")
    if not _val.endswith("_dev"):
        print(f"REFUSING TO RUN: {_var} is {_val!r} — must end in '_dev'. Aborting.")
        sys.exit(1)

# Supabase creds aren't needed by this script (it reads from Typesense's own
# export, not Postgres) but config.py requires them to be set to import
# cleanly — dummy placeholders are fine here since nothing Supabase-related
# is ever called.
os.environ.setdefault("SUPABASE_URL", "http://unused.invalid")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "unused")
os.environ.setdefault("WEBHOOK_SECRET", "unused")
os.environ.setdefault("ADMIN_SECRET", "unused")

import asyncio  # noqa: E402
import sys as _sys  # noqa: E402

_sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from master_candidates.config import TS_COLLECTION, TS_SUGGESTIONS_COLLECTION  # noqa: E402
from master_candidates.typesense_client import ensure_suggestions_collection  # noqa: E402
from master_candidates.suggestions_aggregator import rebuild_suggestions  # noqa: E402


async def main() -> None:
    print(f"Reading from: {TS_COLLECTION}")
    print(f"Writing to:   {TS_SUGGESTIONS_COLLECTION}\n")

    async with httpx.AsyncClient(timeout=None) as client:
        await ensure_suggestions_collection(client)
        report = await rebuild_suggestions(client)

    print(f"Documents processed: {report.documents_processed}")
    print(f"Suggestion rows upserted: {len(report.rows)}\n")
    print("Per-dimension distinct value counts (true cardinality, before the top-2000 cap):")
    for dim, count in sorted(report.per_dimension_distinct_counts.items()):
        print(f"  {dim}: {count}")

    if report.documents_processed == 0:
        print("\n!! 0 documents processed — did you run seed_dev_collection.py first?")
        sys.exit(1)

    empty_dims = [d for d, c in report.per_dimension_distinct_counts.items() if c == 0]
    if empty_dims:
        print(f"\n!! These dimensions had ZERO distinct values in the sample: {empty_dims}")
        print("   (Expected if the seeded sample happens to lack that field — not necessarily a bug;"
              " cross-check against seed_dev_collection.py's field-coverage report.)")


if __name__ == "__main__":
    asyncio.run(main())
