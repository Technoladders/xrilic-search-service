"""
sync_service/master_candidates/seed_dev_collection.py

Step 3 of the validation sequence: creates and seeds master_candidates_v1_dev
with a REAL, meaningful sample (not an empty schema) — pulls a batch of real
rows straight from Postgres `master_candidates` (read-only SELECT, same
SELECT_COLUMNS the production indexer uses) and runs them through the
already-updated transform_row() (so the dev collection actually has values
in industry/job_function/functional_area/company_industry/languages/gender/
age_years/etc., not just an empty schema with the right field names).

Also creates master_candidate_suggestions_v1_dev (empty — populated by
run_dev_aggregation.py, step 4).

SAFETY: this script WRITES documents (unlike inspect_production_collection.py
and verify_*.py, which are read-only). It refuses to run unless
MC_COLLECTION_NAME is explicitly set AND ends with "_dev" — this is a hard
guard against accidentally seeding production if an env var is forgotten,
since config.py's own default for that variable IS the production name.

Usage:
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_COLLECTION_NAME=master_candidates_v1_dev \
    MC_SUGGESTIONS_COLLECTION_NAME=master_candidate_suggestions_v1_dev \
    SEED_SAMPLE_SIZE=5000 \
        python3 seed_dev_collection.py
"""
import os
import sys

# ── Safety guard — validated BEFORE importing anything from the package,
# since config.py reads these env vars once at import time. ─────────────────
_required = ["SUPABASE_URL", "SUPABASE_SERVICE_KEY", "TYPESENSE_HOST", "TYPESENSE_API_KEY"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"Set these env vars first: {_missing}. Aborting.")
    sys.exit(1)

_collection_name = os.environ.get("MC_COLLECTION_NAME", "")
if not _collection_name.endswith("_dev"):
    print(
        f"REFUSING TO RUN: MC_COLLECTION_NAME is {_collection_name!r} — this script "
        f"only ever writes to a collection whose name ends in '_dev'. "
        f"Set MC_COLLECTION_NAME=master_candidates_v1_dev explicitly and retry. "
        f"(This guard exists because config.py's own default for this variable "
        f"IS the production collection name — an unset env var here would "
        f"otherwise silently seed production.)"
    )
    sys.exit(1)
os.environ.setdefault("MC_SUGGESTIONS_COLLECTION_NAME", "master_candidate_suggestions_v1_dev")
if not os.environ["MC_SUGGESTIONS_COLLECTION_NAME"].endswith("_dev"):
    print("REFUSING TO RUN: MC_SUGGESTIONS_COLLECTION_NAME must also end in '_dev'.")
    sys.exit(1)

import asyncio  # noqa: E402
import sys as _sys  # noqa: E402

# Make `master_candidates` importable regardless of the invoking cwd (this
# file lives at sync_service/master_candidates/, so sync_service/ is what
# needs to be on sys.path — matches the same bootstrap conftest.py uses for
# the test suite).
_sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from master_candidates.config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE, TS_COLLECTION, TS_SUGGESTIONS_COLLECTION  # noqa: E402
from master_candidates.indexer import SELECT_COLUMNS, transform_row  # noqa: E402
from master_candidates.typesense_client import ensure_collection, ensure_suggestions_collection, upsert_batch  # noqa: E402

SAMPLE_SIZE = int(os.environ.get("SEED_SAMPLE_SIZE", "5000"))

# Report on these fields to confirm the sample is genuinely "meaningful" —
# i.e. actually populated, not just structurally present with nulls.
NEW_FIELDS_TO_REPORT = [
    "industry", "job_function", "functional_area", "company_industry",
    "gender", "age_years", "marital_status", "disability",
    "desired_job_type", "employment_status_pref", "work_auth_countries",
    "languages", "languages_filter",
]


async def fetch_sample(client: httpx.AsyncClient, limit: int) -> list[dict]:
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates",
        headers=SB_HEADERS,
        params={"select": SELECT_COLUMNS, "order": "updated_at.desc", "limit": str(limit)},
        timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()
    return r.json()


async def main() -> None:
    print(f"Target candidate collection: {TS_COLLECTION}")
    print(f"Target suggestions collection: {TS_SUGGESTIONS_COLLECTION}")
    print(f"Sample size: {SAMPLE_SIZE}\n")

    async with httpx.AsyncClient() as client:
        print("1. Ensuring dev candidate collection exists (create-if-missing, never touches prod)...")
        await ensure_collection(client)

        print("2. Ensuring dev suggestions collection exists...")
        await ensure_suggestions_collection(client)

        print(f"3. Fetching {SAMPLE_SIZE} real rows from Postgres master_candidates (read-only SELECT)...")
        rows = await fetch_sample(client, SAMPLE_SIZE)
        print(f"   Fetched {len(rows)} rows.")

        print("4. Transforming via transform_row() (includes all 12 new-field mappings)...")
        docs = [transform_row(r) for r in rows]

        print("5. Upserting into the dev candidate collection...")
        batch_size = 500
        total_ok, total_errors = 0, []
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            ok, errors = await upsert_batch(client, batch)
            total_ok += ok
            total_errors.extend(errors)
        print(f"   Upserted {total_ok}/{len(docs)} documents. {len(total_errors)} errors.")
        if total_errors:
            print(f"   First few errors: {total_errors[:3]}")

    print("\n=== Field coverage in this sample (proves it's meaningful, not just an empty schema) ===")
    for field in NEW_FIELDS_TO_REPORT:
        non_empty = sum(1 for d in docs if d.get(field))
        pct = (non_empty / len(docs) * 100) if docs else 0
        print(f"  {field}: {non_empty}/{len(docs)} rows ({pct:.1f}%) have a value")

    print("\nDone. Production master_candidates_v1 was never touched by this script "
          "(no calls reference it — only the '_dev'-suffixed collection names above).")


if __name__ == "__main__":
    asyncio.run(main())
