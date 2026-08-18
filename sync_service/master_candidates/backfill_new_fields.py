"""
sync_service/master_candidates/backfill_new_fields.py

Controlled, manual backfill command — populates the 12 additive fields
(industry, job_function, functional_area, company_industry, languages_filter,
gender, age_years, marital_status, disability, desired_job_type,
employment_status_pref, work_auth_countries) onto EXISTING documents already
in master_candidates_v1. Schema additions alone don't backfill historical
data — this script is that separate, deliberate step, per explicit
instruction ("do not automatically trigger a full historical backfill
during container startup").

NEVER imported or called by main.py / any request handler / any background
loop — it only runs when a human invokes it directly.

Design: walks `master_candidates` via KEYSET pagination on `id` (a UUID
primary key — `order=id.asc&id=gt.<cursor>` gives a stable, deterministic
walk; this is intentionally NOT the same cursor the incremental poller uses,
since `updated_at` only advances for rows that change, and a full backfill
needs to touch every row regardless of when it last changed). For each
batch: fetch from Postgres (read-only SELECT, same SELECT_COLUMNS the
production indexer already uses) -> transform_row() -> upsert_batch()
(the SAME upsert path the indexer/poller already use — this is an upsert,
so untouched fields on each document are left as Typesense already has
them; only the fields transform_row() produces are written).

SAFETY:
  - Dry-run by default. Nothing is written to Typesense unless --execute
    is passed. A dry run reports exactly what WOULD happen (row count,
    field coverage) using the same live Postgres read the real run uses.
  - Bounded per invocation (--max-batches, default 200 batches = ~100k
    rows at the default batch size) so one invocation can't run
    unsupervised for the full ~2.3M-document walk without a checkpoint.
    Prints the exact --resume-from-id to pass next time.
  - --resume-from-id continues a previous run's keyset cursor exactly.
  - Every batch is a plain upsert against the EXISTING collection — no
    delete, no recreate, no schema change performed by this script (the
    schema/PATCH step is a separate, already-reviewed operation).

Usage (dry run — always start here):
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_COLLECTION_NAME=master_candidates_v1 \
        python3 backfill_new_fields.py

Usage (actually write, first chunk):
    ... same env vars ... \
        python3 backfill_new_fields.py --execute --max-batches 200

Usage (resume from a printed cursor):
    ... same env vars ... \
        python3 backfill_new_fields.py --execute --max-batches 200 \
            --resume-from-id 3f9a1c2e-...

Usage (run to full completion in one supervised invocation):
        python3 backfill_new_fields.py --execute --max-batches 0
"""
import argparse
import os
import sys
import time

_required = ["SUPABASE_URL", "SUPABASE_SERVICE_KEY", "TYPESENSE_HOST", "TYPESENSE_API_KEY"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"Set these env vars first: {_missing}. Aborting.")
    sys.exit(1)
# Not used by this script but required by config.py's import-time validation.
os.environ.setdefault("WEBHOOK_SECRET", "unused")
os.environ.setdefault("ADMIN_SECRET", "unused")

import asyncio  # noqa: E402
import sys as _sys  # noqa: E402

_sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from master_candidates.config import (  # noqa: E402
    SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE, TS_COLLECTION,
)
from master_candidates.indexer import SELECT_COLUMNS, transform_row  # noqa: E402
from master_candidates.typesense_client import upsert_batch  # noqa: E402

NEW_FIELDS = [
    "industry", "job_function", "functional_area", "company_industry", "languages_filter",
    "gender", "age_years", "marital_status", "disability",
    "desired_job_type", "employment_status_pref", "work_auth_countries",
]


async def fetch_batch(client: httpx.AsyncClient, after_id: str | None, limit: int) -> list[dict]:
    params = {"select": SELECT_COLUMNS, "order": "id.asc", "limit": str(limit)}
    if after_id:
        params["id"] = f"gt.{after_id}"
    r = await client.get(f"{SUPABASE_REST}/master_candidates", headers=SB_HEADERS,
                         params=params, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()


async def run(execute: bool, batch_size: int, max_batches: int, resume_from_id: str | None) -> None:
    print(f"Target collection: {TS_COLLECTION}")
    print(f"Mode: {'EXECUTE (writing)' if execute else 'DRY RUN (no writes)'}")
    print(f"Batch size: {batch_size}, max batches this invocation: {max_batches or 'unbounded'}")
    if resume_from_id:
        print(f"Resuming after id: {resume_from_id}")
    print()

    cursor = resume_from_id
    batch_num = 0
    total_rows = 0
    total_ok = 0
    total_errors: list[str] = []
    field_coverage = {f: 0 for f in NEW_FIELDS}
    start_time = time.monotonic()

    async with httpx.AsyncClient() as client:
        while True:
            if max_batches and batch_num >= max_batches:
                print(f"\nReached --max-batches={max_batches} for this invocation. Stopping (not an error).")
                break

            rows = await fetch_batch(client, cursor, batch_size)
            if not rows:
                print("\nNo more rows — backfill is COMPLETE.")
                break

            batch_num += 1
            docs = [transform_row(r) for r in rows]
            for doc in docs:
                for f in NEW_FIELDS:
                    if doc.get(f):
                        field_coverage[f] += 1

            if execute:
                ok, errors = await upsert_batch(client, docs)
                total_ok += ok
                total_errors.extend(errors)
            else:
                ok, errors = len(docs), []

            total_rows += len(rows)
            cursor = rows[-1]["id"]
            elapsed = time.monotonic() - start_time
            print(f"  batch {batch_num}: {len(rows)} rows, {ok} upserted, "
                  f"{len(errors)} errors, cursor now {cursor}, elapsed {elapsed:.0f}s")

            if len(rows) < batch_size:
                print("\nLast page was short — backfill is COMPLETE.")
                break

    print(f"\n=== Summary ===")
    print(f"Batches this invocation: {batch_num}")
    print(f"Rows read: {total_rows}")
    if execute:
        print(f"Rows upserted: {total_ok}")
        print(f"Errors: {len(total_errors)}")
        if total_errors:
            print(f"First few errors: {total_errors[:3]}")
    else:
        print("(dry run — nothing was written)")

    print("\nField coverage in rows processed this invocation:")
    for f, count in field_coverage.items():
        pct = (count / total_rows * 100) if total_rows else 0
        print(f"  {f}: {count}/{total_rows} ({pct:.1f}%)")

    if cursor and (not max_batches or batch_num >= max_batches):
        print(f"\nTo continue, re-run with: --resume-from-id {cursor}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true",
                        help="Actually write to Typesense. Without this flag, runs as a dry run.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int, default=200,
                        help="0 = unbounded (run to full completion in this invocation).")
    parser.add_argument("--resume-from-id", type=str, default=None,
                        help="Continue from a previously-printed cursor id.")
    args = parser.parse_args()

    if args.execute:
        print("!! --execute is set: this WILL write to the collection above. "
              "Ctrl+C now to abort. Continuing in 5 seconds...")
        time.sleep(5)

    asyncio.run(run(args.execute, args.batch_size, args.max_batches, args.resume_from_id))


if __name__ == "__main__":
    main()
