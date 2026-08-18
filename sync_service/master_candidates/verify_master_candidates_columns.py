"""
sync_service/master_candidates/verify_master_candidates_columns.py

Manual, read-only consistency check: confirms the LIVE `master_candidates`
table matches the DDL supplied during planning for the 12 fields this
search-suggestions feature depends on, before considering the backend
complete (per the explicit instruction — this is a consistency check
against a known-good DDL, not a discovery script).

Checks two things per field:
  1. The column exists at all (a `select=` naming a nonexistent column
     makes PostgREST fail the whole request with a 400/404 — if this
     script's single request succeeds, every listed column exists).
  2. The VALUE TYPE returned for a small sample matches what the DDL says
     (e.g. `work_auth_countries` should come back as a JSON array on every
     row that has it; the others should come back as plain strings/None).
     PostgREST doesn't expose column type metadata directly over the
     public REST API (information_schema is typically not in the exposed
     schema list), so this is the closest practical verification available
     without direct SQL access.

SECURITY: pass a freshly-generated Supabase service-role key via env vars.
Never hardcode a key here, never commit one, never reuse a key that has
appeared in chat/logs/tickets.

Usage:
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... python3 verify_master_candidates_columns.py
"""
import os
import sys

import httpx

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("Set SUPABASE_URL and SUPABASE_SERVICE_KEY env vars first. Aborting.")
    sys.exit(1)

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
}

# (column, expected_python_types_when_present) — per the DDL supplied during
# planning. `text[]` columns come back from PostgREST as a JSON list; plain
# `text` columns come back as a str.
FIELDS = {
    "industry":               (str,),
    "job_function":           (str,),
    "functional_area":        (str,),
    "company_industry":       (str,),
    "seniority":               (str,),
    "gender":                  (str,),
    "dob":                      (str,),   # free-text, NOT a date column — confirmed in the DDL
    "marital_status":          (str,),
    "disability":               (str,),   # confirmed TEXT ("Yes"/"No"), not boolean
    "desired_job_type":        (str,),    # single delimited TEXT column, not an array
    "employment_status_pref":  (str,),    # same
    "work_auth_countries":     (list,),   # genuine text[]
}


def main() -> None:
    cols = ",".join(FIELDS.keys())
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/master_candidates",
        headers=HEADERS,
        params={"select": f"id,{cols}", "limit": 20},
        timeout=20.0,
    )
    if r.status_code >= 400:
        print(f"FAILED — PostgREST rejected the select (status {r.status_code}):")
        print(r.text[:1000])
        print("\nThis means at least one column name doesn't exist on the live table.")
        print("Do NOT proceed with the indexer.py SELECT_COLUMNS change until this is fixed.")
        sys.exit(1)

    rows = r.json()
    print(f"Query succeeded — all {len(FIELDS)} columns exist on the live table. Sampled {len(rows)} rows.\n")

    mismatches = []
    for field, expected_types in FIELDS.items():
        seen_non_null = [row.get(field) for row in rows if row.get(field) is not None]
        if not seen_non_null:
            print(f"  {field}: no non-null sample in this batch (can't type-check — try a larger --limit or different offset)")
            continue
        bad = [v for v in seen_non_null if not isinstance(v, expected_types)]
        if bad:
            mismatches.append((field, expected_types, type(bad[0]), bad[0]))
            print(f"  {field}: MISMATCH — expected {expected_types}, got {type(bad[0])} (value: {bad[0]!r})")
        else:
            print(f"  {field}: OK — {len(seen_non_null)} sample value(s) match expected type {expected_types[0].__name__}")

    print()
    if mismatches:
        print(f"FAILED — {len(mismatches)} field(s) don't match the expected type from the DDL.")
        print("Re-check the corresponding Typesense schema field / transform_row() mapping before deploying.")
        sys.exit(1)
    print("PASSED — live database matches the supplied DDL for all checkable fields.")


if __name__ == "__main__":
    main()
