"""
sync_service/master_candidates/inspect_production_collection.py

Step 2 of the validation sequence: READ-ONLY inspection of the production
master_candidates_v1 collection. Confirms:
  1. The collection exists.
  2. Document count (record it before/after any later dev-only work, as a
     baseline proving nothing touched production).
  3. Existing schema field list is intact (no drops/changes).
  4. A real search still works (one representative query, no filters).

Performs ZERO writes. No PATCH, no POST /documents/import, no DELETE.

Usage:
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_COLLECTION_NAME=master_candidates_v1 \
        python3 inspect_production_collection.py
"""
import os
import sys

import httpx

HOST = os.environ.get("TYPESENSE_HOST")
PORT = os.environ.get("TYPESENSE_PORT", "8108")
API_KEY = os.environ.get("TYPESENSE_API_KEY")
COLLECTION = os.environ.get("MC_COLLECTION_NAME", "master_candidates_v1")

if not HOST or not API_KEY:
    print("Set TYPESENSE_HOST and TYPESENSE_API_KEY env vars first. Aborting.")
    sys.exit(1)

BASE = f"http://{HOST}:{PORT}"
HEADERS = {"X-TYPESENSE-API-KEY": API_KEY, "Content-Type": "application/json"}


def main() -> None:
    print(f"=== Inspecting '{COLLECTION}' (read-only) ===\n")

    r = httpx.get(f"{BASE}/collections/{COLLECTION}", headers=HEADERS, timeout=20.0)
    if r.status_code == 404:
        print(f"FAILED — collection '{COLLECTION}' does not exist.")
        sys.exit(1)
    r.raise_for_status()
    meta = r.json()

    print(f"1. Collection exists: YES")
    print(f"2. Document count (num_documents): {meta.get('num_documents')}")
    print(f"   Memory usage (num_memory_bytes): {meta.get('num_memory_bytes')}")

    fields = meta.get("fields", [])
    print(f"\n3. Schema field count: {len(fields)}")
    field_names = sorted(f["name"] for f in fields)
    print("   Field names:")
    for name in field_names:
        print(f"     - {name}")

    # Fields this project expects to ALREADY be absent from production
    # (they're additive-only, per Operation A) — report which of them are
    # already present vs. genuinely still missing, purely informational.
    new_fields_expected = [
        "industry", "job_function", "functional_area", "company_industry",
        "gender", "age_years", "marital_status", "disability",
        "desired_job_type", "employment_status_pref", "work_auth_countries",
    ]
    present = [f for f in new_fields_expected if f in field_names]
    missing = [f for f in new_fields_expected if f not in field_names]
    print(f"\n   Of the 11 new dimension fields: {len(present)} already present, {len(missing)} missing.")
    if present:
        print(f"     Already present (unexpected if this is the FIRST run of this check): {present}")
    print(f"     Missing (expected — these are what Operation A would add): {missing}")

    languages_field = next((f for f in fields if f["name"] == "languages"), None)
    if languages_field is None:
        print("\n   'languages' field: NOT PRESENT — Operation B (migration) would be unnecessary;")
        print("   Operation A would create it correctly-typed from scratch.")
    else:
        print(f"\n   'languages' field: PRESENT — index={languages_field.get('index', True)}, "
              f"facet={languages_field.get('facet', False)}, type={languages_field.get('type')}")
        if languages_field.get("index") is False:
            print("   -> retrieve-only, as expected. Operation B (drop+recreate) would be needed"
                  " to make it filterable/faceted.")
        else:
            print("   -> already indexed/filterable! Operation B may be unnecessary — re-check"
                  " its exact type/facet settings against what's needed.")

    print("\n4. Running one representative unfiltered search to confirm search still works...")
    r = httpx.get(
        f"{BASE}/collections/{COLLECTION}/documents/search",
        headers=HEADERS,
        params={"q": "*", "query_by": "full_name", "per_page": 1},
        timeout=20.0,
    )
    r.raise_for_status()
    data = r.json()
    print(f"   Search OK — found={data.get('found')}, took_ms={data.get('search_time_ms')}")

    print("\n=== Inspection complete. No writes were performed. ===")


if __name__ == "__main__":
    main()
