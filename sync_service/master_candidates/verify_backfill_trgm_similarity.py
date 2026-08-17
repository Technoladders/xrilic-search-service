"""
sync_service/master_candidates/verify_backfill_trgm_similarity.py

Manual, real-credentials validation script -- NOT run by pytest, mirrors
verify_typesense_semantics.py's existing convention exactly. Captures a
fixture of real pg_trgm similarity() outputs so backfill/trgm.py's pure
Python port can be checked for exact parity offline
(test_backfill_trgm.py::test_fixture_parity_with_live_postgres).

REQUIRED MANUAL PRE-STEP -- run this once in the Supabase SQL editor before
running this script (PostgREST cannot create functions, only call existing
ones):

    create or replace function public.debug_trgm_similarity(a text, b text)
    returns real language sql immutable as $$
      select similarity(lower(a), lower(b))
    $$;

Run this script, confirm testdata/trgm_similarity_fixture.json was written,
then DROP the wrapper function -- it must never ship to production:

    drop function public.debug_trgm_similarity(text, text);

Usage:
    SUPABASE_URL=https://xxx.supabase.co SUPABASE_SERVICE_KEY=xxx \\
      python3 verify_backfill_trgm_similarity.py

NEVER commit real Supabase credentials to this repo.
"""

import asyncio
import itertools
import json
import os
import sys

import httpx

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "testdata", "trgm_similarity_fixture.json")

# Deliberately includes: exact duplicates, near-duplicates (one word
# dropped/swapped), common-surname collisions, short names, initials vs.
# full names, and punctuation/hyphenated names -- the edge cases most
# likely to expose a subtle divergence in the Python port.
_HANDCRAFTED_PAIRS = [
    ("Rahul Kumar", "Rahul Kumar"),
    ("Rahul Kumar", "Rahul Kumar Sharma"),
    ("Priya Sharma", "Priya Sarma"),
    ("Amit Patel", "Amit Patil"),
    ("S. Venkatesh", "Srinivas Venkatesh"),
    ("Kumar", "Kumari"),
    ("Mohammed Ali", "Md Ali"),
    ("Jane Doe", "John Smith"),
    ("", "Rahul Kumar"),
    ("A.K. Singh", "Ajay Kumar Singh"),
    ("Sai Kiran", "Sai Kiran Reddy"),
    ("...", "Rahul Kumar"),
    ("O'Brien", "OBrien"),
]


async def _fetch_real_names(client: httpx.AsyncClient, limit: int = 150) -> list[str]:
    r = await client.get(
        f"{SUPABASE_URL}/rest/v1/naukri_candidates",
        params={"select": "name", "name": "not.is.null", "limit": str(limit)},
        headers={"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"},
        timeout=30.0)
    r.raise_for_status()
    return [row["name"] for row in r.json() if row.get("name")]


async def _similarity(client: httpx.AsyncClient, a: str, b: str) -> float:
    r = await client.post(
        f"{SUPABASE_URL}/rest/v1/rpc/debug_trgm_similarity",
        headers={"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                 "Content-Type": "application/json"},
        json={"a": a, "b": b}, timeout=30.0)
    if r.status_code >= 400:
        raise RuntimeError(
            f"debug_trgm_similarity RPC failed ({r.status_code}): {r.text[:300]}\n"
            "Did you run the manual pre-step SQL in the Supabase SQL editor first?")
    return r.json()


async def main() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Set SUPABASE_URL and SUPABASE_SERVICE_KEY env vars first.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(FIXTURE_PATH), exist_ok=True)

    async with httpx.AsyncClient() as client:
        real_names = await _fetch_real_names(client)
        real_pairs = list(itertools.islice(
            (p for p in itertools.combinations(real_names, 2)), 60))

        pairs = _HANDCRAFTED_PAIRS + real_pairs
        fixture = []
        for a, b in pairs:
            expected = await _similarity(client, a, b)
            fixture.append({"a": a, "b": b, "expected_similarity": expected})

    with open(FIXTURE_PATH, "w", encoding="utf-8") as f:
        json.dump(fixture, f, indent=2, ensure_ascii=False)

    print(f"Captured {len(fixture)} pairs to {FIXTURE_PATH}")
    print("Now run: pytest master_candidates/test_backfill_trgm.py")
    print("Then DROP the debug_trgm_similarity function in the SQL editor -- it must never ship.")


if __name__ == "__main__":
    asyncio.run(main())
