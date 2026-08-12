"""
sync_service/master_candidates/verify_typesense_semantics.py

Manual, read-only verification script for the 3 Typesense behaviors this
search-planner rewrite depends on, which are NOT conclusively settled by
Typesense's public docs (checked during planning — see the implementation
plan). Run this once against the real master_candidates_v1 collection
before trusting the corresponding assumptions in skill_logic.py /
keyword_query.py.

SECURITY: pass a freshly-generated Typesense API key / admin key via env
vars below. Never hardcode a key here, never commit one, never reuse a key
that has appeared in chat/logs/tickets.

Usage:
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
        python3 verify_typesense_semantics.py
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


def search(params: dict) -> dict:
    r = httpx.get(
        f"{BASE}/collections/{COLLECTION}/documents/search",
        headers=HEADERS, params=params, timeout=20.0,
    )
    r.raise_for_status()
    return r.json()


def check_case_sensitivity():
    """
    Q1: is `skills:=value` case-sensitive on a string[] field?
    Pick a skill value you KNOW exists in a specific case (inspect a real
    document first if unsure), then compare found counts across casings.
    Update SAMPLE_SKILL below to a real value before running.
    """
    SAMPLE_SKILL = "B2B"  # <-- replace with a real, known-case skill value
    variants = [SAMPLE_SKILL, SAMPLE_SKILL.lower(), SAMPLE_SKILL.upper(),
                SAMPLE_SKILL.capitalize()]
    print("\n=== Q1: case-sensitivity of skills:=value ===")
    results = {}
    for v in variants:
        data = search({
            "q": "*", "query_by": "full_name",
            "filter_by": f"skills:=`{v}`",
            "per_page": 1,
        })
        results[v] = data.get("found", 0)
        print(f"  skills:=`{v}` -> found={results[v]}")
    if len(set(results.values())) == 1:
        print("  => CASE-INSENSITIVE (all casings return the same count)")
    else:
        print("  => CASE-SENSITIVE (counts differ by casing) — "
              "normalize_skill() on the query side alone will NOT match "
              "differently-cased indexed values; a reindex/shadow-field "
              "would be required to fully fix this, which is out of scope.")


def check_drop_tokens_threshold():
    """
    Q2: does drop_tokens_threshold=0 force ALL tokens required (true AND),
    vs. the default progressive relaxation?
    Pick two real, common terms (SAMPLE_A common, SAMPLE_B rare) so the
    "all tokens" vs "relaxed" result counts are visibly different.
    """
    SAMPLE_A, SAMPLE_B = "sales", "kubernetes"  # <-- replace with real terms
    print("\n=== Q2: drop_tokens_threshold=0 forces AND ===")
    default = search({
        "q": f"{SAMPLE_A} {SAMPLE_B}", "query_by": "skills_text,full_name,title",
        "per_page": 1,
    })
    forced_and = search({
        "q": f"{SAMPLE_A} {SAMPLE_B}", "query_by": "skills_text,full_name,title",
        "drop_tokens_threshold": 0, "per_page": 1,
    })
    print(f"  default found={default.get('found', 0)}")
    print(f"  drop_tokens_threshold=0 found={forced_and.get('found', 0)}")
    print("  If the second number is smaller (and plausibly equal to a "
          "true intersection), drop_tokens_threshold=0 is forcing AND as "
          "expected. If they're equal, it likely had no effect for this "
          "query and Tier A's AND-chain translation needs a different "
          "mechanism (e.g. filter_by on skills_text via infix, or per-term "
          "sub-queries intersected in Python).")


def check_bare_exclude():
    """Q3: does a bare `-word` in q actually remove docs containing it?"""
    SAMPLE = "redux"  # <-- replace with a real term you can spot-check
    print("\n=== Q3: bare -word exclude in q ===")
    without = search({"q": "*", "query_by": "full_name", "per_page": 1})
    with_excl = search({
        "q": f"* -{SAMPLE}", "query_by": "skills_text,full_name,title",
        "per_page": 1,
    })
    print(f"  q='*' found={without.get('found', 0)}")
    print(f"  q='* -{SAMPLE}' found={with_excl.get('found', 0)}")
    print("  Manually inspect a hit containing the term to confirm it's "
          "genuinely absent from the excluded result set, not just "
          "down-ranked.")


if __name__ == "__main__":
    check_case_sensitivity()
    check_drop_tokens_threshold()
    check_bare_exclude()
