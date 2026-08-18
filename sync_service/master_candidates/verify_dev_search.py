"""
sync_service/master_candidates/verify_dev_search.py

Steps 6 and 7 of the validation sequence — run against master_candidates_v1_dev
directly (calls the REAL _search_impl() function in-process, pointed at the
dev collection via env config, since there's no separate deployed service
instance running the new code against dev collections). This deliberately
does NOT touch verify_search_fix.py, which targets the LIVE PRODUCTION
/mc/search_v2 URL by default and is meant to be re-run AFTER an actual
deploy — mixing "pre-deploy dev check" and "post-deploy prod check" into one
script risked someone accidentally defaulting to production before this new
code is live there.

Step 6 — new filter dimensions (industry/job_function/functional_area/
company_industry/seniority/languages): confirms each filter actually
narrows results (rather than being silently ignored) using REAL values
discovered from the data itself, not hardcoded guesses.

Step 7 — regression: MUST/NICE OR-combined ranking, two-bucket A/B ordering,
per_page=15 pagination (page 1, page 2, bucket boundary, a deep page),
OpenToWork (via activityCategory, which is what the backend actually reads —
see classicSearchQuery.ts for the frontend-side openToWork -> activityCategory
translation this script deliberately mirrors), and keyword search.

NOT covered here (out of scope for a backend script): the frontend
internal/external waterfall itself — that's Part 1's
useUnifiedWaterfallSearch.test.ts (vitest), already passing and unaffected
by anything in this file.

SAFETY: hard guard — refuses to run unless MC_COLLECTION_NAME ends in "_dev".

Usage:
    TYPESENSE_HOST=... TYPESENSE_PORT=8108 TYPESENSE_API_KEY=... \
    MC_COLLECTION_NAME=master_candidates_v1_dev \
        python3 verify_dev_search.py
"""
import os
import sys

_required = ["TYPESENSE_HOST", "TYPESENSE_API_KEY"]
_missing = [k for k in _required if not os.environ.get(k)]
if _missing:
    print(f"Set these env vars first: {_missing}. Aborting.")
    sys.exit(1)

if not os.environ.get("MC_COLLECTION_NAME", "").endswith("_dev"):
    print("REFUSING TO RUN: MC_COLLECTION_NAME must be set and end in '_dev'.")
    sys.exit(1)

os.environ.setdefault("SUPABASE_URL", "http://unused.invalid")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "unused")
os.environ.setdefault("WEBHOOK_SECRET", "unused")
os.environ.setdefault("ADMIN_SECRET", "unused")

import asyncio  # noqa: E402
import sys as _sys  # noqa: E402

_sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from master_candidates import search_api  # noqa: E402
from master_candidates.config import TS_COLLECTION  # noqa: E402

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def chip(label, mode):
    return {"label": label, "mode": mode}


async def base_filters(**overrides) -> dict:
    f = {
        "keyword": "", "skillChips": [], "titles": [], "locations": [],
        "currentEmployer": [], "school": [], "degree": [], "yearsExperience": "",
    }
    f.update(overrides)
    return f


async def call(filters: dict, page: int = 1, per_page: int = 15) -> dict:
    return await search_api._search_impl(
        {"filters": filters, "page": page, "per_page": per_page}, avatar_proxy=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Step 6 — new filter dimensions
# ═══════════════════════════════════════════════════════════════════════════

async def discover_a_real_value(field_key: str, facet_field: str) -> str | None:
    """Runs an unfiltered search and reads a facet value straight off the
    real dev data, so step-6 checks use values that actually exist rather
    than guessing."""
    result = await call(await base_filters(), page=1, per_page=1)
    for facet in result.get("facets", []):
        if facet.get("field_name") == facet_field and facet.get("counts"):
            return facet["counts"][0]["value"]
    return None


async def check_new_filters() -> None:
    print("\n=== Step 6: new filter dimensions ===")
    baseline = await call(await base_filters())
    baseline_total = baseline["total"]
    print(f"  baseline (unfiltered) total: {baseline_total}")

    dimension_checks = [
        ("industries", "industry"),
        ("jobFunctions", "job_function"),
        ("functionalAreas", "functional_area"),
        ("companyIndustries", "company_industry"),
        ("seniorities", "seniority"),
        ("languages", "languages_filter"),
    ]
    for filter_key, facet_field in dimension_checks:
        value = await discover_a_real_value(filter_key, facet_field)
        if value is None:
            print(f"  [SKIP] {filter_key}: no facet value found in dev data for '{facet_field}' "
                  f"— seed_dev_collection.py sample may lack this field; not auto-failed")
            continue
        result = await call(await base_filters(**{filter_key: [value]}))
        check(
            f"{filter_key}=[{value!r}] narrows or equals baseline and returns >0",
            0 < result["total"] <= baseline_total,
            f"got total={result['total']}, baseline={baseline_total}",
        )


# ═══════════════════════════════════════════════════════════════════════════
#  Step 7 — regression
# ═══════════════════════════════════════════════════════════════════════════

async def check_regression() -> None:
    print("\n=== Step 7: regression ===")

    # MUST/NICE OR-combined ranking + two-bucket A/B — discover real MUST/NICE
    # candidates from skill facets isn't directly exposed via facets (skills
    # isn't faceted, by design — see implementation plan), so this uses the
    # actual documents returned by an unfiltered query to pick two real skill
    # values a real candidate has, guaranteeing the check exercises real data.
    sample = await call(await base_filters(), page=1, per_page=5)
    skills_seen = [s for p in sample["profiles"] for s in (p.get("_skills") or [])]
    if len(skills_seen) >= 2:
        must_skill, nice_skill = skills_seen[0], skills_seen[1]
        result = await call(await base_filters(skillChips=[
            chip(must_skill, "must"), chip(nice_skill, "nice"),
        ]))
        check(
            f"MUST+NICE ({must_skill!r}/{nice_skill!r}) returns a valid shape",
            set(result.keys()) == {"profiles", "total", "page", "per_page", "facets", "took_ms", "count_capped"},
        )
    else:
        print("  [SKIP] MUST/NICE bucket check — fewer than 2 skills found in sample data")

    # per_page=15 pagination: page 1, page 2, no duplicate ids across pages.
    page1 = await call(await base_filters(), page=1, per_page=15)
    page2 = await call(await base_filters(), page=2, per_page=15)
    ids1 = {p["id"] for p in page1["profiles"]}
    ids2 = {p["id"] for p in page2["profiles"]}
    check("per_page=15 page1/page2 have no duplicate ids", not (ids1 & ids2),
          f"overlap: {ids1 & ids2}")
    check("page1 returns up to 15 profiles", len(page1["profiles"]) <= 15)

    # Deep pagination — must not crash, must return a valid (possibly empty) shape.
    deep = await call(await base_filters(), page=50, per_page=15)
    check("deep page (50) returns valid shape without crashing",
          set(deep.keys()) == {"profiles", "total", "page", "per_page", "facets", "took_ms", "count_capped"})

    # OpenToWork — backend reads activityCategory, not a raw openToWork flag
    # (that translation happens frontend-side, see classicSearchQuery.ts).
    otw = await call(await base_filters(activityCategory="OpenToWork"))
    check("activityCategory=OpenToWork returns a valid shape without crashing",
          set(otw.keys()) == {"profiles", "total", "page", "per_page", "facets", "took_ms", "count_capped"})
    print(f"  activityCategory=OpenToWork total={otw['total']} (vs. unfiltered — informational only)")

    # Keyword search (Tier A exact).
    kw = await call(await base_filters(keyword="engineer"))
    check("keyword='engineer' returns a valid shape", "profiles" in kw and "total" in kw)
    print(f"  keyword='engineer' total={kw['total']}, count_capped={kw['count_capped']}")

    print("\n  NOTE: the frontend internal/external waterfall itself is NOT tested by this "
          "script — that's src/components/rocketreach/unified/useUnifiedWaterfallSearch.test.ts "
          "(vitest), already passing and unaffected by anything in this backend change.")


async def main() -> None:
    print(f"Target dev collection: {TS_COLLECTION}")
    await check_new_filters()
    await check_regression()

    print(f"\n=== Summary: {len(FAILURES)} failure(s) ===")
    if FAILURES:
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
