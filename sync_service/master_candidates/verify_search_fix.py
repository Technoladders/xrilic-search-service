"""
sync_service/master_candidates/verify_search_fix.py

Manual, read-only smoke test for the MUST/NICE/EXCLUDE OR-fix and the
keyword Boolean Tier A/B split, run against the REAL deployed /mc/search_v2
endpoint (not the unit-test fakes in test_search_planner.py). This is the
"Integration tests" section of the implementation plan — run once after
deploying this change, with a freshly-issued short-lived Supabase JWT.

SECURITY: never hardcode a token here, never commit one, never reuse a
token that has appeared in chat/logs/tickets — get a fresh one for this run.

Usage:
    MC_SEARCH_URL=https://sync.xrilic.ai/mc/search_v2 \
    MC_AUTH_TOKEN=<fresh-bearer-token> \
        python3 verify_search_fix.py
"""
import os
import sys

import httpx

URL = os.environ.get("MC_SEARCH_URL", "https://sync.xrilic.ai/mc/search_v2")
TOKEN = os.environ.get("MC_AUTH_TOKEN")

if not TOKEN:
    print("Set MC_AUTH_TOKEN to a freshly-issued bearer token first. Aborting.")
    sys.exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def search(skill_chips=(), keyword="", page=1, per_page=1):
    body = {
        "filters": {
            "keyword": keyword, "skillChips": list(skill_chips),
            "name": "", "linkedinUrl": "", "titles": [], "previousTitle": [],
            "excludeJobTitles": [], "currentTitlesOnly": True,
            "includeRelatedJobTitles": False, "matchExperience": "",
            "locations": [], "locationRadius": "", "currentWorkLocation": [],
            "pastWorkLocation": [], "currentEmployer": [], "previousEmployer": [],
            "companyFilter": "current", "excludeCompanies": [],
            "excludeCompaniesFilter": "both", "domain": [], "companySize": [],
            "companyIndustry": [], "companyRevenue": "", "companyPubliclyTraded": False,
            "companyFundingMin": "", "companyFundingMax": "", "companyTags": [],
            "yearsExperience": "", "yearsInCurrentRole": "", "recentlyChangedJobs": False,
            "school": [], "degree": [], "major": [], "seniority": [], "department": [],
            "managementLevels": [], "contactMethod": [], "emailGrade": "",
            "jobChangeSignal": "", "newsSignal": "", "jobPostingSignal": "",
            "orderBy": "popularity", "languages": [], "openToWork": False,
        },
        "page": page, "per_page": per_page,
    }
    r = httpx.post(URL, headers=HEADERS, json=body, timeout=30.0)
    r.raise_for_status()
    return r.json()


def chip(label, mode):
    return {"label": label, "mode": mode}


print("=== 1. Skill MUST/NICE OR-fix (re-run of the 3 scenarios from investigation) ===")
r1 = search(skill_chips=[chip("b2b", "nice"), chip("business developement", "nice")])
print(f"  nice=[b2b,business developement] -> total={r1['total']} (expect same universe as before: pure OR of the two)")

r2 = search(skill_chips=[chip("b2b", "nice"), chip("business developement", "nice"), chip("sales", "nice")])
print(f"  nice=[b2b,business developement,sales] -> total={r2['total']} (expect >> r1, still pure OR — unchanged from before, nice-only was already correct)")

r3 = search(skill_chips=[chip("b2b", "nice"), chip("business developement", "nice"), chip("sales", "must")])
print(f"  must=[sales] nice=[b2b,business developement] -> total={r3['total']}")
print("  BEFORE the fix this was 1,604 (AND-combined). AFTER the fix this should be")
print("  MUCH LARGER — the union of (sales-havers) with (b2b-or-bizdev-havers),")
print("  since must+nice now combine via OR, not AND. If this is still ~1,604,")
print("  the fix did not deploy correctly.")

print("\n=== 2. Keyword Tier A (exact) ===")
r4 = search(keyword="java AND react AND sql NOT redux")
print(f"  'java AND react AND sql NOT redux' -> total={r4['total']}, count_capped={r4['count_capped']}")
print("  Manually spot-check a few hits contain java+react+sql and NOT redux.")

print("\n=== 3. Keyword Tier B (bounded OR) ===")
r5 = search(keyword="java OR react")
print(f"  'java OR react' -> total={r5['total']}, count_capped={r5['count_capped']}")
print("  If count_capped=True, `total` is an HONEST LOWER BOUND, not the exact")
print("  union size — this is expected/correct for a wide OR, not a bug.")

print("\n=== 4. Combined ===")
r6 = search(
    skill_chips=[chip("aws", "must"), chip("postgresql", "must"), chip("php", "exclude")],
    keyword="java AND react",
)
print(f"  keyword='java AND react' + must=[aws,postgresql] + exclude=[php] -> total={r6['total']}")
print("  Manually spot-check every hit has aws AND postgresql, NOT php, and matches java+react.")
