"""
Standalone verification harness for search_api.py's v4 bucket-split logic.

Monkeypatches _ts_search with an in-memory fake Typesense that understands
the subset of filter_by syntax this file actually generates (skills:=X,
skills:=[a,b,c], skills:!=X, joined with &&), plus page/per_page and a
native "last_active_date_ts desc" sort — enough to exercise every branch:
simple path, nice-only (no must), must+nice straddling the A/B boundary,
and the has_base_constraint fallback.

Run: python3 test_bucket_split.py
"""
import asyncio
import sys
import types

# ── Stub out the two modules search_api imports from, so we can import it
# standalone without the real Supabase/Typesense config/env vars present. ──
config_stub = types.ModuleType("config")
config_stub.TYPESENSE_BASE = "http://fake"
config_stub.TS_HEADERS = {}
config_stub.TS_COLLECTION = "master_candidates_v1"
config_stub.SUPABASE_URL = "http://fake"
config_stub.SB_HEADERS = {"apikey": "x"}
config_stub.HTTP_TIMEOUT_TYPESENSE = 5.0
sys.modules["config"] = config_stub

ts_client_stub = types.ModuleType("typesense_client")
ts_client_stub.QUERY_BY = "full_name,title,skills_text"
ts_client_stub.QUERY_BY_WEIGHTS = "5,4,5"
sys.modules["typesense_client"] = ts_client_stub

# search_api.py uses relative imports (`from .config import ...`) — load it
# as a top-level module with a synthetic package so those resolve to the
# stubs above.
import importlib.util
pkg = types.ModuleType("mc_pkg")
pkg.__path__ = ["."]
sys.modules["mc_pkg"] = pkg
sys.modules["mc_pkg.config"] = config_stub
sys.modules["mc_pkg.typesense_client"] = ts_client_stub

spec = importlib.util.spec_from_file_location("mc_pkg.search_api", "search_api.py")
search_api = importlib.util.module_from_spec(spec)
sys.modules["mc_pkg.search_api"] = search_api
spec.loader.exec_module(search_api)


# ══════════════════════════════════════════════════════════════════════════
#  Synthetic dataset
# ══════════════════════════════════════════════════════════════════════════
# 20 "accounts" candidates: 3 in bucket A (3/2/1 nice matches), 17 in bucket B
# (zero nice matches) — same shape as the real accounts/java/react/devops
# case, just scaled down. Plus 5 candidates WITHOUT accounts at all, to
# verify must still hard-filters correctly.
DB = []
def _mk(id_, skills, last_active_ts):
    DB.append({"id": id_, "skills": skills, "last_active_date_ts": last_active_ts,
               "data_freshness_ts": last_active_ts, "full_name": id_, "title": "",
               "current_employer": "", "location": "", "country": "India",
               "linkedin_url": None, "profile_picture_url": None, "followers": 0})

# Bucket A (accounts + at least one nice skill) — deliberately NOT inserted
# in match-count order, to prove the ranking actually sorts them.
_mk("A-1match", ["accounts", "java"], 100)
_mk("A-3match", ["accounts", "java", "react", "devops"], 50)   # oldest, but most matches
_mk("A-2match", ["accounts", "react", "devops"], 90)

# Bucket B (accounts, zero nice skills) — 17 of them, varying recency so we
# can verify native (last_active_date_ts desc) order is preserved.
for i in range(17):
    _mk(f"B-{i:02d}", ["accounts", "sap", "excel"], 200 - i)

# Non-accounts candidates — must never appear once must=accounts is set.
for i in range(5):
    _mk(f"NOPE-{i}", ["java", "react", "devops"], 300 - i)

# ── Nice-only dataset (no must at all) — separate small pool for that case.
NICE_ONLY_DB = []
def _mk2(id_, skills, ts):
    NICE_ONLY_DB.append({"id": id_, "skills": skills, "last_active_date_ts": ts,
                          "data_freshness_ts": ts, "full_name": id_, "title": "",
                          "current_employer": "", "location": "", "country": "India",
                          "linkedin_url": None, "profile_picture_url": None, "followers": 0})
_mk2("N-4", ["java", "react", "devops", "accounts"], 10)
_mk2("N-2a", ["java", "react"], 20)
_mk2("N-2b", ["devops", "accounts"], 15)
_mk2("N-1", ["java"], 30)
_mk2("N-0", ["cobol"], 40)   # zero nice matches — must NOT appear (nice-only = OR filter)


# ══════════════════════════════════════════════════════════════════════════
#  Fake Typesense — understands the filter_by dialect this file generates
# ══════════════════════════════════════════════════════════════════════════
def _parse_clause(clause: str):
    clause = clause.strip()
    if clause.startswith("skills:=[") and clause.endswith("]"):
        vals = [v.strip("`") for v in clause[len("skills:=["):-1].split(",")]
        return lambda doc: any(v in doc["skills"] for v in vals)
    if clause.startswith("skills:!="):
        val = clause[len("skills:!="):].strip("`")
        return lambda doc: val not in doc["skills"]
    if clause.startswith("skills:="):
        val = clause[len("skills:="):].strip("`")
        return lambda doc: val in doc["skills"]
    raise ValueError(f"unhandled clause: {clause}")


def _matches(doc, filter_by: str) -> bool:
    if not filter_by:
        return True
    for clause in filter_by.split(" && "):
        if not _parse_clause(clause)(doc):
            return False
    return True


CURRENT_DB = DB

async def fake_ts_search(ts_params):
    filter_by = ts_params.get("filter_by", "")
    matched = [d for d in CURRENT_DB if _matches(d, filter_by)]
    matched.sort(key=lambda d: -d["last_active_date_ts"])  # native order
    page = ts_params.get("page", 1)
    per_page = ts_params.get("per_page", 10)
    start = (page - 1) * per_page
    page_docs = matched[start:start + per_page]
    hits = [{"document": d, "text_match": 0} for d in page_docs]
    return {"found": len(matched), "hits": hits, "facet_counts": [], "search_time_ms": 1}


search_api._ts_search = fake_ts_search


# ══════════════════════════════════════════════════════════════════════════
#  Driver — calls the real branch logic from search_api.search()'s body by
#  re-implementing just the dispatch (can't call search() directly, it's a
#  FastAPI-decorated endpoint expecting a Request + auth dependency).
# ══════════════════════════════════════════════════════════════════════════
async def run_search(filters: dict, page: int, per_page: int):
    q = (filters.get("keyword") or "").strip() or "*"
    _, nice, _ = search_api._extract_skill_chips(filters)
    base_filter_by = search_api._build_filter_by_base(filters)
    common_params = {
        "q": q, "query_by": "x", "query_by_weights": "1",
        "num_typos": "0", "prioritize_exact_match": "true",
        "facet_by": "", "max_facet_values": "10", "highlight_fields": "",
        "exclude_fields": "x",
        "sort_by": "last_active_date_ts:desc,data_freshness_ts:desc",
    }

    if not nice:
        ts_params = {**common_params, "page": page, "per_page": per_page}
        if base_filter_by:
            ts_params["filter_by"] = base_filter_by
        data = await fake_ts_search(ts_params)
        return {
            "profiles": [search_api._to_rr_profile(h) for h in data["hits"]],
            "total": data["found"], "count_capped": False,
        }

    nice_or_clause = search_api._filter_any("skills", nice)

    if not base_filter_by:
        a_filter_by = nice_or_clause or ""
        total, _facets = await search_api._count_only(common_params, a_filter_by)
        start, end = (page - 1) * per_page, page * per_page
        profiles, capped = await search_api._bucket_a_page(common_params, a_filter_by, nice, start, end)
        return {"profiles": profiles, "total": total, "count_capped": capped}

    nice_none_clause = search_api._filter_none_skills(nice)
    a_filter_by = " && ".join([p for p in [base_filter_by, nice_or_clause] if p])
    b_filter_by = " && ".join([p for p in [base_filter_by, nice_none_clause] if p])

    (total, _facets), (bucket_a_count, _) = await asyncio.gather(
        search_api._count_only(common_params, base_filter_by),
        search_api._count_only(common_params, a_filter_by),
    )
    start, end = (page - 1) * per_page, page * per_page

    if end <= bucket_a_count:
        profiles, capped = await search_api._bucket_a_page(common_params, a_filter_by, nice, start, end)
    elif start >= bucket_a_count:
        b_start, b_end = start - bucket_a_count, end - bucket_a_count
        profiles, capped = await search_api._bucket_b_page(common_params, b_filter_by, b_start, b_end)
    else:
        (a_profiles, a_capped), (b_profiles, b_capped) = await asyncio.gather(
            search_api._bucket_a_page(common_params, a_filter_by, nice, start, bucket_a_count),
            search_api._bucket_b_page(common_params, b_filter_by, 0, end - bucket_a_count),
        )
        profiles = a_profiles + b_profiles
        capped = a_capped or b_capped

    return {"profiles": profiles, "total": total, "count_capped": capped}


def chips(must=(), nice=(), exclude=()):
    return {"skillChips": (
        [{"label": s, "mode": "must"} for s in must] +
        [{"label": s, "mode": "nice"} for s in nice] +
        [{"label": s, "mode": "exclude"} for s in exclude]
    )}


async def main():
    failures = []
    def check(name, cond, detail=""):
        status = "PASS" if cond else "FAIL"
        print(f"[{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    # ── Case 3 equivalent: must=accounts alone ──────────────────────────
    r = await run_search(chips(must=["accounts"]), page=1, per_page=10)
    check("Case3: must-only total == 20", r["total"] == 20, f"got {r['total']}")
    check("Case3: no NOPE-* leaked in", all(not p["name"].startswith("NOPE") for p in r["profiles"]))

    # ── Case 2 equivalent: must=accounts, nice=java/react/devops, page 1, per_page=10 ──
    global CURRENT_DB
    r = await run_search(chips(must=["accounts"], nice=["java", "react", "devops"]), page=1, per_page=10)
    check("Case2: total stays 20 (not reduced to 3)", r["total"] == 20, f"got {r['total']}")
    names = [p["name"] for p in r["profiles"]]
    check("Case2: bucket A (3/2/1 match) ranked first, in that order",
          names[:3] == ["A-3match", "A-2match", "A-1match"], f"got {names[:3]}")
    check("Case2: bucket B follows, native recency order (B-00 newest)",
          names[3:10] == [f"B-{i:02d}" for i in range(7)], f"got {names[3:10]}")
    check("Case2: no NOPE-* leaked in", all(not p["name"].startswith("NOPE") for p in r["profiles"]))
    check("Case2: page 1 not marked capped", r["count_capped"] is False)

    # ── Straddle test: per_page=2, page=2 → rows [2:4) — last of A + first of B ──
    r = await run_search(chips(must=["accounts"], nice=["java", "react", "devops"]), page=2, per_page=2)
    names = [p["name"] for p in r["profiles"]]
    check("Straddle: row2=A-1match (last of A), row3=B-00 (first of B)",
          names == ["A-1match", "B-00"], f"got {names}")
    check("Straddle: total still 20", r["total"] == 20, f"got {r['total']}")

    # ── Pure bucket-B page: per_page=5, page=2 → rows [5:10), all bucket B ──
    r = await run_search(chips(must=["accounts"], nice=["java", "react", "devops"]), page=2, per_page=5)
    names = [p["name"] for p in r["profiles"]]
    check("Bucket-B-only page: rows [5:10) = B-02..B-06",
          names == [f"B-{i:02d}" for i in range(2, 7)], f"got {names}")

    # ── Case 1 equivalent: nice-only, no must at all ────────────────────
    CURRENT_DB = NICE_ONLY_DB
    r = await run_search(chips(nice=["java", "react", "devops", "accounts"]), page=1, per_page=10)
    names = [p["name"] for p in r["profiles"]]
    check("NiceOnly: acts as OR-filter — N-0 (zero matches) excluded",
          "N-0" not in names, f"got {names}")
    check("NiceOnly: total == union count (4, not full pool of 5)",
          r["total"] == 4, f"got {r['total']}")
    check("NiceOnly: ranked by match count desc (N-4 first)",
          names[0] == "N-4", f"got {names}")

    print()
    if failures:
        print(f"❌ {len(failures)} FAILED: {failures}")
        sys.exit(1)
    else:
        print("✅ ALL CHECKS PASSED")


asyncio.run(main())