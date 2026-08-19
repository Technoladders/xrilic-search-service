"""
sync_service/master_candidates/test_search_planner.py

Real pytest (run: `pytest` from sync_service/, or `pytest master_candidates`)
replacing the stale, already-broken test_bucket_split.py.

Covers, per the implementation plan's test plan:
  - MUST/NICE/EXCLUDE filter_by construction (skill_logic.py), including the
    core regression test for the fixed bug: MUST+NICE combine via OR, not AND.
  - Keyword Boolean parser (keyword_query.py): malformed inputs -> 400/
    KeywordSyntaxError, valid inputs -> correct AST shape, Tier A/B
    classification.
  - End-to-end _search_impl() behavior against a fake in-memory Typesense
    (no network), including ranking order and the empty-filters no-scan
    regression check.

Live-Typesense integration tests (case-sensitivity, drop_tokens_threshold
behavior, exact union counts against the real 1M+ collection) are NOT here
— see verify_typesense_semantics.py and the implementation plan's
"Integration tests" section for what still needs to run against production
with a freshly-rotated token.
"""
import re

import pytest

from master_candidates import search_api
from master_candidates.search import keyword_query, skill_logic
from master_candidates.search.bucket_pagination import compute_bucket_slice
from master_candidates.search.query_types import (
    AndNode, KeywordSyntaxError, NotNode, OrNode, PhraseNode, SkillCriteria, TermNode,
)
from master_candidates.search.ranking import fetch_direct_slice


# ═══════════════════════════════════════════════════════════════════════════
#  skill_logic.py — MUST / NICE / EXCLUDE filter_by construction
# ═══════════════════════════════════════════════════════════════════════════

def test_must_only_is_and_chained():
    skills = SkillCriteria(must=["sales", "b2b"], nice=[], exclude=[])
    assert skill_logic.build_inclusion_filter(skills) == "skills:=`sales` && skills:=`b2b`"


def test_must_empty_list_is_none():
    skills = SkillCriteria(must=[], nice=[], exclude=[])
    assert skill_logic.build_inclusion_filter(skills) is None


def test_nice_only_is_or_array():
    skills = SkillCriteria(must=[], nice=["saas", "crm"], exclude=[])
    assert skill_logic.build_inclusion_filter(skills) == "skills:=[`saas`,`crm`]"


def test_must_and_nice_combine_via_or_not_and():
    """The core regression test for the fixed bug: (ALL must) OR (ANY nice)."""
    skills = SkillCriteria(must=["sales", "b2b"], nice=["saas", "crm"], exclude=[])
    result = skill_logic.build_inclusion_filter(skills)
    assert result == "(skills:=`sales` && skills:=`b2b`) || (skills:=[`saas`,`crm`])"
    assert "&&" not in result.split("||")[0].replace("skills:=`sales` && skills:=`b2b`", "X")


def test_exclude_is_and_chained_not():
    skills = SkillCriteria(must=[], nice=[], exclude=["insurance", "telecalling"])
    assert skill_logic.build_exclude_filter(skills) == "skills:!=`insurance` && skills:!=`telecalling`"


def test_exclude_empty_is_none():
    assert skill_logic.build_exclude_filter(SkillCriteria(must=[], nice=[], exclude=[])) is None


def test_extract_skill_chips_normalizes_case_and_whitespace():
    filters = {"skillChips": [
        {"label": " B2B ", "mode": "must"},
        {"label": "SaaS", "mode": "nice"},
        {"label": "Insurance", "mode": "exclude"},
    ]}
    skills = skill_logic.extract_skill_chips(filters)
    assert skills.must == ["b2b"]
    assert skills.nice == ["saas"]
    assert skills.exclude == ["insurance"]


def test_extract_skill_chips_empty_filters():
    skills = skill_logic.extract_skill_chips({})
    assert skills == SkillCriteria(must=[], nice=[], exclude=[])


@pytest.mark.parametrize("must", [[], ["Sales"], ["Sales", "B2B"], ["Sales", "B2B", "Python"]])
def test_must_all_combinations_are_and(must):
    skills = SkillCriteria(must=[m.lower() for m in must], nice=[], exclude=[])
    result = skill_logic.build_inclusion_filter(skills)
    if not must:
        assert result is None
    else:
        assert result.count("&&") == len(must) - 1
        assert "||" not in result


@pytest.mark.parametrize("exclude", [[], ["Insurance"], ["Insurance", "Telecalling"]])
def test_exclude_all_combinations_are_and_not(exclude):
    skills = SkillCriteria(must=[], nice=[], exclude=[e.lower() for e in exclude])
    result = skill_logic.build_exclude_filter(skills)
    if not exclude:
        assert result is None
    else:
        assert all("skills:!=" in part for part in result.split(" && "))


# ═══════════════════════════════════════════════════════════════════════════
#  keyword_query.py — parser correctness
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("expr", [
    "java AND", "AND react", "java OR", "java NOT",
    "(java AND react", "java AND )", "java AND ()",
])
def test_malformed_keyword_expressions_raise(expr):
    with pytest.raises(KeywordSyntaxError):
        keyword_query.parse(expr)


def test_empty_keyword_is_none():
    assert keyword_query.parse("") is None
    assert keyword_query.parse("   ") is None


def test_single_term():
    assert keyword_query.parse("java") == TermNode("java")


def test_and_chain():
    node = keyword_query.parse("java AND react")
    assert node == AndNode((TermNode("java"), TermNode("react")))


def test_implicit_and_without_keyword():
    """Adjacent bare terms with no explicit operator are implicit AND."""
    node = keyword_query.parse("java react")
    assert node == AndNode((TermNode("java"), TermNode("react")))


def test_or():
    node = keyword_query.parse("java OR react")
    assert node == OrNode((TermNode("java"), TermNode("react")))


def test_and_with_not():
    node = keyword_query.parse("java AND react NOT redux")
    assert node == AndNode((TermNode("java"), TermNode("react"), NotNode(TermNode("redux"))))


def test_parens_override_precedence():
    node = keyword_query.parse("java AND (react OR vue)")
    assert node == AndNode((TermNode("java"), OrNode((TermNode("react"), TermNode("vue")))))


def test_not_precedence_over_and_over_or():
    # NOT > AND > OR: "java AND NOT react OR vue" == (java AND (NOT react)) OR vue
    node = keyword_query.parse("java AND NOT react OR vue")
    assert node == OrNode((
        AndNode((TermNode("java"), NotNode(TermNode("react")))),
        TermNode("vue"),
    ))


def test_quoted_phrase():
    node = keyword_query.parse('"react developer"')
    assert node == PhraseNode("react developer")


def test_combined_phrase_or_and_not():
    node = keyword_query.parse('("react developer" OR "frontend engineer") AND typescript NOT php')
    assert node == AndNode((
        OrNode((PhraseNode("react developer"), PhraseNode("frontend engineer"))),
        TermNode("typescript"),
        NotNode(TermNode("php")),
    ))


def test_unterminated_quote_raises():
    with pytest.raises(KeywordSyntaxError):
        keyword_query.parse('"react developer AND typescript')


# ═══════════════════════════════════════════════════════════════════════════
#  keyword_query.py — Tier A (exact) vs Tier B (bounded) classification
# ═══════════════════════════════════════════════════════════════════════════

def test_and_chain_is_tier_a():
    node = keyword_query.parse("java AND react AND sql")
    tier, q = keyword_query.plan_keyword(node)
    assert tier == "A"
    assert q == "java react sql"


def test_bare_not_is_tier_a():
    node = keyword_query.parse("java NOT redux")
    tier, q = keyword_query.plan_keyword(node)
    assert tier == "A"
    assert q == "java -redux"


def test_or_is_tier_b():
    node = keyword_query.parse("java OR react")
    tier, q = keyword_query.plan_keyword(node)
    assert tier == "B"
    assert q is None


def test_not_of_group_is_tier_b():
    node = keyword_query.parse("NOT (java AND react)")
    tier, _ = keyword_query.plan_keyword(node)
    assert tier == "B"


def test_and_containing_or_is_tier_b():
    node = keyword_query.parse("java AND (react OR vue)")
    tier, _ = keyword_query.plan_keyword(node)
    assert tier == "B"


def test_empty_keyword_is_tier_empty():
    assert keyword_query.plan_keyword(None) == ("empty", None)


def test_phrase_compiles_with_quotes():
    node = keyword_query.parse('"react developer" AND typescript')
    tier, q = keyword_query.plan_keyword(node)
    assert tier == "A"
    assert q == '"react developer" typescript'


# ═══════════════════════════════════════════════════════════════════════════
#  End-to-end _search_impl() against a fake in-memory Typesense
# ═══════════════════════════════════════════════════════════════════════════

def _mk(id_, skills=(), text="", last_active_ts=0):
    return {
        "id": id_, "skills": list(skills), "text": text,
        "last_active_date_ts": last_active_ts, "data_freshness_ts": last_active_ts,
        "full_name": id_, "title": "", "current_employer": "", "location": "",
        "country": "India", "linkedin_url": None, "profile_picture_url": None,
        "followers": 0,
    }


_CLAUSE_RE = re.compile(r"\(|\)|&&|\|\||[^\s()]+")


def _eval_clause(clause: str, doc: dict) -> bool:
    if clause.startswith("skills:=[") and clause.endswith("]"):
        vals = [v.strip("`") for v in clause[len("skills:=["):-1].split(",")]
        return any(v in doc.get("skills", []) for v in vals)
    if clause.startswith("skills:!="):
        return clause[len("skills:!="):].strip("`") not in doc.get("skills", [])
    if clause.startswith("skills:="):
        return clause[len("skills:="):].strip("`") in doc.get("skills", [])
    if clause.startswith("id:=[") and clause.endswith("]"):
        vals = [v.strip("`") for v in clause[len("id:=["):-1].split(",")]
        return doc["id"] in vals
    raise ValueError(f"unhandled filter_by clause in fake Typesense: {clause!r}")


class _FilterEval:
    """Tiny recursive-descent evaluator for the `&&`/`||`/`()` filter_by
    grammar our own skill_logic.py generates — just enough to drive
    realistic end-to-end tests without a live Typesense instance."""

    def __init__(self, tokens: list[str], doc: dict):
        self.tokens, self.doc, self.pos = tokens, doc, 0

    def _peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _advance(self):
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def parse_or(self) -> bool:
        v = self.parse_and()
        while self._peek() == "||":
            self._advance()
            v = self.parse_and() or v
        return v

    def parse_and(self) -> bool:
        v = self.parse_primary()
        while self._peek() == "&&":
            self._advance()
            v = self.parse_primary() and v
        return v

    def parse_primary(self) -> bool:
        tok = self._peek()
        if tok == "(":
            self._advance()
            v = self.parse_or()
            assert self._advance() == ")"
            return v
        self._advance()
        return _eval_clause(tok, self.doc)


def _matches_filter(doc: dict, filter_by: str) -> bool:
    if not filter_by:
        return True
    tokens = _CLAUSE_RE.findall(filter_by)
    return _FilterEval(tokens, doc).parse_or()


def _matches_query(doc: dict, ts_params: dict) -> bool:
    """
    Simulates just enough real Typesense `q` behavior to test search_api's
    WIRING (does it pass drop_tokens_threshold=0 for Tier-A AND-chains, and
    a leading `-token` for NOT) — not a real relevance engine.
    """
    q = ts_params.get("q", "*")
    if not q or q == "*":
        return True
    text = doc.get("text", "").lower()
    positive, excluded = [], []
    for raw in q.split():
        tok = raw.strip('"').lower()
        (excluded if raw.startswith("-") else positive).append(tok.lstrip("-"))
    if any(e in text for e in excluded):
        return False
    if ts_params.get("drop_tokens_threshold") == 0:
        return all(p in text for p in positive)
    return (not positive) or any(p in text for p in positive)


def make_fake_ts_search(db: list[dict]):
    async def fake_ts_search(ts_params: dict) -> dict:
        matched = [d for d in db if _matches_filter(d, ts_params.get("filter_by", ""))]
        matched = [d for d in matched if _matches_query(d, ts_params)]
        matched.sort(key=lambda d: -d.get("last_active_date_ts", 0))
        page = ts_params.get("page", 1)
        per_page = ts_params.get("per_page", 10)
        start = (page - 1) * per_page
        page_docs = matched[start:start + per_page]
        return {
            "found": len(matched),
            "hits": [{"document": d, "text_match": 0} for d in page_docs],
            "facet_counts": [],
            "search_time_ms": 1,
        }
    return fake_ts_search


def chips(must=(), nice=(), exclude=()):
    return {"skillChips": (
        [{"label": s, "mode": "must"} for s in must]
        + [{"label": s, "mode": "nice"} for s in nice]
        + [{"label": s, "mode": "exclude"} for s in exclude]
    )}


@pytest.fixture
def patch_ts_search(monkeypatch):
    def _patch(db):
        monkeypatch.setattr(search_api, "_ts_search", make_fake_ts_search(db))
    return _patch


async def test_must_only_excludes_non_matching(patch_ts_search):
    db = [
        _mk("has-both", skills=["sales", "b2b"], last_active_ts=100),
        _mk("has-one", skills=["sales"], last_active_ts=90),
        _mk("has-neither", skills=["cobol"], last_active_ts=80),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": chips(must=["sales", "b2b"]), "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"has-both"}
    assert result["total"] == 1
    assert result["count_capped"] is False


async def test_must_and_nice_both_present_are_or_combined(patch_ts_search):
    """The core regression test: a MUST-only candidate (0 nice matches) and a
    NICE-only candidate (0 must matches) are BOTH included."""
    db = [
        _mk("must-only", skills=["sales", "b2b"], last_active_ts=100),          # satisfies MUST, 0 nice
        _mk("nice-only", skills=["saas"], last_active_ts=90),                   # satisfies NICE, 0 must
        _mk("both", skills=["sales", "b2b", "saas", "crm"], last_active_ts=80),  # satisfies both
        _mk("neither", skills=["cobol"], last_active_ts=70),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": chips(must=["sales", "b2b"], nice=["saas", "crm"]), "page": 1, "per_page": 10},
        avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"must-only", "nice-only", "both"}
    assert "neither" not in ids
    # ranking: more nice matches ranks first
    order = [p["id"] for p in result["profiles"]]
    assert order.index("both") < order.index("must-only")
    assert order.index("both") < order.index("nice-only")


async def test_exclude_removes_candidates_regardless_of_inclusion_branch(patch_ts_search):
    db = [
        _mk("must-excluded", skills=["sales", "b2b", "telecalling"], last_active_ts=100),
        _mk("nice-excluded", skills=["saas", "telecalling"], last_active_ts=90),
        _mk("clean", skills=["sales", "b2b"], last_active_ts=80),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": chips(must=["sales", "b2b"], nice=["saas"], exclude=["telecalling"]),
         "page": 1, "per_page": 10},
        avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"clean"}


async def test_empty_filters_no_scan_regression(patch_ts_search):
    db = [_mk(f"c{i}", last_active_ts=100 - i) for i in range(5)]
    patch_ts_search(db)
    result = await search_api._search_impl({"filters": {}, "page": 1, "per_page": 10}, avatar_proxy=True)
    assert result["total"] == 5
    assert result["count_capped"] is False


# ═══════════════════════════════════════════════════════════════════════════
#  search_api.py — new enterprise filter-dimension clauses
# ═══════════════════════════════════════════════════════════════════════════

def test_enterprise_filter_clauses_render_correctly():
    f = {
        "industries": ["IT Services"], "jobFunctions": ["Engineering"],
        "functionalAreas": ["Engineering - Software & QA"],
        "companyIndustries": ["IT Services & Consulting"],
        "seniorities": ["senior", "lead"], "languages": ["English"],
    }
    result = search_api._build_other_hard_filters(f)
    assert "industry:=[`IT Services`]" in result
    assert "job_function:=[`Engineering`]" in result
    assert "functional_area:=[`Engineering - Software & QA`]" in result
    assert "company_industry:=[`IT Services & Consulting`]" in result
    assert "seniority:=[`senior`,`lead`]" in result
    # Filters against the new languages_filter field, not the existing
    # (production, unchanged, retrieve-only) languages field.
    assert "languages_filter:=[`English`]" in result


def test_future_analytics_fields_have_no_filter_clause():
    """gender/age_years/marital_status/disability/desired_job_type/
    employment_status_pref/work_auth_countries are indexed but must not be
    filterable from the UI in this pass."""
    f = {
        "gender": ["Male"], "maritalStatus": ["Married"], "disability": ["No"],
        "desiredJobType": ["Permanent"], "employmentStatusPref": ["Full time"],
        "workAuthCountries": ["India"],
    }
    assert search_api._build_other_hard_filters(f) == ""


def test_empty_enterprise_filters_produce_no_clauses():
    assert search_api._build_other_hard_filters({}) == ""


# ── Past title/employer + field-of-study: clauses that were missing entirely.
# Before these existed, the frontend sent previousTitle/previousEmployer/major
# and the backend built NO clause, so setting one returned the whole
# collection rather than narrowing it.

def test_previous_title_filters_on_all_titles():
    result = search_api._build_other_hard_filters({"previousTitle": ["Software Engineer"]})
    assert result == "all_titles:=[`Software Engineer`]"


def test_previous_employer_filters_on_all_employers():
    result = search_api._build_other_hard_filters({"previousEmployer": ["Infosys", "TCS"]})
    assert result == "all_employers:=[`Infosys`,`TCS`]"


def test_major_filters_on_fields_of_study():
    result = search_api._build_other_hard_filters({"major": ["Computer Science"]})
    assert result == "fields_of_study:=[`Computer Science`]"


def test_previous_and_current_title_are_independent_clauses():
    """Current title filters `title`, previous filters `all_titles` — both
    present means both clauses, ANDed."""
    result = search_api._build_other_hard_filters({
        "titles": ["Senior Engineer"], "previousTitle": ["Junior Engineer"],
    })
    assert "title:=[`Senior Engineer`]" in result
    assert "all_titles:=[`Junior Engineer`]" in result


# ── Experience: numeric min/max overrides the bucket ──────────────────────

def test_years_bucket_still_works_unchanged():
    result = search_api._build_other_hard_filters({"yearsExperience": "3_5"})
    assert result == "total_experience_months:>=36 && total_experience_months:<=60"


def test_years_min_max_override_bucket():
    result = search_api._build_other_hard_filters({
        "yearsExperience": "3_5", "yearsMin": 7, "yearsMax": 12,
    })
    assert result == "total_experience_months:>=84 && total_experience_months:<=144"


def test_years_min_only():
    result = search_api._build_other_hard_filters({"yearsMin": 3})
    assert result == "total_experience_months:>=36"


def test_years_max_only():
    result = search_api._build_other_hard_filters({"yearsMax": 8})
    assert result == "total_experience_months:<=96"


def test_years_min_max_accepts_numeric_strings():
    """The wire may carry them as strings depending on how the UI serializes."""
    result = search_api._build_other_hard_filters({"yearsMin": "3", "yearsMax": "8"})
    assert result == "total_experience_months:>=36 && total_experience_months:<=96"


@pytest.mark.parametrize("bad", ["", None, "abc", "3.5.1", [], {}])
def test_years_min_max_ignore_malformed_values_without_raising(bad):
    """A malformed value must degrade to the bucket, never 500 the search."""
    result = search_api._build_other_hard_filters({"yearsExperience": "3_5", "yearsMin": bad})
    assert result == "total_experience_months:>=36 && total_experience_months:<=60"


def test_years_zero_min_is_honoured_not_treated_as_empty():
    """0 is a legitimate minimum and must not be swallowed by a falsy check."""
    result = search_api._build_other_hard_filters({"yearsMin": 0, "yearsMax": 2})
    assert result == "total_experience_months:>=0 && total_experience_months:<=24"


async def test_malformed_keyword_returns_400():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        await search_api._search_impl(
            {"filters": {"keyword": "java AND"}, "page": 1, "per_page": 10}, avatar_proxy=True,
        )
    assert exc_info.value.status_code == 400


async def test_keyword_tier_a_and_chain_is_exact(patch_ts_search):
    db = [
        _mk("has-both", text="expert in java and react", last_active_ts=100),
        _mk("has-java-only", text="java developer", last_active_ts=90),
        _mk("has-neither", text="cobol mainframe", last_active_ts=80),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {"keyword": "java AND react"}, "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"has-both"}
    assert result["count_capped"] is False
    assert result["total"] == 1


async def test_keyword_tier_a_not_excludes(patch_ts_search):
    db = [
        _mk("java-no-redux", text="java react developer", last_active_ts=100),
        _mk("java-with-redux", text="java react redux developer", last_active_ts=90),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {"keyword": "java AND react NOT redux"}, "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"java-no-redux"}


async def test_keyword_tier_b_or_unions_branches(patch_ts_search):
    db = [
        _mk("java-only", text="java backend engineer", last_active_ts=100),
        _mk("react-only", text="react frontend engineer", last_active_ts=90),
        _mk("neither", text="cobol mainframe", last_active_ts=80),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {"keyword": "java OR react"}, "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"java-only", "react-only"}
    # Tier B: total is an honest pool-derived count, not asserted equal to
    # any single-query Typesense `found` — but for a pool well within cap,
    # it should still equal the true union size.
    assert result["total"] == 2


async def test_keyword_tier_b_and_with_or_group_respects_parens(patch_ts_search):
    db = [
        _mk("java-and-react", text="java react developer", last_active_ts=100),
        _mk("java-and-vue", text="java vue developer", last_active_ts=90),
        _mk("java-only", text="java developer", last_active_ts=80),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {"keyword": "java AND (react OR vue)"}, "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"java-and-react", "java-and-vue"}


async def test_fallback_response_shape_is_frozen(patch_ts_search):
    """Every code path must return exactly these keys — this is what
    useUnifiedWaterfallSearch.ts's fallback trigger depends on."""
    patch_ts_search([_mk("x", last_active_ts=1)])
    result = await search_api._search_impl({"filters": {}, "page": 1, "per_page": 10}, avatar_proxy=True)
    assert set(result.keys()) == {"profiles", "total", "page", "per_page", "facets", "took_ms", "count_capped"}


async def test_single_nice_skill_no_must_skips_pool(patch_ts_search):
    """Live-verification finding: nice=[x] alone is a degenerate ranking case
    (every match has match_count=1 by construction of skills:=[x]) — it must
    take the single-call fast path, not the bounded/rerank pool, and must
    report count_capped=False with Typesense's own exact `found`."""
    db = [_mk(f"c{i}", skills=["react"], last_active_ts=100 - i) for i in range(5)]
    patch_ts_search(db)
    call_count = 0
    real_fake = search_api._ts_search

    async def counting_fake(params):
        nonlocal call_count
        call_count += 1
        return await real_fake(params)

    import unittest.mock as mock
    with mock.patch.object(search_api, "_ts_search", counting_fake):
        result = await search_api._search_impl(
            {"filters": chips(nice=["react"]), "page": 1, "per_page": 10}, avatar_proxy=True,
        )
    assert call_count == 1, f"expected exactly one Typesense call, got {call_count}"
    assert result["count_capped"] is False
    assert result["total"] == 5
    assert len(result["profiles"]) == 5


async def test_single_nice_skill_with_must_still_pools(patch_ts_search):
    """Not degenerate: a MUST-branch-qualifying candidate may or may not also
    have the one nice skill, so ranking is meaningful and the pool stays."""
    db = [
        _mk("must-plus-nice", skills=["sales", "b2b", "saas"], last_active_ts=100),
        _mk("must-only", skills=["sales", "b2b"], last_active_ts=90),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": chips(must=["sales", "b2b"], nice=["saas"]), "page": 1, "per_page": 10},
        avatar_proxy=True,
    )
    order = [p["id"] for p in result["profiles"]]
    assert order == ["must-plus-nice", "must-only"]


async def test_multi_nice_skill_still_pools_and_can_be_capped(patch_ts_search, monkeypatch):
    """2+ nice skills is a real ranking range (0..N) — pooling/capping still applies."""
    monkeypatch.setattr(search_api, "RERANK_POOL_HARD_CAP", 2)
    db = [_mk(f"c{i}", skills=["react"], last_active_ts=100 - i) for i in range(5)]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": chips(nice=["react", "vue"]), "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    assert result["count_capped"] is True
    assert result["total"] == 5  # true Typesense `found` — always exact for this (non-Tier-B) path


async def test_keyword_and_skills_compose_together(patch_ts_search):
    """keyword (Tier A, via q) and MUST/EXCLUDE (via filter_by) must both apply."""
    db = [
        _mk("match", skills=["aws", "postgresql"], text="java react developer", last_active_ts=100),
        _mk("wrong-skill", skills=["azure"], text="java react developer", last_active_ts=90),
        _mk("wrong-keyword", skills=["aws", "postgresql"], text="cobol mainframe", last_active_ts=80),
        _mk("excluded", skills=["aws", "postgresql", "php"], text="java react developer", last_active_ts=70),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {**chips(must=["aws", "postgresql"], exclude=["php"]), "keyword": "java AND react"},
         "page": 1, "per_page": 10},
        avatar_proxy=True,
    )
    ids = {p["id"] for p in result["profiles"]}
    assert ids == {"match"}


async def test_deep_page_beyond_pool_cap_falls_back_to_native_order(patch_ts_search, monkeypatch):
    """v3 behavior, preserved: a page requested beyond RERANK_POOL_HARD_CAP is
    fetched directly from Typesense (still filter-matching, just recency-
    ordered instead of exact nice-match-count ranked), and marked capped."""
    monkeypatch.setattr(search_api, "RERANK_POOL_HARD_CAP", 3)
    db = [_mk(f"c{i}", skills=["accounts"], last_active_ts=100 - i) for i in range(10)]
    patch_ts_search(db)
    # nice=[] would take the fast path; set a nice skill so ranking (and thus
    # the pool machinery) engages, matching every candidate's actual eligibility.
    result = await search_api._search_impl(
        {"filters": chips(must=["accounts"], nice=["accounts"]), "page": 3, "per_page": 2},
        avatar_proxy=True,
    )
    # page 3 * per_page 2 = 6 > RERANK_POOL_HARD_CAP(3) -> direct-page-fetch branch
    assert result["count_capped"] is True
    assert result["total"] == 10
    assert len(result["profiles"]) == 2


async def test_keyword_tier_b_capped_when_branch_exceeds_pool(patch_ts_search, monkeypatch):
    """Tier-B honesty contract: count_capped=True when a branch's true
    `found` exceeds its pool cap, AND `total` is the count of IDs actually
    fetched (an honest, possibly-truncated lower bound) — never Typesense's
    own exact `found` for that leaf. page_size is also shrunk here so the
    pool genuinely truncates instead of one page happening to return
    everything (which would mask the undercount in a tiny test dataset)."""
    monkeypatch.setattr(search_api, "RERANK_POOL_HARD_CAP", 2)
    monkeypatch.setattr(search_api, "TYPESENSE_MAX_PER_PAGE", 1)
    db = [_mk(f"java{i}", text="java developer", last_active_ts=100 - i) for i in range(5)]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {"keyword": "java OR nonexistent"}, "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    assert result["count_capped"] is True
    # honest lower bound: only the first `pool_cap` java-matches were fetched,
    # strictly less than the true 5 that exist — never dressed up as exact.
    assert result["total"] == 2
    assert len(result["profiles"]) == 2


# ═══════════════════════════════════════════════════════════════════════════
#  Two-bucket MUST+NICE tiering — MUST-completeness-first ranking
# ═══════════════════════════════════════════════════════════════════════════

def test_must_partial_or_none_filter_grows_linearly():
    for n in [1, 2, 5, 10, 15]:
        must = [f"m{i}" for i in range(n)]
        clause = skill_logic.build_must_partial_or_none_filter(must)
        assert clause.count("skills:!=") == n
        assert clause.count("||") == max(0, n - 1)  # linear, never combinatorial


def test_compute_bucket_slice_page_fully_in_a():
    s = compute_bucket_slice(page=1, per_page=5, count_a=20)
    assert (s.a_offset, s.a_limit, s.needs_a, s.needs_b) == (0, 5, True, False)


def test_compute_bucket_slice_page_fully_in_b():
    s = compute_bucket_slice(page=5, per_page=5, count_a=10)
    assert (s.a_limit, s.b_offset, s.b_limit, s.needs_a, s.needs_b) == (0, 10, 5, False, True)


def test_compute_bucket_slice_boundary_page():
    s = compute_bucket_slice(page=3, per_page=5, count_a=12)
    # offset [10,15): 2 from A (10,11), 3 from B (0,1,2)
    assert (s.a_offset, s.a_limit, s.b_offset, s.b_limit, s.is_boundary) == (10, 2, 0, 3, True)


def test_compute_bucket_slice_count_a_zero():
    s = compute_bucket_slice(page=1, per_page=5, count_a=0)
    assert (s.needs_a, s.needs_b, s.b_offset, s.b_limit) == (False, True, 0, 5)


@pytest.mark.parametrize("offset,limit", [(0, 5), (10, 5), (7, 5), (2, 5), (23, 5)])
async def test_fetch_direct_slice_hits_exact_window_regardless_of_alignment(offset, limit):
    """fetch_direct_slice must return exactly [offset, offset+limit), whether
    or not `offset` is a multiple of `limit` — proven at both aligned (0, 10)
    and non-aligned (7, 2, 23) offsets, at a cost of at most 2 Typesense calls."""
    db = [_mk(f"c{i:02d}", last_active_ts=1000 - i) for i in range(40)]  # native order = c00..c39
    fake = make_fake_ts_search(db)
    hits, _took = await fetch_direct_slice(
        fake, {"q": "*", "query_by": "full_name"}, "last_active_date_ts:desc", offset, limit,
    )
    got_ids = [h["document"]["id"] for h in hits]
    expected_ids = [f"c{i:02d}" for i in range(offset, offset + limit)]
    assert got_ids == expected_ids


@pytest.mark.parametrize("must_n,nice_n", [(1, 1), (2, 5), (5, 10), (10, 20), (15, 30)])
async def test_two_bucket_scale_matrix(patch_ts_search, must_n, nice_n):
    """Enterprise-scale MUST/NICE combinations: correct bucket routing and
    within-bucket ranking hierarchy hold at realistic sizes, not just N=2."""
    must = [f"m{i}" for i in range(must_n)]
    nice = [f"n{i}" for i in range(nice_n)]

    # Bucket A: all must skills, plus 0/1/(2 if nice_n>=2) nice matches (secondary ranking).
    a0 = _mk("A-0nice", skills=list(must), last_active_ts=50)
    a1 = _mk("A-1nice", skills=must + nice[:1], last_active_ts=40)
    a2 = _mk("A-2nice", skills=must + nice[:2], last_active_ts=30) if nice_n >= 2 else None
    # Bucket B: missing exactly one must skill, has some nice matches.
    b_more = _mk("B-2must-1nice", skills=must[1:] + nice[:1], last_active_ts=90)   # must_n-1 of must, 1 nice
    b_fewer = _mk("B-1must-1nice", skills=must[:1] + nice[:1], last_active_ts=90) if must_n > 1 else None
    # Ineligible: neither all-must nor any nice.
    nope = _mk("NOPE", skills=["unrelated"], last_active_ts=200)

    db = [a0, a1, b_more, nope] + ([a2] if a2 else []) + ([b_fewer] if b_fewer else [])
    patch_ts_search(db)

    result = await search_api._search_impl(
        {"filters": chips(must=must, nice=nice), "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    ids = [p["id"] for p in result["profiles"]]

    assert "NOPE" not in ids
    assert result["total"] == len(db) - 1  # everything except NOPE

    # Bucket A must precede Bucket B entries, and rank by nice count desc.
    a_ids = ["A-2nice", "A-1nice", "A-0nice"] if a2 else ["A-1nice", "A-0nice"]
    assert ids[:len(a_ids)] == a_ids
    assert all(ids.index(a) < ids.index(b_more["id"]) for a in a_ids)
    if b_fewer:
        # B-2must (has must_n-1 of must skills) outranks B-1must (has only 1) within Bucket B.
        assert ids.index("B-2must-1nice") < ids.index("B-1must-1nice")


async def test_two_bucket_deep_pagination_never_leaks_bucket_b_before_a_exhausted(patch_ts_search, monkeypatch):
    """The single highest-priority test for this feature: even when Bucket A
    alone exceeds RERANK_POOL_HARD_CAP, and even at a non-page-aligned bucket
    boundary, no Bucket-B candidate may appear on any page until Bucket A's
    EXACT count is genuinely exhausted. Bucket routing is count-based, never
    dependent on the ranking-pool fallback."""
    monkeypatch.setattr(search_api, "RERANK_POOL_HARD_CAP", 10)
    per_page = 4
    count_a = 23  # deliberately > cap AND not a multiple of per_page
    must = ["react", "node"]
    nice = ["aws"]

    bucket_a_docs = [
        _mk(f"A-{i:02d}", skills=must, last_active_ts=1000 - i) for i in range(count_a)
    ]
    bucket_b_docs = [
        _mk(f"B-{i:02d}", skills=must[:1] + nice, last_active_ts=1000 - i) for i in range(6)
    ]
    patch_ts_search(bucket_a_docs + bucket_b_docs)

    seen_ids: list[str] = []
    total_pages = (count_a + len(bucket_b_docs) + per_page - 1) // per_page
    for page in range(1, total_pages + 1):
        result = await search_api._search_impl(
            {"filters": chips(must=must, nice=nice), "page": page, "per_page": per_page},
            avatar_proxy=True,
        )
        page_ids = [p["id"] for p in result["profiles"]]
        seen_ids.extend(page_ids)

        b_count_seen_so_far = sum(1 for i in seen_ids if i.startswith("B-"))
        a_count_seen_so_far = sum(1 for i in seen_ids if i.startswith("A-"))
        if b_count_seen_so_far > 0:
            # Once ANY Bucket-B candidate has appeared, EVERY Bucket-A
            # candidate must already have appeared — Bucket A is exhausted.
            assert a_count_seen_so_far == count_a, (
                f"page {page}: Bucket-B candidate appeared before Bucket A "
                f"({a_count_seen_so_far}/{count_a}) was exhausted"
            )

    # Sanity: every candidate was actually returned exactly once across all pages.
    assert len(seen_ids) == len(set(seen_ids)) == count_a + len(bucket_b_docs)
    assert seen_ids[:count_a] == [f"A-{i:02d}" for i in range(count_a)]


async def test_keyword_or_with_must_and_nice_does_not_crash(patch_ts_search):
    """Documented interim behavior (implementation plan, Edge Cases): keyword
    Tier-B combined with MUST+NICE bucket tiering is a follow-up — for now it
    must not crash, and falls back to the pre-existing single-filter path."""
    db = [
        _mk("match", skills=["react", "node", "aws"], text="java developer", last_active_ts=100),
        _mk("no-match", skills=["cobol"], text="java developer", last_active_ts=90),
    ]
    patch_ts_search(db)
    result = await search_api._search_impl(
        {"filters": {**chips(must=["react", "node"], nice=["aws"]), "keyword": "java OR python"},
         "page": 1, "per_page": 10},
        avatar_proxy=True,
    )
    assert set(result.keys()) == {"profiles", "total", "page", "per_page", "facets", "took_ms", "count_capped"}


async def test_zero_internal_matches_returns_total_zero_not_error(patch_ts_search):
    """A Boolean query matching nothing must return the normal empty shape
    (total=0), not raise — this is exactly what lets the existing external
    fallback in useUnifiedWaterfallSearch.ts fire unmodified."""
    patch_ts_search([_mk("irrelevant", skills=["cobol"], last_active_ts=1)])
    result = await search_api._search_impl(
        {"filters": chips(must=["sales", "b2b"]), "page": 1, "per_page": 10}, avatar_proxy=True,
    )
    assert result == {
        "profiles": [], "total": 0, "page": 1, "per_page": 10,
        "facets": result["facets"], "took_ms": result["took_ms"], "count_capped": False,
    }
