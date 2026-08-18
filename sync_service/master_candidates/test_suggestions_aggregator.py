"""
sync_service/master_candidates/test_suggestions_aggregator.py

Unit tests for the suggestions aggregation job — normalize_value reuse,
canonicalization, per-dimension counting (including the current-vs-previous
employer/title split), the per-dimension cap, and the admin-triggered
rebuild_suggestions() orchestration against a fake Typesense client.
"""
import pytest

from master_candidates import suggestions_aggregator as sa


# ═══════════════════════════════════════════════════════════════════════════
#  Canonicalization
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("raw,expected", [
    ("Tata Consultancy Services Ltd", "Tata Consultancy Services"),
    ("Tata Consultancy Services Limited", "Tata Consultancy Services"),
    ("Some Company Pvt Ltd", "Some Company"),
    ("Some Company Private Limited", "Some Company"),
    ("Acme Inc.", "Acme"),
    ("Acme Inc", "Acme"),
    ("Acme LLC", "Acme"),
    ("TCS", "TCS"),                 # no suffix — unchanged (abbreviation aliasing is mc_synonyms' job, not this)
    ("Infosys", "Infosys"),
])
def test_canonicalize_employer_strips_legal_suffixes(raw, expected):
    assert sa.canonicalize_employer(raw) == expected


# ═══════════════════════════════════════════════════════════════════════════
#  Dimension aggregation — single streaming pass, all 15 dimensions
# ═══════════════════════════════════════════════════════════════════════════

def _doc(**overrides):
    base = {
        "id": "x", "skills": [], "title": None, "all_titles": [],
        "current_employer": None, "all_employers": [],
        "schools": [], "degrees": [], "fields_of_study": [], "languages": [],
        "location": None, "preferred_locations": [],
        "industry": None, "job_function": None, "functional_area": None,
        "company_industry": None, "seniority": None,
    }
    base.update(overrides)
    return base


async def _run(docs, cap=2000):
    return await sa.run_aggregation(sa._as_async_iter(docs), cap_per_dimension=cap)


def _row_by_id(report, row_id):
    return next((r for r in report.rows if r["id"] == row_id), None)


async def test_case_variants_merge_into_one_suggestion():
    docs = [_doc(skills=["React"]), _doc(skills=["react"]), _doc(skills=["REACT"])]
    report = await _run(docs)
    row = _row_by_id(report, "skill:react")
    assert row is not None
    assert row["candidate_count"] == 3


async def test_current_vs_previous_employer_split():
    docs = [
        # current=TCS, previously at Infosys -> Infosys counts as previous only.
        _doc(current_employer="Tata Consultancy Services Ltd",
             all_employers=["Tata Consultancy Services Ltd", "Infosys"]),
        # current=Infosys, no other history -> Infosys should NOT count as
        # "previous" for this candidate (it's their current employer).
        _doc(current_employer="Infosys", all_employers=["Infosys"]),
    ]
    report = await _run(docs)
    current_tcs = _row_by_id(report, "current_employer:tata consultancy services")
    current_infosys = _row_by_id(report, "current_employer:infosys")
    previous_infosys = _row_by_id(report, "previous_employer:infosys")

    assert current_tcs["candidate_count"] == 1
    assert current_infosys["candidate_count"] == 1
    assert previous_infosys["candidate_count"] == 1  # only from doc 1, not doc 2
    assert _row_by_id(report, "previous_employer:tata consultancy services") is None


async def test_current_vs_previous_title_split():
    docs = [
        _doc(title="Senior Software Engineer",
             all_titles=["Senior Software Engineer", "Software Engineer", "Intern"]),
    ]
    report = await _run(docs)
    assert _row_by_id(report, "current_title:senior software engineer")["candidate_count"] == 1
    assert _row_by_id(report, "previous_title:software engineer")["candidate_count"] == 1
    assert _row_by_id(report, "previous_title:intern")["candidate_count"] == 1
    # The current title must not ALSO show up as a "previous" title for the
    # same candidate, even though it's technically present in all_titles.
    assert _row_by_id(report, "previous_title:senior software engineer") is None


async def test_location_combines_current_and_preferred():
    docs = [_doc(location="Bangalore", preferred_locations=["Bangalore", "Chennai"])]
    report = await _run(docs)
    assert _row_by_id(report, "location:bangalore")["candidate_count"] == 2  # current + preferred
    assert _row_by_id(report, "location:chennai")["candidate_count"] == 1


async def test_all_15_dimensions_present_in_report():
    docs = [_doc(
        skills=["Python"], title="Engineer", all_titles=["Engineer"],
        current_employer="Acme", all_employers=["Acme"],
        schools=["Anna University"], degrees=["B.Tech"], fields_of_study=["CS"],
        languages=["English"], location="Chennai", preferred_locations=[],
        industry="IT Services", job_function="Engineering",
        functional_area="Engineering - Software & QA",
        company_industry="IT Services & Consulting", seniority="senior",
    )]
    report = await _run(docs)
    dims = {r["type"] for r in report.rows}
    expected = {
        "skill", "current_title", "current_employer", "school", "degree",
        "field_of_study", "language", "location", "industry", "job_function",
        "functional_area", "company_industry", "seniority",
    }
    assert expected.issubset(dims)


async def test_per_dimension_cap_is_enforced():
    docs = [_doc(skills=[f"skill-{i}"]) for i in range(50)]
    report = await _run(docs, cap=10)
    skill_rows = [r for r in report.rows if r["type"] == "skill"]
    assert len(skill_rows) == 10
    assert report.per_dimension_distinct_counts["skill"] == 50  # true cardinality, unaffected by the cap


async def test_empty_export_produces_no_rows():
    report = await _run([])
    assert report.rows == []
    assert report.documents_processed == 0


async def test_future_analytics_fields_never_read_by_aggregator():
    """The aggregator must not even look at gender/dob/etc — confirmed by
    checking EXPORT_FIELDS (what the real export call requests) excludes them."""
    for forbidden in ["gender", "dob", "age_years", "marital_status", "disability",
                       "desired_job_type", "employment_status_pref", "work_auth_countries"]:
        assert forbidden not in sa.EXPORT_FIELDS


# ═══════════════════════════════════════════════════════════════════════════
#  rebuild_suggestions() — admin-triggered orchestration (fake Typesense client)
# ═══════════════════════════════════════════════════════════════════════════

class _FakeAsyncClient:
    """Stands in for httpx.AsyncClient — only what rebuild_suggestions()'s
    dependencies (export_documents/upsert_suggestions_batch) would call, but
    since export_documents() itself needs a real streaming response, this
    test calls rebuild_suggestions() with an injected async doc iterator via
    monkeypatching run_aggregation's input path instead of hitting export."""


async def test_rebuild_suggestions_upserts_all_rows(monkeypatch):
    docs = [_doc(skills=["Python", "Go"]), _doc(skills=["Python"])]

    async def fake_export_documents(client):
        for d in docs:
            yield d

    upserted_batches = []

    async def fake_upsert(client, batch):
        upserted_batches.append(batch)
        return len(batch), []

    monkeypatch.setattr(sa, "export_documents", fake_export_documents)
    monkeypatch.setattr(sa, "upsert_suggestions_batch", fake_upsert)

    report = await sa.rebuild_suggestions(client=None)
    assert report.documents_processed == 2
    all_upserted = [row for batch in upserted_batches for row in batch]
    assert any(r["id"] == "skill:python" and r["candidate_count"] == 2 for r in all_upserted)
    assert any(r["id"] == "skill:go" and r["candidate_count"] == 1 for r in all_upserted)


async def test_rebuild_suggestions_handles_upsert_errors_without_raising(monkeypatch):
    async def fake_export_documents(client):
        yield _doc(skills=["Python"])

    async def fake_upsert_with_error(client, batch):
        return 0, ["some typesense error"]

    monkeypatch.setattr(sa, "export_documents", fake_export_documents)
    monkeypatch.setattr(sa, "upsert_suggestions_batch", fake_upsert_with_error)

    report = await sa.rebuild_suggestions(client=None)  # must not raise
    assert report.documents_processed == 1
