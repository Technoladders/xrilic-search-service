"""
sync_service/master_candidates/test_indexer.py

Unit tests for the search-suggestions-era additions to transform_row():
the dob -> age_years lenient parser, the desired_job_type/
employment_status_pref delimiter-split heuristic, and the industry/
functional_area/job_function/company_industry mapping fix (previously
selected from Postgres but silently dropped before reaching the Typesense
doc — see the implementation plan).
"""
import pytest

from master_candidates import indexer


# ═══════════════════════════════════════════════════════════════════════════
#  dob -> age_years (free-text source, never raises)
# ═══════════════════════════════════════════════════════════════════════════

def test_age_years_parses_the_observed_sample_format():
    # "12 Apr 1988" is the exact sample value seen in the supplied DDL.
    age = indexer._parse_age_years("12 Apr 1988")
    assert age is not None and age > 30  # exact value depends on "today", just sanity-bound it


@pytest.mark.parametrize("dob", [None, "", "garbage", "not a date", "0000-00-00", "99/99/9999"])
def test_age_years_never_raises_on_bad_input(dob):
    assert indexer._parse_age_years(dob) is None


@pytest.mark.parametrize("dob,fmt_family", [
    ("1990-05-20", "iso"),
    ("20/05/1990", "d-m-y slash"),
    ("20-05-1990", "d-m-y dash"),
    ("05/20/1990", "m-d-y slash"),
    ("20 May 1990", "d Mon y"),
    ("20 May, 1990".replace(",", ""), "d Month y"),
])
def test_age_years_handles_multiple_formats(dob, fmt_family):
    age = indexer._parse_age_years(dob)
    assert age is not None, f"failed to parse {fmt_family} format: {dob!r}"


def test_age_years_rejects_implausible_ages():
    # A parseable-but-nonsensical date (e.g. someone born "in the future" or
    # implausibly long ago) should come back None, not a garbage age.
    assert indexer._parse_age_years("01 Jan 2999") is None


# ═══════════════════════════════════════════════════════════════════════════
#  desired_job_type / employment_status_pref — single delimited TEXT column
# ═══════════════════════════════════════════════════════════════════════════

def test_split_multi_value_matches_the_observed_sample():
    assert indexer._split_multi_value("Permanent / Temporary") == ["Permanent", "Temporary"]


@pytest.mark.parametrize("raw,expected", [
    (None, []),
    ("", []),
    ("Full time", ["Full time"]),
    ("Full time, Part time", ["Full time", "Part time"]),
    ("A/B/C", ["A", "B", "C"]),
    ("  spaced  /  out  ", ["spaced", "out"]),
])
def test_split_multi_value_cases(raw, expected):
    assert indexer._split_multi_value(raw) == expected


# ═══════════════════════════════════════════════════════════════════════════
#  transform_row() — full mapping for all 12 confirmed fields
# ═══════════════════════════════════════════════════════════════════════════

def _row(**overrides):
    base = {"id": "abc", "skills": [], "experience": [], "education": [], "certifications": []}
    base.update(overrides)
    return base


def test_industry_and_functional_area_no_longer_silently_dropped():
    """Regression test for the pre-existing gap: these were already selected
    from Postgres but never written into the Typesense doc."""
    doc = indexer.transform_row(_row(industry="IT Services", functional_area="Engineering - Software & QA"))
    assert doc["industry"] == "IT Services"
    assert doc["functional_area"] == "Engineering - Software & QA"


def test_languages_filter_mirrors_languages_without_altering_it():
    """languages_filter is a brand-new, purely additive field carrying the
    identical data as the existing `languages` field — production's
    existing field is never altered (see typesense_client.py)."""
    doc = indexer.transform_row(_row(languages=[{"name": "English"}, {"name": "Hindi"}]))
    assert doc["languages"] == ["English", "Hindi"]
    assert doc["languages_filter"] == ["English", "Hindi"]


def test_all_five_live_enterprise_dimensions_mapped():
    doc = indexer.transform_row(_row(
        industry="IT Services", job_function="Engineering",
        functional_area="Engineering - Software & QA",
        company_industry="IT Services & Consulting", seniority="senior",
    ))
    assert doc["industry"] == "IT Services"
    assert doc["job_function"] == "Engineering"
    assert doc["functional_area"] == "Engineering - Software & QA"
    assert doc["company_industry"] == "IT Services & Consulting"
    assert doc["seniority"] == "senior"


def test_future_analytics_fields_all_mapped_but_raw_dob_never_sent():
    doc = indexer.transform_row(_row(
        gender="Male", dob="12 Apr 1988", marital_status="Married", disability="No",
        desired_job_type="Permanent / Temporary", employment_status_pref="Full time",
        work_auth_countries=["India"],
    ))
    assert doc["gender"] == "Male"
    assert doc["marital_status"] == "Married"
    assert doc["disability"] == "No"
    assert doc["desired_job_type"] == ["Permanent", "Temporary"]
    assert doc["employment_status_pref"] == ["Full time"]
    assert doc["work_auth_countries"] == ["India"]
    assert "age_years" in doc and isinstance(doc["age_years"], int)
    assert "dob" not in doc  # raw dob is deliberately never sent to Typesense


def test_missing_optional_fields_are_omitted_not_null():
    """Matches the existing transform_row() convention: the final dict
    comprehension drops None values so optional Typesense fields are simply
    absent rather than present-with-null."""
    doc = indexer.transform_row(_row())
    for field in ["industry", "job_function", "functional_area", "company_industry",
                  "gender", "age_years", "marital_status", "disability"]:
        assert field not in doc, f"{field} should be omitted when absent, not null"
    # array-typed fields default to [] (present, matching the existing
    # `sources`/`preferred_locations` convention) rather than being omitted.
    assert doc["desired_job_type"] == []
    assert doc["employment_status_pref"] == []
    assert doc["work_auth_countries"] == []
