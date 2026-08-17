"""
sync_service/master_candidates/test_backfill_match.py

Table-driven, no-network unit tests for backfill/match.py's pure functions
-- the merge/insert logic ported from ingest_master_candidate's SQL. These
tests exist specifically to pin down the field-by-field M1/M2 behavior
verified against the literal SQL (see match.py's module docstring), so a
future edit that accidentally generalizes M2 into "fill any empty field"
(a real mistake made once already during this migration's design, caught
only by re-reading the original SQL directly) fails a test immediately.
"""

from datetime import date, datetime, timezone

import pytest

from master_candidates.backfill import match


# ─────────────────────────────────────────────────────────────────────────────
# pg_greatest / pg_least -- NULL-skipping, not NULL-propagating
# ─────────────────────────────────────────────────────────────────────────────
def test_pg_greatest_ignores_none_does_not_propagate_it():
    assert match.pg_greatest(None, 5) == 5
    assert match.pg_greatest(5, None) == 5
    assert match.pg_greatest(None, None) is None
    assert match.pg_greatest(3, 7, None, 1) == 7


def test_pg_least_ignores_none():
    assert match.pg_least(None, 5) == 5
    assert match.pg_least(None, None) is None
    assert match.pg_least(3, 7, None, 1) == 1


def test_pg_greatest_works_on_dates_and_datetimes():
    d1, d2 = date(2024, 1, 1), date(2024, 6, 1)
    assert match.pg_greatest(d1, d2) == d2
    dt1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dt2 = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert match.pg_greatest(dt1, None, dt2) == dt2


# ─────────────────────────────────────────────────────────────────────────────
# Array/object merge helpers
# ─────────────────────────────────────────────────────────────────────────────
def test_jsonb_union_distinct_dedupes_exact_values_case_sensitive():
    out = match.jsonb_union_distinct(["Python", "SQL"], ["python", "SQL", "Go"])
    assert out == ["Python", "SQL", "python", "Go"]  # case-sensitive: Python != python


def test_jsonb_union_distinct_dedupes_dicts_by_content():
    a = [{"skill": "Python"}]
    b = [{"skill": "Python"}, {"skill": "Go"}]
    out = match.jsonb_union_distinct(a, b)
    assert out == [{"skill": "Python"}, {"skill": "Go"}]


def test_text_array_union_only_unions_when_incoming_non_empty():
    assert match.text_array_union_if_incoming(["Mumbai"], []) == ["Mumbai"]
    assert match.text_array_union_if_incoming(["Mumbai"], None) == ["Mumbai"]
    assert match.text_array_union_if_incoming(["Mumbai"], ["Mumbai", "Pune"]) == ["Mumbai", "Pune"]
    assert match.text_array_union_if_incoming(None, ["Pune"]) == ["Pune"]


def test_merge_contact_list_prefers_verified_then_confidence():
    existing = [{"value": "a@x.com", "verified": False, "confidence": 0.5}]
    incoming = [{"value": "a@x.com", "verified": True, "confidence": 0.9}]
    out = match.merge_contact_list(existing, incoming)
    assert len(out) == 1
    assert out[0]["verified"] is True


def test_merge_contact_list_keeps_distinct_values_sorted():
    existing = [{"value": "b@x.com", "verified": True, "confidence": 1.0}]
    incoming = [{"value": "a@x.com", "verified": True, "confidence": 1.0}]
    out = match.merge_contact_list(existing, incoming)
    assert [c["value"] for c in out] == ["a@x.com", "b@x.com"]


def test_merge_contact_list_tie_keeps_existing_first_seen():
    existing = [{"value": "a@x.com", "verified": True, "confidence": 1.0, "source": "existing"}]
    incoming = [{"value": "a@x.com", "verified": True, "confidence": 1.0, "source": "incoming"}]
    out = match.merge_contact_list(existing, incoming)
    assert out[0]["source"] == "existing"


def test_bool_or_dict_ors_each_key():
    existing = {"phone": True, "work_email": False, "personal_email": False}
    incoming = {"phone": False, "work_email": False, "personal_email": True}
    out = match.bool_or_dict(existing, incoming, ("phone", "work_email", "personal_email"))
    assert out == {"phone": True, "work_email": False, "personal_email": True}


def test_array_append_if_absent():
    assert match.array_append_if_absent(["portal_a"], "apollo") == ["portal_a", "apollo"]
    assert match.array_append_if_absent(["portal_a"], "portal_a") == ["portal_a"]
    assert match.array_append_if_absent(None, "portal_a") == ["portal_a"]


def test_source_authority_known_and_unknown_sources():
    assert match.source_authority("contactout") == 1.00
    assert match.source_authority("portal_a") == 0.90
    assert match.source_authority("something_unknown") == 0.50
    assert match.source_authority(None) == 0.50


# ─────────────────────────────────────────────────────────────────────────────
# build_insert_row
# ─────────────────────────────────────────────────────────────────────────────
def test_build_insert_row_defaults_country_to_india():
    row = match.build_insert_row({}, "portal_a")
    assert row["country"] == "India"


def test_build_insert_row_excludes_has_contact():
    row = match.build_insert_row({"full_name": "Test"}, "portal_a")
    assert "has_contact" not in row


def test_build_insert_row_sets_primary_source_and_sources():
    row = match.build_insert_row({}, "portal_a")
    assert row["primary_source"] == "portal_a"
    assert row["sources"] == ["portal_a"]


# ─────────────────────────────────────────────────────────────────────────────
# build_merge_patch -- the critical, previously-miscategorized behavior
# ─────────────────────────────────────────────────────────────────────────────
def _existing_row(**overrides):
    base = {
        "full_name": "Old Name", "title": "Old Title", "experience": [{"title": "Old Job"}],
        "education": [{"school_name": "Old School"}], "skills": ["Python"],
        "may_also_know_skills": [], "contact_availability": {"phone": True, "work_email": False, "personal_email": False},
        "available_emails": [], "available_phones": [], "preferred_locations": ["Mumbai"],
        "work_auth_countries": ["India"], "sources": ["talent_pool"], "primary_source": "talent_pool",
        "has_full_profile": False, "profile_completeness": 0.3,
        "data_freshness": "2024-01-01T00:00:00+00:00", "last_active_date": "2024-01-01",
        "raw_profile_by_source": {"talent_pool": {}}, "notice_period_days": None,
    }
    base.update(overrides)
    return base


def _incoming_payload(**overrides):
    base = {
        "full_name": "New Name", "title": "New Title", "experience": [{"title": "New Job"}],
        "education": [{"school_name": "New School"}], "skills": ["SQL"],
        "may_also_know_skills": [], "contact_availability": {"phone": False, "work_email": False, "personal_email": True},
        "available_emails": [], "available_phones": [], "preferred_locations": ["Pune"],
        "work_auth_countries": ["USA"], "has_full_profile": True, "profile_completeness": 0.8,
        "data_freshness": "2024-06-01T00:00:00+00:00", "last_active_date": "2024-06-01",
        "raw_metadata": {}, "resume_url": "https://example.com/resume.pdf",
    }
    base.update(overrides)
    return base


def test_m1_scalar_new_wins():
    patch = match.build_merge_patch(_existing_row(), _incoming_payload(), "portal_a", is_full_merge=True)
    assert patch["full_name"] == "New Name"
    assert patch["title"] == "New Title"


def test_m1_wholesale_replaces_rich_arrays_when_incoming_non_empty():
    patch = match.build_merge_patch(_existing_row(), _incoming_payload(), "portal_a", is_full_merge=True)
    assert patch["experience"] == [{"title": "New Job"}]
    assert patch["education"] == [{"school_name": "New School"}]


def test_m1_touches_work_auth_countries_primary_source_has_full_profile_completeness():
    existing = _existing_row(primary_source="talent_pool")  # authority 0.85
    patch = match.build_merge_patch(existing, _incoming_payload(), "portal_a", is_full_merge=True)  # authority 0.90
    assert patch["work_auth_countries"] == ["India", "USA"]
    assert patch["primary_source"] == "portal_a"  # strictly greater authority -> flips
    assert patch["has_full_profile"] is True
    assert patch["profile_completeness"] == pytest.approx(0.8)


def test_m1_primary_source_does_not_flip_on_authority_tie():
    existing = _existing_row(primary_source="portal_a")  # same authority as incoming
    patch = match.build_merge_patch(existing, _incoming_payload(), "portal_a", is_full_merge=True)
    assert patch["primary_source"] == "portal_a"  # unchanged either way here, but exercise the >= entry vs > flip distinction
    existing2 = _existing_row(primary_source="apollo")  # authority 0.95 > portal_a's 0.90
    patch2 = match.build_merge_patch(existing2, _incoming_payload(), "portal_a", is_full_merge=True)
    assert patch2["primary_source"] == "apollo"  # incoming is NOT strictly greater -> does not flip


def test_m2_scalar_old_wins_fill_only():
    existing = _existing_row(full_name="Old Name", title=None)
    patch = match.build_merge_patch(existing, _incoming_payload(), "portal_a", is_full_merge=False)
    assert patch["full_name"] == "Old Name"   # old present -> old wins
    assert patch["title"] == "New Title"      # old missing -> fills from incoming


def test_m2_does_not_touch_rich_fields_at_all_even_when_existing_is_empty():
    """The corrected, verified behavior: M2 does NOT generalize to 'fill
    any empty field'. experience/education/certifications/company/CTC/
    resume/etc. are simply absent from M2's SET clause in the real SQL and
    must be left completely untouched, not filled from an empty existing
    value. This test exists specifically because an earlier draft of this
    migration's design got this wrong by inference before the literal SQL
    was re-checked."""
    existing = _existing_row(experience=[], education=[])
    existing["current_ctc_lacs"] = None
    existing["resume_url"] = None
    patch = match.build_merge_patch(existing, _incoming_payload(), "portal_a", is_full_merge=False)
    assert "experience" not in patch
    assert "education" not in patch
    assert "current_ctc_lacs" not in patch
    assert "resume_url" not in patch
    assert "work_auth_countries" not in patch
    assert "primary_source" not in patch
    assert "has_full_profile" not in patch
    assert "profile_completeness" not in patch
    assert "company" not in patch


def test_both_paths_touch_skills_union_contact_dedup_sources_freshness():
    for is_full_merge in (True, False):
        patch = match.build_merge_patch(_existing_row(), _incoming_payload(), "portal_a", is_full_merge=is_full_merge)
        assert set(patch["skills"]) == {"Python", "SQL"}
        assert patch["contact_availability"] == {"phone": True, "work_email": False, "personal_email": True}
        assert patch["sources"] == ["talent_pool", "portal_a"]
        assert patch["data_freshness"].startswith("2024-06-01")  # incoming is more recent
        assert "last_seen_at" in patch


def test_greatest_never_nulls_out_existing_when_incoming_missing():
    existing = _existing_row(data_freshness="2024-06-01T00:00:00+00:00")
    incoming = _incoming_payload(data_freshness=None)
    patch = match.build_merge_patch(existing, incoming, "portal_a", is_full_merge=True)
    assert patch["data_freshness"].startswith("2024-06-01")  # preserved, not nulled
