"""
sync_service/master_candidates/test_suggestions_api.py

Unit tests for GET /mc/search_suggestions — tiered typo tolerance, type
validation, request construction against a fake Typesense client, and the
response contract. No network — search_suggestions() is called directly
with FastAPI's dependency (require_user) overridden.
"""
import pytest
from fastapi import HTTPException

from master_candidates import suggestions_api as sapi


# ═══════════════════════════════════════════════════════════════════════════
#  Tiered typo tolerance
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("qlen,expected", [(0, 0), (1, 0), (2, 0), (3, 1), (4, 1), (5, 2), (10, 2)])
def test_tiered_num_typos(qlen, expected):
    assert sapi._tiered_num_typos(qlen) == expected


# ═══════════════════════════════════════════════════════════════════════════
#  search_suggestions() — request construction + response shape
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def fake_ts_search(monkeypatch):
    captured = {}

    async def fake(ts_params):
        captured["params"] = ts_params
        return {"hits": [
            {"document": {"value": "React", "candidate_count": 42821, "type": "skill"}},
            {"document": {"value": "React Native", "candidate_count": 8932, "type": "skill"}},
        ]}

    monkeypatch.setattr(sapi, "_ts_suggestions_search", fake)
    return captured


async def test_basic_single_type_request(fake_ts_search):
    result = await sapi.search_suggestions(type="skill", q="rea", limit=8, user_id="u1")
    assert result == {"suggestions": [
        {"value": "React", "label": "React", "count": 42821, "type": "skill"},
        {"value": "React Native", "label": "React Native", "count": 8932, "type": "skill"},
    ]}
    params = fake_ts_search["params"]
    assert params["q"] == "rea"
    assert params["filter_by"] == "type:=[`skill`]"
    assert params["per_page"] == 8
    assert params["num_typos"] == 1  # len("rea") == 3 -> tier 2


async def test_multi_type_comma_separated(fake_ts_search):
    await sapi.search_suggestions(type="skill,title,employer".replace("title", "current_title").replace("employer", "current_employer"),
                                   q="soft", limit=5, user_id="u1")
    params = fake_ts_search["params"]
    assert params["filter_by"] == "type:=[`skill`,`current_title`,`current_employer`]"


async def test_empty_query_still_returns_top_suggestions(fake_ts_search):
    await sapi.search_suggestions(type="skill", q="", limit=8, user_id="u1")
    params = fake_ts_search["params"]
    assert params["q"] == "*"
    assert "num_typos" not in params  # no tiering needed for a wildcard query


async def test_unknown_type_rejected(fake_ts_search):
    with pytest.raises(HTTPException) as exc:
        await sapi.search_suggestions(type="not_a_real_dimension", q="x", limit=8, user_id="u1")
    assert exc.value.status_code == 400


async def test_future_analytics_types_are_not_valid_suggestion_dimensions(fake_ts_search):
    for forbidden in ["gender", "dob", "age_years", "marital_status", "disability",
                       "desired_job_type", "employment_status_pref", "work_auth_countries"]:
        with pytest.raises(HTTPException):
            await sapi.search_suggestions(type=forbidden, q="x", limit=8, user_id="u1")


async def test_empty_type_rejected(fake_ts_search):
    with pytest.raises(HTTPException) as exc:
        await sapi.search_suggestions(type="", q="x", limit=8, user_id="u1")
    assert exc.value.status_code == 400


def test_all_15_live_dimensions_are_valid_types():
    expected = {
        "skill", "current_title", "previous_title", "current_employer", "previous_employer",
        "school", "degree", "field_of_study", "language", "location",
        "industry", "job_function", "functional_area", "company_industry", "seniority",
    }
    assert sapi.VALID_TYPES == expected
