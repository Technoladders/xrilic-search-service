"""
sync_service/master_candidates/test_backfill_trgm.py

Two layers of testing for backfill/trgm.py's pg_trgm similarity() port:

1. Structural self-checks against Postgres's OWN documented show_trgm()
   examples (from the pg_trgm docs) -- these don't require a live database
   and can run in any environment, verifying the trigram extraction and
   Jaccard formula match the documented algorithm exactly.
2. A live-fixture replay: if testdata/trgm_similarity_fixture.json exists
   (captured once via verify_backfill_trgm_similarity.py against real
   Postgres), assert exact parity for every pair in it. This is the real
   blocking gate the implementation plan requires before the backfill is
   trusted with production matching decisions -- if the fixture doesn't
   exist yet, this test is skipped with a clear pointer to that script,
   rather than silently passing or failing.
"""

import json
import os

import pytest

from master_candidates.backfill import trgm

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "testdata", "trgm_similarity_fixture.json")


# ─────────────────────────────────────────────────────────────────────────────
# Structural checks against pg_trgm's own documented examples
# ─────────────────────────────────────────────────────────────────────────────
def test_trigram_set_matches_documented_show_trgm_cat():
    # show_trgm('cat') -> {"  c"," ca",cat,"at "}
    assert trgm.trigram_set("cat") == {"  c", " ca", "cat", "at "}


def test_trigram_set_matches_documented_show_trgm_hello():
    # show_trgm('hello') -> {"  h"," he",hel,ell,llo,"lo "}
    assert trgm.trigram_set("hello") == {"  h", " he", "hel", "ell", "llo", "lo "}


def test_identical_strings_give_similarity_one():
    assert trgm.similarity("hello", "hello") == pytest.approx(1.0)


def test_empty_string_gives_zero_not_one_or_error():
    assert trgm.similarity("", "hello") == 0.0
    assert trgm.similarity("hello", "") == 0.0
    assert trgm.similarity("", "") == 0.0


def test_pure_punctuation_gives_zero_trigrams():
    assert trgm.trigram_set("...") == set()
    assert trgm.similarity("...", "hello") == 0.0


def test_words_do_not_bridge_trigrams():
    # "hello world"'s trigram set must equal the union of "hello"'s and
    # "world"'s trigrams computed INDEPENDENTLY (each with its own 2-blank/
    # 1-blank padding). If a trigram bridging the two words existed (e.g.
    # spanning "o", the literal space, and "w"), it would appear in
    # `combined` but in neither individual word's set, breaking this
    # equality -- note " wo" legitimately belongs to "world"'s OWN padding
    # ("  world " -> "  w"," wo",...) and is not itself evidence of bridging.
    combined = trgm.trigram_set("hello world")
    expected = trgm.trigram_set("hello") | trgm.trigram_set("world")
    assert combined == expected
    assert len(combined) == len(trgm.trigram_set("hello")) + len(trgm.trigram_set("world"))


def test_similarity_is_case_sensitive_callers_must_lower_first():
    # similarity() itself does not lowercase -- matches real Postgres,
    # where the SQL always calls similarity(lower(a), lower(b)) explicitly.
    assert trgm.similarity("Cat", "cat") < 1.0
    assert trgm.similarity("cat", "cat") == pytest.approx(1.0)


def test_similarity_is_jaccard_not_dice():
    # "cat" -> {"  c"," ca","cat","at "} (4 trigrams)
    # "cats" -> {"  c"," ca","cat","ats","ts "} (5 trigrams)
    # intersection = {"  c"," ca","cat"} = 3; union = 4+5-3 = 6 -> 3/6 = 0.5
    assert trgm.similarity("cat", "cats") == pytest.approx(0.5, abs=1e-6)


def test_returns_python_float():
    assert isinstance(trgm.similarity("cat", "cats"), float)


# ─────────────────────────────────────────────────────────────────────────────
# Live-fixture replay (blocking gate before production use)
# ─────────────────────────────────────────────────────────────────────────────
def test_fixture_parity_with_live_postgres():
    if not os.path.exists(FIXTURE_PATH):
        pytest.skip(
            "testdata/trgm_similarity_fixture.json not captured yet -- run "
            "verify_backfill_trgm_similarity.py against a live Supabase "
            "instance first. This test MUST pass before the backfill's "
            "matching decisions (resolve_master_candidate T5/T7) are "
            "trusted in production."
        )
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        fixture = json.load(f)

    assert fixture, "fixture file exists but is empty"
    for case in fixture:
        a, b, expected = case["a"], case["b"], case["expected_similarity"]
        actual = trgm.similarity(a.lower(), b.lower())
        assert actual == pytest.approx(expected, abs=1e-6), (
            f"trigram similarity mismatch for ({a!r}, {b!r}): "
            f"python={actual!r} postgres={expected!r}"
        )
