"""
sync_service/master_candidates/backfill/trgm.py

Pure Python port of Postgres pg_trgm's similarity(text, text) -> real.

Deliberately dependency-free (no rapidfuzz, no numpy) -- fuzzy-matching
libraries like rapidfuzz implement Levenshtein/other edit-distance
algorithms, NOT trigram Jaccard similarity, and would not reproduce
Postgres's actual matching decisions. This module exists specifically to be
bit-for-bit parity-checked against real Postgres output (see
verify_backfill_trgm_similarity.py / test_backfill_trgm.py, both flat under
sync_service/master_candidates/) BEFORE it is trusted for any production
matching decision -- resolve_master_candidate's T5/T7 tiers gate whether a
row auto-merges into an existing master, gets queued for human review, or
becomes a brand-new master, so a silently wrong similarity score here is a
data-quality regression, not just a cosmetic bug.

Algorithm, precisely (not approximated):
  - Words are maximal runs of alphanumeric characters (Unicode-aware).
    Everything else -- spaces, punctuation, hyphens, apostrophes -- is a
    pure separator and never appears inside a trigram.
  - Each word is padded INDEPENDENTLY with 2 leading blanks + 1 trailing
    blank ("  " + word + " "). Trigrams never span two different words --
    "hello world" produces the union of "hello"'s trigrams and "world"'s
    trigrams computed separately, with no trigram bridging the gap between
    them (confirmed against documented show_trgm() examples: "cat" ->
    padded "  cat " -> {"  c"," ca","cat","at "}).
  - The trigram signature of a string is a SET (deduplicated), not a
    multiset -- repeated trigrams within one string count once.
  - similarity(a, b) = |trigrams(a) & trigrams(b)| / |trigrams(a) | trigrams(b)|
    (Jaccard, not Dice). Empty on either side -> 0.0, never 1.0 and never
    an error (a name that's pure punctuation yields zero trigrams).
  - similarity() itself is case-SENSITIVE -- the SQL always calls
    similarity(lower(a), lower(b)); callers here must lower() both inputs
    first too, exactly like the SQL call sites do. This module does not
    lower() internally on purpose, so callers can't accidentally skip it
    without it being visible at the call site.
  - Postgres's similarity() returns `real` (IEEE-754 single precision), not
    `double precision`. The intersection/union division happens in float4.
    This matters for the >=0.90 / >=0.85 threshold comparisons in
    resolve_master_candidate: a true ratio of exactly 9/10 rounds in
    float32 to slightly LESS than the decimal literal 0.90, while a naive
    Python float64 division stays >= 0.9. Without replicating the float32
    rounding step, this port can flip a threshold decision at the boundary
    -- silently changing which rows auto-match vs. queue vs. become new.
    _as_pg_real() below performs that rounding explicitly via a
    pack/unpack round-trip through IEEE-754 single precision.
"""

import re
import struct

_WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)


def _word_trigrams(word: str) -> set[str]:
    padded = "  " + word + " "
    return {padded[i:i + 3] for i in range(len(padded) - 2)}


def trigram_set(s: str) -> set[str]:
    """Full trigram signature of a string, unioned across all its words."""
    if not s:
        return set()
    trigrams: set[str] = set()
    for word in _WORD_RE.findall(s):
        trigrams |= _word_trigrams(word)
    return trigrams


def _as_pg_real(x: float) -> float:
    """Round a Python float64 down to IEEE-754 single precision, matching
    the precision Postgres's `real`-returning similarity() computes in.
    Required for threshold comparisons to agree with Postgres exactly at
    boundary values, not just approximately."""
    return struct.unpack("f", struct.pack("f", x))[0]


def similarity(a: str, b: str) -> float:
    """Faithful port of pg_trgm's similarity(text, text). Case-sensitive,
    matching real Postgres behavior -- callers must lower() both inputs
    first, exactly like the SQL's `similarity(lower(a), lower(b))` calls."""
    ta, tb = trigram_set(a), trigram_set(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta) + len(tb) - inter
    if union == 0:
        return 0.0
    return _as_pg_real(inter / union)
