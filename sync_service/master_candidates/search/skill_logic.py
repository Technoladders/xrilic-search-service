"""
sync_service/master_candidates/search/skill_logic.py

Owns MUST/NICE/EXCLUDE Boolean semantics for the `skills` string[] field.

Confirmed inclusion logic (explicit product decision — see the
implementation plan, "Fix NICE semantics"):

    MUST only    -> ALL must skills required        skills:=m1 && skills:=m2 ...
    NICE only    -> ANY nice skill required          skills:=[n1,n2,...]
    MUST + NICE  -> (ALL must) OR (ANY nice)         (must-clause) || (nice-clause)

This is a deliberate change from the pre-existing behavior, which ANDed the
nice-clause onto must/exclude (making "nice" skills silently mandatory-OR
instead of a ranking preference). EXCLUDE is always a separate, always-
applied hard NOT, ANDed onto the outside of whichever inclusion branch a
candidate qualifies through — a candidate can never bypass EXCLUDE by
matching only the NICE branch.
"""
from __future__ import annotations

from .query_types import SkillCriteria


def _escape(v: str) -> str:
    """Escape a value for filter_by. Backtick-wrap and escape backticks."""
    return "`" + v.replace("`", "\\`") + "`"


def normalize_skill(label: str) -> str:
    """
    Lowercase + trim a chip label before it's used in a filter_by clause.

    Indexed `skills` values are stored with zero normalization (see
    indexer.py's transform_row()), so this only fixes a case mismatch
    between what a recruiter types and what's indexed IF Typesense's `:=`
    filter is confirmed case-insensitive on this field (unresolved — see
    verify_typesense_semantics.py). If the filter turns out to be
    case-sensitive, normalizing only the query side is not sufficient to
    match differently-cased indexed values; that would require also
    normalizing at index time (a reindex), which is explicitly out of scope
    for this change. Applied unconditionally regardless, since it's a safe
    no-op-or-fix and never a regression on its own.
    """
    return label.strip().lower()


def extract_skill_chips(filters: dict) -> SkillCriteria:
    """(must, nice, exclude) labels from filters.skillChips, normalized."""
    chips = filters.get("skillChips") or []
    must = [normalize_skill(c["label"]) for c in chips
            if c.get("mode") == "must" and c.get("label")]
    nice = [normalize_skill(c["label"]) for c in chips
            if c.get("mode") == "nice" and c.get("label")]
    exclude = [normalize_skill(c["label"]) for c in chips
               if c.get("mode") == "exclude" and c.get("label")]
    return SkillCriteria(must=must, nice=nice, exclude=exclude)


def _must_clause(values: list[str]) -> str | None:
    if not values:
        return None
    return " && ".join(f"skills:={_escape(v)}" for v in values)


def _nice_clause(values: list[str]) -> str | None:
    if not values:
        return None
    return "skills:=[" + ",".join(_escape(v) for v in values) + "]"


def build_inclusion_filter(skills: SkillCriteria) -> str | None:
    """
    (ALL must) OR (ANY nice) when both are set; whichever side alone when
    only one is set; None when neither is set (no inclusion constraint).
    """
    must_clause = _must_clause(skills.must)
    nice_clause = _nice_clause(skills.nice)
    if must_clause and nice_clause:
        return f"({must_clause}) || ({nice_clause})"
    return must_clause or nice_clause


def build_exclude_filter(skills: SkillCriteria) -> str | None:
    if not skills.exclude:
        return None
    return " && ".join(f"skills:!={_escape(v)}" for v in skills.exclude)


def nice_match_count(skills_field: list[str] | None, nice: list[str]) -> int:
    """Case-insensitive nice-skill match count on one candidate doc — ranking only."""
    if not nice:
        return 0
    doc_skills = {str(s).lower() for s in (skills_field or [])}
    nice_lower = {s.lower() for s in nice}  # nice is already normalize_skill()'d; .lower() here is a no-op safety net
    return len(doc_skills & nice_lower)
