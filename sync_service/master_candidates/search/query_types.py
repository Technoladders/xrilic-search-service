"""
sync_service/master_candidates/search/query_types.py

Canonical types the query planner operates on. Kept dependency-free
(dataclasses only) so skill_logic.py / keyword_query.py / ranking.py can be
unit-tested without importing FastAPI, httpx, or hitting Typesense.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class SkillCriteria:
    must: list[str]
    nice: list[str]
    exclude: list[str]


# ── Keyword Boolean AST ─────────────────────────────────────────────────────
# Precedence (highest to lowest): NOT > AND > OR. Parentheses override.

@dataclass(frozen=True)
class TermNode:
    text: str


@dataclass(frozen=True)
class PhraseNode:
    text: str  # already de-quoted


@dataclass(frozen=True)
class NotNode:
    child: "KeywordNode"


@dataclass(frozen=True)
class AndNode:
    children: tuple["KeywordNode", ...]


@dataclass(frozen=True)
class OrNode:
    children: tuple["KeywordNode", ...]


KeywordNode = Union[TermNode, PhraseNode, NotNode, AndNode, OrNode]


@dataclass(frozen=True)
class SearchPlan:
    keyword_ast: "KeywordNode | None"   # None for empty/"*" keyword
    skills: SkillCriteria
    inclusion_filter_by: str            # "" if neither must nor nice set
    exclude_filter_by: str              # "" if no exclude chips
    other_hard_filter_by: str           # titles/employer/location/education/experience/activity/contact


class KeywordSyntaxError(ValueError):
    """Raised for a malformed keyword Boolean expression. Caller (search_api.py)
    turns this into an HTTP 400 — never silently falls back to a literal search."""
