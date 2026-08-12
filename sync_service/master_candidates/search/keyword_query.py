"""
sync_service/master_candidates/search/keyword_query.py

Real Boolean parsing for the `keyword` search box (AND/OR/NOT, parentheses,
quoted phrases), because Typesense's `q` parameter has NO native AND/OR/
parenthesization — confirmed against Typesense's own documentation during
planning: the only native operator is a bare `-word` exclude prefix, plus
genuine phrase-adjacency matching for quoted substrings. Everything else
about multi-token `q` behavior is relevance ranking with progressive
token-dropping, not guaranteed Boolean logic. A prior version of this
backend passed the raw keyword string straight into `q` and appeared to
work by coincidence (common English words like "AND"/"NOT" happened to
overlap with real query tokens) — that is not real Boolean search and is
replaced here.

Two evaluation tiers, and the distinction is load-bearing, not cosmetic:

  Tier A (EXACT) — the AST has no OrNode, and every NotNode wraps a single
  term/phrase (not a group). Translates directly to ONE native Typesense
  call: an AND-chain of bare terms becomes `q = "a b c"` with
  `drop_tokens_threshold=0` (forces all tokens required instead of the
  default progressive relaxation), and `NOT term` becomes a native
  `-term` prefix. `found` from that single call is Typesense's own exact
  count — total/pagination are exact.

  Tier B (BOUNDED) — anything with an OrNode, or a NOT applied to a group.
  Typesense cannot compute an exact cross-query union/intersection count
  natively, so this evaluates each sub-expression via its own bounded pool
  (reusing ranking.fetch_bounded_pool — the same honest "capped depth, but
  never a fabricated total" pattern already used for nice-skill ranking)
  and combines candidate IDs in Python (union for OR, intersection for AND
  with a non-exact child, complement-within-pool for NOT-of-group). The
  resulting total is the count of unique IDs actually observed across the
  fetched pools — an honest, possibly-conservative lower bound, NEVER
  presented as equal to a single Typesense `found`. `capped=True` whenever
  any branch's own true `found` exceeds its pool cap.

Malformed expressions (dangling AND/OR/NOT, unmatched parens, empty
groups) raise KeywordSyntaxError — the caller (search_api.py) turns this
into an HTTP 400. This never silently falls back to a confusing literal
search.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .query_types import (
    AndNode, KeywordNode, KeywordSyntaxError, NotNode, OrNode, PhraseNode, TermNode,
)
from .ranking import TsSearchFn, fetch_bounded_pool

_OPERATORS = {"AND", "OR", "NOT"}
_TOKEN_RE = re.compile(r'"[^"]*"|\(|\)|[^\s()]+')


# ── Tokenizer ────────────────────────────────────────────────────────────────

def _tokenize(expr: str) -> list[str]:
    if expr.count('"') % 2 != 0:
        raise KeywordSyntaxError("unterminated quoted phrase")
    return _TOKEN_RE.findall(expr)


# ── Recursive-descent parser — precedence NOT > AND > OR, parens override ──

class _Parser:
    def __init__(self, tokens: list[str]):
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _advance(self) -> str:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def parse(self) -> KeywordNode:
        node = self._parse_or()
        if self.pos != len(self.tokens):
            raise KeywordSyntaxError(f"unexpected token {self._peek()!r}")
        return node

    def _parse_or(self) -> KeywordNode:
        children = [self._parse_and()]
        while self._peek() is not None and self._peek().upper() == "OR":
            self._advance()
            children.append(self._parse_and())
        return children[0] if len(children) == 1 else OrNode(tuple(children))

    def _parse_and(self) -> KeywordNode:
        children = [self._parse_not()]
        while True:
            tok = self._peek()
            if tok is None or tok == ")" or tok.upper() == "OR":
                break
            if tok.upper() == "AND":
                self._advance()
                children.append(self._parse_not())
            else:
                # implicit AND between adjacent terms/groups (e.g. "java react")
                children.append(self._parse_not())
        return children[0] if len(children) == 1 else AndNode(tuple(children))

    def _parse_not(self) -> KeywordNode:
        if self._peek() is not None and self._peek().upper() == "NOT":
            self._advance()
            return NotNode(self._parse_not())
        return self._parse_primary()

    def _parse_primary(self) -> KeywordNode:
        tok = self._peek()
        if tok is None:
            raise KeywordSyntaxError("unexpected end of expression")
        if tok == "(":
            self._advance()
            node = self._parse_or()
            if self._peek() != ")":
                raise KeywordSyntaxError("expected closing ')'")
            self._advance()
            return node
        if tok == ")":
            raise KeywordSyntaxError("unexpected ')'")
        if tok.upper() in _OPERATORS:
            raise KeywordSyntaxError(f"unexpected operator {tok!r}")
        self._advance()
        if tok.startswith('"'):
            phrase = tok[1:-1].strip()
            if not phrase:
                raise KeywordSyntaxError("empty quoted phrase")
            return PhraseNode(phrase)
        return TermNode(tok)


def parse(expr: str) -> KeywordNode | None:
    """None for an empty/whitespace-only expression (equivalent to no keyword)."""
    text = (expr or "").strip()
    if not text:
        return None
    tokens = _tokenize(text)
    if not tokens:
        return None
    return _Parser(tokens).parse()


# ── Tier classification + Tier-A compilation ────────────────────────────────

def is_exact(node: KeywordNode) -> bool:
    """True iff this AST can be answered by ONE native Typesense query."""
    if isinstance(node, (TermNode, PhraseNode)):
        return True
    if isinstance(node, NotNode):
        return isinstance(node.child, (TermNode, PhraseNode)) and is_exact(node.child)
    if isinstance(node, AndNode):
        return all(is_exact(c) for c in node.children)
    if isinstance(node, OrNode):
        return False
    raise TypeError(f"unhandled node type: {node!r}")


def compile_exact_query(node: KeywordNode) -> str:
    """Tier-A only — raises if called on a non-exact node."""
    if isinstance(node, TermNode):
        return node.text
    if isinstance(node, PhraseNode):
        return f'"{node.text}"'
    if isinstance(node, NotNode):
        return f"-{compile_exact_query(node.child)}"
    if isinstance(node, AndNode):
        return " ".join(compile_exact_query(c) for c in node.children)
    raise TypeError(f"cannot compile non-exact node as a single query: {node!r}")


def plan_keyword(node: KeywordNode | None) -> tuple[str, str | None]:
    """Returns (tier, exact_q). tier is 'empty' | 'A' | 'B'. exact_q is set only for 'A'."""
    if node is None:
        return "empty", None
    if is_exact(node):
        return "A", compile_exact_query(node)
    return "B", None


# ── Tier-B bounded evaluator ─────────────────────────────────────────────────

@dataclass
class _PoolResult:
    hits_by_id: dict[str, dict]
    observed_count: int   # len(hits_by_id) — the honest, possibly-truncated pool size actually fetched
    capped: bool


@dataclass(frozen=True)
class BoundedEvalContext:
    ts_search: TsSearchFn
    base_params: dict[str, Any]      # query_by/weights/exclude_fields/etc — no q, no filter_by
    structured_filter_by: str        # skill + other hard filters, ANDed onto every leaf query
    pool_cap: int
    page_size: int


async def _eval_node(node: KeywordNode, ctx: BoundedEvalContext) -> _PoolResult:
    if is_exact(node):
        q = compile_exact_query(node)
        params = {**ctx.base_params, "q": q, "drop_tokens_threshold": 0}
        if ctx.structured_filter_by:
            params["filter_by"] = ctx.structured_filter_by
        hits, found, _facets, _took = await fetch_bounded_pool(
            ctx.ts_search, params, ctx.pool_cap, ctx.pool_cap, ctx.page_size,
        )
        hits_by_id = {h["document"]["id"]: h for h in hits}
        # `found` is Typesense's own exact count for this leaf — used ONLY to
        # decide `capped`. The reported count is len(hits_by_id): the pool
        # actually fetched, which honestly reflects Tier B's total contract
        # even when a single Typesense page (page_size > pool_cap) happens to
        # return the leaf's complete match set despite found > pool_cap.
        return _PoolResult(hits_by_id, len(hits_by_id), found > ctx.pool_cap)

    if isinstance(node, OrNode):
        merged: dict[str, dict] = {}
        capped = False
        for child in node.children:
            sub = await _eval_node(child, ctx)
            merged.update(sub.hits_by_id)
            capped = capped or sub.capped
        return _PoolResult(merged, len(merged), capped)

    if isinstance(node, NotNode):
        # NOT applied to a GROUP (leaf NOT was already handled by is_exact above).
        inner = await _eval_node(node.child, ctx)
        params = {**ctx.base_params, "q": "*"}
        if ctx.structured_filter_by:
            params["filter_by"] = ctx.structured_filter_by
        hits, found, _facets, _took = await fetch_bounded_pool(
            ctx.ts_search, params, ctx.pool_cap, ctx.pool_cap, ctx.page_size,
        )
        hits_by_id = {
            h["document"]["id"]: h for h in hits
            if h["document"]["id"] not in inner.hits_by_id
        }
        return _PoolResult(hits_by_id, len(hits_by_id), True)

    if isinstance(node, AndNode):
        # Only reached when at least one child is non-exact (else is_exact(node) was True).
        subs = [await _eval_node(c, ctx) for c in node.children]
        common_ids = set(subs[0].hits_by_id)
        for s in subs[1:]:
            common_ids &= set(s.hits_by_id)
        hits_by_id = {}
        for i in common_ids:
            for s in subs:
                if i in s.hits_by_id:
                    hits_by_id[i] = s.hits_by_id[i]
                    break
        return _PoolResult(hits_by_id, len(hits_by_id), any(s.capped for s in subs))

    raise TypeError(f"unhandled node type: {node!r}")


@dataclass(frozen=True)
class BoundedEvalResult:
    hits: list[dict]
    total: int        # count of unique IDs actually observed — honest lower bound, not a Typesense `found`
    capped: bool


async def evaluate_bounded(
    node: KeywordNode,
    *,
    ts_search: TsSearchFn,
    base_params: dict[str, Any],
    structured_filter_by: str,
    pool_cap: int,
    page_size: int,
) -> BoundedEvalResult:
    ctx = BoundedEvalContext(ts_search, base_params, structured_filter_by, pool_cap, page_size)
    result = await _eval_node(node, ctx)
    return BoundedEvalResult(list(result.hits_by_id.values()), result.observed_count, result.capped)
