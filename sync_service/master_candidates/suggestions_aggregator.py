"""
sync_service/master_candidates/suggestions_aggregator.py

Populates master_candidate_suggestions_v1 from master_candidates_v1 — a
periodic, OFFLINE batch job (triggered via POST /mc/admin/suggestions/rebuild,
see admin_api.py), never a per-request/hot-path operation.

Design (see implementation plan, Part 2 §2): avoids both anti-patterns
explicitly rejected during planning —
  1. Faceting every dimension on the 1M+ candidate collection just to power
     autocomplete (expensive, and the proposal itself argued against it).
  2. Fetching the full candidate set into Python on every search request
     (the exact pattern already rejected in the MUST/NICE ranking work).
Instead: Typesense's bulk `/documents/export` streams the collection ONCE
per run, scoped via `include_fields` to only the ~15 dimension-relevant
fields (never the 7 future-analytics-only fields — this job only ever reads
what it's about to turn into a live suggestion), and this module aggregates
all 15 dimensions in a SINGLE streaming pass (one document in memory at a
time; only the resulting per-dimension value→count maps grow, which are
orders of magnitude smaller than the raw corpus).
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Iterable, Optional

import httpx

from .config import TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION, HTTP_TIMEOUT_TYPESENSE
from .search.skill_logic import normalize_skill as normalize_value
from .typesense_client import upsert_suggestions_batch

logger = logging.getLogger(__name__)

# Long-tail one-off values aren't useful autocomplete suggestions — capping
# bounds the suggestion collection's size regardless of raw field cardinality.
DEFAULT_CAP_PER_DIMENSION = 2000


# ── Canonicalization (suggestion-collection grouping ONLY — never rewrites
# what's stored in master_candidates_v1 or sent to /mc/search_v2) ──────────
_EMPLOYER_SUFFIX_RE = re.compile(
    r"\s*[,\-]?\s*(private\s+limited|pvt\.?\s*ltd\.?|limited|ltd\.?|llc|inc\.?|corporation|corp\.?)\s*$",
    re.IGNORECASE,
)


def canonicalize_employer(raw: str) -> str:
    """Strips common legal suffixes for grouping (e.g. "Tata Consultancy
    Services Ltd" / "... Limited" -> "Tata Consultancy Services"). Does NOT
    resolve abbreviations (TCS <-> Tata Consultancy Services) — that's the
    existing mc_synonyms/sync_synonyms() alias layer's job, not this."""
    stripped = _EMPLOYER_SUFFIX_RE.sub("", raw).strip()
    return stripped or raw


# ── Dimension specs — all 15 live suggestion dimensions ─────────────────────
@dataclass(frozen=True)
class DimensionSpec:
    type_name: str
    scalar_field: Optional[str] = None
    array_field: Optional[str] = None
    # For previous_title/previous_employer: exclude the doc's own current
    # value from the array tally, so "previous" genuinely means past-only.
    exclude_scalar_field: Optional[str] = None
    canonicalize: Optional[Callable[[str], str]] = None


DIMENSION_SPECS: tuple[DimensionSpec, ...] = (
    DimensionSpec("skill", array_field="skills"),
    DimensionSpec("current_title", scalar_field="title"),
    DimensionSpec("previous_title", array_field="all_titles", exclude_scalar_field="title"),
    DimensionSpec("current_employer", scalar_field="current_employer", canonicalize=canonicalize_employer),
    DimensionSpec("previous_employer", array_field="all_employers",
                  exclude_scalar_field="current_employer", canonicalize=canonicalize_employer),
    DimensionSpec("school", array_field="schools"),
    DimensionSpec("degree", array_field="degrees"),
    DimensionSpec("field_of_study", array_field="fields_of_study"),
    DimensionSpec("language", array_field="languages"),
    # Location suggestions come from the existing single-string location
    # fields (current + preferred) — a structured city/state/country
    # decomposition is a deliberate follow-up (implementation plan), not
    # built here since it needs new indexed fields.
    DimensionSpec("location", scalar_field="location", array_field="preferred_locations"),
    DimensionSpec("industry", scalar_field="industry"),
    DimensionSpec("job_function", scalar_field="job_function"),
    DimensionSpec("functional_area", scalar_field="functional_area"),
    DimensionSpec("company_industry", scalar_field="company_industry"),
    DimensionSpec("seniority", scalar_field="seniority"),
)

# Only these fields are ever requested from the export — deliberately
# excludes the 7 future-analytics-only fields (gender, age_years,
# marital_status, disability, desired_job_type, employment_status_pref,
# work_auth_countries): this job only reads what it turns into a suggestion.
EXPORT_FIELDS: tuple[str, ...] = (
    "id", "skills", "title", "all_titles", "current_employer", "all_employers",
    "schools", "degrees", "fields_of_study", "languages",
    "location", "preferred_locations",
    "industry", "job_function", "functional_area", "company_industry", "seniority",
)


def _bump(counts: dict[str, dict[str, Any]], raw: Any, canonicalize: Optional[Callable[[str], str]]) -> None:
    if not raw:
        return
    display = str(raw).strip()
    if not display:
        return
    if canonicalize:
        display = canonicalize(display)
    norm = normalize_value(display)
    if not norm:
        return
    entry = counts.setdefault(norm, {"display": display, "count": 0})
    entry["count"] += 1
    # Keep the fullest/longest observed display form for this normalized key
    # (e.g. prefer "React.js" or "React Native" over a truncated variant).
    if len(display) > len(entry["display"]):
        entry["display"] = display


def _aggregate_one(doc: dict[str, Any], spec: DimensionSpec, counts: dict[str, dict[str, Any]]) -> None:
    exclude_norm = None
    if spec.exclude_scalar_field:
        ev = doc.get(spec.exclude_scalar_field)
        exclude_norm = normalize_value(str(ev)) if ev else None

    if spec.scalar_field:
        _bump(counts, doc.get(spec.scalar_field), spec.canonicalize)

    if spec.array_field:
        for raw in (doc.get(spec.array_field) or []):
            if exclude_norm is not None and raw:
                if normalize_value(str(raw)) == exclude_norm:
                    continue
            _bump(counts, raw, spec.canonicalize)


@dataclass(frozen=True)
class AggregationReport:
    rows: list[dict[str, Any]]
    per_dimension_distinct_counts: dict[str, int]
    documents_processed: int


async def run_aggregation(
    docs: AsyncIterator[dict[str, Any]],
    cap_per_dimension: int = DEFAULT_CAP_PER_DIMENSION,
) -> AggregationReport:
    """
    Single streaming pass over `docs` (an async iterator of exported
    master_candidates_v1 documents, scoped to EXPORT_FIELDS) — every document
    is run through all 15 dimension specs before being discarded, so this
    never holds more than one raw document in memory at a time. Returns the
    suggestion rows ready to upsert (NOT yet written to Typesense — see
    rebuild_suggestions() below, which is the actual admin-triggered entry
    point that also performs the upsert).
    """
    counts: dict[str, dict[str, dict[str, Any]]] = {spec.type_name: {} for spec in DIMENSION_SPECS}
    processed = 0

    async for doc in docs:
        for spec in DIMENSION_SPECS:
            _aggregate_one(doc, spec, counts[spec.type_name])
        processed += 1

    rows: list[dict[str, Any]] = []
    per_dimension_distinct_counts: dict[str, int] = {}
    for spec in DIMENSION_SPECS:
        dim_counts = counts[spec.type_name]
        per_dimension_distinct_counts[spec.type_name] = len(dim_counts)
        top = sorted(dim_counts.items(), key=lambda kv: -kv[1]["count"])[:cap_per_dimension]
        for norm, stats in top:
            rows.append({
                "id":               f"{spec.type_name}:{norm}",
                "type":             spec.type_name,
                "value":            stats["display"],
                "normalized_value": norm,
                "candidate_count":  stats["count"],
            })

    return AggregationReport(
        rows=rows, per_dimension_distinct_counts=per_dimension_distinct_counts,
        documents_processed=processed,
    )


async def _as_async_iter(iterable: Iterable[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    """Wraps a plain sync iterable (e.g. a test fixture list) as an async
    iterator, so run_aggregation()'s signature stays uniform whether the
    source is a live Typesense export or synthetic test data."""
    for item in iterable:
        yield item


async def export_documents(client: httpx.AsyncClient) -> AsyncIterator[dict[str, Any]]:
    """
    Streams master_candidates_v1 via Typesense's bulk /documents/export,
    scoped to EXPORT_FIELDS only. One HTTP request; Typesense streams
    newline-delimited JSON documents in the response body — this does NOT
    require walking page-by-page like the search-facing pagination code
    elsewhere in this service, and it never touches /mc/search_v2's
    request path.
    """
    url = f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/export"
    params = {"include_fields": ",".join(EXPORT_FIELDS)}
    async with client.stream(
        "GET", url, headers=TS_HEADERS, params=params, timeout=HTTP_TIMEOUT_TYPESENSE,
    ) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                logger.warning("suggestions_aggregator: skipped a malformed export line")
                continue


async def rebuild_suggestions(
    client: httpx.AsyncClient, cap_per_dimension: int = DEFAULT_CAP_PER_DIMENSION,
) -> AggregationReport:
    """
    The actual admin-triggered entry point (POST /mc/admin/suggestions/rebuild):
    export -> aggregate -> upsert. Manual-trigger only in this pass — no
    cron/background loop (see implementation plan) — schedulable later once
    the manual path is verified.
    """
    report = await run_aggregation(export_documents(client), cap_per_dimension)
    if report.rows:
        # Batch upserts the same way indexer.py already does for the main
        # collection (typesense_client.upsert_batch) — reused pattern, new
        # target collection.
        batch_size = 500
        for i in range(0, len(report.rows), batch_size):
            batch = report.rows[i:i + batch_size]
            ok, errors = await upsert_suggestions_batch(client, batch)
            if errors:
                logger.warning(f"suggestions rebuild: {len(errors)} upsert errors in batch {i // batch_size}")
    logger.info(
        f"suggestions rebuild: processed={report.documents_processed} "
        f"rows_upserted={len(report.rows)} per_dimension={report.per_dimension_distinct_counts}"
    )
    return report
