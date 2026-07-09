"""
sync_service/master_candidates/typesense_client.py

Thin async wrapper over Typesense HTTP API for the master_candidates_v1
collection. Handles: schema bootstrap, upserts, deletes, search, synonyms.
"""

import logging
from typing import Any

import httpx

from .config import (
    TYPESENSE_BASE, TS_HEADERS, TS_COLLECTION,
    SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_TYPESENSE, HTTP_TIMEOUT_SUPABASE,
)

logger = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────────
# Field weights and facet choices reflect the top-5 filter set: skills, titles,
# location, education, companies. Rich JSON (experience, education arrays,
# emails, phones) is deliberately NOT stored here — it's fetched from Supabase
# when a candidate detail view is opened.
COLLECTION_SCHEMA: dict[str, Any] = {
    "name": TS_COLLECTION,
    "enable_nested_fields": False,
    "default_sorting_field": "data_freshness_ts",
    "token_separators": [".", "-", "/", "+", "(", ")", ",", "|"],
    "symbols_to_index":  ["#"],
    "fields": [
        # ── identity / display ─────────────────────────────────────────
        {"name": "id",                    "type": "string"},
        {"name": "full_name",             "type": "string"},
        {"name": "title",                 "type": "string"},
        {"name": "headline",              "type": "string", "optional": True},
        {"name": "summary_short",         "type": "string", "optional": True},

        # ── searchable text (weighted via query_by_weights) ────────────
        {"name": "skills_text",           "type": "string"},
        {"name": "all_titles_text",       "type": "string", "optional": True},
        {"name": "all_employers_text",    "type": "string", "optional": True},
        {"name": "schools_text",          "type": "string", "optional": True},
        {"name": "degrees_text",          "type": "string", "optional": True},
        {"name": "fields_of_study_text",  "type": "string", "optional": True},
        {"name": "certifications_text",   "type": "string", "optional": True},

        # ── structured filters ────────────────────────────────────────
        {"name": "skills",                "type": "string[]"},
        {"name": "all_titles",            "type": "string[]", "optional": True},
        {"name": "all_employers",         "type": "string[]", "optional": True},
        {"name": "current_employer",      "type": "string",   "optional": True},
        {"name": "location",              "type": "string",   "optional": True},
        {"name": "preferred_locations",   "type": "string[]", "optional": True},
        {"name": "schools",               "type": "string[]", "optional": True},
        {"name": "degrees",               "type": "string[]", "optional": True},
        {"name": "fields_of_study",       "type": "string[]", "optional": True},

        # ── facets (memory-conscious — only the 6 that pay for themselves) ─
        {"name": "has_full_profile", "type": "bool",     "facet": True, "optional": True},
        {"name": "has_contact",      "type": "bool",     "facet": True, "optional": True},
        {"name": "sources",          "type": "string[]", "facet": True, "optional": True},
        {"name": "primary_source",   "type": "string",   "facet": True, "optional": True},
        {"name": "seniority",        "type": "string",   "facet": True, "optional": True},
        {"name": "country",          "type": "string",   "facet": True, "optional": True},

        # ── ranges (numeric filters + sort) ────────────────────────────
        {"name": "total_experience_months", "type": "int32", "optional": True},
        {"name": "current_ctc_lacs",        "type": "float", "optional": True},
        {"name": "expected_ctc_lacs",       "type": "float", "optional": True},
        {"name": "notice_period_days",      "type": "int32", "optional": True},
        {"name": "followers",               "type": "int32", "optional": True},
        {"name": "data_freshness_ts",       "type": "int64"},         # unix seconds
        {"name": "last_active_date_ts",     "type": "int64", "optional": True},

        # ── retrieve-only display fields ───────────────────────────────
        {"name": "linkedin_url",         "type": "string", "optional": True, "index": False},
        {"name": "profile_picture_url",  "type": "string", "optional": True, "index": False},
        {"name": "current_ctc_display",  "type": "string", "optional": True, "index": False},
        {"name": "experience_display",   "type": "string", "optional": True, "index": False},
        {"name": "notice_period_display","type": "string", "optional": True, "index": False},
        {"name": "languages",             "type": "string[]", "optional": True, "index": False},
        {"name": "last_active_date",      "type": "string",   "optional": True, "index": False},
        {"name": "contact_personal_email","type": "bool",     "optional": True, "index": False},
        {"name": "contact_phone",         "type": "bool",     "optional": True, "index": False},
        {"name": "summary_full",          "type": "string",   "optional": True, "index": False},
        {"name": "experience_json",        "type": "string",   "optional": True, "index": False},
        {"name": "education_json",         "type": "string",   "optional": True, "index": False},
        {"name": "certifications_json",    "type": "string",   "optional": True, "index": False},
        {"name": "emails_json",            "type": "string",   "optional": True, "index": False},
        {"name": "phones_json",            "type": "string",   "optional": True, "index": False},
    ],
}

# Query weights: skills > title > name > employer > everything else
QUERY_BY = ",".join([
    "full_name", "title", "skills_text", "all_titles_text",
    "current_employer", "all_employers_text",
    "headline", "summary_short",
    "schools_text", "degrees_text", "fields_of_study_text",
    "certifications_text",
])
QUERY_BY_WEIGHTS = ",".join([
    "5", "4", "5", "3",   # name, title, skills, all_titles
    "3", "2",             # current_employer, all_employers
    "2", "1",             # headline, summary
    "2", "2", "1", "1",   # schools, degrees, fields, certs
])

NEW_FIELDS = [
    {"name": "languages",              "type": "string[]", "optional": True, "index": False},
    {"name": "last_active_date",       "type": "string",   "optional": True, "index": False},
    {"name": "contact_personal_email", "type": "bool",     "optional": True, "index": False},
    {"name": "contact_phone",          "type": "bool",     "optional": True, "index": False},
    {"name": "summary_full",           "type": "string",   "optional": True, "index": False},
    {"name": "experience_json",        "type": "string",   "optional": True, "index": False},
    {"name": "education_json",         "type": "string",   "optional": True, "index": False},
    {"name": "certifications_json",    "type": "string",   "optional": True, "index": False},
    {"name": "emails_json",            "type": "string",   "optional": True, "index": False},
    {"name": "phones_json",            "type": "string",   "optional": True, "index": False},
]

async def ensure_fields(client: httpx.AsyncClient) -> None:
    """Alter existing collection to add any missing retrieve-only fields."""
    r = await client.get(f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}",
                         headers=TS_HEADERS, timeout=HTTP_TIMEOUT_TYPESENSE)
    r.raise_for_status()
    existing = {f["name"] for f in r.json().get("fields", [])}
    missing = [f for f in NEW_FIELDS if f["name"] not in existing]
    if not missing:
        return
    r = await client.patch(
        f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}",
        headers=TS_HEADERS, json={"fields": missing},
        timeout=60.0,   # alter walks existing docs; give it time
    )
    r.raise_for_status()
    logger.info(f"typesense: added fields {[f['name'] for f in missing]}")


async def ensure_collection(client: httpx.AsyncClient) -> None:
    """Create the collection if it doesn't exist. Never modify an existing one."""
    r = await client.get(
        f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}",
        headers=TS_HEADERS, timeout=HTTP_TIMEOUT_TYPESENSE,
    )
    if r.status_code == 200:
        logger.info(f"typesense collection '{TS_COLLECTION}' already exists")
        return
    if r.status_code != 404:
        r.raise_for_status()

    logger.info(f"typesense: creating collection '{TS_COLLECTION}'")
    r = await client.post(
        f"{TYPESENSE_BASE}/collections",
        headers=TS_HEADERS, json=COLLECTION_SCHEMA,
        timeout=HTTP_TIMEOUT_TYPESENSE,
    )
    r.raise_for_status()


async def upsert_batch(client: httpx.AsyncClient, docs: list[dict]) -> tuple[int, list[str]]:
    """Upsert a batch of documents. Returns (success_count, error_messages)."""
    if not docs:
        return 0, []
    body = "\n".join([__import__("json").dumps(d) for d in docs])
    r = await client.post(
        f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/import",
        params={"action": "upsert"},
        headers={**TS_HEADERS, "Content-Type": "text/plain"},
        content=body, timeout=HTTP_TIMEOUT_TYPESENSE,
    )
    r.raise_for_status()
    ok, errors = 0, []
    for line in r.text.strip().split("\n"):
        try:
            result = __import__("json").loads(line)
            if result.get("success"):
                ok += 1
            else:
                errors.append(str(result)[:200])
        except Exception:
            errors.append(line[:200])
    return ok, errors


async def delete_id(client: httpx.AsyncClient, doc_id: str) -> None:
    r = await client.delete(
        f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/documents/{doc_id}",
        headers=TS_HEADERS, timeout=HTTP_TIMEOUT_TYPESENSE,
    )
    if r.status_code not in (200, 404):
        r.raise_for_status()


# ── Synonyms sync (Supabase mc_synonyms → Typesense) ──────────────────────
async def sync_synonyms(client: httpx.AsyncClient) -> int:
    """Load all synonyms from mc_synonyms table and upsert to Typesense."""
    r = await client.get(
        f"{SUPABASE_REST}/mc_synonyms",
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()
    rows = r.json()

    count = 0
    for row in rows:
        body: dict[str, Any] = {"synonyms": row["synonyms"]}
        if row["synonym_type"] == "oneway" and row.get("root"):
            body["root"] = row["root"]
        try:
            resp = await client.put(
                f"{TYPESENSE_BASE}/collections/{TS_COLLECTION}/synonyms/{row['id']}",
                headers=TS_HEADERS, json=body,
                timeout=HTTP_TIMEOUT_TYPESENSE,
            )
            resp.raise_for_status()
            count += 1
        except Exception as e:
            logger.warning(f"synonym {row['id']} sync failed: {e}")
    return count