"""
indexer.py

Manages the Typesense 'candidates' collection.
  - Schema definition
  - Document transformation (Supabase row → Typesense doc)
  - Upsert / delete / bulk-import operations
"""

import json
import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("indexer")

# ── Collection schema ─────────────────────────────────────────────────────────
COLLECTION_SCHEMA = {
    "name": "candidates",
    "fields": [
        {"name": "id",                   "type": "string"},
        {"name": "organization_id",      "type": "string", "facet": True},
        {"name": "full_name",            "type": "string"},
        {"name": "email",                "type": "string", "optional": True},
        {"name": "phone",                "type": "string", "optional": True},
        # ── Role / Title (highest weight in search) ──────────────────────────
        {"name": "suggested_title",      "type": "string", "optional": True},
        {"name": "current_designation",  "type": "string", "optional": True, "facet": True},
        {"name": "current_company",      "type": "string", "optional": True, "facet": True},
        {"name": "previous_designation", "type": "string", "optional": True},
        {"name": "previous_company",     "type": "string", "optional": True},
        # ── Location ─────────────────────────────────────────────────────────
        {"name": "current_location",     "type": "string", "optional": True, "facet": True},
        # ── Skills ───────────────────────────────────────────────────────────
        # Array of skill name strings — facetable for exact filter
        {"name": "skills",               "type": "string[]", "optional": True, "facet": True},
        # ── Experience / Compensation ─────────────────────────────────────────
        {"name": "exp_years",            "type": "int32",   "optional": True},
        {"name": "current_ctc",          "type": "float",   "optional": True},
        {"name": "expected_ctc",         "type": "float",   "optional": True},
        # ── Notice / Availability ─────────────────────────────────────────────
        {"name": "notice_period",        "type": "string",  "optional": True, "facet": True},
        # ── Education ────────────────────────────────────────────────────────
        {"name": "education_summary",    "type": "string",  "optional": True},
        # ── Body text (lowest weight — resume snippet for keyword boost) ─────
        # Keep short to reduce RAM — first 2000 chars of resume_text
        # NOT stored, only indexed
        {"name": "resume_snippet",       "type": "string",  "optional": True, "index": True, "store": False},
        # ── Sorting ───────────────────────────────────────────────────────────
        {"name": "created_at_ts",        "type": "int64"},
    ],
    "default_sorting_field": "created_at_ts",
    # Token separators allow "C++" "C#" to be found
    "token_separators": ["+", "#", "."],
}

# Fields searched in order of importance — weights control ranking
QUERY_BY_FIELDS  = "suggested_title,current_designation,current_company,skills,education_summary,resume_snippet"
QUERY_BY_WEIGHTS = "10,9,5,4,2,1"


class CandidateIndexer:
    def __init__(self, host: str, port: int, api_key: str):
        self.base_url = f"http://{host}:{port}"
        self.headers = {
            "X-TYPESENSE-API-KEY": api_key,
            "Content-Type": "application/json",
        }

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{self.base_url}/health", headers=self.headers)
                return r.status_code == 200
        except Exception:
            return False

    async def ensure_collection(self):
        """Create collection if it doesn't exist. Never drops existing data."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{self.base_url}/collections/candidates",
                headers=self.headers,
            )
            if r.status_code == 200:
                logger.info("Collection 'candidates' already exists.")
                return

            if r.status_code == 404:
                cr = await c.post(
                    f"{self.base_url}/collections",
                    headers=self.headers,
                    content=json.dumps(COLLECTION_SCHEMA),
                )
                cr.raise_for_status()
                logger.info("Collection 'candidates' created.")
                return

            r.raise_for_status()

    async def get_stats(self) -> dict:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{self.base_url}/collections/candidates",
                headers=self.headers,
            )
            if r.status_code == 200:
                data = r.json()
                return {
                    "num_documents": data.get("num_documents", 0),
                    "name": data.get("name"),
                }
            return {"error": r.text}

    # ── Document transform ────────────────────────────────────────────────────

    def transform_record(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert a hr_talent_pool row into a Typesense document.
        Returns None if the row is missing required fields.
        """
        if not row.get("id") or not row.get("organization_id"):
            return None

        # ── Skills: extract from top_skills JSONB ────────────────────────────
        # top_skills can be:
        #   ["React", "Python"]          (already an array of strings)
        #   [{"name": "React"}, ...]     (array of objects)
        #   null
        skills: List[str] = []
        raw_skills = row.get("top_skills")
        if isinstance(raw_skills, list):
            for s in raw_skills:
                if isinstance(s, str) and s.strip():
                    skills.append(s.strip().lower())
                elif isinstance(s, dict):
                    name = s.get("name") or s.get("skill_name") or ""
                    if name.strip():
                        skills.append(name.strip().lower())
        elif isinstance(raw_skills, str):
            # Sometimes stored as JSON string
            try:
                parsed = json.loads(raw_skills)
                if isinstance(parsed, list):
                    for s in parsed:
                        v = s if isinstance(s, str) else (s.get("name") or "")
                        if v.strip():
                            skills.append(v.strip().lower())
            except Exception:
                pass

        # Deduplicate
        skills = list(dict.fromkeys(skills))

        # ── Work experience fields ────────────────────────────────────────────
        we = row.get("work_experience") or []
        if isinstance(we, str):
            try:
                we = json.loads(we)
            except Exception:
                we = []

        current_designation = row.get("current_designation") or ""
        current_company     = row.get("current_company") or ""
        previous_designation = ""
        previous_company     = ""

        if isinstance(we, list) and len(we) > 0:
            if not current_designation:
                current_designation = (we[0].get("designation") or "").strip()
            if not current_company:
                current_company = (we[0].get("company") or "").strip()
            if len(we) > 1:
                previous_designation = (we[1].get("designation") or "").strip()
                previous_company     = (we[1].get("company") or "").strip()

        # ── Education ─────────────────────────────────────────────────────────
        edu = row.get("education") or []
        if isinstance(edu, str):
            try:
                edu = json.loads(edu)
            except Exception:
                edu = []
        education_summary = ""
        if isinstance(edu, list) and len(edu) > 0:
            education_summary = (edu[0].get("degree") or "").strip()

        # ── Resume snippet (first 2000 chars) ────────────────────────────────
        resume_text = row.get("resume_text") or ""
        resume_snippet = resume_text[:2000].strip()

        # ── Timestamp ─────────────────────────────────────────────────────────
        created_at_str = row.get("created_at") or "2020-01-01T00:00:00+00:00"
        try:
            from datetime import datetime, timezone
            if "+" in created_at_str or "Z" in created_at_str:
                dt = datetime.fromisoformat(
                    created_at_str.replace("Z", "+00:00")
                )
            else:
                dt = datetime.fromisoformat(created_at_str).replace(
                    tzinfo=timezone.utc
                )
            created_at_ts = int(dt.timestamp())
        except Exception:
            created_at_ts = 0

        # ── Build document ────────────────────────────────────────────────────
        doc: Dict[str, Any] = {
            "id":                   str(row["id"]),
            "organization_id":      str(row["organization_id"]),
            "full_name":            (row.get("candidate_name") or "").strip(),
            "created_at_ts":        created_at_ts,
        }

        # Optional fields — only include if non-empty
        def _set(key: str, val: Any):
            if val and str(val).strip():
                doc[key] = val

        _set("email",               row.get("email"))
        _set("phone",               row.get("phone"))
        _set("suggested_title",     row.get("suggested_title"))
        _set("current_designation", current_designation)
        _set("current_company",     current_company)
        _set("previous_designation", previous_designation)
        _set("previous_company",    previous_company)
        _set("current_location",    row.get("current_location"))
        _set("notice_period",       row.get("notice_period"))
        _set("education_summary",   education_summary)
        _set("resume_snippet",      resume_snippet)

        if skills:
            doc["skills"] = skills

        # Numeric fields
        exp = row.get("parsed_experience_years")
        if exp is not None:
            try:
                doc["exp_years"] = int(exp)
            except (TypeError, ValueError):
                pass

        ctc = row.get("parsed_current_ctc")
        if ctc is not None:
            try:
                doc["current_ctc"] = float(ctc)
            except (TypeError, ValueError):
                pass

        exp_ctc = row.get("parsed_expected_ctc")
        if exp_ctc is not None:
            try:
                doc["expected_ctc"] = float(exp_ctc)
            except (TypeError, ValueError):
                pass

        return doc

    # ── Write operations ──────────────────────────────────────────────────────

    async def upsert_document(self, row: Dict[str, Any]):
        """Upsert a single document (from webhook)."""
        doc = self.transform_record(row)
        if not doc:
            logger.warning(f"Skipping upsert — transform returned None for row {row.get('id')}")
            return

        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"{self.base_url}/collections/candidates/documents?action=upsert",
                headers=self.headers,
                content=json.dumps(doc),
            )
            if r.status_code not in (200, 201):
                logger.error(f"Upsert failed for {doc['id']}: {r.text}")
            else:
                logger.debug(f"Upserted document {doc['id']}")

    async def delete_document(self, doc_id: str):
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.delete(
                f"{self.base_url}/collections/candidates/documents/{doc_id}",
                headers=self.headers,
            )
            if r.status_code not in (200, 404):
                logger.error(f"Delete failed for {doc_id}: {r.text}")

    async def bulk_upsert(self, docs: List[Dict[str, Any]]):
        """
        Import a batch of documents using Typesense's bulk import endpoint.
        Uses JSONL format (one JSON object per line).
        action=upsert → insert new, update existing by 'id'.
        """
        if not docs:
            return

        jsonl = "\n".join(json.dumps(d) for d in docs)

        async with httpx.AsyncClient(timeout=120) as c:
            r = await c.post(
                f"{self.base_url}/collections/candidates/documents/import?action=upsert&batch_size=100",
                headers={**self.headers, "Content-Type": "text/plain"},
                content=jsonl.encode("utf-8"),
            )

            if r.status_code != 200:
                logger.error(f"Bulk upsert failed: {r.status_code} {r.text[:500]}")
                return

            # Parse JSONL response — each line is success/error per doc
            errors = []
            for line in r.text.strip().split("\n"):
                try:
                    result = json.loads(line)
                    if not result.get("success"):
                        errors.append(result)
                except Exception:
                    pass

            if errors:
                logger.warning(f"Bulk upsert: {len(errors)} errors out of {len(docs)} docs")
                for e in errors[:5]:
                    logger.warning(f"  → {e}")
            else:
                logger.info(f"Bulk upsert: {len(docs)} docs OK")