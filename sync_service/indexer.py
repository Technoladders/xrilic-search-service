"""
indexer.py — v2.1 (crash fix)
CRASH FIX: Some JSONB rows store designation/company/degree as a LIST instead of a
string — e.g. ["Executive - Records Management"] — causing AttributeError: 'list'
object has no attribute 'strip'.
Added _to_str() helper used everywhere a string is extracted from JSONB.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("indexer")


# ── Safe JSONB string extractor ────────────────────────────────────────────────
def _to_str(val: Any) -> str:
    """
    Convert a JSONB field value to a clean string.
    Handles: str, list (takes first element), int/float, None.
    This is needed because some malformed records store designation/company/etc
    as ["Value"] (a list) instead of "Value" (a string).
    """
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, (list, tuple)):
        # Take first non-empty element
        for item in val:
            if item is not None:
                s = str(item).strip()
                if s:
                    return s
        return ""
    if val is not None:
        return str(val).strip()
    return ""


# ── Collection schema ──────────────────────────────────────────────────────────
COLLECTION_SCHEMA = {
    "name": "candidates",
    "fields": [
        {"name": "id",                   "type": "string"},
        {"name": "organization_id",      "type": "string", "facet": True},
        {"name": "full_name",            "type": "string"},
        {"name": "email",                "type": "string",   "optional": True},
        {"name": "phone",                "type": "string",   "optional": True},
        # ── Role / Title ─────────────────────────────────────────────────────
        {"name": "suggested_title",      "type": "string",   "optional": True},
        {"name": "current_designation",  "type": "string",   "optional": True, "facet": True},
        {"name": "current_company",      "type": "string",   "optional": True, "facet": True},
        {"name": "previous_designation", "type": "string",   "optional": True},
        {"name": "previous_company",     "type": "string",   "optional": True},
        # ── v2: all past titles/companies ─────────────────────────────────────
        {"name": "previous_titles",      "type": "string[]", "optional": True},
        {"name": "previous_companies",   "type": "string[]", "optional": True},
        # ── Location ─────────────────────────────────────────────────────────
        {"name": "current_location",     "type": "string",   "optional": True, "facet": True},
        # ── Skills ───────────────────────────────────────────────────────────
        {"name": "skills",               "type": "string[]", "optional": True, "facet": True},
        # ── Experience / Compensation ────────────────────────────────────────
        {"name": "exp_years",            "type": "int32",    "optional": True},
        {"name": "current_ctc",          "type": "float",    "optional": True},
        {"name": "expected_ctc",         "type": "float",    "optional": True},
        # ── Notice / Availability ────────────────────────────────────────────
        {"name": "notice_period",        "type": "string",   "optional": True, "facet": True},
        # ── Education ────────────────────────────────────────────────────────
        {"name": "education_summary",    "type": "string",   "optional": True},
        # ── v2: degree facet + institution ────────────────────────────────────
        {"name": "degree",               "type": "string",   "optional": True, "facet": True},
        {"name": "institution",          "type": "string",   "optional": True},
        # ── v2: companies count ───────────────────────────────────────────────
        {"name": "companies_count",      "type": "int32",    "optional": True},
        # ── Resume snippet (not stored, only indexed) ────────────────────────
        {"name": "resume_snippet",       "type": "string",   "optional": True, "index": True, "store": False},
        # ── Sorting ──────────────────────────────────────────────────────────
        {"name": "created_at_ts",        "type": "int64"},
    ],
    "default_sorting_field": "created_at_ts",
    "token_separators": ["+", "#", "."],
}

QUERY_BY_FIELDS  = "suggested_title,current_designation,current_company,previous_titles,previous_companies,skills,degree,institution,education_summary,resume_snippet"
QUERY_BY_WEIGHTS = "10,9,8,7,6,5,4,3,2,1"


class CandidateIndexer:
    def __init__(self, host: str, port: int, api_key: str):
        self.base_url = f"http://{host}:{port}"
        self.headers  = {
            "X-TYPESENSE-API-KEY": api_key,
            "Content-Type": "application/json",
        }

    # ── Lifecycle ──────────────────────────────────────────────────────────────

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
            r = await c.get(f"{self.base_url}/collections/candidates", headers=self.headers)
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
            r = await c.get(f"{self.base_url}/collections/candidates", headers=self.headers)
            if r.status_code == 200:
                data = r.json()
                return {"num_documents": data.get("num_documents", 0), "name": data.get("name")}
            return {"error": r.text}

    # ── Document transform ─────────────────────────────────────────────────────

    def transform_record(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert a hr_talent_pool row into a Typesense document.
        Returns None if the row is missing required fields.

        CRASH FIX: uses _to_str() for all JSONB field extractions to handle
        cases where fields contain lists instead of strings.
        """
        if not row.get("id") or not row.get("organization_id"):
            return None

        # ── Skills ────────────────────────────────────────────────────────────
        skills: List[str] = []
        raw_skills = row.get("top_skills")
        if isinstance(raw_skills, list):
            for s in raw_skills:
                if isinstance(s, str) and s.strip():
                    skills.append(s.strip().lower())
                elif isinstance(s, dict):
                    name = _to_str(s.get("name") or s.get("skill_name"))
                    if name:
                        skills.append(name.lower())
        elif isinstance(raw_skills, str):
            try:
                parsed = json.loads(raw_skills)
                if isinstance(parsed, list):
                    for s in parsed:
                        v = s if isinstance(s, str) else _to_str(s.get("name") if isinstance(s, dict) else s)
                        if v.strip():
                            skills.append(v.strip().lower())
            except Exception:
                pass
        skills = list(dict.fromkeys(skills))

        # ── Work experience ────────────────────────────────────────────────────
        we = row.get("work_experience") or []
        if isinstance(we, str):
            try:
                we = json.loads(we)
            except Exception:
                we = []
        if not isinstance(we, list):
            we = []

        # Start from DB columns (these are already strings or None)
        current_designation  = _to_str(row.get("current_designation"))
        current_company      = _to_str(row.get("current_company"))
        previous_designation = ""
        previous_company     = ""
        previous_titles:    List[str] = []
        previous_companies: List[str] = []

        if len(we) > 0:
            # we[0] → current role (only fills in if DB columns are blank)
            we_0 = we[0] if isinstance(we[0], dict) else {}
            if not current_designation:
                # FIX: _to_str handles the case where "designation" is a list
                current_designation = _to_str(we_0.get("designation"))
            if not current_company:
                current_company = _to_str(we_0.get("company"))

        for idx, item in enumerate(we[1:], start=1):
            if not isinstance(item, dict):
                continue
            # FIX: _to_str on every field extraction from JSONB
            title   = _to_str(item.get("designation"))
            company = _to_str(item.get("company"))

            if idx == 1:
                previous_designation = title
                previous_company     = company

            if title and title not in previous_titles:
                previous_titles.append(title)
            if company and company not in previous_companies:
                previous_companies.append(company)

        # ── Companies count ────────────────────────────────────────────────────
        all_companies: List[str] = []
        if current_company:
            all_companies.append(current_company)
        for c in previous_companies:
            if c not in all_companies:
                all_companies.append(c)
        companies_count = len(all_companies)

        # ── Education ──────────────────────────────────────────────────────────
        edu = row.get("education") or []
        if isinstance(edu, str):
            try:
                edu = json.loads(edu)
            except Exception:
                edu = []
        if not isinstance(edu, list):
            edu = []

        education_summary = ""
        degree            = ""
        institution       = ""
        if len(edu) > 0:
            edu_0 = edu[0] if isinstance(edu[0], dict) else {}
            # FIX: _to_str on degree and institution (can be lists in malformed data)
            degree      = _to_str(edu_0.get("degree"))
            institution = _to_str(edu_0.get("institution"))
            parts = [p for p in [degree, institution] if p]
            education_summary = ", ".join(parts)

        # ── Resume snippet ─────────────────────────────────────────────────────
        resume_text    = row.get("resume_text") or ""
        resume_snippet = resume_text[:2000].strip() if isinstance(resume_text, str) else ""

        # ── Timestamp ─────────────────────────────────────────────────────────
        created_at_str = row.get("created_at") or "2020-01-01T00:00:00+00:00"
        try:
            from datetime import datetime, timezone
            if "+" in created_at_str or "Z" in created_at_str:
                dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(created_at_str).replace(tzinfo=timezone.utc)
            created_at_ts = int(dt.timestamp())
        except Exception:
            created_at_ts = 0

        # ── Build document ─────────────────────────────────────────────────────
        doc: Dict[str, Any] = {
            "id":              str(row["id"]),
            "organization_id": str(row["organization_id"]),
            "full_name":       _to_str(row.get("candidate_name")),
            "created_at_ts":   created_at_ts,
        }

        def _set(key: str, val: Any):
            if val and str(val).strip():
                doc[key] = val

        _set("email",                row.get("email"))
        _set("phone",                row.get("phone"))
        _set("suggested_title",      row.get("suggested_title"))
        _set("current_designation",  current_designation)
        _set("current_company",      current_company)
        _set("previous_designation", previous_designation)
        _set("previous_company",     previous_company)
        _set("current_location",     row.get("current_location"))
        _set("notice_period",        row.get("notice_period"))
        _set("education_summary",    education_summary)
        _set("resume_snippet",       resume_snippet)
        _set("degree",               degree)
        _set("institution",          institution)

        if previous_titles:
            doc["previous_titles"] = previous_titles
        if previous_companies:
            doc["previous_companies"] = previous_companies
        if companies_count > 0:
            doc["companies_count"] = companies_count
        if skills:
            doc["skills"] = skills

        # Numeric fields
        exp = row.get("parsed_experience_years")
        if exp is not None:
            try:
                v = int(exp)
                if v >= 0:   # skip -1 sentinel values
                    doc["exp_years"] = v
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

    # ── Write operations ───────────────────────────────────────────────────────

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

    async def delete_document(self, doc_id: str):
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.delete(
                f"{self.base_url}/collections/candidates/documents/{doc_id}",
                headers=self.headers,
            )
            if r.status_code not in (200, 404):
                logger.error(f"Delete failed for {doc_id}: {r.text}")

    async def bulk_upsert(self, docs: List[Dict[str, Any]]):
        """Import a batch via Typesense bulk import (JSONL, action=upsert)."""
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