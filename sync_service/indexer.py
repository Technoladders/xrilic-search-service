"""
indexer.py — v2.2 (full resume text)
CHANGES vs v2.1:
  v2.2 — Full resume text indexing
    • resume_snippet (2000 chars) → resume_full_text (100,000 chars)
    • Rename field in COLLECTION_SCHEMA
    • run_full_reindex SELECT_FIELDS now includes resume_text column
    • All webhook upserts already include resume_text (row is the full record)
    REQUIRES: POST /reindex after deploy to rebuild the index with the new field.

  v2.1 — _to_str() crash fix
    Some JSONB rows store designation/company/degree as a LIST instead of a
    string — e.g. ["Executive - Records Management"] — causing AttributeError.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("indexer")


# ── Safe JSONB string extractor ────────────────────────────────────────────────
def _to_str(val: Any) -> str:
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, (list, tuple)):
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
        # ── v2: all past titles / companies ─────────────────────────────────
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
        {"name": "degree",               "type": "string",   "optional": True, "facet": True},
        {"name": "highest_education",    "type": "string", "optional": True, "facet": True},
        {"name": "all_degrees",          "type": "string[]", "optional": True, "facet": True},
        {"name": "institution",          "type": "string",   "optional": True},
        # ── Companies count ───────────────────────────────────────────────────
        {"name": "companies_count",      "type": "int32",    "optional": True},
        # ── v2.2: full resume text (indexed, NOT stored — store:false saves disk) ──
        # Previously: resume_snippet (only first 2000 chars)
        # Now: resume_full_text (up to 100,000 chars = ~99% of all resumes)
        # search.hrumbles.ai only tokenises and inverts this — it is never
        # returned in search hits, so bandwidth is unaffected.
        {"name": "resume_full_text", "type": "string", "optional": True, "index": True, "store": False},
        # ── Sorting ──────────────────────────────────────────────────────────
        {"name": "created_at_ts",    "type": "int64"},
    ],
    "default_sorting_field": "created_at_ts",
    "token_separators": ["+", "#", "."],
}

QUERY_BY_FIELDS  = (
    "suggested_title,current_designation,current_company,"
    "previous_titles,previous_companies,"
    "skills,degree,institution,education_summary,resume_full_text"
)
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

        v2.2: resume_full_text now includes up to 100,000 chars of resume_text.
        v2.1: _to_str() handles JSONB fields that are lists instead of strings.
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

        current_designation  = _to_str(row.get("current_designation"))
        current_company      = _to_str(row.get("current_company"))
        previous_designation = ""
        previous_company     = ""
        previous_titles:    List[str] = []
        previous_companies: List[str] = []

        if len(we) > 0:
            we_0 = we[0] if isinstance(we[0], dict) else {}
            if not current_designation:
                current_designation = _to_str(we_0.get("designation"))
            if not current_company:
                current_company = _to_str(we_0.get("company"))

        for idx, item in enumerate(we[1:], start=1):
            if not isinstance(item, dict):
                continue
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

# ── Education — full history ───────────────────────────────────────────
        edu = row.get("education") or []
        if isinstance(edu, str):
            try:
                edu = json.loads(edu)
            except Exception:
                edu = []
        if not isinstance(edu, list):
            edu = []

        all_degrees:      List[str] = []
        all_institutions: List[str] = []
        degree      = ""
        institution = ""

        for idx, entry in enumerate(edu):
            if not isinstance(entry, dict):
                continue
            deg  = _to_str(entry.get("degree"))
            inst = _to_str(entry.get("institution"))
            if deg and deg not in all_degrees:
                all_degrees.append(deg)
            if inst and inst not in all_institutions:
                all_institutions.append(inst)
            if idx == 0:          # first entry = primary degree / institution
                degree      = deg
                institution = inst

        # highest_education flat column — authoritative, add at front if not duplicate
        highest_education = _to_str(row.get("highest_education"))
        if highest_education:
            if highest_education not in all_degrees:
                all_degrees.insert(0, highest_education)
            if not degree:
                degree = highest_education

        # education_summary = all degrees + all institutions joined
        # searched by keyword branch A (QB_KEYWORD_TEXT includes education_summary)
        edu_parts     = all_degrees + all_institutions
        education_summary = ", ".join(edu_parts) if edu_parts else ""

        # ── Resume full text (v2.2) ────────────────────────────────────────────
        # Index up to 100,000 chars. store:False means Typesense tokenises it
        # for search but never returns it in hits — zero bandwidth impact.
        # 100K covers essentially every resume (a typical 3-page resume is ~6KB).
        resume_text     = row.get("resume_text") or ""
        resume_full_text = resume_text[:100_000].strip() if isinstance(resume_text, str) else ""

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
        _set("resume_full_text",     resume_full_text)   # v2.2 (was resume_snippet)
        _set("degree",               degree)
        _set("highest_education",    highest_education)
        _set("institution",          institution)
        

        if previous_titles:
            doc["previous_titles"] = previous_titles
        if previous_companies:
            doc["previous_companies"] = previous_companies
        if companies_count > 0:
            doc["companies_count"] = companies_count
        if skills:
            doc["skills"] = skills
        if all_degrees:
            doc["all_degrees"] = all_degrees

        exp = row.get("parsed_experience_years")
        if exp is not None:
            try:
                v = int(exp)
                if v >= 0:
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
        """Upsert a single document (from webhook). Row is the full record."""
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

    async def bulk_upsert(self, docs: List[Dict[str, Any]]) -> bool:
        """
        Returns True on success, False if Typesense is unavailable.
        Retries 3× with backoff on transient failures.
        """
        if not docs:
            return True

        jsonl = "\n".join(json.dumps(d) for d in docs)

        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=30) as c:
                    r = await c.post(
                        f"{self.base_url}/collections/candidates/documents/import"
                        f"?action=upsert&batch_size=100",
                        headers={
                            **self.headers,
                            "Content-Type": "text/plain",
                        },
                        content=jsonl.encode("utf-8"),
                    )

                if r.status_code == 503:
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        logger.warning(
                            f"Bulk upsert 503 "
                            f"(attempt {attempt + 1}/3) "
                            f"— waiting {wait}s"
                        )
                        await asyncio.sleep(wait)
                        continue

                    logger.warning(
                        "Bulk upsert still returning 503 "
                        "after 3 attempts"
                    )
                    return False

                if r.status_code != 200:
                    logger.error(
                        f"Bulk upsert failed: "
                        f"{r.status_code} "
                        f"{r.text[:300]}"
                    )
                    return False

                errors = []

                for line in r.text.strip().split("\n"):
                    try:
                        result = json.loads(line)

                        if not result.get("success"):
                            errors.append(result)

                    except Exception:
                        pass

                if errors:
                    logger.warning(
                        f"Bulk upsert: "
                        f"{len(errors)} errors out of {len(docs)}"
                    )

                    for e in errors[:5]:
                        logger.warning(f"  → {e}")

                else:
                    logger.info(
                        f"Bulk upsert: {len(docs)} docs OK"
                    )

                return True

            except (
                httpx.ReadTimeout,
                httpx.RemoteProtocolError,
                httpx.ConnectError,
            ) as e:

                if attempt < 2:
                    wait = 5 * (attempt + 1)

                    logger.warning(
                        f"Bulk upsert "
                        f"{type(e).__name__} "
                        f"(attempt {attempt + 1}/3) "
                        f"— waiting {wait}s"
                    )

                    await asyncio.sleep(wait)
                    continue

                logger.error(
                    f"Bulk upsert failed after 3 attempts: "
                    f"{type(e).__name__}"
                )
                return False

        return False