"""
sync_service/master_candidates/backfill/match.py

Python port of resolve_master_candidate() and ingest_master_candidate()'s
identity-matching and merge logic. Every Postgres access here is a plain
PostgREST HTTP call (candidate_identities / master_candidates /
candidate_source_links / candidate_merge_queue) -- no Supabase RPC or Edge
Function is used for any part of this. This is the highest-risk file in the
migration: it is a from-scratch reimplementation of complex, correctness-
critical PL/pgSQL, not a thin wrapper around the original functions.

Two concurrency mechanisms live here, guarding two different races that
don't exist in the original (which ran as one atomic Postgres statement):

  1. _IDENTITY_LOCKS (pre-resolution): two concurrent rows sharing an
     identity value (same email/phone/LinkedIn URL/etc.) that would both
     independently resolve to "new" must not both create a master. Locking
     on every non-empty matching-relevant identity key BEFORE resolving,
     and holding through the insert-or-merge decision and identity/source-
     link writes, makes this a deterministic fix given this service's
     confirmed single-process deployment (chunks run one asyncio.gather
     wave at a time, never two chunks concurrently).
  2. _MASTER_LOCKS (post-resolution): two rows -- possibly reached via
     different identity paths, so not necessarily caught by lock #1 --
     resolving to the SAME existing master must not lose one's writes to
     the other's read-modify-write. Backed by optimistic concurrency
     (a conditional PATCH keyed on master_candidates.updated_at) as the
     real correctness mechanism regardless of the lock.

Field-by-field M1 (full merge) vs M2 (fill-only merge) behavior below was
verified directly against the literal ingest_master_candidate SQL, not
inferred from a summary -- see the _M1_*/_M2_* constants' comments for
exactly which fields each path does and does not touch. In particular:
  - M2 is NOT a generic "fill any empty field" operation. It touches a
    specific, narrower list of ~20 scalars (old-wins/fill-only direction)
    plus the handful of fields both paths always touch (skills union,
    contact dedup, sources, data_freshness/last_active_date, raw_profile_
    by_source). Rich fields (experience, education, certifications,
    projects, structured_skills, company, all CTC/resume/date fields,
    work_auth_countries, primary_source, has_full_profile,
    profile_completeness) are simply absent from M2's SET clause and are
    left completely untouched, even when currently empty.
  - work_status, followers, publications, volunteering_experiences, awards,
    and notice_period_days are set only at INSERT time and are never
    touched by EITHER merge path -- frozen forever after row creation.
  - GREATEST/LEAST in Postgres ignore NULLs (return NULL only if every
    argument is NULL) -- the OPPOSITE of standard comparison operators.
    pg_greatest/pg_least below replicate that explicitly; a bare
    max()/min() would raise on None and, if worked around naively, could
    silently null out an existing value that Postgres's GREATEST would
    have preserved.

Field-mapping gap carried forward as-is from mapping.py/ingest.py (not
fixed in this pass): notice_period_days is always None from map_row(), and
-- confirmed here -- is never touched by either merge path either, so it
is permanently NULL for every row this pipeline has ever produced or ever
will, until mapping.py is deliberately extended to parse it.
"""

import asyncio
import json
import random
import uuid as uuid_mod
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Any, Optional

import httpx

from ..config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE
from . import mapping
from . import trgm
from .config import SOURCE, LOCK_SHARD_COUNT, MERGE_RETRY_ATTEMPTS, MERGE_RETRY_BASE_DELAY_SEC


# ─────────────────────────────────────────────────────────────────────────────
# Small value helpers
# ─────────────────────────────────────────────────────────────────────────────
def _nullif(v: Any) -> Any:
    return v if v not in (None, "") else None


def _int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pgrst_escape(value: str) -> str:
    """Escape a value for use inside a PostgREST or=(...) filter group --
    values containing a comma or parenthesis must be double-quoted, since
    commas separate group members and parens delimit sub-groups."""
    if any(c in value for c in (",", "(", ")", '"')):
        return '"' + value.replace('"', '\\"') + '"'
    return value


def pg_greatest(*args):
    """Postgres GREATEST(): ignores NULLs; result is NULL only if every
    argument is NULL. The opposite of a bare comparison operator."""
    vals = [a for a in args if a is not None]
    return max(vals) if vals else None


def pg_least(*args):
    vals = [a for a in args if a is not None]
    return min(vals) if vals else None


def _parse_dt(v: Any) -> Optional[datetime]:
    """Parse a PostgREST-serialized timestamptz string into a datetime,
    matching indexer.py's existing ISO-parsing approach. Passes datetimes
    through unchanged; returns None for anything unparseable."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v)
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Source-authority scoring (verbatim from ingest_master_candidate's CASE)
# ─────────────────────────────────────────────────────────────────────────────
_SOURCE_AUTHORITY = {
    "contactout": 1.00, "rocketreach": 1.00, "apollo": 0.95,
    "portal_a": 0.90, "talent_pool": 0.85, "invite": 0.80,
}


def source_authority(source: Optional[str]) -> float:
    return _SOURCE_AUTHORITY.get(source, 0.50)


# ─────────────────────────────────────────────────────────────────────────────
# Concurrency safety -- two independent lock shards, see module docstring
# ─────────────────────────────────────────────────────────────────────────────
_IDENTITY_LOCKS = [asyncio.Lock() for _ in range(LOCK_SHARD_COUNT)]
_MASTER_LOCKS = [asyncio.Lock() for _ in range(LOCK_SHARD_COUNT)]


def _shard_for(key: str) -> int:
    return hash(key) % LOCK_SHARD_COUNT


class _MultiLock:
    """Acquire a set of locks together (sorted by the caller) and release in
    reverse order. Sorted acquisition order prevents deadlock when two rows
    need overlapping-but-not-identical sets of shards."""
    def __init__(self, locks):
        self._locks = list(locks)

    async def __aenter__(self):
        for lock in self._locks:
            await lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        for lock in reversed(self._locks):
            lock.release()
        return False


def _acquire_all(locks):
    return _MultiLock(locks)


# ─────────────────────────────────────────────────────────────────────────────
# Identity extraction -- shared between resolve_master_candidate's lookups,
# the pre-resolution lock, and the identities table writes.
# ─────────────────────────────────────────────────────────────────────────────
# (payload_field, identity_type, normalize_fn, confidence)
_IDENTITY_FIELD_SPECS = [
    ("linkedin_url", "linkedin_url", lambda v: v.strip().lower(), 1.00),
    ("portal_a_encrypted_username", "portal_a_encrypted_username", lambda v: v.strip(), 1.00),
    ("portal_a_sid", "portal_a_sid", lambda v: v.strip(), 0.80),
    ("portal_a_user_id", "portal_a_user_id", lambda v: v.strip(), 1.00),
    ("portal_a_res_id", "portal_a_res_id", lambda v: v.strip(), 1.00),
    ("apollo_person_id", "apollo_person_id", lambda v: v.strip(), 1.00),
    ("rocketreach_id", "rocketreach_id", lambda v: v.strip(), 1.00),
]

# Only these identity types participate in resolve_master_candidate's
# matching tiers -- portal_a_sid/user_id/res_id are write-only.
_MATCHING_IDENTITY_TYPES = {
    "linkedin_url", "portal_a_encrypted_username",
    "apollo_person_id", "rocketreach_id",
    "email_normalized", "phone_e164",
}


def _identities_to_write(payload: dict) -> list[dict]:
    out: list[dict] = []
    for field, itype, norm, conf in _IDENTITY_FIELD_SPECS:
        raw = payload.get(field)
        if raw and str(raw).strip():
            out.append({"identity_type": itype, "identity_value": norm(str(raw)), "confidence": conf})
    email_norm = mapping.norm_email(payload.get("primary_email"))
    if email_norm:
        out.append({"identity_type": "email_normalized", "identity_value": email_norm, "confidence": 1.00})
    phone_norm = mapping.norm_phone_e164(payload.get("primary_phone"))
    if phone_norm:
        out.append({"identity_type": "phone_e164", "identity_value": phone_norm, "confidence": 1.00})
    return out


def _candidate_identity_keys(payload: dict) -> list[str]:
    """Non-empty matching-relevant (type, value) identity keys for this
    row, used to pick which _IDENTITY_LOCKS shards to acquire."""
    return [
        f"{item['identity_type']}:{item['identity_value']}"
        for item in _identities_to_write(payload)
        if item["identity_type"] in _MATCHING_IDENTITY_TYPES
    ]


# ─────────────────────────────────────────────────────────────────────────────
# resolve_master_candidate port
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MatchResult:
    master_candidate_id: Optional[str]
    match_method: str
    match_confidence: float
    needs_review: bool


async def resolve_master_candidate(
    client: httpx.AsyncClient, *,
    linkedin_url: Optional[str] = None,
    portal_a_encrypted_username: Optional[str] = None,
    apollo_person_id: Optional[str] = None,
    rocketreach_id: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    full_name: Optional[str] = None,
) -> MatchResult:
    email_norm = mapping.norm_email(email)
    phone_norm = mapping.norm_phone_e164(phone)

    li = (linkedin_url or "").strip().lower()
    pau = (portal_a_encrypted_username or "").strip()
    ap = (apollo_person_id or "").strip()
    rr = (rocketreach_id or "").strip()

    lookups: list[tuple[str, str]] = []
    if li:
        lookups.append(("linkedin_url", li))
    if pau:
        lookups.append(("portal_a_encrypted_username", pau))
    if ap:
        lookups.append(("apollo_person_id", ap))
    if rr:
        lookups.append(("rocketreach_id", rr))
    if email_norm:
        lookups.append(("email_normalized", email_norm))
    if phone_norm:
        lookups.append(("phone_e164", phone_norm))

    found: dict[tuple[str, str], str] = {}
    if lookups:
        or_filter = ",".join(
            f"and(identity_type.eq.{_pgrst_escape(t)},identity_value.eq.{_pgrst_escape(v)})"
            for t, v in lookups
        )
        r = await client.get(
            f"{SUPABASE_REST}/candidate_identities",
            params={"or": f"({or_filter})", "select": "identity_type,identity_value,master_candidate_id"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        r.raise_for_status()
        for row in r.json():
            found[(row["identity_type"], row["identity_value"])] = row["master_candidate_id"]

    # T1
    if li and ("linkedin_url", li) in found:
        return MatchResult(found[("linkedin_url", li)], "linkedin_url", 1.00, False)
    # T2
    if pau and ("portal_a_encrypted_username", pau) in found:
        return MatchResult(found[("portal_a_encrypted_username", pau)], "portal_a_encrypted_username", 1.00, False)
    # T3
    if ap and ("apollo_person_id", ap) in found:
        return MatchResult(found[("apollo_person_id", ap)], "apollo_person_id", 1.00, False)
    # T3b
    if rr and ("rocketreach_id", rr) in found:
        return MatchResult(found[("rocketreach_id", rr)], "rocketreach_id", 1.00, False)

    email_master = found.get(("email_normalized", email_norm)) if email_norm else None
    phone_master = found.get(("phone_e164", phone_norm)) if phone_norm else None

    # T4: email + phone -> same master, auto-merge
    if email_master is not None and phone_master is not None and email_master == phone_master:
        return MatchResult(email_master, "email_phone", 0.97, False)

    full_name_clean = (full_name or "").strip()
    master_names: dict[str, Optional[str]] = {}
    ids_needed = [i for i in {email_master, phone_master} if i]
    if ids_needed and full_name_clean:
        r2 = await client.get(
            f"{SUPABASE_REST}/master_candidates",
            params={"id": f"in.({','.join(ids_needed)})", "select": "id,full_name"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        r2.raise_for_status()
        for row in r2.json():
            master_names[row["id"]] = row.get("full_name")

    # T5: email + name_sim >= 0.90 -> queue
    if email_master is not None and full_name_clean:
        master_name = master_names.get(email_master)
        if master_name and trgm.similarity(master_name.lower(), full_name_clean.lower()) >= 0.90:
            return MatchResult(email_master, "email", 0.90, True)

    # T6: email alone -> queue
    if email_master is not None:
        return MatchResult(email_master, "email", 0.82, True)

    # T7: phone + name_sim >= 0.85 -> queue
    if phone_master is not None and full_name_clean:
        master_name = master_names.get(phone_master)
        if master_name and trgm.similarity(master_name.lower(), full_name_clean.lower()) >= 0.85:
            return MatchResult(phone_master, "phone", 0.80, True)

    return MatchResult(None, "new", 1.00, False)


# ─────────────────────────────────────────────────────────────────────────────
# New-master INSERT (Branch A/C) -- pure function, no I/O
# ─────────────────────────────────────────────────────────────────────────────
def build_insert_row(payload: dict, source: str) -> dict:
    """NOTE: deliberately does not include has_contact -- absent from the
    original INSERT's column list, almost certainly a generated/trigger-
    maintained column (see pre-flight checklist)."""
    return {
        "linkedin_url": _nullif(payload.get("linkedin_url")),
        "li_vanity": _nullif(payload.get("li_vanity")),
        "apollo_person_id": _nullif(payload.get("apollo_person_id")),
        "rocketreach_id": _nullif(payload.get("rocketreach_id")),
        "full_name": _nullif(payload.get("full_name")),
        "title": _nullif(payload.get("title")),
        "headline": _nullif(payload.get("headline")),
        "summary": _nullif(payload.get("summary")),
        "profile_picture_url": _nullif(payload.get("profile_picture_url")),
        "location": _nullif(payload.get("location")),
        "country": _nullif(payload.get("country")) or "India",
        "industry": _nullif(payload.get("industry")),
        "job_function": _nullif(payload.get("job_function")),
        "seniority": _nullif(payload.get("seniority")),
        "work_status": _nullif(payload.get("work_status")),
        "followers": _int(_nullif(payload.get("followers"))),
        "company": payload.get("company") or {},
        "company_name": _nullif(payload.get("company_name")),
        "company_domain": _nullif(payload.get("company_domain")),
        "company_industry": _nullif(payload.get("company_industry")),
        "company_size": _nullif(payload.get("company_size")),
        "experience": payload.get("experience") or [],
        "education": payload.get("education") or [],
        "skills": payload.get("skills") or [],
        "certifications": payload.get("certifications") or [],
        "publications": payload.get("publications") or [],
        "projects": payload.get("projects") or [],
        "languages": payload.get("languages") or [],
        "volunteering_experiences": payload.get("volunteering_experiences") or [],
        "awards": payload.get("awards") or [],
        "may_also_know_skills": payload.get("may_also_know_skills") or [],
        "structured_skills": payload.get("structured_skills") or [],
        "contact_availability": payload.get("contact_availability")
            or {"phone": False, "work_email": False, "personal_email": False},
        "available_emails": payload.get("available_emails") or [],
        "available_phones": payload.get("available_phones") or [],
        "current_ctc_lacs": _num(_nullif(payload.get("current_ctc_lacs"))),
        "expected_ctc_lacs": _num(_nullif(payload.get("expected_ctc_lacs"))),
        "current_ctc_display": _nullif(payload.get("current_ctc_display")),
        "notice_period_days": _int(_nullif(payload.get("notice_period_days"))),
        "notice_period_display": _nullif(payload.get("notice_period_display")),
        "total_experience_years": _num(_nullif(payload.get("total_experience_years"))),
        "total_experience_months": _int(_nullif(payload.get("total_experience_months"))),
        "experience_display": _nullif(payload.get("experience_display")),
        "preferred_locations": list(payload.get("preferred_locations") or []),
        "current_location": _nullif(payload.get("current_location")),
        "functional_area": _nullif(payload.get("functional_area")),
        "role": _nullif(payload.get("role")),
        "resume_url": _nullif(payload.get("resume_url")),
        "resume_text": _nullif(payload.get("resume_text")),
        "resume_last_updated": _nullif(payload.get("resume_last_updated")),
        "gender": _nullif(payload.get("gender")),
        "dob": _nullif(payload.get("dob")),
        "marital_status": _nullif(payload.get("marital_status")),
        "category": _nullif(payload.get("category")),
        "disability": _nullif(payload.get("disability")),
        "desired_job_type": _nullif(payload.get("desired_job_type")),
        "employment_status_pref": _nullif(payload.get("employment_status_pref")),
        "work_auth_countries": list(payload.get("work_auth_countries") or []),
        "primary_source": source,
        "sources": [source],
        "has_full_profile": bool(payload.get("has_full_profile")),
        "profile_completeness": _num(_nullif(payload.get("profile_completeness"))),
        "data_freshness": _nullif(payload.get("data_freshness")),
        "last_active_date": _nullif(payload.get("last_active_date")),
        "raw_profile_by_source": {source: payload.get("raw_metadata") or {}},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Merge (M1 full-merge / M2 fill-only) -- field lists verified directly
# against the literal SQL, see module docstring.
# ─────────────────────────────────────────────────────────────────────────────
_M1_SCALAR_NEW_WINS = [
    "linkedin_url", "li_vanity", "apollo_person_id", "rocketreach_id",
    "full_name", "title", "headline", "summary", "profile_picture_url",
    "location", "country", "industry", "job_function", "seniority",
    "company_name", "company_domain", "company_industry", "company_size",
    "current_ctc_lacs", "expected_ctc_lacs", "current_ctc_display",
    "notice_period_display", "total_experience_months", "total_experience_years",
    "experience_display", "current_location", "functional_area", "role",
    "resume_url", "resume_text", "resume_last_updated", "gender", "dob",
    "marital_status", "category", "disability", "desired_job_type",
    "employment_status_pref",
]

# Strict subset of the above that M2 ALSO touches, in the opposite
# (old-wins / fill-only) direction. Everything in _M1_SCALAR_NEW_WINS not
# listed here (li_vanity, job_function, company_size, all CTC/experience/
# resume fields, category/disability/desired_job_type/employment_status_pref)
# is left completely untouched by M2 -- confirmed against the SQL, not inferred.
_M2_SCALAR_OLD_WINS = [
    "linkedin_url", "apollo_person_id", "rocketreach_id", "full_name",
    "title", "headline", "summary", "profile_picture_url", "location",
    "country", "industry", "seniority", "company_name", "company_domain",
    "company_industry", "current_location", "gender", "dob",
    "marital_status", "role", "functional_area",
]

# jsonb ARRAY fields M1 replaces wholesale if incoming is non-empty
# ("higher authority wins for arrays too"). M2 does not touch any of these.
_M1_WHOLESALE_REPLACE_ARRAYS = [
    "experience", "education", "languages", "certifications",
    "structured_skills", "projects",
]

# Numeric fields needing type coercion when read back off a fetched row /
# incoming payload (values may arrive as JSON numbers already, but coerce
# defensively rather than assume).
_NUMERIC_CASTERS = {
    "current_ctc_lacs": _num, "expected_ctc_lacs": _num,
    "total_experience_months": _int, "total_experience_years": _num,
    "profile_completeness": _num,
}


def jsonb_union_distinct(existing: Optional[list], incoming: Optional[list]) -> list:
    """Exact-value dedup (case-sensitive), first-occurrence order."""
    seen: set = set()
    out: list = []
    for item in list(existing or []) + list(incoming or []):
        key = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else item
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def text_array_union_if_incoming(existing: Optional[list], incoming: Optional[list]) -> list:
    """Distinct union, but only if incoming is non-empty -- matches the
    SQL's `CASE WHEN jsonb_array_length(incoming) > 0 THEN union ELSE
    existing END` pattern for preferred_locations/work_auth_countries."""
    if not incoming:
        return list(existing or [])
    return list(dict.fromkeys(list(existing or []) + list(incoming or [])))


def merge_contact_list(existing: Optional[list], incoming: Optional[list]) -> list:
    """DISTINCT ON (value), preferring verified DESC then confidence DESC,
    ordered by value ascending. Ties keep whichever was seen first
    (existing before incoming) -- a documented, deterministic tiebreak
    since Postgres itself doesn't guarantee one for true ties."""
    best: dict[str, dict] = {}
    for item in list(existing or []) + list(incoming or []):
        v = item.get("value")
        if v is None:
            continue
        cur = best.get(v)
        key = (bool(item.get("verified")), item.get("confidence") or 0)
        if cur is None or key > (bool(cur.get("verified")), cur.get("confidence") or 0):
            best[v] = item
    return [best[v] for v in sorted(best)]


def bool_or_dict(existing: Optional[dict], incoming: Optional[dict], keys: tuple) -> dict:
    existing = existing or {}
    incoming = incoming or {}
    return {k: bool(existing.get(k)) or bool(incoming.get(k)) for k in keys}


def array_append_if_absent(existing: Optional[list], value: str) -> list:
    existing = list(existing or [])
    return existing if value in existing else existing + [value]


def build_merge_patch(existing: dict, payload: dict, source: str, is_full_merge: bool) -> dict:
    """Compute the PATCH body for merging `payload` into `existing` (the
    just-fetched current master_candidates row). is_full_merge=True is
    Path M1; False is Path M2."""
    patch: dict[str, Any] = {}

    def new_wins(field: str) -> None:
        caster = _NUMERIC_CASTERS.get(field)
        new_v = _nullif(payload.get(field))
        if new_v is not None and caster:
            new_v = caster(new_v)
        patch[field] = new_v if new_v is not None else existing.get(field)

    def old_wins(field: str) -> None:
        old_v = _nullif(existing.get(field))
        if old_v is not None:
            patch[field] = old_v
        else:
            caster = _NUMERIC_CASTERS.get(field)
            new_v = _nullif(payload.get(field))
            patch[field] = caster(new_v) if (new_v is not None and caster) else new_v

    if is_full_merge:
        for f in _M1_SCALAR_NEW_WINS:
            new_wins(f)
        incoming_company = payload.get("company") or {}
        patch["company"] = incoming_company if incoming_company else existing.get("company")
        for f in _M1_WHOLESALE_REPLACE_ARRAYS:
            incoming = payload.get(f) or []
            patch[f] = incoming if incoming else existing.get(f)
    else:
        for f in _M2_SCALAR_OLD_WINS:
            old_wins(f)
        # M2 touches neither `company` nor any _M1_WHOLESALE_REPLACE_ARRAYS field.

    # Fields both paths touch identically:
    patch["skills"] = jsonb_union_distinct(existing.get("skills"), payload.get("skills"))
    patch["may_also_know_skills"] = jsonb_union_distinct(
        existing.get("may_also_know_skills"), payload.get("may_also_know_skills"))
    patch["contact_availability"] = bool_or_dict(
        existing.get("contact_availability"), payload.get("contact_availability"),
        ("phone", "work_email", "personal_email"))
    patch["available_emails"] = merge_contact_list(
        existing.get("available_emails"), payload.get("available_emails"))
    patch["available_phones"] = merge_contact_list(
        existing.get("available_phones"), payload.get("available_phones"))
    patch["preferred_locations"] = text_array_union_if_incoming(
        existing.get("preferred_locations"), payload.get("preferred_locations"))
    patch["sources"] = array_append_if_absent(existing.get("sources"), source)
    fresh = pg_greatest(_parse_dt(existing.get("data_freshness")), _parse_dt(payload.get("data_freshness")))
    patch["data_freshness"] = fresh.isoformat() if fresh is not None else None
    active = pg_greatest(_parse_date(existing.get("last_active_date")), _parse_date(payload.get("last_active_date")))
    patch["last_active_date"] = active.isoformat() if active is not None else None
    patch["raw_profile_by_source"] = {
        **(existing.get("raw_profile_by_source") or {}),
        source: payload.get("raw_metadata") or {},
    }

    if is_full_merge:
        patch["work_auth_countries"] = text_array_union_if_incoming(
            existing.get("work_auth_countries"), payload.get("work_auth_countries"))
        new_auth = source_authority(source)
        existing_auth = source_authority(existing.get("primary_source"))
        patch["primary_source"] = source if new_auth > existing_auth else existing.get("primary_source")
        patch["has_full_profile"] = bool(payload.get("has_full_profile")) or bool(existing.get("has_full_profile"))
        patch["profile_completeness"] = pg_greatest(
            _num(existing.get("profile_completeness")), _num(payload.get("profile_completeness")))
    # M2 touches neither work_auth_countries, primary_source, has_full_profile,
    # nor profile_completeness at all.

    patch["last_seen_at"] = datetime.now(timezone.utc).isoformat()
    return patch


# ─────────────────────────────────────────────────────────────────────────────
# candidate_identities / candidate_source_links / candidate_merge_queue writes
# ─────────────────────────────────────────────────────────────────────────────
async def upsert_identities(client: httpx.AsyncClient, master_id: str, payload: dict, source: str) -> None:
    items = _identities_to_write(payload)
    if not items:
        return
    body = [{**item, "master_candidate_id": master_id, "source": source} for item in items]
    r = await client.post(
        f"{SUPABASE_REST}/candidate_identities?on_conflict=identity_type,identity_value",
        headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates"},
        json=body, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def get_source_link(client: httpx.AsyncClient, source: str, source_row_id: str) -> Optional[dict]:
    r = await client.get(
        f"{SUPABASE_REST}/candidate_source_links",
        params={"source": f"eq.{source}", "source_row_id": f"eq.{source_row_id}",
                "select": "master_candidate_id,match_method,match_confidence"},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None


async def upsert_source_link(client: httpx.AsyncClient, source: str, source_row_id: str,
                              master_id: str, match_method: str, match_confidence: float) -> None:
    """Cannot use a single naive PostgREST upsert: the original SQL's
    ON CONFLICT DO UPDATE only touches master_candidate_id/last_synced_at,
    freezing match_method/match_confidence/ingested_at at their first-ever
    values. PostgREST's merge-duplicates upsert always overwrites every
    column present in the body, so a two-step design is required."""
    now_iso = datetime.now(timezone.utc).isoformat()
    r = await client.post(
        f"{SUPABASE_REST}/candidate_source_links?on_conflict=source,source_row_id",
        headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates,return=representation"},
        json={
            "source": source, "source_row_id": source_row_id,
            "master_candidate_id": master_id, "match_method": match_method,
            "match_confidence": match_confidence,
            "ingested_at": now_iso, "last_synced_at": now_iso,
        },
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    if r.json():
        return  # inserted fresh
    r2 = await client.patch(
        f"{SUPABASE_REST}/candidate_source_links",
        params={"source": f"eq.{source}", "source_row_id": f"eq.{source_row_id}"},
        headers=SB_HEADERS,
        json={"master_candidate_id": master_id, "last_synced_at": now_iso},
        timeout=HTTP_TIMEOUT_SUPABASE)
    r2.raise_for_status()


async def insert_merge_queue_row(client: httpx.AsyncClient, potential_dup_id: str, new_master_id: str,
                                  match: MatchResult, payload: dict, source: str) -> None:
    a = str(uuid_mod.UUID(potential_dup_id))
    b = str(uuid_mod.UUID(new_master_id))
    primary_id, candidate_id = (a, b) if a < b else (b, a)
    r = await client.post(
        f"{SUPABASE_REST}/candidate_merge_queue"
        f"?on_conflict=primary_master_candidate_id,candidate_master_candidate_id",
        headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates"},
        json={
            "primary_master_candidate_id": primary_id,
            "candidate_master_candidate_id": candidate_id,
            "match_signals": {
                "match_method": match.match_method,
                "email_norm": mapping.norm_email(payload.get("primary_email")),
                "phone_norm": mapping.norm_phone_e164(payload.get("primary_phone")),
                "incoming_name": payload.get("full_name"),
                "incoming_source": source,
            },
            "confidence": match.match_confidence,
            "status": "pending",
            "suggested_by": "auto_resolver",
        },
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def insert_new_master(client: httpx.AsyncClient, payload: dict, source: str) -> str:
    row = build_insert_row(payload, source)
    r = await client.post(
        f"{SUPABASE_REST}/master_candidates",
        headers={**SB_HEADERS, "Prefer": "return=representation"},
        json=row, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()[0]["id"]


async def _merge_into_existing(client: httpx.AsyncClient, master_id: str, payload: dict, source: str) -> None:
    """Optimistic-concurrency read-compute-write. An empty PATCH response
    (HTTP 200, not an error) signals a lost race against another writer --
    re-read, re-merge, re-patch. On exhaustion, raises (never silently
    drops the write); the source row is untouched, so a later retry of
    this whole row is always safe."""
    has_full_profile = bool(payload.get("has_full_profile"))
    new_auth = source_authority(source)
    for attempt in range(MERGE_RETRY_ATTEMPTS):
        r = await client.get(
            f"{SUPABASE_REST}/master_candidates",
            params={"id": f"eq.{master_id}", "select": "*"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            raise RuntimeError(f"master_candidates row {master_id} vanished during merge")
        existing = rows[0]
        existing_auth = source_authority(existing.get("primary_source"))
        is_full_merge = (new_auth >= existing_auth) and has_full_profile
        patch = build_merge_patch(existing, payload, source, is_full_merge)

        params = {"id": f"eq.{master_id}"}
        captured_updated_at = existing.get("updated_at")
        if captured_updated_at is not None:
            params["updated_at"] = f"eq.{captured_updated_at}"

        r2 = await client.patch(
            f"{SUPABASE_REST}/master_candidates", params=params,
            headers={**SB_HEADERS, "Prefer": "return=representation"},
            json=patch, timeout=HTTP_TIMEOUT_SUPABASE)
        r2.raise_for_status()
        if r2.json():
            return
        await asyncio.sleep(MERGE_RETRY_BASE_DELAY_SEC * (2 ** attempt) * (1 + random.uniform(-0.3, 0.3)))
    raise RuntimeError(f"optimistic_concurrency_exhausted: master_id={master_id}")


# ─────────────────────────────────────────────────────────────────────────────
# Per-row entry point -- the orchestration integration contract
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RowResult:
    source_row_id: str
    ok: bool
    is_new: bool = False
    master_candidate_id: Optional[str] = None
    match_method: Optional[str] = None
    match_confidence: Optional[float] = None
    needs_review: bool = False
    error: Optional[str] = None


async def ingest_backfill_row(client: httpx.AsyncClient, row: dict, *, dry_run: bool = False) -> RowResult:
    """Stateless aside from the module-level lock shards. Auth-agnostic --
    the caller (worker.py / the /retry-errors and /dry-run handlers) has
    already enforced require_global_superadmin. All exceptions are caught
    here and turned into RowResult(ok=False, ...), never raised past this
    function, mirroring ingest.py's _ingest_one swallow-and-report
    convention.

    dry_run=True: every read below still happens normally (so the returned
    decision -- is_new/match_method/match_confidence/needs_review -- is
    the real decision a live run would reach), but every write call
    (insert_new_master, upsert_source_link, upsert_identities,
    insert_merge_queue_row, the merge PATCH) is skipped entirely."""
    source_row_id = str(row.get("id"))
    try:
        payload = mapping.map_row(row)
    except Exception as e:
        return RowResult(source_row_id=source_row_id, ok=False, error=f"map_row_failed: {str(e)[:300]}")

    if not payload.get("portal_a_encrypted_username"):
        return RowResult(source_row_id=source_row_id, ok=False, error="missing_encrypted_username")

    try:
        # Crash-retry idempotency fast path: this row was already linked by
        # a prior (possibly partial) attempt -- reuse its master and merge,
        # self-healing the narrow orphan window described in the plan.
        existing_link = await get_source_link(client, SOURCE, source_row_id)
        if existing_link and existing_link.get("master_candidate_id"):
            master_id = existing_link["master_candidate_id"]
            if not dry_run:
                async with _acquire_all([_MASTER_LOCKS[_shard_for(master_id)]]):
                    await _merge_into_existing(client, master_id, payload, SOURCE)
                    await upsert_source_link(
                        client, SOURCE, source_row_id, master_id,
                        existing_link.get("match_method") or "new",
                        existing_link.get("match_confidence") or 1.00)
                    await upsert_identities(client, master_id, payload, SOURCE)
            return RowResult(
                source_row_id=source_row_id, ok=True, is_new=False, master_candidate_id=master_id,
                match_method=existing_link.get("match_method"),
                match_confidence=existing_link.get("match_confidence"))

        keys = _candidate_identity_keys(payload)
        shards = sorted({_shard_for(k) for k in keys}) or [_shard_for(source_row_id)]
        async with _acquire_all(_IDENTITY_LOCKS[i] for i in shards):
            match = await resolve_master_candidate(
                client,
                linkedin_url=payload.get("linkedin_url"),
                portal_a_encrypted_username=payload.get("portal_a_encrypted_username"),
                apollo_person_id=payload.get("apollo_person_id"),
                rocketreach_id=payload.get("rocketreach_id"),
                email=payload.get("primary_email"),
                phone=payload.get("primary_phone"),
                full_name=payload.get("full_name"),
            )

            if match.master_candidate_id is None or match.needs_review:
                potential_dup_id = match.master_candidate_id if match.needs_review else None
                if dry_run:
                    return RowResult(
                        source_row_id=source_row_id, ok=True, is_new=True, master_candidate_id=None,
                        match_method=match.match_method, match_confidence=match.match_confidence,
                        needs_review=match.needs_review)
                new_id = await insert_new_master(client, payload, SOURCE)
                await upsert_source_link(client, SOURCE, source_row_id, new_id,
                                          match.match_method, match.match_confidence)
                await upsert_identities(client, new_id, payload, SOURCE)
                if potential_dup_id:
                    await insert_merge_queue_row(client, potential_dup_id, new_id, match, payload, SOURCE)
                return RowResult(
                    source_row_id=source_row_id, ok=True, is_new=True, master_candidate_id=new_id,
                    match_method=match.match_method, match_confidence=match.match_confidence,
                    needs_review=match.needs_review)

            master_id = match.master_candidate_id
            if dry_run:
                return RowResult(
                    source_row_id=source_row_id, ok=True, is_new=False, master_candidate_id=master_id,
                    match_method=match.match_method, match_confidence=match.match_confidence)
            async with _acquire_all([_MASTER_LOCKS[_shard_for(master_id)]]):
                await _merge_into_existing(client, master_id, payload, SOURCE)
                await upsert_source_link(client, SOURCE, source_row_id, master_id,
                                          match.match_method, match.match_confidence)
                await upsert_identities(client, master_id, payload, SOURCE)
            return RowResult(
                source_row_id=source_row_id, ok=True, is_new=False, master_candidate_id=master_id,
                match_method=match.match_method, match_confidence=match.match_confidence)
    except Exception as e:
        return RowResult(source_row_id=source_row_id, ok=False, error=str(e)[:500])
