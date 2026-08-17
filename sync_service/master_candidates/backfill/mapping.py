"""
sync_service/master_candidates/backfill/mapping.py

Field mapping: naukri_candidates row -> the payload shape
ingest_master_candidate's p_payload argument expects.

This is a deliberate, acknowledged fresh copy of the normalizer/mapper
functions in ../ingest.py (norm_email, norm_phone_e164, extract_linkedin_url,
derive_seniority, derive_completeness, parse_date_range, build_experience,
build_education, build_languages, map_row), copied verbatim rather than
imported, because ../ingest.py is explicitly out of scope for this migration
and must not be edited -- not even to add a shared import. Every function
body below is byte-for-byte identical to its ingest.py counterpart except
map_row()'s return shape (see note on map_row below).

Do NOT let this drift from ingest.py's copies without a reason -- if you fix
a bug or add a field here, the same fix likely belongs in ingest.py too
(tracked as a fast-follow to point ingest.py at this module instead of
maintaining two copies, once the backfill has shipped and stabilized).

The payload's field names mirror the original Deno buildPayload() contract
(supabase/functions/backfill-portal-a-to-master's adapter, and the sibling
ingest-portal-a-to-master) exactly, since that's the contract
ingest_master_candidate's SQL body was written against.
"""

import re
from datetime import datetime, timezone
from typing import Any, Optional

SOURCE = "portal_a"

MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}

LINKEDIN_URL_RE = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[^\s\"'<>)]+", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Normalizers (verbatim copy of ingest.py's normalizers)
# ─────────────────────────────────────────────────────────────────────────────
def norm_email(v: Any) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip().lower()
    if "@" not in s:
        return None
    local, _, domain = s.partition("@")
    local = local.split("+", 1)[0]
    return f"{local}@{domain}" if local and domain else None


def norm_phone_e164(v: Any) -> Optional[str]:
    if not v:
        return None
    digits = re.sub(r"\D", "", str(v))
    if not digits:
        return None
    if len(digits) == 10:                      # bare Indian mobile
        return f"+91{digits}"
    if digits.startswith("91") and len(digits) == 12:
        return f"+{digits}"
    if len(digits) >= 11:                      # already has some country code
        return f"+{digits}"
    return None


def extract_linkedin_url(text: Any) -> Optional[str]:
    if not text:
        return None
    m = LINKEDIN_URL_RE.search(str(text))
    if not m:
        return None
    return re.sub(r"[.,;]+$", "", m.group(0))


def derive_seniority(months: Optional[int]) -> Optional[str]:
    if not months or months <= 0:
        return None
    if months < 24:
        return "entry"
    if months < 72:
        return "mid"
    if months < 144:
        return "senior"
    if months < 240:
        return "lead"
    return "executive"


def derive_completeness(payload: dict[str, Any]) -> float:
    """0-1 rubric score, NOT authoritative - used for ranking/freshness only.
    Mirrors the Deno buildPayload()'s deriveCompleteness() exactly."""
    scalar_fields = [
        "full_name", "title", "headline", "summary", "location",
        "company_name", "current_ctc_display", "gender", "dob",
        "total_experience_months", "seniority",
    ]
    count = 0
    for f in scalar_fields:
        v = payload.get(f)
        if v not in (None, "", 0):
            count += 1
    if payload.get("experience"):        count += 2
    if payload.get("education"):         count += 2
    if payload.get("skills"):            count += 1
    if payload.get("languages"):         count += 1
    if payload.get("structured_skills"): count += 1
    if payload.get("available_emails"):  count += 2
    if payload.get("available_phones"):  count += 2
    max_score = len(scalar_fields) + 2 + 2 + 1 + 1 + 1 + 2 + 2
    return round((count / max_score) * 100) / 100


def _parse_month_year(tok: str) -> tuple[int, int]:
    """ "Jul '25" -> (2025, 7);  returns (0,0) on failure. """
    m = re.search(r"([A-Za-z]{3})\w*\s*'?(\d{2,4})", tok.strip())
    if not m:
        return 0, 0
    mon = MONTHS.get(m.group(1).lower()[:3], 0)
    yr = int(m.group(2))
    if yr < 100:
        yr += 2000 if yr < 70 else 1900
    return yr, mon


def parse_date_range(dr: Any) -> dict[str, Any]:
    """ "Jul '25 to till date" / "Mar '24 to Jul '25" -> CO date fields. """
    out = {"start_date_year": 0, "start_date_month": 0,
           "end_date_year": 0, "end_date_month": 0, "is_current": False}
    if not dr:
        return out
    s = str(dr)
    parts = re.split(r"\s+to\s+", s, flags=re.IGNORECASE)
    if parts:
        y, m = _parse_month_year(parts[0])
        out["start_date_year"], out["start_date_month"] = y, m
    if len(parts) > 1:
        if re.search(r"till|present|current", parts[1], re.IGNORECASE):
            out["is_current"] = True
        else:
            y, m = _parse_month_year(parts[1])
            out["end_date_year"], out["end_date_month"] = y, m
    return out


def _packed(y: int, m: int) -> str:
    return f"{y}{m:02d}" if y else "00"


def build_experience(row: dict[str, Any]) -> list[dict[str, Any]]:
    hist = row.get("historical_employment") or []
    if isinstance(hist, list) and hist:
        out = []
        for e in hist:
            if not isinstance(e, dict):
                continue
            d = parse_date_range(e.get("date_range"))
            out.append({
                "title":            e.get("designation") or e.get("title") or "",
                "company_name":     e.get("organization") or e.get("company") or "",
                "summary":          e.get("description") or e.get("summary"),
                "domain": None, "locality": None, "logo_url": None, "linkedin_url": None,
                "is_current":       d["is_current"],
                "start_date":       _packed(d["start_date_year"], d["start_date_month"]),
                "end_date":         _packed(d["end_date_year"], d["end_date_month"]) if not d["is_current"] else "00",
                "start_date_year":  d["start_date_year"],
                "start_date_month": d["start_date_month"],
                "end_date_year":    0 if d["is_current"] else d["end_date_year"],
                "end_date_month":   0 if d["is_current"] else d["end_date_month"],
                "source": SOURCE,
            })
        if out:
            return out
    # search-batch fallback: synthetic current entry
    if row.get("curr_designation") or row.get("curr_organization"):
        return [{
            "title":         row.get("curr_designation") or "",
            "company_name":  row.get("curr_organization") or "",
            "is_current":    True,
            "start_date_year": None, "start_date_month": None,
            "end_date_year": 0, "end_date_month": 0,
            "start_date": "00", "end_date": "00",
            "source": SOURCE, "partial": True,
        }]
    return []


def build_education(row: dict[str, Any]) -> list[dict[str, Any]]:
    edu = row.get("education")
    out: list[dict[str, Any]] = []
    if isinstance(edu, list) and edu:
        for e in edu:
            if not isinstance(e, dict):
                continue
            spec = str(e.get("course_spec_year") or "")
            parts = [p.strip() for p in spec.split(",")]
            year = parts[-1] if parts and re.fullmatch(r"\d{4}", parts[-1] or "") else ""
            degree = parts[0] if parts else ""
            field = parts[1] if len(parts) > 2 else (parts[1] if len(parts) == 2 and not year else "")
            out.append({
                "school_name":     e.get("institute") or "",
                "degree":          degree,
                "field_of_study":  field,
                "start_date_year": "",
                "end_date_year":   year,
                "url": None, "location": None, "description": None,
                "source": SOURCE,
            })
        if out:
            return out
    # search-batch fallback: education_summary "Degree, Field, Institute,Year"
    summary = str(row.get("education_summary") or "").strip()
    if summary:
        for chunk in summary.split("|"):
            parts = [p.strip() for p in chunk.split(",") if p.strip()]
            if not parts:
                continue
            year = parts[-1] if re.fullmatch(r"\d{4}", parts[-1]) else ""
            body = parts[:-1] if year else parts
            out.append({
                "school_name":    body[-1] if len(body) >= 2 else "",
                "degree":         body[0] if body else "",
                "field_of_study": body[1] if len(body) >= 3 else "",
                "start_date_year": "", "end_date_year": year,
                "source": SOURCE, "partial": True,
            })
    return out


def build_languages(row: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for lang in (row.get("languages_known") or []):
        s = str(lang)
        name = s.split("-", 1)[0].strip()
        prof_m = re.search(r"-\s*([A-Za-z]+)", s)
        skills = re.findall(r"(Read|Write|Speak)", s, re.IGNORECASE)
        if name:
            out.append({
                "name": name,
                "proficiency": prof_m.group(1) if prof_m else None,
                "skills": [x.capitalize() for x in skills],
                "source": SOURCE,
            })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Row -> ingest_master_candidate's p_payload shape
#
# NOTE on the one intentional structural difference from ingest.py's
# map_row(): ingest.py's map_row() returns {"p_source", "p_source_row_id",
# "p_payload"} because it POSTs that wrapper straight to the RPC. This
# module's map_row() returns the payload dict directly (unwrapped) -- the
# field-mapping computation inside is otherwise identical. Callers here
# (backfill/match.py) already know p_source ("portal_a", a module constant)
# and p_source_row_id (row["id"]) themselves, so the wrapper served no
# purpose once there's no RPC call to address it to.
# ─────────────────────────────────────────────────────────────────────────────
def map_row(row: dict[str, Any]) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    email = norm_email(row.get("email"))
    phone = norm_phone_e164(row.get("phone"))

    about = str(row.get("about") or "").strip()
    work_summary = str(row.get("work_summary") or "").strip()
    summary = " ".join(x for x in [about, work_summary] if x) or None

    exp_months = row.get("exp_total_months") or 0
    exp_years = round((exp_months / 12) * 10) / 10 if exp_months else None

    skills = [str(s) for s in (row.get("key_skills_array") or []) if s]
    functional_area = row.get("functional_area")

    available_emails = ([{
        "value": email, "type": "personal", "source": SOURCE,
        "verified": bool(row.get("email_verified")),
        "confidence": 1.0, "added_at": now_iso,
    }] if email else [])
    available_phones = ([{
        "value": phone, "type": "mobile", "source": SOURCE,
        "verified": bool(row.get("phone_verified")),
        "confidence": 1.0, "added_at": now_iso,
    }] if phone else [])

    structured_skills = [
        {
            "skill":     s.get("skill") if isinstance(s, dict) else None,
            "exp_txt":   s.get("exp_txt") if isinstance(s, dict) else None,
            "version":   s.get("version") if isinstance(s, dict) else None,
            "last_used": s.get("last_used") if isinstance(s, dict) else None,
            "source":    SOURCE,
        }
        for s in (row.get("it_skills") or []) if isinstance(s, dict)
    ]

    payload: dict[str, Any] = {
        # identifiers
        "linkedin_url":                extract_linkedin_url(about) or extract_linkedin_url(work_summary),
        "li_vanity":                   None,
        "apollo_person_id":            None,
        "rocketreach_id":              None,
        "portal_a_encrypted_username": row.get("encrypted_username"),
        "portal_a_sid":                row.get("sid") or row.get("parent_sid"),
        "portal_a_user_id":            str(row["naukri_user_id"]) if row.get("naukri_user_id") else None,
        "portal_a_res_id":             str(row["naukri_res_id"]) if row.get("naukri_res_id") else None,

        # matching contact points (used by the resolver, not stored as-is)
        "primary_email": row.get("email"),
        "primary_phone": row.get("phone"),

        # core profile
        "full_name":            row.get("name"),
        "title":                row.get("curr_designation"),
        "headline":             (about[:220] or None),
        "summary":              summary,
        "profile_picture_url":  row.get("photo_url"),
        "location":             row.get("current_location"),
        "country":              "India",
        "industry":             row.get("industry"),
        "job_function":         functional_area.split("-")[0].strip() if functional_area else None,
        "seniority":            derive_seniority(exp_months),
        "work_status":          None,
        "followers":            None,

        # current company
        "company":          {"name": row.get("curr_organization")} if row.get("curr_organization") else {},
        "company_name":     row.get("curr_organization"),
        "company_domain":   None,
        "company_industry": row.get("industry"),
        "company_size":     None,

        # multi-entry arrays
        "experience":               build_experience(row),
        "education":                build_education(row),
        "skills":                   skills,
        "certifications":           row.get("certifications_array") or row.get("certifications") or [],
        "publications":             [],
        "projects":                 row.get("projects") or [],
        "languages":                build_languages(row),
        "volunteering_experiences": [],
        "awards":                   [],
        "may_also_know_skills":     row.get("may_also_know_skills") or [],
        "structured_skills":        structured_skills,

        # contact
        "contact_availability": {
            "phone": bool(phone), "work_email": False, "personal_email": bool(email)},
        "available_emails": available_emails,
        "available_phones": available_phones,

        # recruitment extras
        "current_ctc_lacs":       row.get("current_ctc_lacs"),
        "expected_ctc_lacs":      row.get("expected_ctc_lacs"),
        "current_ctc_display":   row.get("current_ctc_display"),
        "notice_period_days":    None,
        "notice_period_display": row.get("notice_period_display"),
        "total_experience_years":  exp_years,
        "total_experience_months": exp_months or None,
        "experience_display":     row.get("experience_display"),
        "preferred_locations":    row.get("preferred_locations_array") or [],
        "current_location":       row.get("current_location"),
        "functional_area":        functional_area,
        "role":                   row.get("role"),
        "resume_url":             row.get("cv_download_url") or row.get("resume_file_url"),
        "resume_text":            None,
        "resume_last_updated":    row.get("resume_last_updated"),
        "gender":            row.get("gender"),
        "dob":               row.get("dob"),
        "marital_status":    row.get("marital_status"),
        "category":          row.get("category"),
        "disability":        row.get("disability"),
        "desired_job_type":  row.get("desired_job_type"),
        "employment_status_pref": row.get("employment_status_pref"),
        "work_auth_countries":    row.get("work_auth_countries") or [],

        # meta
        "has_full_profile": bool(row.get("has_full_profile")),
        "data_freshness": (row.get("updated_at") or row.get("search_batch_at")
                            or row.get("localdb_profile_at") or row.get("resdex_profile_at")
                            or row.get("captured_at") or now_iso),
        "last_active_date": row.get("naukri_active_date"),

        # kept lean - naukri_candidates remains the source-of-truth for raw HTML/JSON
        "raw_metadata": {
            "source_row_id":      str(row.get("id")),
            "encrypted_username": row.get("encrypted_username"),
            "captured_from":      row.get("captured_from"),
            "captured_at":        row.get("captured_at"),
            "search_batch_at":    row.get("search_batch_at"),
            "localdb_profile_at": row.get("localdb_profile_at"),
            "resdex_profile_at":  row.get("resdex_profile_at"),
            "first_captured_via": row.get("first_captured_via"),
            "last_captured_via":  row.get("last_captured_via"),
            "capture_count":      row.get("capture_count"),
            "rec_id":             row.get("rec_id"),
            "sid":                row.get("sid"),
            "parent_sid":         row.get("parent_sid"),
            "sid_group_id":       row.get("sid_group_id"),
            "ldb_freshness":      row.get("ldb_freshness"),
            "it_skills_raw":      row.get("it_skills") if isinstance(row.get("it_skills"), list) else None,
        },
    }
    payload["profile_completeness"] = derive_completeness(payload)
    return payload
