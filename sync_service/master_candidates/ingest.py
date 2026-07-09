"""
sync_service/master_candidates/ingest.py

Ingest worker: naukri_candidates → master_candidates.

Two entry paths:
  1. FAST PATH  — /mc/webhook/naukri-inserted pushes ids into WEBHOOK_QUEUE;
                  worker drains it within seconds of a capture.
  2. CURSOR POLL — (updated_at, id) cursor over naukri_candidates catches
                  everything (inserts AND full-profile updates), including
                  anything the webhook missed. This is the source of truth.

Identity resolution & merge stay in Postgres (ingest_master_candidate RPC,
mean ~60ms/call) — this worker does the field mapping in Python and drives
concurrency, which is the part that was burning Supabase edge-function CPU.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from .config import (
    SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE,
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
)
from .state import RunLog, advance_cursor, heartbeat, read_control

logger = logging.getLogger(__name__)

SOURCE = "portal_a"

# Fast-path queue fed by the webhook endpoint (naukri row ids)
WEBHOOK_QUEUE: asyncio.Queue[str] = asyncio.Queue(maxsize=10_000)

MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}


# ─────────────────────────────────────────────────────────────────────────────
# Normalizers (ported from the Phase 1 adapter contract §6a-6d)
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


def _parse_month_year(tok: str) -> tuple[int, int]:
    """ "Jul '25" → (2025, 7);  returns (0,0) on failure. """
    m = re.search(r"([A-Za-z]{3})\w*\s*'?(\d{2,4})", tok.strip())
    if not m:
        return 0, 0
    mon = MONTHS.get(m.group(1).lower()[:3], 0)
    yr = int(m.group(2))
    if yr < 100:
        yr += 2000 if yr < 70 else 1900
    return yr, mon


def parse_date_range(dr: Any) -> dict[str, Any]:
    """ "Jul '25 to till date" / "Mar '24 to Jul '25" → CO date fields. """
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
            field  = parts[1] if len(parts) > 2 else (parts[1] if len(parts) == 2 and not year else "")
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
# Row → ingest_master_candidate payload
# ─────────────────────────────────────────────────────────────────────────────
def map_row(row: dict[str, Any]) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    email = norm_email(row.get("email"))
    phone = norm_phone_e164(row.get("phone"))

    identifiers: dict[str, Any] = {}
    if row.get("encrypted_username"):
        identifiers["naukri_encrypted_username"] = str(row["encrypted_username"])
    if row.get("naukri_user_id"):
        identifiers["naukri_user_id"] = str(row["naukri_user_id"])
    if email:
        identifiers["email_normalized"] = email
    if phone:
        identifiers["phone_e164"] = phone

    skills = [str(s) for s in (row.get("key_skills_array") or []) if s]
    about = str(row.get("about") or "").strip()
    work_summary = str(row.get("work_summary") or "").strip()
    summary = " ".join(x for x in [about, work_summary] if x) or None

    exp_months = row.get("exp_total_months") or 0

    profile: dict[str, Any] = {
        "full_name":            row.get("name"),
        "title":                row.get("curr_designation"),
        "headline":             about[:220] or None,
        "summary":              summary,
        "profile_picture_url":  row.get("photo_url"),
        "location":             row.get("current_location"),
        "country":              "India",
        "industry":             row.get("industry"),
        "functional_area":      row.get("functional_area"),
        "role":                 row.get("role"),
        "seniority": ("entry" if exp_months < 24 else
                      "mid" if exp_months < 72 else
                      "senior" if exp_months < 144 else "lead") if exp_months else None,
        "company":              {"name": row.get("curr_organization")} if row.get("curr_organization") else None,
        "company_name":         row.get("curr_organization"),
        "experience":           build_experience(row),
        "education":            build_education(row),
        "skills":               skills,
        "certifications":       row.get("certifications_array") or row.get("certifications") or [],
        "languages":            build_languages(row),
        "contact_availability": {
            "personal_email": bool(email), "phone": bool(phone), "work_email": False},
        "current_ctc_display":    row.get("current_ctc_display"),
        "notice_period_display":  row.get("notice_period_display"),
        "total_experience_months": exp_months or None,
        "last_active_date":       row.get("naukri_active_date"),
        "experience_display":     row.get("experience_display"),
        "preferred_locations":    row.get("preferred_locations_array") or [],
        "current_location":       row.get("current_location"),
        "resume_url":             row.get("cv_download_url") or row.get("resume_file_url"),
        "resume_last_updated":    row.get("resume_last_updated"),
        "gender":            row.get("gender"),
        "dob":               row.get("dob"),
        "marital_status":    row.get("marital_status"),
        "category":          row.get("category"),
        "desired_job_type":  row.get("desired_job_type"),
        "employment_status_pref": row.get("employment_status_pref"),
        "work_auth_countries":    row.get("work_auth_countries") or [],
        "may_also_know_skills":   row.get("may_also_know_skills") or [],
        "has_full_profile":       bool(row.get("has_full_profile")),
    }
    profile = {k: v for k, v in profile.items() if v is not None}

    emails = ([{"value": email, "type": "personal", "source": SOURCE,
                "verified": bool(row.get("email_verified")),
                "confidence": 1.0, "added_at": now_iso}] if email else [])
    phones = ([{"value": phone, "type": "mobile", "source": SOURCE,
                "verified": bool(row.get("phone_verified")),
                "confidence": 1.0, "added_at": now_iso}] if phone else [])

    return {
        "p_source":          SOURCE,
        "p_source_row_id":   str(row["id"]),
        "p_organization_id": None,
        "p_identifiers":     identifiers,
        "p_profile":         profile,
        "p_emails":          emails,
        "p_phones":          phones,
        "p_raw_payload":     row,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RPC calls
# ─────────────────────────────────────────────────────────────────────────────
async def _ingest_one(client: httpx.AsyncClient, row: dict[str, Any]) -> tuple[bool, str | None]:
    try:
        payload = map_row(row)
        r = await client.post(
            f"{SUPABASE_REST}/rpc/ingest_master_candidate",
            headers=SB_HEADERS, json=payload, timeout=HTTP_TIMEOUT_SUPABASE)
        if r.status_code >= 400:
            return False, f"{r.status_code}: {r.text[:200]}"
        return True, None
    except Exception as e:
        return False, str(e)[:200]


async def _fetch_batch(client: httpx.AsyncClient,
                       after_ts: str | None, after_id: str | None,
                       limit: int) -> list[dict]:
    r = await client.post(
        f"{SUPABASE_REST}/rpc/get_naukri_rows_for_ingest",
        headers=SB_HEADERS,
        json={"p_after_updated_at": after_ts, "p_after_id": after_id, "p_limit": limit},
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return [x["source_row"] for x in r.json()]


async def _fetch_by_ids(client: httpx.AsyncClient, ids: list[str]) -> list[dict]:
    if not ids:
        return []
    r = await client.post(
        f"{SUPABASE_REST}/rpc/get_naukri_rows_by_ids",
        headers=SB_HEADERS, json={"p_ids": ids}, timeout=HTTP_TIMEOUT_SUPABASE)
    if r.status_code >= 400:      # RPC optional; fall back to poll catching it
        logger.warning(f"[mc-ingest] fetch_by_ids failed: {r.text[:200]}")
        return []
    return [x["source_row"] for x in r.json()]


async def _process_rows(client: httpx.AsyncClient, rows: list[dict],
                        concurrency: int, run: RunLog) -> None:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(row: dict) -> None:
        async with sem:
            ok, err = await _ingest_one(client, row)
            run.bump(processed=1, ingested=1 if ok else 0,
                     errors=0 if ok else 1, first_error=err)

    await asyncio.gather(*(one(r) for r in rows))


# ─────────────────────────────────────────────────────────────────────────────
# Worker loop
# ─────────────────────────────────────────────────────────────────────────────
async def run_ingest_loop() -> None:
    logger.info("[mc-ingest] starting ingest loop")
    async with httpx.AsyncClient() as client:
        # cursor is stored as "ISO|uuid" in cursor_naukri_id? No — keep two:
        # cursor_updated_at holds ts, cursor_naukri_id holds tie-break id.
        while True:
            try:
                ctrl = await read_control(client, "ingest")
                if not ctrl:
                    await asyncio.sleep(30)
                    continue
                await heartbeat(client, "ingest")
                if not ctrl.should_run:
                    # still drain webhook queue into oblivion? no — keep queued
                    await asyncio.sleep(min(ctrl.poll_interval_sec, 30))
                    continue

                run = RunLog(client, "ingest")
                await run.start(cursor_before=ctrl.cursor_updated_at)

                # 1. Fast path: drain webhook ids first (low latency)
                webhook_ids: list[str] = []
                while not WEBHOOK_QUEUE.empty() and len(webhook_ids) < 500:
                    webhook_ids.append(WEBHOOK_QUEUE.get_nowait())
                if webhook_ids:
                    rows = await _fetch_by_ids(client, webhook_ids)
                    if rows:
                        await _process_rows(client, rows, ctrl.concurrency, run)
                        run.bump(batches=1)

                # 2. Cursor poll: catches everything (inserts + updates)
                after_ts = ctrl.cursor_updated_at
                after_id = ctrl.cursor_naukri_id
                for _ in range(20):
                    rows = await _fetch_batch(client, after_ts, after_id, ctrl.batch_size)
                    if not rows:
                        break
                    await _process_rows(client, rows, ctrl.concurrency, run)
                    run.bump(batches=1)
                    after_ts = rows[-1].get("updated_at")
                    after_id = rows[-1].get("id")
                    # persist cursor after each batch
                    await advance_cursor(client, "ingest",
                                         cursor_updated_at=after_ts,
                                         cursor_naukri_id=after_id)
                    fresh = await read_control(client, "ingest")
                    if not fresh or not fresh.should_run:
                        break

                await run.finish(
                    "ok" if run.errors_count == 0 else "error",
                    cursor_after=after_ts,
                    note=f"processed={run.rows_processed} ok={run.rows_ingested} err={run.errors_count}")

            except Exception as e:
                logger.exception(f"[mc-ingest] loop error: {e}")
                await asyncio.sleep(15)

            ctrl = await read_control(client, "ingest")
            await asyncio.sleep(ctrl.poll_interval_sec if ctrl else 60)