"""
sync_service/master_candidates/indexer.py

Two things live here:
  1. transform_row()  — pure function: master_candidates row (dict) → Typesense doc
  2. run_index_loop() — background worker: polls Supabase, upserts to Typesense
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from .config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE
from .state import RunLog, advance_cursor, heartbeat, read_control
from .typesense_client import upsert_batch, delete_id

logger = logging.getLogger(__name__)


# ── Selected columns pulled from master_candidates ─────────────────────────
# Everything the transformer needs; nothing huge (no raw_profile_by_source,
# no full experience/education arrays — those stay in Postgres for detail view).
SELECT_COLUMNS = ",".join([
    "id","primary_source","sources","linkedin_url","full_name","title","headline",
    "summary","profile_picture_url","location","country","industry","seniority",
    "followers","company_name","current_location","functional_area","role",
    "skills","experience","education","certifications","languages","contact_availability",
    "available_emails","available_phones","preferred_locations",
    "current_ctc_lacs","expected_ctc_lacs","current_ctc_display",
    "notice_period_days","notice_period_display",
    "total_experience_months","experience_display",
    "has_full_profile","has_contact","data_freshness","last_active_date",
    "updated_at",
])


# ── Pure transformer ───────────────────────────────────────────────────────
def _to_ts(v: Any) -> int:
    """String/timestamp → unix seconds; None → 0."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    try:
        # Postgres returns "2026-07-08T04:00:00.123+00:00"
        s = str(v).replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


def _truncate(s: Any, n: int) -> str | None:
    if not s:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]


def transform_row(row: dict[str, Any]) -> dict[str, Any]:
    """Row from master_candidates → Typesense document."""
    skills_list = [str(s) for s in (row.get("skills") or []) if s]

    experience = row.get("experience") or []
    all_titles     = [e.get("title") for e in experience if isinstance(e, dict) and e.get("title")]
    all_employers  = [e.get("company_name") for e in experience if isinstance(e, dict) and e.get("company_name")]

    education = row.get("education") or []
    schools         = [e.get("school_name") for e in education if isinstance(e, dict) and e.get("school_name")]
    degrees         = [e.get("degree")      for e in education if isinstance(e, dict) and e.get("degree")]
    fields_of_study = [e.get("field_of_study") for e in education if isinstance(e, dict) and e.get("field_of_study")]

    certifications = row.get("certifications") or []
    cert_names     = [c.get("name") for c in certifications if isinstance(c, dict) and c.get("name")]

    doc: dict[str, Any] = {
        "id":                    str(row["id"]),
        "full_name":             row.get("full_name") or "",
        "title":                 row.get("title") or "",
        "headline":              _truncate(row.get("headline"), 300),
        "summary_short":         _truncate(row.get("summary"), 1500),

        "skills_text":           " ".join(skills_list),
        "all_titles_text":       " ".join([t for t in all_titles if t]),
        "all_employers_text":    " ".join([e for e in all_employers if e]),
        "schools_text":          " ".join([s for s in schools if s]),
        "degrees_text":          " ".join([d for d in degrees if d]),
        "fields_of_study_text":  " ".join([f for f in fields_of_study if f]),
        "certifications_text":   " ".join([c for c in cert_names if c]),

        "skills":               skills_list,
        "all_titles":           [t for t in all_titles if t],
        "all_employers":        [e for e in all_employers if e],
        "current_employer":     row.get("company_name"),
        "location":             row.get("current_location") or row.get("location"),
        "preferred_locations":  list(row.get("preferred_locations") or []),
        "schools":              [s for s in schools if s],
        "degrees":              [d for d in degrees if d],
        "fields_of_study":      [f for f in fields_of_study if f],

        "has_full_profile": bool(row.get("has_full_profile")),
        "has_contact":      bool(row.get("has_contact")),
        "sources":          list(row.get("sources") or []),
        "primary_source":   row.get("primary_source") or "unknown",
        "seniority":        row.get("seniority"),
        "country":          row.get("country"),

        "total_experience_months": row.get("total_experience_months") or 0,
        "current_ctc_lacs":        float(row["current_ctc_lacs"]) if row.get("current_ctc_lacs") is not None else None,
        "expected_ctc_lacs":       float(row["expected_ctc_lacs"]) if row.get("expected_ctc_lacs") is not None else None,
        "notice_period_days":      row.get("notice_period_days") or 0,
        "followers":               row.get("followers") or 0,
        "data_freshness_ts":       _to_ts(row.get("data_freshness") or row.get("updated_at")),
        "last_active_date_ts":     _to_ts(row.get("last_active_date")),

        "languages":              [l.get("name") for l in (row.get("languages") or [])
                                   if isinstance(l, dict) and l.get("name")],
        "last_active_date":       str(row.get("last_active_date")) if row.get("last_active_date") else None,
        "contact_personal_email": bool((row.get("contact_availability") or {}).get("personal_email")),
        "contact_phone":          bool((row.get("contact_availability") or {}).get("phone")),
        "summary_full":           _truncate(row.get("summary"), 4000),

        "experience_json":     json.dumps((row.get("experience") or [])[:20], ensure_ascii=False),
        "education_json":      json.dumps((row.get("education") or [])[:10], ensure_ascii=False),
        "certifications_json": json.dumps((row.get("certifications") or [])[:15], ensure_ascii=False),
        "emails_json":         json.dumps(row.get("available_emails") or [], ensure_ascii=False),
        "phones_json":         json.dumps(row.get("available_phones") or [], ensure_ascii=False),

        "linkedin_url":            row.get("linkedin_url"),
        "profile_picture_url":     row.get("profile_picture_url"),
        "current_ctc_display":     row.get("current_ctc_display"),
        "experience_display":      row.get("experience_display"),
        "notice_period_display":   row.get("notice_period_display"),
    }
    return {k: v for k, v in doc.items() if v is not None}


# ── Fetch page of rows updated after cursor ────────────────────────────────
async def _fetch_updated_since(
    client: httpx.AsyncClient, cursor: str | None, batch_size: int
) -> list[dict]:
    params = {
        "select": SELECT_COLUMNS,
        "order":  "updated_at.asc",
        "limit":  str(batch_size),
    }
    if cursor:
        params["updated_at"] = f"gt.{cursor}"
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates",
        headers=SB_HEADERS, params=params, timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()
    return r.json()


# ── Index a specific list of ids on demand (used by webhook) ──────────────
async def index_ids(client: httpx.AsyncClient, ids: list[str]) -> tuple[int, list[str]]:
    if not ids:
        return 0, []
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates",
        headers=SB_HEADERS,
        params={"select": SELECT_COLUMNS, "id": f"in.({','.join(ids)})"},
        timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()
    rows = r.json()
    docs = [transform_row(row) for row in rows]
    return await upsert_batch(client, docs)


# ── Background loop ────────────────────────────────────────────────────────
async def run_index_loop() -> None:
    """
    Polling loop with the exact cadence pattern from your existing poller.py:
    read control, tick, sleep. Cursor and heartbeat live in Supabase so state
    survives container restarts.
    """
    logger.info("[mc-index] starting index loop")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                ctrl = await read_control(client, "index")
                if not ctrl:
                    logger.warning("[mc-index] no control row; sleeping 30s")
                    await asyncio.sleep(30)
                    continue

                await heartbeat(client, "index")

                if not ctrl.should_run:
                    await asyncio.sleep(min(ctrl.poll_interval_sec, 30))
                    continue

                run = RunLog(client, "index")
                await run.start(cursor_before=ctrl.cursor_updated_at)

                cursor = ctrl.cursor_updated_at
                did_work = False
                hit_batch_cap = True

                # Process up to N batches per cycle to keep loop responsive
                for _ in range(20):
                    rows = await _fetch_updated_since(client, cursor, ctrl.batch_size)
                    if not rows:
                        hit_batch_cap = False
                        break
                    docs = [transform_row(row) for row in rows]
                    ok, errors = await upsert_batch(client, docs)
                    run.bump(batches=1, processed=len(rows), indexed=ok,
                             errors=len(errors),
                             first_error=errors[0] if errors else None)
                    cursor = rows[-1]["updated_at"]
                    await advance_cursor(client, "index", cursor_updated_at=cursor)
                    did_work = True

                    # Mid-run pause check — respect UI toggle mid-cycle
                    fresh = await read_control(client, "index")
                    if not fresh or not fresh.should_run:
                        await run.finish("paused_mid_run", cursor_after=cursor)
                        hit_batch_cap = False
                        did_work = False  # already finished
                        break

                if did_work:
                    note = (f"batch_cap after {run.batches} batches"
                            if hit_batch_cap
                            else f"processed={run.rows_processed} indexed={run.rows_indexed}")
                    await run.finish("ok", cursor_after=cursor, note=note)
                elif hit_batch_cap:
                    # loop exited via break with did_work already False (rare)
                    pass
                else:
                    await run.finish("ok", cursor_after=cursor, note="no_new_rows")

            except Exception as e:
                logger.exception(f"[mc-index] loop error: {e}")
                await asyncio.sleep(15)

            # Sleep between cycles
            ctrl = await read_control(client, "index")
            interval = ctrl.poll_interval_sec if ctrl else 60
            await asyncio.sleep(interval)