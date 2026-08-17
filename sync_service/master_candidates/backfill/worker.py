"""
sync_service/master_candidates/backfill/worker.py

Background job orchestration for the portal_a backfill: page fetch over
naukri_candidates via a composite (updated_at, id) cursor (plain PostgREST,
no RPC), chunked concurrent processing via match.ingest_backfill_row(),
cursor persistence after every chunk, and the single-claimant / never-
gives-up-on-transient-errors resumability design.

Two independent async loops live here, both started once from main.py's
lifespan and left running for the life of the process:
  - run_backfill_loop(): the actual job. A `while True` that only exits
    when desired_state leaves "running" (explicit /pause or /stop, or
    genuine completion) -- never on a transient error, which is instead
    logged and retried after a short sleep.
  - claim_watchdog(): a persistent retry loop, NOT a one-shot boot check.
    If this instance starts (or is already running) while another
    instance's heartbeat is still fresh, it keeps checking every tick
    rather than giving up -- so if that peer crashes, this instance picks
    the job up automatically without needing a restart.

Dry-run (run_dry_run) is intentionally NOT part of this state machine at
all: it never touches _current_task/_stop_event/the control row's cursor,
runs synchronously within one call, and is safe to invoke at any time
regardless of whether a real job is running.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from ..config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE
from . import match
from . import state
from .config import (
    DEFAULT_BATCH_SIZE, DEFAULT_CONCURRENCY,
    CLAIM_WATCHDOG_INTERVAL_SEC, CYCLE_ERROR_SLEEP_SEC,
    DEFAULT_DRY_RUN_PAGES, AUTO_RESUME,
)

logger = logging.getLogger(__name__)

_INSTANCE_ID = uuid.uuid4().hex

_current_task: Optional[asyncio.Task] = None
_watchdog_task: Optional[asyncio.Task] = None
_stop_event = asyncio.Event()

# naukri_candidates columns mapping.map_row() actually reads, plus id/
# updated_at -- deliberately an explicit include-list rather than select=*,
# so the heavy HTML/raw-json blob columns (raw_search_row_html,
# raw_profile_html, attached_resume_html, raw_json) are never fetched,
# without needing this service's full naukri_candidates schema to exclude
# them by name.
SELECT_FIELDS = (
    "id,updated_at,email,phone,about,work_summary,exp_total_months,"
    "key_skills_array,functional_area,email_verified,phone_verified,it_skills,"
    "encrypted_username,sid,parent_sid,naukri_user_id,naukri_res_id,name,"
    "curr_designation,photo_url,current_location,industry,curr_organization,"
    "historical_employment,education,education_summary,certifications_array,"
    "certifications,projects,languages_known,languages,may_also_know_skills,"
    "current_ctc_lacs,expected_ctc_lacs,current_ctc_display,notice_period_display,"
    "experience_display,preferred_locations_array,role,cv_download_url,"
    "resume_file_url,resume_last_updated,gender,dob,marital_status,category,"
    "disability,desired_job_type,employment_status_pref,work_auth_countries,"
    "has_full_profile,search_batch_at,localdb_profile_at,resdex_profile_at,"
    "captured_at,naukri_active_date,captured_from,first_captured_via,"
    "last_captured_via,capture_count,rec_id,sid_group_id,ldb_freshness"
)


@dataclass
class RunState:
    batch_number: int = 0
    rows_attempted: int = 0
    rows_inserted: int = 0
    rows_merged: int = 0
    errors_count: int = 0
    last_batch_at: Optional[str] = None


_run_state = RunState()


def is_running() -> bool:
    return _current_task is not None and not _current_task.done()


def _chunked(items: list, size: int):
    step = max(1, size)
    for i in range(0, len(items), step):
        yield items[i:i + step]


async def fetch_page(client: httpx.AsyncClient, after_ts: Optional[str],
                      after_id: Optional[str], limit: int) -> list[dict]:
    """Composite (updated_at, id) cursor page fetch -- same pattern
    main.py::run_full_reindex already uses for hr_talent_pool, chosen
    specifically because this codebase already fixed the bug class a
    single `gt` cursor is prone to (bulk-imported rows sharing identical
    timestamps skipping past a naive cursor)."""
    params: dict[str, str] = {
        "select": SELECT_FIELDS,
        "order": "updated_at.asc,id.asc",
        "limit": str(limit),
    }
    if after_ts is not None:
        if after_id is not None:
            params["or"] = f"(updated_at.gt.{after_ts},and(updated_at.eq.{after_ts},id.gt.{after_id}))"
        else:
            params["updated_at"] = f"gt.{after_ts}"
    r = await client.get(f"{SUPABASE_REST}/naukri_candidates", params=params,
                          headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()


def _start_task() -> None:
    global _current_task
    _current_task = asyncio.create_task(run_backfill_loop())


# ─────────────────────────────────────────────────────────────────────────────
# The real, stateful job
# ─────────────────────────────────────────────────────────────────────────────
async def run_backfill_loop() -> None:
    global _run_state
    _run_state = RunState()
    _stop_event.clear()
    logger.info(f"[mc-backfill] loop starting (instance={_INSTANCE_ID})")

    async with httpx.AsyncClient() as client:
        while True:
            progress_id: Optional[str] = None
            try:
                ctrl = await state.read_control(client)
                if not ctrl or not ctrl.should_run:
                    logger.info("[mc-backfill] desired_state left 'running' -- loop exiting")
                    return

                page = await fetch_page(client, ctrl.cursor_updated_at, ctrl.cursor_id, ctrl.batch_size)
                if not page:
                    await state.mark_completed(client)
                    logger.info("[mc-backfill] reached end of table -- marking completed")
                    return

                _run_state.batch_number += 1
                batch_number = _run_state.batch_number
                progress_id = await state.start_batch_progress_row(client, batch_number)

                batch_attempted = batch_inserted = batch_merged = batch_errors = 0
                last_row_id: Optional[str] = None
                stopped_mid_page = False

                for chunk in _chunked(page, ctrl.concurrency):
                    if _stop_event.is_set():
                        stopped_mid_page = True
                        break

                    results: list[match.RowResult] = await asyncio.gather(
                        *(match.ingest_backfill_row(client, row) for row in chunk))

                    ok_new = sum(1 for r in results if r.ok and r.is_new)
                    ok_merged = sum(1 for r in results if r.ok and not r.is_new)
                    failed = [r for r in results if not r.ok]

                    batch_attempted += len(results)
                    batch_inserted += ok_new
                    batch_merged += ok_merged
                    batch_errors += len(failed)

                    if failed:
                        await state.write_error_rows(
                            client, progress_id,
                            [{"source_row_id": r.source_row_id, "error": r.error} for r in failed])

                    last_row = chunk[-1]
                    after_ts = last_row.get("updated_at")
                    after_id = str(last_row.get("id"))
                    last_row_id = after_id
                    await state.advance_cursor(client, after_ts, after_id, batch_number)
                    await state.heartbeat(client, _INSTANCE_ID)

                    _run_state.rows_attempted += len(results)
                    _run_state.rows_inserted += ok_new
                    _run_state.rows_merged += ok_merged
                    _run_state.errors_count += len(failed)
                    _run_state.last_batch_at = datetime.now(timezone.utc).isoformat()

                    if _stop_event.is_set():
                        stopped_mid_page = True
                        break

                await state.finish_batch_progress_row(
                    client, progress_id,
                    rows_attempted=batch_attempted, rows_inserted=batch_inserted,
                    rows_merged=batch_merged, errors_count=batch_errors,
                    last_source_row_id=last_row_id, status="completed")

                if stopped_mid_page:
                    logger.info("[mc-backfill] pause/stop requested -- loop exiting")
                    return

            except Exception as e:
                logger.exception(f"[mc-backfill] cycle error: {e}")
                if progress_id is not None:
                    try:
                        await state.finish_batch_progress_row(
                            client, progress_id, rows_attempted=0, rows_inserted=0,
                            rows_merged=0, errors_count=0, last_source_row_id=None, status="failed")
                    except Exception:
                        logger.warning("[mc-backfill] failed to finalize crashed batch's progress row")
                try:
                    await state.record_transient_error(client, str(e))
                except Exception:
                    logger.warning("[mc-backfill] failed to record transient error note")
                # Never flips desired_state away from "running" -- a
                # transient error must not halt an otherwise-running job.
                await asyncio.sleep(CYCLE_ERROR_SLEEP_SEC)
                continue


# ─────────────────────────────────────────────────────────────────────────────
# Single-claimant watchdog -- persistent, not a one-shot boot check
# ─────────────────────────────────────────────────────────────────────────────
async def claim_watchdog() -> None:
    """Runs for the life of the process. AUTO_RESUME=false (an emergency
    ops override, not the expected operating mode) disables the automatic
    claim attempt entirely -- desired_state may say "running" after a
    restart, but nothing here will act on it; an explicit POST /start is
    then required. AUTO_RESUME=true (the default) is what makes a running
    backfill survive a container restart and keep going until it actually
    finishes or a superadmin explicitly pauses/stops it."""
    logger.info(f"[mc-backfill] claim watchdog starting (instance={_INSTANCE_ID}, auto_resume={AUTO_RESUME})")
    async with httpx.AsyncClient() as client:
        try:
            await state.ensure_control_row(client)
        except Exception as e:
            logger.warning(f"[mc-backfill] ensure_control_row failed: {e}")
        while True:
            try:
                if AUTO_RESUME and not is_running():
                    ctrl = await state.read_control(client)
                    if ctrl and ctrl.should_run:
                        claimed = await state.try_claim(client, _INSTANCE_ID)
                        if claimed:
                            _start_task()
                            logger.warning(f"[mc-backfill] claimed job (instance={_INSTANCE_ID})")
                        # else: someone else's heartbeat is still fresh this
                        # tick -- do nothing and retry next tick, this is a
                        # loop, not a one-time attempt.
            except Exception as e:
                logger.warning(f"[mc-backfill] claim watchdog tick failed: {e}")
            await asyncio.sleep(CLAIM_WATCHDOG_INTERVAL_SEC)


def start_watchdog() -> None:
    global _watchdog_task
    if _watchdog_task is None or _watchdog_task.done():
        _watchdog_task = asyncio.create_task(claim_watchdog())


def cancel_all() -> None:
    """Called from main.py's lifespan shutdown."""
    if _watchdog_task is not None:
        _watchdog_task.cancel()
    if _current_task is not None:
        _current_task.cancel()


# ─────────────────────────────────────────────────────────────────────────────
# Public entry points for api.py
# ─────────────────────────────────────────────────────────────────────────────
async def start(client: httpx.AsyncClient, batch_size: Optional[int] = None,
                 concurrency: Optional[int] = None) -> dict[str, Any]:
    if is_running():
        return {"ok": False, "already_running": True}
    ctrl = await state.read_control(client)
    resuming = bool(ctrl and ctrl.desired_state == "paused")
    await state.mark_running(client, batch_size=batch_size, concurrency=concurrency,
                              fresh_session=not resuming)
    claimed = await state.try_claim(client, _INSTANCE_ID)
    if claimed:
        _start_task()
    # If not claimed, some other instance's heartbeat is fresh -- that
    # instance's own worker (or its watchdog) is already running the job;
    # nothing more to do here.
    return {"ok": True, "already_running": False, "resumed": resuming, "claimed_here": claimed}


async def request_pause(client: httpx.AsyncClient) -> None:
    _stop_event.set()
    await state.mark_paused(client)


async def request_stop(client: httpx.AsyncClient) -> None:
    _stop_event.set()
    await state.mark_stopped(client)


async def get_status(client: httpx.AsyncClient) -> dict[str, Any]:
    ctrl = await state.read_control(client)
    running_here = is_running()
    return {
        "desired_state": ctrl.desired_state if ctrl else "stopped",
        "is_this_process_running": running_here,
        "last_run_status": ctrl.last_run_status if ctrl else None,
        "last_error": ctrl.last_error if ctrl else None,
        "cursor_updated_at": ctrl.cursor_updated_at if ctrl else None,
        "cursor_id": ctrl.cursor_id if ctrl else None,
        "session_started_at": ctrl.session_started_at if ctrl else None,
        "batch_size": ctrl.batch_size if ctrl else DEFAULT_BATCH_SIZE,
        "concurrency": ctrl.concurrency if ctrl else DEFAULT_CONCURRENCY,
        "worker_instance_id": ctrl.worker_instance_id if ctrl else None,
        "worker_heartbeat_at": ctrl.worker_heartbeat_at if ctrl else None,
        "current_run": {
            "batch_number": _run_state.batch_number,
            "rows_attempted": _run_state.rows_attempted,
            "rows_inserted": _run_state.rows_inserted,
            "rows_merged": _run_state.rows_merged,
            "errors_count": _run_state.errors_count,
            "last_batch_at": _run_state.last_batch_at,
        } if running_here else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dry run -- structurally separate, synchronous, provably non-mutating.
# Never touches _current_task/_stop_event/the control row's cursor.
# ─────────────────────────────────────────────────────────────────────────────
async def run_dry_run(client: httpx.AsyncClient, batch_size: int, concurrency: int,
                       max_pages: int = DEFAULT_DRY_RUN_PAGES) -> dict[str, Any]:
    ctrl = await state.read_control(client)
    after_ts = ctrl.cursor_updated_at if ctrl else None
    after_id = ctrl.cursor_id if ctrl else None

    rows_previewed = would_insert = would_merge = would_need_review = would_error = 0
    sample: list[dict[str, Any]] = []

    for _ in range(max_pages):
        page = await fetch_page(client, after_ts, after_id, batch_size)
        if not page:
            break
        for chunk in _chunked(page, concurrency):
            results = await asyncio.gather(
                *(match.ingest_backfill_row(client, row, dry_run=True) for row in chunk))
            for r in results:
                rows_previewed += 1
                if not r.ok:
                    would_error += 1
                elif r.is_new:
                    would_insert += 1
                else:
                    would_merge += 1
                if r.needs_review:
                    would_need_review += 1
                if len(sample) < 20:
                    sample.append({
                        "source_row_id": r.source_row_id, "ok": r.ok, "is_new": r.is_new,
                        "match_method": r.match_method, "match_confidence": r.match_confidence,
                        "needs_review": r.needs_review, "error": r.error,
                    })
        last_row = page[-1]
        # Local variables only -- never written to state.advance_cursor /
        # master_candidates_backfill_control. This is the entire non-
        # mutation guarantee for dry-run in one place.
        after_ts, after_id = last_row.get("updated_at"), str(last_row.get("id"))

    return {
        "rows_previewed": rows_previewed, "would_insert": would_insert,
        "would_merge": would_merge, "would_need_review": would_need_review,
        "would_error": would_error, "sample": sample,
    }
