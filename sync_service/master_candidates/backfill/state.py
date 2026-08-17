"""
sync_service/master_candidates/backfill/state.py

Persistence for the backfill job's control state (master_candidates_
backfill_control -- new table, singleton row keyed by process_name), and
its per-batch history / errors (master_candidates_backfill_progress /
master_candidates_backfill_errors -- existing tables, unchanged shape,
still written the same way the Edge Function always did). All access is
plain PostgREST HTTP -- no Supabase RPC calls anywhere in this module.

NOTE (pre-flight checklist item): master_candidates_backfill_progress's
exact column list/types were not available when this was written -- the
write shape below matches the original Edge Function's own insert/update
calls exactly (batch_number, source, status, rows_attempted, rows_inserted,
rows_merged, rows_skipped, errors_count, last_source_row_id, completed_at),
and the status literals used ('running'/'completed'/'failed') are the exact
ones that Edge Function used, since those are proven to pass whatever CHECK
constraint exists. The one still-unconfirmed detail is the column this
module orders "recent batches" by (see get_recent_batches) -- confirm the
real timestamp column name against the live schema before relying on it,
since batch ids are UUIDs and are not sortable by insertion order.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from ..config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE
from .config import SOURCE, DEFAULT_BATCH_SIZE, DEFAULT_CONCURRENCY, CLAIM_STALE_AFTER_SEC

logger = logging.getLogger(__name__)

PROCESS_NAME = SOURCE  # "portal_a" -- matches the control table's default


@dataclass
class BackfillControl:
    process_name: str
    desired_state: str
    last_run_status: Optional[str]
    last_error: Optional[str]
    cursor_updated_at: Optional[str]
    cursor_id: Optional[str]
    last_batch_number: int
    batch_size: int
    concurrency: int
    worker_instance_id: Optional[str]
    worker_heartbeat_at: Optional[str]
    session_started_at: Optional[str]

    @property
    def should_run(self) -> bool:
        return self.desired_state == "running"


def _row_to_control(row: dict[str, Any]) -> BackfillControl:
    return BackfillControl(
        process_name=row["process_name"],
        desired_state=row["desired_state"],
        last_run_status=row.get("last_run_status"),
        last_error=row.get("last_error"),
        cursor_updated_at=row.get("cursor_updated_at"),
        cursor_id=row.get("cursor_id"),
        last_batch_number=row.get("last_batch_number") or 0,
        batch_size=row.get("batch_size") or DEFAULT_BATCH_SIZE,
        concurrency=row.get("concurrency") or DEFAULT_CONCURRENCY,
        worker_instance_id=row.get("worker_instance_id"),
        worker_heartbeat_at=row.get("worker_heartbeat_at"),
        session_started_at=row.get("session_started_at"),
    )


async def ensure_control_row(client: httpx.AsyncClient) -> None:
    """Idempotent seed -- safe to call on every boot. Real production usage
    should also seed this row once via migration_002_backfill_control.sql;
    this is just a defensive fallback so the app doesn't 500 on a fresh
    environment where that migration step was somehow skipped."""
    r = await client.post(
        f"{SUPABASE_REST}/master_candidates_backfill_control?on_conflict=process_name",
        headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates"},
        json={"process_name": PROCESS_NAME, "desired_state": "stopped",
              "batch_size": DEFAULT_BATCH_SIZE, "concurrency": DEFAULT_CONCURRENCY},
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def read_control(client: httpx.AsyncClient) -> Optional[BackfillControl]:
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates_backfill_control",
        params={"process_name": f"eq.{PROCESS_NAME}", "select": "*"},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    rows = r.json()
    return _row_to_control(rows[0]) if rows else None


async def _patch_control(client: httpx.AsyncClient, fields: dict[str, Any]) -> None:
    r = await client.patch(
        f"{SUPABASE_REST}/master_candidates_backfill_control",
        params={"process_name": f"eq.{PROCESS_NAME}"},
        headers=SB_HEADERS, json=fields, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def mark_running(client: httpx.AsyncClient, *, batch_size: Optional[int] = None,
                        concurrency: Optional[int] = None, fresh_session: bool = False) -> None:
    fields: dict[str, Any] = {"desired_state": "running", "last_error": None}
    if batch_size is not None:
        fields["batch_size"] = batch_size
    if concurrency is not None:
        fields["concurrency"] = concurrency
    if fresh_session:
        fields["session_started_at"] = datetime.now(timezone.utc).isoformat()
    await _patch_control(client, fields)


async def mark_paused(client: httpx.AsyncClient, note: Optional[str] = None) -> None:
    fields: dict[str, Any] = {"desired_state": "paused"}
    if note:
        fields["last_error"] = note[:2000]
    await _patch_control(client, fields)


async def mark_stopped(client: httpx.AsyncClient) -> None:
    await _patch_control(client, {"desired_state": "stopped", "last_run_status": "stopped_by_user"})


async def mark_completed(client: httpx.AsyncClient) -> None:
    await _patch_control(client, {"desired_state": "stopped", "last_run_status": "completed", "last_error": None})


async def mark_failed(client: httpx.AsyncClient, error: str) -> None:
    await _patch_control(client, {"desired_state": "stopped", "last_run_status": "failed",
                                   "last_error": error[:2000]})


async def record_transient_error(client: httpx.AsyncClient, error: str) -> None:
    """Records a note WITHOUT changing desired_state -- transient errors
    (Supabase unreachable, a flaky HTTP call) must never halt an otherwise-
    running job. See worker.py's run_backfill_loop()."""
    await _patch_control(client, {"last_error": f"transient: {error[:1900]}"})


async def advance_cursor(client: httpx.AsyncClient, cursor_updated_at: Optional[str],
                          cursor_id: Optional[str], batch_number: Optional[int] = None) -> None:
    fields: dict[str, Any] = {"cursor_updated_at": cursor_updated_at, "cursor_id": cursor_id}
    if batch_number is not None:
        fields["last_batch_number"] = batch_number
    await _patch_control(client, fields)


async def heartbeat(client: httpx.AsyncClient, instance_id: str) -> None:
    await _patch_control(client, {
        "worker_instance_id": instance_id,
        "worker_heartbeat_at": datetime.now(timezone.utc).isoformat(),
    })


def _parse_iso(v: Optional[str]) -> Optional[datetime]:
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


async def try_claim(client: httpx.AsyncClient, instance_id: str) -> bool:
    """Read-then-compare-and-swap claim, mirroring backfill/match.py's
    optimistic-concurrency merge pattern (_merge_into_existing) rather than
    an inequality comparison embedded inside a PostgREST `or=(...)` filter
    string.

    ROOT CAUSE this replaces: the previous implementation issued a single
    conditional PATCH with
      process_name=eq.<x>&desired_state=eq.running&or=(worker_heartbeat_at.is.null,worker_heartbeat_at.lt.<cutoff>)
    and treated an empty `Prefer: return=representation` response as
    "someone else's heartbeat is still fresh". In production this returned
    an empty response on every single attempt, for minutes at a stretch,
    even against a heartbeat that was unambiguously more than
    CLAIM_STALE_AFTER_SEC old by wall-clock comparison against the value
    /status was independently reporting moments earlier -- i.e. the
    inequality-in-a-compound-OR-filter was not matching rows it should
    have matched, and there was no way to observe *why* from outside
    Postgres/PostgREST, since a 0-row match and an RLS-filtered match are
    both indistinguishable 200-with-empty-body responses. Rather than
    keep guessing at the exact byte-level cause of that specific filter
    combination, this version removes the entire class of risk: read the
    row explicitly, decide staleness with a plain Python datetime
    comparison (fully logged, fully unit-testable), and only ever send
    PostgREST an exact-value filter (`eq.<value>` or `is.null`) for the
    actual claim PATCH -- the same kind of filter every other write in
    this codebase already uses successfully (e.g. desired_state=eq.running
    itself works fine everywhere else it's used).

    Still race-safe: two instances can both pass the staleness check in
    Python from a similarly-stale read, but the final PATCH is a
    compare-and-swap keyed on the EXACT worker_heartbeat_at value each one
    read -- only the first PATCH to land actually changes that column
    (bumping it to a fresh timestamp), so the second one's `eq.<stale
    value>` filter no longer matches and it correctly gets an empty
    response, i.e. still loses the race deterministically."""
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates_backfill_control",
        params={"process_name": f"eq.{PROCESS_NAME}", "select": "desired_state,worker_heartbeat_at"},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        logger.warning(f"[mc-backfill] try_claim: no control row found for process_name={PROCESS_NAME}")
        return False
    row = rows[0]

    if row.get("desired_state") != "running":
        logger.info(f"[mc-backfill] try_claim: desired_state={row.get('desired_state')!r}, not 'running' -- not claiming")
        return False

    current_heartbeat_raw = row.get("worker_heartbeat_at")
    current_heartbeat = _parse_iso(current_heartbeat_raw)
    now = datetime.now(timezone.utc)
    is_eligible = current_heartbeat is None or (now - current_heartbeat) > timedelta(seconds=CLAIM_STALE_AFTER_SEC)
    logger.info(
        f"[mc-backfill] try_claim: desired_state=running, "
        f"current_heartbeat={current_heartbeat_raw!r}, age_sec="
        f"{(now - current_heartbeat).total_seconds() if current_heartbeat else 'null'}, "
        f"eligible={is_eligible}"
    )
    if not is_eligible:
        return False

    cas_filter = "is.null" if current_heartbeat_raw is None else f"eq.{current_heartbeat_raw}"
    patch_r = await client.patch(
        f"{SUPABASE_REST}/master_candidates_backfill_control",
        params={
            "process_name": f"eq.{PROCESS_NAME}",
            "desired_state": "eq.running",
            "worker_heartbeat_at": cas_filter,
        },
        headers={**SB_HEADERS, "Prefer": "return=representation"},
        json={"worker_instance_id": instance_id, "worker_heartbeat_at": now.isoformat()},
        timeout=HTTP_TIMEOUT_SUPABASE)
    patch_r.raise_for_status()
    claimed = bool(patch_r.json())
    logger.info(f"[mc-backfill] try_claim: compare-and-swap PATCH claimed={claimed}")
    return claimed


# ─────────────────────────────────────────────────────────────────────────────
# Per-batch history (master_candidates_backfill_progress) -- unchanged table
# ─────────────────────────────────────────────────────────────────────────────
async def start_batch_progress_row(client: httpx.AsyncClient, batch_number: int) -> str:
    r = await client.post(
        f"{SUPABASE_REST}/master_candidates_backfill_progress",
        headers={**SB_HEADERS, "Prefer": "return=representation"},
        json={"batch_number": batch_number, "source": SOURCE, "status": "running"},
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()[0]["id"]


async def finish_batch_progress_row(
    client: httpx.AsyncClient, progress_id: str, *,
    rows_attempted: int, rows_inserted: int, rows_merged: int,
    errors_count: int, last_source_row_id: Optional[str], status: str = "completed",
) -> None:
    r = await client.patch(
        f"{SUPABASE_REST}/master_candidates_backfill_progress",
        params={"id": f"eq.{progress_id}"},
        headers=SB_HEADERS,
        json={
            "rows_attempted": rows_attempted, "rows_inserted": rows_inserted,
            "rows_merged": rows_merged, "rows_skipped": 0, "errors_count": errors_count,
            "last_source_row_id": last_source_row_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
        },
        timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def get_recent_batches(client: httpx.AsyncClient, limit: int) -> list[dict]:
    # Confirmed live schema: started_at is NOT NULL (defaults to now()),
    # completed_at is nullable (NULL while a batch is still running) --
    # order by started_at so an in-flight batch is never misplaced.
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates_backfill_progress",
        params={"source": f"eq.{SOURCE}", "order": "started_at.desc", "limit": str(limit)},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()


# ─────────────────────────────────────────────────────────────────────────────
# Errors (master_candidates_backfill_errors) -- unchanged table
# ─────────────────────────────────────────────────────────────────────────────
async def write_error_rows(client: httpx.AsyncClient, batch_id: Optional[str], errors: list[dict]) -> None:
    """errors: list of {"source_row_id": str, "error": str}. error_context
    is always None here, matching the original Edge Function, which never
    populated that column either."""
    rows = [{
        "batch_id": batch_id, "source": SOURCE,
        "source_row_id": str(e["source_row_id"]),
        "error_message": str(e.get("error") or "unknown_error")[:4000],
        "error_context": None,
    } for e in errors if e.get("source_row_id")]
    if not rows:
        return
    r = await client.post(
        f"{SUPABASE_REST}/master_candidates_backfill_errors",
        headers=SB_HEADERS, json=rows, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()


async def get_recent_errors(client: httpx.AsyncClient, limit: int) -> list[dict]:
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates_backfill_errors",
        params={"source": f"eq.{SOURCE}", "order": "occurred_at.desc", "limit": str(limit)},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    return r.json()


async def get_unretried_error_source_ids(client: httpx.AsyncClient, limit: int) -> list[str]:
    """Distinct source_row_ids from recent errors that don't yet have a
    candidate_source_links row -- the worklist for POST /retry-errors.
    Over-fetches from the errors table since some candidates will already
    be linked by the time this runs (fixed by a later normal pass)."""
    r = await client.get(
        f"{SUPABASE_REST}/master_candidates_backfill_errors",
        params={"source": f"eq.{SOURCE}", "order": "occurred_at.desc",
                "select": "source_row_id", "limit": str(min(limit * 3, 3000))},
        headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
    r.raise_for_status()
    candidate_ids = list(dict.fromkeys(row["source_row_id"] for row in r.json()))
    if not candidate_ids:
        return []

    linked: set[str] = set()
    chunk_size = 200
    for i in range(0, len(candidate_ids), chunk_size):
        chunk = candidate_ids[i:i + chunk_size]
        r2 = await client.get(
            f"{SUPABASE_REST}/candidate_source_links",
            params={"source": f"eq.{SOURCE}", "source_row_id": f"in.({','.join(chunk)})",
                    "select": "source_row_id"},
            headers=SB_HEADERS, timeout=HTTP_TIMEOUT_SUPABASE)
        r2.raise_for_status()
        linked.update(row["source_row_id"] for row in r2.json())

    return [i for i in candidate_ids if i not in linked][:limit]
