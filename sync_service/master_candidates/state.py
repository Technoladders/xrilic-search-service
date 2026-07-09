"""
sync_service/master_candidates/state.py

Shared helpers for reading control state, writing heartbeats, and logging
per-cycle run rows. Every background worker uses these.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

import httpx

from .config import SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE

logger = logging.getLogger(__name__)


class ProcessControl:
    """Snapshot of one row from mc_process_control at a point in time."""
    def __init__(self, row: dict[str, Any]):
        self.process_name        = row["process_name"]
        self.desired_state       = row["desired_state"]
        self.cursor_updated_at   = row.get("cursor_updated_at")
        self.cursor_naukri_id    = row.get("cursor_naukri_id")
        self.poll_interval_sec   = row["poll_interval_sec"]
        self.batch_size          = row["batch_size"]
        self.concurrency         = row["concurrency"]

    @property
    def should_run(self) -> bool:
        return self.desired_state == "running"


async def read_control(client: httpx.AsyncClient, process: str) -> Optional[ProcessControl]:
    r = await client.get(
        f"{SUPABASE_REST}/mc_process_control",
        params={"process_name": f"eq.{process}", "select": "*"},
        headers=SB_HEADERS,
        timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()
    rows = r.json()
    return ProcessControl(rows[0]) if rows else None


async def heartbeat(client: httpx.AsyncClient, process: str, note: str | None = None) -> None:
    payload = {"worker_heartbeat_at": datetime.now(timezone.utc).isoformat()}
    if note is not None:
        payload["last_run_note"] = note
    try:
        r = await client.patch(
            f"{SUPABASE_REST}/mc_process_control",
            params={"process_name": f"eq.{process}"},
            headers=SB_HEADERS,
            json=payload,
            timeout=HTTP_TIMEOUT_SUPABASE,
        )
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"[{process}] heartbeat failed: {e}")


async def advance_cursor(
    client: httpx.AsyncClient,
    process: str,
    *,
    cursor_updated_at: str | None = None,
    cursor_naukri_id: str | None = None,
) -> None:
    payload: dict[str, Any] = {}
    if cursor_updated_at is not None:
        payload["cursor_updated_at"] = cursor_updated_at
    if cursor_naukri_id is not None:
        payload["cursor_naukri_id"] = cursor_naukri_id
    if not payload:
        return
    r = await client.patch(
        f"{SUPABASE_REST}/mc_process_control",
        params={"process_name": f"eq.{process}"},
        headers=SB_HEADERS,
        json=payload,
        timeout=HTTP_TIMEOUT_SUPABASE,
    )
    r.raise_for_status()


class RunLog:
    """Per-cycle append log — one row inserted at cycle start, updated at end."""
    def __init__(self, client: httpx.AsyncClient, process: str):
        self.client   = client
        self.process  = process
        self.id       = str(uuid4())
        self.batches         = 0
        self.rows_processed  = 0
        self.rows_ingested   = 0
        self.rows_indexed    = 0
        self.errors_count    = 0
        self.first_error: str | None = None
        self.cursor_before: str | None = None
        self.cursor_after:  str | None = None

    async def start(self, cursor_before: str | None = None) -> None:
        self.cursor_before = cursor_before
        payload = {
            "id": self.id,
            "process_name": self.process,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "cursor_before": cursor_before,
        }
        try:
            r = await self.client.post(
                f"{SUPABASE_REST}/mc_process_runs",
                headers=SB_HEADERS,
                json=payload,
                timeout=HTTP_TIMEOUT_SUPABASE,
            )
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"[{self.process}] run-log start failed: {e}")

    def bump(
        self, *,
        batches: int = 0,
        processed: int = 0,
        ingested: int = 0,
        indexed: int = 0,
        errors: int = 0,
        first_error: str | None = None,
    ) -> None:
        self.batches        += batches
        self.rows_processed += processed
        self.rows_ingested  += ingested
        self.rows_indexed   += indexed
        self.errors_count   += errors
        if first_error and not self.first_error:
            self.first_error = first_error[:500]

    async def finish(self, status: str, note: str | None = None, cursor_after: str | None = None) -> None:
        self.cursor_after = cursor_after
        payload = {
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "batches": self.batches,
            "rows_processed": self.rows_processed,
            "rows_ingested": self.rows_ingested,
            "rows_indexed": self.rows_indexed,
            "errors_count": self.errors_count,
            "first_error": self.first_error,
            "cursor_after": cursor_after,
            "note": note,
        }
        try:
            r = await self.client.patch(
                f"{SUPABASE_REST}/mc_process_runs",
                params={"id": f"eq.{self.id}"},
                headers=SB_HEADERS,
                json=payload,
                timeout=HTTP_TIMEOUT_SUPABASE,
            )
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"[{self.process}] run-log finish failed: {e}")