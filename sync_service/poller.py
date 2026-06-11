"""
poller.py

Polls Supabase every N seconds for records updated since last_synced_at.
Handles the case where webhooks might miss events (network issues, downtime).

State: last_synced_at is stored in memory (resets to 5 minutes ago on restart
to catch any missed events during downtime).
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger("poller")


class SyncPoller:
    def __init__(
        self,
        indexer,
        supabase_url: str,
        supabase_key: str,
        interval_sec: int = 60,
    ):
        self.indexer       = indexer
        self.supabase_url  = supabase_url
        self.supabase_key  = supabase_key
        self.interval_sec  = interval_sec
        self._running      = True

        # On startup, go back 5 min to catch anything missed while offline
        self.last_synced_at: datetime = datetime.now(timezone.utc) - timedelta(minutes=5)

        self._total_synced   = 0
        self._last_poll_time: Optional[datetime] = None
        self._last_poll_count = 0
        self._errors          = 0

    def stop(self):
        self._running = False

    def get_stats(self) -> dict:
        return {
            "last_synced_at":   self.last_synced_at.isoformat(),
            "last_poll_time":   self._last_poll_time.isoformat() if self._last_poll_time else None,
            "last_poll_count":  self._last_poll_count,
            "total_synced":     self._total_synced,
            "errors":           self._errors,
        }

    async def run(self):
        logger.info("Poller running...")
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                self._errors += 1
                logger.error(f"Poller error: {e}", exc_info=True)
            await asyncio.sleep(self.interval_sec)

async def _poll_once(self):
    """
    v2.3 CHANGES:
      - Health check BEFORE polling — skip cycle if Typesense is not ready.
        Prevents the poller from crashing Typesense during search load.
      - Cursor not advanced on upsert failure — retried next cycle.
    """
    # ── Guard: skip entire cycle if Typesense is unhealthy ─────────────
    if not await self.indexer.ping():
        logger.warning("Typesense not healthy — skipping poll cycle (will retry next interval)")
        return

    since      = self.last_synced_at.isoformat().replace("+", "%2B")
    poll_start = datetime.now(timezone.utc)

    headers = {
        "apikey": self.supabase_key,
        "Authorization": f"Bearer {self.supabase_key}",
    }

    total_this_poll = 0
    offset          = 0
    batch_size      = 500

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            url = (
                f"{self.supabase_url}/rest/v1/hr_talent_pool"
                f"?select=id,candidate_name,email,phone,suggested_title,"
                f"current_designation,current_company,current_location,"
                f"notice_period,top_skills,parsed_experience_years,"
                f"parsed_current_ctc,parsed_expected_ctc,organization_id,"
                f"created_at,updated_at,work_experience,education,"
                f"resume_text"
                f"&updated_at=gt.{since}"
                f"&order=updated_at.asc"
                f"&offset={offset}&limit={batch_size}"
            )

            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            records = resp.json()

            if not records:
                break

            docs = [
                self.indexer.transform_record(r)
                for r in records
                if r.get("id")
            ]
            docs = [d for d in docs if d is not None]

            if docs:
                success = await self.indexer.bulk_upsert(docs)
                if not success:
                    # Typesense became unhealthy mid-cycle — stop now.
                    # Cursor is NOT advanced, so these records will be
                    # retried on the next healthy poll cycle.
                    logger.warning(
                        "Bulk upsert failed — stopping poll cycle early. "
                        "Cursor not advanced; records will be retried next interval."
                    )
                    return

            total_this_poll += len(docs)
            offset += batch_size

            if len(records) < batch_size:
                break

            await asyncio.sleep(0.1)

    if total_this_poll > 0:
        logger.info(f"Poller: synced {total_this_poll} updated records.")

    self._total_synced    += total_this_poll
    self._last_poll_count  = total_this_poll
    self._last_poll_time   = poll_start
    # Only advance cursor after a fully successful cycle
    self.last_synced_at    = poll_start