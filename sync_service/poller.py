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
        Fetch all hr_talent_pool rows updated since last_synced_at.
        Upsert them into Typesense in batches of 500.
        """
        since = self.last_synced_at.isoformat()
        poll_start = datetime.now(timezone.utc)

        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
        }

        total_this_poll = 0
        offset = 0
        batch_size = 500

        async with httpx.AsyncClient(timeout=60) as client:
            while True:
                url = (
                    f"{self.supabase_url}/rest/v1/hr_talent_pool"
                    f"?select=id,candidate_name,email,phone,suggested_title,"
                    f"current_designation,current_company,current_location,"
                    f"notice_period,top_skills,parsed_experience_years,"
                    f"parsed_current_ctc,parsed_expected_ctc,organization_id,"
                    f"created_at,updated_at,work_experience,education"
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
                    await self.indexer.bulk_upsert(docs)

                total_this_poll += len(docs)
                offset += batch_size

                if len(records) < batch_size:
                    break

                await asyncio.sleep(0.1)

        if total_this_poll > 0:
            logger.info(f"Poller: synced {total_this_poll} updated records.")

        self._total_synced     += total_this_poll
        self._last_poll_count   = total_this_poll
        self._last_poll_time    = poll_start
        # Advance cursor only after successful poll
        self.last_synced_at     = poll_start