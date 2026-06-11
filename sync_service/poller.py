"""
poller.py

Polls Supabase every N seconds for records updated since last_synced_at.
Handles missed webhook events and safely syncs large datasets.

Uses composite cursor pagination:
    (updated_at, id)

This prevents skipped records when many rows share the same timestamp.
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
        self.indexer = indexer
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.interval_sec = interval_sec
        self._running = True

        # Catch anything missed during downtime
        self.last_synced_at = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        )

        self._total_synced = 0
        self._last_poll_time: Optional[datetime] = None
        self._last_poll_count = 0
        self._errors = 0

    def stop(self):
        self._running = False

    def get_stats(self) -> dict:
        return {
            "last_synced_at": self.last_synced_at.isoformat(),
            "last_poll_time": (
                self._last_poll_time.isoformat()
                if self._last_poll_time
                else None
            ),
            "last_poll_count": self._last_poll_count,
            "total_synced": self._total_synced,
            "errors": self._errors,
        }

    async def run(self):
        logger.info("Poller running...")

        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                self._errors += 1
                logger.error(
                    f"Poller error: {e}",
                    exc_info=True,
                )

            await asyncio.sleep(self.interval_sec)

    async def _poll_once(self):
        # Skip cycle if Typesense unhealthy
        if not await self.indexer.ping():
            logger.warning(
                "Typesense not healthy — skipping poll cycle"
            )
            return

        poll_start = datetime.now(timezone.utc)

        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
        }

        total_this_poll = 0
        batch_size = 200

        last_dt = self.last_synced_at.isoformat()
        last_id = "00000000-0000-0000-0000-000000000000"

        async with httpx.AsyncClient(timeout=60) as client:
            while True:
                safe_dt = last_dt.replace("+", "%2B")

                or_filter = (
                    f"or=("
                    f"updated_at.gt.{safe_dt},"
                    f"and(updated_at.eq.{safe_dt},id.gt.{last_id})"
                    f")"
                )

                url = (
                    f"{self.supabase_url}/rest/v1/hr_talent_pool"
                    f"?select="
                    f"id,"
                    f"candidate_name,"
                    f"email,"
                    f"phone,"
                    f"suggested_title,"
                    f"current_designation,"
                    f"current_company,"
                    f"current_location,"
                    f"notice_period,"
                    f"top_skills,"
                    f"parsed_experience_years,"
                    f"parsed_current_ctc,"
                    f"parsed_expected_ctc,"
                    f"organization_id,"
                    f"created_at,"
                    f"updated_at,"
                    f"work_experience,"
                    f"education,"
                    f"resume_text"
                    f"&{or_filter}"
                    f"&order=updated_at.asc,id.asc"
                    f"&limit={batch_size}"
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
                        logger.warning(
                            "Bulk upsert failed — "
                            "stopping cycle early, "
                            "cursor not advanced"
                        )
                        return

                total_this_poll += len(docs)

                last_dt = records[-1]["updated_at"]
                last_id = records[-1]["id"]

                if len(records) < batch_size:
                    break

                await asyncio.sleep(0.1)

        if total_this_poll > 0:
            logger.info(
                f"Poller: synced {total_this_poll} updated records."
            )

        self._total_synced += total_this_poll
        self._last_poll_count = total_this_poll
        self._last_poll_time = poll_start

        # Advance cursor only after successful cycle
        self.last_synced_at = poll_start