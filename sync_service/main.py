"""
main.py — xrilic-search-service
Flask app: webhook + full reindex + health + stats

COMPOSITE CURSOR FIX (Phase 2):
  Naukri bulk imports create groups of 50 rows with identical created_at.
  A single gt.{timestamp} cursor skips all rows after the first batch in each
  group. Fix: composite cursor (created_at, id) using PostgREST OR filter:
    or=(created_at.gt.{dt},and(created_at.eq.{dt},id.gt.{id}))
    order=created_at.asc,id.asc
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from indexer import CandidateIndexer
from poller import SyncPoller

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sync_service")

# ── Config from env ───────────────────────────────────────────────────────────
TYPESENSE_HOST      = os.environ["TYPESENSE_HOST"]
TYPESENSE_PORT      = int(os.environ.get("TYPESENSE_PORT", "8108"))
TYPESENSE_API_KEY   = os.environ["TYPESENSE_API_KEY"]
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
WEBHOOK_SECRET      = os.environ["WEBHOOK_SECRET"]
ADMIN_SECRET        = os.environ["ADMIN_SECRET"]
POLL_INTERVAL_SEC   = int(os.environ.get("POLL_INTERVAL_SEC", "60"))

# ── Global instances ──────────────────────────────────────────────────────────
indexer: CandidateIndexer = None
poller:  SyncPoller       = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init indexer, ensure collection exists, start poller."""
    global indexer, poller

    logger.info("Starting xrilic-search-service...")

    indexer = CandidateIndexer(
        host=TYPESENSE_HOST,
        port=TYPESENSE_PORT,
        api_key=TYPESENSE_API_KEY,
    )

    await indexer.ensure_collection()
    logger.info("Typesense collection ready.")

    poller = SyncPoller(
        indexer=indexer,
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_SERVICE_KEY,
        interval_sec=POLL_INTERVAL_SEC,
    )
    asyncio.create_task(poller.run())
    logger.info(f"Poller started (interval={POLL_INTERVAL_SEC}s).")

    yield

    if poller:
        poller.stop()
    logger.info("xrilic-search-service stopped.")


app = FastAPI(title="Xrilic Search Sync Service", lifespan=lifespan)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    ts_ok = await indexer.ping() if indexer else False
    return {"status": "ok", "typesense": ts_ok}


# ── Webhook — Supabase fires this on INSERT / UPDATE to hr_talent_pool ────────

@app.post("/webhook/talent-pool")
async def webhook_talent_pool(
    request: Request,
    background_tasks: BackgroundTasks,
    x_webhook_secret: str = Header(None),
):
    """
    Supabase sends:
    {
      "type": "INSERT" | "UPDATE" | "DELETE",
      "table": "hr_talent_pool",
      "record": { ...full row... },
      "old_record": { ... }   (UPDATE/DELETE only)
    }
    """
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload    = await request.json()
    event_type = payload.get("type", "").upper()
    record     = payload.get("record")

    if not record:
        return {"ok": True, "skipped": "no record"}

    if event_type in ("INSERT", "UPDATE"):
        background_tasks.add_task(indexer.upsert_document, record)
        logger.info(f"Webhook {event_type} → queued upsert for {record.get('id')}")
    elif event_type == "DELETE":
        old = payload.get("old_record", {})
        if old.get("id"):
            background_tasks.add_task(indexer.delete_document, old["id"])
            logger.info(f"Webhook DELETE → queued delete for {old.get('id')}")

    return {"ok": True}


# ── Full Re-index (admin only) ─────────────────────────────────────────────────

@app.post("/reindex")
async def trigger_reindex(
    background_tasks: BackgroundTasks,
    x_admin_secret: str = Header(None),
):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(
        run_full_reindex,
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_SERVICE_KEY,
        indexer=indexer,
    )
    return {"ok": True, "message": "Full re-index started in background"}


@app.get("/stats")
async def stats(x_admin_secret: str = Header(None)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    collection_stats = await indexer.get_stats()
    poller_stats     = poller.get_stats() if poller else {}
    return {"collection": collection_stats, "poller": poller_stats}


# ── Full re-index helper ───────────────────────────────────────────────────────

async def run_full_reindex(supabase_url: str, supabase_key: str, indexer: CandidateIndexer):
    """
    Reads ALL records from hr_talent_pool in batches of 500.
    Uses COMPOSITE CURSOR (created_at, id) to handle groups of rows
    sharing the same created_at (Naukri bulk imports).

    PostgREST OR filter pattern:
      &or=(created_at.gt.{dt},and(created_at.eq.{dt},id.gt.{id}))
      &order=created_at.asc,id.asc

    This correctly pages past all duplicate-timestamp groups.
    """
    logger.info("Starting full re-index (composite cursor)...")
    headers = {
        "apikey":        supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    # Epoch start — will be replaced after first batch
    last_dt = "1970-01-01T00:00:00+00:00"
    last_id = "00000000-0000-0000-0000-000000000000"

    total      = 0
    batch_size = 500

    # Fields needed by transform_record — resume_text omitted to avoid 50MB+ batches
    SELECT_FIELDS = (
        "id,candidate_name,email,phone,suggested_title,"
        "current_designation,current_company,current_location,"
        "notice_period,top_skills,parsed_experience_years,"
        "parsed_current_ctc,parsed_expected_ctc,organization_id,"
        "created_at,work_experience,education"
    )

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            # Encode + in ISO timestamp for URL safety
            safe_dt = last_dt.replace("+", "%2B")

            # Composite cursor OR filter — handles duplicate created_at groups
            or_filter = (
                f"or=(created_at.gt.{safe_dt},"
                f"and(created_at.eq.{safe_dt},id.gt.{last_id}))"
            )

            url = (
                f"{supabase_url}/rest/v1/hr_talent_pool"
                f"?select={SELECT_FIELDS}"
                f"&{or_filter}"
                f"&order=created_at.asc,id.asc"
                f"&limit={batch_size}"
            )

            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            records = resp.json()

            if not records:
                break

            docs = [indexer.transform_record(r) for r in records if r.get("id")]
            docs = [d for d in docs if d is not None]

            if docs:
                await indexer.bulk_upsert(docs)

            total    += len(docs)
            last_dt   = records[-1]["created_at"]
            last_id   = records[-1]["id"]

            logger.info(
                f"Re-index progress: {total} records indexed "
                f"(cursor: {last_dt[:19]}, id: {last_id[:8]}...)"
            )

            if len(records) < batch_size:
                break

            # Avoid hammering Supabase
            await asyncio.sleep(0.2)

    logger.info(f"Full re-index complete: {total} total records.")