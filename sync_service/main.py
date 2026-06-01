"""
xrilic-search-service / sync_service / main.py

Responsibilities:
  1. POST /webhook/talent-pool  — Supabase DB webhook on hr_talent_pool INSERT/UPDATE
  2. POST /reindex              — Trigger full re-index (admin, protected)
  3. GET  /health               — Liveness probe
  4. Background task: polls Supabase every 60s for changed records

Flow:
  Supabase hr_talent_pool → this service → Typesense collection "candidates"
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

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sync_service")

# ── Config from env ───────────────────────────────────────────────────────────
TYPESENSE_HOST     = os.environ["TYPESENSE_HOST"]       # e.g. typesense (container name)
TYPESENSE_PORT     = int(os.environ.get("TYPESENSE_PORT", "8108"))
TYPESENSE_API_KEY  = os.environ["TYPESENSE_API_KEY"]
SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
WEBHOOK_SECRET     = os.environ["WEBHOOK_SECRET"]       # shared secret for webhook auth
ADMIN_SECRET       = os.environ["ADMIN_SECRET"]         # for /reindex endpoint
POLL_INTERVAL_SEC  = int(os.environ.get("POLL_INTERVAL_SEC", "60"))

# ── Global instances ──────────────────────────────────────────────────────────
indexer: CandidateIndexer = None
poller: SyncPoller = None


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

    # Ensure Typesense collection exists (idempotent)
    await indexer.ensure_collection()
    logger.info("Typesense collection ready.")

    # Start background poller
    poller = SyncPoller(
        indexer=indexer,
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_SERVICE_KEY,
        interval_sec=POLL_INTERVAL_SEC,
    )
    asyncio.create_task(poller.run())
    logger.info(f"Poller started (interval={POLL_INTERVAL_SEC}s).")

    yield

    # Shutdown
    if poller:
        poller.stop()
    logger.info("xrilic-search-service stopped.")


app = FastAPI(title="Xrilic Search Sync Service", lifespan=lifespan)


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    ts_ok = await indexer.ping() if indexer else False
    return {"status": "ok", "typesense": ts_ok}


# ─────────────────────────────────────────────────────────────────────────────
# Webhook — Supabase fires this on INSERT / UPDATE to hr_talent_pool
# ─────────────────────────────────────────────────────────────────────────────

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

    payload = await request.json()
    event_type = payload.get("type", "").upper()
    record = payload.get("record")

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


# ─────────────────────────────────────────────────────────────────────────────
# Full Re-index (admin only)
# ─────────────────────────────────────────────────────────────────────────────

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
    poller_stats = poller.get_stats() if poller else {}
    return {"collection": collection_stats, "poller": poller_stats}


# ─────────────────────────────────────────────────────────────────────────────
# Full re-index helper
# ─────────────────────────────────────────────────────────────────────────────

async def run_full_reindex(supabase_url: str, supabase_key: str, indexer: CandidateIndexer):
    """
    Reads ALL records from hr_talent_pool in batches of 500
    and upserts them into Typesense.
    """
    logger.info("Starting full re-index...")
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    total = 0
    offset = 0
    batch_size = 500

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            url = (
                f"{supabase_url}/rest/v1/hr_talent_pool"
                f"?select=id,candidate_name,email,phone,suggested_title,"
                f"current_designation,current_company,current_location,"
                f"notice_period,top_skills,parsed_experience_years,"
                f"parsed_current_ctc,parsed_expected_ctc,organization_id,"
                f"created_at,work_experience,education"
                f"&order=created_at.asc"
                f"&offset={offset}&limit={batch_size}"
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

            total += len(docs)
            offset += batch_size
            logger.info(f"Re-index progress: {total} records indexed...")

            if len(records) < batch_size:
                break

            # Avoid hammering Supabase
            await asyncio.sleep(0.2)

    logger.info(f"Full re-index complete: {total} total records.")
    # 