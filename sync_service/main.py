"""
main.py — xrilic-search-service
Flask app: webhook + full reindex + health + stats

v2.2 CHANGES:
  - SELECT_FIELDS in run_full_reindex now includes resume_text
  - batch_size reduced from 500 to 200 to keep HTTP responses manageable
    (200 rows × avg 10KB resume = ~2MB per batch, safe for Supabase)
  - The indexer will now populate resume_full_text (up to 100KB per doc)
    instead of the old resume_snippet (2000 chars)

COMPOSITE CURSOR FIX (Phase 2):
  Naukri bulk imports create groups of 50 rows with identical created_at.
  A single gt.{timestamp} cursor skips all rows after the first batch.
  Fix: composite cursor (created_at, id) using PostgREST OR filter.
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
from fastapi.middleware.cors import CORSMiddleware

from indexer import CandidateIndexer
from poller import SyncPoller

from master_candidates import (
    config as mc_config,
    typesense_client as mc_ts,
    indexer as mc_indexer,
    search_api as mc_search,
    admin_api as mc_admin,
    ingest as mc_ingest,
)
from master_candidates.backfill import api as mc_backfill_api
from master_candidates.backfill import worker as mc_backfill_worker

logger = logging.getLogger("sync_service")

TYPESENSE_HOST      = os.environ["TYPESENSE_HOST"]
TYPESENSE_PORT      = int(os.environ.get("TYPESENSE_PORT", "8108"))
TYPESENSE_API_KEY   = os.environ["TYPESENSE_API_KEY"]
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
WEBHOOK_SECRET      = os.environ["WEBHOOK_SECRET"]
ADMIN_SECRET        = os.environ["ADMIN_SECRET"]
POLL_INTERVAL_SEC   = int(os.environ.get("POLL_INTERVAL_SEC", "60"))

indexer: CandidateIndexer = None
poller:  SyncPoller       = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global indexer, poller

    # Re-applied here (not at module import time) with force=True: uvicorn
    # configures its own logging (defaulting to disable_existing_loggers)
    # either before or interleaved with importing this module, which was
    # silently disabling every application logger created via basicConfig()/
    # getLogger() at import time (sync_service, master_candidates.*, incl.
    # master_candidates.backfill.worker) -- httpx's own logger kept working
    # only because it's created lazily, on first request, well after that
    # point. lifespan() is guaranteed to run after uvicorn's own logging
    # setup, so force=True here reliably wins regardless of import order.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )

    logger.info("Starting xrilic-search-service...")

    # ------------------------------------------------------------------
    # Existing Zive-X Search setup
    # ------------------------------------------------------------------

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

    zx_poller_task = asyncio.create_task(poller.run())

    logger.info(f"Poller started (interval={POLL_INTERVAL_SEC}s).")

    # ------------------------------------------------------------------
    # NEW: Master Candidates setup
    # ------------------------------------------------------------------

    async with httpx.AsyncClient() as client:
        await mc_ts.ensure_collection(client)
        synonym_count = await mc_ts.sync_synonyms(client)
        logger.info(f"[mc] booted, {synonym_count} synonyms loaded")
    logger.info("[mc] schema fields NOT auto-migrated on boot — "
                "trigger manually via POST /mc/admin/schema/ensure-fields")

    mc_index_task = None
    mc_ingest_task = None

    if mc_config.INDEX_ENABLED:
        mc_index_task = asyncio.create_task(
            mc_indexer.run_index_loop()
        )
        logger.info("[mc] index loop started")
    else:
        logger.info("[mc] index loop DISABLED via MC_INDEX_ENABLED=false")
        
    if mc_config.INGEST_ENABLED:
        mc_ingest_task = asyncio.create_task(
            mc_ingest.run_ingest_loop()
        )
        logger.info("[mc] ingest loop started")
    else:
        logger.info(
            "[mc] ingest loop DISABLED via MC_INGEST_ENABLED=false"
        )

    # ------------------------------------------------------------------
    # NEW: Master Candidates backfill (portal_a) -- separate, purpose-built
    # module; does not touch mc_ingest/mc_index/state.py's mc_process_*
    # tables at all. claim_watchdog() is a persistent loop (not a one-shot
    # boot check) so it keeps retrying the single-claimant claim rather
    # than giving up if another instance's heartbeat is still fresh.
    # ------------------------------------------------------------------
    mc_backfill_worker.start_watchdog()
    logger.info("[mc-backfill] claim watchdog started")

    # ------------------------------------------------------------------

    yield

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    if poller:
        poller.stop()

    zx_poller_task.cancel()

    if mc_index_task:
        mc_index_task.cancel()

    if mc_ingest_task:
        mc_ingest_task.cancel()

    mc_backfill_worker.cancel_all()

    logger.info("xrilic-search-service stopped.")


app = FastAPI(title="Xrilic Search Sync Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(mc_search.router)
app.include_router(mc_admin.router)
app.include_router(mc_backfill_api.router)


@app.get("/health")
async def health():
    ts_ok = await indexer.ping() if indexer else False
    return {"status": "ok", "typesense": ts_ok}


@app.post("/webhook/talent-pool")
async def webhook_talent_pool(
    request: Request,
    background_tasks: BackgroundTasks,
    x_webhook_secret: str = Header(None),
):
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


@app.post("/reindex")
async def trigger_reindex(
    background_tasks: BackgroundTasks,
    x_admin_secret: str = Header(None),
):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(
        run_full_reindex,
        supabase_url=SUPABASE_URL, supabase_key=SUPABASE_SERVICE_KEY, indexer=indexer,
    )
    return {"ok": True, "message": "Full re-index started in background"}


@app.get("/stats")
async def stats(x_admin_secret: str = Header(None)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    collection_stats = await indexer.get_stats()
    poller_stats     = poller.get_stats() if poller else {}
    return {"collection": collection_stats, "poller": poller_stats}


async def run_full_reindex(supabase_url: str, supabase_key: str, indexer: CandidateIndexer):
    """
    Reads ALL records from hr_talent_pool in batches using composite cursor.

    v2.2 CHANGE:
      SELECT_FIELDS now includes resume_text so that resume_full_text
      (up to 100KB) is indexed in Typesense.
      batch_size reduced from 500 → 200 to keep HTTP responses manageable
      when resume_text is included (200 × avg 10KB = ~2MB per batch).
    """
    logger.info("Starting full re-index v2.2 (with resume_text)...")
    headers = {
        "apikey":        supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    last_dt = "1970-01-01T00:00:00+00:00"
    last_id = "00000000-0000-0000-0000-000000000000"

    total      = 0
    batch_size = 200  # reduced from 500 — resume_text adds ~10KB per row

    # v2.2: resume_text included so indexer can build resume_full_text
    SELECT_FIELDS = (
        "id,candidate_name,email,phone,suggested_title,"
        "current_designation,current_company,current_location,"
        "notice_period,top_skills,parsed_experience_years,"
        "parsed_current_ctc,parsed_expected_ctc,organization_id,"
        "created_at,work_experience,education,"
        "resume_text,"  # ← v2.2 addition
        "highest_education"
    )

    async with httpx.AsyncClient(timeout=120) as client:
        while True:
            safe_dt    = last_dt.replace("+", "%2B")
            or_filter  = (
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

            total   += len(docs)
            last_dt  = records[-1]["created_at"]
            last_id  = records[-1]["id"]

            logger.info(
                f"Re-index progress: {total} records "
                f"(cursor: {last_dt[:19]}, id: {last_id[:8]}...)"
            )

            if len(records) < batch_size:
                break

            await asyncio.sleep(0.2)

    logger.info(f"Full re-index v2.2 complete: {total} total records.")