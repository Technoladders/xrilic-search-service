"""
sync_service/master_candidates/admin_api.py

FastAPI router mounted at /mc/admin.
POST /mc/admin/control              — set desired_state / config (superadmin JWT)
GET  /mc/admin/status               — dashboard payload (uses get_mc_process_status RPC)
POST /mc/admin/index/reindex-ids    — force-reindex specific master_candidates ids
POST /mc/admin/synonyms/reload      — reload synonyms from Supabase to Typesense
POST /mc/webhook/naukri-inserted    — called by naukri-save edge fn after RPC succeeds
POST /mc/webhook/master-changed     — optional realtime hook if you add a DB trigger later

Auth model matches your existing main.py:
  - /webhook/... require X-Webhook-Secret header
  - /admin/... require Authorization: Bearer <supabase JWT>, then RPC-side superadmin gate
"""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request

from .config import (
    SB_HEADERS, SUPABASE_REST, HTTP_TIMEOUT_SUPABASE, WEBHOOK_SECRET,
)
from .indexer import index_ids
from .typesense_client import sync_synonyms, ensure_fields, ensure_suggestions_collection
from .ingest import WEBHOOK_QUEUE
from .suggestions_aggregator import rebuild_suggestions

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mc", tags=["master_candidates_admin"])


# ── Auth deps ──────────────────────────────────────────────────────────────
def require_webhook(x_webhook_secret: str | None = Header(None)) -> None:
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid webhook secret")


def require_admin(authorization: str | None = Header(None)) -> str:
    """Extracts the Supabase JWT. Enforcement happens on the RPC we call
    (SECURITY DEFINER checks global_superadmin). Same shape as existing main.py."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    return authorization[len("Bearer "):]


# ── Admin: read dashboard status ───────────────────────────────────────────
@router.get("/admin/status")
async def admin_status(token: str = Depends(require_admin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_REST}/rpc/get_mc_process_status",
            headers={
                "apikey":        SB_HEADERS["apikey"],
                "Authorization": f"Bearer {token}",  # user JWT — RLS applies
                "Content-Type":  "application/json",
            },
            json={},
            timeout=HTTP_TIMEOUT_SUPABASE,
        )
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text[:400])
    return r.json()


# ── Admin: set control state ───────────────────────────────────────────────
@router.post("/admin/control")
async def admin_control(request: Request, token: str = Depends(require_admin)) -> dict[str, Any]:
    payload = await request.json()
    process = payload.get("process")
    if process not in ("ingest", "index"):
        raise HTTPException(status_code=400, detail="process must be 'ingest' or 'index'")
    body = {
        "p_process":            process,
        "p_desired_state":      payload.get("desired_state"),
        "p_poll_interval_sec":  payload.get("poll_interval_sec"),
        "p_batch_size":         payload.get("batch_size"),
        "p_concurrency":        payload.get("concurrency"),
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_REST}/rpc/set_mc_process_control",
            headers={
                "apikey":        SB_HEADERS["apikey"],
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json=body,
            timeout=HTTP_TIMEOUT_SUPABASE,
        )
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text[:400])
    return {"ok": True, "row": r.json()}


# ── Admin: force reindex specific ids ──────────────────────────────────────
@router.post("/admin/index/reindex-ids")
async def admin_reindex_ids(request: Request, token: str = Depends(require_admin)) -> dict[str, Any]:
    payload = await request.json()
    ids: list[str] = list(payload.get("ids") or [])
    if not ids or len(ids) > 500:
        raise HTTPException(status_code=400, detail="ids: 1-500 items")
    async with httpx.AsyncClient() as client:
        ok, errors = await index_ids(client, ids)
    return {"ok": True, "indexed": ok, "errors_count": len(errors),
            "first_errors": errors[:3]}


# ── Admin: reload synonyms ────────────────────────────────────────────────
@router.post("/admin/synonyms/reload")
async def admin_synonyms_reload(token: str = Depends(require_admin)) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        count = await sync_synonyms(client)
    return {"ok": True, "synonyms_synced": count}



@router.post("/admin/schema/ensure-fields")
async def admin_ensure_fields(
    background_tasks: BackgroundTasks,
    token: str = Depends(require_admin),
) -> dict[str, Any]:
    async def _run():
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                await ensure_fields(client)
            logger.info("[mc] ensure_fields (manual trigger) completed")
        except Exception as e:
            logger.exception(f"[mc] ensure_fields failed: {e}")
    background_tasks.add_task(_run)
    return {"ok": True, "message": "Schema migration started in background — "
                                    "watch logs or poll GET /mc/health."}


# ── Admin: rebuild the search-suggestions collection ───────────────────────
# Manual-trigger only (implementation plan, Part 2) — no cron/background
# loop added by this change. Reads master_candidates_v1 via Typesense's own
# bulk export (never touches Postgres, never affects /mc/search_v2), writes
# only to the separate suggestions collection.
@router.post("/admin/suggestions/rebuild")
async def admin_suggestions_rebuild(
    background_tasks: BackgroundTasks,
    token: str = Depends(require_admin),
) -> dict[str, Any]:
    async def _run():
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                await ensure_suggestions_collection(client)
                report = await rebuild_suggestions(client)
            logger.info(
                f"[mc] suggestions rebuild completed: "
                f"processed={report.documents_processed} rows={len(report.rows)}"
            )
        except Exception as e:
            logger.exception(f"[mc] suggestions rebuild failed: {e}")
    background_tasks.add_task(_run)
    return {"ok": True, "message": "Suggestions rebuild started in background — "
                                    "watch logs for completion."}


# ── Webhook: naukri-save posts here AFTER merge_naukri_candidates succeeds ─
@router.post("/webhook/naukri-inserted", dependencies=[Depends(require_webhook)])
async def webhook_naukri_inserted(request: Request) -> dict[str, Any]:
    payload = await request.json()

    naukri_ids = [
        str(x)
        for x in (payload.get("naukri_ids") or [])
        if x
    ]

    queued = 0

    for nid in naukri_ids[:1000]:
        try:
            WEBHOOK_QUEUE.put_nowait(nid)
            queued += 1
        except Exception:
            break

    return {
        "ok": True,
        "queued": queued,
    }

# ── Webhook: master_candidates row changed (optional trigger-driven path) ─
@router.post("/webhook/master-changed", dependencies=[Depends(require_webhook)])
async def webhook_master_changed(request: Request) -> dict[str, Any]:
    """
    Optional low-latency path. If you add a Supabase trigger on
    master_candidates that pg_net.http_post's the row id here, we index it
    within seconds instead of waiting for the poll cycle.
    """
    payload = await request.json()
    ids = payload.get("ids") or []
    if not ids:
        return {"ok": True, "indexed": 0}
    async with httpx.AsyncClient() as client:
        ok, errors = await index_ids(client, ids[:500])
    return {"ok": True, "indexed": ok, "errors_count": len(errors)}