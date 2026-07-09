"""
sync_service/master_candidates/config.py

Centralized settings for the master_candidates pipeline. Reads env vars the
docker-compose.yml is going to inject. Matches the pattern used in main.py.
"""

import os


# ── Supabase (reused from main service) ────────────────────────────────────
SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SUPABASE_REST        = f"{SUPABASE_URL}/rest/v1"
SB_HEADERS = {
    "apikey":        SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type":  "application/json",
}

# ── Typesense (reused, but different collection than Zive-X) ──────────────
TYPESENSE_HOST    = os.environ.get("TYPESENSE_HOST", "typesense")
TYPESENSE_PORT    = int(os.environ.get("TYPESENSE_PORT", "8108"))
TYPESENSE_API_KEY = os.environ["TYPESENSE_API_KEY"]
TYPESENSE_BASE    = f"http://{TYPESENSE_HOST}:{TYPESENSE_PORT}"
TS_HEADERS = {
    "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
    "Content-Type":        "application/json",
}
TS_COLLECTION = os.environ.get("MC_COLLECTION_NAME", "master_candidates_v1")

# ── Webhook / admin auth (reused from main service) ────────────────────────
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
ADMIN_SECRET   = os.environ["ADMIN_SECRET"]

# ── Feature flags ──────────────────────────────────────────────────────────
INGEST_ENABLED = os.environ.get("MC_INGEST_ENABLED", "true").lower() == "true"
INDEX_ENABLED  = os.environ.get("MC_INDEX_ENABLED",  "true").lower() == "true"

# ── Poll / batch defaults (override via mc_process_control table) ──────────
DEFAULT_POLL_INTERVAL_SEC = int(os.environ.get("MC_POLL_INTERVAL_SEC", "60"))
DEFAULT_BATCH_SIZE        = int(os.environ.get("MC_BATCH_SIZE", "500"))
DEFAULT_CONCURRENCY       = int(os.environ.get("MC_CONCURRENCY", "10"))

# ── Timeouts ───────────────────────────────────────────────────────────────
HTTP_TIMEOUT_SUPABASE = 30.0
HTTP_TIMEOUT_TYPESENSE = 20.0