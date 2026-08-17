"""
sync_service/master_candidates/backfill/config.py

Settings specific to the portal_a backfill module. Reuses the shared
Supabase connection constants from ../config.py (SB_HEADERS, SUPABASE_REST,
SUPABASE_URL, HTTP_TIMEOUT_SUPABASE) rather than duplicating them -- this
module does not define its own Supabase client config.
"""

import os

SOURCE = "portal_a"

DEFAULT_BATCH_SIZE  = int(os.environ.get("MC_BACKFILL_BATCH_SIZE", "500"))
DEFAULT_CONCURRENCY = int(os.environ.get("MC_BACKFILL_CONCURRENCY", "20"))
AUTO_RESUME         = os.environ.get("MC_BACKFILL_AUTO_RESUME", "true").lower() == "true"

# Server-side caps -- a caller-supplied batch_size/concurrency above these is
# clamped, mirroring admin_api.py's existing "ids: 1-500 items" cap style.
MAX_BATCH_SIZE  = 2000
MAX_CONCURRENCY = 50

# Dry-run is capped small and deliberately runs synchronously within one
# HTTP request -- see backfill/api.py's dry_run handler.
DEFAULT_DRY_RUN_PAGES = 1
MAX_DRY_RUN_PAGES     = 5

# Single-claimant heartbeat: a claim older than this is considered dead and
# may be reclaimed by another instance (or this one, on a later watchdog tick).
CLAIM_STALE_AFTER_SEC = 120
CLAIM_WATCHDOG_INTERVAL_SEC = 30

# Per-chunk optimistic-concurrency retry (existing-master merge race).
MERGE_RETRY_ATTEMPTS = 5
MERGE_RETRY_BASE_DELAY_SEC = 0.05

# Number of lock shards for both the pre-resolution identity lock and the
# post-resolution master-id lock. Bounded so memory never grows with the
# number of distinct rows/masters a long backfill touches.
LOCK_SHARD_COUNT = 256

# Transient-error backoff inside the main worker loop (run_backfill_loop).
CYCLE_ERROR_SLEEP_SEC = 15
