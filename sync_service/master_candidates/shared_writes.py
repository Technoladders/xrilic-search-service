"""
sync_service/master_candidates/shared_writes.py

Kill switch for an instance that shares the production Supabase project but
must never mutate the state production depends on -- the test stack behind
sync-test.xrilic.ai (docker-compose.test.yml sets
SHARED_STATE_WRITES_ENABLED=false).

When disabled:
  - main.py does not start the mc index loop, the mc ingest loop, or the
    backfill claim watchdog (each one advances shared cursors / claims the
    single-claimant backfill job / writes master_candidates)
  - every admin endpoint that writes that shared state returns 403 via
    require_shared_writes()

Defaults to enabled, so production behaviour is unchanged.
"""

from fastapi import HTTPException

from .config import SHARED_STATE_WRITES_ENABLED


def require_shared_writes() -> None:
    if not SHARED_STATE_WRITES_ENABLED:
        raise HTTPException(
            status_code=403,
            detail="shared_state_writes_disabled (SHARED_STATE_WRITES_ENABLED=false on this instance)",
        )
