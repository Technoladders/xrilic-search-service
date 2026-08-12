"""
sync_service/conftest.py

Test-only setup: puts sync_service/ on sys.path (matching how main.py itself
imports — `from master_candidates import ...` as a top-level package, not a
dotted sync_service.master_candidates path) and provides dummy env vars so
config.py's required os.environ[...] lookups succeed at import time.

These are placeholder values only — never real secrets, and this file is
never used against the live Typesense/Supabase instances (see
master_candidates/verify_typesense_semantics.py for that, which takes real
credentials via env vars explicitly and is run manually, not via pytest).
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

for _key, _val in {
    "SUPABASE_URL": "http://fake.invalid",
    "SUPABASE_SERVICE_KEY": "fake-key",
    "TYPESENSE_API_KEY": "fake-key",
    "WEBHOOK_SECRET": "fake-secret",
    "ADMIN_SECRET": "fake-secret",
}.items():
    os.environ.setdefault(_key, _val)
