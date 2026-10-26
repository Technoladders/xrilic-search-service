"""
sync_service/master_candidates/test_deploy_guards.py

Static guards for production-impacting settings that live outside Python:
  * a code deploy must recreate only the API container (recreating typesense
    means a 20-40 min reload of the ~2M-document index);
  * the backfill must not default back to 20 parallel merges, which saturated
    the production database's disk and memory.
"""
import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")


def test_deploy_recreates_only_the_api_container():
    assert "docker compose up -d --no-deps --force-recreate xrilic-search-service" in DEPLOY
    assert "--remove-orphans" not in DEPLOY
    # no bare `docker compose up -d --force-recreate` (which would include typesense)
    for line in DEPLOY.splitlines():
        s = line.strip()
        if s.startswith("docker compose up"):
            assert "xrilic-search-service" in s and "--no-deps" in s


def test_deploy_sets_a_modest_backfill_concurrency():
    assert 'echo "MC_BACKFILL_CONCURRENCY=5" >> .env' in DEPLOY


def test_default_backfill_concurrency_is_modest(monkeypatch):
    monkeypatch.delenv("MC_BACKFILL_CONCURRENCY", raising=False)
    from master_candidates.backfill import config
    try:
        importlib.reload(config)
        assert config.DEFAULT_CONCURRENCY == 5
    finally:
        monkeypatch.undo()
        importlib.reload(config)
