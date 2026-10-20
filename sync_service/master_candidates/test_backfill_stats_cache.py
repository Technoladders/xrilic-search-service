"""
sync_service/master_candidates/test_backfill_stats_cache.py

GET /mc/admin/backfill/stats runs six exact counts (~4 min of DB time per
call in production). It must compute at most once per TTL, and overlapping
requests must share one computation. No network — _compute_stats is faked.
"""
import asyncio

import pytest

from master_candidates.backfill import api


def _stats(**overrides):
    base = {k: 1 for k in api._STATS_COUNT_KEYS}
    base.update(total_uningested=0, latest_source_updated_at=None,
                oldest_source_updated_at=None, latest_master_updated_at=None)
    base.update(overrides)
    return base


@pytest.fixture
def fake_compute(monkeypatch):
    calls = {"n": 0, "result": _stats(), "delay": 0.0}

    async def fake():
        calls["n"] += 1
        if calls["delay"]:
            await asyncio.sleep(calls["delay"])
        return dict(calls["result"])

    monkeypatch.setattr(api, "_compute_stats", fake)
    monkeypatch.setattr(api, "_stats_lock", asyncio.Lock())
    monkeypatch.setattr(api, "_stats_cache", {"at": None, "data": None})
    return calls


@pytest.fixture
def clock(monkeypatch):
    now = {"t": 1000.0}
    monkeypatch.setattr(api, "_monotonic", lambda: now["t"])
    return now


def _call(refresh=False):
    return api.backfill_stats(refresh=refresh, user_id="superadmin")


def test_manual_refresh_bypasses_ttl_but_is_rate_limited(fake_compute, clock):
    asyncio.run(_call())
    clock["t"] += api.STATS_MIN_REFRESH_SEC - 1
    assert asyncio.run(_call(refresh=True))["cached"] is True     # too soon: cached
    clock["t"] += 2
    assert asyncio.run(_call(refresh=True))["cached"] is False    # >= min age: recomputed
    assert fake_compute["n"] == 2


def test_responses_carry_computed_at(fake_compute, clock):
    first = asyncio.run(_call())
    second = asyncio.run(_call())
    assert isinstance(first["computed_at"], float)
    assert second["computed_at"] == first["computed_at"]


def test_second_call_within_ttl_is_served_from_cache(fake_compute, clock):
    first = asyncio.run(_call())
    clock["t"] += api.STATS_CACHE_TTL_SEC - 1
    second = asyncio.run(_call())
    assert fake_compute["n"] == 1
    assert first["cached"] is False and second["cached"] is True
    assert second["total_master_candidates"] == first["total_master_candidates"]


def test_recomputes_after_ttl(fake_compute, clock):
    asyncio.run(_call())
    clock["t"] += api.STATS_CACHE_TTL_SEC
    asyncio.run(_call())
    assert fake_compute["n"] == 2


def test_overlapping_requests_share_one_computation(fake_compute, clock):
    fake_compute["delay"] = 0.05

    async def burst():
        return await asyncio.gather(*[_call() for _ in range(5)])

    results = asyncio.run(burst())
    assert fake_compute["n"] == 1
    assert sum(1 for r in results if r["cached"] is False) == 1


def test_partial_result_is_not_cached(fake_compute, clock):
    fake_compute["result"] = _stats(total_source_rows=None)   # one count timed out
    asyncio.run(_call())
    asyncio.run(_call())
    assert fake_compute["n"] == 2


def test_failure_is_not_cached(fake_compute, clock, monkeypatch):
    async def boom():
        raise RuntimeError("supabase down")

    monkeypatch.setattr(api, "_compute_stats", boom)
    with pytest.raises(RuntimeError):
        asyncio.run(_call())
    assert api._stats_cache["at"] is None
