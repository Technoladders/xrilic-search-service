import pytest
from fastapi import HTTPException

from master_candidates.search_api import _UserSearchRateLimiter


@pytest.mark.asyncio
async def test_rate_limit_applies_per_user_across_requests(monkeypatch):
    now = 100.0
    monkeypatch.setattr("master_candidates.search_api._time.monotonic", lambda: now)
    limiter = _UserSearchRateLimiter(limit=2, window_seconds=60)

    await limiter.check("user-1")
    await limiter.check("user-1")
    await limiter.check("user-2")

    with pytest.raises(HTTPException) as exc_info:
        await limiter.check("user-1")

    assert exc_info.value.status_code == 429
    assert exc_info.value.headers["Retry-After"] == "60"


@pytest.mark.asyncio
async def test_rate_limit_reopens_after_window(monkeypatch):
    now = 100.0
    monkeypatch.setattr("master_candidates.search_api._time.monotonic", lambda: now)
    limiter = _UserSearchRateLimiter(limit=1, window_seconds=60)

    await limiter.check("user-1")
    now += 60

    await limiter.check("user-1")
