"""
app/utils/helpers.py
Retry wrapper, simple in-memory cache, misc helpers.
"""
from __future__ import annotations
import asyncio
import random
import time as _time_mod
from typing import Optional

from app.core.logging import log

# -- Retry with exponential backoff -------------------------------------------

async def api_call_with_retry(coro_fn, *args, retries: int = 3, base_delay: float = 0.5, **kwargs):
    """Call an async function with exponential backoff on failure."""
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(retries):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.3)
                log.warning("api_retry", attempt=attempt + 1, delay=round(delay, 2), error=str(exc))
                await asyncio.sleep(delay)
    raise last_exc


# -- Simple in-memory response cache ------------------------------------------
_CACHE_TTL_SECS = 300  # 5 minutes
_response_cache: dict[str, tuple[float, str]] = {}


def cache_get(key: str) -> Optional[str]:
    entry = _response_cache.get(key)
    if entry and (_time_mod.monotonic() - entry[0]) < _CACHE_TTL_SECS:
        return entry[1]
    return None


def cache_set(key: str, value: str) -> None:
    _response_cache[key] = (_time_mod.monotonic(), value)


def cache_flush() -> int:
    count = len(_response_cache)
    _response_cache.clear()
    return count
