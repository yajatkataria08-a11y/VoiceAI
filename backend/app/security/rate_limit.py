"""
app/security/rate_limit.py
Redis-backed rate limiting with in-memory fallback.
Also contains PerToolRateLimiter for per-session tool abuse detection.
"""
from __future__ import annotations
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from app.core.config import RATE_LIMIT_MAX_CALLS, RATE_LIMIT_WINDOW_SECS, REDIS_URL, ABUSE_PER_TOOL_LIMIT
from app.core.logging import log

_call_timestamps: dict[str, list[datetime]] = defaultdict(list)
_redis_client = None


async def init_redis() -> None:
    global _redis_client
    if not REDIS_URL:
        return
    try:
        import redis.asyncio as aioredis
        _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await _redis_client.ping()
        log.info("redis_connected")
    except Exception as exc:
        log.warning("redis_unavailable", error=str(exc), fallback="in-memory rate limiting")
        _redis_client = None


async def is_rate_limited(caller_number: str) -> bool:
    """True if caller has exceeded the call rate limit."""
    if not caller_number:
        return False
    now          = datetime.utcnow()
    window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW_SECS)

    if _redis_client is not None:
        try:
            key   = f"rl:{caller_number}"
            score = now.timestamp()
            pipe  = _redis_client.pipeline()
            pipe.zremrangebyscore(key, "-inf", window_start.timestamp())
            pipe.zcard(key)
            pipe.zadd(key, {str(score): score})
            pipe.expire(key, RATE_LIMIT_WINDOW_SECS + 10)
            results = await pipe.execute()
            count_before = results[1]
            if count_before >= RATE_LIMIT_MAX_CALLS:
                log.warning("rate_limit_exceeded", caller=caller_number, backend="redis")
                await _redis_client.zrem(key, str(score))
                return True
            return False
        except Exception as exc:
            log.warning("redis_rate_limit_error", error=str(exc), fallback="in-memory")

    # In-memory fallback
    _call_timestamps[caller_number] = [
        ts for ts in _call_timestamps[caller_number] if ts >= window_start
    ]
    if len(_call_timestamps[caller_number]) >= RATE_LIMIT_MAX_CALLS:
        log.warning("rate_limit_exceeded", caller=caller_number, backend="memory")
        return True
    _call_timestamps[caller_number].append(now)
    return False


async def acquire_scheduler_lock(job_name: str, ttl_secs: int = 55) -> bool:
    """
    Distributed lock for APScheduler jobs — prevents duplicate firing on multi-worker deploys.
    Returns True if this worker acquired the lock (should proceed).
    Falls back to True (always proceed) when Redis is not available.
    """
    if _redis_client is None:
        return True
    key = f"sched_lock:{job_name}"
    try:
        acquired = await _redis_client.set(key, "1", nx=True, ex=ttl_secs)
        return bool(acquired)
    except Exception as exc:
        log.warning("scheduler_lock_error", job=job_name, error=str(exc))
        return True  # fail open


class PerToolRateLimiter:
    """
    Per-session tool call limiter. Blocks a tool after ABUSE_PER_TOOL_LIMIT calls
    within a single WebSocket session to prevent LLM loops.
    """
    def __init__(self) -> None:
        self._counts: dict[str, int] = defaultdict(int)

    def record(self, tool_name: str) -> None:
        self._counts[tool_name] += 1

    def is_blocked(self, tool_name: str) -> bool:
        return self._counts[tool_name] >= ABUSE_PER_TOOL_LIMIT

    def summary(self) -> dict:
        return dict(self._counts)
