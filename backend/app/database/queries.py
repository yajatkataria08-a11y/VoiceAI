"""
app/database/queries.py
All DB read/write operations. No business logic here.
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import Optional

from app.core.logging import log
from app.database.db import db_pool


# -- Call logging --------------------------------------------------------------

async def log_call_to_db(
    stream_sid: str,
    caller: str,
    start_time: datetime,
    end_time: datetime,
    transcript: list[dict],
    language: str,
    emotions_json: list[dict],
    dominant_emotion: str,
    topics: list[str],
    flagged: bool = False,
    ab_bucket: str = "",
) -> None:
    if db_pool is None:
        return
    duration = int((end_time - start_time).total_seconds())
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calls
                    (stream_sid, caller, start_time, end_time,
                     transcript, language, duration_seconds,
                     emotions, dominant_emotion, topics, flagged, ab_bucket)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8::jsonb,$9,$10::jsonb,$11,$12)
                """,
                stream_sid, caller, start_time, end_time,
                json.dumps(transcript), language, duration,
                json.dumps(emotions_json), dominant_emotion,
                json.dumps(topics), flagged, ab_bucket,
            )
        log.info("call_logged", stream_sid=stream_sid, duration=duration, dominant_emotion=dominant_emotion)
    except Exception as exc:
        log.error("db_log_failed", error=str(exc))


async def fetch_recent_calls(limit: int = 20) -> list[dict]:
    if db_pool is None:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, stream_sid, caller, start_time, end_time,
                      language, duration_seconds, emotions, dominant_emotion,
                      topics, flagged, ab_bucket, satisfied
               FROM calls ORDER BY start_time DESC LIMIT $1""",
            min(limit, 100),
        )
    return [dict(r) for r in rows]


async def fetch_flagged_calls(limit: int = 50) -> list[dict]:
    if db_pool is None:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, stream_sid, caller, start_time, dominant_emotion, topics
               FROM calls WHERE flagged=TRUE ORDER BY start_time DESC LIMIT $1""",
            limit,
        )
    return [dict(r) for r in rows]


# -- Caller profiles -----------------------------------------------------------

async def load_caller_profile(caller_number: str) -> dict:
    if not db_pool or not caller_number:
        return {}
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT name, city, language, last_topic,
                          elderly_mode, cloned_voice_id, family_contacts
                   FROM caller_profiles WHERE caller=$1""",
                caller_number,
            )
        if row:
            result = {k: v for k, v in dict(row).items() if v is not None}
            if "family_contacts" in result and isinstance(result["family_contacts"], str):
                try:
                    result["family_contacts"] = json.loads(result["family_contacts"])
                except Exception:
                    result["family_contacts"] = []
            return result
    except Exception as exc:
        log.warning("load_caller_profile_failed", error=str(exc))
    return {}


async def save_caller_profile(caller_number: str, profile: dict) -> None:
    if not db_pool or not caller_number:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles
                    (caller, name, city, language, last_topic, last_seen, elderly_mode, cloned_voice_id)
                VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)
                ON CONFLICT (caller) DO UPDATE
                    SET name            = COALESCE(EXCLUDED.name,            caller_profiles.name),
                        city            = COALESCE(EXCLUDED.city,            caller_profiles.city),
                        language        = COALESCE(EXCLUDED.language,        caller_profiles.language),
                        last_topic      = COALESCE(EXCLUDED.last_topic,      caller_profiles.last_topic),
                        elderly_mode    = EXCLUDED.elderly_mode,
                        cloned_voice_id = COALESCE(EXCLUDED.cloned_voice_id, caller_profiles.cloned_voice_id),
                        last_seen       = NOW()
                """,
                caller_number,
                profile.get("name"), profile.get("city"), profile.get("language"),
                profile.get("last_topic"), bool(profile.get("elderly_mode", False)),
                profile.get("cloned_voice_id"),
            )
    except Exception as exc:
        log.warning("save_caller_profile_failed", error=str(exc))


# -- PIN auth ------------------------------------------------------------------

async def get_or_create_pin(caller_number: str) -> str:
    import secrets
    if db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT pin FROM caller_pins WHERE caller=$1", caller_number)
            if row:
                return row["pin"]
            new_pin = str(secrets.randbelow(9000) + 1000)
            await conn.execute(
                "INSERT INTO caller_pins(caller, pin) VALUES($1,$2) ON CONFLICT(caller) DO NOTHING",
                caller_number, new_pin,
            )
            return new_pin
    import secrets
    return str(secrets.randbelow(9000) + 1000)


# -- Voice memos ---------------------------------------------------------------

async def db_save_memo(caller: str, memo: str) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO voice_memos(caller, memo) VALUES($1, $2)", caller, memo
        )


async def db_recall_memos(caller: str) -> list[str]:
    if not db_pool:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT memo, created_at FROM voice_memos WHERE caller=$1 ORDER BY created_at DESC LIMIT 5",
            caller,
        )
    return [f"{r['memo']} (saved {r['created_at'].strftime('%b %d %H:%M')})" for r in rows]


# -- Analytics -----------------------------------------------------------------

async def analytics_emotions(days: int = 7) -> list[dict]:
    if db_pool is None:
        return []
    from datetime import timedelta
    since = datetime.utcnow() - timedelta(days=days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT dominant_emotion, COUNT(*) AS count FROM calls "
            "WHERE start_time >= $1 AND dominant_emotion IS NOT NULL "
            "GROUP BY dominant_emotion ORDER BY count DESC",
            since,
        )
    return [{"emotion": r["dominant_emotion"], "count": r["count"]} for r in rows]


async def analytics_topics(days: int = 7) -> list[dict]:
    if db_pool is None:
        return []
    from collections import Counter
    from datetime import timedelta
    since = datetime.utcnow() - timedelta(days=days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT topics FROM calls WHERE start_time >= $1 AND topics IS NOT NULL", since
        )
    counter: Counter = Counter()
    for r in rows:
        try:
            for t in json.loads(r["topics"] or "[]"):
                counter[t] += 1
        except Exception:
            pass
    return [{"topic": t, "count": c} for t, c in counter.most_common()]


async def analytics_languages(days: int = 7) -> list[dict]:
    if db_pool is None:
        return []
    from datetime import timedelta
    since = datetime.utcnow() - timedelta(days=days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT language, COUNT(*) AS count FROM calls "
            "WHERE start_time >= $1 AND language IS NOT NULL "
            "GROUP BY language ORDER BY count DESC",
            since,
        )
    return [{"language": r["language"], "count": r["count"]} for r in rows]
