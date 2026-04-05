"""
app/database/db.py
asyncpg connection pool — created at startup, closed at shutdown.
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import Optional

import asyncpg

from app.core.config import DATABASE_URL
from app.core.logging import log

db_pool: Optional[asyncpg.Pool] = None

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS calls (
    id               SERIAL PRIMARY KEY,
    stream_sid       TEXT,
    caller           TEXT,
    start_time       TIMESTAMPTZ,
    end_time         TIMESTAMPTZ,
    transcript       JSONB,
    language         TEXT,
    duration_seconds INT,
    emotions         JSONB,
    dominant_emotion TEXT,
    topics           JSONB,
    flagged          BOOLEAN DEFAULT FALSE,
    ab_bucket        TEXT,
    satisfied        BOOLEAN
);
CREATE TABLE IF NOT EXISTS caller_pins (
    caller TEXT PRIMARY KEY,
    pin    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS medicine_reminders (
    id          SERIAL PRIMARY KEY,
    caller      TEXT,
    medicine    TEXT,
    remind_time TEXT,
    active      BOOLEAN DEFAULT TRUE,
    language    TEXT DEFAULT 'hi'
);
CREATE TABLE IF NOT EXISTS caller_profiles (
    caller          TEXT PRIMARY KEY,
    name            TEXT,
    city            TEXT,
    language        TEXT,
    last_topic      TEXT,
    last_seen       TIMESTAMPTZ,
    elderly_mode    BOOLEAN DEFAULT FALSE,
    cloned_voice_id TEXT,
    family_contacts JSONB
);
CREATE TABLE IF NOT EXISTS voice_memos (
    id         SERIAL PRIMARY KEY,
    caller     TEXT,
    memo       TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

_MIGRATIONS = [
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS emotions JSONB",
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS dominant_emotion TEXT",
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS topics JSONB",
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS flagged BOOLEAN DEFAULT FALSE",
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS ab_bucket TEXT",
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS satisfied BOOLEAN",
    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS elderly_mode BOOLEAN DEFAULT FALSE",
    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS cloned_voice_id TEXT",
    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS family_contacts JSONB",
    "ALTER TABLE medicine_reminders ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'hi'",
]


async def init_db() -> None:
    global db_pool
    if not DATABASE_URL:
        log.warning("db_disabled", reason="DATABASE_URL not set")
        return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, command_timeout=30)
        async with db_pool.acquire() as conn:
            await conn.execute(_CREATE_TABLES_SQL)
            for sql in _MIGRATIONS:
                await conn.execute(sql)
        log.info("db_pool_created")
    except Exception as exc:
        log.error("db_pool_failed", error=str(exc))
        db_pool = None


async def close_db() -> None:
    global db_pool
    if db_pool:
        await db_pool.close()
        log.info("db_pool_closed")
