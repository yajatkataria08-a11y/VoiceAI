"""
app/main.py
FastAPI application factory with lifespan startup/shutdown.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket

from app.core.logging import log
from app.database.db import init_db, close_db
from app.security.rate_limit import init_redis
from app.services.calendar_service import init_calendar
from app.scheduler.scheduler import start_scheduler, stop_scheduler
from app.api.routes import router as http_router
from app.api.dashboard import router as dashboard_router
from app.api.websocket import media_stream


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_redis()
    await init_db()
    init_calendar()
    await start_scheduler()
    log.info("voiceai_started")
    yield
    # Shutdown
    stop_scheduler()
    await close_db()
    log.info("voiceai_stopped")


app = FastAPI(title="VoiceAI Telecom Backend", lifespan=lifespan)

# Register routers
app.include_router(http_router)
app.include_router(dashboard_router)

# WebSocket endpoint
app.websocket("/media-stream")(media_stream)
