"""
app/scheduler/scheduler.py
APScheduler setup and all scheduled jobs.
"""
from __future__ import annotations
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import TIMEZONE, DAILY_BRIEFING_ENABLED
from app.core.logging import log

app_scheduler = AsyncIOScheduler(timezone=TIMEZONE)


async def start_scheduler() -> None:
    from app.scheduler.daily_briefing import (
        check_appointment_reminders,
        check_medicine_reminders,
        check_and_send_festive_greetings,
    )

    app_scheduler.add_job(
        check_appointment_reminders, "interval", minutes=1,
        id="appointment_reminders", replace_existing=True,
    )
    app_scheduler.add_job(
        check_medicine_reminders, "interval", minutes=1,
        id="medicine_reminders", replace_existing=True,
    )
    app_scheduler.add_job(
        check_and_send_festive_greetings, "interval", hours=12,
        id="festive_greetings", replace_existing=True,
    )

    if DAILY_BRIEFING_ENABLED:
        from app.scheduler.daily_briefing import send_daily_briefings
        from app.core.config import DAILY_BRIEFING_MORNING_TIME, DAILY_BRIEFING_EVENING_TIME

        h_m, h_e = DAILY_BRIEFING_MORNING_TIME.split(":"), DAILY_BRIEFING_EVENING_TIME.split(":")
        app_scheduler.add_job(
            send_daily_briefings, "cron",
            hour=int(h_m[0]), minute=int(h_m[1]),
            args=["morning"], id="daily_briefing_morning", replace_existing=True,
        )
        app_scheduler.add_job(
            send_daily_briefings, "cron",
            hour=int(h_e[0]), minute=int(h_e[1]),
            args=["evening"], id="daily_briefing_evening", replace_existing=True,
        )

    app_scheduler.start()
    log.info("scheduler_started")


def stop_scheduler() -> None:
    app_scheduler.shutdown(wait=False)
    log.info("scheduler_stopped")
