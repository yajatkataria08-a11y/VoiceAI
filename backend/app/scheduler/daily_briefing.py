"""
app/scheduler/daily_briefing.py
Proactive outbound call jobs: appointment reminders, medicine reminders,
festive greetings, daily briefings.
All jobs acquire a Redis distributed lock before running to prevent
duplicate firing on multi-worker deployments.
"""
from __future__ import annotations
import asyncio
import html
import re
from datetime import datetime, timedelta

from app.core.config import (
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
    GOOGLE_CALENDAR_ID, TIMEZONE,
)
from app.core.constants import INDIAN_FESTIVALS
from app.core.logging import log
from app.security.rate_limit import acquire_scheduler_lock


# -- Appointment reminders ----------------------------------------------------

async def check_appointment_reminders() -> None:
    """Fire outbound reminder calls 30 min before Google Calendar events."""
    if not await acquire_scheduler_lock("appointment_reminders"):
        return
    from app.services.calendar_service import gcal_service
    if gcal_service is None or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        now     = datetime.utcnow()
        window  = (now + timedelta(minutes=29)).isoformat() + "Z"
        loop    = asyncio.get_running_loop()
        events  = await loop.run_in_executor(
            None,
            lambda: gcal_service.events().list(
                calendarId=GOOGLE_CALENDAR_ID,
                timeMin=now.isoformat() + "Z", timeMax=window,
                singleEvents=True, orderBy="startTime",
            ).execute(),
        )
        from app.scheduler import _sent_reminders
        for event in events.get("items", []):
            event_id = event["id"]
            if event_id in _sent_reminders:
                continue
            desc  = event.get("description", "")
            phone = re.search(r"\+\d{10,15}", desc)
            if not phone:
                continue
            caller = phone.group(0)
            title  = event.get("summary", "your appointment")
            _sent_reminders.add(event_id)
            asyncio.ensure_future(_fire_outbound_say(
                caller,
                f"Hello! VoiceAI reminder: {title} starts in 30 minutes.",
            ))
            log.info("reminder_scheduled", event_id=event_id, caller=caller)
    except Exception as exc:
        log.error("reminder_check_failed", error=str(exc))


# -- Medicine reminders -------------------------------------------------------

async def check_medicine_reminders() -> None:
    """Fire medicine reminder calls at the scheduled time."""
    if not await acquire_scheduler_lock("medicine_reminders"):
        return
    from app.database.db import db_pool
    if db_pool is None or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        from zoneinfo import ZoneInfo
        now_time = datetime.now(ZoneInfo(TIMEZONE)).strftime("%H:%M")
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT caller, medicine, language FROM medicine_reminders "
                "WHERE remind_time=$1 AND active=TRUE",
                now_time,
            )
        for row in rows:
            asyncio.ensure_future(_fire_medicine_call(
                row["caller"], row["medicine"], row.get("language") or "hi"
            ))
    except Exception as exc:
        log.error("medicine_reminder_check_failed", error=str(exc))


async def _fire_medicine_call(caller: str, medicine: str, lang: str) -> None:
    reminder_texts = {
        "hi":    f"नमस्ते! अब आपकी दवाई {medicine} लेने का समय है। कृपया अभी दवाई लें।",
        "hi-en": f"Namaste! Time to take your {medicine}. Please take your medicine now.",
        "ta":    f"வணக்கம்! இப்போது உங்கள் {medicine} மருந்து எடுக்கும் நேரம்.",
        "bn":    f"নমস্কার! এখন আপনার {medicine} ওষুধ খাওয়ার সময়।",
        "mr":    f"नमस्कार! आता तुमची {medicine} औषधे घेण्याची वेळ आहे.",
    }
    text = reminder_texts.get(lang, reminder_texts["hi-en"])
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Aditi">{html.escape(text)}</Say></Response>'
    )
    await _fire_twiml_call(caller, twiml)
    log.info("medicine_call_fired", caller=caller, medicine=medicine)


# -- Daily briefings ----------------------------------------------------------

async def send_daily_briefings(period: str = "morning") -> None:
    """Outbound personalised morning/evening briefing calls."""
    if not await acquire_scheduler_lock(f"daily_briefing_{period}"):
        return
    from app.database.db import db_pool
    if not db_pool or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        async with db_pool.acquire() as conn:
            callers = await conn.fetch(
                "SELECT caller, name, city, language FROM caller_profiles "
                "WHERE city IS NOT NULL AND caller IS NOT NULL"
            )
        for row in callers:
            asyncio.ensure_future(_fire_daily_briefing(
                row["caller"], row.get("name") or "",
                row.get("city") or "Delhi", row.get("language") or "en", period,
            ))
    except Exception as exc:
        log.error("daily_briefing_job_failed", error=str(exc))


async def _fire_daily_briefing(caller: str, name: str, city: str, lang: str, period: str) -> None:
    from app.services.weather_service import get_weather
    from app.services.news_service import get_news
    greeting  = "Good morning" if period == "morning" else "Good evening"
    name_part = f", {name}" if name else ""
    try:
        weather  = await get_weather(city)
        news     = await get_news("general")
        briefing = f"{greeting}{name_part}! Here's your {period} briefing. Weather: {weather} News: {news}"
    except Exception:
        briefing = f"{greeting}{name_part}! VoiceAI daily briefing. Have a wonderful {period}!"
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Joanna">{html.escape(briefing)}</Say></Response>'
    )
    await _fire_twiml_call(caller, twiml)


# -- Festive greetings --------------------------------------------------------

async def check_and_send_festive_greetings() -> None:
    """Send outbound festive greeting calls on festival dates."""
    if not await acquire_scheduler_lock("festive_greetings", ttl_secs=43200):
        return
    from app.database.db import db_pool
    if not db_pool or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    today = datetime.now()
    for festival in INDIAN_FESTIVALS:
        if festival["month"] == today.month and festival["day"] == today.day:
            try:
                async with db_pool.acquire() as conn:
                    callers = await conn.fetch(
                        "SELECT caller, language FROM caller_profiles WHERE caller IS NOT NULL"
                    )
                for row in callers:
                    lang     = row.get("language") or "en"
                    greeting = festival.get("greeting_hi" if lang in ("hi", "hi-en") else "greeting_en", "")
                    asyncio.ensure_future(_fire_outbound_say(row["caller"], greeting))
                log.info("festive_calls_queued", festival=festival["name"], count=len(callers))
            except Exception as exc:
                log.error("festive_greeting_failed", festival=festival["name"], error=str(exc))


# -- Shared helper ------------------------------------------------------------

async def _fire_outbound_say(caller: str, text: str) -> None:
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Joanna">{html.escape(text)}</Say></Response>'
    )
    await _fire_twiml_call(caller, twiml)


async def _fire_twiml_call(caller: str, twiml: str) -> None:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    loop = asyncio.get_running_loop()
    try:
        from app.telecom.twilio_handler import fire_outbound_call_sync
        await loop.run_in_executor(None, fire_outbound_call_sync, caller, twiml)
    except Exception as exc:
        log.error("outbound_call_failed", caller=caller, error=str(exc))
