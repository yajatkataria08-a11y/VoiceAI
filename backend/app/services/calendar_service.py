"""app/services/calendar_service.py — Google Calendar booking + callback scheduling."""
from __future__ import annotations
import asyncio
import html
import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from app.core.config import (
    GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_CALENDAR_ID, TIMEZONE,
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
)
from app.core.logging import log
from app.telecom.twilio_handler import send_sms

# Global calendar service (set by app startup via init_calendar())
gcal_service = None
_pending_callbacks: list[dict] = []


def init_calendar():
    global gcal_service
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        log.warning("gcal_disabled", reason="GOOGLE_SERVICE_ACCOUNT_JSON not set")
        return
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/calendar"],
        )
        gcal_service = build("calendar", "v3", credentials=creds)
        log.info("gcal_initialised")
    except Exception as exc:
        log.error("gcal_init_failed", error=str(exc))


def _parse_rfc3339(date: str, time_str: str, tz_name: str) -> tuple[str, str]:
    tz       = ZoneInfo(tz_name)
    start_dt = datetime.strptime(f"{date} {time_str}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    end_dt   = start_dt + timedelta(hours=1)
    def _colon_tz(s: str) -> str:
        return s[:-2] + ":" + s[-2:] if len(s) > 3 else s
    fmt = "%Y-%m-%dT%H:%M:%S%z"
    return _colon_tz(start_dt.strftime(fmt)), _colon_tz(end_dt.strftime(fmt))


async def book_appointment(title: str, date: str, time_str: str, caller_number: str = "") -> str:
    if gcal_service is None:
        return "Calendar booking is not configured."
    try:
        start_rfc, end_rfc = _parse_rfc3339(date, time_str, TIMEZONE)
    except (ValueError, KeyError) as exc:
        return f"I couldn't understand that date or time: {exc}."
    event_body = {
        "summary": title,
        "description": f"Booked via VoiceAI. Caller: {caller_number}",
        "start": {"dateTime": start_rfc, "timeZone": TIMEZONE},
        "end":   {"dateTime": end_rfc,   "timeZone": TIMEZONE},
    }
    try:
        loop    = asyncio.get_running_loop()
        created = await loop.run_in_executor(
            None,
            lambda: gcal_service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event_body).execute(),
        )
        log.info("gcal_event_created", link=created.get("htmlLink"))
        _dt = datetime.strptime(time_str, "%H:%M")
        display_time = f"{_dt.hour % 12 or 12}:{_dt.minute:02d} {_dt.strftime('%p')}"
        confirmation = f"Booked: {title} on {date} at {display_time}."
        if caller_number:
            await loop.run_in_executor(
                None, send_sms, caller_number,
                f"Appointment confirmed!\n{title}\n{date} at {display_time}\nPowered by VoiceAI",
            )
        return confirmation
    except Exception as exc:
        log.error("gcal_error", error=str(exc))
        return "Sorry, something went wrong while booking."


async def schedule_callback(callback_time: str, callback_date: str, reason: str = "", caller_number: str = "") -> str:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return "Callback service is not configured."
    if not caller_number:
        return "I need your phone number to schedule a callback."
    try:
        from app.scheduler.scheduler import app_scheduler
        from zoneinfo import ZoneInfo
        tz     = ZoneInfo(TIMEZONE)
        run_at = datetime.strptime(f"{callback_date} {callback_time}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    except ValueError:
        return "I couldn't understand that time. Please say a time like '5 PM' or '17:00'."
    _pending_callbacks.append({"caller": caller_number, "at": run_at.isoformat(), "reason": reason})
    try:
        from app.scheduler.scheduler import app_scheduler
        app_scheduler.add_job(
            _fire_callback_sync, "date", run_date=run_at,
            args=[caller_number, reason or "your requested callback"],
            id=f"cb_{caller_number}_{callback_time}", replace_existing=True,
        )
    except Exception as exc:
        log.warning("scheduler_add_job_failed", error=str(exc))
    _dt = datetime.strptime(callback_time, "%H:%M")
    display_time = f"{_dt.hour % 12 or 12}:{_dt.minute:02d} {_dt.strftime('%p')}"
    return f"Got it! I'll call you back at {display_time}."


def _fire_callback_sync(caller: str, reason: str) -> None:
    from twilio.rest import Client
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        f'<Say voice="Polly.Joanna">Hello! VoiceAI calling back as requested. '
        f'You asked me to call about: {html.escape(reason)}. How can I help?</Say>'
        '</Response>'
    )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("callback_fired", caller=caller)
