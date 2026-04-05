"""
app/api/routes.py
All HTTP routes: incoming call, IVR, callbacks, admin, analytics, health.
"""
from __future__ import annotations
import asyncio
import html
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from app.core.config import (
    TWILIO_PHONE_NUMBER, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
    PUBLIC_HOST, DASHBOARD_TOKEN,
)
from app.core.logging import log
from app.database.db import db_pool
from app.database.queries import (
    fetch_recent_calls, fetch_flagged_calls,
    analytics_emotions, analytics_topics, analytics_languages,
)
from app.security.rate_limit import is_rate_limited
from app.security.request_validation import verify_twilio_signature
from app.services.calendar_service import _pending_callbacks
from app.services.stt_service import transcribe_batch

router = APIRouter()

# -- Twilio webhooks ----------------------------------------------------------

@router.post("/incoming-call")
async def incoming_call(request: Request):
    form          = await request.form()
    form_dict     = dict(form)
    caller_number = form.get("From", "")
    host          = request.headers.get("host")
    log.info("incoming_call", caller=caller_number)

    if not verify_twilio_signature(request, form_dict):
        log.warning("twilio_signature_invalid", caller=caller_number)
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    if await is_rate_limited(caller_number):
        twiml = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Sorry, too many calls recently. Please try again later.</Say>
  <Hangup/>
</Response>"""
        return Response(content=twiml, media_type="application/xml")

    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Hello! I'm your voice assistant. How can I help you today?</Say>
  <Connect>
    <Stream url="wss://{html.escape(host)}/media-stream">
      <Parameter name="callerNumber" value="{html.escape(caller_number)}" />
    </Stream>
  </Connect>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


@router.post("/missed-call")
async def missed_call(request: Request):
    form          = await request.form()
    caller_number = form.get("From", form.get("caller", ""))
    call_status   = form.get("CallStatus", "missed").lower()
    if not caller_number:
        return {"error": "No caller number"}
    if call_status in ("no-answer", "busy", "missed", ""):
        asyncio.ensure_future(_auto_callback(caller_number))
        log.info("missed_call_received", caller=caller_number)
    return {"status": "ok", "caller": caller_number}


async def _auto_callback(caller: str) -> None:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    await asyncio.sleep(3)
    host  = PUBLIC_HOST
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response><Say voice="Polly.Joanna">Hello! You gave us a missed call. How can I help?</Say>'
        f'<Connect><Stream url="wss://{html.escape(host)}/media-stream">'
        f'<Parameter name="callerNumber" value="{html.escape(caller)}" />'
        '</Stream></Connect></Response>'
    )
    loop = asyncio.get_running_loop()
    try:
        from app.telecom.twilio_handler import fire_outbound_call_sync
        await loop.run_in_executor(None, fire_outbound_call_sync, caller, twiml)
    except Exception as exc:
        log.error("auto_callback_failed", caller=caller, error=str(exc))


@router.post("/ivr-input")
async def ivr_input(request: Request):
    form      = await request.form()
    form_dict = dict(form)
    digits    = form.get("Digits", "")
    sid       = form.get("StreamSid", "")
    if not verify_twilio_signature(request, form_dict):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")
    ivr_map = {
        "1": "What is the weather today",
        "2": "Give me the latest news",
        "3": "I want to book an appointment",
        "4": "Check my PNR status",
        "0": "Help me, what can you do",
    }
    # IVR state is session-local in websocket.py; we just acknowledge
    say = {
        "1": "Fetching weather.", "2": "Getting headlines.",
        "3": "Let me help you book.", "4": "Please say your PNR number.",
        "0": "I can help with weather, news, bookings and train status.",
    }.get(digits, "Sorry, I didn't understand that option.")
    return Response(
        content=f'<?xml version="1.0"?><Response><Say voice="Polly.Joanna">{html.escape(say)}</Say></Response>',
        media_type="application/xml",
    )


@router.post("/recording/consent")
async def recording_consent(request: Request):
    form     = await request.form()
    digits   = form.get("Digits", "")
    body     = form.get("SpeechResult", "").lower()
    call_sid = form.get("CallSid", "")
    consented = digits == "1" or "yes" in body or "haan" in body
    if consented and call_sid:
        from app.telecom.twilio_handler import start_recording_sync
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(loop.run_in_executor(None, start_recording_sync, call_sid))
    say = "Thank you. The call will now be recorded." if consented else "No problem. Not recording."
    return Response(
        content=f'<?xml version="1.0"?><Response><Say voice="Polly.Joanna">{html.escape(say)}</Say></Response>',
        media_type="application/xml",
    )


@router.post("/survey/response")
async def survey_response(request: Request):
    form    = await request.form()
    digits  = form.get("Digits", "")
    caller  = form.get("To", "")
    helpful = digits == "1"
    log.info("survey_response", caller=caller, helpful=helpful)
    if db_pool and caller:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE calls SET satisfied=$1 WHERE caller=$2 "
                    "AND start_time >= NOW() - INTERVAL '30 minutes' "
                    "ORDER BY start_time DESC LIMIT 1",
                    helpful, caller,
                )
        except Exception:
            pass
    msg = "Great, we're glad it helped!" if helpful else "Thanks for the feedback!"
    return Response(
        content=f'<?xml version="1.0"?><Response><Say voice="Polly.Joanna">{html.escape(msg)}</Say></Response>',
        media_type="application/xml",
    )


# -- Admin / data endpoints ---------------------------------------------------

@router.get("/health")
async def health():
    from app.services.calendar_service import gcal_service
    return {
        "status":          "ok",
        "db_connected":    db_pool is not None,
        "gcal_configured": gcal_service is not None,
        "sms_configured":  bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN),
    }


@router.get("/calls/recent")
async def recent_calls(limit: int = 20):
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        return {"calls": await fetch_recent_calls(limit), "count": limit}
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/admin/flagged")
async def flagged_calls():
    if db_pool is None:
        return {"error": "Database not configured"}
    return {"flagged": await fetch_flagged_calls()}


@router.get("/callbacks/list")
async def list_callbacks():
    return {"callbacks": _pending_callbacks}


@router.get("/cache/flush")
async def flush_cache_endpoint():
    from app.utils.helpers import cache_flush
    count = cache_flush()
    log.info("cache_flushed", entries_removed=count)
    return {"status": "flushed", "entries_removed": count}


# -- Analytics ----------------------------------------------------------------

@router.get("/analytics/emotions")
async def analytics_emotions_endpoint(days: int = 7):
    if db_pool is None:
        return {"error": "Database not configured"}
    return {"period_days": days, "emotions": await analytics_emotions(days)}


@router.get("/analytics/topics")
async def analytics_topics_endpoint(days: int = 7):
    if db_pool is None:
        return {"error": "Database not configured"}
    return {"period_days": days, "topics": await analytics_topics(days)}


@router.get("/analytics/languages")
async def analytics_languages_endpoint(days: int = 7):
    if db_pool is None:
        return {"error": "Database not configured"}
    return {"period_days": days, "languages": await analytics_languages(days)}


# -- Reminders ----------------------------------------------------------------

@router.post("/reminders/medicine")
async def register_medicine_reminder(request: Request):
    body        = await request.json()
    caller      = body.get("caller", "")
    medicine    = body.get("medicine", "")
    remind_time = body.get("remind_time", "")
    language    = body.get("language", "hi")
    if not all([caller, medicine, remind_time]):
        raise HTTPException(status_code=400, detail="caller, medicine, remind_time required")
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO medicine_reminders(caller, medicine, remind_time, language) VALUES($1,$2,$3,$4)",
                caller, medicine, remind_time, language,
            )
    log.info("medicine_reminder_registered", caller=caller, medicine=medicine, time=remind_time)
    return {"status": "registered", "caller": caller, "medicine": medicine, "remind_time": remind_time}


# -- Batch transcription (test) -----------------------------------------------

@router.post("/transcribe")
async def transcribe_endpoint(request: Request):
    form       = await request.form()
    lang       = form.get("lang", "en")
    audio_file = form.get("audio")
    if not audio_file:
        raise HTTPException(status_code=400, detail="audio field required")
    audio_bytes = await audio_file.read()
    transcript  = await transcribe_batch(audio_bytes, lang=lang)
    return {"transcript": transcript, "lang": lang}


# -- Voice cloning ------------------------------------------------------------

@router.post("/voice/clone")
async def clone_voice(request: Request):
    from app.core.config import ELEVENLABS_API_KEY
    import httpx
    content_type = request.headers.get("content-type", "")
    if "multipart" in content_type:
        form       = await request.form()
        caller     = form.get("caller", "")
        voice_name = form.get("voice_name", f"VoiceAI-{caller[-4:]}" if caller else "VoiceAI")
        audio_file = form.get("audio_file")
        if not caller or not audio_file:
            raise HTTPException(status_code=400, detail="caller and audio_file required")
        if not ELEVENLABS_API_KEY:
            raise HTTPException(status_code=503, detail="ElevenLabs not configured")
        audio_bytes = await audio_file.read()
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    "https://api.elevenlabs.io/v1/voices/add",
                    headers={"xi-api-key": ELEVENLABS_API_KEY},
                    files={"files": (audio_file.filename, audio_bytes, "audio/wav")},
                    data={"name": voice_name, "description": f"VoiceAI clone for {caller}"},
                )
                resp.raise_for_status()
                voice_id = resp.json().get("voice_id", "")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"ElevenLabs upload failed: {exc.response.status_code}")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"ElevenLabs error: {exc}")
        if not voice_id:
            raise HTTPException(status_code=502, detail="ElevenLabs did not return a voice_id")
    else:
        body     = await request.json()
        caller   = body.get("caller", "")
        voice_id = body.get("voice_id", "")
        if not caller or not voice_id:
            raise HTTPException(status_code=400, detail="caller and voice_id required")

    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO caller_profiles (caller, cloned_voice_id, last_seen) VALUES ($1,$2,NOW())
                   ON CONFLICT (caller) DO UPDATE SET cloned_voice_id=$2, last_seen=NOW()""",
                caller, voice_id,
            )
    log.info("voice_cloned", caller=caller, voice_id=voice_id)
    return {"status": "ok", "caller": caller, "voice_id": voice_id}


@router.post("/family/contacts")
async def update_family_contacts(request: Request):
    body     = await request.json()
    caller   = body.get("caller", "")
    contacts = body.get("contacts", [])
    if not caller:
        raise HTTPException(status_code=400, detail="caller required")
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO caller_profiles(caller, family_contacts, last_seen) VALUES($1,$2::jsonb,NOW())
                   ON CONFLICT (caller) DO UPDATE SET family_contacts=$2::jsonb, last_seen=NOW()""",
                caller, json.dumps(contacts),
            )
    return {"status": "ok", "caller": caller, "contacts_count": len(contacts)}
