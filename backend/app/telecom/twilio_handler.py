"""
app/telecom/twilio_handler.py
Twilio REST helpers: SMS, WhatsApp, Polly fallback TTS, outbound calls.
"""
from __future__ import annotations
import asyncio
import html
import json
from typing import Optional

from app.core.config import (
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
    EMERGENCY_CONTACT_NUMBER, PUBLIC_HOST,
)
from app.core.logging import log


def _get_client():
    from twilio.rest import Client
    return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


# -- SMS / WhatsApp -----------------------------------------------------------

def send_sms(to_number: str, message: str) -> None:
    """Synchronous — call via run_in_executor from async contexts."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        log.warning("sms_not_configured")
        return
    if not to_number:
        log.warning("sms_skipped", reason="no caller number")
        return
    try:
        msg = _get_client().messages.create(body=message, from_=TWILIO_PHONE_NUMBER, to=to_number)
        log.info("sms_sent", to=to_number, sid=msg.sid)
    except Exception as exc:
        log.error("sms_failed", to=to_number, error=str(exc))


def send_whatsapp_summary(to_number: str, summary: str) -> None:
    """Best-effort WhatsApp summary — caller may not have WhatsApp."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    if not to_number:
        return
    try:
        _get_client().messages.create(
            body=summary,
            from_=f"whatsapp:{TWILIO_PHONE_NUMBER}",
            to=f"whatsapp:{to_number}",
        )
        log.info("whatsapp_summary_sent", to=to_number)
    except Exception as exc:
        log.warning("whatsapp_summary_skipped", to=to_number, error=str(exc))


def build_call_summary(transcript: list[dict], topics: list[str],
                        caller_number: str, duration_secs: int) -> str:
    topics_str = ", ".join(topics)
    snippet    = " / ".join(t["content"] for t in transcript if t.get("role") == "user")[:200]
    return (
        f"📞 *VoiceAI Call Summary*\n"
        f"Number: {caller_number}\n"
        f"Duration: {duration_secs}s\n"
        f"Topics: {topics_str}\n"
        f"Your questions: {snippet}\n"
        f"_Powered by VoiceAI_"
    )


# -- Polly TTS fallback -------------------------------------------------------

def inject_polly_twiml(call_sid: str, text: str) -> None:
    """Inject a <Say> mid-call when ElevenLabs fails. Synchronous."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN]):
        return
    try:
        twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<Response><Say voice="Polly.Joanna">{html.escape(text)}</Say></Response>'
        )
        _get_client().calls(call_sid).update(twiml=twiml)
        log.info("polly_fallback_used", call_sid=call_sid)
    except Exception as exc:
        log.error("polly_fallback_failed", call_sid=call_sid, error=str(exc))


# -- Outbound calls -----------------------------------------------------------

def fire_outbound_call_sync(to: str, twiml: str) -> None:
    """Fire any outbound call synchronously. Use run_in_executor from async."""
    _get_client().calls.create(to=to, from_=TWILIO_PHONE_NUMBER, twiml=twiml)


def escalate_to_human_sync(caller: str) -> None:
    if not EMERGENCY_CONTACT_NUMBER:
        return
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Say voice="Polly.Joanna">Please hold. Connecting you to a human agent now.</Say>'
        f'<Dial><Number>{html.escape(EMERGENCY_CONTACT_NUMBER)}</Number></Dial>'
        '</Response>'
    )
    _get_client().calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("emergency_escalation", caller=caller, contact=EMERGENCY_CONTACT_NUMBER)


async def maybe_escalate_emergency(
    caller_number: str, escalation_score: int, emotion_arc: list, call_sid: str
) -> bool:
    if escalation_score < 5:
        return False
    recent = [r.emotion for r in emotion_arc[-3:]]
    if "urgent" not in recent:
        return False
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, EMERGENCY_CONTACT_NUMBER]):
        log.warning("emergency_escalation_skipped", reason="not configured")
        return False
    log.warning("emergency_escalation_triggered", caller=caller_number, score=escalation_score)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, escalate_to_human_sync, caller_number)
    return True


# -- Recording ----------------------------------------------------------------

def start_recording_sync(call_sid: str) -> Optional[str]:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN]):
        return None
    try:
        rec = _get_client().calls(call_sid).recordings.create()
        log.info("recording_started", call_sid=call_sid, recording_sid=rec.sid)
        return rec.sid
    except Exception as exc:
        log.error("recording_start_failed", call_sid=call_sid, error=str(exc))
        return None
