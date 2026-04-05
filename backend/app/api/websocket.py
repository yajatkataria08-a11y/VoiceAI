"""
app/api/websocket.py
Main WebSocket handler: bridges Twilio <-> Deepgram <-> Groq <-> TTS.
All 6 bug fixes applied. Session state is fully local to each connection.
"""
from __future__ import annotations
import asyncio
import base64
import json
import time as _time
from collections import deque
from datetime import datetime
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect

from app.core.config import (
    OTP_PIN_REQUIRED, IVR_CONFIDENCE_THRESHOLD,
    ABUSE_URGENCY_THRESHOLD, VOICE_CHALLENGE_ENABLED, ELDERLY_MODE_DEFAULT,
    SESSION_TIMEOUT_SECS, EMERGENCY_CONTACT_NUMBER,
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
    ELDERLY_SLOW_DOWN_PHRASES, PGVECTOR_ENABLED,
)
from app.core.logging import log
from app.database.queries import (
    load_caller_profile, save_caller_profile, log_call_to_db,
)
from app.security.rate_limit import PerToolRateLimiter
from app.security.otp import verify_pin
from app.security.abuse_detection import generate_voice_challenge, check_voice_challenge
from app.services.llm_service import (
    EmotionResult, detect_emotion, get_emotion_routing_hint,
    compute_escalation_score, dominant_emotion, get_voice_settings_for_emotion,
    maybe_compress_conversation, classify_call_topics, extract_entities,
    check_reply_confidence, ask_clarifying_question, get_llm_reply,
)
from app.services.tts_service import text_to_speech
from app.services.stt_service import make_deepgram_options
from app.telecom.audio_pipeline import AudioPipeline
from app.telecom.barge_in import handle_barge_in
from app.telecom.twilio_handler import (
    inject_polly_twiml, maybe_escalate_emergency,
    build_call_summary, send_whatsapp_summary,
)
from app.utils.language import detect_language, get_ab_bucket
from app.utils.text_cleaning import extract_pin
from deepgram import DeepgramClient, LiveTranscriptionEvents


async def media_stream(ws: WebSocket) -> None:
    """Main WebSocket handler. One instance per Twilio call."""
    await ws.accept()

    # -- Session state --------------------------------------------------------
    stream_sid:        Optional[str]          = None
    call_sid:          Optional[str]          = None
    caller_number:     str                    = ""
    conversation:      list[dict]             = []
    transcript_buffer: list[str]             = []
    transcript_log:    list[dict]             = []
    silence_task:      Optional[asyncio.Task] = None
    tts_task:          Optional[asyncio.Task] = None
    is_speaking:       bool                   = False
    session_lang:      str                    = "en"
    lang_detected:     bool                   = False
    start_time:        datetime               = datetime.utcnow()
    last_activity:     float                  = _time.monotonic()
    caller_context:    dict                   = {}
    caller_profile:    dict                   = {}
    ab_bucket:         str                    = ""
    auth_state:        str                    = "unauthenticated"
    urgency_count:     int                    = 0
    current_emotion:   EmotionResult          = EmotionResult("neutral", 0.5, "low")
    emotion_arc:       list[EmotionResult]    = []
    _challenge_active:   bool = False
    _challenge_expected: str  = ""
    _challenge_attempts: int  = 0
    _CHALLENGE_MAX      = 3
    tool_limiter       = PerToolRateLimiter()
    ivr_active:        bool = False
    _ivr_pending:      dict[str, str] = {}   # stream_sid → synthetic utterance

    session_log = log.bind(stream_sid="pending", caller="unknown")
    session_log.info("ws_connected")

    loop     = asyncio.get_running_loop()
    pipeline = AudioPipeline(None)   # callback wired below

    # -- Inner functions ------------------------------------------------------

    async def start_deepgram(lang: str) -> None:
        nonlocal pipeline
        pipeline._cb = on_transcript
        await pipeline.start(lang)

    async def on_transcript(self, result, **kwargs) -> None:   # noqa: N805
        nonlocal silence_task, lang_detected, session_lang, last_activity, ivr_active

        if not result.is_final:
            return
        alt      = result.channel.alternatives[0]
        sentence = alt.transcript
        if not sentence:
            return

        last_activity = _time.monotonic()
        confidence    = getattr(alt, "confidence", 1.0) or 1.0

        # IVR fallback
        if confidence < IVR_CONFIDENCE_THRESHOLD and not ivr_active and stream_sid and call_sid:
            ivr_active = True
            session_log.info("ivr_fallback_triggered", confidence=confidence)
            asyncio.ensure_future(_serve_ivr_menu())
            return

        if stream_sid and _ivr_pending.get(stream_sid):
            sentence   = _ivr_pending.pop(stream_sid)
            ivr_active = False

        if not lang_detected:
            lang_detected = True
            detected = detect_language(sentence)
            if detected != session_lang:
                session_log.info("language_switched", to=detected)
                session_lang = detected
                await pipeline.switch_language(session_lang)

        transcript_buffer.append(sentence)
        session_log.info("stt_transcript", text=sentence, confidence=round(confidence, 2))

        if silence_task and not silence_task.done():
            silence_task.cancel()
        silence_task = asyncio.ensure_future(handle_silence())

    async def _serve_ivr_menu() -> None:
        if not (call_sid and TWILIO_AUTH_TOKEN):
            return
        menu_twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Response><Gather numDigits="1" action="/ivr-input" method="POST">'
            '<Say voice="Polly.Joanna">'
            "Sorry, I didn't catch that. "
            "Press 1 for weather, 2 for news, 3 to book, 4 for PNR, 0 for help."
            '</Say></Gather></Response>'
        )
        from twilio.rest import Client
        await loop.run_in_executor(
            None,
            lambda: Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN).calls(call_sid).update(twiml=menu_twiml),
        )

    async def handle_silence() -> None:
        nonlocal is_speaking, tts_task, conversation, auth_state
        nonlocal current_emotion, emotion_arc, caller_context, urgency_count
        nonlocal ab_bucket, _challenge_active, _challenge_expected, _challenge_attempts

        await asyncio.sleep(0.8)
        if not transcript_buffer:
            return

        user_text = " ".join(transcript_buffer)
        transcript_buffer.clear()
        session_log.info("user_utterance", text=user_text)

        # BUG FIX 4: cancel TTS + clear audio on barge-in
        await handle_barge_in(ws, stream_sid, tts_task, is_speaking)

        # Voice challenge gate
        if _challenge_active:
            if check_voice_challenge(user_text, _challenge_expected):
                _challenge_active = False; _challenge_expected = ""; _challenge_attempts = 0
                session_log.info("voice_challenge_passed")
                tts_task = asyncio.ensure_future(send_audio("Great, that's correct! How can I help?"))
            else:
                _challenge_attempts += 1
                session_log.warning("voice_challenge_failed", attempt=_challenge_attempts)
                if _challenge_attempts >= _CHALLENGE_MAX:
                    _challenge_active = False; _challenge_expected = ""; _challenge_attempts = 0
                    session_log.warning("voice_challenge_dismissed", reason="max_attempts")
                    tts_task = asyncio.ensure_future(send_audio("No worries, let me help you."))
                else:
                    q, _challenge_expected = await generate_voice_challenge()
                    tts_task = asyncio.ensure_future(send_audio(f"That doesn't seem right. Try: {q}"))
            return

        # PIN auth gate
        if OTP_PIN_REQUIRED and auth_state != "authenticated":
            if auth_state == "unauthenticated":
                from app.database.queries import get_or_create_pin
                pin = await get_or_create_pin(caller_number)
                await loop.run_in_executor(
                    None, lambda: __import__("app.telecom.twilio_handler", fromlist=["send_sms"]).send_sms(
                        caller_number, f"Your VoiceAI PIN is: {pin}"
                    )
                )
                auth_state = "verifying"
                tts_task = asyncio.ensure_future(send_audio(
                    "I've sent a 4-digit PIN to your number. Please say it now."
                ))
            elif auth_state == "verifying":
                entered = extract_pin(user_text)
                if entered and await verify_pin(caller_number, entered):
                    auth_state = "authenticated"
                    session_log.info("caller_authenticated")
                    tts_task = asyncio.ensure_future(send_audio("Identity verified. How can I help?"))
                else:
                    tts_task = asyncio.ensure_future(send_audio("PIN didn't match. Please try again."))
            return

        # Emotion + conversation compression (parallel)
        emotion_result, conversation = await asyncio.gather(
            detect_emotion(user_text),
            maybe_compress_conversation(conversation),
        )
        current_emotion = emotion_result
        emotion_arc.append(emotion_result)
        session_log.info("emotion_detected", **emotion_result.to_dict())

        # Abuse / urgency detection
        if emotion_result.emotion == "urgent":
            urgency_count += 1
            if urgency_count == ABUSE_URGENCY_THRESHOLD:
                alert = EMERGENCY_CONTACT_NUMBER or TWILIO_PHONE_NUMBER
                if alert:
                    msg = f"VoiceAI ALERT: Caller {caller_number}, urgency x{urgency_count}, stream {stream_sid}"
                    from app.telecom.twilio_handler import send_sms
                    asyncio.ensure_future(loop.run_in_executor(None, send_sms, alert, msg))
                if VOICE_CHALLENGE_ENABLED:
                    q, _challenge_expected = await generate_voice_challenge()
                    _challenge_active = True
                    tts_task = asyncio.ensure_future(send_audio("Before we continue, please answer: " + q))
                    return

        # Escalation hint (frustrated/angry streak OR emotion routing)
        escalation_score = compute_escalation_score(emotion_arc)
        escalation_hint  = (
            "Caller has been frustrated/angry. Proactively offer a human agent or callback."
            if escalation_score >= 3 else ""
        )
        if escalation_hint:
            session_log.info("escalation_triggered", score=escalation_score)
        routing_hint = get_emotion_routing_hint(emotion_result.emotion, session_lang)
        if routing_hint and not escalation_hint:
            escalation_hint = routing_hint
            session_log.info("emotion_routing_applied", emotion=emotion_result.emotion)

        # Emergency auto-dial
        if call_sid and caller_number:
            escalated = await maybe_escalate_emergency(caller_number, escalation_score, emotion_arc, call_sid)
            if escalated:
                tts_task = asyncio.ensure_future(send_audio("Connecting you to a human agent now. Please hold."))
                return

        # Entity extraction + long-term memory
        caller_context = await extract_entities(user_text, caller_context)
        if PGVECTOR_ENABLED and caller_number:
            from app.database.queries import db_pool
            if db_pool:
                try:
                    # retrieve_memories imported lazily to avoid hard dep on pgvector
                    from app.database import memory   # type: ignore
                    memories = await memory.retrieve_memories(caller_number, user_text)
                    if memories:
                        caller_context["long_term_memories"] = "; ".join(memories)
                except Exception:
                    pass

        # Detect elderly mode from speech
        if ELDERLY_SLOW_DOWN_PHRASES.search(user_text):
            caller_profile["elderly_mode"] = True
            session_log.info("elderly_mode_activated")
        elderly_active = ELDERLY_MODE_DEFAULT or bool(caller_profile.get("elderly_mode"))

        conversation.append({"role": "user", "content": user_text})
        transcript_log.append({"role": "user", "content": user_text, **emotion_result.to_dict()})

        reply = await get_llm_reply(
            conversation,
            lang=session_lang,
            caller_number=caller_number,
            emotion_result=current_emotion,
            escalation_hint=escalation_hint,
            context=caller_context,
            caller_profile=caller_profile,
            elderly_mode=elderly_active,
            tool_limiter=tool_limiter,
        )

        # Intent confidence check
        conf = await check_reply_confidence(user_text, reply)
        if conf < 0.5:
            reply = await ask_clarifying_question(user_text, session_lang)
            session_log.info("low_confidence_clarify", confidence=conf)

        conversation.append({"role": "assistant", "content": reply})
        transcript_log.append({"role": "assistant", "content": reply})
        session_log.info("ai_reply", text=reply)
        tts_task = asyncio.ensure_future(send_audio(reply))

    async def send_audio(reply: str) -> None:
        nonlocal is_speaking
        is_speaking = True
        try:
            elderly_active = ELDERLY_MODE_DEFAULT or bool(caller_profile.get("elderly_mode"))
            audio_bytes = await text_to_speech(
                reply, lang=session_lang,
                emotion=current_emotion.emotion,
                caller_number=caller_number,
                caller_profile=caller_profile,
                slow_mode=elderly_active,
            )
            audio_b64 = base64.b64encode(audio_bytes).decode()
            if stream_sid:
                await ws.send_json({
                    "event": "media", "streamSid": stream_sid,
                    "media": {"payload": audio_b64},
                })
        except asyncio.CancelledError:
            session_log.info("tts_cancelled", reason="barge_in")
        except Exception as exc:
            session_log.error("tts_failed", error=str(exc))
            if call_sid:
                await loop.run_in_executor(None, inject_polly_twiml, call_sid, reply)
        finally:
            is_speaking = False

    # Inactivity watchdog
    async def inactivity_watchdog() -> None:
        while True:
            await asyncio.sleep(10)
            if _time.monotonic() - last_activity > SESSION_TIMEOUT_SECS:
                session_log.info("session_timeout")
                if stream_sid:
                    asyncio.ensure_future(send_audio(
                        "I haven't heard from you for a while. Goodbye! Feel free to call back anytime."
                    ))
                await asyncio.sleep(4)
                await ws.close()
                return

    await start_deepgram(session_lang)
    watchdog = asyncio.ensure_future(inactivity_watchdog())

    # -- Main receive loop ----------------------------------------------------
    try:
        async for raw in ws.iter_text():
            msg   = json.loads(raw)
            event = msg.get("event")

            if event == "start":
                stream_sid    = msg["start"]["streamSid"]
                call_sid      = msg["start"].get("callSid", "")
                custom        = msg["start"].get("customParameters", {})
                caller_number = custom.get("callerNumber", "")
                start_time    = datetime.utcnow()
                ab_bucket     = get_ab_bucket(caller_number)
                session_log   = log.bind(stream_sid=stream_sid, caller=caller_number, call_sid=call_sid)
                session_log.info("stream_started", ab_bucket=ab_bucket)
                caller_profile = await load_caller_profile(caller_number)
                await pipeline.flush_buffer()

            elif event == "media":
                audio = base64.b64decode(msg["media"]["payload"])
                await pipeline.send(audio, stream_sid_known=stream_sid is not None)

            elif event == "stop":
                session_log.info("stream_stopped")
                break

    except WebSocketDisconnect:
        session_log.info("ws_disconnected")
    finally:
        watchdog.cancel()
        await pipeline.finish()
        if silence_task and not silence_task.done():
            silence_task.cancel()
        if tts_task and not tts_task.done():
            tts_task.cancel()

        end_time      = datetime.utcnow()
        duration_secs = int((end_time - start_time).total_seconds())
        call_flagged  = urgency_count >= ABUSE_URGENCY_THRESHOLD
        topics        = await classify_call_topics(transcript_log)
        dom_emotion   = dominant_emotion(emotion_arc)
        emotions_json = [{"turn": i, **r.to_dict()} for i, r in enumerate(emotion_arc)]

        await log_call_to_db(
            stream_sid=stream_sid or "unknown",
            caller=caller_number,
            start_time=start_time,
            end_time=end_time,
            transcript=transcript_log,
            language=session_lang,
            emotions_json=emotions_json,
            dominant_emotion=dom_emotion,
            topics=topics,
            flagged=call_flagged,
            ab_bucket=ab_bucket,
        )

        if caller_number:
            summary = build_call_summary(transcript_log, topics, caller_number, duration_secs)
            await loop.run_in_executor(None, send_whatsapp_summary, caller_number, summary)
            await save_caller_profile(caller_number, {
                "language":   session_lang,
                "last_topic": topics[0] if topics else None,
                "city":       caller_context.get("city"),
                "name":       caller_context.get("person_name"),
                "elderly_mode": bool(caller_profile.get("elderly_mode")),
                "cloned_voice_id": caller_profile.get("cloned_voice_id"),
            })
            # Long-term memory extraction (fire and forget)
            if PGVECTOR_ENABLED:
                try:
                    from app.database import memory  # type: ignore
                    asyncio.ensure_future(memory.extract_and_store_call_memories(caller_number, transcript_log))
                except Exception:
                    pass

        session_log.info(
            "session_cleaned_up",
            duration_secs=duration_secs, turns=len(transcript_log),
            dominant_emotion=dom_emotion, topics=topics, flagged=call_flagged,
        )
