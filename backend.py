"""
VoiceAI Backend — Real-Time Telecom AI
Stack: Twilio + Deepgram STT + Groq LLM + ElevenLabs TTS
All over WebSockets for ultra-low latency.
"""

import asyncio
import base64
import json
import os
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import Response
from deepgram import DeepgramClient, LiveTranscriptionEvents, LiveOptions
from groq import AsyncGroq
import websockets

# ── ENV ─────────────────────────────────────────────────────────────────────
DEEPGRAM_API_KEY   = os.environ["DEEPGRAM_API_KEY"]
GROQ_API_KEY       = os.environ["GROQ_API_KEY"]
ELEVENLABS_API_KEY = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_VOICE   = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")  # Rachel

SYSTEM_PROMPT = """You are a helpful voice assistant reachable by phone.
You help elderly users, visually impaired users, and busy workers (truck drivers, 
construction workers) who cannot safely use a screen.
Keep every reply SHORT (1-3 sentences). Speak naturally. Never ask multiple questions at once.
You can help with: news headlines, weather, directions, booking reminders, and general Q&A."""

# ── APP ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="VoiceAI Telecom Backend")

groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# ── TWILIO WEBHOOK — returns TwiML to connect the call to our WS ─────────────
@app.post("/incoming-call")
async def incoming_call(request: Request):
    """
    Twilio calls this endpoint when someone dials the phone number.
    We respond with TwiML that upgrades the call to a WebSocket stream.
    """
    host = request.headers.get("host")
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Hello! I'm your voice assistant. How can I help you today?</Say>
  <Connect>
    <Stream url="wss://{host}/media-stream" />
  </Connect>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


# ── ELEVENLABS TTS ────────────────────────────────────────────────────────────
async def text_to_speech_stream(text: str) -> bytes:
    """Convert text to μ-law 8kHz audio bytes via ElevenLabs (Twilio format)."""
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE}/stream"
    headers = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"}
    payload = {
        "text": text,
        "model_id": "eleven_turbo_v2",           # fastest model
        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
        "output_format": "ulaw_8000",             # native Twilio format — no re-encoding needed
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.content


# ── GROQ LLM ──────────────────────────────────────────────────────────────────
async def get_llm_reply(conversation: list[dict]) -> str:
    """Send conversation history to Groq and return the assistant reply."""
    response = await groq_client.chat.completions.create(
        # Fix 5: updated to current Groq model ID — llama3-8b-8192 may 404
        model="llama-3.1-8b-instant",
        messages=[{"role": "system", "content": SYSTEM_PROMPT}] + conversation,
        max_tokens=120,
        temperature=0.6,
    )
    return response.choices[0].message.content.strip()


# ── MAIN WEBSOCKET — bridges Twilio ↔ Deepgram ↔ Groq ↔ ElevenLabs ──────────
@app.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    """
    Twilio sends us μ-law audio frames over this WebSocket.
    We pipe them to Deepgram for streaming STT, feed transcripts to Groq,
    then stream TTS audio back to Twilio.
    """
    await ws.accept()
    print("[WS] Twilio connected")

    stream_sid: Optional[str] = None
    conversation: list[dict] = []
    transcript_buffer: list[str] = []
    silence_task: Optional[asyncio.Task] = None
    tts_task: Optional[asyncio.Task] = None   # Fix 4: track in-flight TTS task for cancellation
    is_speaking: bool = False                 # tracks whether AI is currently sending audio

    # Fix 1: capture the running loop once here, in the correct async context.
    # asyncio.get_event_loop() inside Deepgram's callback thread is deprecated
    # in Python 3.10+ and raises DeprecationWarning or breaks entirely.
    loop = asyncio.get_running_loop()

    # ── Deepgram live connection ─────────────────────────────────────────────
    dg_client = DeepgramClient(DEEPGRAM_API_KEY)
    dg_connection = dg_client.listen.asynclive.v("1")

    # NOTE: `self` here is a Deepgram SDK artifact — it is NOT the class instance.
    # Deepgram passes it positionally; just leave it in the signature.
    async def on_transcript(self, result, **kwargs):
        """Called by Deepgram whenever a transcript segment is ready."""
        nonlocal silence_task

        # Ignore partial/interim results — only act on final transcripts.
        # Without this, partial chunks like "what's the wea" AND "what's the weather"
        # both land in transcript_buffer, corrupting the final utterance sent to Groq.
        if not result.is_final:
            return

        sentence = result.channel.alternatives[0].transcript
        if not sentence:
            return

        transcript_buffer.append(sentence)
        print(f"[STT] {sentence}")

        # Reset silence timer — user is still speaking
        if silence_task and not silence_task.done():
            silence_task.cancel()

        # Fix 1: use the pre-captured loop with call_soon_threadsafe + ensure_future.
        # Deepgram fires this callback from its own internal thread; asyncio.create_task()
        # requires the event loop thread, so we schedule via call_soon_threadsafe.
        silence_task = loop.call_soon_threadsafe(
            asyncio.ensure_future, handle_silence()
        )

    async def handle_silence():
        """Wait 800ms of silence, then treat buffered transcript as complete utterance."""
        nonlocal is_speaking, tts_task, conversation
        await asyncio.sleep(0.8)
        if not transcript_buffer:
            return
        user_text = " ".join(transcript_buffer)
        transcript_buffer.clear()
        print(f"[USER] {user_text}")

        # Fix 4: if AI was mid-speech, cancel the in-flight TTS task AND send
        # Twilio a clear event — previously only the clear was sent, so ElevenLabs
        # would still be called and the audio would arrive after the interrupt.
        if tts_task and not tts_task.done():
            tts_task.cancel()
        if is_speaking:
            await ws.send_json({"event": "clear", "streamSid": stream_sid})

        # Fix 2: trim conversation in-place to prevent unbounded memory growth.
        # Previously only the Groq call used a sliced window, but the full list
        # kept growing — on a 30-min call this becomes a real memory leak.
        conversation.append({"role": "user", "content": user_text})
        if len(conversation) > 20:
            conversation = conversation[-20:]

        reply = await get_llm_reply(conversation)
        conversation.append({"role": "assistant", "content": reply})
        print(f"[AI] {reply}")

        # Run TTS + send as a separate task so the main WS receive loop
        # is never blocked waiting for ElevenLabs (~300–800ms).
        # Fix 4: store the task reference so it can be cancelled on barge-in.
        tts_task = asyncio.ensure_future(send_audio(reply))

    async def send_audio(reply: str):
        """Fetch TTS audio and stream it back to Twilio — runs as its own task."""
        nonlocal is_speaking
        is_speaking = True
        try:
            audio_bytes = await text_to_speech_stream(reply)
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            await ws.send_json({
                "event": "media",
                "streamSid": stream_sid,
                "media": {"payload": audio_b64},
            })
        except asyncio.CancelledError:
            # Barge-in cancellation — this is expected, not an error
            print("[TTS] Cancelled due to barge-in")
        except Exception as e:
            # Fix 3: previously any ElevenLabs/network error crashed silently,
            # leaving the call dead. Now we log and let the caller keep going.
            print(f"[TTS ERROR] {e}")
        finally:
            is_speaking = False

    dg_connection.on(LiveTranscriptionEvents.Transcript, on_transcript)

    dg_options = LiveOptions(
        model="nova-2",
        language="en-US",
        encoding="mulaw",
        sample_rate=8000,
        channels=1,
        interim_results=True,
        endpointing=300,       # ms — detect end of speech
        smart_format=True,
    )
    await dg_connection.start(dg_options)

    # ── Main receive loop ────────────────────────────────────────────────────
    try:
        async for raw in ws.iter_text():
            msg = json.loads(raw)
            event = msg.get("event")

            if event == "start":
                stream_sid = msg["start"]["streamSid"]
                print(f"[WS] Stream started: {stream_sid}")

            # Fix 6: guard against media events arriving before the start event.
            # If stream_sid is None here, send_audio would emit malformed JSON to
            # Twilio with a null streamSid — now we simply skip the frame.
            elif event == "media" and stream_sid:
                # Forward Twilio's μ-law audio chunk to Deepgram
                audio = base64.b64decode(msg["media"]["payload"])
                await dg_connection.send(audio)

            elif event == "stop":
                print("[WS] Stream stopped by Twilio")
                break

    except WebSocketDisconnect:
        print("[WS] Twilio disconnected")
    finally:
        await dg_connection.finish()
        if silence_task and not silence_task.done():
            silence_task.cancel()
        if tts_task and not tts_task.done():   # Fix 4: also cancel any pending TTS on teardown
            tts_task.cancel()
        print("[WS] Cleaned up")


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}