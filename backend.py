"""
VoiceAI Backend — Real-Time Telecom AI
Stack: Twilio + Deepgram STT + Groq LLM + ElevenLabs TTS
All over WebSockets for ultra-low latency.

UPGRADE 1: Multilingual support (English + Hindi)
  - Detects caller language from first utterance using langdetect
  - Switches Deepgram language param and ElevenLabs voice per session
  - Passes detected language into LLM system prompt

UPGRADE 2: Real-time weather via OpenWeatherMap + Groq tool calling
  - get_weather(city) fetches current conditions from OpenWeatherMap
  - Groq tool/function calling triggers it when user asks about weather
  - Two-pass Groq call: first pass detects tool intent, second pass
    produces the final natural-language reply with weather data injected
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

# ── UPGRADE 1: langdetect ────────────────────────────────────────────────────
from langdetect import detect, LangDetectException

# ── ENV ─────────────────────────────────────────────────────────────────────
DEEPGRAM_API_KEY        = os.environ["DEEPGRAM_API_KEY"]
GROQ_API_KEY            = os.environ["GROQ_API_KEY"]
ELEVENLABS_API_KEY      = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_VOICE_EN     = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")  # Rachel (English)
ELEVENLABS_VOICE_HI     = os.environ.get("ELEVENLABS_HINDI_VOICE_ID", "pNInz6obpgDQGcFmaJgB")  # Adam (fallback)
# UPGRADE 2: OpenWeatherMap API key
OPENWEATHERMAP_API_KEY  = os.environ.get("OPENWEATHERMAP_API_KEY", "")

SYSTEM_PROMPT_EN = """You are a helpful voice assistant reachable by phone.
You help elderly users, visually impaired users, and busy workers (truck drivers, 
construction workers) who cannot safely use a screen.
Keep every reply SHORT (1-3 sentences). Speak naturally. Never ask multiple questions at once.
You can help with: news headlines, weather, directions, booking reminders, and general Q&A."""

SYSTEM_PROMPT_HI = """आप एक सहायक वॉइस असिस्टेंट हैं जो फोन पर उपलब्ध हैं।
आप बुजुर्ग उपयोगकर्ताओं, दृष्टिबाधित उपयोगकर्ताओं और व्यस्त श्रमिकों की मदद करते हैं।
हर जवाब छोटा रखें (1-3 वाक्य)। स्वाभाविक रूप से बोलें। एक बार में एक से अधिक प्रश्न न पूछें।
आप समाचार, मौसम, दिशा-निर्देश, बुकिंग और सामान्य प्रश्नों में मदद कर सकते हैं।
हमेशा हिंदी में उत्तर दें।"""


# ── UPGRADE 2: Groq tool schema ───────────────────────────────────────────────
# This list is passed to Groq's chat.completions.create() as `tools=`.
# Groq will emit a tool_call when the user asks about weather.
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": (
                "Get the current weather conditions for a given city. "
                "Call this whenever the user asks about weather, temperature, "
                "rain, humidity, or climate in any location."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": (
                            "The city name to fetch weather for, e.g. 'Mumbai', "
                            "'London', 'New York'. Include country code if ambiguous, "
                            "e.g. 'Springfield,US'."
                        ),
                    }
                },
                "required": ["city"],
            },
        },
    }
]


# ── UPGRADE 1 helpers ─────────────────────────────────────────────────────────
def get_system_prompt(lang: str) -> str:
    """Return the appropriate system prompt for the detected language."""
    return SYSTEM_PROMPT_HI if lang == "hi" else SYSTEM_PROMPT_EN


def get_elevenlabs_voice(lang: str) -> str:
    """Return the ElevenLabs voice ID for the detected language."""
    return ELEVENLABS_VOICE_HI if lang == "hi" else ELEVENLABS_VOICE_EN


def get_deepgram_language(lang: str) -> str:
    """Return Deepgram language code for the detected language."""
    return "hi" if lang == "hi" else "en-US"


def detect_language(text: str) -> str:
    """
    Detect language from text using langdetect.

    Returns "hi" for Hindi, "en" for English/others.
    Falls back to "en" on any detection error.
    """
    try:
        detected = detect(text)
        return "hi" if detected == "hi" else "en"
    except LangDetectException:
        return "en"


# ── APP ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="VoiceAI Telecom Backend")

groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# ── TWILIO WEBHOOK ────────────────────────────────────────────────────────────
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
async def text_to_speech_stream(text: str, lang: str = "en") -> bytes:
    """
    Convert text to μ-law 8kHz audio bytes via ElevenLabs (Twilio format).

    UPGRADE 1: Accepts `lang` to select the correct voice ID per session.
    """
    voice_id = get_elevenlabs_voice(lang)
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
    headers = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"}
    payload = {
        "text": text,
        "model_id": "eleven_turbo_v2",
        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
        "output_format": "ulaw_8000",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.content


# ── UPGRADE 2: OpenWeatherMap fetch ──────────────────────────────────────────
async def get_weather(city: str) -> str:
    """
    Fetch current weather for `city` from OpenWeatherMap and return a
    concise, voice-friendly string.

    Returns an error string (never raises) so the LLM can relay it gracefully.

    Example return:
        "Mumbai: 32°C, feels like 36°C, humidity 78%, scattered clouds."
    """
    if not OPENWEATHERMAP_API_KEY:
        return "Weather service is not configured. Please set OPENWEATHERMAP_API_KEY."

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": OPENWEATHERMAP_API_KEY,
        "units": "metric",   # Celsius; change to "imperial" for Fahrenheit
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return f"Sorry, I couldn't find weather data for {city}."
            resp.raise_for_status()
            data = resp.json()

        temp        = round(data["main"]["temp"])
        feels_like  = round(data["main"]["feels_like"])
        humidity    = data["main"]["humidity"]
        description = data["weather"][0]["description"]
        city_name   = data["name"]
        country     = data["sys"]["country"]

        return (
            f"{city_name}, {country}: {temp}°C, feels like {feels_like}°C, "
            f"humidity {humidity}%, {description}."
        )
    except httpx.HTTPStatusError as e:
        return f"Weather service returned an error: {e.response.status_code}."
    except Exception as e:
        print(f"[WEATHER ERROR] {e}")
        return "Sorry, I couldn't fetch the weather right now. Please try again."


# ── UPGRADE 2: tool dispatcher ────────────────────────────────────────────────
async def dispatch_tool_call(tool_name: str, tool_args: dict) -> str:
    """
    Route a Groq tool_call to the appropriate function and return its result
    as a string. Add new tools here as they are introduced in later upgrades.
    """
    if tool_name == "get_weather":
        city = tool_args.get("city", "")
        return await get_weather(city)
    # (future tools: get_news, book_appointment, etc.)
    return f"Unknown tool: {tool_name}"


# ── GROQ LLM ──────────────────────────────────────────────────────────────────
async def get_llm_reply(conversation: list[dict], lang: str = "en") -> str:
    """
    Send conversation history to Groq and return the assistant reply.

    UPGRADE 1: Uses language-aware system prompt.
    UPGRADE 2: Passes GROQ_TOOLS to Groq. If Groq emits a tool_call,
    we execute the tool, inject the result as a `tool` message, and make
    a second Groq call to produce the final natural-language reply.
    The caller always receives a plain string — tool calling is transparent.
    """
    system_prompt = get_system_prompt(lang)
    messages = [{"role": "system", "content": system_prompt}] + conversation

    # ── First Groq pass — may return a tool_call ──────────────────────────
    response = await groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        tools=GROQ_TOOLS,
        tool_choice="auto",   # let Groq decide when to call a tool
        max_tokens=120,
        temperature=0.6,
    )

    choice = response.choices[0]

    # ── No tool call — return the reply directly ──────────────────────────
    if choice.finish_reason != "tool_calls":
        return choice.message.content.strip()

    # ── Tool call detected — execute and make a second Groq pass ─────────
    tool_calls = choice.message.tool_calls  # list of tool call objects

    # Append the assistant's tool-call turn to the message thread
    messages.append({
        "role": "assistant",
        "content": choice.message.content or "",   # may be None
        "tool_calls": [
            {
                "id":       tc.id,
                "type":     "function",
                "function": {
                    "name":      tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in tool_calls
        ],
    })

    # Execute every tool call (typically just one for voice use-cases)
    for tc in tool_calls:
        tool_name = tc.function.name
        try:
            tool_args = json.loads(tc.function.arguments)
        except json.JSONDecodeError:
            tool_args = {}

        print(f"[TOOL] Calling {tool_name}({tool_args})")
        tool_result = await dispatch_tool_call(tool_name, tool_args)
        print(f"[TOOL] Result: {tool_result}")

        # Inject result as a `tool` role message — Groq requires the matching id
        messages.append({
            "role":         "tool",
            "tool_call_id": tc.id,
            "content":      tool_result,
        })

    # ── Second Groq pass — synthesise a natural reply from tool results ───
    second_response = await groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        max_tokens=120,
        temperature=0.6,
        # No tools= here — we want a plain text reply, not another tool call
    )
    return second_response.choices[0].message.content.strip()


# ── MAIN WEBSOCKET — bridges Twilio ↔ Deepgram ↔ Groq ↔ ElevenLabs ──────────
@app.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    """
    Twilio sends us μ-law audio frames over this WebSocket.
    We pipe them to Deepgram for streaming STT, feed transcripts to Groq,
    then stream TTS audio back to Twilio.

    UPGRADE 1: Per-session language detection and Deepgram/ElevenLabs switching.
    UPGRADE 2: get_llm_reply now handles Groq tool calling transparently;
               no changes needed in the WebSocket session logic itself.
    """
    await ws.accept()
    print("[WS] Twilio connected")

    stream_sid: Optional[str] = None
    conversation: list[dict] = []
    transcript_buffer: list[str] = []
    silence_task: Optional[asyncio.Task] = None
    tts_task: Optional[asyncio.Task] = None
    is_speaking: bool = False

    session_lang: str = "en"
    lang_detected: bool = False

    loop = asyncio.get_running_loop()

    dg_client = DeepgramClient(DEEPGRAM_API_KEY)
    dg_conn_holder: list = [None]

    async def start_deepgram(lang: str):
        """Start (or restart) Deepgram with the correct language option."""
        conn = dg_client.listen.asynclive.v("1")
        conn.on(LiveTranscriptionEvents.Transcript, on_transcript)
        options = LiveOptions(
            model="nova-2",
            language=get_deepgram_language(lang),
            encoding="mulaw",
            sample_rate=8000,
            channels=1,
            interim_results=True,
            endpointing=300,
            smart_format=True,
        )
        await conn.start(options)
        dg_conn_holder[0] = conn
        print(f"[DG] Started with language={get_deepgram_language(lang)}")

    async def on_transcript(self, result, **kwargs):
        """Called by Deepgram whenever a transcript segment is ready."""
        nonlocal silence_task, lang_detected, session_lang

        if not result.is_final:
            return

        sentence = result.channel.alternatives[0].transcript
        if not sentence:
            return

        if not lang_detected:
            lang_detected = True
            detected = detect_language(sentence)
            if detected != session_lang:
                print(f"[LANG] Detected '{detected}' — switching from '{session_lang}'")
                session_lang = detected
                old_conn = dg_conn_holder[0]
                if old_conn:
                    await old_conn.finish()
                await start_deepgram(session_lang)
            else:
                print(f"[LANG] Detected '{detected}' — keeping current session")

        transcript_buffer.append(sentence)
        print(f"[STT] {sentence}")

        if silence_task and not silence_task.done():
            silence_task.cancel()

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

        if tts_task and not tts_task.done():
            tts_task.cancel()
        if is_speaking:
            await ws.send_json({"event": "clear", "streamSid": stream_sid})

        conversation.append({"role": "user", "content": user_text})
        if len(conversation) > 20:
            conversation = conversation[-20:]

        reply = await get_llm_reply(conversation, lang=session_lang)
        conversation.append({"role": "assistant", "content": reply})
        print(f"[AI] ({session_lang}) {reply}")

        tts_task = asyncio.ensure_future(send_audio(reply))

    async def send_audio(reply: str):
        """Fetch TTS audio and stream it back to Twilio — runs as its own task."""
        nonlocal is_speaking
        is_speaking = True
        try:
            audio_bytes = await text_to_speech_stream(reply, lang=session_lang)
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            await ws.send_json({
                "event": "media",
                "streamSid": stream_sid,
                "media": {"payload": audio_b64},
            })
        except asyncio.CancelledError:
            print("[TTS] Cancelled due to barge-in")
        except Exception as e:
            print(f"[TTS ERROR] {e}")
        finally:
            is_speaking = False

    await start_deepgram(session_lang)

    try:
        async for raw in ws.iter_text():
            msg = json.loads(raw)
            event = msg.get("event")

            if event == "start":
                stream_sid = msg["start"]["streamSid"]
                print(f"[WS] Stream started: {stream_sid}")

            elif event == "media" and stream_sid:
                audio = base64.b64decode(msg["media"]["payload"])
                if dg_conn_holder[0]:
                    await dg_conn_holder[0].send(audio)

            elif event == "stop":
                print("[WS] Stream stopped by Twilio")
                break

    except WebSocketDisconnect:
        print("[WS] Twilio disconnected")
    finally:
        if dg_conn_holder[0]:
            await dg_conn_holder[0].finish()
        if silence_task and not silence_task.done():
            silence_task.cancel()
        if tts_task and not tts_task.done():
            tts_task.cancel()
        print("[WS] Cleaned up")


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}
