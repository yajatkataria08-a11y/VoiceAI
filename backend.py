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

UPGRADE 3: News headlines via NewsAPI
  - get_news(topic) fetches top 3 headlines from NewsAPI.org
  - Groq tool calling triggers it when user says "news", "headlines",
    or "what's happening"
  - Returns a short, voice-friendly "Top headlines: 1. ... 2. ... 3. ..."
  - HTML stripped from headline text before returning

UPGRADE 4: Google Calendar booking
  - book_appointment(title, date, time) creates a real Calendar event
  - Groq tool calling triggers it when user says "book", "schedule",
    "remind me", "set a meeting"
  - Uses a service account JSON for server-to-server auth (no OAuth)
  - Date/time parsed into RFC3339 with configurable timezone

UPGRADE 5: SMS Confirmation via Twilio
  - After a booking is made, automatically SMS the caller a confirmation
  - send_sms(to_number, message) sends via Twilio REST API
  - Caller number extracted from the Twilio /incoming-call POST param "From"
  - Passed into the media stream via TwiML customParameters

UPGRADE 6: Call logging to Supabase / Postgres
  - Logs every call session to a Postgres database via asyncpg
  - Stores: stream_sid, caller, start_time, end_time, transcript (JSONB),
    language, duration_seconds
  - Connection pool created at app startup via lifespan event

UPGRADE 7: Fallback TTS (Twilio Polly)
  - If ElevenLabs TTS fails, falls back to Twilio's built-in Polly TTS
  - Uses Twilio REST API to inject a mid-call TwiML <Say> verb
  - Zero-downtime fallback: caller hears Polly voice instead of silence

UPGRADE 8: Structured logging (replace print)
  - Replaces all print() statements with structlog
  - JSON-formatted, structured log lines with session context binding
  - Per-session context: stream_sid, caller, language bound to each log

UPGRADE 9: Rate limiting per phone number
  - Limits each phone number to max 5 active calls per hour
  - Rejects excess calls with a polite TwiML message
  - In-memory dict with pruning; swappable for Redis in production

UPGRADE 10: Conversation summarization
  - Instead of hard-trimming to last 20 turns, summarizes older turns
  - Uses Groq to compress old turns into a single "Earlier in this call" message
  - Preserves context across long calls without memory bloat
"""

# ── STANDARD LIBRARY ──────────────────────────────────────────────────────────
import asyncio
import base64
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import html
import json
import os
import re
from typing import Optional
from zoneinfo import ZoneInfo

# ── THIRD-PARTY ───────────────────────────────────────────────────────────────
import httpx
import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import Response
from deepgram import DeepgramClient, LiveTranscriptionEvents, LiveOptions
from groq import AsyncGroq
import websockets

# ── UPGRADE 1: langdetect ────────────────────────────────────────────────────
from langdetect import detect, LangDetectException

# ── UPGRADE 4: Google Calendar ───────────────────────────────────────────────
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── UPGRADE 5: Twilio REST (SMS + Fallback TTS) ───────────────────────────────
from twilio.rest import Client as TwilioClient

# ── UPGRADE 6: asyncpg (Postgres) ─────────────────────────────────────────────
import asyncpg

# ── UPGRADE 8: Configure structlog ───────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger()

# ── ENV ───────────────────────────────────────────────────────────────────────
DEEPGRAM_API_KEY        = os.environ["DEEPGRAM_API_KEY"]
GROQ_API_KEY            = os.environ["GROQ_API_KEY"]
ELEVENLABS_API_KEY      = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_VOICE_EN     = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")   # Rachel (English)
ELEVENLABS_VOICE_HI     = os.environ.get("ELEVENLABS_HINDI_VOICE_ID", "pNInz6obpgDQGcFmaJgB")  # Adam (fallback)

# UPGRADE 2: OpenWeatherMap API key
OPENWEATHERMAP_API_KEY  = os.environ.get("OPENWEATHERMAP_API_KEY", "")
# UPGRADE 3: NewsAPI key
NEWS_API_KEY            = os.environ.get("NEWS_API_KEY", "")
# UPGRADE 4: Google Calendar
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_CALENDAR_ID          = os.environ.get("GOOGLE_CALENDAR_ID", "primary")
TIMEZONE                    = os.environ.get("TIMEZONE", "Asia/Kolkata")
# UPGRADE 5: Twilio credentials
TWILIO_ACCOUNT_SID      = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN       = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER     = os.environ.get("TWILIO_PHONE_NUMBER", "")
# UPGRADE 6: Database URL
DATABASE_URL            = os.environ.get("DATABASE_URL", "")
# UPGRADE 9: Rate limit config
RATE_LIMIT_MAX_CALLS    = int(os.environ.get("RATE_LIMIT_MAX_CALLS", "5"))
RATE_LIMIT_WINDOW_SECS  = int(os.environ.get("RATE_LIMIT_WINDOW_SECS", "3600"))

# ── SYSTEM PROMPTS ────────────────────────────────────────────────────────────
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

# ── GROQ TOOL SCHEMAS ─────────────────────────────────────────────────────────
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
    },
    # ── UPGRADE 3: News headlines ─────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "get_news",
            "description": (
                "Fetch the top 3 current news headlines for a given topic or "
                "category. Call this whenever the user asks about news, "
                "headlines, what's happening, current events, or any specific "
                "news topic (e.g. 'sports news', 'tech news', 'India news')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": (
                            "The news topic or keyword to search for, e.g. "
                            "'technology', 'sports', 'India', 'business'. "
                            "Use 'general' if the user just says 'news' or "
                            "'headlines' with no specific topic."
                        ),
                    }
                },
                "required": ["topic"],
            },
        },
    },
    # ── UPGRADE 4: Google Calendar booking ───────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "book_appointment",
            "description": (
                "Book an appointment or meeting on Google Calendar. "
                "Call this whenever the user says 'book', 'schedule', "
                "'set a meeting', 'remind me', 'add to calendar', or any "
                "similar intent to create a future event."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": (
                            "The title or subject of the appointment, "
                            "e.g. 'Doctor appointment', 'Team meeting', "
                            "'Call with Rahul'."
                        ),
                    },
                    "date": {
                        "type": "string",
                        "description": (
                            "The date of the appointment in YYYY-MM-DD format. "
                            "Infer from relative phrases like 'tomorrow', "
                            "'next Monday', 'on Friday' using today's date."
                        ),
                    },
                    "time": {
                        "type": "string",
                        "description": (
                            "The time of the appointment in HH:MM 24-hour format, "
                            "e.g. '14:30' for 2:30 PM. Infer from phrases like "
                            "'at 3pm', 'half past two', 'morning at 9'."
                        ),
                    },
                },
                "required": ["title", "date", "time"],
            },
        },
    },
]


# ── UPGRADE 1 HELPERS ─────────────────────────────────────────────────────────

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

    Returns 'hi' for Hindi, 'en' for English/others.
    Falls back to 'en' on any detection error.
    """
    try:
        detected = detect(text)
        return "hi" if detected == "hi" else "en"
    except LangDetectException:
        return "en"


# ── UPGRADE 5: SMS helper ─────────────────────────────────────────────────────

def send_sms(to_number: str, message: str) -> None:
    """
    Send an SMS via Twilio REST API.

    Args:
        to_number: The recipient's phone number in E.164 format, e.g. '+919876543210'.
        message:   The SMS body text.

    This function is intentionally synchronous (Twilio's SDK is sync).
    Call it with loop.run_in_executor() when inside an async context.
    Logs errors without raising so the call flow is never interrupted.
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        log.warning("sms_not_configured", reason="Missing Twilio credentials")
        return
    if not to_number:
        log.warning("sms_skipped", reason="No caller number available")
        return
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg = client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=to_number,
        )
        log.info("sms_sent", to=to_number, sid=msg.sid)
    except Exception as exc:
        log.error("sms_failed", to=to_number, error=str(exc))


# ── UPGRADE 9: Rate limiting ──────────────────────────────────────────────────

# Maps phone number → list of call start timestamps within the window
_call_timestamps: dict[str, list[datetime]] = defaultdict(list)


def is_rate_limited(caller_number: str) -> bool:
    """
    Check whether `caller_number` has exceeded the allowed call rate.

    Prunes timestamps older than RATE_LIMIT_WINDOW_SECS before checking.
    Returns True if the caller should be blocked, False if the call is allowed.

    Thread-safe enough for asyncio's single-threaded event loop; if you switch
    to a multi-process deployment, replace with Redis (see .env.example).
    """
    if not caller_number:
        return False  # unknown number: allow (can't rate-limit what we can't identify)

    now = datetime.utcnow()
    window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW_SECS)

    # Prune stale timestamps
    _call_timestamps[caller_number] = [
        ts for ts in _call_timestamps[caller_number] if ts >= window_start
    ]

    if len(_call_timestamps[caller_number]) >= RATE_LIMIT_MAX_CALLS:
        log.warning(
            "rate_limit_exceeded",
            caller=caller_number,
            count=len(_call_timestamps[caller_number]),
            max_calls=RATE_LIMIT_MAX_CALLS,
        )
        return True

    # Record this call attempt
    _call_timestamps[caller_number].append(now)
    return False


# ── APP GLOBALS ──────────────────────────────────────────────────────────────
gcal_service = None   # Google Calendar service; set inside lifespan
db_pool: Optional[asyncpg.Pool] = None   # Postgres connection pool; set inside lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise long-lived clients at startup, clean up on shutdown."""
    global gcal_service, db_pool

    # ── UPGRADE 4: Google Calendar ────────────────────────────────────────
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            creds = service_account.Credentials.from_service_account_file(
                GOOGLE_SERVICE_ACCOUNT_JSON,
                scopes=["https://www.googleapis.com/auth/calendar"],
            )
            gcal_service = build("calendar", "v3", credentials=creds)
            log.info("gcal_initialised")
        except Exception as exc:
            log.error("gcal_init_failed", error=str(exc))
            gcal_service = None
    else:
        log.warning("gcal_disabled", reason="GOOGLE_SERVICE_ACCOUNT_JSON not set")

    # ── UPGRADE 6: Postgres connection pool ───────────────────────────────
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            # Ensure the calls table exists (idempotent)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS calls (
                        id               SERIAL PRIMARY KEY,
                        stream_sid       TEXT,
                        caller           TEXT,
                        start_time       TIMESTAMPTZ,
                        end_time         TIMESTAMPTZ,
                        transcript       JSONB,
                        language         TEXT,
                        duration_seconds INT
                    )
                """)
            log.info("db_pool_created", database_url=DATABASE_URL[:30] + "...")
        except Exception as exc:
            log.error("db_pool_failed", error=str(exc))
            db_pool = None
    else:
        log.warning("db_disabled", reason="DATABASE_URL not set — call logging disabled")

    yield  # app runs here

    # ── Shutdown cleanup ──────────────────────────────────────────────────
    if db_pool:
        await db_pool.close()
        log.info("db_pool_closed")
    log.info("app_shutdown")


app = FastAPI(title="VoiceAI Telecom Backend", lifespan=lifespan)
groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# ── UPGRADE 9: Rate-limited incoming-call endpoint ────────────────────────────
@app.post("/incoming-call")
async def incoming_call(request: Request):
    """
    Twilio calls this endpoint when someone dials the phone number.
    We respond with TwiML that upgrades the call to a WebSocket stream.

    UPGRADE 5: Captures caller number from request.form["From"] and passes it
               into the stream as a TwiML customParameter.
    UPGRADE 9: Checks rate limit before accepting the call; rejects if exceeded.
    """
    form  = await request.form()
    caller_number = form.get("From", "")
    host  = request.headers.get("host")

    log.info("incoming_call", caller=caller_number, host=host)

    # ── UPGRADE 9: Rate limit check ───────────────────────────────────────
    if is_rate_limited(caller_number):
        twiml = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Sorry, you have made too many calls recently. Please try again later.</Say>
  <Hangup/>
</Response>"""
        log.warning("call_rejected_rate_limit", caller=caller_number)
        return Response(content=twiml, media_type="application/xml")

    # ── UPGRADE 5: Pass caller number into stream as customParameter ──────
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Hello! I'm your voice assistant. How can I help you today?</Say>
  <Connect>
    <Stream url="wss://{host}/media-stream">
      <Parameter name="callerNumber" value="{caller_number}" />
    </Stream>
  </Connect>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


# ── UPGRADE 7: Twilio fallback TTS ────────────────────────────────────────────

def _inject_polly_twiml(call_sid: str, text: str) -> None:
    """
    Inject a Twilio <Say> verb mid-call using Polly TTS as a fallback.

    This is a synchronous operation (Twilio SDK); call via run_in_executor
    from async contexts.

    Args:
        call_sid: The active Twilio call SID from the stream start event.
        text:     The text to speak to the caller.
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN]):
        log.warning("polly_fallback_skipped", reason="Twilio credentials not set")
        return
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        twiml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Say voice="Polly.Joanna">{text}</Say></Response>'
        client.calls(call_sid).update(twiml=twiml)
        log.info("polly_fallback_used", call_sid=call_sid)
    except Exception as exc:
        log.error("polly_fallback_failed", call_sid=call_sid, error=str(exc))


# ── ELEVENLABS TTS ────────────────────────────────────────────────────────────

async def text_to_speech_stream(text: str, lang: str = "en") -> bytes:
    """
    Convert text to μ-law 8kHz audio bytes via ElevenLabs (Twilio format).

    UPGRADE 1: Accepts `lang` to select the correct voice ID per session.

    Raises httpx.HTTPStatusError on non-2xx responses so the caller
    (send_audio) can catch it and trigger the Polly fallback.
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
        "q":     city,
        "appid": OPENWEATHERMAP_API_KEY,
        "units": "metric",
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
    except httpx.HTTPStatusError as exc:
        return f"Weather service returned an error: {exc.response.status_code}."
    except Exception as exc:
        log.error("weather_fetch_failed", city=city, error=str(exc))
        return "Sorry, I couldn't fetch the weather right now. Please try again."


# ── UPGRADE 3: NewsAPI headlines fetch ───────────────────────────────────────

def _strip_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities from a string."""
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return text.strip()


async def get_news(topic: str) -> str:
    """
    Fetch the top 3 headlines for `topic` from NewsAPI and return a
    concise, voice-friendly string.

    Uses /v2/top-headlines with the `q` param for topic-specific queries,
    falling back to category='general' when topic is 'general'.

    Returns an error string (never raises) so the LLM can relay it gracefully.

    Example return:
        "Top headlines: 1. India wins T20 series against Australia.
         2. Tech stocks rally as inflation eases. 3. New climate deal signed
         at UN summit."
    """
    if not NEWS_API_KEY:
        return "News service is not configured. Please set NEWS_API_KEY."

    url = "https://newsapi.org/v2/top-headlines"
    params: dict = {"apiKey": NEWS_API_KEY, "pageSize": 3, "language": "en"}
    if topic.lower() == "general":
        params["country"] = "in"
    else:
        params["q"] = topic

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 401:
                return "News API key is invalid. Please check NEWS_API_KEY."
            resp.raise_for_status()
            data = resp.json()

        articles = data.get("articles", [])
        if not articles:
            return f"Sorry, I couldn't find any news about {topic} right now."

        headlines = []
        for i, article in enumerate(articles[:3], start=1):
            title = _strip_html(article.get("title") or "")
            title = re.sub(r"\s*-\s*[^-]+$", "", title).strip()
            if title:
                headlines.append(f"{i}. {title}")

        if not headlines:
            return f"Sorry, I couldn't find readable headlines for {topic}."

        return "Top headlines: " + " ".join(headlines)

    except httpx.HTTPStatusError as exc:
        return f"News service returned an error: {exc.response.status_code}."
    except Exception as exc:
        log.error("news_fetch_failed", topic=topic, error=str(exc))
        return "Sorry, I couldn't fetch the news right now. Please try again."


# ── UPGRADE 4: Google Calendar booking ───────────────────────────────────────

def _parse_rfc3339(date: str, time: str, tz_name: str) -> tuple[str, str]:
    """
    Convert a YYYY-MM-DD date and HH:MM time into a pair of RFC3339 strings
    (start, end) with a 1-hour duration, localised to `tz_name`.

    Example:
        _parse_rfc3339("2025-08-10", "14:30", "Asia/Kolkata")
        → ("2025-08-10T14:30:00+05:30", "2025-08-10T15:30:00+05:30")
    """
    tz       = ZoneInfo(tz_name)
    start_dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    end_dt   = start_dt + timedelta(hours=1)
    fmt      = "%Y-%m-%dT%H:%M:%S%z"

    def _colon_tz(s: str) -> str:
        return s[:-2] + ":" + s[-2:] if len(s) > 3 else s

    return _colon_tz(start_dt.strftime(fmt)), _colon_tz(end_dt.strftime(fmt))


async def book_appointment(
    title: str,
    date: str,
    time: str,
    caller_number: str = "",
) -> str:
    """
    Create a 1-hour Google Calendar event and return a voice-friendly
    confirmation string.

    UPGRADE 5: After successful booking, sends an SMS confirmation to
               `caller_number` if provided.

    Args:
        title:         Human-readable event title, e.g. "Doctor appointment".
        date:          Date string in YYYY-MM-DD format.
        time:          Time string in HH:MM 24-hour format.
        caller_number: Caller's phone number for SMS confirmation (optional).

    Returns a plain string — never raises — so the LLM can relay errors.
    """
    if gcal_service is None:
        return (
            "Calendar booking is not configured. "
            "Please set GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_CALENDAR_ID."
        )

    try:
        start_rfc, end_rfc = _parse_rfc3339(date, time, TIMEZONE)
    except (ValueError, KeyError) as exc:
        return f"I couldn't understand that date or time: {exc}. Please try again."

    event_body = {
        "summary": title,
        "start":   {"dateTime": start_rfc, "timeZone": TIMEZONE},
        "end":     {"dateTime": end_rfc,   "timeZone": TIMEZONE},
    }

    try:
        loop = asyncio.get_running_loop()
        created = await loop.run_in_executor(
            None,
            lambda: gcal_service.events()
                .insert(calendarId=GOOGLE_CALENDAR_ID, body=event_body)
                .execute(),
        )
        log.info("gcal_event_created", link=created.get("htmlLink"), title=title)

        display_time = datetime.strptime(time, "%H:%M").strftime("%-I:%M %p")
        confirmation = f"Booked: {title} on {date} at {display_time}."

        # ── UPGRADE 5: SMS confirmation ───────────────────────────────────
        if caller_number:
            sms_body = (
                f"✅ Appointment confirmed!\n"
                f"📅 {title}\n"
                f"🗓  {date} at {display_time}\n"
                f"Powered by VoiceAI"
            )
            await loop.run_in_executor(None, send_sms, caller_number, sms_body)

        return confirmation

    except HttpError as exc:
        log.error("gcal_http_error", error=str(exc))
        return f"Sorry, I couldn't create the calendar event. Google returned: {exc.reason}."
    except Exception as exc:
        log.error("gcal_unexpected_error", error=str(exc))
        return "Sorry, something went wrong while booking. Please try again."


# ── TOOL DISPATCHER ───────────────────────────────────────────────────────────

async def dispatch_tool_call(
    tool_name: str,
    tool_args: dict,
    caller_number: str = "",
) -> str:
    """
    Route a Groq tool_call to the appropriate function and return its result
    as a string. Add new tools here as they are introduced in later upgrades.

    Args:
        tool_name:     The function name returned by Groq.
        tool_args:     Parsed JSON arguments for the tool.
        caller_number: Caller's phone number, forwarded to tools that need it
                       (e.g. book_appointment for SMS).
    """
    if tool_name == "get_weather":
        city = tool_args.get("city", "")
        return await get_weather(city)

    if tool_name == "get_news":
        topic = tool_args.get("topic", "general")
        return await get_news(topic)

    if tool_name == "book_appointment":
        return await book_appointment(
            title=tool_args.get("title", "Appointment"),
            date=tool_args.get("date", ""),
            time=tool_args.get("time", "09:00"),
            caller_number=caller_number,
        )

    return f"Unknown tool: {tool_name}"


# ── UPGRADE 10: Conversation summarization ────────────────────────────────────

async def summarize_old_turns(old_turns: list[dict]) -> str:
    """
    Compress a list of conversation turns into a brief 2-3 sentence summary
    using Groq. Used by maybe_compress_conversation() to keep context lean.

    Args:
        old_turns: A list of {"role": ..., "content": ...} dicts to summarize.

    Returns a plain string summary or a fallback message on error.
    """
    formatted = "\n".join(
        f"{t['role'].capitalize()}: {t['content']}" for t in old_turns
    )
    try:
        response = await groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a concise summarizer. Summarize the following "
                        "conversation history in 2-3 sentences. Focus on the key "
                        "information exchanged and any actions taken."
                    ),
                },
                {"role": "user", "content": formatted},
            ],
            max_tokens=150,
            temperature=0.3,
        )
        summary = response.choices[0].message.content.strip()
        log.info("conversation_summarized", turns_compressed=len(old_turns))
        return summary
    except Exception as exc:
        log.error("summarization_failed", error=str(exc))
        return "Earlier conversation context is unavailable."


async def maybe_compress_conversation(conversation: list[dict]) -> list[dict]:
    """
    If the conversation exceeds 20 turns, summarize the oldest 10 turns into
    a single system message and return the compressed conversation.

    This replaces the naive hard-trim (conversation[-20:]) from the original
    backend, preserving semantic context across long calls.

    Args:
        conversation: The full conversation history (user + assistant turns).

    Returns the (possibly compressed) conversation list.
    """
    COMPRESS_THRESHOLD = 20
    TURNS_TO_SUMMARIZE = 10

    if len(conversation) <= COMPRESS_THRESHOLD:
        return conversation

    old_turns   = conversation[:TURNS_TO_SUMMARIZE]
    recent_turns = conversation[TURNS_TO_SUMMARIZE:]

    summary = await summarize_old_turns(old_turns)
    summary_turn = {
        "role":    "system",
        "content": f"Earlier in this call: {summary}",
    }
    return [summary_turn] + recent_turns


# ── GROQ LLM ──────────────────────────────────────────────────────────────────

async def get_llm_reply(
    conversation: list[dict],
    lang: str = "en",
    caller_number: str = "",
) -> str:
    """
    Send conversation history to Groq and return the assistant reply.

    UPGRADE 1:  Uses language-aware system prompt.
    UPGRADE 2:  Passes GROQ_TOOLS to Groq. If Groq emits a tool_call,
                we execute the tool, inject the result as a `tool` message,
                and make a second Groq call to produce the final reply.
    UPGRADE 5:  Forwards caller_number to dispatch_tool_call so book_appointment
                can send an SMS confirmation.

    The caller always receives a plain string — tool calling is transparent.

    Args:
        conversation:  Full conversation history (user + assistant turns).
        lang:          Detected session language ('en' or 'hi').
        caller_number: Caller's phone number for tool calls that need it.
    """
    system_prompt = get_system_prompt(lang)
    messages = [{"role": "system", "content": system_prompt}] + conversation

    # ── First Groq pass — may return a tool_call ──────────────────────────
    response = await groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        tools=GROQ_TOOLS,
        tool_choice="auto",
        max_tokens=120,
        temperature=0.6,
    )

    choice = response.choices[0]

    # ── No tool call — return the reply directly ──────────────────────────
    if choice.finish_reason != "tool_calls":
        return choice.message.content.strip()

    # ── Tool call detected — execute and make a second Groq pass ─────────
    tool_calls = choice.message.tool_calls

    messages.append({
        "role":       "assistant",
        "content":    choice.message.content or "",
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

    for tc in tool_calls:
        tool_name = tc.function.name
        try:
            tool_args = json.loads(tc.function.arguments)
        except json.JSONDecodeError:
            tool_args = {}

        log.info("tool_calling", tool=tool_name, args=tool_args)
        tool_result = await dispatch_tool_call(tool_name, tool_args, caller_number)
        log.info("tool_result", tool=tool_name, result=tool_result[:120])

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
    )
    return second_response.choices[0].message.content.strip()


# ── UPGRADE 6: DB logging helper ──────────────────────────────────────────────

async def log_call_to_db(
    stream_sid: str,
    caller: str,
    start_time: datetime,
    end_time: datetime,
    transcript: list[dict],
    language: str,
) -> None:
    """
    Write a completed call record to the Postgres `calls` table.

    Args:
        stream_sid:  Twilio stream SID for the call.
        caller:      Caller's phone number.
        start_time:  UTC datetime when the WebSocket connection opened.
        end_time:    UTC datetime when it closed.
        transcript:  Full conversation list (user + assistant turns) as JSONB.
        language:    Detected session language code ('en' or 'hi').
    """
    if db_pool is None:
        log.warning("db_log_skipped", reason="No DB pool")
        return

    duration_seconds = int((end_time - start_time).total_seconds())

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calls
                    (stream_sid, caller, start_time, end_time,
                     transcript, language, duration_seconds)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                """,
                stream_sid,
                caller,
                start_time,
                end_time,
                json.dumps(transcript),
                language,
                duration_seconds,
            )
        log.info(
            "call_logged",
            stream_sid=stream_sid,
            caller=caller,
            duration=duration_seconds,
        )
    except Exception as exc:
        log.error("db_log_failed", stream_sid=stream_sid, error=str(exc))


# ── MAIN WEBSOCKET — bridges Twilio ↔ Deepgram ↔ Groq ↔ ElevenLabs ──────────

@app.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    """
    Twilio sends us μ-law audio frames over this WebSocket.
    We pipe them to Deepgram for streaming STT, feed transcripts to Groq,
    then stream TTS audio back to Twilio.

    UPGRADE 1:  Per-session language detection and Deepgram/ElevenLabs switching.
    UPGRADE 2:  get_llm_reply handles Groq tool calling transparently.
    UPGRADE 5:  Extracts callerNumber from TwiML customParameters.
    UPGRADE 6:  Logs every call (transcript, duration, language) to Postgres.
    UPGRADE 7:  Falls back to Twilio Polly TTS if ElevenLabs fails.
    UPGRADE 8:  Uses structlog with per-session context binding.
    UPGRADE 10: Uses maybe_compress_conversation() instead of hard-trim.
    """
    await ws.accept()

    # Per-session state
    stream_sid:       Optional[str]  = None
    call_sid:         Optional[str]  = None
    caller_number:    str            = ""
    conversation:     list[dict]     = []
    transcript_buffer: list[str]    = []
    transcript_log:   list[dict]    = []   # full log for DB (UPGRADE 6)
    silence_task:     Optional[asyncio.Task] = None
    tts_task:         Optional[asyncio.Task] = None
    is_speaking:      bool           = False
    session_lang:     str            = "en"
    lang_detected:    bool           = False
    start_time:       datetime       = datetime.utcnow()

    # UPGRADE 8: Bind session context to logger early; rebind once we have IDs
    session_log = log.bind(stream_sid="pending", caller="unknown")
    session_log.info("ws_connected")

    loop = asyncio.get_running_loop()

    dg_client      = DeepgramClient(DEEPGRAM_API_KEY)
    dg_conn_holder: list = [None]

    # ── Deepgram helpers ──────────────────────────────────────────────────

    async def start_deepgram(lang: str) -> None:
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
        session_log.info("deepgram_started", language=get_deepgram_language(lang))

    async def on_transcript(self, result, **kwargs) -> None:
        """Called by Deepgram whenever a transcript segment is ready."""
        nonlocal silence_task, lang_detected, session_lang

        if not result.is_final:
            return

        sentence = result.channel.alternatives[0].transcript
        if not sentence:
            return

        # ── UPGRADE 1: language detection on first utterance ──────────────
        if not lang_detected:
            lang_detected = True
            detected = detect_language(sentence)
            if detected != session_lang:
                session_log.info(
                    "language_switched",
                    from_lang=session_lang,
                    to_lang=detected,
                )
                session_lang = detected
                old_conn = dg_conn_holder[0]
                if old_conn:
                    await old_conn.finish()
                await start_deepgram(session_lang)
            else:
                session_log.info("language_kept", lang=session_lang)

        transcript_buffer.append(sentence)
        session_log.info("stt_transcript", text=sentence)

        if silence_task and not silence_task.done():
            silence_task.cancel()

        silence_task = loop.call_soon_threadsafe(
            asyncio.ensure_future, handle_silence()
        )

    async def handle_silence() -> None:
        """Wait 800ms of silence, then treat buffered transcript as complete utterance."""
        nonlocal is_speaking, tts_task, conversation

        await asyncio.sleep(0.8)
        if not transcript_buffer:
            return

        user_text = " ".join(transcript_buffer)
        transcript_buffer.clear()
        session_log.info("user_utterance", text=user_text)

        if tts_task and not tts_task.done():
            tts_task.cancel()
        if is_speaking:
            await ws.send_json({"event": "clear", "streamSid": stream_sid})

        conversation.append({"role": "user", "content": user_text})
        transcript_log.append({"role": "user", "content": user_text})

        # ── UPGRADE 10: Compress conversation instead of hard-trimming ────
        conversation = await maybe_compress_conversation(conversation)

        reply = await get_llm_reply(
            conversation,
            lang=session_lang,
            caller_number=caller_number,
        )
        conversation.append({"role": "assistant", "content": reply})
        transcript_log.append({"role": "assistant", "content": reply})
        session_log.info("ai_reply", lang=session_lang, text=reply)

        tts_task = asyncio.ensure_future(send_audio(reply))

    async def send_audio(reply: str) -> None:
        """
        Fetch TTS audio and stream it back to Twilio.

        UPGRADE 7: Catches ElevenLabs failures and falls back to Twilio Polly TTS.
        """
        nonlocal is_speaking
        is_speaking = True
        try:
            audio_bytes = await text_to_speech_stream(reply, lang=session_lang)
            audio_b64   = base64.b64encode(audio_bytes).decode("utf-8")
            await ws.send_json({
                "event":     "media",
                "streamSid": stream_sid,
                "media":     {"payload": audio_b64},
            })

        except asyncio.CancelledError:
            session_log.info("tts_cancelled", reason="barge_in")

        except Exception as exc:
            # ── UPGRADE 7: ElevenLabs failed — fall back to Polly ─────────
            session_log.error(
                "elevenlabs_failed",
                error=str(exc),
                fallback="polly",
            )
            if call_sid:
                await loop.run_in_executor(
                    None, _inject_polly_twiml, call_sid, reply
                )
            else:
                session_log.warning(
                    "polly_fallback_skipped",
                    reason="call_sid_not_available_yet",
                )

        finally:
            is_speaking = False

    # ── Start Deepgram and enter message loop ─────────────────────────────
    await start_deepgram(session_lang)

    try:
        async for raw in ws.iter_text():
            msg   = json.loads(raw)
            event = msg.get("event")

            if event == "start":
                stream_sid    = msg["start"]["streamSid"]
                call_sid      = msg["start"].get("callSid", "")
                custom_params = msg["start"].get("customParameters", {})
                caller_number = custom_params.get("callerNumber", "")
                start_time    = datetime.utcnow()

                # Re-bind logger with real IDs (UPGRADE 8)
                session_log = log.bind(
                    stream_sid=stream_sid,
                    caller=caller_number,
                    call_sid=call_sid,
                )
                session_log.info("stream_started")

            elif event == "media" and stream_sid:
                audio = base64.b64decode(msg["media"]["payload"])
                if dg_conn_holder[0]:
                    await dg_conn_holder[0].send(audio)

            elif event == "stop":
                session_log.info("stream_stopped_by_twilio")
                break

    except WebSocketDisconnect:
        session_log.info("ws_disconnected")

    finally:
        # ── Cleanup ───────────────────────────────────────────────────────
        if dg_conn_holder[0]:
            await dg_conn_holder[0].finish()
        if silence_task and not silence_task.done():
            silence_task.cancel()
        if tts_task and not tts_task.done():
            tts_task.cancel()

        # ── UPGRADE 6: Log the call to Postgres ───────────────────────────
        end_time = datetime.utcnow()
        await log_call_to_db(
            stream_sid=stream_sid or "unknown",
            caller=caller_number,
            start_time=start_time,
            end_time=end_time,
            transcript=transcript_log,
            language=session_lang,
        )

        session_log.info(
            "session_cleaned_up",
            duration_secs=int((end_time - start_time).total_seconds()),
            turns=len(transcript_log),
        )


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Health check endpoint. Returns DB and Calendar service availability.
    Used by load balancers and monitoring tools (e.g. UptimeRobot, Render).
    """
    return {
        "status":           "ok",
        "db_connected":     db_pool is not None,
        "gcal_configured":  gcal_service is not None,
        "sms_configured":   bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN),
    }


# ── DB STATUS ─────────────────────────────────────────────────────────────────

@app.get("/calls/recent")
async def recent_calls(limit: int = 20):
    """
    Return the most recent `limit` call records from Postgres.
    Useful for debugging and monitoring call activity.

    Args:
        limit: Max number of records to return (default 20, max 100).
    """
    if db_pool is None:
        return {"error": "Database not configured"}

    limit = min(limit, 100)
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, stream_sid, caller, start_time, end_time,
                       language, duration_seconds
                FROM calls
                ORDER BY start_time DESC
                LIMIT $1
                """,
                limit,
            )
        return {
            "calls": [dict(row) for row in rows],
            "count": len(rows),
        }
    except Exception as exc:
        log.error("recent_calls_query_failed", error=str(exc))
        return {"error": "Failed to fetch call records"}
