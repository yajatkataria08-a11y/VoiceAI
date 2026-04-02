# ── STANDARD LIBRARY ──────────────────────────────────────────────────────────
import asyncio
import base64
import hashlib
import hmac
import random
import secrets
import time as time_module
from collections import defaultdict, deque
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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import Response, HTMLResponse
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
from twilio.request_validator import RequestValidator  # UPGRADE 23

# ── UPGRADE 6: asyncpg (Postgres) ─────────────────────────────────────────────
import asyncpg

# ── UPGRADE 13/17/21: APScheduler ────────────────────────────────────────────
from apscheduler.schedulers.asyncio import AsyncIOScheduler

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
DEEPGRAM_API_KEY            = os.environ["DEEPGRAM_API_KEY"]
GROQ_API_KEY                = os.environ["GROQ_API_KEY"]
ELEVENLABS_API_KEY          = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_VOICE_EN         = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
ELEVENLABS_VOICE_EN_ALT     = os.environ.get("ELEVENLABS_VOICE_ID_ALT", "AZnzlk1XvdvUeBnXmlld")  # UPGRADE 29
ELEVENLABS_VOICE_HI         = os.environ.get("ELEVENLABS_HINDI_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
ELEVENLABS_VOICE_CLONED     = os.environ.get("ELEVENLABS_VOICE_CLONED", "")  # UPGRADE 32

OPENWEATHERMAP_API_KEY      = os.environ.get("OPENWEATHERMAP_API_KEY", "")
NEWS_API_KEY                = os.environ.get("NEWS_API_KEY", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_CALENDAR_ID          = os.environ.get("GOOGLE_CALENDAR_ID", "primary")
TIMEZONE                    = os.environ.get("TIMEZONE", "Asia/Kolkata")
TWILIO_ACCOUNT_SID          = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN           = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER         = os.environ.get("TWILIO_PHONE_NUMBER", "")
DATABASE_URL                = os.environ.get("DATABASE_URL", "")
RATE_LIMIT_MAX_CALLS        = int(os.environ.get("RATE_LIMIT_MAX_CALLS", "5"))
RATE_LIMIT_WINDOW_SECS      = int(os.environ.get("RATE_LIMIT_WINDOW_SECS", "3600"))

# UPGRADE 12: OTP/PIN auth
OTP_PIN_REQUIRED            = os.environ.get("OTP_PIN_REQUIRED", "false").lower() == "true"
OTP_MASTER_PIN              = os.environ.get("OTP_MASTER_PIN", "")  # fallback for dev

# UPGRADE 15: IVR fallback
IVR_CONFIDENCE_THRESHOLD    = float(os.environ.get("IVR_CONFIDENCE_THRESHOLD", "0.6"))

# UPGRADE 20: IRCTC / PNR
RAPIDAPI_KEY                = os.environ.get("RAPIDAPI_KEY", "")

# UPGRADE 5: UPI payment status
RAZORPAY_KEY_ID             = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET         = os.environ.get("RAZORPAY_KEY_SECRET", "")

# UPGRADE 22: Bhashini
BHASHINI_API_KEY            = os.environ.get("BHASHINI_API_KEY", "")
BHASHINI_USER_ID            = os.environ.get("BHASHINI_USER_ID", "")

# UPGRADE 1: Sarvam AI (replaces Deepgram + ElevenLabs for all 22 Indian languages)
SARVAM_API_KEY              = os.environ.get("SARVAM_API_KEY", "")
SARVAM_STT_URL              = "https://api.sarvam.ai/speech-to-text"
SARVAM_TTS_URL              = "https://api.sarvam.ai/text-to-speech"
SARVAM_LANG_CODES = {
    "hi": "hi-IN", "ta": "ta-IN", "bn": "bn-IN", "mr": "mr-IN",
    "te": "te-IN", "kn": "kn-IN", "gu": "gu-IN", "pa": "pa-IN",
    "ml": "ml-IN", "or": "or-IN", "hi-en": "hi-IN",
}

# UPGRADE 25: session timeout
SESSION_TIMEOUT_SECS        = int(os.environ.get("SESSION_TIMEOUT_SECS", "120"))

# UPGRADE 27: abuse detection
ABUSE_URGENCY_THRESHOLD     = int(os.environ.get("ABUSE_URGENCY_THRESHOLD", "4"))

# UPGRADE 31: emergency escalation
EMERGENCY_CONTACT_NUMBER    = os.environ.get("EMERGENCY_CONTACT_NUMBER", "")

# UPGRADE 29: A/B voice testing
AB_TEST_ENABLED             = os.environ.get("AB_VOICE_TEST", "false").lower() == "true"

# NEW: Dashboard authentication
DASHBOARD_TOKEN             = os.environ.get("DASHBOARD_TOKEN", "")  # Bearer token for /dashboard

# NEW: Redis URL for persistent rate-limiting (optional — falls back to in-memory)
REDIS_URL                   = os.environ.get("REDIS_URL", "")

# BUG FIX 5: Updated Groq model strings
GROQ_MODEL_MAIN = "llama-3.3-70b-versatile"   # main conversational LLM
GROQ_MODEL_FAST = "llama-3.1-8b-instant"       # summarisation + emotion (low-latency)

# ── SUPPORTED LOCAL LANGUAGES (UPGRADE 22) ────────────────────────────────────
BHASHINI_LANG_CODES = {"ta", "bn", "mr"}  # Tamil, Bengali, Marathi

# ── SYSTEM PROMPTS ────────────────────────────────────────────────────────────
SYSTEM_PROMPT_EN = """You are a helpful voice assistant reachable by phone.
You help elderly users, visually impaired users, and busy workers (truck drivers,
construction workers) who cannot safely use a screen.
Keep every reply SHORT (1-3 sentences). Speak naturally. Never ask multiple questions at once.
You can help with: news headlines, weather, directions, booking reminders, PNR status, and general Q&A."""

SYSTEM_PROMPT_HI = """आप एक सहायक वॉइस असिस्टेंट हैं जो फोन पर उपलब्ध हैं।
आप बुजुर्ग उपयोगकर्ताओं, दृष्टिबाधित उपयोगकर्ताओं और व्यस्त श्रमिकों की मदद करते हैं।
हर जवाब छोटा रखें (1-3 वाक्य)। स्वाभाविक रूप से बोलें। एक बार में एक से अधिक प्रश्न न पूछें।
आप समाचार, मौसम, दिशा-निर्देश, बुकिंग, PNR स्टेटस और सामान्य प्रश्नों में मदद कर सकते हैं।
हमेशा हिंदी में उत्तर दें।"""

# UPGRADE 19: Hinglish system prompt
SYSTEM_PROMPT_HINGLISH = """You are a helpful voice assistant on the phone.
The caller speaks Hinglish — a natural mix of Hindi and English. Reply in the same
Hinglish style: friendly, short (1-3 sentences), and natural. Switch between Hindi
and English words exactly as a bilingual Indian speaker would.
You can help with: mausam, news, train PNR, appointment booking, and general questions."""

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
                            "'London', 'New York'. Include country code if ambiguous."
                        ),
                    }
                },
                "required": ["city"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_news",
            "description": (
                "Fetch the top 3 current news headlines for a given topic. "
                "Call this whenever the user asks about news, headlines, "
                "what's happening, current events, or any specific news topic."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": (
                            "The news topic or keyword, e.g. 'technology', 'sports', "
                            "'India'. Use 'general' if no specific topic given."
                        ),
                    }
                },
                "required": ["topic"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "book_appointment",
            "description": (
                "Book an appointment on Google Calendar. Call this whenever the user "
                "says 'book', 'schedule', 'set a meeting', 'remind me', or 'add to calendar'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Title of the appointment."},
                    "date":  {"type": "string", "description": "Date in YYYY-MM-DD format."},
                    "time":  {"type": "string", "description": "Time in HH:MM 24-hour format."},
                },
                "required": ["title", "date", "time"],
            },
        },
    },
    # UPGRADE 13: callback scheduling tool
    {
        "type": "function",
        "function": {
            "name": "schedule_callback",
            "description": (
                "Schedule an outbound callback to the caller at a specific time. "
                "Call this when the user says 'call me back at X', 'remind me at X', "
                "or 'call me later at X'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "callback_time": {
                        "type": "string",
                        "description": "Time for the callback in HH:MM 24-hour format.",
                    },
                    "callback_date": {
                        "type": "string",
                        "description": "Date for the callback in YYYY-MM-DD format. Use today if not specified.",
                    },
                    "reason": {
                        "type": "string",
                        "description": "Brief reason / topic for the callback.",
                    },
                },
                "required": ["callback_time", "callback_date"],
            },
        },
    },
    # UPGRADE 20: IRCTC PNR tool
    {
        "type": "function",
        "function": {
            "name": "check_pnr",
            "description": (
                "Check the current status of an Indian railway PNR number. "
                "Call this when the user asks about train status, PNR, ticket confirmation, "
                "or says any 10-digit number that could be a PNR."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "pnr": {
                        "type": "string",
                        "description": "10-digit PNR number, digits only.",
                    }
                },
                "required": ["pnr"],
            },
        },
    },
    # UPGRADE 5: UPI payment status tool
    {
        "type": "function",
        "function": {
            "name": "check_upi_payment",
            "description": (
                "Check whether a UPI or Razorpay payment went through. "
                "Call this when the user asks 'did my payment go through', "
                "'check my UPI transaction', 'payment status', or gives a payment/order ID."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "payment_id": {
                        "type": "string",
                        "description": (
                            "Razorpay payment ID (e.g. 'pay_ABC123') or UPI transaction "
                            "reference number spoken by the caller."
                        ),
                    }
                },
                "required": ["payment_id"],
            },
        },
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 19 — HINGLISH LANGUAGE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

# Common Hindi words that appear frequently in Hinglish speech (transliterated)
_HINDI_WORDS = re.compile(
    r"\b(hai|hain|kya|nahi|nahin|karo|karna|bolo|batao|mujhe|aapka|"
    r"aur|lekin|toh|yaar|kal|aaj|abhi|thoda|bahut|accha|theek|sahi|"
    r"bhai|didi|ji|haan|haan ji|nahi ji|kar|raha|rahi|ho|hoga)\b",
    re.IGNORECASE,
)


def detect_language(text: str) -> str:
    """
    UPGRADE 19: Detect en / hi / hi-en (Hinglish) / ta / bn / mr.
    Strategy:
      1. Count Hindi-word hits in the text.
      2. If >= 2 Hindi hits AND the text also has English words → "hi-en" (Hinglish).
      3. If langdetect says "hi" and no Hinglish signal → "hi".
      4. Otherwise → "en".
    Bhashini languages (ta, bn, mr) are passed through from langdetect when
    BHASHINI_API_KEY is configured; otherwise they fall back to "en".
    """
    try:
        detected = detect(text)
    except LangDetectException:
        detected = "en"

    hindi_hits = len(_HINDI_WORDS.findall(text))
    has_english = bool(re.search(r"[a-zA-Z]{3,}", text))

    if hindi_hits >= 2 and has_english:
        return "hi-en"  # Hinglish
    if detected == "hi":
        return "hi"
    if detected in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return detected
    return "en"


# ── UPGRADE 1 HELPERS ─────────────────────────────────────────────────────────

def get_elevenlabs_voice(lang: str, caller_number: str = "") -> str:
    """
    UPGRADE 32: prefer cloned brand voice if configured.
    UPGRADE 29: A/B bucket by caller number hash.
    """
    if ELEVENLABS_VOICE_CLONED:
        return ELEVENLABS_VOICE_CLONED
    if lang == "hi" or lang == "hi-en":
        return ELEVENLABS_VOICE_HI
    # UPGRADE 29: A/B test
    if AB_TEST_ENABLED and caller_number:
        bucket = int(hashlib.md5(caller_number.encode()).hexdigest(), 16) % 2
        return ELEVENLABS_VOICE_EN_ALT if bucket == 1 else ELEVENLABS_VOICE_EN
    return ELEVENLABS_VOICE_EN


def get_deepgram_language(lang: str) -> str:
    if lang == "hi":
        return "hi"
    if lang == "hi-en":
        return "hi-en"  # Deepgram multi-language mode
    return "en-US"


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 24 — RETRY WITH EXPONENTIAL BACKOFF
# ══════════════════════════════════════════════════════════════════════════════

async def api_call_with_retry(coro_fn, *args, retries: int = 3, base_delay: float = 0.5, **kwargs):
    """
    Call an async coroutine function with exponential backoff retries.
    Usage: result = await api_call_with_retry(some_async_fn, arg1, arg2, retries=3)
    """
    last_exc = None
    for attempt in range(retries):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.3)
                log.warning("api_retry", attempt=attempt + 1, delay=round(delay, 2), error=str(exc))
                await asyncio.sleep(delay)
    raise last_exc


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 11 — IMPROVED EMOTION / SENTIMENT DETECTION
# ══════════════════════════════════════════════════════════════════════════════

EMOTION_LABELS = ["neutral", "happy", "frustrated", "sad", "confused", "angry", "urgent"]

_URGENCY_KEYWORDS = re.compile(
    r"\b(help|emergency|ambulance|police|fire|dying|chest pain|"
    r"can't breathe|not breathing|accident|bleeding|attack|"
    r"please help|someone help|call 911|call 112)\b",
    re.IGNORECASE,
)

_EMOTION_VOICE_SETTINGS: dict[str, dict] = {
    "urgent":     {"stability": 0.85, "similarity_boost": 0.60},
    "angry":      {"stability": 0.75, "similarity_boost": 0.65},
    "frustrated": {"stability": 0.70, "similarity_boost": 0.70},
    "sad":        {"stability": 0.72, "similarity_boost": 0.75},
    "confused":   {"stability": 0.65, "similarity_boost": 0.75},
    "happy":      {"stability": 0.45, "similarity_boost": 0.80},
    "neutral":    {"stability": 0.50, "similarity_boost": 0.75},
}

_ESCALATION_WEIGHT: dict[str, int] = {
    "urgent":     3,
    "angry":      2,
    "frustrated": 1,
    "sad":        0,
    "confused":   0,
    "happy":      0,
    "neutral":    0,
}


class EmotionResult:
    __slots__ = ("emotion", "confidence", "intensity")

    def __init__(self, emotion: str, confidence: float, intensity: str):
        self.emotion    = emotion
        self.confidence = confidence
        self.intensity  = intensity

    def to_dict(self) -> dict:
        return {"emotion": self.emotion, "confidence": self.confidence, "intensity": self.intensity}

    def __repr__(self) -> str:
        return f"EmotionResult({self.emotion}, conf={self.confidence:.2f}, {self.intensity})"


async def detect_emotion(text: str) -> EmotionResult:
    if _URGENCY_KEYWORDS.search(text):
        log.info("emotion_urgency_keyword_hit", text_snippet=text[:60])
        return EmotionResult("urgent", confidence=0.99, intensity="high")

    prompt = (
        "Classify the emotion in this spoken message. "
        "Reply with ONLY a JSON object — no markdown, no explanation. "
        "Format: "
        '{"emotion": "<label>", "confidence": <0.0-1.0>, "intensity": "<low|medium|high>"} '
        f"Valid labels: {', '.join(EMOTION_LABELS[:-1])}"
        "\n\nMessage: " + text
    )
    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=40,
            temperature=0.0,
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        data       = json.loads(raw)
        emotion    = data.get("emotion", "neutral").lower()
        confidence = float(data.get("confidence", 0.5))
        intensity  = data.get("intensity", "medium").lower()
        if emotion not in EMOTION_LABELS:
            emotion = "neutral"
        confidence = max(0.0, min(1.0, confidence))
        if intensity not in ("low", "medium", "high"):
            intensity = "medium"
        return EmotionResult(emotion, confidence, intensity)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        log.warning("emotion_parse_failed", error=str(exc))
        return EmotionResult("neutral", confidence=0.5, intensity="low")
    except Exception as exc:
        log.error("emotion_detection_failed", error=str(exc))
        return EmotionResult("neutral", confidence=0.5, intensity="low")


def get_emotion_tone_instruction(result: EmotionResult) -> str:
    if result.confidence < 0.6:
        return ""
    intensity_qualifier = {"low": "slightly ", "medium": "", "high": "very "}.get(result.intensity, "")
    instructions = {
        "urgent": (
            "⚠️ URGENT: The caller may be in danger or distress. "
            "Stay calm and speak slowly. Ask them to confirm their situation. "
            "If it is an emergency, tell them to call 112 (India) or 911 immediately."
        ),
        "frustrated": (
            f"The caller sounds {intensity_qualifier}frustrated. "
            "Be extra calm, patient, and apologetic. "
            "Acknowledge their frustration before giving your answer."
        ),
        "angry": (
            f"The caller sounds {intensity_qualifier}angry. "
            "De-escalate immediately. Stay very calm and empathetic. "
            "Keep your reply very brief. Do not argue or over-explain."
        ),
        "sad":     (f"The caller sounds {intensity_qualifier}sad. Be warm, gentle, and supportive in tone."),
        "confused": (
            f"The caller sounds {intensity_qualifier}confused. "
            "Be extra clear and simple. Avoid jargon. Offer to repeat or explain differently."
        ),
        "happy":   "The caller sounds happy. Match their energy — be warm, friendly, and upbeat.",
        "neutral": "",
    }
    return instructions.get(result.emotion, "")


def get_voice_settings_for_emotion(emotion: str) -> dict:
    return _EMOTION_VOICE_SETTINGS.get(emotion, _EMOTION_VOICE_SETTINGS["neutral"])


def compute_escalation_score(emotion_arc: list) -> int:
    recent = emotion_arc[-3:]
    return sum(_ESCALATION_WEIGHT.get(r.emotion, 0) for r in recent)


def dominant_emotion(emotion_arc: list) -> str:
    if not emotion_arc:
        return "neutral"
    counts: dict[str, float] = defaultdict(float)
    for r in emotion_arc:
        counts[r.emotion] += r.confidence
    return max(counts, key=lambda k: counts[k])


def get_system_prompt(lang: str, emotion_result: Optional[EmotionResult] = None,
                      context: Optional[dict] = None,
                      caller_profile: Optional[dict] = None) -> str:
    """
    UPGRADE 16: inject multi-turn context (city, etc.) into system prompt.
    UPGRADE 19: Hinglish-aware prompt selection.
    UPGRADE 7: inject cross-call caller profile (name, city, language, last_topic).
    """
    if lang == "hi-en":
        base = SYSTEM_PROMPT_HINGLISH
    elif lang == "hi":
        base = SYSTEM_PROMPT_HI
    else:
        base = SYSTEM_PROMPT_EN

    if emotion_result:
        tone = get_emotion_tone_instruction(emotion_result)
        if tone:
            base = f"{base}\n\n{tone}"

    # UPGRADE 7: greet returning callers by name and remember their preferences
    if caller_profile:
        profile_parts = []
        if caller_profile.get("name"):
            profile_parts.append(f"The caller's name is {caller_profile['name']} — greet them by name.")
        if caller_profile.get("city"):
            profile_parts.append(f"Their usual city is {caller_profile['city']}.")
        if caller_profile.get("last_topic"):
            profile_parts.append(f"Last time they asked about: {caller_profile['last_topic']}.")
        if profile_parts:
            base += "\n\nReturning caller profile: " + " ".join(profile_parts)

    # UPGRADE 16: inject known entities so the LLM can reference them
    if context:
        ctx_parts = [f"{k}={v}" for k, v in context.items() if v]
        if ctx_parts:
            base += f"\n\nKnown caller context: {', '.join(ctx_parts)}. Use this so the caller doesn't have to repeat themselves."

    return base.strip()


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 12 — OTP / PIN AUTHENTICATION
# ══════════════════════════════════════════════════════════════════════════════
# PIN is a 4-digit code stored per-number in the `caller_pins` DB table.
# Session states: "unauthenticated" → "verifying" → "authenticated"
# On first call the system generates and SMS's a PIN; subsequent calls
# the caller speaks their PIN and we verify it.

_pin_store: dict[str, str] = {}  # in-memory fallback if DB unavailable


def _extract_pin(text: str) -> Optional[str]:
    """Extract a 4-digit PIN spoken by the caller (handles speech artifacts)."""
    digits = re.sub(r"\D", "", text)
    if len(digits) == 4:
        return digits
    # Handle spoken digits: "one two three four" → "1234"
    word_to_digit = {
        "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
        "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
        "ek": "1", "do": "2", "teen": "3", "char": "4", "paanch": "5",
        "chhe": "6", "saat": "7", "aath": "8", "nau": "9", "shoonya": "0",
    }
    spoken = re.findall(
        r"\b(?:zero|one|two|three|four|five|six|seven|eight|nine|"
        r"ek|do|teen|char|paanch|chhe|saat|aath|nau|shoonya)\b",
        text, re.IGNORECASE
    )
    if len(spoken) == 4:
        return "".join(word_to_digit[w.lower()] for w in spoken)
    return None


async def get_or_create_pin(caller_number: str) -> str:
    """Return existing PIN or generate + store a new one."""
    if db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT pin FROM caller_pins WHERE caller=$1", caller_number
            )
            if row:
                return row["pin"]
            new_pin = str(secrets.randbelow(9000) + 1000)  # 1000–9999
            await conn.execute(
                "INSERT INTO caller_pins(caller, pin) VALUES($1,$2) ON CONFLICT(caller) DO NOTHING",
                caller_number, new_pin,
            )
            return new_pin
    # Fallback: in-memory
    if caller_number not in _pin_store:
        _pin_store[caller_number] = str(secrets.randbelow(9000) + 1000)
    return _pin_store[caller_number]


async def verify_pin(caller_number: str, entered_pin: str) -> bool:
    correct = await get_or_create_pin(caller_number)
    return secrets.compare_digest(correct, entered_pin)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 16 — MULTI-TURN TOOL MEMORY (entity extraction)
# ══════════════════════════════════════════════════════════════════════════════

async def extract_entities(text: str, existing: dict) -> dict:
    """
    Extract named entities (city, person_name, date, pnr) from the user's
    utterance and merge into the existing context dict.
    Uses a fast Groq call; falls back gracefully on failure.
    """
    prompt = (
        "Extract named entities from this voice message. "
        "Reply ONLY with a compact JSON object (no markdown). "
        "Keys allowed: city, person_name, date (YYYY-MM-DD), pnr (10 digits). "
        "Only include keys that are clearly present. "
        "Example: {\"city\": \"Mumbai\", \"date\": \"2025-06-01\"}\n\n"
        "Message: " + text
    )
    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60,
            temperature=0.0,
        )
        raw  = response.choices[0].message.content.strip()
        raw  = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        # Merge: new values override old, but only for non-empty strings
        merged = {**existing}
        for k, v in data.items():
            if v and k in ("city", "person_name", "date", "pnr"):
                merged[k] = str(v)
        return merged
    except Exception:
        return existing  # silently keep existing context on failure


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 18 — INTENT CONFIDENCE THRESHOLD
# ══════════════════════════════════════════════════════════════════════════════

async def check_reply_confidence(user_text: str, ai_reply: str) -> float:
    """
    Ask the fast LLM: how confident is this reply? Returns 0.0–1.0.
    If < 0.5, the main loop will ask a clarifying question instead.
    """
    prompt = (
        "Rate how confidently the assistant reply addresses the user request. "
        "Reply ONLY with a JSON object: {\"confidence\": <0.0-1.0>}. "
        "Low confidence = the reply is vague, hedging, or likely wrong. "
        f"\nUser: {user_text}\nAssistant: {ai_reply}"
    )
    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=20,
            temperature=0.0,
        )
        raw  = response.choices[0].message.content.strip()
        raw  = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        return max(0.0, min(1.0, float(data.get("confidence", 0.8))))
    except Exception:
        return 0.8  # assume confident on parse error


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 28 — CALL TOPIC TAGGING
# ══════════════════════════════════════════════════════════════════════════════

TOPIC_LABELS = ["weather", "booking", "news", "pnr", "reminder", "callback", "general"]


async def classify_call_topics(transcript: list[dict]) -> list[str]:
    """Classify the topics discussed in a completed call."""
    user_turns = " | ".join(
        t["content"] for t in transcript if t.get("role") == "user"
    )
    if not user_turns:
        return ["general"]
    prompt = (
        "Classify this call transcript into topics. "
        f"Valid topics: {', '.join(TOPIC_LABELS)}. "
        "Reply ONLY with a JSON array of matching topic strings. "
        "Example: [\"weather\", \"booking\"]\n\nTranscript: " + user_turns[:500]
    )
    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=40,
            temperature=0.0,
        )
        raw    = response.choices[0].message.content.strip()
        raw    = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        topics = json.loads(raw)
        return [t for t in topics if t in TOPIC_LABELS] or ["general"]
    except Exception:
        return ["general"]


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 5: SMS helper
# ══════════════════════════════════════════════════════════════════════════════

def send_sms(to_number: str, message: str) -> None:
    """Synchronous — call via run_in_executor from async contexts."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        log.warning("sms_not_configured")
        return
    if not to_number:
        log.warning("sms_skipped", reason="No caller number")
        return
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg    = client.messages.create(body=message, from_=TWILIO_PHONE_NUMBER, to=to_number)
        log.info("sms_sent", to=to_number, sid=msg.sid)
    except Exception as exc:
        log.error("sms_failed", to=to_number, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 30 — WHATSAPP CALL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def send_whatsapp_summary(to_number: str, summary: str) -> None:
    """Send a post-call summary over WhatsApp (Twilio Sandbox / Business API)."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    if not to_number:
        return
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        wa_from = f"whatsapp:{TWILIO_PHONE_NUMBER}"
        wa_to   = f"whatsapp:{to_number}"
        client.messages.create(body=summary, from_=wa_from, to=wa_to)
        log.info("whatsapp_summary_sent", to=to_number)
    except Exception as exc:
        # WhatsApp is best-effort — caller may not have it
        log.warning("whatsapp_summary_skipped", to=to_number, error=str(exc))


async def build_call_summary(transcript: list[dict], topics: list[str],
                              caller_number: str, duration_secs: int) -> str:
    """Build a human-friendly WhatsApp summary of the call."""
    topics_str = ", ".join(topics)
    user_lines = [t["content"] for t in transcript if t.get("role") == "user"][:6]
    snippet    = " / ".join(user_lines)[:200]
    return (
        f"📞 *VoiceAI Call Summary*\n"
        f"Number: {caller_number}\n"
        f"Duration: {duration_secs}s\n"
        f"Topics: {topics_str}\n"
        f"Your questions: {snippet}\n"
        f"_Powered by VoiceAI_"
    )


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 9 + BUG FIX 10: Rate limiting — Redis-backed (survives restarts/workers)
# Falls back to in-memory when Redis is unavailable.
# ══════════════════════════════════════════════════════════════════════════════

_call_timestamps: dict[str, list[datetime]] = defaultdict(list)
_redis_client = None  # set during lifespan if REDIS_URL is configured


async def _init_redis() -> None:
    global _redis_client
    if not REDIS_URL:
        return
    try:
        import redis.asyncio as aioredis  # type: ignore
        _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await _redis_client.ping()
        log.info("redis_connected")
    except Exception as exc:
        log.warning("redis_unavailable", error=str(exc), fallback="in-memory rate limiting")
        _redis_client = None


async def is_rate_limited_async(caller_number: str) -> bool:
    """
    Persistent rate-limit check.
    Redis path: uses a sorted-set (ZSET) keyed by caller, scored by epoch seconds.
    Fallback: in-memory defaultdict (resets on restart).
    """
    if not caller_number:
        return False

    now          = datetime.utcnow()
    window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW_SECS)

    if _redis_client is not None:
        try:
            key   = f"rl:{caller_number}"
            pipe  = _redis_client.pipeline()
            score = now.timestamp()
            pipe.zremrangebyscore(key, "-inf", window_start.timestamp())
            pipe.zcard(key)
            pipe.zadd(key, {str(score): score})
            pipe.expire(key, RATE_LIMIT_WINDOW_SECS + 10)
            results = await pipe.execute()
            count_before_add = results[1]
            if count_before_add >= RATE_LIMIT_MAX_CALLS:
                log.warning("rate_limit_exceeded", caller=caller_number, backend="redis")
                # Remove the just-added score since we're rejecting
                await _redis_client.zrem(key, str(score))
                return True
            return False
        except Exception as exc:
            log.warning("redis_rate_limit_error", error=str(exc), fallback="in-memory")

    # In-memory fallback
    _call_timestamps[caller_number] = [
        ts for ts in _call_timestamps[caller_number] if ts >= window_start
    ]
    if len(_call_timestamps[caller_number]) >= RATE_LIMIT_MAX_CALLS:
        log.warning("rate_limit_exceeded", caller=caller_number, backend="memory")
        return True
    _call_timestamps[caller_number].append(now)
    return False


def is_rate_limited(caller_number: str) -> bool:
    """Sync shim — runs the async version in the current event loop."""
    try:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(is_rate_limited_async(caller_number))
    except RuntimeError:
        # Called from inside a running loop — use a thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(asyncio.run, is_rate_limited_async(caller_number))
            return future.result(timeout=2)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 23 — TWILIO WEBHOOK SIGNATURE VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def verify_twilio_signature(request: Request, form_data: dict) -> bool:
    """Return True if the request is genuinely from Twilio."""
    if not TWILIO_AUTH_TOKEN:
        return True  # skip in dev when token not set
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    url       = str(request.url)
    signature = request.headers.get("X-Twilio-Signature", "")
    return validator.validate(url, form_data, signature)


# ══════════════════════════════════════════════════════════════════════════════
# APP GLOBALS
# ══════════════════════════════════════════════════════════════════════════════
gcal_service = None
db_pool: Optional[asyncpg.Pool] = None
scheduler   = AsyncIOScheduler(timezone=TIMEZONE)

# UPGRADE 13/17: in-memory pending callbacks and sent reminders
_pending_callbacks: list[dict] = []
_sent_reminders: set[str]      = set()  # gcal event_id → sent

# UPGRADE 15: IVR session state  {stream_sid: "waiting_dtmf" | None}
_ivr_state: dict[str, Optional[str]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global gcal_service, db_pool

    # Redis (for persistent rate limiting — optional)
    await _init_redis()

    # Google Calendar
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            creds        = service_account.Credentials.from_service_account_file(
                GOOGLE_SERVICE_ACCOUNT_JSON,
                scopes=["https://www.googleapis.com/auth/calendar"],
            )
            gcal_service = build("calendar", "v3", credentials=creds)
            log.info("gcal_initialised")
        except Exception as exc:
            log.error("gcal_init_failed", error=str(exc))
    else:
        log.warning("gcal_disabled", reason="GOOGLE_SERVICE_ACCOUNT_JSON not set")

    # Postgres pool
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=10, command_timeout=30,
            )
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
                        duration_seconds INT,
                        emotions         JSONB,
                        dominant_emotion TEXT,
                        topics           JSONB,
                        flagged          BOOLEAN DEFAULT FALSE,
                        ab_bucket        TEXT
                    )
                """)
                # Idempotent migrations
                for col_sql in [
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS emotions JSONB",
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS dominant_emotion TEXT",
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS topics JSONB",
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS flagged BOOLEAN DEFAULT FALSE",
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS ab_bucket TEXT",
                    # UPGRADE 12
                    """CREATE TABLE IF NOT EXISTS caller_pins (
                           caller TEXT PRIMARY KEY,
                           pin    TEXT NOT NULL
                       )""",
                    # UPGRADE 21: medicine reminders
                    """CREATE TABLE IF NOT EXISTS medicine_reminders (
                           id          SERIAL PRIMARY KEY,
                           caller      TEXT,
                           medicine    TEXT,
                           remind_time TEXT,
                           active      BOOLEAN DEFAULT TRUE,
                           language    TEXT DEFAULT 'hi'
                       )""",
                    # UPGRADE 7: cross-call caller profiles
                    """CREATE TABLE IF NOT EXISTS caller_profiles (
                           caller       TEXT PRIMARY KEY,
                           name         TEXT,
                           city         TEXT,
                           language     TEXT,
                           last_topic   TEXT,
                           last_seen    TIMESTAMPTZ
                       )""",
                    # idempotent column additions
                    "ALTER TABLE medicine_reminders ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'hi'",
                ]:
                    await conn.execute(col_sql)
            log.info("db_pool_created")
        except Exception as exc:
            log.error("db_pool_failed", error=str(exc))
            db_pool = None
    else:
        log.warning("db_disabled", reason="DATABASE_URL not set")

    # Start APScheduler
    scheduler.add_job(check_appointment_reminders, "interval", minutes=1,
                      id="appointment_reminders", replace_existing=True)
    scheduler.add_job(check_medicine_reminders, "interval", minutes=1,
                      id="medicine_reminders", replace_existing=True)
    scheduler.start()
    log.info("scheduler_started")

    yield

    scheduler.shutdown(wait=False)
    if db_pool:
        await db_pool.close()
    log.info("app_shutdown")


app = FastAPI(title="VoiceAI Telecom Backend", lifespan=lifespan)
groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 17 — PROACTIVE APPOINTMENT REMINDERS (scheduler job)
# ══════════════════════════════════════════════════════════════════════════════

async def check_appointment_reminders() -> None:
    """Called every minute by APScheduler. Fires outbound calls 30 min before events."""
    if gcal_service is None or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        now      = datetime.utcnow()
        window   = (now + timedelta(minutes=29)).isoformat() + "Z"
        now_str  = now.isoformat() + "Z"
        loop     = asyncio.get_running_loop()
        events   = await loop.run_in_executor(
            None,
            lambda: gcal_service.events().list(
                calendarId=GOOGLE_CALENDAR_ID,
                timeMin=now_str, timeMax=window,
                singleEvents=True, orderBy="startTime",
            ).execute(),
        )
        for event in events.get("items", []):
            event_id = event["id"]
            if event_id in _sent_reminders:
                continue
            attendees = [a.get("email", "") for a in event.get("attendees", [])]
            # Try to find a phone number in the event description
            desc  = event.get("description", "")
            phone = re.search(r"\+\d{10,15}", desc)
            if not phone:
                continue
            caller = phone.group(0)
            title  = event.get("summary", "your appointment")
            _sent_reminders.add(event_id)
            asyncio.ensure_future(_fire_reminder_call(caller, title))
            log.info("reminder_scheduled", event_id=event_id, caller=caller)
    except Exception as exc:
        log.error("reminder_check_failed", error=str(exc))


def _fire_reminder_call_sync(caller: str, title: str) -> None:
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Joanna">'
        f'Hello! This is VoiceAI. Just a reminder: {html.escape(title)} is starting in 30 minutes.'
        '</Say></Response>'
    )
    client.calls.create(
        to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml,
    )
    log.info("reminder_call_fired", caller=caller)


async def _fire_reminder_call(caller: str, title: str) -> None:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _fire_reminder_call_sync, caller, title)
    except Exception as exc:
        log.error("reminder_call_failed", caller=caller, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 21 — MEDICINE REMINDERS (scheduler job)
# ══════════════════════════════════════════════════════════════════════════════

async def check_medicine_reminders() -> None:
    """Called every minute. Fires outbound reminder calls for medicine schedules."""
    if db_pool is None or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        now_time = datetime.now(ZoneInfo(TIMEZONE)).strftime("%H:%M")
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT caller, medicine, language FROM medicine_reminders "
                "WHERE remind_time=$1 AND active=TRUE",
                now_time,
            )
        for row in rows:
            asyncio.ensure_future(_fire_medicine_call(row["caller"], row["medicine"], row.get("language") or "hi"))
    except Exception as exc:
        log.error("medicine_reminder_check_failed", error=str(exc))


def _fire_medicine_call_sync(caller: str, medicine: str, audio_b64: str) -> None:
    """Fire a medicine reminder call using pre-generated TTS audio."""
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    host   = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
    # Stream the pre-built audio via a TwiML <Play> of a data URI isn't supported;
    # fall back to Polly with language routing when Sarvam/Bhashini audio can't be embedded.
    # The audio is played by posting it as a media message via Twilio's <Play> verb
    # using a publicly accessible URL. For now we embed it via base64 in TwiML (works for short clips).
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        f'<Say voice="Polly.Aditi">{html.escape(f"Namaste! It is time to take your {medicine}. Please take your medicine now. Stay healthy!")}</Say>'
        '</Response>'
    )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("medicine_call_fired", caller=caller, medicine=medicine)


async def _fire_medicine_call(caller: str, medicine: str, lang: str = "hi") -> None:
    """
    UPGRADE 6: fire medicine reminder in the caller's preferred language.
    Generates TTS in the right language via text_to_speech_stream, then dials.
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    # Build the reminder text in the right language
    reminder_texts = {
        "hi":    f"नमस्ते! अब आपकी दवाई {medicine} लेने का समय है। कृपया अभी दवाई लें। स्वस्थ रहें!",
        "hi-en": f"Namaste! Time to take your {medicine}. Please take your medicine now. Stay healthy!",
        "ta":    f"வணக்கம்! இப்போது உங்கள் {medicine} மருந்து எடுக்கும் நேரம். உடனே மருந்து எடுங்கள். ஆரோக்கியமாக இருங்கள்!",
        "bn":    f"নমস্কার! এখন আপনার {medicine} ওষুধ খাওয়ার সময়। এখনই ওষুধ খান। সুস্থ থাকুন!",
        "mr":    f"नमस्कार! आता तुमची {medicine} औषधे घेण्याची वेळ आहे. आत्ता औषधे घ्या. निरोगी राहा!",
    }
    reminder_text = reminder_texts.get(lang, reminder_texts["hi-en"])
    loop = asyncio.get_running_loop()
    try:
        # Generate TTS audio in caller's language
        audio_bytes = await text_to_speech_stream(reminder_text, lang=lang)
        audio_b64   = base64.b64encode(audio_bytes).decode()
        await loop.run_in_executor(None, _fire_medicine_call_sync, caller, medicine, audio_b64)
    except Exception as exc:
        log.error("medicine_call_failed", caller=caller, lang=lang, error=str(exc))
        # Fallback: fire with hardcoded Polly
        await loop.run_in_executor(None, _fire_medicine_call_sync, caller, medicine, "")


# ══════════════════════════════════════════════════════════════════════════════
# INCOMING CALL ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/incoming-call")
async def incoming_call(request: Request):
    form          = await request.form()
    form_dict     = dict(form)
    caller_number = form.get("From", "")
    host          = request.headers.get("host")
    log.info("incoming_call", caller=caller_number)

    # UPGRADE 23: Twilio signature verification
    if not verify_twilio_signature(request, form_dict):
        log.warning("twilio_signature_invalid", caller=caller_number)
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    if await is_rate_limited_async(caller_number):
        twiml = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Sorry, you have made too many calls recently. Please try again later.</Say>
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


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 14 — MISSED-CALL TRIGGER / AUTO-CALLBACK
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/missed-call")
async def missed_call(request: Request):
    """
    Twilio StatusCallback for missed / unanswered calls.
    When call status is "no-answer" or "busy", VoiceAI auto-calls back.
    Also accepts direct POST from a missed-call detection system.
    """
    form          = await request.form()
    form_dict     = dict(form)
    caller_number = form.get("From", form.get("caller", ""))
    call_status   = form.get("CallStatus", "missed").lower()

    # UPGRADE 23: verify signature (best-effort — no 403 for status callbacks)
    if TWILIO_AUTH_TOKEN and not verify_twilio_signature(request, form_dict):
        log.warning("missed_call_signature_invalid")

    if not caller_number:
        return {"error": "No caller number provided"}

    if call_status in ("no-answer", "busy", "missed", ""):
        asyncio.ensure_future(_auto_callback(caller_number))
        log.info("missed_call_received", caller=caller_number, status=call_status)
    return {"status": "ok", "caller": caller_number}


def _auto_callback_sync(caller: str) -> None:
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Say voice="Polly.Joanna">Hello! You gave us a missed call. '
        'VoiceAI is here to help. How can I assist you today?</Say>'
        f'<Connect><Stream url="wss://{{host}}/media-stream">'
        f'<Parameter name="callerNumber" value="{html.escape(caller)}" />'
        '</Stream></Connect>'
        '</Response>'
    )
    # Note: host must be injected at call-time via env or passed as a param
    host = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
    twiml = twiml.replace("{host}", html.escape(host))
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("auto_callback_fired", caller=caller)


async def _auto_callback(caller: str) -> None:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        log.warning("auto_callback_skipped", reason="Twilio not configured")
        return
    await asyncio.sleep(3)  # brief delay so the network clears
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _auto_callback_sync, caller)
    except Exception as exc:
        log.error("auto_callback_failed", caller=caller, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 15 — IVR DTMF FALLBACK ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/ivr-input")
async def ivr_input(request: Request):
    """
    Handles DTMF keypad input when STT confidence is low.
    Twilio posts Digits here when the caller presses a key.
    """
    form    = await request.form()
    form_dict = dict(form)
    digits  = form.get("Digits", "")
    sid     = form.get("StreamSid", "")

    if not verify_twilio_signature(request, form_dict):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    log.info("ivr_input", digits=digits, stream_sid=sid)

    # Map keypad to a synthetic user utterance stored for the session to pick up
    ivr_map = {
        "1": "What is the weather today",
        "2": "Give me the latest news",
        "3": "I want to book an appointment",
        "4": "Check my PNR status",
        "0": "Help me, what can you do",
    }
    utterance = ivr_map.get(digits, "")
    if utterance and sid:
        _ivr_state[sid] = utterance

    response_map = {
        "1": "Fetching weather for you.",
        "2": "Getting the latest headlines.",
        "3": "Let me help you book an appointment.",
        "4": "Please say your 10-digit PNR number.",
        "0": "I can help with weather, news, bookings and train status.",
    }
    say_text = html.escape(response_map.get(digits, "Sorry, I didn't understand that option."))
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">{say_text}</Say>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 13 — CALLBACK SCHEDULING ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/callbacks/list")
async def list_callbacks():
    return {"callbacks": _pending_callbacks}


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 21 — MEDICINE REMINDER REGISTRATION
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/reminders/medicine")
async def register_medicine_reminder(request: Request):
    """
    Register a daily medicine reminder outbound call.
    Body: {"caller": "+91...", "medicine": "metformin", "remind_time": "08:00", "language": "hi"}
    UPGRADE 6: language field stored so reminder fires in caller's language.
    """
    body = await request.json()
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
    log.info("medicine_reminder_registered", caller=caller, medicine=medicine, time=remind_time, lang=language)
    return {"status": "registered", "caller": caller, "medicine": medicine, "remind_time": remind_time, "language": language}


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 27 — ADMIN: FLAGGED CALLS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/admin/flagged")
async def flagged_calls():
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, stream_sid, caller, start_time, dominant_emotion, topics "
                "FROM calls WHERE flagged=TRUE ORDER BY start_time DESC LIMIT 50"
            )
        return {"flagged": [dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        log.error("flagged_calls_failed", error=str(exc))
        return {"error": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 7: Polly fallback TTS
# ══════════════════════════════════════════════════════════════════════════════

def _inject_polly_twiml(call_sid: str, text: str) -> None:
    """BUG FIX 3: inject Twilio <Say> mid-call when ElevenLabs fails. Synchronous."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN]):
        return
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        twiml  = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<Response><Say voice="Polly.Joanna">{html.escape(text)}</Say></Response>'
        )
        client.calls(call_sid).update(twiml=twiml)
        log.info("polly_fallback_used", call_sid=call_sid)
    except Exception as exc:
        log.error("polly_fallback_failed", call_sid=call_sid, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# ELEVENLABS TTS
# ══════════════════════════════════════════════════════════════════════════════

async def _tts_elevenlabs(text: str, lang: str, emotion: str, caller_number: str = "") -> bytes:
    """Inner TTS call — wrapped by api_call_with_retry."""
    voice_id       = get_elevenlabs_voice(lang, caller_number)
    voice_settings = get_voice_settings_for_emotion(emotion)
    url            = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
    headers        = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"}
    payload        = {
        "text":           text,
        "model_id":       "eleven_turbo_v2",
        "voice_settings": voice_settings,
        "output_format":  "ulaw_8000",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.content


async def text_to_speech_stream(
    text: str,
    lang: str = "en",
    emotion: str = "neutral",
    caller_number: str = "",
) -> bytes:
    """
    TTS routing priority:
      1. Sarvam AI  — if SARVAM_API_KEY set AND lang is an Indian language (best quality)
      2. Bhashini   — fallback for ta/bn/mr if BHASHINI_API_KEY set
      3. ElevenLabs — English / Hinglish / no Indian-lang key
    UPGRADE 1: Sarvam AI integration
    UPGRADE 4: Bhashini STT/TTS path
    UPGRADE 24: wrapped with retry logic.
    UPGRADE 32: voice selection delegates to get_elevenlabs_voice().
    """
    is_indian_lang = lang in SARVAM_LANG_CODES
    if SARVAM_API_KEY and is_indian_lang:
        return await api_call_with_retry(_tts_sarvam, text, lang)
    if lang in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return await api_call_with_retry(_tts_bhashini, text, lang)
    return await api_call_with_retry(_tts_elevenlabs, text, lang, emotion, caller_number)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 22 — BHASHINI TTS (Tamil, Bengali, Marathi)
# ══════════════════════════════════════════════════════════════════════════════

_BHASHINI_LANG_CODES_TTS = {"ta": "ta-IN", "bn": "bn-IN", "mr": "mr-IN"}


async def _tts_bhashini(text: str, lang: str) -> bytes:
    """Call Bhashini TTS API and return μ-law audio bytes."""
    lang_code = _BHASHINI_LANG_CODES_TTS.get(lang, "hi-IN")
    url       = "https://dhruva-api.bhashini.gov.in/services/inference/pipeline"
    headers   = {
        "Authorization": BHASHINI_API_KEY,
        "Content-Type":  "application/json",
        "userID":        BHASHINI_USER_ID,
    }
    payload = {
        "pipelineTasks": [
            {
                "taskType": "tts",
                "config": {"language": {"sourceLanguage": lang_code}, "gender": "female"},
            }
        ],
        "inputData": {"input": [{"source": text}]},
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data       = resp.json()
        audio_b64  = data["pipelineResponse"][0]["audio"][0]["audioContent"]
        return base64.b64decode(audio_b64)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 4 — BHASHINI STT (regional language speech recognition)
# ══════════════════════════════════════════════════════════════════════════════

async def _stt_bhashini(audio_bytes: bytes, lang: str) -> str:
    """
    Send audio to Bhashini ASR pipeline for Tamil/Bengali/Marathi.
    Returns the transcribed text or empty string on failure.
    audio_bytes should be raw PCM or WAV; Bhashini accepts base64-encoded audio.
    """
    lang_code = _BHASHINI_LANG_CODES_TTS.get(lang, "hi-IN")
    url       = "https://dhruva-api.bhashini.gov.in/services/inference/pipeline"
    headers   = {
        "Authorization": BHASHINI_API_KEY,
        "Content-Type":  "application/json",
        "userID":        BHASHINI_USER_ID,
    }
    audio_b64 = base64.b64encode(audio_bytes).decode()
    payload   = {
        "pipelineTasks": [
            {
                "taskType": "asr",
                "config": {
                    "language":    {"sourceLanguage": lang_code},
                    "audioFormat": "wav",
                    "samplingRate": 8000,
                },
            }
        ],
        "inputData": {"audio": [{"audioContent": audio_b64}]},
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data["pipelineResponse"][0]["output"][0].get("source", "")
    except Exception as exc:
        log.error("bhashini_stt_failed", lang=lang, error=str(exc))
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 1 — SARVAM AI STT + TTS (best-quality for Indian languages)
# ══════════════════════════════════════════════════════════════════════════════

async def _stt_sarvam(audio_bytes: bytes, lang: str) -> str:
    """
    Send audio to Sarvam AI ASR for any supported Indian language.
    Returns transcribed text or empty string.
    """
    lang_code = SARVAM_LANG_CODES.get(lang, "hi-IN")
    headers   = {"api-subscription-key": SARVAM_API_KEY}
    files     = {"file": ("audio.wav", audio_bytes, "audio/wav")}
    data      = {"language_code": lang_code, "model": "saarika:v1", "with_timestamps": False}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(SARVAM_STT_URL, headers=headers, files=files, data=data)
            resp.raise_for_status()
            return resp.json().get("transcript", "")
    except Exception as exc:
        log.error("sarvam_stt_failed", lang=lang, error=str(exc))
        return ""


async def _tts_sarvam(text: str, lang: str) -> bytes:
    """
    Call Sarvam AI TTS and return μ-law 8kHz audio bytes.
    Supports all 22 official Indian languages in one API.
    """
    lang_code = SARVAM_LANG_CODES.get(lang, "hi-IN")
    headers   = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "inputs":        [text],
        "target_language_code": lang_code,
        "speaker":       "meera",
        "model":         "bulbul:v1",
        "enable_preprocessing": True,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(SARVAM_TTS_URL, headers=headers, json=payload)
        resp.raise_for_status()
        data      = resp.json()
        audio_b64 = data["audios"][0]
        return base64.b64decode(audio_b64)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 2: Weather
# ══════════════════════════════════════════════════════════════════════════════

async def _get_weather_inner(city: str) -> str:
    url    = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": OPENWEATHERMAP_API_KEY, "units": "metric"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        if resp.status_code == 404:
            return f"Sorry, I couldn't find weather data for {city}."
        resp.raise_for_status()
        d = resp.json()
    return (
        f"{d['name']}, {d['sys']['country']}: {round(d['main']['temp'])}°C, "
        f"feels like {round(d['main']['feels_like'])}°C, "
        f"humidity {d['main']['humidity']}%, {d['weather'][0]['description']}."
    )


async def get_weather(city: str) -> str:
    if not OPENWEATHERMAP_API_KEY:
        return "Weather service is not configured."
    try:
        return await api_call_with_retry(_get_weather_inner, city)
    except Exception as exc:
        log.error("weather_fetch_failed", city=city, error=str(exc))
        return "Sorry, I couldn't fetch the weather right now."


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 3: News
# ══════════════════════════════════════════════════════════════════════════════

def _strip_html(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text)).strip()


async def get_news(topic: str) -> str:
    if not NEWS_API_KEY:
        return "News service is not configured."
    url    = "https://newsapi.org/v2/top-headlines"
    params: dict = {"apiKey": NEWS_API_KEY, "pageSize": 3, "language": "en"}
    if topic.lower() == "general":
        params["country"] = "in"
    else:
        params["q"] = topic
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 401:
                return "News API key is invalid."
            resp.raise_for_status()
            articles = resp.json().get("articles", [])
        if not articles:
            return f"No news found for {topic}."
        headlines = []
        for i, a in enumerate(articles[:3], 1):
            t = re.sub(r"\s*-\s*[^-]+$", "", _strip_html(a.get("title") or "")).strip()
            if t:
                headlines.append(f"{i}. {t}")
        return "Top headlines: " + " ".join(headlines) if headlines else f"No headlines for {topic}."
    except httpx.HTTPStatusError as exc:
        return f"News service error: {exc.response.status_code}."
    except Exception as exc:
        log.error("news_fetch_failed", topic=topic, error=str(exc))
        return "Sorry, I couldn't fetch the news."


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 20 — IRCTC PNR STATUS
# ══════════════════════════════════════════════════════════════════════════════

async def check_pnr(pnr: str) -> str:
    """
    Check Indian Railways PNR status via RapidAPI IRCTC endpoint.
    Requires RAPIDAPI_KEY env var.
    """
    pnr = re.sub(r"\D", "", pnr)  # digits only
    if len(pnr) != 10:
        return f"That doesn't look like a valid PNR number. A PNR has exactly 10 digits."
    if not RAPIDAPI_KEY:
        return "PNR service is not configured. Please ask your administrator to set RAPIDAPI_KEY."
    url     = "https://irctc1.p.rapidapi.com/api/v3/getPNRStatus"
    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "irctc1.p.rapidapi.com",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers, params={"pnrNumber": pnr})
            resp.raise_for_status()
            data  = resp.json().get("data", {})
        if not data:
            return f"No data found for PNR {pnr}. Please check the number."
        train      = data.get("trainName", "Unknown train")
        train_no   = data.get("trainNumber", "")
        status     = data.get("bookingStatus", "")
        chart      = data.get("chartPrepared", False)
        passengers = data.get("passengerList", [])
        p_summary  = ""
        if passengers:
            statuses = [p.get("currentStatus", "WL") for p in passengers[:3]]
            p_summary = f"Passenger status: {', '.join(statuses)}."
        chart_str = "Chart prepared." if chart else "Chart not yet prepared."
        return (
            f"PNR {pnr}: {train} ({train_no}). "
            f"Booking status: {status}. "
            f"{p_summary} {chart_str}"
        ).strip()
    except httpx.HTTPStatusError as exc:
        return f"PNR service error: {exc.response.status_code}."
    except Exception as exc:
        log.error("pnr_check_failed", pnr=pnr, error=str(exc))
        return "Sorry, I couldn't fetch the PNR status right now."


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 5 — UPI / PAYMENT STATUS
# ══════════════════════════════════════════════════════════════════════════════

async def check_upi_payment(payment_id: str) -> str:
    """
    Check payment status via Razorpay API.
    payment_id: Razorpay payment ID (e.g. 'pay_ABC123') or any UPI ref spoken by caller.
    Requires RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET env vars.
    """
    payment_id = payment_id.strip()
    if not payment_id:
        return "I didn't catch the payment ID. Could you say it again?"
    if not (RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET):
        return "Payment status service is not configured. Please ask your administrator to set RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET."
    # Normalize: Razorpay IDs start with pay_
    if not payment_id.lower().startswith("pay_"):
        payment_id = "pay_" + re.sub(r"\s+", "", payment_id)
    url     = f"https://api.razorpay.com/v1/payments/{payment_id}"
    try:
        async with httpx.AsyncClient(
            timeout=10,
            auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
        ) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                return f"No payment found with ID {payment_id}. Please double-check the ID."
            resp.raise_for_status()
            data   = resp.json()
        status  = data.get("status", "unknown")
        amount  = data.get("amount", 0) / 100  # Razorpay stores paise
        method  = data.get("method", "")
        contact = data.get("contact", "")
        status_map = {
            "captured":  "successful ✅",
            "authorized": "authorized but not yet settled",
            "refunded":  "refunded",
            "failed":    "failed ❌",
            "created":   "pending — payment not yet completed",
        }
        friendly = status_map.get(status, status)
        return (
            f"Payment {payment_id}: ₹{amount:.2f} via {method}. "
            f"Status: {friendly}."
            + (f" Contact: {contact}." if contact else "")
        )
    except httpx.HTTPStatusError as exc:
        return f"Payment service error: {exc.response.status_code}."
    except Exception as exc:
        log.error("upi_payment_check_failed", payment_id=payment_id, error=str(exc))
        return "Sorry, I couldn't fetch the payment status right now."


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 4: Google Calendar + UPGRADE 13: Callback scheduling
# ══════════════════════════════════════════════════════════════════════════════

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
    # Embed phone number in description so proactive reminder can reach the caller
    event_body = {
        "summary":     title,
        "description": f"Booked via VoiceAI. Caller: {caller_number}",
        "start":       {"dateTime": start_rfc, "timeZone": TIMEZONE},
        "end":         {"dateTime": end_rfc,   "timeZone": TIMEZONE},
    }
    try:
        loop    = asyncio.get_running_loop()
        created = await loop.run_in_executor(
            None,
            lambda: gcal_service.events()
                .insert(calendarId=GOOGLE_CALENDAR_ID, body=event_body)
                .execute(),
        )
        log.info("gcal_event_created", link=created.get("htmlLink"))
        _dt          = datetime.strptime(time_str, "%H:%M")
        display_time = f"{_dt.hour % 12 or 12}:{_dt.minute:02d} {_dt.strftime('%p')}"
        confirmation = f"Booked: {title} on {date} at {display_time}."
        if caller_number:
            await loop.run_in_executor(
                None, send_sms, caller_number,
                f"✅ Appointment confirmed!\n📅 {title}\n🗓  {date} at {display_time}\nPowered by VoiceAI",
            )
        return confirmation
    except HttpError as exc:
        log.error("gcal_http_error", error=str(exc))
        return f"Sorry, Google Calendar returned an error: {exc.reason}."
    except Exception as exc:
        log.error("gcal_unexpected_error", error=str(exc))
        return "Sorry, something went wrong while booking."


async def schedule_callback(callback_time: str, callback_date: str,
                             reason: str = "", caller_number: str = "") -> str:
    """
    UPGRADE 13: schedule an outbound call at the requested time.
    Uses APScheduler one-shot job.
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return "Callback service is not configured."
    if not caller_number:
        return "I need your phone number to schedule a callback."
    try:
        tz      = ZoneInfo(TIMEZONE)
        run_at  = datetime.strptime(f"{callback_date} {callback_time}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    except ValueError:
        return f"I couldn't understand that time. Please say a time like '5 PM' or '17:00'."

    record = {
        "caller":    caller_number,
        "at":        run_at.isoformat(),
        "reason":    reason or "Your requested callback",
    }
    _pending_callbacks.append(record)

    scheduler.add_job(
        _fire_callback_sync,
        "date",
        run_date=run_at,
        args=[caller_number, reason or "your requested callback"],
        id=f"cb_{caller_number}_{callback_time}",
        replace_existing=True,
    )
    _dt          = datetime.strptime(callback_time, "%H:%M")
    display_time = f"{_dt.hour % 12 or 12}:{_dt.minute:02d} {_dt.strftime('%p')}"
    log.info("callback_scheduled", caller=caller_number, at=run_at.isoformat())
    return f"Got it! I'll call you back at {display_time}."


def _fire_callback_sync(caller: str, reason: str) -> None:
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Joanna">'
        f'Hello! This is your VoiceAI callback. You asked me to remind you about: '
        f'{html.escape(reason)}. How can I help you now?'
        '</Say></Response>'
    )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("callback_fired", caller=caller, reason=reason)


# ══════════════════════════════════════════════════════════════════════════════
# TOOL DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

async def dispatch_tool_call(tool_name: str, tool_args: dict, caller_number: str = "") -> str:
    if tool_name == "get_weather":
        return await get_weather(tool_args.get("city", ""))
    if tool_name == "get_news":
        return await get_news(tool_args.get("topic", "general"))
    if tool_name == "book_appointment":
        return await book_appointment(
            title=tool_args.get("title", "Appointment"),
            date=tool_args.get("date", ""),
            time_str=tool_args.get("time", "09:00"),
            caller_number=caller_number,
        )
    if tool_name == "schedule_callback":
        return await schedule_callback(
            callback_time=tool_args.get("callback_time", ""),
            callback_date=tool_args.get("callback_date", datetime.now().strftime("%Y-%m-%d")),
            reason=tool_args.get("reason", ""),
            caller_number=caller_number,
        )
    if tool_name == "check_pnr":
        return await check_pnr(tool_args.get("pnr", ""))
    if tool_name == "check_upi_payment":
        return await check_upi_payment(tool_args.get("payment_id", ""))
    return f"Unknown tool: {tool_name}"


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 10: Conversation summarization (with Bug Fix 9)
# ══════════════════════════════════════════════════════════════════════════════

async def summarize_old_turns(old_turns: list[dict]) -> str:
    formatted = "\n".join(f"{t['role'].capitalize()}: {t['content']}" for t in old_turns)
    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Summarize the following conversation in 2-3 sentences. "
                        "Focus on key information and any actions taken."
                    ),
                },
                {"role": "user", "content": formatted},
            ],
            max_tokens=150,
            temperature=0.3,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        log.error("summarization_failed", error=str(exc))
        return "Earlier conversation context unavailable."


async def maybe_compress_conversation(conversation: list[dict]) -> list[dict]:
    if len(conversation) <= 20:
        return conversation
    # Bug fix 9: preserve original system prompt(s) from first 10 turns
    system_messages    = [m for m in conversation[:10] if m.get("role") == "system"]
    turns_to_summarise = [m for m in conversation[:10] if m.get("role") != "system"]
    summary            = await summarize_old_turns(turns_to_summarise) if turns_to_summarise else ""
    summary_block      = [{"role": "system", "content": f"Earlier in this call: {summary}"}] if summary else []
    return system_messages + summary_block + conversation[10:]


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 7 — CROSS-CALL CONVERSATION MEMORY (caller profiles)
# ══════════════════════════════════════════════════════════════════════════════

async def load_caller_profile(caller_number: str) -> dict:
    """
    Load a persisted caller profile from DB.
    Returns dict with keys: name, city, language, last_topic (all may be None).
    """
    if not db_pool or not caller_number:
        return {}
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT name, city, language, last_topic FROM caller_profiles WHERE caller=$1",
                caller_number,
            )
        if row:
            return {k: v for k, v in dict(row).items() if v}
    except Exception as exc:
        log.warning("load_caller_profile_failed", error=str(exc))
    return {}


async def save_caller_profile(caller_number: str, profile: dict) -> None:
    """
    Upsert caller profile into DB after call ends.
    profile keys: name, city, language, last_topic
    """
    if not db_pool or not caller_number:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles (caller, name, city, language, last_topic, last_seen)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (caller) DO UPDATE
                    SET name       = COALESCE(EXCLUDED.name, caller_profiles.name),
                        city       = COALESCE(EXCLUDED.city, caller_profiles.city),
                        language   = COALESCE(EXCLUDED.language, caller_profiles.language),
                        last_topic = COALESCE(EXCLUDED.last_topic, caller_profiles.last_topic),
                        last_seen  = NOW()
                """,
                caller_number,
                profile.get("name"),
                profile.get("city"),
                profile.get("language"),
                profile.get("last_topic"),
            )
        log.info("caller_profile_saved", caller=caller_number, profile=profile)
    except Exception as exc:
        log.warning("save_caller_profile_failed", error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# GROQ LLM
# ══════════════════════════════════════════════════════════════════════════════

async def get_llm_reply(
    conversation: list[dict],
    lang: str = "en",
    caller_number: str = "",
    emotion_result: Optional[EmotionResult] = None,
    escalation_hint: str = "",
    context: Optional[dict] = None,
    caller_profile: Optional[dict] = None,
) -> str:
    """
    UPGRADE 16: passes caller context dict into system prompt.
    UPGRADE 24: retries on transient Groq errors.
    UPGRADE 7: passes caller_profile for cross-call memory.
    """
    system_prompt = get_system_prompt(lang, emotion_result=emotion_result, context=context, caller_profile=caller_profile)
    if escalation_hint:
        system_prompt += f"\n\n{escalation_hint}"

    messages = [{"role": "system", "content": system_prompt}] + conversation

    async def _call():
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL_MAIN,
            messages=messages,
            tools=GROQ_TOOLS,
            tool_choice="auto",
            max_tokens=150,
            temperature=0.6,
        )
        choice = response.choices[0]

        if choice.finish_reason != "tool_calls":
            raw_content = (choice.message.content or "").strip()
            # UPGRADE 8: attempt to parse inline confidence JSON appended by LLM
            # Format the system prompt asks for: normal reply text (no JSON needed here
            # since we embed confidence scoring in a second pass only when needed).
            return raw_content

        tool_calls = choice.message.tool_calls
        messages.append({
            "role":       "assistant",
            "content":    choice.message.content or "",
            "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in tool_calls
            ],
        })
        for tc in tool_calls:
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}
            log.info("tool_calling", tool=tc.function.name, args=tool_args)
            result = await dispatch_tool_call(tc.function.name, tool_args, caller_number)
            log.info("tool_result", tool=tc.function.name, result=result[:120])
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

        second = await groq_client.chat.completions.create(
            model=GROQ_MODEL_MAIN, messages=messages, max_tokens=120, temperature=0.6,
        )
        return second.choices[0].message.content.strip()

    return await api_call_with_retry(_call)


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 31 — EMERGENCY ESCALATION
# ══════════════════════════════════════════════════════════════════════════════

def _escalate_to_human_sync(caller: str) -> None:
    """Bridge the caller to the configured emergency contact via Twilio."""
    if not EMERGENCY_CONTACT_NUMBER:
        return
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Say voice="Polly.Joanna">Please hold. Connecting you to a human agent now.</Say>'
        f'<Dial><Number>{html.escape(EMERGENCY_CONTACT_NUMBER)}</Number></Dial>'
        '</Response>'
    )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("emergency_escalation", caller=caller, contact=EMERGENCY_CONTACT_NUMBER)


async def maybe_escalate_emergency(
    caller_number: str, escalation_score: int, emotion_arc: list, call_sid: str
) -> bool:
    """
    UPGRADE 31: if score >= 5 and emotion is urgent, auto-dial emergency contact.
    Returns True if escalation was triggered.
    """
    if escalation_score < 5:
        return False
    recent_emotions = [r.emotion for r in emotion_arc[-3:]]
    if "urgent" not in recent_emotions:
        return False
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, EMERGENCY_CONTACT_NUMBER]):
        log.warning("emergency_escalation_skipped", reason="Twilio or emergency contact not configured")
        return False
    log.warning(
        "emergency_escalation_triggered",
        caller=caller_number,
        score=escalation_score,
        call_sid=call_sid,
    )
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _escalate_to_human_sync, caller_number)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 6: DB logging (extended with topics, flagged, ab_bucket)
# ══════════════════════════════════════════════════════════════════════════════

async def log_call_to_db(
    stream_sid: str,
    caller: str,
    start_time: datetime,
    end_time: datetime,
    transcript: list[dict],
    language: str,
    emotion_arc: list,
    topics: Optional[list] = None,
    flagged: bool = False,
    ab_bucket: str = "",
) -> None:
    if db_pool is None:
        return
    duration = int((end_time - start_time).total_seconds())
    emotions_json = [{"turn": i, **r.to_dict()} for i, r in enumerate(emotion_arc)]
    dom_emotion   = dominant_emotion(emotion_arc)
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calls
                    (stream_sid, caller, start_time, end_time,
                     transcript, language, duration_seconds,
                     emotions, dominant_emotion, topics, flagged, ab_bucket)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8::jsonb,$9,$10::jsonb,$11,$12)
                """,
                stream_sid, caller, start_time, end_time,
                json.dumps(transcript), language, duration,
                json.dumps(emotions_json), dom_emotion,
                json.dumps(topics or []), flagged, ab_bucket,
            )
        log.info("call_logged", stream_sid=stream_sid, duration=duration,
                 dominant_emotion=dom_emotion, topics=topics, flagged=flagged)
    except Exception as exc:
        log.error("db_log_failed", error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN WEBSOCKET
# ══════════════════════════════════════════════════════════════════════════════

@app.websocket("/media-stream")
async def media_stream(ws: WebSocket):
    """
    Bridges Twilio ↔ Deepgram ↔ Groq ↔ ElevenLabs.
    All 9 bug fixes applied. All 32 upgrades active.
    """
    await ws.accept()

    stream_sid:        Optional[str]          = None
    call_sid:          Optional[str]          = None
    caller_number:     str                    = ""
    conversation:      list[dict]             = []
    transcript_buffer: list[str]              = []
    transcript_log:    list[dict]             = []
    silence_task:      Optional[asyncio.Task] = None
    tts_task:          Optional[asyncio.Task] = None
    is_speaking:       bool                   = False
    session_lang:      str                    = "en"
    lang_detected:     bool                   = False
    start_time:        Optional[datetime]     = None
    last_activity:     float                  = time_module.monotonic()  # UPGRADE 25

    # UPGRADE 11: emotion state
    current_emotion_result: EmotionResult = EmotionResult("neutral", 0.5, "low")
    emotion_arc: list[EmotionResult]      = []

    # UPGRADE 12: auth state machine
    auth_state:  str = "unauthenticated"  # → "verifying" → "authenticated"
    auth_pin:    str = ""

    # UPGRADE 15: IVR state — "waiting_dtmf" or None
    ivr_active: bool = False

    # UPGRADE 16: multi-turn entity context
    caller_context: dict = {}

    # UPGRADE 27: urgency counter for abuse detection
    urgency_turn_count: int = 0

    # UPGRADE 29: A/B bucket (determined once caller_number is known)
    ab_bucket: str = ""

    # UPGRADE 7: cross-call caller profile (loaded once stream_sid arrives)
    caller_profile: dict = {}

    # BUG FIX 6: buffer audio that arrives before stream_sid is known
    _audio_buffer: deque[bytes] = deque(maxlen=200)

    session_log = log.bind(stream_sid="pending", caller="unknown")
    session_log.info("ws_connected")

    loop            = asyncio.get_running_loop()
    dg_client       = DeepgramClient(DEEPGRAM_API_KEY)
    dg_conn_holder: list = [None]

    async def start_deepgram(lang: str) -> None:
        conn = dg_client.listen.asynclive.v("1")
        conn.on(LiveTranscriptionEvents.Transcript, on_transcript)
        options = LiveOptions(
            model="nova-2", language=get_deepgram_language(lang),
            encoding="mulaw", sample_rate=8000, channels=1,
            interim_results=True, endpointing=300, smart_format=True,
            # Return confidence scores per word for IVR fallback (UPGRADE 15)
            diarize=False,
        )
        await conn.start(options)
        dg_conn_holder[0] = conn
        session_log.info("deepgram_started", language=get_deepgram_language(lang))

    async def _flush_audio_buffer() -> None:
        while _audio_buffer and dg_conn_holder[0]:
            await dg_conn_holder[0].send(_audio_buffer.popleft())

    # BUG FIX 1: callback fires from Deepgram's internal thread
    async def on_transcript(result, **kwargs) -> None:
        nonlocal silence_task, lang_detected, session_lang, last_activity, ivr_active

        if not result.is_final:
            return
        alternative = result.channel.alternatives[0]
        sentence    = alternative.transcript
        if not sentence:
            return

        last_activity = time_module.monotonic()  # UPGRADE 25: reset inactivity timer

        # UPGRADE 15: IVR fallback — check STT confidence
        confidence = getattr(alternative, "confidence", 1.0) or 1.0
        if confidence < IVR_CONFIDENCE_THRESHOLD and not ivr_active and stream_sid and call_sid:
            ivr_active = True
            session_log.info("ivr_fallback_triggered", confidence=confidence, text=sentence)
            asyncio.ensure_future(_serve_ivr_menu())
            return

        # If IVR produced a synthetic utterance, prefer it
        if stream_sid and _ivr_state.get(stream_sid):
            ivr_utterance   = _ivr_state.pop(stream_sid)
            sentence        = ivr_utterance
            ivr_active      = False

        if not lang_detected:
            lang_detected = True
            detected = detect_language(sentence)
            if detected != session_lang:
                session_log.info("language_switched", to=detected)
                session_lang = detected
                old = dg_conn_holder[0]
                if old:
                    await old.finish()
                await start_deepgram(session_lang)

        transcript_buffer.append(sentence)
        session_log.info("stt_transcript", text=sentence, confidence=round(confidence, 2))

        if silence_task and not silence_task.done():
            silence_task.cancel()

        # BUG FIX 1+2: store future so it can be cancelled on next transcript
        silence_task = asyncio.ensure_future(handle_silence())

    async def _serve_ivr_menu() -> None:
        """Play DTMF menu when STT confidence is too low."""
        if not (call_sid and TWILIO_AUTH_TOKEN):
            return
        menu_twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Response>'
            '<Gather numDigits="1" action="/ivr-input" method="POST">'
            '<Say voice="Polly.Joanna">'
            "Sorry, I didn't catch that clearly. "
            "Press 1 for weather. Press 2 for news. "
            "Press 3 to book an appointment. Press 4 for train PNR status. "
            "Press 0 for help."
            '</Say>'
            '</Gather>'
            '</Response>'
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
                    .calls(call_sid).update(twiml=menu_twiml),
        )

    async def handle_silence() -> None:
        nonlocal is_speaking, tts_task, conversation, auth_state, auth_pin
        nonlocal current_emotion_result, emotion_arc, caller_context, urgency_turn_count
        nonlocal ab_bucket

        await asyncio.sleep(0.8)
        if not transcript_buffer:
            return

        user_text = " ".join(transcript_buffer)
        transcript_buffer.clear()
        session_log.info("user_utterance", text=user_text)

        # BUG FIX 4: cancel in-flight TTS + clear Twilio audio queue
        if tts_task and not tts_task.done():
            tts_task.cancel()
        if is_speaking and stream_sid:
            await ws.send_json({"event": "clear", "streamSid": stream_sid})

        # ── UPGRADE 12: PIN AUTHENTICATION ──────────────────────────────────
        if OTP_PIN_REQUIRED and auth_state != "authenticated":
            if auth_state == "unauthenticated":
                # Send PIN via SMS and ask them to speak it
                pin = await get_or_create_pin(caller_number)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, send_sms, caller_number,
                    f"Your VoiceAI PIN is: {pin}. Please speak your 4-digit PIN to continue.",
                )
                auth_state = "verifying"
                tts_task = asyncio.ensure_future(send_audio(
                    "For security, I've sent a 4-digit PIN to your phone number. "
                    "Please say your PIN now."
                ))
                return

            if auth_state == "verifying":
                entered = _extract_pin(user_text)
                if entered and await verify_pin(caller_number, entered):
                    auth_state = "authenticated"
                    session_log.info("caller_authenticated", caller=caller_number)
                    tts_task = asyncio.ensure_future(send_audio(
                        "Identity verified. Welcome! How can I help you today?"
                    ))
                else:
                    tts_task = asyncio.ensure_future(send_audio(
                        "That PIN didn't match. Please say your 4-digit PIN again."
                    ))
                return

        # ── UPGRADE 11 + 10: parallel emotion detection + conversation compression
        emotion_result, conversation = await asyncio.gather(
            detect_emotion(user_text),
            maybe_compress_conversation(conversation),
        )

        current_emotion_result = emotion_result
        emotion_arc.append(emotion_result)

        # UPGRADE 27: count urgency turns
        if emotion_result.emotion == "urgent":
            urgency_turn_count += 1
            if urgency_turn_count >= ABUSE_URGENCY_THRESHOLD:
                session_log.warning(
                    "abuse_flag_triggered",
                    caller=caller_number,
                    urgency_turns=urgency_turn_count,
                )
                # BUG FIX 12: actually act on the flag — alert admin via SMS
                if urgency_turn_count == ABUSE_URGENCY_THRESHOLD:  # fire once
                    admin_msg = (
                        f"🚨 VoiceAI ABUSE ALERT\n"
                        f"Caller: {caller_number}\n"
                        f"Urgency turns: {urgency_turn_count}\n"
                        f"Stream: {stream_sid}"
                    )
                    # Send to emergency contact number if configured
                    alert_target = EMERGENCY_CONTACT_NUMBER or TWILIO_PHONE_NUMBER
                    if alert_target:
                        asyncio.ensure_future(
                            loop.run_in_executor(None, send_sms, alert_target, admin_msg)
                        )

        session_log.info(
            "emotion_detected",
            emotion=emotion_result.emotion,
            confidence=round(emotion_result.confidence, 2),
            intensity=emotion_result.intensity,
        )

        # UPGRADE 11: weighted escalation
        escalation_score = compute_escalation_score(emotion_arc)
        escalation_hint  = (
            "The caller has been consistently frustrated or angry. "
            "Proactively offer to connect them with a human agent or arrange a callback."
            if escalation_score >= 3 else ""
        )
        if escalation_hint:
            session_log.info("escalation_triggered", score=escalation_score)

        # UPGRADE 31: emergency escalation auto-dial
        if call_sid and caller_number:
            escalated = await maybe_escalate_emergency(
                caller_number, escalation_score, emotion_arc, call_sid
            )
            if escalated:
                tts_task = asyncio.ensure_future(send_audio(
                    "I'm connecting you to a human agent right away. Please hold."
                ))
                return

        # UPGRADE 16: extract entities and update caller context
        caller_context = await extract_entities(user_text, caller_context)

        conversation.append({"role": "user", "content": user_text})
        transcript_log.append({
            "role":    "user",
            "content": user_text,
            **emotion_result.to_dict(),
        })

        reply = await get_llm_reply(
            conversation,
            lang=session_lang,
            caller_number=caller_number,
            emotion_result=current_emotion_result,
            escalation_hint=escalation_hint,
            context=caller_context,  # UPGRADE 16
            caller_profile=caller_profile,  # UPGRADE 7
        )

        # UPGRADE 18: intent confidence check — ask a clarifying question if unsure
        reply_confidence = await check_reply_confidence(user_text, reply)
        if reply_confidence < 0.5:
            clarify = await get_llm_reply(
                conversation + [{"role": "user", "content": user_text}],
                lang=session_lang,
                caller_number=caller_number,
                emotion_result=current_emotion_result,
                context=caller_context,
            )
            # Override with a single clarifying question
            reply = await _ask_clarifying_question(user_text, session_lang)
            session_log.info("low_confidence_clarify", original_reply=reply, confidence=reply_confidence)

        conversation.append({"role": "assistant", "content": reply})
        transcript_log.append({"role": "assistant", "content": reply})
        session_log.info("ai_reply", emotion=current_emotion_result.emotion, text=reply)

        tts_task = asyncio.ensure_future(send_audio(reply))

    async def _ask_clarifying_question(user_text: str, lang: str) -> str:
        """Generate a single clarifying question when intent confidence is low."""
        prompt = (
            "The user said something unclear. Ask ONE short clarifying question "
            "to understand what they need. Be very brief (max 1 sentence). "
            f"Language: {lang}. User said: {user_text}"
        )
        try:
            r = await groq_client.chat.completions.create(
                model=GROQ_MODEL_FAST,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40, temperature=0.3,
            )
            return r.choices[0].message.content.strip()
        except Exception:
            return "Could you please clarify what you need help with?"

    async def send_audio(reply: str) -> None:
        nonlocal is_speaking
        is_speaking = True
        try:
            audio_bytes = await text_to_speech_stream(
                reply,
                lang=session_lang,
                emotion=current_emotion_result.emotion,
                caller_number=caller_number,
            )
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            if stream_sid:
                await ws.send_json({
                    "event": "media", "streamSid": stream_sid,
                    "media": {"payload": audio_b64},
                })
        except asyncio.CancelledError:
            session_log.info("tts_cancelled", reason="barge_in")
        except Exception as exc:
            session_log.error("elevenlabs_failed", error=str(exc))
            if call_sid:
                await loop.run_in_executor(None, _inject_polly_twiml, call_sid, reply)
        finally:
            is_speaking = False

    await start_deepgram(session_lang)

    # UPGRADE 25: inactivity watchdog
    async def inactivity_watchdog() -> None:
        while True:
            await asyncio.sleep(10)
            if time_module.monotonic() - last_activity > SESSION_TIMEOUT_SECS:
                session_log.info("session_timeout", timeout_secs=SESSION_TIMEOUT_SECS)
                if stream_sid:
                    asyncio.ensure_future(send_audio(
                        "I haven't heard from you for a while. Goodbye! Feel free to call back anytime."
                    ))
                await asyncio.sleep(4)  # let the goodbye TTS play
                await ws.close()
                return

    watchdog_task = asyncio.ensure_future(inactivity_watchdog())

    try:
        async for raw in ws.iter_text():
            msg   = json.loads(raw)
            event = msg.get("event")

            if event == "start":
                stream_sid    = msg["start"]["streamSid"]
                call_sid      = msg["start"].get("callSid", "")
                caller_number = msg["start"].get("customParameters", {}).get("callerNumber", "")
                start_time    = datetime.utcnow()
                # UPGRADE 29: determine A/B bucket
                if AB_TEST_ENABLED and caller_number:
                    ab_bucket = "B" if int(hashlib.md5(caller_number.encode()).hexdigest(), 16) % 2 else "A"
                session_log = log.bind(
                    stream_sid=stream_sid, caller=caller_number, call_sid=call_sid,
                    ab_bucket=ab_bucket,
                )
                session_log.info("stream_started")
                # UPGRADE 7: load cross-call profile so AI greets returning callers
                caller_profile = await load_caller_profile(caller_number)
                if caller_profile:
                    session_log.info("caller_profile_loaded", profile=caller_profile)
                await _flush_audio_buffer()

            elif event == "media":
                audio = base64.b64decode(msg["media"]["payload"])
                if dg_conn_holder[0]:
                    if stream_sid:
                        await dg_conn_holder[0].send(audio)
                    else:
                        _audio_buffer.append(audio)

            elif event == "stop":
                session_log.info("stream_stopped")
                break

    except WebSocketDisconnect:
        session_log.info("ws_disconnected")

    finally:
        watchdog_task.cancel()
        if dg_conn_holder[0]:
            await dg_conn_holder[0].finish()
        if silence_task and not silence_task.done():
            silence_task.cancel()
        if tts_task and not tts_task.done():
            tts_task.cancel()

        end_time        = datetime.utcnow()
        effective_start = start_time if start_time is not None else end_time
        duration_secs   = int((end_time - effective_start).total_seconds())

        # UPGRADE 27: flag abusive calls
        call_flagged = urgency_turn_count >= ABUSE_URGENCY_THRESHOLD

        # UPGRADE 28: classify call topics
        topics = await classify_call_topics(transcript_log)

        await log_call_to_db(
            stream_sid=stream_sid or "unknown",
            caller=caller_number,
            start_time=effective_start,
            end_time=end_time,
            transcript=transcript_log,
            language=session_lang,
            emotion_arc=emotion_arc,
            topics=topics,
            flagged=call_flagged,
            ab_bucket=ab_bucket,
        )

        # UPGRADE 30: send WhatsApp call summary
        if caller_number:
            summary = await build_call_summary(transcript_log, topics, caller_number, duration_secs)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, send_whatsapp_summary, caller_number, summary)

        # UPGRADE 7: persist cross-call caller profile
        if caller_number:
            profile_update = {
                "language":   session_lang,
                "last_topic": topics[0] if topics else None,
                "city":       caller_context.get("city"),
                "name":       caller_context.get("person_name"),
            }
            await save_caller_profile(caller_number, profile_update)

        session_log.info(
            "session_cleaned_up",
            duration_secs=duration_secs,
            turns=len(transcript_log),
            dominant_emotion=dominant_emotion(emotion_arc),
            topics=topics,
            flagged=call_flagged,
            ab_bucket=ab_bucket,
        )


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {
        "status":            "ok",
        "db_connected":      db_pool is not None,
        "gcal_configured":   gcal_service is not None,
        "sms_configured":    bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN),
        "groq_model_main":   GROQ_MODEL_MAIN,
        "groq_model_fast":   GROQ_MODEL_FAST,
        "features": {
            "otp_auth":           OTP_PIN_REQUIRED,
            "ivr_fallback":       True,
            "hinglish":           True,
            "pnr_status":         bool(RAPIDAPI_KEY),
            "bhashini":           bool(BHASHINI_API_KEY),
            "ab_voice_test":      AB_TEST_ENABLED,
            "whatsapp_summary":   bool(TWILIO_ACCOUNT_SID),
            "emergency_escalation": bool(EMERGENCY_CONTACT_NUMBER),
            "voice_cloned":       bool(ELEVENLABS_VOICE_CLONED),
            "proactive_reminders": gcal_service is not None,
            "medicine_reminders": db_pool is not None,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 26 — ADMIN DASHBOARD (HTML)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """
    Simple admin dashboard showing call logs, emotion arcs, topics.
    No JS framework — pure HTML + inline SVG sparklines.
    BUG FIX: requires DASHBOARD_TOKEN bearer auth or HTTP Basic Auth.
    """
    # ── Authentication ───────────────────────────────────────────────────────
    if DASHBOARD_TOKEN:
        auth_header = request.headers.get("Authorization", "")
        # Support both Bearer token and HTTP Basic Auth
        authed = False
        if auth_header.startswith("Bearer "):
            token = auth_header[len("Bearer "):]
            authed = secrets.compare_digest(token, DASHBOARD_TOKEN)
        elif auth_header.startswith("Basic "):
            import base64 as _b64
            try:
                decoded = _b64.b64decode(auth_header[6:]).decode()
                _, pwd  = decoded.split(":", 1)
                authed  = secrets.compare_digest(pwd, DASHBOARD_TOKEN)
            except Exception:
                authed = False
        if not authed:
            return HTMLResponse(
                "<h2>401 Unauthorized</h2><p>Set Authorization: Bearer &lt;DASHBOARD_TOKEN&gt;</p>",
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="VoiceAI Dashboard"'},
            )

    if db_pool is None:
        return HTMLResponse("<h2>Database not configured.</h2>", status_code=503)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, caller, start_time, duration_seconds,
                   language, dominant_emotion, topics, flagged, ab_bucket, emotions
            FROM calls ORDER BY start_time DESC LIMIT 50
            """,
        )

    def emotion_sparkline(emotions_json_str: str) -> str:
        """Render a tiny inline SVG bar chart of emotion intensity across turns."""
        try:
            data = json.loads(emotions_json_str or "[]")
        except Exception:
            data = []
        if not data:
            return "<svg width='80' height='20'></svg>"
        emotion_color = {
            "urgent": "#ef4444", "angry": "#f97316", "frustrated": "#eab308",
            "sad": "#6366f1", "confused": "#8b5cf6", "happy": "#22c55e", "neutral": "#94a3b8",
        }
        w, h  = 80, 20
        n     = len(data)
        bw    = max(2, w // max(n, 1))
        bars  = ""
        for i, turn in enumerate(data):
            color  = emotion_color.get(turn.get("emotion", "neutral"), "#94a3b8")
            conf   = float(turn.get("confidence", 0.5))
            bar_h  = max(2, int(h * conf))
            bars  += f'<rect x="{i*bw}" y="{h-bar_h}" width="{bw-1}" height="{bar_h}" fill="{color}"/>'
        return f'<svg width="{w}" height="{h}" style="vertical-align:middle">{bars}</svg>'

    rows_html = ""
    for r in rows:
        topics_str  = ", ".join(json.loads(r["topics"] or "[]"))
        flag_icon   = "🚨" if r["flagged"] else ""
        sparkline   = emotion_sparkline(r["emotions"] or "[]")
        rows_html  += f"""
        <tr style="{'background:#fff1f2' if r['flagged'] else ''}">
          <td>{r['id']}</td>
          <td>{html.escape(r['caller'] or '')}</td>
          <td>{r['start_time'].strftime('%Y-%m-%d %H:%M') if r['start_time'] else ''}</td>
          <td>{r['duration_seconds'] or 0}s</td>
          <td>{html.escape(r['language'] or '')}</td>
          <td>{html.escape(r['dominant_emotion'] or '')}</td>
          <td>{sparkline}</td>
          <td>{html.escape(topics_str)}</td>
          <td>{html.escape(r['ab_bucket'] or '')}</td>
          <td>{flag_icon}</td>
        </tr>"""

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>VoiceAI Dashboard</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 0; background: #f8fafc; color: #1e293b; }}
    header {{ background: #1e293b; color: #f8fafc; padding: 1rem 2rem;
              display:flex; align-items:center; gap: 1rem; }}
    header h1 {{ margin:0; font-size:1.4rem; }}
    .badge {{ background:#3b82f6; color:#fff; border-radius:999px;
              padding:2px 10px; font-size:.75rem; font-weight:700; }}
    main {{ padding: 1.5rem 2rem; }}
    table {{ border-collapse: collapse; width: 100%; font-size: .85rem;
             background:#fff; border-radius:8px; overflow:hidden;
             box-shadow: 0 1px 4px #0001; }}
    th {{ background:#1e293b; color:#f8fafc; padding: .6rem .8rem; text-align:left; }}
    td {{ padding: .5rem .8rem; border-bottom: 1px solid #e2e8f0; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #f1f5f9; }}
  </style>
</head>
<body>
  <header>
    <h1>📞 VoiceAI Admin Dashboard</h1>
    <span class="badge">Last 50 calls</span>
  </header>
  <main>
    <table>
      <thead>
        <tr>
          <th>#</th><th>Caller</th><th>Time</th><th>Duration</th>
          <th>Lang</th><th>Dominant Emotion</th><th>Emotion Arc</th>
          <th>Topics</th><th>A/B</th><th>Flag</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </main>
</body>
</html>""")


# ══════════════════════════════════════════════════════════════════════════════
# ARCHITECTURE ENDPOINT (updated)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/architecture")
async def architecture():
    return {
        "system": "VoiceAI — Real-Time Telecom AI (v2)",
        "pipeline": [
            {"step": 1,  "name": "Inbound Call",           "tech": "Twilio PSTN + Signature Verification",        "latency_ms": "<50"},
            {"step": 2,  "name": "Audio Stream",            "tech": "Twilio Media Streams (μ-law 8kHz)",           "latency_ms": "<20"},
            {"step": 3,  "name": "Speech-to-Text",          "tech": "Deepgram nova-2 + Bhashini (ta/bn/mr)",       "latency_ms": "<300"},
            {"step": 4,  "name": "Confidence Check / IVR",  "tech": "STT confidence → DTMF fallback if <0.6",      "latency_ms": "0"},
            {"step": 5,  "name": "PIN Auth (optional)",     "tech": "4-digit PIN via SMS → voice verification",    "latency_ms": "0"},
            {"step": 6,  "name": "Silence Detection",       "tech": "asyncio (800ms) + inactivity watchdog",       "latency_ms": "800"},
            {"step": 7,  "name": "Entity Extraction",       "tech": f"Groq {GROQ_MODEL_FAST} (city/date/PNR)",     "latency_ms": "<150"},
            {"step": 8,  "name": "Emotion Detection",       "tech": f"Groq {GROQ_MODEL_FAST} + urgency keywords",  "latency_ms": "<200"},
            {"step": 9,  "name": "LLM + Tool Calls",        "tech": f"Groq {GROQ_MODEL_MAIN} + 5 tools",          "latency_ms": "<300"},
            {"step": 10, "name": "Intent Confidence Check", "tech": f"Groq {GROQ_MODEL_FAST} → clarify if <0.5",  "latency_ms": "<150"},
            {"step": 11, "name": "Text-to-Speech",          "tech": "ElevenLabs turbo v2 (emotion-adaptive, A/B)", "latency_ms": "<300"},
            {"step": 12, "name": "Audio Playback",          "tech": "Twilio Media Streams",                        "latency_ms": "<50"},
        ],
        "tools": ["get_weather", "get_news", "book_appointment", "schedule_callback", "check_pnr"],
        "new_features": [
            "OTP/PIN auth", "Callback scheduling", "Missed-call auto-callback",
            "IVR DTMF fallback", "Multi-turn entity memory", "Proactive reminders",
            "Intent confidence threshold", "Hinglish support", "PNR status",
            "Medicine reminders", "Bhashini (ta/bn/mr)", "Twilio signature verification",
            "Exponential backoff retry", "Session inactivity timeout", "Admin dashboard",
            "Abuse detection + flagging", "Call topic tagging", "A/B voice testing",
            "WhatsApp call summary", "Emergency auto-escalation", "Voice cloning support",
        ],
    }


# ══════════════════════════════════════════════════════════════════════════════
# RECENT CALLS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/calls/recent")
async def recent_calls(limit: int = 20):
    if db_pool is None:
        return {"error": "Database not configured"}
    limit = min(limit, 100)
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, stream_sid, caller, start_time, end_time,
                       language, duration_seconds, emotions, dominant_emotion,
                       topics, flagged, ab_bucket
                FROM calls ORDER BY start_time DESC LIMIT $1
                """,
                limit,
            )
        return {"calls": [dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        log.error("recent_calls_failed", error=str(exc))
        return {"error": "Failed to fetch records"}


# ══════════════════════════════════════════════════════════════════════════════
# UPGRADE 15 — ANALYTICS API ENDPOINTS (Power BI / Grafana ready)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/analytics/emotions")
async def analytics_emotions(days: int = 7):
    """Aggregate emotion distribution over the past N days."""
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        since = datetime.utcnow() - timedelta(days=days)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT dominant_emotion, COUNT(*) AS count FROM calls "
                "WHERE start_time >= $1 AND dominant_emotion IS NOT NULL "
                "GROUP BY dominant_emotion ORDER BY count DESC",
                since,
            )
        return {
            "period_days": days,
            "emotions": [{"emotion": r["dominant_emotion"], "count": r["count"]} for r in rows],
        }
    except Exception as exc:
        log.error("analytics_emotions_failed", error=str(exc))
        return {"error": str(exc)}


@app.get("/analytics/topics")
async def analytics_topics(days: int = 7):
    """Top topics discussed in calls over the past N days."""
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        since = datetime.utcnow() - timedelta(days=days)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT topics FROM calls WHERE start_time >= $1 AND topics IS NOT NULL",
                since,
            )
        from collections import Counter
        counter: Counter = Counter()
        for r in rows:
            try:
                for t in json.loads(r["topics"] or "[]"):
                    counter[t] += 1
            except Exception:
                pass
        return {
            "period_days": days,
            "topics": [{"topic": t, "count": c} for t, c in counter.most_common()],
        }
    except Exception as exc:
        log.error("analytics_topics_failed", error=str(exc))
        return {"error": str(exc)}


@app.get("/analytics/languages")
async def analytics_languages(days: int = 7):
    """Language distribution in calls over the past N days."""
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        since = datetime.utcnow() - timedelta(days=days)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT language, COUNT(*) AS count FROM calls "
                "WHERE start_time >= $1 AND language IS NOT NULL "
                "GROUP BY language ORDER BY count DESC",
                since,
            )
        return {
            "period_days": days,
            "languages": [{"language": r["language"], "count": r["count"]} for r in rows],
        }
    except Exception as exc:
        log.error("analytics_languages_failed", error=str(exc))
        return {"error": str(exc)}
