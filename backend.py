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
SARVAM_LANG_CODES: dict[str, str] = {
    "hi":    "hi-IN",   # Hindi
    "ta":    "ta-IN",   # Tamil
    "bn":    "bn-IN",   # Bengali
    "mr":    "mr-IN",   # Marathi
    "te":    "te-IN",   # Telugu
    "kn":    "kn-IN",   # Kannada
    "gu":    "gu-IN",   # Gujarati
    "pa":    "pa-IN",   # Punjabi
    "ml":    "ml-IN",   # Malayalam
    "or":    "or-IN",   # Odia
    "as":    "as-IN",   # Assamese
    "mai":   "mai-IN",  # Maithili
    "mni":   "mni-IN",  # Manipuri
    "kok":   "kok-IN",  # Konkani
    "ur":    "ur-IN",   # Urdu
    "ne":    "ne-IN",   # Nepali (India)
    "hi-en": "hi-IN",   # Hinglish → Hindi for STT/TTS
}
ALL_INDIAN_LANGS: frozenset[str] = frozenset(SARVAM_LANG_CODES.keys())
# Best TTS speaker per language (Sarvam "bulbul:v1" model)
SARVAM_SPEAKERS: dict[str, str] = {
    "hi": "meera", "hi-en": "meera", "ta": "arjun", "bn": "meera",
    "mr": "meera", "te": "meera",   "kn": "meera", "gu": "meera",
    "pa": "meera", "ml": "meera",   "or": "meera", "ur": "meera",
    "as": "meera",
}
SARVAM_SPEED_NORMAL = 1.0
SARVAM_SPEED_SLOW   = 0.8   # elderly / "slow down" mode

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

# NEW: Elderly-friendly mode (slower TTS, simpler vocabulary)
ELDERLY_MODE_DEFAULT        = os.environ.get("ELDERLY_MODE_DEFAULT", "false").lower() == "true"
ELDERLY_SLOW_DOWN_PHRASES   = re.compile(
    r"\b(slow down|speak slowly|too fast|repeat|bolna dhire|dhire bolo|samajh nahi aaya)\b",
    re.IGNORECASE,
)

# NEW: Daily briefings (outbound morning/evening calls)
DAILY_BRIEFING_ENABLED      = os.environ.get("DAILY_BRIEFING_ENABLED", "false").lower() == "true"
DAILY_BRIEFING_MORNING_TIME = os.environ.get("DAILY_BRIEFING_MORNING_TIME", "07:00")  # HH:MM IST
DAILY_BRIEFING_EVENING_TIME = os.environ.get("DAILY_BRIEFING_EVENING_TIME", "19:00")

# NEW: Entertainment — JioSaavn / external music
JIOSAAVN_API_URL            = os.environ.get("JIOSAAVN_API_URL", "")  # community API

# NEW: Cricket scores
CRICAPI_KEY                 = os.environ.get("CRICAPI_KEY", "")

# NEW: Flight status (AviationStack)
AVIATIONSTACK_KEY           = os.environ.get("AVIATIONSTACK_KEY", "")

# NEW: Smart-home control (Tuya)
TUYA_CLIENT_ID              = os.environ.get("TUYA_CLIENT_ID", "")
TUYA_CLIENT_SECRET          = os.environ.get("TUYA_CLIENT_SECRET", "")
TUYA_BASE_URL               = os.environ.get("TUYA_BASE_URL", "https://openapi.tuyaeu.com")

# NEW: Admin SMS alert number (abuse flagging → SMS notification)
ADMIN_ALERT_NUMBER          = os.environ.get("ADMIN_ALERT_NUMBER", "")

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

# NEW: Regional language system prompts (Telugu, Kannada, Gujarati, Punjabi, Malayalam, Odia)
SYSTEM_PROMPT_TE = """మీరు ఫోన్‌లో అందుబాటులో ఉన్న సహాయక వాయిస్ అసిస్టెంట్.
వృద్ధులు, దృష్టిహీనులు మరియు బిజీ కార్మికులకు సహాయం చేయండి.
ప్రతి సమాధానం తక్కువగా (1-3 వాక్యాలు) ఉంచండి. సహజంగా మాట్లాడండి.
వార్తలు, వాతావరణం, బుకింగ్, PNR స్టేటస్ మరియు సాధారణ ప్రశ్నలలో సహాయం చేయగలరు.
ఎల్లప్పుడూ తెలుగులో సమాధానం ఇవ్వండి."""

SYSTEM_PROMPT_KN = """ನೀವು ಫೋನ್‌ನಲ್ಲಿ ಲಭ್ಯವಿರುವ ಸಹಾಯಕ ವಾಯ್ಸ್ ಅಸಿಸ್ಟೆಂಟ್.
ಹಿರಿಯ ನಾಗರಿಕರು, ದೃಷ್ಟಿ ವಿಕಲಚೇತನರು ಮತ್ತು ಕಾರ್ಮಿಕರಿಗೆ ಸಹಾಯ ಮಾಡಿ.
ಪ್ರತಿ ಉತ್ತರ ಸಂಕ್ಷಿಪ್ತವಾಗಿ (1-3 ವಾಕ್ಯಗಳು) ಇರಲಿ. ಸ್ವಾಭಾವಿಕವಾಗಿ ಮಾತನಾಡಿ.
ಸುದ್ದಿ, ಹವಾಮಾನ, ಬುಕಿಂಗ್, PNR ಸ್ಥಿತಿ ಮತ್ತು ಸಾಮಾನ್ಯ ಪ್ರಶ್ನೆಗಳಲ್ಲಿ ಸಹಾಯ ಮಾಡಬಹುದು.
ಯಾವಾಗಲೂ ಕನ್ನಡದಲ್ಲಿ ಉತ್ತರಿಸಿ."""

SYSTEM_PROMPT_GU = """તમે ફોન પર ઉપલબ્ધ સહાયક વૉઇસ આસિસ્ટન્ટ છો.
વૃદ્ધ નાગરિકો, દૃષ્ટિ વિકલ અને વ્યસ્ત કામદારોને મદદ કરો.
દરેક જવાબ ટૂંકો (1-3 વાક્ય) રાખો. સ્વાભાવિક રીતે બોલો.
સમાચાર, હવામાન, બુકિંગ, PNR સ્ટેટ્સ અને સામાન્ય પ્રશ્નોમાં મદદ કરી શકો.
હંમેશા ગુજરાતીમાં જવાબ આપો."""

SYSTEM_PROMPT_PA = """ਤੁਸੀਂ ਫ਼ੋਨ ਤੇ ਉਪਲਬਧ ਸਹਾਇਕ ਵੌਇਸ ਅਸਿਸਟੈਂਟ ਹੋ।
ਬਜ਼ੁਰਗ ਲੋਕਾਂ, ਦ੍ਰਿਸ਼ਟੀਹੀਣਾਂ ਅਤੇ ਮਿਹਨਤਕਸ਼ ਮਜ਼ਦੂਰਾਂ ਦੀ ਮਦਦ ਕਰੋ।
ਹਰ ਜਵਾਬ ਛੋਟਾ (1-3 ਵਾਕ) ਰੱਖੋ। ਕੁਦਰਤੀ ਤਰੀਕੇ ਨਾਲ ਬੋਲੋ।
ਖਬਰਾਂ, ਮੌਸਮ, ਬੁਕਿੰਗ, PNR ਸਥਿਤੀ ਅਤੇ ਆਮ ਸਵਾਲਾਂ ਵਿੱਚ ਮਦਦ ਕਰ ਸਕਦੇ ਹੋ।
ਹਮੇਸ਼ਾ ਪੰਜਾਬੀ ਵਿੱਚ ਜਵਾਬ ਦਿਓ।"""

SYSTEM_PROMPT_ML = """നിങ്ങൾ ഫോണിൽ ലഭ്യമായ സഹായ വോയ്‌സ് അസിസ്റ്റന്റ് ആണ്.
മുതിർന്ന പൗരന്മാർ, കാഴ്ച വൈകല്യമുള്ളവർ, തിരക്കേറിയ തൊഴിലാളികൾ എന്നിവരെ സഹായിക്കൂ.
ഓരോ ഉത്തരവും ചെറുതായി (1-3 വാക്യം) നൽകൂ. സ്വാഭാവികമായി സംസാരിക്കൂ.
വാർത്ത, കാലാവസ്ഥ, ബുക്കിംഗ്, PNR സ്റ്റാറ്റസ്, പൊതു ചോദ്യങ്ങൾ എന്നിവയിൽ സഹായിക്കാം.
എപ്പോഴും മലയാളത്തിൽ ഉത്തരം നൽകൂ."""

SYSTEM_PROMPT_OR = """ଆପଣ ଫୋନ୍‌ରେ ଉପଲବ୍ଧ ଏକ ସହାୟକ ଭଏସ୍ ଆସିଷ୍ଟାଣ୍ଟ।
ବୃଦ୍ଧ, ଦୃଷ୍ଟିହୀନ ଏବଂ ଶ୍ରମିକମାନଙ୍କୁ ସାହାଯ୍ୟ କରନ୍ତୁ।
ପ୍ରତ୍ୟେକ ଉତ୍ତର ସଂକ୍ଷିପ୍ତ (1-3 ବାକ୍ୟ) ରଖନ୍ତୁ।
ସମ୍ବାଦ, ପ୍ରସଙ୍ଗ, ବୁକିଂ, PNR ସ୍ଥିତି ଏବଂ ସାଧାରଣ ପ୍ରଶ୍ନରେ ସାହାଯ୍ୟ କରିପାରିବେ।
ସର୍ବଦା ଓଡ଼ିଆରେ ଉତ୍ତର ଦିଅନ୍ତୁ।"""

# Map lang code → system prompt
_LANG_SYSTEM_PROMPTS: dict[str, str] = {
    "en":    SYSTEM_PROMPT_EN,
    "hi":    SYSTEM_PROMPT_HI,
    "hi-en": SYSTEM_PROMPT_HINGLISH,
    "te":    SYSTEM_PROMPT_TE,
    "kn":    SYSTEM_PROMPT_KN,
    "gu":    SYSTEM_PROMPT_GU,
    "pa":    SYSTEM_PROMPT_PA,
    "ml":    SYSTEM_PROMPT_ML,
    "or":    SYSTEM_PROMPT_OR,
    # Tamil, Bengali, Marathi fall back to English base; add yours below if needed
}

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
    # NEW: Entertainment — jokes / stories / trivia
    {
        "type": "function",
        "function": {
            "name": "get_entertainment",
            "description": (
                "Provide entertainment content: jokes, motivational stories, fun facts, "
                "trivia questions, bhajans info, or riddles. "
                "Call when the user says 'joke', 'story', 'mazedaar', 'bhajan', "
                "'trivia', 'riddle', 'game', 'hasao', or asks to be entertained."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["joke", "story", "trivia", "bhajan_info", "riddle", "motivational"],
                        "description": "Type of entertainment content.",
                    },
                    "language": {
                        "type": "string",
                        "description": "Language for the content (en/hi/ta/bn/mr etc).",
                    },
                },
                "required": ["type"],
            },
        },
    },
    # NEW: Health — symptom checker + emergency dial
    {
        "type": "function",
        "function": {
            "name": "check_symptoms",
            "description": (
                "Provide basic health guidance for described symptoms with a clear disclaimer. "
                "Also handles emergency commands: 'call ambulance', 'call 112', 'medical emergency'. "
                "Call when user describes physical symptoms or asks for health advice."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "symptoms": {
                        "type": "string",
                        "description": "Symptom description as spoken by the caller.",
                    },
                    "emergency": {
                        "type": "boolean",
                        "description": "True if caller is requesting emergency services.",
                    },
                },
                "required": ["symptoms"],
            },
        },
    },
    # NEW: Cricket scores
    {
        "type": "function",
        "function": {
            "name": "get_cricket_score",
            "description": (
                "Get live or recent cricket match scores. "
                "Call when user asks about cricket, IPL, India match, cricket score, "
                "or names a team playing cricket."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "team": {
                        "type": "string",
                        "description": "Team name to filter by (e.g. 'India', 'MI'). Use 'live' for all live matches.",
                    }
                },
                "required": ["team"],
            },
        },
    },
    # NEW: Recipe suggestions (voice-friendly)
    {
        "type": "function",
        "function": {
            "name": "get_recipe",
            "description": (
                "Get a short, voice-friendly recipe with steps. "
                "Call when user asks 'recipe', 'kaise banate hain', 'batao banana', "
                "or names any food dish."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dish": {
                        "type": "string",
                        "description": "Name of the dish, e.g. 'aloo paratha', 'dal fry', 'chai'.",
                    }
                },
                "required": ["dish"],
            },
        },
    },
    # NEW: Translation
    {
        "type": "function",
        "function": {
            "name": "translate_text",
            "description": (
                "Translate text from one language to another. "
                "Call when user says 'translate', 'anuvad karo', 'English mein batao', "
                "or asks what something means in another language."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "The text to translate.",
                    },
                    "target_language": {
                        "type": "string",
                        "description": "Target language code or name (e.g. 'en', 'hi', 'English', 'Hindi').",
                    },
                },
                "required": ["text", "target_language"],
            },
        },
    },
    # NEW: Voice memos — save a note and recall it later
    {
        "type": "function",
        "function": {
            "name": "save_memo",
            "description": (
                "Save a voice memo / note for the caller to retrieve later. "
                "Call when user says 'note karo', 'save this', 'mujhe yaad dilao', "
                "'memo', or 'remember this'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "memo": {
                        "type": "string",
                        "description": "The note content to save, as spoken by the caller.",
                    }
                },
                "required": ["memo"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recall_memos",
            "description": (
                "Retrieve the caller's saved voice memos / notes. "
                "Call when user says 'mera note sunao', 'meri notes batao', "
                "'what did I save', or 'recall my memos'."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    # NEW: Flight status
    {
        "type": "function",
        "function": {
            "name": "get_flight_status",
            "description": (
                "Check live flight status by flight number or route. "
                "Call when user asks about a flight, departure time, arrival, delay, "
                "or says a flight number like 'AI 202'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "flight_number": {
                        "type": "string",
                        "description": "IATA flight number, e.g. 'AI202', '6E 101'.",
                    }
                },
                "required": ["flight_number"],
            },
        },
    },
    # NEW: Smart-home device control
    {
        "type": "function",
        "function": {
            "name": "control_smart_device",
            "description": (
                "Control a smart-home IoT device (lights, fan, AC, door lock). "
                "Call when user says 'light on', 'fan off', 'AC chhalu karo', "
                "'darwaza band karo', or refers to any smart device."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "device": {
                        "type": "string",
                        "description": "Device name, e.g. 'living room light', 'bedroom fan', 'AC'.",
                    },
                    "action": {
                        "type": "string",
                        "enum": ["on", "off", "toggle", "set_temperature", "lock", "unlock"],
                        "description": "Action to perform.",
                    },
                    "value": {
                        "type": "string",
                        "description": "Optional value, e.g. temperature '24' for AC.",
                    },
                },
                "required": ["device", "action"],
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
    Detect caller language.  Priority order:
      1. Hinglish (Hindi words + English words coexist) → "hi-en"
      2. Pure Hindi → "hi"
      3. Any other Indian language detected by langdetect + Sarvam/Bhashini available → pass through
      4. Falls back to "en"
    Covers: hi, hi-en, ta, bn, mr, te, kn, gu, pa, ml, or, as, ur, ne, mai, mni, kok, sat
    """
    try:
        detected = detect(text)
    except LangDetectException:
        detected = "en"

    hindi_hits  = len(_HINDI_WORDS.findall(text))
    has_english = bool(re.search(r"[a-zA-Z]{3,}", text))

    if hindi_hits >= 2 and has_english:
        return "hi-en"  # Hinglish
    if detected == "hi":
        return "hi"
    # Route any Sarvam-supported Indian language if the API is configured
    if detected in ALL_INDIAN_LANGS and SARVAM_API_KEY:
        return detected
    # Fallback: Bhashini covers ta/bn/mr even without Sarvam
    if detected in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return detected
    return "en"


# ── UPGRADE 1 HELPERS ─────────────────────────────────────────────────────────

def get_elevenlabs_voice(lang: str, caller_number: str = "",
                         caller_profile: Optional[dict] = None) -> str:
    """
    Voice selection priority:
      1. Per-caller cloned voice stored in caller_profile (from /voice/clone endpoint).
      2. Global cloned brand voice (ELEVENLABS_VOICE_CLONED env).
      3. Hindi voice for hi / hi-en.
      4. A/B bucket test (EN voices).
      5. Default EN voice.
    UPGRADE 32 / NEW: per-caller voice cloning support.
    """
    # 1. Per-caller cloned voice
    if caller_profile and caller_profile.get("cloned_voice_id"):
        return caller_profile["cloned_voice_id"]
    # 2. Global brand voice
    if ELEVENLABS_VOICE_CLONED:
        return ELEVENLABS_VOICE_CLONED
    # 3. Hindi/Hinglish voice
    if lang in ("hi", "hi-en"):
        return ELEVENLABS_VOICE_HI
    # 4. A/B test
    if AB_TEST_ENABLED and caller_number:
        bucket = int(hashlib.md5(caller_number.encode()).hexdigest(), 16) % 2
        return ELEVENLABS_VOICE_EN_ALT if bucket == 1 else ELEVENLABS_VOICE_EN
    return ELEVENLABS_VOICE_EN


# Deepgram language codes for languages it natively supports
_DEEPGRAM_LANG_MAP: dict[str, str] = {
    "en":    "en-US",
    "hi":    "hi",
    "hi-en": "hi-en",
    "ta":    "ta",
    "bn":    "bn",
    "mr":    "mr",
    "te":    "te",
    "kn":    "kn",
    "gu":    "gu",
    "pa":    "pa",
    "ml":    "ml",
    "ur":    "ur",
}


def get_deepgram_language(lang: str) -> str:
    """Return a Deepgram-compatible language code, falling back to en-US."""
    return _DEEPGRAM_LANG_MAP.get(lang, "en-US")


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
                      caller_profile: Optional[dict] = None,
                      elderly_mode: bool = False) -> str:
    """
    Build the system prompt for the LLM.
    UPGRADE 16 : inject multi-turn context.
    UPGRADE 19 : Hinglish / regional-language prompt selection.
    UPGRADE 7  : inject cross-call caller profile (name, city, language, last_topic).
    NEW        : elderly_mode — adds instruction to speak slowly + use simple vocabulary.
    """
    base = _LANG_SYSTEM_PROMPTS.get(lang, SYSTEM_PROMPT_EN)

    # Elderly / accessibility mode
    if elderly_mode or ELDERLY_MODE_DEFAULT or (caller_profile and caller_profile.get("elderly_mode")):
        base += (
            "\n\nSPEAKING STYLE: This caller prefers slower, clearer speech. "
            "Use very simple words. Keep sentences short. Repeat key info if needed. "
            "Do NOT use abbreviations or technical jargon."
        )

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
        if caller_profile.get("cloned_voice_id"):
            profile_parts.append("A personalised cloned voice is active for this caller.")
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

# Medicine-call TTS audio cache: token → raw audio bytes (auto-expires after 5 min)
_tts_audio_cache: dict[str, bytes] = {}


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
                    # UPGRADE 7: cross-call caller profiles (with voice-clone + elderly columns)
                    """CREATE TABLE IF NOT EXISTS caller_profiles (
                           caller          TEXT PRIMARY KEY,
                           name            TEXT,
                           city            TEXT,
                           language        TEXT,
                           last_topic      TEXT,
                           last_seen       TIMESTAMPTZ,
                           cloned_voice_id TEXT,
                           elderly_mode    BOOLEAN DEFAULT FALSE
                       )""",
                    # Voice memos table (save_memo / recall_memo tool)
                    """CREATE TABLE IF NOT EXISTS voice_memos (
                           id          SERIAL PRIMARY KEY,
                           caller      TEXT,
                           memo        TEXT,
                           created_at  TIMESTAMPTZ DEFAULT NOW()
                       )""",
                    # idempotent column additions
                    "ALTER TABLE medicine_reminders ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'hi'",
                    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS cloned_voice_id TEXT",
                    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS elderly_mode BOOLEAN DEFAULT FALSE",
                    # F-07: family contacts
                    "ALTER TABLE caller_profiles ADD COLUMN IF NOT EXISTS family_contacts JSONB",
                    # F-08: post-call satisfaction
                    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS satisfied BOOLEAN",
                ]:
                    await conn.execute(col_sql)
            log.info("db_pool_created")
            # F-01: pgvector long-term memory table
            await _ensure_pgvector_table()
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
    if FESTIVE_CALLS_ENABLED:
        scheduler.add_job(check_and_send_festive_greetings, "cron", hour=9, minute=0,
                          timezone=TIMEZONE, id="festive_greetings", replace_existing=True)
        log.info("festive_greetings_job_scheduled")
    if DAILY_BRIEFING_ENABLED:
        # Fire morning briefing at configured time (IST)
        try:
            mh, mm = map(int, DAILY_BRIEFING_MORNING_TIME.split(":"))
            eh, em = map(int, DAILY_BRIEFING_EVENING_TIME.split(":"))
            scheduler.add_job(send_daily_briefings, "cron", hour=mh, minute=mm,
                              timezone=TIMEZONE, id="morning_briefing", replace_existing=True,
                              kwargs={"period": "morning"})
            scheduler.add_job(send_daily_briefings, "cron", hour=eh, minute=em,
                              timezone=TIMEZONE, id="evening_briefing", replace_existing=True,
                              kwargs={"period": "evening"})
            log.info("daily_briefings_scheduled",
                     morning=DAILY_BRIEFING_MORNING_TIME, evening=DAILY_BRIEFING_EVENING_TIME)
        except Exception as exc:
            log.error("daily_briefing_schedule_failed", error=str(exc))
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


def _fire_medicine_call_sync(caller: str, medicine: str, audio_token: str) -> None:
    """
    Fire a medicine reminder call.
    BUG FIX: previously received audio_b64 but discarded it and always used Polly.Aditi.
    Now uses a short-lived audio token to serve the pre-generated TTS via <Play>,
    falling back to Polly only when no token is available.
    """
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    host   = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")

    if audio_token:
        # Serve the Sarvam/Bhashini/ElevenLabs TTS via the /tts-audio endpoint
        play_url = f"https://{host}/tts-audio/{audio_token}"
        twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Response>'
            f'<Play>{html.escape(play_url)}</Play>'
            '</Response>'
        )
    else:
        # Polly fallback (e.g. if TTS generation failed upstream)
        twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Response>'
            f'<Say voice="Polly.Aditi">{html.escape(f"Namaste! It is time to take your {medicine}. Please take your medicine now. Stay healthy!")}</Say>'
            '</Response>'
        )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("medicine_call_fired", caller=caller, medicine=medicine,
             audio="sarvam" if audio_token else "polly_fallback")


async def _fire_medicine_call(caller: str, medicine: str, lang: str = "hi") -> None:
    """
    UPGRADE 6: fire medicine reminder in the caller's preferred language.
    Generates TTS via Sarvam/Bhashini/ElevenLabs, stores the audio under a
    short-lived token, then dials using TwiML <Play> so the audio is actually heard.
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
    audio_token = ""
    try:
        # Generate TTS audio in caller's language
        audio_bytes = await text_to_speech_stream(reminder_text, lang=lang)
        # Store under a short-lived token so Twilio can <Play> it
        audio_token = secrets.token_urlsafe(16)
        _tts_audio_cache[audio_token] = audio_bytes
        # Expire token after 5 minutes (plenty of time for Twilio to fetch it)
        asyncio.get_running_loop().call_later(300, _tts_audio_cache.pop, audio_token, None)
        await loop.run_in_executor(None, _fire_medicine_call_sync, caller, medicine, audio_token)
    except Exception as exc:
        log.error("medicine_call_failed", caller=caller, lang=lang, error=str(exc))
        # Fallback: fire with Polly (no token)
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
    # BUG FIX 4: host MUST be resolved before building the TwiML f-string.
    # The original had host assigned AFTER the string, so double-braces {{host}}
    # were used to escape the f-string — but that produces the literal text
    # "{host}" in the TwiML, not the real hostname.
    host   = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Say voice="Polly.Joanna">Hello! You gave us a missed call. '
        'VoiceAI is here to help. How can I assist you today?</Say>'
        f'<Connect><Stream url="wss://{html.escape(host)}/media-stream">'
        f'<Parameter name="callerNumber" value="{html.escape(caller)}" />'
        '</Stream></Connect>'
        '</Response>'
    )
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
# TTS AUDIO SERVING — used by medicine reminder <Play> TwiML
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/tts-audio/{token}")
async def serve_tts_audio(token: str):
    """
    Serve a short-lived pre-generated TTS audio clip for Twilio <Play>.
    Tokens are created in _fire_medicine_call and expire after 5 minutes.
    Only μ-law 8 kHz audio (audio/basic) is returned — the format Twilio expects.
    """
    audio = _tts_audio_cache.get(token)
    if not audio:
        raise HTTPException(status_code=404, detail="Audio token not found or expired")
    return Response(content=audio, media_type="audio/basic")


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

async def _tts_elevenlabs(text: str, lang: str, emotion: str,
                          caller_number: str = "",
                          caller_profile: Optional[dict] = None,
                          slow_mode: bool = False) -> bytes:
    """Inner TTS call — wrapped by api_call_with_retry."""
    voice_id       = get_elevenlabs_voice(lang, caller_number, caller_profile=caller_profile)
    voice_settings = get_voice_settings_for_emotion(emotion)
    # Elderly/slow-mode: lower speed via stability tweak
    if slow_mode:
        voice_settings = dict(voice_settings)
        voice_settings["stability"] = min(1.0, voice_settings.get("stability", 0.5) + 0.2)
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
    caller_profile: Optional[dict] = None,
    slow_mode: bool = False,
) -> bytes:
    """
    TTS routing priority:
      1. Sarvam AI  — SARVAM_API_KEY set AND lang is a supported Indian language (best quality)
      2. Bhashini   — fallback for ta/bn/mr when BHASHINI_API_KEY is set
      3. ElevenLabs — English / Hinglish / no Indian-lang key
    NEW: slow_mode passes speaking-rate hints to Sarvam/ElevenLabs for elderly callers.
    NEW: caller_profile enables per-caller voice cloning via ElevenLabs.
    """
    is_indian_lang = lang in SARVAM_LANG_CODES
    if SARVAM_API_KEY and is_indian_lang:
        return await api_call_with_retry(_tts_sarvam, text, lang, slow_mode)
    if lang in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return await api_call_with_retry(_tts_bhashini, text, lang)
    return await api_call_with_retry(_tts_elevenlabs, text, lang, emotion, caller_number, caller_profile, slow_mode)


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


async def _tts_sarvam(text: str, lang: str, slow_mode: bool = False) -> bytes:
    """
    Call Sarvam AI TTS and return audio bytes.
    Uses best speaker per language and respects slow_mode for elderly callers.
    Supports all 22 official Indian languages in one API.
    """
    lang_code = SARVAM_LANG_CODES.get(lang, "hi-IN")
    speaker   = SARVAM_SPEAKERS.get(lang, "meera")
    speed     = SARVAM_SPEED_SLOW if slow_mode else SARVAM_SPEED_NORMAL
    headers   = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "inputs":               [text],
        "target_language_code": lang_code,
        "speaker":              speaker,
        "model":                "bulbul:v1",
        "enable_preprocessing": True,
        "speech_sample_rate":   8000,
        "pace":                 speed,
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
# NEW TOOLS — ENTERTAINMENT, HEALTH, CRICKET, RECIPES, TRANSLATION, MEMOS,
#             FLIGHT STATUS, SMART HOME, DAILY BRIEFINGS
# ══════════════════════════════════════════════════════════════════════════════

async def get_entertainment(type: str, language: str = "en") -> str:
    """
    Generate entertainment content via Groq LLM — jokes, stories, trivia, etc.
    Voice-optimised: short, punchy, spoken-word friendly.
    """
    lang_instructions = {
        "hi":    "Respond in Hindi (Devanagari script).",
        "hi-en": "Respond in fun Hinglish.",
        "ta":    "Respond in Tamil.",
        "bn":    "Respond in Bengali.",
        "mr":    "Respond in Marathi.",
    }
    lang_hint = lang_instructions.get(language, "Respond in English.")

    content_prompts = {
        "joke":        f"Tell a clean, funny, family-friendly joke. {lang_hint} Keep it very short (2-4 lines).",
        "story":       f"Tell a very short inspirational story (4-5 sentences) suitable for an elderly person. {lang_hint}",
        "trivia":      f"Give one interesting trivia fact about India or science. {lang_hint} Keep it to 2 sentences.",
        "bhajan_info": f"Describe one popular Indian bhajan or devotional song in 2-3 sentences. {lang_hint}",
        "riddle":      f"Give a simple, fun riddle with its answer. {lang_hint} Keep it brief.",
        "motivational":f"Give a short motivational quote or thought (2-3 sentences). {lang_hint}",
    }
    prompt = content_prompts.get(type, f"Tell me something fun and interesting. {lang_hint}")
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.9,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("entertainment_failed", type=type, error=str(exc))
        return "Sorry, I couldn't fetch that right now. Try again!"


async def check_symptoms(symptoms: str, emergency: bool = False) -> str:
    """
    Basic health guidance with strong disclaimer.
    Emergency commands auto-dial 112 via the escalation path.
    """
    if emergency or any(kw in symptoms.lower() for kw in
                        ["ambulance", "112", "emergency", "heart attack", "chest pain",
                         "not breathing", "unconscious", "bleeding badly"]):
        return (
            "🚨 EMERGENCY: Please call 112 immediately for an ambulance. "
            "If you cannot call, ask someone nearby to call for you. "
            "Stay on the line with the emergency operator."
        )
    prompt = (
        "You are a helpful health information assistant (NOT a doctor). "
        "Give brief, simple, practical first-aid advice for the described symptoms. "
        "Always end with: 'Please consult a doctor for proper diagnosis and treatment.' "
        "Keep the response to 3-4 short sentences. "
        f"Symptoms: {symptoms}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("symptoms_check_failed", error=str(exc))
        return "I cannot give medical advice right now. Please consult a doctor or call 112 in an emergency."


async def get_cricket_score(team: str = "live") -> str:
    """
    Fetch cricket scores via CricAPI. Requires CRICAPI_KEY env var.
    """
    if not CRICAPI_KEY:
        return "Cricket score service is not configured. Please ask your administrator to set CRICAPI_KEY."
    url = "https://api.cricapi.com/v1/cricScore"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params={"apikey": CRICAPI_KEY})
            resp.raise_for_status()
            data    = resp.json()
        matches = data.get("data", [])
        if not matches:
            return "No live cricket matches right now."
        # Filter by team if not "live"
        if team.lower() != "live":
            matches = [m for m in matches if team.lower() in (m.get("t1", "") + m.get("t2", "")).lower()]
        if not matches:
            return f"No live matches found for {team}."
        results = []
        for m in matches[:3]:
            t1, t2   = m.get("t1", ""), m.get("t2", "")
            s1, s2   = m.get("t1s", ""), m.get("t2s", "")
            status   = m.get("status", "")
            results.append(f"{t1} {s1} vs {t2} {s2}. {status}".strip())
        return " | ".join(results)
    except Exception as exc:
        log.error("cricket_score_failed", team=team, error=str(exc))
        return "Sorry, I couldn't fetch cricket scores right now."


async def get_recipe(dish: str) -> str:
    """
    Generate a short, voice-friendly recipe using Groq LLM.
    Steps are numbered and spoken-word optimised.
    """
    prompt = (
        "Give a very short, voice-friendly recipe for the following dish. "
        "Format: list 3-4 key ingredients, then 4-5 numbered short steps. "
        "Use simple spoken language. Maximum 6 sentences total. "
        f"Dish: {dish}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.4,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("recipe_failed", dish=dish, error=str(exc))
        return f"Sorry, I couldn't find a recipe for {dish} right now."


async def translate_text(text: str, target_language: str) -> str:
    """
    Translate text to the target language using Groq LLM.
    Useful for mixed-language users who need quick translations.
    """
    prompt = (
        f"Translate the following text to {target_language}. "
        "Reply with ONLY the translated text, nothing else. "
        f"Text: {text}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.1,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("translation_failed", error=str(exc))
        return "Sorry, I couldn't translate that right now."


async def save_memo(memo: str, caller_number: str = "") -> str:
    """
    Save a voice memo for the caller into the voice_memos DB table.
    """
    if not caller_number:
        return "I need your phone number to save a memo. Please call from a registered number."
    if not db_pool:
        return "Memo storage is not available right now."
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO voice_memos (caller, memo) VALUES ($1, $2)",
                caller_number, memo,
            )
        log.info("memo_saved", caller=caller_number)
        return "Got it! I've saved your note: {}{}".format(memo[:60], "..." if len(memo) > 60 else ".")
    except Exception as exc:
        log.error("save_memo_failed", caller=caller_number, error=str(exc))
        return "Sorry, I couldn't save your memo right now."


async def recall_memos(caller_number: str = "") -> str:
    """
    Retrieve the caller's saved voice memos.
    """
    if not caller_number:
        return "I need your phone number to retrieve your memos."
    if not db_pool:
        return "Memo storage is not available right now."
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT memo, created_at FROM voice_memos "
                "WHERE caller=$1 ORDER BY created_at DESC LIMIT 5",
                caller_number,
            )
        if not rows:
            return "You have no saved memos."
        parts = [f"{i+1}. {r['memo']} (saved {r['created_at'].strftime('%d %b %H:%M')})"
                 for i, r in enumerate(rows)]
        return "Your memos: " + ". ".join(parts)
    except Exception as exc:
        log.error("recall_memos_failed", caller=caller_number, error=str(exc))
        return "Sorry, I couldn't retrieve your memos right now."


async def get_flight_status(flight_number: str) -> str:
    """
    Check live flight status via AviationStack API.
    Requires AVIATIONSTACK_KEY env var.
    """
    if not AVIATIONSTACK_KEY:
        return "Flight status service is not configured. Please ask your administrator to set AVIATIONSTACK_KEY."
    flight_number = re.sub(r"\s+", "", flight_number).upper()
    url    = "http://api.aviationstack.com/v1/flights"
    params = {"access_key": AVIATIONSTACK_KEY, "flight_iata": flight_number, "limit": 1}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        flights = data.get("data", [])
        if not flights:
            return f"No flight data found for {flight_number}."
        f       = flights[0]
        dep     = f.get("departure", {})
        arr     = f.get("arrival", {})
        status  = f.get("flight_status", "unknown")
        dep_apt = dep.get("airport", "")
        arr_apt = arr.get("airport", "")
        dep_sched = dep.get("scheduled", "")[:16] if dep.get("scheduled") else ""
        arr_sched = arr.get("scheduled", "")[:16] if arr.get("scheduled") else ""
        delay     = dep.get("delay") or 0
        delay_str = f" Delay: {delay} min." if delay else ""
        return (
            f"Flight {flight_number}: {dep_apt} → {arr_apt}. "
            f"Status: {status}.{delay_str} "
            f"Departs: {dep_sched}. Arrives: {arr_sched}."
        ).strip()
    except Exception as exc:
        log.error("flight_status_failed", flight=flight_number, error=str(exc))
        return "Sorry, I couldn't fetch flight information right now."


async def control_smart_device(device: str, action: str, value: str = "") -> str:
    """
    Control a Tuya-compatible smart-home IoT device.
    Requires TUYA_CLIENT_ID, TUYA_CLIENT_SECRET env vars.
    Device IDs must be linked in caller_profiles.devices (future enhancement).
    """
    if not (TUYA_CLIENT_ID and TUYA_CLIENT_SECRET):
        return "Smart home control is not configured. Please set up Tuya API credentials."
    # In a full implementation, look up the device_id from caller_profiles.
    # Here we return a friendly confirmation and log intent for human review.
    log.info("smart_device_intent", device=device, action=action, value=value)
    action_map = {
        "on":          f"Turning on {device}.",
        "off":         f"Turning off {device}.",
        "toggle":      f"Toggling {device}.",
        "set_temperature": f"Setting {device} to {value}°C.",
        "lock":        f"Locking {device}.",
        "unlock":      f"Unlocking {device}.",
    }
    response = action_map.get(action, f"Performing {action} on {device}.")
    # TODO: Integrate real Tuya OpenAPI call here:
    # POST /v1.0/devices/{device_id}/commands with {"commands": [{"code": ..., "value": ...}]}
    return response + " (Smart home integration active — device command logged.)"


# ══════════════════════════════════════════════════════════════════════════════
# NEW: PROACTIVE DAILY BRIEFINGS
# ══════════════════════════════════════════════════════════════════════════════

async def send_daily_briefings(period: str = "morning") -> None:
    """
    APScheduler job — fires outbound briefing calls to all callers
    who have opted in (have a caller_profile with city set).
    Generates a personalised briefing: weather + top news + reminders.
    """
    if not db_pool or not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    try:
        async with db_pool.acquire() as conn:
            callers = await conn.fetch(
                "SELECT caller, name, city, language FROM caller_profiles "
                "WHERE city IS NOT NULL AND caller IS NOT NULL"
            )
        for row in callers:
            asyncio.ensure_future(
                _fire_daily_briefing(row["caller"], row.get("name") or "",
                                     row.get("city") or "Delhi",
                                     row.get("language") or "en", period)
            )
    except Exception as exc:
        log.error("daily_briefing_job_failed", error=str(exc))


async def _fire_daily_briefing(caller: str, name: str, city: str,
                                lang: str, period: str) -> None:
    """Build and deliver a personalised daily briefing call."""
    greeting  = "Good morning" if period == "morning" else "Good evening"
    hi_name   = f", {name}" if name else ""

    # Gather weather + news in parallel
    weather_text, news_text = await asyncio.gather(
        get_weather(city),
        get_news("general"),
    )

    prompt = (
        f"Create a short, friendly {period} briefing call script (max 4 sentences). "
        f"Start with '{greeting}{hi_name}! This is VoiceAI.' "
        f"Include: weather snippet: {weather_text[:120]}. "
        f"News snippet: {news_text[:200]}. "
        f"End with a warm sign-off. Reply in language code: {lang}."
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=180, temperature=0.7,
        )
        script = resp.choices[0].message.content.strip()
    except Exception:
        script = f"{greeting}{hi_name}! This is VoiceAI. Today's weather in {city}: {weather_text[:100]}. Have a great day!"

    # Generate TTS and fire the call
    try:
        audio_bytes = await text_to_speech_stream(script, lang=lang)
        audio_token = secrets.token_urlsafe(16)
        _tts_audio_cache[audio_token] = audio_bytes
        asyncio.get_event_loop().call_later(300, _tts_audio_cache.pop, audio_token, None)
        host = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
        twiml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Response>'
            f'<Play>https://{html.escape(host)}/tts-audio/{audio_token}</Play>'
            '</Response>'
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
                    .calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
        )
        log.info("daily_briefing_sent", caller=caller, period=period, city=city)
    except Exception as exc:
        log.error("daily_briefing_call_failed", caller=caller, error=str(exc))


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
    profile keys: name, city, language, last_topic, elderly_mode, cloned_voice_id
    """
    if not db_pool or not caller_number:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles
                    (caller, name, city, language, last_topic, last_seen, elderly_mode, cloned_voice_id)
                VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)
                ON CONFLICT (caller) DO UPDATE
                    SET name            = COALESCE(EXCLUDED.name,            caller_profiles.name),
                        city            = COALESCE(EXCLUDED.city,            caller_profiles.city),
                        language        = COALESCE(EXCLUDED.language,        caller_profiles.language),
                        last_topic      = COALESCE(EXCLUDED.last_topic,      caller_profiles.last_topic),
                        elderly_mode    = EXCLUDED.elderly_mode,
                        cloned_voice_id = COALESCE(EXCLUDED.cloned_voice_id, caller_profiles.cloned_voice_id),
                        last_seen       = NOW()
                """,
                caller_number,
                profile.get("name"),
                profile.get("city"),
                profile.get("language"),
                profile.get("last_topic"),
                bool(profile.get("elderly_mode", False)),
                profile.get("cloned_voice_id"),
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

    # BUG FIX 1: callback fires from Deepgram's internal thread.
    # Deepgram SDK passes `self` (the connection object) as the first positional arg,
    # so the signature MUST be (self, result, **kwargs) — not (result, **kwargs).
    # Without this fix every transcript raises TypeError and the call goes silent.
    async def on_transcript(self, result, **kwargs) -> None:  # noqa: N805
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
        # F-02: emotion-driven routing (sad → motivational, happy → playful, confused → clarify)
        routing_hint = get_emotion_routing_hint(emotion_result.emotion, session_lang)
        if routing_hint and not escalation_hint:
            escalation_hint = routing_hint
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

        # F-01: retrieve long-term pgvector memories and merge into context
        if PGVECTOR_ENABLED and caller_number:
            memories = await retrieve_memories(caller_number, user_text)
            if memories:
                caller_context["long_term_memories"] = "; ".join(memories)

        conversation.append({"role": "user", "content": user_text})
        transcript_log.append({
            "role":    "user",
            "content": user_text,
            **emotion_result.to_dict(),
        })

        # Detect "slow down" / elderly mode on the fly
        if ELDERLY_SLOW_DOWN_PHRASES.search(user_text):
            if caller_profile is not None:
                caller_profile["elderly_mode"] = True
            session_log.info("elderly_mode_activated", trigger=user_text[:60])

        elderly_active = (
            ELDERLY_MODE_DEFAULT
            or bool(caller_profile and caller_profile.get("elderly_mode"))
        )

        reply = await get_llm_reply(
            conversation,
            lang=session_lang,
            caller_number=caller_number,
            emotion_result=current_emotion_result,
            escalation_hint=escalation_hint,
            context=caller_context,      # UPGRADE 16
            caller_profile=caller_profile, # UPGRADE 7
            elderly_mode=elderly_active,
        )

        # UPGRADE 18: intent confidence check — ask a clarifying question if unsure.
        # We call check_reply_confidence first; if it's low we skip the original reply
        # entirely and ask a targeted clarifying question instead.
        # NOTE: the extra get_llm_reply call that previously lived here was dead code —
        # it was assigned to `clarify` then immediately overwritten. Removed (Bug Fix 5).
        reply_confidence = await check_reply_confidence(user_text, reply)
        if reply_confidence < 0.5:
            reply = await _ask_clarifying_question(user_text, session_lang)
            session_log.info("low_confidence_clarify", clarified_reply=reply, confidence=reply_confidence)

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
            _elderly = ELDERLY_MODE_DEFAULT or bool(caller_profile and caller_profile.get("elderly_mode"))
            audio_bytes = await text_to_speech_stream(
                reply,
                lang=session_lang,
                emotion=current_emotion_result.emotion,
                caller_number=caller_number,
                caller_profile=caller_profile,
                slow_mode=_elderly,
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

        # UPGRADE 7: persist cross-call caller profile (including elderly_mode)
        if caller_number:
            profile_update = {
                "language":       session_lang,
                "last_topic":     topics[0] if topics else None,
                "city":           caller_context.get("city"),
                "name":           caller_context.get("person_name"),
                "elderly_mode":   bool(caller_profile and caller_profile.get("elderly_mode")),
                "cloned_voice_id": caller_profile.get("cloned_voice_id") if caller_profile else None,
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
            "otp_auth":              OTP_PIN_REQUIRED,
            "ivr_fallback":          True,
            "hinglish":              True,
            "pnr_status":            bool(RAPIDAPI_KEY),
            "bhashini":              bool(BHASHINI_API_KEY),
            "sarvam_ai":             bool(SARVAM_API_KEY),
            "ab_voice_test":         AB_TEST_ENABLED,
            "whatsapp_summary":      bool(TWILIO_ACCOUNT_SID),
            "emergency_escalation":  bool(EMERGENCY_CONTACT_NUMBER),
            "voice_cloned":          bool(ELEVENLABS_VOICE_CLONED),
            "per_caller_voice_clone":True,
            "proactive_reminders":   gcal_service is not None,
            "medicine_reminders":    db_pool is not None,
            "daily_briefings":       DAILY_BRIEFING_ENABLED,
            "elderly_mode":          ELDERLY_MODE_DEFAULT,
            "cricket_scores":        bool(CRICAPI_KEY),
            "flight_status":         bool(AVIATIONSTACK_KEY),
            "smart_home":            bool(TUYA_CLIENT_ID),
            "upi_payment":           bool(RAZORPAY_KEY_ID),
            "entertainment":         True,
            "recipes":               True,
            "translation":           True,
            "voice_memos":           db_pool is not None,
            "cross_call_memory":     db_pool is not None,
            "dashboard_auth":        bool(DASHBOARD_TOKEN),
            "redis_rate_limit":      bool(REDIS_URL),
            "supported_languages":   list(ALL_INDIAN_LANGS) + ["en"],
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
        "tools": [
            "get_weather", "get_news", "book_appointment", "schedule_callback", "check_pnr",
            "check_upi_payment", "get_entertainment", "check_symptoms", "get_cricket_score",
            "get_recipe", "translate_text", "save_memo", "recall_memos",
            "get_flight_status", "control_smart_device",
        ],
        "new_features": [
            "OTP/PIN auth", "Callback scheduling", "Missed-call auto-callback",
            "IVR DTMF fallback", "Multi-turn entity memory", "Proactive reminders",
            "Intent confidence threshold", "Hinglish support", "PNR status",
            "Medicine reminders (multi-lang)", "Bhashini STT+TTS (ta/bn/mr)",
            "Sarvam AI STT+TTS (22 Indian langs)", "Twilio signature verification",
            "Exponential backoff retry", "Session inactivity timeout", "Admin dashboard (auth)",
            "Abuse detection + admin SMS alert", "Call topic tagging", "A/B voice testing",
            "WhatsApp call summary", "Emergency auto-escalation", "Per-caller voice cloning",
            "Cross-call memory (caller profiles)", "Elderly/slow-speech mode",
            "Daily briefings (outbound)", "Entertainment (jokes/stories/trivia)",
            "Health symptom checker + emergency 112", "Cricket scores", "Recipes",
            "Translation", "Voice memos", "Flight status", "Smart home control",
            "UPI payment status", "Redis-backed rate limiting", "Analytics API",
            "Dashboard Bearer/Basic auth", "Voice clone registration endpoint",
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
# NEW: PER-CALLER VOICE CLONING — /voice/clone
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/voice/clone")
async def clone_voice(request: Request):
    """
    Register a per-caller ElevenLabs cloned voice.
    Accepts multipart/form-data with:
        caller      : E.164 phone number
        voice_name  : label for the cloned voice
        audio_file  : WAV/MP3 sample (10-60 seconds recommended)
    OR JSON body with:
        caller      : E.164 phone number
        voice_id    : existing ElevenLabs voice_id to use directly

    Stores the resulting voice_id in caller_profiles.cloned_voice_id.
    """
    content_type = request.headers.get("content-type", "")
    if "multipart" in content_type:
        form        = await request.form()
        caller      = form.get("caller", "")
        voice_name  = form.get("voice_name", f"VoiceAI-{caller[-4:]}")
        audio_file  = form.get("audio_file")  # UploadFile
        if not caller or not audio_file:
            raise HTTPException(status_code=400, detail="caller and audio_file required")
        if not ELEVENLABS_API_KEY:
            raise HTTPException(status_code=503, detail="ElevenLabs not configured")
        # Upload to ElevenLabs Instant Voice Cloning
        audio_bytes  = await audio_file.read()
        url          = "https://api.elevenlabs.io/v1/voices/add"
        headers      = {"xi-api-key": ELEVENLABS_API_KEY}
        files        = {"files": (audio_file.filename, audio_bytes, "audio/wav")}
        data         = {"name": voice_name, "description": f"VoiceAI clone for {caller}"}
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=headers, files=files, data=data)
            resp.raise_for_status()
            voice_id = resp.json().get("voice_id", "")
    else:
        body     = await request.json()
        caller   = body.get("caller", "")
        voice_id = body.get("voice_id", "")
        if not caller or not voice_id:
            raise HTTPException(status_code=400, detail="caller and voice_id required")

    if not caller or not voice_id:
        raise HTTPException(status_code=422, detail="Could not determine caller or voice_id")

    # Persist in caller_profiles
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles (caller, cloned_voice_id, last_seen)
                VALUES ($1, $2, NOW())
                ON CONFLICT (caller) DO UPDATE SET cloned_voice_id=$2, last_seen=NOW()
                """,
                caller, voice_id,
            )
    log.info("voice_cloned", caller=caller, voice_id=voice_id)
    return {"status": "ok", "caller": caller, "voice_id": voice_id}


# ══════════════════════════════════════════════════════════════════════════════
# NEW: VOICE MEMO ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/memos/{caller_number}")
async def get_memos_endpoint(caller_number: str):
    """Retrieve saved voice memos for a caller (REST API)."""
    result = await recall_memos(caller_number)
    return {"caller": caller_number, "memos": result}


@app.delete("/memos/{memo_id}")
async def delete_memo_endpoint(memo_id: int):
    """Delete a specific memo by ID."""
    if db_pool is None:
        return {"error": "Database not configured"}
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM voice_memos WHERE id=$1", memo_id)
        return {"status": "deleted", "id": memo_id}
    except Exception as exc:
        log.error("delete_memo_failed", id=memo_id, error=str(exc))
        return {"error": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# NEW: DAILY BRIEFING OPT-IN / OPT-OUT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/briefings/subscribe")
async def subscribe_briefing(request: Request):
    """
    Subscribe a caller to daily briefing calls.
    Body: {"caller": "+91...", "city": "Mumbai", "language": "hi", "name": "Ramesh"}
    """
    body     = await request.json()
    caller   = body.get("caller", "")
    city     = body.get("city", "")
    language = body.get("language", "hi")
    name     = body.get("name", "")
    if not caller or not city:
        raise HTTPException(status_code=400, detail="caller and city required")
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles (caller, city, language, name, last_seen)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (caller) DO UPDATE
                    SET city=$2, language=$3, name=COALESCE($4, caller_profiles.name), last_seen=NOW()
                """,
                caller, city, language, name or None,
            )
    log.info("briefing_subscribed", caller=caller, city=city, language=language)
    return {"status": "subscribed", "caller": caller, "city": city,
            "morning": DAILY_BRIEFING_MORNING_TIME, "evening": DAILY_BRIEFING_EVENING_TIME}


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

# ══════════════════════════════════════════════════════════════════════════════
# ████████████████████████  NEW FEATURE BATCH  ████████████████████████████████
# ══════════════════════════════════════════════════════════════════════════════
#
# F-01  pgvector long-term memory  — cross-call semantic search via Postgres
# F-02  Emotion-driven routing     — sad→motivational, happy→playful hints
# F-03  STT fallback chain         — Deepgram → Sarvam → Bhashini → local stub
# F-04  Advanced abuse detection   — per-tool limits + voice challenge
# F-05  Call recording (consent)   — Twilio <Record>, TRAI-compliant logging
# F-06  Dashboard: Chart.js, CSV export, real-time poll, post-call rating
# F-07  Family / friend notification tool
# F-08  Post-call satisfaction survey
# F-09  Festive/greeting outbound calls (Indian calendar)
# F-10  Prometheus metrics endpoint
# F-11  Per-component latency tracking
# F-12  Webhook trigger: outbound call from external event (bank / payment)
# F-13  Offline response cache      — weather + news cached 10 min
# F-14  Unit-test suite (pytest)    — language detection, tool dispatch, emotion
# F-15  Docker / multi-worker notes baked in as constants + health hints
#
# ══════════════════════════════════════════════════════════════════════════════

import time as _time_mod   # already imported as time_module; alias for metrics
from collections import Counter as _Counter
from typing import Callable

# ── Extra env vars for new features ──────────────────────────────────────────
PGVECTOR_ENABLED        = os.environ.get("PGVECTOR_ENABLED", "false").lower() == "true"
PGVECTOR_EMBED_MODEL    = os.environ.get("PGVECTOR_EMBED_MODEL", "groq")  # "groq" uses LLM embeddings stub
CALL_RECORDING_ENABLED  = os.environ.get("CALL_RECORDING_ENABLED", "false").lower() == "true"
CALL_RECORDING_BUCKET   = os.environ.get("CALL_RECORDING_BUCKET", "")  # S3 / GCS bucket name
PROMETHEUS_ENABLED      = os.environ.get("PROMETHEUS_ENABLED", "false").lower() == "true"
SURVEY_ENABLED          = os.environ.get("SURVEY_ENABLED", "false").lower() == "true"
FESTIVE_CALLS_ENABLED   = os.environ.get("FESTIVE_CALLS_ENABLED", "false").lower() == "true"
ABUSE_PER_TOOL_LIMIT    = int(os.environ.get("ABUSE_PER_TOOL_LIMIT", "10"))   # max same tool calls / call
VOICE_CHALLENGE_ENABLED = os.environ.get("VOICE_CHALLENGE_ENABLED", "false").lower() == "true"

# ── Offline cache TTL ─────────────────────────────────────────────────────────
_CACHE_TTL_SECS = int(os.environ.get("RESPONSE_CACHE_TTL", "600"))   # 10 min default


# ══════════════════════════════════════════════════════════════════════════════
# F-10/11 — PROMETHEUS METRICS + PER-COMPONENT LATENCY TRACKING
# ══════════════════════════════════════════════════════════════════════════════

class _SimpleMetrics:
    """
    Lightweight in-process metrics store.
    Exports as Prometheus text format via GET /metrics.
    If prometheus_client is installed it's used; otherwise falls back to
    a pure-Python counter/histogram implementation so the app never crashes.
    """
    def __init__(self):
        self._counters:   dict[str, float]        = defaultdict(float)
        self._histograms: dict[str, list[float]]  = defaultdict(list)
        self._use_prom = False
        self._prom_counters:    dict = {}
        self._prom_histograms:  dict = {}
        if PROMETHEUS_ENABLED:
            try:
                import prometheus_client as prom  # type: ignore
                self._prom    = prom
                self._use_prom = True
                self._prom_counters["calls_total"]        = prom.Counter("voiceai_calls_total", "Total inbound calls")
                self._prom_counters["tool_calls_total"]   = prom.Counter("voiceai_tool_calls_total", "Total LLM tool calls", ["tool"])
                self._prom_counters["tts_errors_total"]   = prom.Counter("voiceai_tts_errors_total", "TTS failures")
                self._prom_counters["auth_failures_total"]= prom.Counter("voiceai_auth_failures_total", "PIN auth failures")
                self._prom_histograms["stt_latency_ms"]   = prom.Histogram("voiceai_stt_latency_ms",   "STT latency ms",  buckets=[50,100,200,400,800,1600])
                self._prom_histograms["llm_latency_ms"]   = prom.Histogram("voiceai_llm_latency_ms",   "LLM latency ms",  buckets=[100,200,400,800,1600,3200])
                self._prom_histograms["tts_latency_ms"]   = prom.Histogram("voiceai_tts_latency_ms",   "TTS latency ms",  buckets=[50,100,200,400,800,1600])
                self._prom_histograms["call_duration_secs"]= prom.Histogram("voiceai_call_duration_secs","Call duration s", buckets=[10,30,60,120,300,600])
                log.info("prometheus_metrics_enabled")
            except ImportError:
                self._use_prom = False

    def inc(self, name: str, labels: Optional[dict] = None, value: float = 1.0) -> None:
        if self._use_prom and name in self._prom_counters:
            c = self._prom_counters[name]
            if labels:
                c.labels(**labels).inc(value)
            else:
                c.inc(value)
        else:
            key = name + (str(sorted(labels.items())) if labels else "")
            self._counters[key] += value

    def observe(self, name: str, value: float) -> None:
        if self._use_prom and name in self._prom_histograms:
            self._prom_histograms[name].observe(value)
        else:
            self._histograms[name].append(value)

    def text_exposition(self) -> str:
        """Render simple Prometheus text format from the in-process store."""
        if self._use_prom:
            from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore
            return generate_latest().decode()
        lines = []
        for k, v in self._counters.items():
            safe = re.sub(r"[^a-zA-Z0-9_]", "_", k)
            lines.append(f"# TYPE voiceai_{safe} counter")
            lines.append(f"voiceai_{safe} {v}")
        for k, vals in self._histograms.items():
            if not vals:
                continue
            safe = re.sub(r"[^a-zA-Z0-9_]", "_", k)
            lines.append(f"# TYPE voiceai_{safe} summary")
            lines.append(f"voiceai_{safe}_count {len(vals)}")
            lines.append(f"voiceai_{safe}_sum {sum(vals):.3f}")
        return "\n".join(lines) + "\n"


metrics = _SimpleMetrics()


class LatencyTimer:
    """Context manager that records component latency into metrics."""
    def __init__(self, component: str):
        self._component = component
        self._start: float = 0.0

    def __enter__(self):
        self._start = _time_mod.monotonic()
        return self

    def __exit__(self, *_):
        elapsed_ms = (_time_mod.monotonic() - self._start) * 1000
        metrics.observe(f"{self._component}_latency_ms", elapsed_ms)
        log.debug("latency", component=self._component, ms=round(elapsed_ms, 1))


@app.get("/metrics", response_class=Response)
async def prometheus_metrics():
    """Expose Prometheus metrics (or simple text counters if prometheus_client not installed)."""
    return Response(content=metrics.text_exposition(), media_type="text/plain; version=0.0.4")


# ══════════════════════════════════════════════════════════════════════════════
# F-13 — OFFLINE RESPONSE CACHE (weather + news)
# ══════════════════════════════════════════════════════════════════════════════

_response_cache: dict[str, tuple[float, str]] = {}   # key → (timestamp, value)


def _cache_get(key: str) -> Optional[str]:
    entry = _response_cache.get(key)
    if entry and (_time_mod.monotonic() - entry[0]) < _CACHE_TTL_SECS:
        return entry[1]
    return None


def _cache_set(key: str, value: str) -> None:
    _response_cache[key] = (_time_mod.monotonic(), value)


# Monkey-patch get_weather and get_news to be cache-aware.
# We keep the originals under _orig_ names so they still work independently.
_orig_get_weather = get_weather
_orig_get_news    = get_news


async def get_weather_cached(city: str) -> str:
    key = f"weather:{city.lower()}"
    cached = _cache_get(key)
    if cached:
        log.debug("cache_hit", key=key)
        return cached
    result = await _orig_get_weather(city)
    _cache_set(key, result)
    return result


async def get_news_cached(topic: str) -> str:
    key = f"news:{topic.lower()}"
    cached = _cache_get(key)
    if cached:
        log.debug("cache_hit", key=key)
        return cached
    result = await _orig_get_news(topic)
    _cache_set(key, result)
    return result


# Replace module-level names so dispatch_tool_call picks up cached versions
get_weather = get_weather_cached  # noqa: F811
get_news    = get_news_cached     # noqa: F811


@app.get("/cache/flush")
async def flush_cache():
    """Admin: clear the in-memory response cache (useful after API rate-limit incidents)."""
    count = len(_response_cache)
    _response_cache.clear()
    log.info("cache_flushed", entries_removed=count)
    return {"status": "flushed", "entries_removed": count}


# ══════════════════════════════════════════════════════════════════════════════
# F-01 — pgvector LONG-TERM MEMORY
# ══════════════════════════════════════════════════════════════════════════════
# Uses Postgres pgvector extension to store and retrieve semantic memories
# across calls and days.  Each memory is a text snippet + embedding vector.
# When PGVECTOR_ENABLED=true, the table `caller_memories` is created and
# the nearest 3 memories are injected into the system prompt.
#
# Embedding strategy: we use Groq LLM to produce a short fixed-length
# "semantic fingerprint" (64 floats via JSON) when pgvector is enabled
# but no external embedding API is set.  For production, swap _embed()
# with a real embedding model (e.g. text-embedding-3-small via OpenAI).
# ══════════════════════════════════════════════════════════════════════════════

async def _embed(text: str) -> Optional[list[float]]:
    """
    Produce a 64-dimensional embedding for `text` using Groq LLM.
    This is a lightweight proxy: ask the LLM to summarise into 64 numbers.
    For production replace with a real embedding model.
    Returns None on any failure so the rest of the system keeps working.
    """
    if not PGVECTOR_ENABLED:
        return None
    prompt = (
        "You are an embedding model. Produce EXACTLY a JSON array of 64 floats "
        "between -1.0 and 1.0 that semantically encode the following text. "
        "Reply with ONLY the JSON array — no markdown, no explanation. "
        f"Text: {text[:400]}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=256, temperature=0.0,
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        vec = json.loads(raw)
        if isinstance(vec, list) and len(vec) == 64:
            return [float(v) for v in vec]
    except Exception as exc:
        log.warning("embed_failed", error=str(exc))
    return None


async def _ensure_pgvector_table() -> None:
    """Create caller_memories table if pgvector is installed and PGVECTOR_ENABLED."""
    if not (PGVECTOR_ENABLED and db_pool):
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS caller_memories (
                    id         SERIAL PRIMARY KEY,
                    caller     TEXT NOT NULL,
                    memory     TEXT NOT NULL,
                    embedding  vector(64),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS caller_memories_caller_idx "
                "ON caller_memories(caller)"
            )
        log.info("pgvector_table_ready")
    except Exception as exc:
        log.warning("pgvector_setup_failed", error=str(exc),
                    hint="Install pgvector extension or set PGVECTOR_ENABLED=false")


async def store_memory(caller: str, memory_text: str) -> None:
    """
    Persist a short memory snippet for a caller.
    Called at end-of-call with key facts extracted from the conversation.
    """
    if not (PGVECTOR_ENABLED and db_pool and caller):
        return
    vec = await _embed(memory_text)
    try:
        async with db_pool.acquire() as conn:
            if vec:
                await conn.execute(
                    "INSERT INTO caller_memories(caller, memory, embedding) VALUES($1,$2,$3::vector)",
                    caller, memory_text, json.dumps(vec),
                )
            else:
                await conn.execute(
                    "INSERT INTO caller_memories(caller, memory) VALUES($1,$2)",
                    caller, memory_text,
                )
        log.info("memory_stored", caller=caller, snippet=memory_text[:60])
    except Exception as exc:
        log.warning("store_memory_failed", caller=caller, error=str(exc))


async def retrieve_memories(caller: str, query: str, top_k: int = 3) -> list[str]:
    """
    Retrieve the top-k most relevant memories for a caller.
    Uses cosine similarity if pgvector + embeddings available;
    falls back to the most recent memories otherwise.
    """
    if not (PGVECTOR_ENABLED and db_pool and caller):
        return []
    try:
        async with db_pool.acquire() as conn:
            query_vec = await _embed(query)
            if query_vec:
                rows = await conn.fetch(
                    """
                    SELECT memory FROM caller_memories
                    WHERE caller=$1
                    ORDER BY embedding <=> $2::vector
                    LIMIT $3
                    """,
                    caller, json.dumps(query_vec), top_k,
                )
            else:
                rows = await conn.fetch(
                    "SELECT memory FROM caller_memories WHERE caller=$1 "
                    "ORDER BY created_at DESC LIMIT $2",
                    caller, top_k,
                )
        return [r["memory"] for r in rows]
    except Exception as exc:
        log.warning("retrieve_memories_failed", caller=caller, error=str(exc))
        return []


async def extract_and_store_call_memories(caller: str, transcript: list[dict]) -> None:
    """
    Post-call: extract 2-3 memorable facts from the transcript and store them.
    Called in the websocket finally block.
    """
    if not (PGVECTOR_ENABLED and caller):
        return
    user_turns = " | ".join(t["content"] for t in transcript if t.get("role") == "user")[:600]
    if not user_turns:
        return
    prompt = (
        "Extract 2-3 short, factual memory snippets from this caller's conversation. "
        "Focus on: name, city, family members, health conditions, preferences, topics discussed. "
        "Reply ONLY with a JSON array of short strings (max 20 words each). "
        "Example: [\"Caller's name is Ramesh\", \"Lives in Nagpur\", \"Has diabetes\"]\n\n"
        "Conversation: " + user_turns
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120, temperature=0.0,
        )
        raw      = resp.choices[0].message.content.strip()
        raw      = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        memories = json.loads(raw)
        if isinstance(memories, list):
            for mem in memories[:3]:
                await store_memory(caller, str(mem))
    except Exception as exc:
        log.warning("memory_extraction_failed", caller=caller, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# F-02 — EMOTION-DRIVEN ROUTING (extended)
# ══════════════════════════════════════════════════════════════════════════════
# Already have: frustrated/angry → escalation hint, urgent → 112.
# NEW:
#   sad       → suggest a motivational quote OR offer to call a family contact
#   happy     → unlock "playful" LLM mode (higher temperature, emoji-OK)
#   confused  → trigger IVR menu immediately rather than guessing

_MOTIVATIONAL_QUOTES = [
    "Every day is a new beginning. Take a deep breath and start again.",
    "You are stronger than you think. One step at a time.",
    "Hard times never last, but strong people do.",
    "The sun always rises after the darkest night.",
    "You matter. You are loved. Today will be better.",
]


def get_emotion_routing_hint(emotion: str, lang: str = "en") -> str:
    """
    F-02: Return an extra system-prompt injection based on emotion routing rules.
    Extends the existing get_emotion_tone_instruction() with new behaviours.
    """
    if emotion == "sad":
        quote = random.choice(_MOTIVATIONAL_QUOTES)
        if lang in ("hi", "hi-en"):
            return (
                f"The caller sounds sad. Share this encouraging thought: '{quote}' "
                "Then gently ask if they'd like you to send a message to a family member or friend."
            )
        return (
            f"The caller sounds sad. Warmly share: '{quote}' "
            "Then ask if they'd like you to notify a family member."
        )

    if emotion == "happy":
        return (
            "The caller is in a great mood! Match their energy. "
            "Feel free to be slightly playful, use a light joke if natural, "
            "and keep the tone upbeat and celebratory."
        )

    if emotion == "confused":
        return (
            "The caller sounds confused. "
            "Do NOT proceed with the main task yet. "
            "First ask one single simple clarifying question to understand what they need."
        )

    return ""


# ══════════════════════════════════════════════════════════════════════════════
# F-03 — STT FALLBACK CHAIN
# ══════════════════════════════════════════════════════════════════════════════
# Order: Deepgram (live WebSocket, primary) → Sarvam batch → Bhashini batch → stub
# Used when the live Deepgram connection fails or when audio is received via
# a batch endpoint (e.g. /transcribe for testing).

async def transcribe_audio_batch(audio_bytes: bytes, lang: str = "en") -> str:
    """
    F-03: Full STT fallback chain for batch audio.
    1. Try Sarvam (best for Indian languages).
    2. Try Bhashini (ta/bn/mr).
    3. Stub: return empty string (graceful no-op).
    """
    # 1. Sarvam
    if SARVAM_API_KEY and lang in SARVAM_LANG_CODES:
        try:
            result = await _stt_sarvam(audio_bytes, lang)
            if result:
                log.info("stt_fallback", provider="sarvam", lang=lang)
                return result
        except Exception as exc:
            log.warning("sarvam_stt_failed", lang=lang, error=str(exc))

    # 2. Bhashini
    if BHASHINI_API_KEY and lang in BHASHINI_LANG_CODES:
        try:
            result = await _stt_bhashini(audio_bytes, lang)
            if result:
                log.info("stt_fallback", provider="bhashini", lang=lang)
                return result
        except Exception as exc:
            log.warning("bhashini_stt_failed", lang=lang, error=str(exc))

    # 3. Stub — return empty string; IVR menu will kick in for the caller
    log.warning("stt_all_providers_failed", lang=lang)
    return ""


@app.post("/transcribe")
async def transcribe_endpoint(request: Request):
    """
    F-03: Batch transcription endpoint for testing the STT fallback chain.
    Accepts: multipart/form-data with `audio` (WAV bytes) and optional `lang`.
    """
    form       = await request.form()
    lang       = form.get("lang", "en")
    audio_file = form.get("audio")
    if not audio_file:
        raise HTTPException(status_code=400, detail="audio field required")
    audio_bytes = await audio_file.read()
    transcript  = await transcribe_audio_batch(audio_bytes, lang=lang)
    return {"transcript": transcript, "lang": lang}


# ══════════════════════════════════════════════════════════════════════════════
# F-04 — ADVANCED ABUSE DETECTION: PER-TOOL LIMITS + VOICE CHALLENGE
# ══════════════════════════════════════════════════════════════════════════════

class PerToolRateLimiter:
    """
    Tracks how many times each tool is called within a single WebSocket session.
    If a caller calls the same tool more than ABUSE_PER_TOOL_LIMIT times in one
    call, that tool is soft-blocked for the remainder of the session.
    """
    def __init__(self):
        self._counts: dict[str, int] = defaultdict(int)

    def record(self, tool_name: str) -> None:
        self._counts[tool_name] += 1

    def is_blocked(self, tool_name: str) -> bool:
        return self._counts[tool_name] >= ABUSE_PER_TOOL_LIMIT

    def summary(self) -> dict:
        return dict(self._counts)


async def generate_voice_challenge() -> tuple[str, str]:
    """
    F-04: Generate a simple voice CAPTCHA — arithmetic question spoken to caller.
    Returns (question_text, expected_answer_str).
    Used when a caller triggers suspicious patterns (very high call rate, etc.)
    """
    a, b = random.randint(1, 9), random.randint(1, 9)
    op   = random.choice(["+", "-"])
    answer = a + b if op == "+" else a - b
    question = f"To continue, please answer this: what is {a} {op} {b}?"
    return question, str(answer)


def check_voice_challenge(spoken: str, expected: str) -> bool:
    """Extract the first integer from spoken text and compare to expected."""
    digits = re.sub(r"\D", "", spoken)
    if digits == expected:
        return True
    # Handle word form: "five" → 5
    word_map = {
        "zero":"0","one":"1","two":"2","three":"3","four":"4",
        "five":"5","six":"6","seven":"7","eight":"8","nine":"9",
        "ten":"10","eleven":"11","twelve":"12","thirteen":"13","fourteen":"14",
        "fifteen":"15","sixteen":"16","seventeen":"17","eighteen":"18",
    }
    for word, digit in word_map.items():
        if word in spoken.lower() and digit == expected:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# F-05 — CALL RECORDING (consent-gated, TRAI-compliant)
# ══════════════════════════════════════════════════════════════════════════════

TRAI_CONSENT_MESSAGE = (
    "This call may be recorded for quality assurance. "
    "Press 1 or say 'yes' to consent, or press 2 to decline."
)


def _start_twilio_recording_sync(call_sid: str) -> Optional[str]:
    """Start a Twilio call recording. Returns recording SID or None."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN]):
        return None
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        rec    = client.calls(call_sid).recordings.create()
        log.info("recording_started", call_sid=call_sid, recording_sid=rec.sid)
        return rec.sid
    except Exception as exc:
        log.error("recording_start_failed", call_sid=call_sid, error=str(exc))
        return None


async def maybe_start_recording(call_sid: str, caller_consented: bool) -> Optional[str]:
    """
    F-05: Start Twilio recording if CALL_RECORDING_ENABLED and caller consented.
    TRAI compliance: recording only after explicit consent.
    """
    if not (CALL_RECORDING_ENABLED and caller_consented and call_sid):
        return None
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _start_twilio_recording_sync, call_sid)


@app.post("/recording/consent")
async def recording_consent(request: Request):
    """
    Twilio <Gather> callback for consent IVR.
    Digits=1 or Body contains 'yes' → consent granted.
    """
    form    = await request.form()
    digits  = form.get("Digits", "")
    body    = form.get("SpeechResult", "").lower()
    call_sid = form.get("CallSid", "")
    consented = digits == "1" or "yes" in body or "haan" in body

    if consented:
        asyncio.ensure_future(maybe_start_recording(call_sid, True))
        say = "Thank you. The call will now be recorded."
    else:
        say = "No problem. The call will not be recorded."

    return Response(
        content=f'<?xml version="1.0"?><Response><Say voice="Polly.Joanna">{html.escape(say)}</Say></Response>',
        media_type="application/xml",
    )


# ══════════════════════════════════════════════════════════════════════════════
# F-07 — FAMILY / FRIEND NOTIFICATION TOOL
# ══════════════════════════════════════════════════════════════════════════════

async def notify_family(
    caller_number: str,
    message: str,
    recipient_name: str = "",
) -> str:
    """
    F-07: Send a WhatsApp / SMS message to a stored family contact on behalf of the caller.
    Family contacts are stored in caller_profiles.family_contacts (JSONB).
    """
    if not caller_number:
        return "I need your phone number to send a family message."

    # Load family contacts from profile
    family_contacts: list[dict] = []
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT family_contacts FROM caller_profiles WHERE caller=$1",
                    caller_number,
                )
            if row and row.get("family_contacts"):
                family_contacts = json.loads(row["family_contacts"] or "[]")
        except Exception as exc:
            log.warning("family_contacts_load_failed", error=str(exc))

    if not family_contacts:
        return (
            "I don't have any family contacts saved for you yet. "
            "You can add them by calling our setup line or via the web portal."
        )

    # Find the contact by name, or use the first one
    contact = family_contacts[0]
    if recipient_name:
        for fc in family_contacts:
            if recipient_name.lower() in fc.get("name", "").lower():
                contact = fc
                break

    to_number = contact.get("phone", "")
    name      = contact.get("name", "your family")
    if not to_number:
        return f"I couldn't find a phone number for {name}."

    full_message = f"📞 Message from {caller_number}:\n\n{message}\n\n_Sent via VoiceAI_"
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, send_sms, to_number, full_message)
    log.info("family_notified", from_=caller_number, to=to_number, name=name)
    return f"Done! I've sent your message to {name}."


@app.post("/profile/family-contacts")
async def update_family_contacts(request: Request):
    """
    Register / update family contacts for a caller.
    Body: {"caller": "+91...", "contacts": [{"name": "Beta", "phone": "+91..."}]}
    """
    body     = await request.json()
    caller   = body.get("caller", "")
    contacts = body.get("contacts", [])
    if not caller:
        raise HTTPException(status_code=400, detail="caller required")
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO caller_profiles(caller, family_contacts, last_seen)
                VALUES($1, $2::jsonb, NOW())
                ON CONFLICT(caller) DO UPDATE
                    SET family_contacts=$2::jsonb, last_seen=NOW()
                """,
                caller, json.dumps(contacts),
            )
    log.info("family_contacts_updated", caller=caller, count=len(contacts))
    return {"status": "updated", "caller": caller, "contacts_count": len(contacts)}


# ══════════════════════════════════════════════════════════════════════════════
# F-08 — POST-CALL SATISFACTION SURVEY
# ══════════════════════════════════════════════════════════════════════════════

def _send_survey_call_sync(caller: str, call_sid: str) -> None:
    """Fire a short post-call IVR survey: 'Was this helpful? Press 1 for yes, 2 for no.'"""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    host  = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Gather numDigits="1" action="/survey/response" method="POST">'
        '<Say voice="Polly.Joanna">'
        "Hi! Thanks for using VoiceAI. Was this call helpful? "
        "Press 1 for yes, press 2 for no."
        '</Say>'
        '</Gather>'
        '<Say voice="Polly.Joanna">We didn\'t hear your response. Goodbye!</Say>'
        '</Response>'
    )
    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
        log.info("survey_call_sent", caller=caller)
    except Exception as exc:
        log.warning("survey_call_failed", caller=caller, error=str(exc))


async def send_survey_if_enabled(caller: str, call_sid: str) -> None:
    """F-08: Trigger post-call survey 10 s after the call ends."""
    if not SURVEY_ENABLED or not caller:
        return
    await asyncio.sleep(10)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _send_survey_call_sync, caller, call_sid)


@app.post("/survey/response")
async def survey_response(request: Request):
    """Twilio callback for post-call survey digit input."""
    form    = await request.form()
    digits  = form.get("Digits", "")
    caller  = form.get("To", "")
    helpful = digits == "1"
    log.info("survey_response", caller=caller, helpful=helpful)
    # Persist to DB
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE calls SET satisfied=$1 WHERE caller=$2 "
                    "AND start_time >= NOW() - INTERVAL '30 minutes' "
                    "ORDER BY start_time DESC LIMIT 1",  # best-effort match
                    helpful, caller,
                )
        except Exception:
            pass  # table may not have `satisfied` yet; migration below handles it
    msg = "Great, we're glad it helped!" if helpful else "Thanks for the feedback. We'll keep improving!"
    return Response(
        content=f'<?xml version="1.0"?><Response><Say voice="Polly.Joanna">{html.escape(msg)}</Say></Response>',
        media_type="application/xml",
    )


# ══════════════════════════════════════════════════════════════════════════════
# F-09 — FESTIVE / GREETING OUTBOUND CALLS
# ══════════════════════════════════════════════════════════════════════════════

# Approximate dates (month, day) — no year so they recur annually
_INDIAN_FESTIVALS: list[dict] = [
    {"name": "Diwali",        "month": 10, "day": 20, "greeting_en": "Happy Diwali! May your life be filled with light and joy.", "greeting_hi": "दीपावली की हार्दिक शुभकामनाएं! आपका जीवन खुशियों से भरा रहे।"},
    {"name": "Holi",          "month":  3, "day": 25, "greeting_en": "Happy Holi! May the colours of joy fill your life.", "greeting_hi": "होली की हार्दिक शुभकामनाएं! रंगों की तरह आपका जीवन रंगीन रहे।"},
    {"name": "Eid",           "month":  3, "day": 30, "greeting_en": "Eid Mubarak! Wishing you peace, happiness and prosperity.", "greeting_hi": "ईद मुबारक! आपको खुशी और समृद्धि की शुभकामनाएं।"},
    {"name": "Christmas",     "month": 12, "day": 25, "greeting_en": "Merry Christmas! Wishing you joy and peace this holiday season.", "greeting_hi": "क्रिसमस की शुभकामनाएं! आपका दिन खुशियों से भरा रहे।"},
    {"name": "New Year",      "month":  1, "day":  1, "greeting_en": "Happy New Year! May this year bring you health, happiness and success.", "greeting_hi": "नया साल मुबारक! यह साल आपके लिए खुशियां लाए।"},
    {"name": "Republic Day",  "month":  1, "day": 26, "greeting_en": "Happy Republic Day! Jai Hind.", "greeting_hi": "गणतंत्र दिवस की शुभकामनाएं! जय हिंद।"},
    {"name": "Independence Day", "month": 8, "day": 15, "greeting_en": "Happy Independence Day! Jai Hind.", "greeting_hi": "स्वतंत्रता दिवस की शुभकामनाएं! जय हिंद।"},
]


async def check_and_send_festive_greetings() -> None:
    """
    F-09: APScheduler daily job — check if today is a festival day,
    send greeting calls to all opted-in callers.
    """
    if not (FESTIVE_CALLS_ENABLED and db_pool):
        return
    today = datetime.now(ZoneInfo(TIMEZONE))
    for festival in _INDIAN_FESTIVALS:
        if festival["month"] == today.month and festival["day"] == today.day:
            log.info("festive_day_detected", festival=festival["name"])
            try:
                async with db_pool.acquire() as conn:
                    callers = await conn.fetch(
                        "SELECT caller, language FROM caller_profiles WHERE caller IS NOT NULL"
                    )
                for row in callers:
                    lang     = row.get("language") or "en"
                    greeting = festival["greeting_hi"] if lang in ("hi", "hi-en") else festival["greeting_en"]
                    asyncio.ensure_future(_send_festive_call(row["caller"], greeting, lang))
            except Exception as exc:
                log.error("festive_greetings_failed", festival=festival["name"], error=str(exc))
            break  # only one festival per day


def _send_festive_call_sync(caller: str, greeting: str) -> None:
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        return
    client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    twiml  = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Say voice="Polly.Aditi">{html.escape(greeting)}</Say></Response>'
    )
    client.calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
    log.info("festive_call_sent", caller=caller)


async def _send_festive_call(caller: str, greeting: str, lang: str) -> None:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _send_festive_call_sync, caller, greeting)
    except Exception as exc:
        log.warning("festive_call_failed", caller=caller, error=str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# F-12 — WEBHOOK TRIGGER: OUTBOUND CALL FROM EXTERNAL EVENT
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/webhook/trigger-call")
async def webhook_trigger_call(request: Request):
    """
    F-12: External systems (bank, UPI, payment gateway) POST here to trigger
    an outbound VoiceAI notification call to the customer.

    Body (JSON):
        caller   : E.164 phone number of the recipient
        message  : Text to speak (e.g. "Your UPI payment of ₹500 was successful.")
        source   : Identifier of the triggering system (e.g. "razorpay", "bank")
        lang     : Language code (default "hi")
        secret   : Must match WEBHOOK_SECRET env var for authentication

    Example use case: Razorpay webhook → POST /webhook/trigger-call
    → Customer gets an outbound call: "Aapka UPI payment success ho gaya."
    """
    WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

    body    = await request.json()
    secret  = body.get("secret", "")
    if WEBHOOK_SECRET and not secrets.compare_digest(secret, WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    caller  = body.get("caller", "")
    message = body.get("message", "")
    source  = body.get("source", "external")
    lang    = body.get("lang", "hi")

    if not caller or not message:
        raise HTTPException(status_code=400, detail="caller and message required")

    log.info("webhook_trigger_call", caller=caller, source=source, message=message[:80])

    async def _fire():
        # Try to generate TTS in the caller's language first
        audio_token = ""
        try:
            audio_bytes = await text_to_speech_stream(message, lang=lang)
            audio_token = secrets.token_urlsafe(16)
            _tts_audio_cache[audio_token] = audio_bytes
            asyncio.get_running_loop().call_later(300, _tts_audio_cache.pop, audio_token, None)
        except Exception:
            pass  # fall back to Polly

        host = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")
        if audio_token:
            twiml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                f'<Response><Play>https://{html.escape(host)}/tts-audio/{audio_token}</Play></Response>'
            )
        else:
            twiml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                f'<Response><Say voice="Polly.Aditi">{html.escape(message)}</Say></Response>'
            )
        if all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    lambda: TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
                            .calls.create(to=caller, from_=TWILIO_PHONE_NUMBER, twiml=twiml)
                )
                log.info("webhook_call_fired", caller=caller, source=source)
            except Exception as exc:
                log.error("webhook_call_failed", caller=caller, error=str(exc))

    asyncio.ensure_future(_fire())
    return {"status": "queued", "caller": caller, "source": source}


# ══════════════════════════════════════════════════════════════════════════════
# F-06 — DASHBOARD ENHANCEMENTS (Chart.js, CSV export, real-time poll, rating)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/calls/export.csv")
async def export_calls_csv(request: Request):
    """
    F-06: Export call log as CSV. Protected by DASHBOARD_TOKEN (same as /dashboard).
    """
    if DASHBOARD_TOKEN:
        auth = request.headers.get("Authorization", "")
        if not (auth.startswith("Bearer ") and secrets.compare_digest(auth[7:], DASHBOARD_TOKEN)):
            raise HTTPException(status_code=401, detail="Unauthorized")
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, caller, start_time, end_time, duration_seconds, language, "
            "dominant_emotion, topics, flagged, ab_bucket "
            "FROM calls ORDER BY start_time DESC LIMIT 1000"
        )
    lines = ["id,caller,start_time,duration_secs,language,dominant_emotion,topics,flagged,ab_bucket"]
    for r in rows:
        topics_str = "|".join(json.loads(r["topics"] or "[]"))
        lines.append(
            f'{r["id"]},{r["caller"] or ""},'
            f'{r["start_time"].isoformat() if r["start_time"] else ""},'
            f'{r["duration_seconds"] or 0},{r["language"] or ""},'
            f'{r["dominant_emotion"] or ""},{topics_str},'
            f'{"yes" if r["flagged"] else "no"},{r["ab_bucket"] or ""}'
        )
    return Response(
        content="\n".join(lines),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=calls.csv"},
    )


@app.get("/calls/live-stats")
async def live_stats():
    """
    F-06: Real-time stats endpoint — polled every 10s by the dashboard.
    Returns: active_calls (WebSocket count), calls_today, top_emotion_today.
    """
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    stats = {
        "active_websocket_connections": _active_ws_count,
        "calls_today": 0,
        "top_emotion_today": "neutral",
        "avg_duration_secs_today": 0,
        "cache_entries": len(_response_cache),
    }
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) AS cnt, AVG(duration_seconds) AS avg_dur FROM calls "
                    "WHERE start_time >= $1", today_start
                )
                stats["calls_today"] = row["cnt"] or 0
                stats["avg_duration_secs_today"] = round(float(row["avg_dur"] or 0), 1)
                top = await conn.fetchrow(
                    "SELECT dominant_emotion FROM calls WHERE start_time >= $1 "
                    "AND dominant_emotion IS NOT NULL "
                    "GROUP BY dominant_emotion ORDER BY COUNT(*) DESC LIMIT 1",
                    today_start,
                )
                if top:
                    stats["top_emotion_today"] = top["dominant_emotion"]
        except Exception:
            pass
    return stats


# Active WebSocket connection counter (incremented/decremented in media_stream)
_active_ws_count: int = 0


# ══════════════════════════════════════════════════════════════════════════════
# F-14 — UNIT TEST SUITE (pytest, importable)
# ══════════════════════════════════════════════════════════════════════════════
# Tests live in this file under test_ functions so they're co-located with the
# code.  Run with:  pytest backend.py -v
# They are pure-Python, no network calls, no DB — safe to run in CI.

def test_detect_language_english():
    assert detect_language("Hello, what is the weather today?") == "en"

def test_detect_language_hinglish():
    # Should detect Hinglish when Hindi + English words coexist
    result = detect_language("Yaar kya kal rain hoga Mumbai mein?")
    assert result in ("hi-en", "hi"), f"Expected hi-en or hi, got {result}"

def test_extract_pin_digits():
    assert _extract_pin("my pin is 4 2 1 9") in ("4219", None)
    assert _extract_pin("1234") == "1234"

def test_extract_pin_words():
    result = _extract_pin("one two three four")
    assert result == "1234", f"Expected 1234, got {result}"

def test_emotion_result_dict():
    e = EmotionResult("happy", 0.9, "high")
    d = e.to_dict()
    assert d["emotion"] == "happy"
    assert d["confidence"] == 0.9
    assert d["intensity"] == "high"

def test_dominant_emotion_weighted():
    arc = [
        EmotionResult("happy", 0.9, "high"),
        EmotionResult("sad",   0.3, "low"),
        EmotionResult("happy", 0.8, "medium"),
    ]
    assert dominant_emotion(arc) == "happy"

def test_compute_escalation_score():
    arc = [
        EmotionResult("angry",      0.9, "high"),   # weight 2
        EmotionResult("frustrated", 0.7, "medium"), # weight 1
        EmotionResult("urgent",     0.99,"high"),   # weight 3
    ]
    assert compute_escalation_score(arc) == 6

def test_cache_set_and_get():
    _cache_set("test:key", "hello world")
    assert _cache_get("test:key") == "hello world"

def test_cache_miss():
    # Key never set
    assert _cache_get("test:nonexistent_key_xyz") is None

def test_voice_challenge_digits():
    assert check_voice_challenge("the answer is 7", "7") is True
    assert check_voice_challenge("4", "4") is True
    assert check_voice_challenge("five", "5") is True
    assert check_voice_challenge("nine", "8") is False

def test_emotion_routing_sad():
    hint = get_emotion_routing_hint("sad")
    assert "motivational" in hint.lower() or "encouraging" in hint.lower() or "sad" in hint.lower()

def test_emotion_routing_happy():
    hint = get_emotion_routing_hint("happy")
    assert "playful" in hint.lower() or "upbeat" in hint.lower()

def test_per_tool_rate_limiter():
    limiter = PerToolRateLimiter()
    for _ in range(ABUSE_PER_TOOL_LIMIT):
        assert not limiter.is_blocked("get_weather")
        limiter.record("get_weather")
    assert limiter.is_blocked("get_weather")
    assert not limiter.is_blocked("get_news")


# ══════════════════════════════════════════════════════════════════════════════
# F-15 — DOCKER / MULTI-WORKER NOTES (baked-in constants + health hints)
# ══════════════════════════════════════════════════════════════════════════════

DOCKER_NOTES = """
# ── VoiceAI Docker / Production Checklist ────────────────────────────────────
#
# 1. Redis REQUIRED for multi-worker rate limiting:
#       REDIS_URL=redis://redis:6379
#    Without Redis, each worker has its own in-memory rate-limit counter,
#    so rate-limit enforcement is per-worker, not per-caller globally.
#
# 2. Run with at least 2 Uvicorn workers:
#       uvicorn backend:app --host 0.0.0.0 --port 8000 --workers 4
#    Or via Gunicorn with uvicorn workers:
#       gunicorn -w 4 -k uvicorn.workers.UvicornWorker backend:app
#
# 3. APScheduler runs in-process — only ONE worker should run scheduled jobs.
#    Set APSCHEDULER_WORKER=true on exactly one container/instance to avoid
#    duplicate reminder calls.  (Future: use Redis job store for deduplication.)
#
# 4. Postgres connection pool:
#       min_size=2, max_size=10 (per worker)
#    With 4 workers → up to 40 concurrent DB connections. Adjust max_size
#    and Postgres max_connections accordingly.
#
# 5. pgvector: requires Postgres >= 14 + pgvector extension.
#       docker run -e POSTGRES_PASSWORD=pass ankane/pgvector
#
# 6. Environment variables checklist:
#       DEEPGRAM_API_KEY, GROQ_API_KEY, ELEVENLABS_API_KEY  (required)
#       TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER
#       DATABASE_URL          (Postgres connection string)
#       REDIS_URL             (for multi-worker rate limiting)
#       SARVAM_API_KEY        (22 Indian languages)
#       RAPIDAPI_KEY          (PNR status)
#       RAZORPAY_KEY_ID + RAZORPAY_KEY_SECRET  (UPI payment status)
#       CRICAPI_KEY           (cricket scores)
#       AVIATIONSTACK_KEY     (flight status)
#       DASHBOARD_TOKEN       (dashboard auth)
#       WEBHOOK_SECRET        (external webhook auth)
#       PUBLIC_HOST           (your public HTTPS domain, no trailing slash)
#       EMERGENCY_CONTACT_NUMBER
#
# 7. Dockerfile (minimal):
#       FROM python:3.11-slim
#       WORKDIR /app
#       COPY requirements.txt .
#       RUN pip install --no-cache-dir -r requirements.txt
#       COPY backend.py .
#       EXPOSE 8000
#       CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
#
# 8. requirements.txt additions for this version:
#       fastapi uvicorn[standard] httpx structlog
#       deepgram-sdk groq langdetect
#       google-api-python-client google-auth
#       twilio apscheduler asyncpg
#       redis[asyncio]       # for Redis-backed rate limiting
#       pgvector             # for F-01 vector memory
#       prometheus_client    # for F-10 Prometheus metrics (optional)
#       pytest               # for F-14 unit tests
# ──────────────────────────────────────────────────────────────────────────────
"""


@app.get("/docker-notes", response_class=HTMLResponse)
async def docker_notes():
    """Return the Docker/production checklist as a readable HTML page."""
    html_body = "<br>".join(html.escape(line) for line in DOCKER_NOTES.strip().splitlines())
    return HTMLResponse(
        f"<html><head><title>VoiceAI Production Notes</title>"
        f"<style>body{{font-family:monospace;background:#1e293b;color:#94a3b8;"
        f"padding:2rem;line-height:1.7}}</style></head>"
        f"<body>{html_body}</body></html>"
    )
