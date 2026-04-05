"""
app/core/config.py
All environment variables and computed settings in one place.
Import from here everywhere — never call os.environ directly in other modules.
"""
import os
import re

# -- Core APIs -----------------------------------------------------------------
DEEPGRAM_API_KEY            = os.environ["DEEPGRAM_API_KEY"]
GROQ_API_KEY                = os.environ["GROQ_API_KEY"]
ELEVENLABS_API_KEY          = os.environ["ELEVENLABS_API_KEY"]
ELEVENLABS_VOICE_EN         = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
ELEVENLABS_VOICE_EN_ALT     = os.environ.get("ELEVENLABS_VOICE_ID_ALT", "AZnzlk1XvdvUeBnXmlld")
ELEVENLABS_VOICE_HI         = os.environ.get("ELEVENLABS_HINDI_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
ELEVENLABS_VOICE_CLONED     = os.environ.get("ELEVENLABS_VOICE_CLONED", "")

# -- Weather / News / Payments -------------------------------------------------
OPENWEATHERMAP_API_KEY      = os.environ.get("OPENWEATHERMAP_API_KEY", "")
NEWS_API_KEY                = os.environ.get("NEWS_API_KEY", "")
RAPIDAPI_KEY                = os.environ.get("RAPIDAPI_KEY", "")
RAZORPAY_KEY_ID             = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET         = os.environ.get("RAZORPAY_KEY_SECRET", "")
CRICAPI_KEY                 = os.environ.get("CRICAPI_KEY", "")
AVIATIONSTACK_KEY           = os.environ.get("AVIATIONSTACK_KEY", "")

# -- Google Calendar -----------------------------------------------------------
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_CALENDAR_ID          = os.environ.get("GOOGLE_CALENDAR_ID", "primary")
TIMEZONE                    = os.environ.get("TIMEZONE", "Asia/Kolkata")

# -- Twilio -------------------------------------------------------------------
TWILIO_ACCOUNT_SID          = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN           = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER         = os.environ.get("TWILIO_PHONE_NUMBER", "")
EMERGENCY_CONTACT_NUMBER    = os.environ.get("EMERGENCY_CONTACT_NUMBER", "")

# -- Database / Cache ----------------------------------------------------------
DATABASE_URL                = os.environ.get("DATABASE_URL", "")
REDIS_URL                   = os.environ.get("REDIS_URL", "")

# -- Indian language APIs (Sarvam / Bhashini) ----------------------------------
SARVAM_API_KEY              = os.environ.get("SARVAM_API_KEY", "")
SARVAM_STT_URL              = "https://api.sarvam.ai/speech-to-text"
SARVAM_TTS_URL              = "https://api.sarvam.ai/text-to-speech"
BHASHINI_API_KEY            = os.environ.get("BHASHINI_API_KEY", "")
BHASHINI_USER_ID            = os.environ.get("BHASHINI_USER_ID", "")

# -- Smart home (Tuya) ---------------------------------------------------------
TUYA_CLIENT_ID              = os.environ.get("TUYA_CLIENT_ID", "")
TUYA_CLIENT_SECRET          = os.environ.get("TUYA_CLIENT_SECRET", "")
TUYA_BASE_URL               = os.environ.get("TUYA_BASE_URL", "https://openapi.tuyaeu.com")

# -- Feature flags -------------------------------------------------------------
OTP_PIN_REQUIRED            = os.environ.get("OTP_PIN_REQUIRED", "false").lower() == "true"
OTP_MASTER_PIN              = os.environ.get("OTP_MASTER_PIN", "")
IVR_CONFIDENCE_THRESHOLD    = float(os.environ.get("IVR_CONFIDENCE_THRESHOLD", "0.6"))
AB_TEST_ENABLED             = os.environ.get("AB_VOICE_TEST", "false").lower() == "true"
ELDERLY_MODE_DEFAULT        = os.environ.get("ELDERLY_MODE_DEFAULT", "false").lower() == "true"
DAILY_BRIEFING_ENABLED      = os.environ.get("DAILY_BRIEFING_ENABLED", "false").lower() == "true"
DAILY_BRIEFING_MORNING_TIME = os.environ.get("DAILY_BRIEFING_MORNING_TIME", "07:00")
DAILY_BRIEFING_EVENING_TIME = os.environ.get("DAILY_BRIEFING_EVENING_TIME", "19:00")
CALL_RECORDING_ENABLED      = os.environ.get("CALL_RECORDING_ENABLED", "false").lower() == "true"
SURVEY_ENABLED              = os.environ.get("SURVEY_ENABLED", "false").lower() == "true"
PGVECTOR_ENABLED            = os.environ.get("PGVECTOR_ENABLED", "false").lower() == "true"
VOICE_CHALLENGE_ENABLED     = os.environ.get("VOICE_CHALLENGE_ENABLED", "false").lower() == "true"

# -- Limits & thresholds -------------------------------------------------------
RATE_LIMIT_MAX_CALLS        = int(os.environ.get("RATE_LIMIT_MAX_CALLS", "5"))
RATE_LIMIT_WINDOW_SECS      = int(os.environ.get("RATE_LIMIT_WINDOW_SECS", "3600"))
SESSION_TIMEOUT_SECS        = int(os.environ.get("SESSION_TIMEOUT_SECS", "120"))
ABUSE_URGENCY_THRESHOLD     = int(os.environ.get("ABUSE_URGENCY_THRESHOLD", "4"))
ABUSE_PER_TOOL_LIMIT        = int(os.environ.get("ABUSE_PER_TOOL_LIMIT", "5"))

# -- Admin / monitoring --------------------------------------------------------
DASHBOARD_TOKEN             = os.environ.get("DASHBOARD_TOKEN", "")
ADMIN_ALERT_NUMBER          = os.environ.get("ADMIN_ALERT_NUMBER", "")
PUBLIC_HOST                 = os.environ.get("PUBLIC_HOST", "your-server.ngrok.io")

# -- Groq models ---------------------------------------------------------------
GROQ_MODEL_MAIN = "llama-3.3-70b-versatile"
GROQ_MODEL_FAST = "llama-3.1-8b-instant"

# -- Computed / derived --------------------------------------------------------
ELDERLY_SLOW_DOWN_PHRASES = re.compile(
    r"\b(slow down|speak slowly|too fast|repeat|bolna dhire|dhire bolo|samajh nahi aaya)\b",
    re.IGNORECASE,
)
