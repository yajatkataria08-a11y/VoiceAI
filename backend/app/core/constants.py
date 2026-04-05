"""
app/core/constants.py
Static lookup tables, language maps, prompt strings, tool schemas.
Nothing here reads from os.environ — use config.py for that.
"""
from __future__ import annotations

# -- Sarvam language codes -----------------------------------------------------
SARVAM_LANG_CODES: dict[str, str] = {
    "hi": "hi-IN", "ta": "ta-IN", "bn": "bn-IN", "mr": "mr-IN",
    "te": "te-IN", "kn": "kn-IN", "gu": "gu-IN", "pa": "pa-IN",
    "ml": "ml-IN", "or": "or-IN", "as": "as-IN", "mai": "mai-IN",
    "mni": "mni-IN", "kok": "kok-IN", "ur": "ur-IN", "ne": "ne-IN",
    "hi-en": "hi-IN",
}
ALL_INDIAN_LANGS: frozenset[str] = frozenset(SARVAM_LANG_CODES.keys())
BHASHINI_LANG_CODES: set[str] = {"ta", "bn", "mr"}

SARVAM_SPEAKERS: dict[str, str] = {
    "hi": "meera", "hi-en": "meera", "ta": "arjun", "bn": "meera",
    "mr": "meera", "te": "meera", "kn": "meera", "gu": "meera",
    "pa": "meera", "ml": "meera", "or": "meera", "ur": "meera", "as": "meera",
}
SARVAM_SPEED_NORMAL = 1.0
SARVAM_SPEED_SLOW   = 0.8

# -- System prompts ------------------------------------------------------------
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

SYSTEM_PROMPT_HINGLISH = """You are a helpful voice assistant on the phone.
The caller speaks Hinglish — a natural mix of Hindi and English. Reply in the same
Hinglish style: friendly, short (1-3 sentences), and natural.
You can help with: mausam, news, train PNR, appointment booking, and general questions."""

LANG_SYSTEM_PROMPTS: dict[str, str] = {
    "en": SYSTEM_PROMPT_EN,
    "hi": SYSTEM_PROMPT_HI,
    "hi-en": SYSTEM_PROMPT_HINGLISH,
}

# -- Topic labels for call classification --------------------------------------
TOPIC_LABELS: list[str] = [
    "weather", "booking", "news", "pnr", "reminder", "callback", "general",
    "entertainment", "health", "cricket", "recipe", "flight",
    "smart_home", "family", "payment", "memo", "translation",
]

# -- Emotion system ------------------------------------------------------------
EMOTION_LABELS: list[str] = [
    "neutral", "happy", "frustrated", "sad", "confused", "angry", "urgent",
]

EMOTION_VOICE_SETTINGS: dict[str, dict] = {
    "urgent":     {"stability": 0.85, "similarity_boost": 0.60},
    "angry":      {"stability": 0.75, "similarity_boost": 0.65},
    "frustrated": {"stability": 0.70, "similarity_boost": 0.70},
    "sad":        {"stability": 0.72, "similarity_boost": 0.75},
    "confused":   {"stability": 0.65, "similarity_boost": 0.75},
    "happy":      {"stability": 0.45, "similarity_boost": 0.80},
    "neutral":    {"stability": 0.50, "similarity_boost": 0.75},
}

ESCALATION_WEIGHT: dict[str, int] = {
    "urgent": 3, "angry": 2, "frustrated": 1,
    "sad": 0, "confused": 0, "happy": 0, "neutral": 0,
}

MOTIVATIONAL_QUOTES: list[str] = [
    "Every day is a new beginning. Take a deep breath and start again.",
    "You are stronger than you think.",
    "Small steps every day lead to big changes.",
    "It's okay to ask for help. That's what I'm here for.",
    "Your best is always enough.",
]

INDIAN_FESTIVALS: list[dict] = [
    {"name": "Diwali",       "month": 10, "day": 20,
     "greeting_en": "Happy Diwali! May your life be filled with light and joy.",
     "greeting_hi": "दीपावली की हार्दिक शुभकामनाएं!"},
    {"name": "Holi",         "month":  3, "day": 25,
     "greeting_en": "Happy Holi! May the colours of joy fill your life.",
     "greeting_hi": "होली की हार्दिक शुभकामनाएं!"},
    {"name": "Eid",          "month":  3, "day": 30,
     "greeting_en": "Eid Mubarak! Wishing you peace and prosperity.",
     "greeting_hi": "ईद मुबारक!"},
    {"name": "Christmas",    "month": 12, "day": 25,
     "greeting_en": "Merry Christmas! Wishing you joy and peace.",
     "greeting_hi": "क्रिसमस की शुभकामनाएं!"},
    {"name": "New Year",     "month":  1, "day":  1,
     "greeting_en": "Happy New Year! May this year bring health and happiness.",
     "greeting_hi": "नया साल मुबारक!"},
    {"name": "Republic Day", "month":  1, "day": 26,
     "greeting_en": "Happy Republic Day! Jai Hind.",
     "greeting_hi": "गणतंत्र दिवस की शुभकामनाएं! जय हिंद।"},
    {"name": "Independence Day", "month": 8, "day": 15,
     "greeting_en": "Happy Independence Day! Jai Hind.",
     "greeting_hi": "स्वतंत्रता दिवस की शुभकामनाएं! जय हिंद।"},
]

# -- Groq tool schemas ---------------------------------------------------------
GROQ_TOOLS: list[dict] = [
    {"type": "function", "function": {
        "name": "get_weather",
        "description": "Get current weather for a city. Call when user asks about weather, temperature, rain, or climate.",
        "parameters": {"type": "object", "properties": {
            "city": {"type": "string", "description": "City name, e.g. 'Mumbai'. Include country code if ambiguous."},
        }, "required": ["city"]},
    }},
    {"type": "function", "function": {
        "name": "get_news",
        "description": "Fetch top 3 news headlines for a topic. Call when user asks about news, headlines, or current events.",
        "parameters": {"type": "object", "properties": {
            "topic": {"type": "string", "description": "News topic. Use 'general' if no specific topic given."},
        }, "required": ["topic"]},
    }},
    {"type": "function", "function": {
        "name": "book_appointment",
        "description": "Book appointment on Google Calendar. Call when user says 'book', 'schedule', 'set a meeting'.",
        "parameters": {"type": "object", "properties": {
            "title": {"type": "string"}, "date": {"type": "string", "description": "YYYY-MM-DD"},
            "time":  {"type": "string", "description": "HH:MM 24-hour"},
        }, "required": ["title", "date", "time"]},
    }},
    {"type": "function", "function": {
        "name": "schedule_callback",
        "description": "Schedule outbound callback. Call when user says 'call me back at X'.",
        "parameters": {"type": "object", "properties": {
            "callback_time": {"type": "string"}, "callback_date": {"type": "string"},
            "reason": {"type": "string"},
        }, "required": ["callback_time", "callback_date"]},
    }},
    {"type": "function", "function": {
        "name": "check_pnr",
        "description": "Check Indian railway PNR status. Call for train status, ticket confirmation, or 10-digit PNR numbers.",
        "parameters": {"type": "object", "properties": {
            "pnr": {"type": "string", "description": "10-digit PNR number."},
        }, "required": ["pnr"]},
    }},
    {"type": "function", "function": {
        "name": "check_upi_payment",
        "description": "Check UPI/Razorpay payment status. Call for payment queries or when user gives a payment/order ID.",
        "parameters": {"type": "object", "properties": {
            "payment_id": {"type": "string"},
        }, "required": ["payment_id"]},
    }},
    {"type": "function", "function": {
        "name": "get_entertainment",
        "description": "Entertainment: jokes, stories, trivia, bhajans, riddles. Call when user wants to be entertained.",
        "parameters": {"type": "object", "properties": {
            "type": {"type": "string", "enum": ["joke", "story", "trivia", "bhajan_info", "riddle", "motivational"]},
            "language": {"type": "string"},
        }, "required": ["type"]},
    }},
    {"type": "function", "function": {
        "name": "check_symptoms",
        "description": "Health guidance for symptoms with disclaimer. Also handles emergency commands like 'call 112'.",
        "parameters": {"type": "object", "properties": {
            "symptoms": {"type": "string"}, "emergency": {"type": "boolean"},
        }, "required": ["symptoms"]},
    }},
    {"type": "function", "function": {
        "name": "get_cricket_score",
        "description": "Live cricket scores. Call when user asks about cricket, IPL, or names a team playing cricket.",
        "parameters": {"type": "object", "properties": {
            "team": {"type": "string", "description": "Team name or 'live' for all matches."},
        }, "required": ["team"]},
    }},
    {"type": "function", "function": {
        "name": "get_recipe",
        "description": "Voice-friendly recipe with steps. Call when user asks for a recipe or 'kaise banate hain'.",
        "parameters": {"type": "object", "properties": {
            "dish": {"type": "string"},
        }, "required": ["dish"]},
    }},
    {"type": "function", "function": {
        "name": "translate_text",
        "description": "Translate text between languages. Call when user says 'translate' or 'English mein batao'.",
        "parameters": {"type": "object", "properties": {
            "text": {"type": "string"}, "target_language": {"type": "string"},
        }, "required": ["text", "target_language"]},
    }},
    {"type": "function", "function": {
        "name": "save_memo",
        "description": "Save a voice memo for the caller. Call when user says 'note karo', 'save this', or 'remember this'.",
        "parameters": {"type": "object", "properties": {
            "memo": {"type": "string"},
        }, "required": ["memo"]},
    }},
    {"type": "function", "function": {
        "name": "recall_memos",
        "description": "Retrieve caller's saved memos. Call when user says 'mera note sunao' or 'what did I save'.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "get_flight_status",
        "description": "Live flight status by flight number. Call when user asks about a flight, departure, or delay.",
        "parameters": {"type": "object", "properties": {
            "flight_number": {"type": "string", "description": "IATA flight number e.g. 'AI202'."},
        }, "required": ["flight_number"]},
    }},
    {"type": "function", "function": {
        "name": "control_smart_device",
        "description": "Control smart-home devices. Call when user says 'light on', 'fan off', 'AC chhalu karo'.",
        "parameters": {"type": "object", "properties": {
            "device": {"type": "string"},
            "action": {"type": "string", "enum": ["on", "off", "toggle", "set_temperature", "lock", "unlock"]},
            "value": {"type": "string"},
        }, "required": ["device", "action"]},
    }},
    {"type": "function", "function": {
        "name": "notify_family",
        "description": "Send message to caller's saved family contact. Call when user says 'beta ko batao' or 'tell my son'.",
        "parameters": {"type": "object", "properties": {
            "message": {"type": "string"}, "recipient_name": {"type": "string"},
        }, "required": ["message"]},
    }},
]
