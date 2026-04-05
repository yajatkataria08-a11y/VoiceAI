"""
app/utils/language.py
Language detection, voice/Deepgram routing helpers.
"""
from __future__ import annotations
import hashlib
import re
from typing import Optional

from langdetect import detect, LangDetectException

from app.core.config import (
    SARVAM_API_KEY, BHASHINI_API_KEY,
    ELEVENLABS_VOICE_EN, ELEVENLABS_VOICE_EN_ALT, ELEVENLABS_VOICE_HI,
    ELEVENLABS_VOICE_CLONED, AB_TEST_ENABLED,
)
from app.core.constants import ALL_INDIAN_LANGS, BHASHINI_LANG_CODES

_HINDI_WORDS = re.compile(
    r"\b(hai|hain|kya|nahi|nahin|karo|karna|bolo|batao|mujhe|aapka|"
    r"aur|lekin|toh|yaar|kal|aaj|abhi|thoda|bahut|accha|theek|sahi|"
    r"bhai|didi|ji|haan|nahi ji|kar|raha|rahi|ho|hoga)\b",
    re.IGNORECASE,
)

_DEEPGRAM_LANG_MAP: dict[str, str] = {
    "en": "en-US", "hi": "hi", "hi-en": "hi-en",
    "ta": "ta", "bn": "bn", "mr": "mr", "te": "te",
    "kn": "kn", "gu": "gu", "pa": "pa", "ml": "ml", "ur": "ur",
}


def detect_language(text: str) -> str:
    """
    Detect caller language. Returns: hi, hi-en, ta, bn, mr, te, kn, gu, pa, ml, or, en.
    Priority: Hinglish > pure Hindi > Sarvam Indian lang > Bhashini Indian lang > English.
    """
    try:
        detected = detect(text)
    except LangDetectException:
        detected = "en"

    hindi_hits  = len(_HINDI_WORDS.findall(text))
    has_english = bool(re.search(r"[a-zA-Z]{3,}", text))

    if hindi_hits >= 2 and has_english:
        return "hi-en"
    if detected == "hi":
        return "hi"
    if detected in ALL_INDIAN_LANGS and SARVAM_API_KEY:
        return detected
    if detected in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return detected
    return "en"


def get_elevenlabs_voice(
    lang: str,
    caller_number: str = "",
    caller_profile: Optional[dict] = None,
) -> str:
    """
    Voice priority:
    1. Per-caller cloned voice (from /voice/clone).
    2. Global brand voice (ELEVENLABS_VOICE_CLONED env).
    3. Hindi/Hinglish voice.
    4. A/B bucket.
    5. Default EN voice.
    """
    if caller_profile and caller_profile.get("cloned_voice_id"):
        return caller_profile["cloned_voice_id"]
    if ELEVENLABS_VOICE_CLONED:
        return ELEVENLABS_VOICE_CLONED
    if lang in ("hi", "hi-en"):
        return ELEVENLABS_VOICE_HI
    if AB_TEST_ENABLED and caller_number:
        bucket = int(hashlib.md5(caller_number.encode()).hexdigest(), 16) % 2
        return ELEVENLABS_VOICE_EN_ALT if bucket == 1 else ELEVENLABS_VOICE_EN
    return ELEVENLABS_VOICE_EN


def get_deepgram_language(lang: str) -> str:
    return _DEEPGRAM_LANG_MAP.get(lang, "en-US")


def get_ab_bucket(caller_number: str) -> str:
    """Return A or B bucket label for a caller number."""
    if not caller_number:
        return "A"
    bucket = int(hashlib.md5(caller_number.encode()).hexdigest(), 16) % 2
    return "B" if bucket == 1 else "A"
