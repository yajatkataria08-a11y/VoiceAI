"""
app/services/tts_service.py
TTS routing:
  1. Sarvam AI  — Indian languages (best quality)
  2. Bhashini   — ta/bn/mr fallback
  3. ElevenLabs — English / Hinglish / default
"""
from __future__ import annotations
import base64
import asyncio
import httpx

from app.core.config import (
    ELEVENLABS_API_KEY, SARVAM_API_KEY, SARVAM_TTS_URL,
    BHASHINI_API_KEY, BHASHINI_USER_ID,
)
from app.core.constants import SARVAM_LANG_CODES, SARVAM_SPEAKERS, BHASHINI_LANG_CODES, EMOTION_VOICE_SETTINGS
from app.core.logging import log
from app.utils.helpers import api_call_with_retry
from app.utils.language import get_elevenlabs_voice


async def text_to_speech(
    text: str,
    lang: str = "en",
    emotion: str = "neutral",
    caller_number: str = "",
    caller_profile: dict = None,
    slow_mode: bool = False,
) -> bytes:
    """
    Route TTS to the best provider for the language and return raw audio bytes
    in μ-law 8kHz format (native Twilio format).
    """
    is_indian = lang in SARVAM_LANG_CODES

    if SARVAM_API_KEY and is_indian:
        return await api_call_with_retry(_tts_sarvam, text, lang, slow_mode)

    if lang in BHASHINI_LANG_CODES and BHASHINI_API_KEY:
        return await api_call_with_retry(_tts_bhashini, text, lang)

    return await api_call_with_retry(
        _tts_elevenlabs, text, lang, emotion, caller_number, caller_profile
    )


async def _tts_elevenlabs(
    text: str,
    lang: str,
    emotion: str = "neutral",
    caller_number: str = "",
    caller_profile: dict = None,
) -> bytes:
    voice_id       = get_elevenlabs_voice(lang, caller_number, caller_profile)
    voice_settings = EMOTION_VOICE_SETTINGS.get(emotion, EMOTION_VOICE_SETTINGS["neutral"])
    url     = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
    headers = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"}
    payload = {
        "text":           text,
        "model_id":       "eleven_turbo_v2",
        "voice_settings": voice_settings,
        "output_format":  "ulaw_8000",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.content


async def _tts_sarvam(text: str, lang: str, slow_mode: bool = False) -> bytes:
    lang_code = SARVAM_LANG_CODES.get(lang, "hi-IN")
    speaker   = SARVAM_SPEAKERS.get(lang, "meera")
    speed     = 0.8 if slow_mode else 1.0
    headers   = {"api-subscription-key": SARVAM_API_KEY, "Content-Type": "application/json"}
    payload   = {
        "inputs": [text],
        "target_language_code": lang_code,
        "speaker": speaker,
        "model":   "bulbul:v1",
        "enable_preprocessing": True,
        "speech_sample_rate": 8000,
        "pace": speed,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(SARVAM_TTS_URL, headers=headers, json=payload)
        resp.raise_for_status()
        return base64.b64decode(resp.json()["audios"][0])


async def _tts_bhashini(text: str, lang: str) -> bytes:
    _CODES = {"ta": "ta-IN", "bn": "bn-IN", "mr": "mr-IN"}
    lang_code = _CODES.get(lang, "hi-IN")
    url     = "https://dhruva-api.bhashini.gov.in/services/inference/pipeline"
    headers = {"Authorization": BHASHINI_API_KEY, "Content-Type": "application/json", "userID": BHASHINI_USER_ID}
    payload = {
        "pipelineTasks": [{"taskType": "tts", "config": {
            "language": {"sourceLanguage": lang_code}, "gender": "female"
        }}],
        "inputData": {"input": [{"source": text}]},
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        audio_b64 = resp.json()["pipelineResponse"][0]["audio"][0]["audioContent"]
        return base64.b64decode(audio_b64)
