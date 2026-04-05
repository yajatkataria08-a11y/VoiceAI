"""
app/services/stt_service.py
STT routing: Deepgram (live WebSocket) + Sarvam/Bhashini batch fallback.
"""
from __future__ import annotations
import base64
import httpx

from app.core.config import (
    DEEPGRAM_API_KEY, SARVAM_API_KEY, SARVAM_STT_URL,
    BHASHINI_API_KEY, BHASHINI_USER_ID,
)
from app.core.constants import SARVAM_LANG_CODES, BHASHINI_LANG_CODES
from app.core.logging import log
from deepgram import DeepgramClient, LiveTranscriptionEvents, LiveOptions


def make_deepgram_connection(dg_client: DeepgramClient, on_transcript_cb, lang: str):
    """Create and register a Deepgram live connection. Returns the connection object."""
    from app.utils.language import get_deepgram_language
    conn = dg_client.listen.asynclive.v("1")
    conn.on(LiveTranscriptionEvents.Transcript, on_transcript_cb)
    return conn


def make_deepgram_options(lang: str) -> LiveOptions:
    from app.utils.language import get_deepgram_language
    return LiveOptions(
        model="nova-2",
        language=get_deepgram_language(lang),
        encoding="mulaw",
        sample_rate=8000,
        channels=1,
        interim_results=True,
        endpointing=300,
        smart_format=True,
    )


async def transcribe_batch(audio_bytes: bytes, lang: str = "en") -> str:
    """
    Batch STT fallback chain:
    1. Sarvam AI (best for Indian languages)
    2. Bhashini (ta/bn/mr)
    3. Empty string (IVR menu fallback will kick in)
    """
    if SARVAM_API_KEY and lang in SARVAM_LANG_CODES:
        try:
            result = await _stt_sarvam(audio_bytes, lang)
            if result:
                log.info("stt_batch", provider="sarvam", lang=lang)
                return result
        except Exception as exc:
            log.warning("sarvam_stt_failed", lang=lang, error=str(exc))

    if BHASHINI_API_KEY and lang in BHASHINI_LANG_CODES:
        try:
            result = await _stt_bhashini(audio_bytes, lang)
            if result:
                log.info("stt_batch", provider="bhashini", lang=lang)
                return result
        except Exception as exc:
            log.warning("bhashini_stt_failed", lang=lang, error=str(exc))

    log.warning("stt_all_providers_failed", lang=lang)
    return ""


async def _stt_sarvam(audio_bytes: bytes, lang: str) -> str:
    lang_code = SARVAM_LANG_CODES.get(lang, "hi-IN")
    headers   = {"api-subscription-key": SARVAM_API_KEY}
    files     = {"file": ("audio.wav", audio_bytes, "audio/wav")}
    data      = {"language_code": lang_code, "model": "saarika:v1", "with_timestamps": False}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(SARVAM_STT_URL, headers=headers, files=files, data=data)
        resp.raise_for_status()
        return resp.json().get("transcript", "")


async def _stt_bhashini(audio_bytes: bytes, lang: str) -> str:
    _CODES = {"ta": "ta-IN", "bn": "bn-IN", "mr": "mr-IN"}
    lang_code = _CODES.get(lang, "hi-IN")
    url     = "https://dhruva-api.bhashini.gov.in/services/inference/pipeline"
    headers = {"Authorization": BHASHINI_API_KEY, "Content-Type": "application/json", "userID": BHASHINI_USER_ID}
    payload = {
        "pipelineTasks": [{"taskType": "asr", "config": {
            "language": {"sourceLanguage": lang_code},
            "audioFormat": "wav", "samplingRate": 8000,
        }}],
        "inputData": {"audio": [{"audioContent": base64.b64encode(audio_bytes).decode()}]},
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()["pipelineResponse"][0]["output"][0].get("source", "")
