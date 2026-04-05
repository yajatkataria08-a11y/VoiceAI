"""
app/telecom/audio_pipeline.py
Deepgram session lifecycle: start, language-switch restart, audio buffer flush.
"""
from __future__ import annotations
import asyncio
from collections import deque
from typing import Optional

from deepgram import DeepgramClient, LiveOptions

from app.core.config import DEEPGRAM_API_KEY
from app.core.logging import log
from app.services.stt_service import make_deepgram_options
from app.utils.language import get_deepgram_language


class AudioPipeline:
    """
    Manages a single Deepgram live WebSocket connection per call.
    Handles language switching (closes old connection, opens new one)
    and early-audio buffering (BUG FIX 6).
    """

    def __init__(self, on_transcript_cb) -> None:
        self._cb          = on_transcript_cb
        self._dg_client   = DeepgramClient(DEEPGRAM_API_KEY)
        self._conn        = None
        self._lang        = "en"
        self._audio_buf: deque[bytes] = deque(maxlen=200)
        self._started     = False

    async def start(self, lang: str) -> None:
        self._lang = lang
        conn = self._dg_client.listen.asynclive.v("1")
        conn.on("Transcript", self._cb)
        await conn.start(make_deepgram_options(lang))
        self._conn    = conn
        self._started = True
        log.info("deepgram_started", language=get_deepgram_language(lang))

    async def switch_language(self, new_lang: str) -> None:
        """Close current Deepgram connection and reopen with new language."""
        if self._conn:
            await self._conn.finish()
        self._lang = new_lang
        await self.start(new_lang)
        log.info("deepgram_lang_switched", lang=new_lang)

    async def send(self, audio: bytes, stream_sid_known: bool = True) -> None:
        if not stream_sid_known:
            self._audio_buf.append(audio)
            return
        if self._conn:
            await self._conn.send(audio)

    async def flush_buffer(self) -> None:
        """Replay buffered early frames after stream_sid arrives (BUG FIX 6)."""
        while self._audio_buf and self._conn:
            await self._conn.send(self._audio_buf.popleft())

    async def finish(self) -> None:
        if self._conn:
            await self._conn.finish()
            self._conn = None
