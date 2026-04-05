"""
app/telecom/barge_in.py
Barge-in (caller interrupts AI speech): cancel TTS task + send Twilio clear event.
"""
from __future__ import annotations
import asyncio
from typing import Optional

from fastapi import WebSocket


async def handle_barge_in(
    ws: WebSocket,
    stream_sid: Optional[str],
    tts_task: Optional[asyncio.Task],
    is_speaking: bool,
) -> None:
    """Cancel in-flight TTS and clear Twilio audio queue on barge-in (BUG FIX 4)."""
    if tts_task and not tts_task.done():
        tts_task.cancel()
    if is_speaking and stream_sid:
        await ws.send_json({"event": "clear", "streamSid": stream_sid})
