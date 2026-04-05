"""
app/services/llm_service.py
Groq LLM: get_llm_reply, emotion detection, entity extraction,
intent confidence, conversation summarisation, topic classification.
"""
from __future__ import annotations
import asyncio
import json
import re
import random
from collections import defaultdict
from typing import Optional

from groq import AsyncGroq

from app.core.config import GROQ_API_KEY, GROQ_MODEL_MAIN, GROQ_MODEL_FAST, PGVECTOR_ENABLED
from app.core.constants import (
    GROQ_TOOLS, EMOTION_LABELS, EMOTION_VOICE_SETTINGS, ESCALATION_WEIGHT,
    TOPIC_LABELS, LANG_SYSTEM_PROMPTS, SYSTEM_PROMPT_EN, MOTIVATIONAL_QUOTES,
)
from app.core.logging import log
from app.utils.helpers import api_call_with_retry
from app.security.rate_limit import PerToolRateLimiter

groq_client = AsyncGroq(api_key=GROQ_API_KEY)

# ---------------------------------------------------------------------------
# Emotion detection
# ---------------------------------------------------------------------------

import re as _re
_URGENCY_KEYWORDS = _re.compile(
    r"\b(help|emergency|ambulance|police|fire|dying|chest pain|"
    r"can't breathe|not breathing|accident|bleeding|attack|"
    r"please help|someone help|call 911|call 112)\b",
    _re.IGNORECASE,
)


class EmotionResult:
    __slots__ = ("emotion", "confidence", "intensity")

    def __init__(self, emotion: str, confidence: float, intensity: str) -> None:
        self.emotion    = emotion
        self.confidence = confidence
        self.intensity  = intensity

    def to_dict(self) -> dict:
        return {"emotion": self.emotion, "confidence": self.confidence, "intensity": self.intensity}


async def detect_emotion(text: str) -> EmotionResult:
    if _URGENCY_KEYWORDS.search(text):
        return EmotionResult("urgent", 0.99, "high")
    prompt = (
        "Classify the emotion in this spoken message. "
        "Reply with ONLY a JSON object — no markdown, no explanation. "
        'Format: {"emotion": "<label>", "confidence": <0.0-1.0>, "intensity": "<low|medium|high>"} '
        f"Valid labels: {', '.join(EMOTION_LABELS[:-1])}\n\nMessage: " + text
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=40, temperature=0.0,
        )
        raw  = resp.choices[0].message.content.strip()
        raw  = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        emotion    = data.get("emotion", "neutral").lower()
        confidence = float(data.get("confidence", 0.5))
        intensity  = data.get("intensity", "medium").lower()
        if emotion not in EMOTION_LABELS:  emotion = "neutral"
        confidence = max(0.0, min(1.0, confidence))
        if intensity not in ("low", "medium", "high"): intensity = "medium"
        return EmotionResult(emotion, confidence, intensity)
    except Exception as exc:
        log.error("emotion_detection_failed", error=str(exc))
        return EmotionResult("neutral", 0.5, "low")


def get_emotion_tone_instruction(result: EmotionResult) -> str:
    if result.confidence < 0.6:
        return ""
    iq = {"low": "slightly ", "medium": "", "high": "very "}.get(result.intensity, "")
    return {
        "urgent":     "URGENT: Caller may be in danger. Stay calm. Tell them to call 112 if emergency.",
        "frustrated": f"Caller sounds {iq}frustrated. Be calm, patient, apologetic. Acknowledge frustration first.",
        "angry":      f"Caller sounds {iq}angry. De-escalate. Stay calm and brief. Do not argue.",
        "sad":        f"Caller sounds {iq}sad. Be warm, gentle, and supportive.",
        "confused":   f"Caller sounds {iq}confused. Be very clear and simple. Offer to repeat.",
        "happy":      "Caller sounds happy. Match their energy — warm and upbeat.",
        "neutral":    "",
    }.get(result.emotion, "")


def get_voice_settings_for_emotion(emotion: str) -> dict:
    return EMOTION_VOICE_SETTINGS.get(emotion, EMOTION_VOICE_SETTINGS["neutral"])


def compute_escalation_score(emotion_arc: list[EmotionResult]) -> int:
    return sum(ESCALATION_WEIGHT.get(r.emotion, 0) for r in emotion_arc[-3:])


def dominant_emotion(emotion_arc: list[EmotionResult]) -> str:
    if not emotion_arc:
        return "neutral"
    counts: dict[str, float] = defaultdict(float)
    for r in emotion_arc:
        counts[r.emotion] += r.confidence
    return max(counts, key=lambda k: counts[k])


def get_emotion_routing_hint(emotion: str, lang: str = "en") -> str:
    if emotion == "sad":
        quote = random.choice(MOTIVATIONAL_QUOTES)
        return (
            f"The caller sounds sad. Warmly share: '{quote}' "
            "Then ask if they'd like you to notify a family member."
        )
    if emotion == "happy":
        return "The caller is in a great mood! Be slightly playful and upbeat."
    if emotion == "confused":
        return "Caller is confused. Ask ONE simple clarifying question before proceeding."
    return ""


# ---------------------------------------------------------------------------
# System prompt builder
# ---------------------------------------------------------------------------

def get_system_prompt(
    lang: str,
    emotion_result: Optional[EmotionResult] = None,
    context: Optional[dict] = None,
    caller_profile: Optional[dict] = None,
    elderly_mode: bool = False,
) -> str:
    base = LANG_SYSTEM_PROMPTS.get(lang, SYSTEM_PROMPT_EN)
    if elderly_mode:
        base += "\n\nSpeak SLOWLY and CLEARLY. Use very simple words. Repeat important info."
    if emotion_result:
        tone = get_emotion_tone_instruction(emotion_result)
        if tone:
            base += f"\n\n{tone}"
    if caller_profile:
        parts = []
        if caller_profile.get("name"):
            parts.append(f"Caller's name is {caller_profile['name']} — greet them.")
        if caller_profile.get("city"):
            parts.append(f"Their usual city is {caller_profile['city']}.")
        if caller_profile.get("last_topic"):
            parts.append(f"Last time they asked about: {caller_profile['last_topic']}.")
        if parts:
            base += "\n\nReturning caller: " + " ".join(parts)
    if context:
        ctx = [f"{k}={v}" for k, v in context.items() if v]
        if ctx:
            base += f"\n\nKnown context: {', '.join(ctx)}. Use so caller doesn't repeat themselves."
    return base.strip()


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

async def extract_entities(text: str, existing: dict) -> dict:
    prompt = (
        "Extract named entities from this voice message. "
        "Reply ONLY with compact JSON (no markdown). "
        "Keys allowed: city, person_name, date (YYYY-MM-DD), pnr (10 digits). "
        "Only include clearly present keys. "
        "Message: " + text
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60, temperature=0.0,
        )
        raw  = resp.choices[0].message.content.strip()
        raw  = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        merged = {**existing}
        for k, v in data.items():
            if v and k in ("city", "person_name", "date", "pnr"):
                merged[k] = str(v)
        return merged
    except Exception:
        return existing


# ---------------------------------------------------------------------------
# Intent confidence check
# ---------------------------------------------------------------------------

async def check_reply_confidence(user_text: str, ai_reply: str) -> float:
    prompt = (
        "Rate how confidently this reply addresses the user's request. "
        'Reply ONLY with JSON: {"confidence": <0.0-1.0>}. '
        f"\nUser: {user_text}\nAssistant: {ai_reply}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=20, temperature=0.0,
        )
        raw  = resp.choices[0].message.content.strip()
        raw  = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        return max(0.0, min(1.0, float(json.loads(raw).get("confidence", 0.8))))
    except Exception:
        return 0.8


# ---------------------------------------------------------------------------
# Conversation summarisation
# ---------------------------------------------------------------------------

async def summarize_old_turns(old_turns: list[dict]) -> str:
    formatted = "\n".join(f"{t['role'].capitalize()}: {t['content']}" for t in old_turns)
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[
                {"role": "system", "content": "Summarize the following conversation in 2-3 sentences. Focus on key info and actions taken."},
                {"role": "user", "content": formatted},
            ],
            max_tokens=150, temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("summarization_failed", error=str(exc))
        return "Earlier conversation context unavailable."


async def maybe_compress_conversation(conversation: list[dict]) -> list[dict]:
    if len(conversation) <= 20:
        return conversation
    system_msgs    = [m for m in conversation[:10] if m.get("role") == "system"]
    turns_to_sum   = [m for m in conversation[:10] if m.get("role") != "system"]
    summary        = await summarize_old_turns(turns_to_sum) if turns_to_sum else ""
    summary_block  = [{"role": "system", "content": f"Earlier in this call: {summary}"}] if summary else []
    return system_msgs + summary_block + conversation[10:]


# ---------------------------------------------------------------------------
# Topic classification
# ---------------------------------------------------------------------------

async def classify_call_topics(transcript: list[dict]) -> list[str]:
    user_turns = " | ".join(t["content"] for t in transcript if t.get("role") == "user")
    if not user_turns:
        return ["general"]
    prompt = (
        f"Classify this call into topics. Valid: {', '.join(TOPIC_LABELS)}. "
        "Reply ONLY with a JSON array. Example: [\"weather\", \"booking\"]\n\nTranscript: " + user_turns[:500]
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=40, temperature=0.0,
        )
        raw    = resp.choices[0].message.content.strip()
        raw    = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        topics = json.loads(raw)
        return [t for t in topics if t in TOPIC_LABELS] or ["general"]
    except Exception:
        return ["general"]


# ---------------------------------------------------------------------------
# Clarifying question generator
# ---------------------------------------------------------------------------

async def ask_clarifying_question(user_text: str, lang: str) -> str:
    prompt = (
        "The user said something unclear. Ask ONE short clarifying question "
        f"(max 1 sentence). Language: {lang}. User said: {user_text}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60, temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return "Could you please repeat that? I didn't quite catch what you need."


# ---------------------------------------------------------------------------
# Main LLM reply
# ---------------------------------------------------------------------------

async def get_llm_reply(
    conversation: list[dict],
    lang: str = "en",
    caller_number: str = "",
    emotion_result: Optional[EmotionResult] = None,
    escalation_hint: str = "",
    context: Optional[dict] = None,
    caller_profile: Optional[dict] = None,
    elderly_mode: bool = False,
    tool_limiter: Optional[PerToolRateLimiter] = None,
) -> str:
    # Import here to avoid circular imports (dispatch needs services, services need llm)
    from app.services.tool_dispatch import dispatch_tool_call

    system_prompt = get_system_prompt(lang, emotion_result, context, caller_profile, elderly_mode)
    if escalation_hint:
        system_prompt += f"\n\n{escalation_hint}"

    messages = [{"role": "system", "content": system_prompt}] + conversation

    async def _call():
        resp   = await groq_client.chat.completions.create(
            model=GROQ_MODEL_MAIN, messages=messages,
            tools=GROQ_TOOLS, tool_choice="auto",
            max_tokens=150, temperature=0.6,
        )
        choice = resp.choices[0]

        if choice.finish_reason != "tool_calls":
            return (choice.message.content or "").strip()

        tool_calls = choice.message.tool_calls
        messages.append({
            "role": "assistant", "content": choice.message.content or "",
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
            if tool_limiter and tool_limiter.is_blocked(tc.function.name):
                log.warning("tool_blocked", tool=tc.function.name)
                messages.append({"role": "tool", "tool_call_id": tc.id,
                                  "content": f"Tool {tc.function.name} is temporarily unavailable."})
                continue
            if tool_limiter:
                tool_limiter.record(tc.function.name)
            result = await dispatch_tool_call(tc.function.name, tool_args, caller_number)
            log.info("tool_result", tool=tc.function.name, result=result[:120])
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

        second = await groq_client.chat.completions.create(
            model=GROQ_MODEL_MAIN, messages=messages, max_tokens=120, temperature=0.6,
        )
        return second.choices[0].message.content.strip()

    return await api_call_with_retry(_call)
