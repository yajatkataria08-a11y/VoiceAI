"""app/services/health_service.py — symptom guidance with disclaimer."""
from app.core.config import GROQ_MODEL_FAST
from app.core.logging import log
from app.services.llm_service import groq_client

_EMERGENCY_KEYWORDS = ["ambulance", "112", "emergency", "heart attack", "chest pain",
                        "not breathing", "unconscious", "bleeding badly"]


async def check_symptoms(symptoms: str, emergency: bool = False) -> str:
    if emergency or any(kw in symptoms.lower() for kw in _EMERGENCY_KEYWORDS):
        return (
            "EMERGENCY: Please call 112 immediately for an ambulance. "
            "If you cannot call, ask someone nearby to call for you."
        )
    prompt = (
        "You are a helpful health information assistant (NOT a doctor). "
        "Give brief, simple, practical first-aid advice for the described symptoms. "
        "Always end with: 'Please consult a doctor for proper diagnosis.' "
        f"Keep to 3-4 short sentences. Symptoms: {symptoms}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST, messages=[{"role": "user", "content": prompt}],
            max_tokens=120, temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("symptoms_check_failed", error=str(exc))
        return "I cannot give medical advice right now. Please consult a doctor or call 112 in an emergency."
