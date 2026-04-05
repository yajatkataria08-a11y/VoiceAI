"""app/services/entertainment_service.py — jokes, recipes, translation via Groq LLM."""
from app.core.config import GROQ_MODEL_FAST
from app.core.logging import log
from app.services.llm_service import groq_client


async def get_entertainment(type: str = "joke", language: str = "en") -> str:
    lang_hint = {"hi": "Respond in Hindi.", "hi-en": "Respond in fun Hinglish.",
                 "ta": "Respond in Tamil.", "bn": "Respond in Bengali.", "mr": "Respond in Marathi."
                }.get(language, "Respond in English.")
    prompts = {
        "joke":        f"Tell a clean, funny, family-friendly joke. {lang_hint} Keep it to 2-4 lines.",
        "story":       f"Tell a short inspirational story (4-5 sentences) for an elderly person. {lang_hint}",
        "trivia":      f"Give one interesting trivia fact about India or science (2 sentences). {lang_hint}",
        "bhajan_info": f"Describe one popular Indian bhajan in 2-3 sentences. {lang_hint}",
        "riddle":      f"Give a simple riddle with its answer. {lang_hint} Keep it brief.",
        "motivational":f"Give a short motivational quote (2-3 sentences). {lang_hint}",
    }
    prompt = prompts.get(type, f"Tell me something fun and interesting. {lang_hint}")
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST, messages=[{"role": "user", "content": prompt}],
            max_tokens=120, temperature=0.9,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("entertainment_failed", type=type, error=str(exc))
        return "Sorry, I couldn't fetch that right now."


async def get_recipe(dish: str) -> str:
    prompt = (
        "Give a very short, voice-friendly recipe. "
        "Format: list 3-4 key ingredients, then 4-5 numbered short steps. "
        f"Maximum 6 sentences. Dish: {dish}"
    )
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST, messages=[{"role": "user", "content": prompt}],
            max_tokens=200, temperature=0.4,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("recipe_failed", dish=dish, error=str(exc))
        return f"Sorry, I couldn't find a recipe for {dish} right now."


async def translate_text(text: str, target_language: str) -> str:
    prompt = f"Translate the following text to {target_language}. Reply with ONLY the translated text. Text: {text}"
    try:
        resp = await groq_client.chat.completions.create(
            model=GROQ_MODEL_FAST, messages=[{"role": "user", "content": prompt}],
            max_tokens=200, temperature=0.1,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("translation_failed", error=str(exc))
        return "Sorry, I couldn't translate that right now."
