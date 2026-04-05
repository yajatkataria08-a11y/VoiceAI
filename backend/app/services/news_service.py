"""app/services/news_service.py"""
import httpx
from app.core.config import NEWS_API_KEY
from app.core.logging import log
from app.utils.helpers import cache_get, cache_set
from app.utils.text_cleaning import clean_headline


async def get_news(topic: str) -> str:
    if not NEWS_API_KEY:
        return "News service is not configured."
    key = f"news:{topic.lower()}"
    cached = cache_get(key)
    if cached:
        return cached
    url    = "https://newsapi.org/v2/top-headlines"
    params: dict = {"apiKey": NEWS_API_KEY, "pageSize": 3, "language": "en"}
    if topic.lower() == "general":
        params["country"] = "in"
    else:
        params["q"] = topic
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 401:
                return "News API key is invalid."
            resp.raise_for_status()
            articles = resp.json().get("articles", [])
        if not articles:
            return f"No news found for {topic}."
        headlines = [
            f"{i}. {clean_headline(a.get('title') or '')}"
            for i, a in enumerate(articles[:3], 1)
            if a.get("title")
        ]
        result = "Top headlines: " + " ".join(headlines) if headlines else f"No headlines for {topic}."
        cache_set(key, result)
        return result
    except Exception as exc:
        log.error("news_fetch_failed", topic=topic, error=str(exc))
        return "Sorry, I couldn't fetch the news."
