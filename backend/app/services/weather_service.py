"""app/services/weather_service.py"""
import httpx
from app.core.config import OPENWEATHERMAP_API_KEY
from app.core.logging import log
from app.utils.helpers import api_call_with_retry, cache_get, cache_set


async def _fetch_weather(city: str) -> str:
    if not OPENWEATHERMAP_API_KEY:
        return "Weather service is not configured."
    url    = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": OPENWEATHERMAP_API_KEY, "units": "metric"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        if resp.status_code == 404:
            return f"Sorry, I couldn't find weather data for {city}."
        resp.raise_for_status()
        d = resp.json()
    return (
        f"{d['name']}, {d['sys']['country']}: {round(d['main']['temp'])}°C, "
        f"feels like {round(d['main']['feels_like'])}°C, "
        f"humidity {d['main']['humidity']}%, {d['weather'][0]['description']}."
    )


async def get_weather(city: str) -> str:
    key = f"weather:{city.lower()}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        result = await api_call_with_retry(_fetch_weather, city)
        cache_set(key, result)
        return result
    except Exception as exc:
        log.error("weather_fetch_failed", city=city, error=str(exc))
        return "Sorry, I couldn't fetch the weather right now."
