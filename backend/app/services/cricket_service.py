"""app/services/cricket_service.py"""
import httpx
from app.core.config import CRICAPI_KEY
from app.core.logging import log


async def get_cricket_score(team: str = "live") -> str:
    if not CRICAPI_KEY:
        return "Cricket score service is not configured. Please set CRICAPI_KEY."
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.cricapi.com/v1/cricScore", params={"apikey": CRICAPI_KEY})
            resp.raise_for_status()
            matches = resp.json().get("data", [])
        if not matches:
            return "No live cricket matches right now."
        if team.lower() != "live":
            matches = [m for m in matches if team.lower() in (m.get("t1","") + m.get("t2","")).lower()]
        if not matches:
            return f"No live matches found for {team}."
        results = [
            f"{m.get('t1','')} {m.get('t1s','')} vs {m.get('t2','')} {m.get('t2s','')}. {m.get('status','')}".strip()
            for m in matches[:3]
        ]
        return " | ".join(results)
    except Exception as exc:
        log.error("cricket_score_failed", team=team, error=str(exc))
        return "Sorry, I couldn't fetch cricket scores right now."
