"""app/services/flight_service.py"""
import httpx
from app.core.config import AVIATIONSTACK_KEY
from app.core.logging import log


async def get_flight_status(flight_number: str) -> str:
    if not AVIATIONSTACK_KEY:
        return "Flight status service is not configured. Please set AVIATIONSTACK_KEY."
    flight_number = flight_number.replace(" ", "").upper()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "http://api.aviationstack.com/v1/flights",
                params={"access_key": AVIATIONSTACK_KEY, "flight_iata": flight_number},
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
        if not data:
            return f"No flight data found for {flight_number}."
        f = data[0]
        dep  = f.get("departure", {})
        arr  = f.get("arrival", {})
        status = f.get("flight_status", "unknown")
        return (
            f"Flight {flight_number}: {status}. "
            f"Departs {dep.get('airport','?')} at {dep.get('estimated','?')}. "
            f"Arrives {arr.get('airport','?')} at {arr.get('estimated','?')}."
        )
    except Exception as exc:
        log.error("flight_status_failed", flight=flight_number, error=str(exc))
        return "Sorry, I couldn't fetch the flight status right now."
