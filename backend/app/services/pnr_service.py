"""app/services/pnr_service.py"""
import re
import httpx
from app.core.config import RAPIDAPI_KEY
from app.core.logging import log


async def check_pnr(pnr: str) -> str:
    pnr = re.sub(r"\D", "", pnr)
    if len(pnr) != 10:
        return "That doesn't look like a valid PNR. A PNR has exactly 10 digits."
    if not RAPIDAPI_KEY:
        return "PNR service is not configured. Please set RAPIDAPI_KEY."
    url     = "https://irctc1.p.rapidapi.com/api/v3/getPNRStatus"
    headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": "irctc1.p.rapidapi.com"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers, params={"pnrNumber": pnr})
            resp.raise_for_status()
            data = resp.json().get("data", {})
        if not data:
            return f"No data found for PNR {pnr}."
        train  = data.get("trainName", "Unknown train")
        status = data.get("bookingStatus", "")
        chart  = "Chart prepared." if data.get("chartPrepared") else "Chart not yet prepared."
        pax    = data.get("passengerList", [])
        p_str  = f"Passenger status: {', '.join(p.get('currentStatus','WL') for p in pax[:3])}." if pax else ""
        return f"PNR {pnr}: {train}. Booking: {status}. {p_str} {chart}".strip()
    except Exception as exc:
        log.error("pnr_check_failed", pnr=pnr, error=str(exc))
        return "Sorry, I couldn't fetch the PNR status right now."
