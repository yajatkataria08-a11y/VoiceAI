"""app/services/family_service.py — notify family contacts via SMS/WhatsApp."""
import json
from app.core.logging import log
from app.database.db import db_pool
from app.telecom.twilio_handler import send_sms


async def notify_family(caller_number: str, message: str, recipient_name: str = "") -> str:
    if not caller_number:
        return "I need your phone number to send a family message."
    if not message:
        return "What message would you like me to send?"
    family_contacts: list[dict] = []
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT family_contacts FROM caller_profiles WHERE caller=$1", caller_number
                )
            if row and row.get("family_contacts"):
                family_contacts = json.loads(row["family_contacts"] or "[]")
        except Exception as exc:
            log.warning("family_contacts_load_failed", error=str(exc))
    if not family_contacts:
        return "You don't have any family contacts saved. Please add them via the app first."
    contact = family_contacts[0]
    if recipient_name:
        for fc in family_contacts:
            if recipient_name.lower() in fc.get("name", "").lower():
                contact = fc
                break
    import asyncio
    loop = asyncio.get_running_loop()
    full_msg = f"Message from {caller_number}: {message} (Sent via VoiceAI)"
    await loop.run_in_executor(None, send_sms, contact.get("phone", ""), full_msg)
    return f"Message sent to {contact.get('name', 'your contact')}."
