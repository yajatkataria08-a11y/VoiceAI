"""
app/security/otp.py
PIN/OTP authentication for callers.
"""
import re
import secrets
from typing import Optional

from app.database.queries import get_or_create_pin


async def verify_pin(caller_number: str, entered_pin: str) -> bool:
    correct = await get_or_create_pin(caller_number)
    return secrets.compare_digest(correct, entered_pin)
