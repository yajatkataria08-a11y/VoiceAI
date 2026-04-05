"""
app/security/request_validation.py
Twilio webhook signature verification.
"""
from fastapi import Request

from app.core.config import TWILIO_AUTH_TOKEN


def verify_twilio_signature(request: Request, form_data: dict) -> bool:
    """Return True if the request is genuinely from Twilio."""
    if not TWILIO_AUTH_TOKEN:
        return True  # skip in dev
    from twilio.request_validator import RequestValidator
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    url       = str(request.url)
    signature = request.headers.get("X-Twilio-Signature", "")
    return validator.validate(url, form_data, signature)
