"""app/services/payment_service.py — Razorpay/UPI payment status."""
import re
import httpx
from app.core.config import RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET
from app.core.logging import log


async def check_upi_payment(payment_id: str) -> str:
    payment_id = payment_id.strip()
    if not payment_id:
        return "I didn't catch the payment ID. Could you say it again?"
    if not (RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET):
        return "Payment status service is not configured."
    if not payment_id.lower().startswith("pay_"):
        payment_id = "pay_" + re.sub(r"\s+", "", payment_id)
    try:
        async with httpx.AsyncClient(timeout=10, auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET)) as client:
            resp = await client.get(f"https://api.razorpay.com/v1/payments/{payment_id}")
            if resp.status_code == 404:
                return f"No payment found with ID {payment_id}."
            resp.raise_for_status()
            data = resp.json()
        status = {"captured": "successful", "refunded": "refunded", "failed": "failed",
                  "created": "pending"}.get(data.get("status",""), data.get("status","unknown"))
        amount = data.get("amount", 0) / 100
        method = data.get("method", "")
        return f"Payment {payment_id}: Rs.{amount:.2f} via {method}. Status: {status}."
    except Exception as exc:
        log.error("upi_payment_check_failed", payment_id=payment_id, error=str(exc))
        return "Sorry, I couldn't fetch the payment status right now."
