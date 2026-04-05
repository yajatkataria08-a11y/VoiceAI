"""app/services/memo_service.py — voice memo save/recall."""
from app.core.logging import log
from app.database.queries import db_save_memo, db_recall_memos


async def save_memo(memo: str, caller_number: str = "") -> str:
    if not caller_number:
        return "I need your phone number to save a memo."
    if not memo:
        return "I didn't catch what you want to save. Please try again."
    try:
        await db_save_memo(caller_number, memo)
        return f"Got it! I've saved your note: '{memo[:80]}{'...' if len(memo)>80 else ''}'."
    except Exception as exc:
        log.error("save_memo_failed", caller=caller_number, error=str(exc))
        return "Sorry, I couldn't save your memo right now."


async def recall_memos(caller_number: str = "") -> str:
    if not caller_number:
        return "I need your phone number to retrieve your memos."
    try:
        memos = await db_recall_memos(caller_number)
        if not memos:
            return "You don't have any saved memos yet."
        return "Your memos: " + ". Next: ".join(memos)
    except Exception as exc:
        log.error("recall_memos_failed", caller=caller_number, error=str(exc))
        return "Sorry, I couldn't retrieve your memos right now."
