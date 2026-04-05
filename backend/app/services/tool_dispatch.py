"""
app/services/tool_dispatch.py
Single routing function: tool name → async handler.
Every tool in GROQ_TOOLS must have a branch here.
"""
from __future__ import annotations
from datetime import datetime

from app.core.logging import log


async def dispatch_tool_call(tool_name: str, tool_args: dict, caller_number: str = "") -> str:
    """Route a Groq tool_call to the correct async handler."""

    # -- Core tools -----------------------------------------------------------
    if tool_name == "get_weather":
        from app.services.weather_service import get_weather
        return await get_weather(tool_args.get("city", ""))

    if tool_name == "get_news":
        from app.services.news_service import get_news
        return await get_news(tool_args.get("topic", "general"))

    if tool_name == "book_appointment":
        from app.services.calendar_service import book_appointment
        return await book_appointment(
            title=tool_args.get("title", "Appointment"),
            date=tool_args.get("date", ""),
            time_str=tool_args.get("time", "09:00"),
            caller_number=caller_number,
        )

    if tool_name == "schedule_callback":
        from app.services.calendar_service import schedule_callback
        return await schedule_callback(
            callback_time=tool_args.get("callback_time", ""),
            callback_date=tool_args.get("callback_date", datetime.now().strftime("%Y-%m-%d")),
            reason=tool_args.get("reason", ""),
            caller_number=caller_number,
        )

    if tool_name == "check_pnr":
        from app.services.pnr_service import check_pnr
        return await check_pnr(tool_args.get("pnr", ""))

    if tool_name == "check_upi_payment":
        from app.services.payment_service import check_upi_payment
        return await check_upi_payment(tool_args.get("payment_id", ""))

    # -- Family notification --------------------------------------------------
    if tool_name == "notify_family":
        from app.services.family_service import notify_family
        return await notify_family(
            caller_number=caller_number,
            message=tool_args.get("message", ""),
            recipient_name=tool_args.get("recipient_name", ""),
        )

    # -- Voice memos ----------------------------------------------------------
    if tool_name == "save_memo":
        from app.services.memo_service import save_memo
        return await save_memo(memo=tool_args.get("memo", ""), caller_number=caller_number)

    if tool_name == "recall_memos":
        from app.services.memo_service import recall_memos
        return await recall_memos(caller_number=caller_number)

    # -- Flight status --------------------------------------------------------
    if tool_name == "get_flight_status":
        from app.services.flight_service import get_flight_status
        return await get_flight_status(tool_args.get("flight_number", ""))

    # -- Smart home -----------------------------------------------------------
    if tool_name == "control_smart_device":
        from app.services.smart_home_service import control_smart_device
        return await control_smart_device(
            device=tool_args.get("device", ""),
            action=tool_args.get("action", ""),
            value=tool_args.get("value", ""),
        )

    # -- Entertainment --------------------------------------------------------
    if tool_name == "get_entertainment":
        from app.services.entertainment_service import get_entertainment
        return await get_entertainment(
            type=tool_args.get("type", "joke"),
            language=tool_args.get("language", "en"),
        )

    # -- Health guidance ------------------------------------------------------
    if tool_name == "check_symptoms":
        from app.services.health_service import check_symptoms
        return await check_symptoms(
            symptoms=tool_args.get("symptoms", ""),
            emergency=bool(tool_args.get("emergency", False)),
        )

    # -- Cricket scores -------------------------------------------------------
    if tool_name == "get_cricket_score":
        from app.services.cricket_service import get_cricket_score
        return await get_cricket_score(tool_args.get("team", "live"))

    # -- Recipes --------------------------------------------------------------
    if tool_name == "get_recipe":
        from app.services.entertainment_service import get_recipe
        return await get_recipe(tool_args.get("dish", ""))

    # -- Translation ----------------------------------------------------------
    if tool_name == "translate_text":
        from app.services.entertainment_service import translate_text
        return await translate_text(
            text=tool_args.get("text", ""),
            target_language=tool_args.get("target_language", "English"),
        )

    log.warning("unknown_tool_called", tool=tool_name, args=tool_args)
    return f"Sorry, I don't know how to handle '{tool_name}' yet."
