"""app/services/smart_home_service.py — Tuya IoT integration stub."""
from app.core.config import TUYA_CLIENT_ID, TUYA_CLIENT_SECRET
from app.core.logging import log


async def control_smart_device(device: str, action: str, value: str = "") -> str:
    if not (TUYA_CLIENT_ID and TUYA_CLIENT_SECRET):
        return "Smart home control is not configured. Please set TUYA_CLIENT_ID and TUYA_CLIENT_SECRET."
    log.info("smart_device_intent", device=device, action=action, value=value)
    action_map = {
        "on": f"Turning on {device}.", "off": f"Turning off {device}.",
        "toggle": f"Toggling {device}.", "lock": f"Locking {device}.",
        "unlock": f"Unlocking {device}.",
        "set_temperature": f"Setting {device} to {value}°C.",
    }
    return action_map.get(action, f"Performing {action} on {device}.") + " (Command logged.)"
