# Shared state for scheduler jobs
_sent_reminders: set[str] = set()  # gcal event IDs already reminded
