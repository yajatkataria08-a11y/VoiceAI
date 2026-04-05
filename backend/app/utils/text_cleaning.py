"""
app/utils/text_cleaning.py
HTML stripping, PIN extraction, text normalisation.
"""
import html
import re
from typing import Optional


def strip_html(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text)).strip()


def extract_pin(text: str) -> Optional[str]:
    """Extract 4-digit PIN from spoken text (handles digit words too)."""
    digits = re.sub(r"\D", "", text)
    if len(digits) == 4:
        return digits
    word_to_digit = {
        "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
        "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
        "ek": "1", "do": "2", "teen": "3", "char": "4", "paanch": "5",
        "chhe": "6", "saat": "7", "aath": "8", "nau": "9", "shoonya": "0",
    }
    spoken = re.findall(
        r"\b(?:zero|one|two|three|four|five|six|seven|eight|nine|"
        r"ek|do|teen|char|paanch|chhe|saat|aath|nau|shoonya)\b",
        text, re.IGNORECASE,
    )
    if len(spoken) == 4:
        return "".join(word_to_digit[w.lower()] for w in spoken)
    return None


def clean_headline(title: str) -> str:
    """Remove source attribution suffix from news headlines."""
    return re.sub(r"\s*-\s*[^-]+$", "", strip_html(title)).strip()
