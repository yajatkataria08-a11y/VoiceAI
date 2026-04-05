"""
app/security/abuse_detection.py
Voice CAPTCHA challenge generation/verification and abuse flagging.
"""
from __future__ import annotations
import random
import re


async def generate_voice_challenge() -> tuple[str, str]:
    """Generate an arithmetic CAPTCHA. Returns (question_text, expected_answer)."""
    a, b = random.randint(1, 9), random.randint(1, 9)
    op   = random.choice(["+", "-"])
    answer = a + b if op == "+" else a - b
    question = f"To continue, please answer: what is {a} {op} {b}?"
    return question, str(answer)


def check_voice_challenge(spoken: str, expected: str) -> bool:
    """Check spoken answer against expected. Handles digits and word-form numbers."""
    digits = re.sub(r"\D", "", spoken)
    if digits == expected:
        return True
    word_map = {
        "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
        "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9",
        "ten": "10", "eleven": "11", "twelve": "12", "thirteen": "13",
        "fourteen": "14", "fifteen": "15", "sixteen": "16",
        "seventeen": "17", "eighteen": "18",
    }
    for word, digit in word_map.items():
        if word in spoken.lower() and digit == expected:
            return True
    return False
