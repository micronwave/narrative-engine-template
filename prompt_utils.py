"""Shared sanitization utilities for LLM prompt inputs and chat messages."""
import re

_CONTROL_CHAR_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')

_CHAT_MAX_LENGTH = 2000

_LEAKAGE_PATTERNS = [
    "system prompt",
    "my instructions",
    "i was told to",
    "my programming",
    "as an ai assistant, my instructions are",
]


def sanitize_for_prompt(text: str, max_len: int = 100) -> str:
    """Sanitize text before inserting into an LLM prompt."""
    text = _CONTROL_CHAR_RE.sub('', text)
    text = text.replace('\n', ' ').replace('\r', ' ')
    if len(text) > max_len:
        text = text[:max_len] + "..."
    return text.strip()


def sanitize_chat_input(message: str) -> str:
    """Sanitize user chat message: strip control chars, enforce length limit."""
    message = _CONTROL_CHAR_RE.sub('', message)
    if len(message) > _CHAT_MAX_LENGTH:
        message = message[:_CHAT_MAX_LENGTH]
    return message.strip()


def validate_chat_output(response: str) -> str:
    """Check for system prompt leakage in LLM response."""
    lower = response.lower()
    for pattern in _LEAKAGE_PATTERNS:
        if pattern in lower:
            return (
                "I can help you analyze market narratives and financial data. "
                "What would you like to know?"
            )
    return response


def strip_control_chars(text: str) -> str:
    """Strip control characters from text (keep newlines and tabs)."""
    return _CONTROL_CHAR_RE.sub('', text)
