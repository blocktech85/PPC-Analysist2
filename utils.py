"""Shared utilities."""
import re


def redact_api_keys(text: str) -> str:
    """Remove API keys from error messages/URLs so they are never shown to users."""
    if not text or not isinstance(text, str):
        return text
    # Match api_key= followed by value (alphanumeric, hyphen, underscore) until next & or end
    text = re.sub(r"api_key=[a-zA-Z0-9_-]+", "api_key=***REDACTED***", text, flags=re.IGNORECASE)
    # Also key= in case some APIs use different param name
    text = re.sub(r"key=[a-zA-Z0-9_-]{20,}", "key=***REDACTED***", text, flags=re.IGNORECASE)
    return text
