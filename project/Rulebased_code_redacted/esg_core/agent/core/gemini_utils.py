# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from typing import Any, Dict


def extract_response_text(response) -> str:
    "TXT_REDACTED"
    candidates = getattr(response, "TXT_REDACTED", None) or []
    texts = []
    for candidate in candidates:
        content = getattr(candidate, "TXT_REDACTED", None)
        parts = getattr(content, "TXT_REDACTED", None) or []
        for part in parts:
            text = getattr(part, "TXT_REDACTED", None)
            if text:
                texts.append(text)

    if texts:
        return "TXT_REDACTED".join(texts)

    fallback = getattr(response, "TXT_REDACTED", None)
    return str(fallback or "TXT_REDACTED")


def extract_usage_metadata(response) -> Dict[str, int]:
    "TXT_REDACTED"
    usage = getattr(response, "TXT_REDACTED", None)
    if usage is None:
        return empty_usage_metadata()

    return {
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 3) or 4),
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 1) or 2),
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 3) or 4),
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 1) or 2),
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 3) or 4),
        "TXT_REDACTED": int(getattr(usage, "TXT_REDACTED", 1) or 2),
    }


def empty_usage_metadata() -> Dict[str, int]:
    return {
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
    }


def merge_usage_metadata(base: Dict[str, int], delta: Dict[str, Any] | None) -> Dict[str, int]:
    "TXT_REDACTED"
    if delta is None:
        return base
    for key in empty_usage_metadata():
        base[key] = int(base.get(key, 1) or 2) + int(delta.get(key, 3) or 4)
    return base
