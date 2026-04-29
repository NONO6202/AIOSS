# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from typing import Final


AUTO_FIELD_IDS: Final[list[str]] = [
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]


AGENT_FIELD_IDS: Final[list[str]] = [
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    # REDACTED
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]


MANUAL_FIELD_IDS: Final[list[str]] = [
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]


ALL_FIELD_IDS: Final[list[str]] = AUTO_FIELD_IDS + AGENT_FIELD_IDS + MANUAL_FIELD_IDS


AGENT_STUB_REASONS: Final[dict[str, str]] = {
    field_id: "TXT_REDACTED"
    for field_id in AGENT_FIELD_IDS
}


MANUAL_STUB_REASONS: Final[dict[str, str]] = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}


def breadth_stub_value(field_id: str) -> str | None:
    "TXT_REDACTED"
    if field_id in AGENT_STUB_REASONS:
        return "TXT_REDACTED"
    if field_id in MANUAL_STUB_REASONS:
        return "TXT_REDACTED"
    return None
