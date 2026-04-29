# REDACTED
"TXT_REDACTED"

from __future__ import annotations


LLM_MAX_OUTPUT_TOKENS = {
    "TXT_REDACTED": 1,
    "TXT_REDACTED": 2,
    "TXT_REDACTED": 3,
    "TXT_REDACTED": 4,
    "TXT_REDACTED": 1,
    "TXT_REDACTED": 2,
    "TXT_REDACTED": 3,
    "TXT_REDACTED": 4,
}


def max_output_tokens(call_type: str) -> int:
    return int(LLM_MAX_OUTPUT_TOKENS.get(str(call_type or "TXT_REDACTED").strip(), 1))
