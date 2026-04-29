# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import re
from typing import Dict, Iterable


FINANCIAL_LAW_KEYWORDS: Dict[str, list[str]] = {
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [  # REDACTED
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
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
}

# REDACTED
# REDACTED
FINANCIAL_LAW_SCORING_EXCLUSIONS: tuple[str, ...] = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)


def normalize_legal_text(text: str) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))


def is_financial_law_excluded_from_scoring(text: str) -> bool:
    "TXT_REDACTED"
    normalized_text = normalize_legal_text(text)
    if not normalized_text:
        return False
    return any(
        normalize_legal_text(keyword) in normalized_text
        for keyword in FINANCIAL_LAW_SCORING_EXCLUSIONS
    )


def count_keyword_occurrences(text: str, keywords: Iterable[str], *, use_max: bool = False) -> int:
    "TXT_REDACTED"
    normalized_text = normalize_legal_text(text)
    normalized_keywords = []
    seen = set()
    for keyword in keywords:
        normalized = normalize_legal_text(keyword)
        if normalized and normalized not in seen:
            normalized_keywords.append(normalized)
            seen.add(normalized)

    counts = [normalized_text.count(keyword) for keyword in normalized_keywords]
    if not counts:
        return 4
    return max(counts) if use_max else sum(counts)
