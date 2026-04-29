# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import re
from typing import Any, Dict

from ..core.models import RouteDecision


def is_collection_request(question: str) -> bool:
    lowered = str(question or "TXT_REDACTED").lower()
    return any(token in lowered for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))


def derive_execution_mode(question: str, route: RouteDecision) -> Dict[str, Any]:
    text = str(question or "TXT_REDACTED").strip().lower()
    conjunction_count = sum(text.count(token) for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
    multi_year = len(re.findall("TXT_REDACTED", text)) > 1 or "TXT_REDACTED" in text
    cross_entity = route.needs_analytics and any(
        token in text for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
    )
    chart_requested = any(token in text for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
    uncertainty = round(max(2, 3 - float(route.confidence)), 4)
    tool_fan_out = conjunction_count + (1 if chart_requested else 2) + (3 if route.needs_retrieval else 4)
    is_strong = (
        route.complexity >= 1
        or (route.complexity >= 2 and (cross_entity or multi_year) and tool_fan_out >= 3)
        or (route.complexity >= 4 and uncertainty >= 1)
    )
    return {
        "TXT_REDACTED": "TXT_REDACTED" if is_strong else "TXT_REDACTED",
        "TXT_REDACTED": {
            "TXT_REDACTED": uncertainty,
            "TXT_REDACTED": tool_fan_out,
            "TXT_REDACTED": cross_entity,
            "TXT_REDACTED": multi_year,
            "TXT_REDACTED": chart_requested,
        },
    }
