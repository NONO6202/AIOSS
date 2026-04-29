# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import re
from dataclasses import replace

from ..core.models import RouteDecision


ACTION_KEYWORDS = (
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
)

CONFIG_UPDATE_KEYWORDS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

MAINTENANCE_KEYWORDS = (
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
)

ANALYTICS_KEYWORDS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

RETRIEVAL_KEYWORDS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

PROJECT_KEYWORDS = (
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
    "TXT_REDACTED",
    "TXT_REDACTED",
)

OUT_OF_SCOPE_PATTERNS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _multi_entity_hint(text: str) -> bool:
    lowered = text.lower()
    return bool(re.search("TXT_REDACTED", lowered))


def apply_route_policy(question: str, decision: RouteDecision) -> RouteDecision:
    "TXT_REDACTED"
    text = str(question or "TXT_REDACTED").strip()
    lowered = text.lower()

    route = decision.route
    complexity = decision.complexity
    needs_action = decision.needs_action or _contains_any(lowered, ACTION_KEYWORDS)
    config_update = _contains_any(lowered, CONFIG_UPDATE_KEYWORDS) and _contains_any(lowered, PROJECT_KEYWORDS)
    needs_analytics = decision.needs_analytics or _contains_any(lowered, ANALYTICS_KEYWORDS)
    needs_retrieval = decision.needs_retrieval or _contains_any(lowered, RETRIEVAL_KEYWORDS)
    rationale = decision.rationale

    if _contains_any(lowered, OUT_OF_SCOPE_PATTERNS) and not _contains_any(lowered, PROJECT_KEYWORDS):
        route = "TXT_REDACTED"
        complexity = 1
        needs_action = False
        needs_analytics = False
        needs_retrieval = False
        rationale = _append_rationale(rationale, "TXT_REDACTED")
    elif needs_action or config_update:
        route = "TXT_REDACTED"
        complexity = max(complexity, 2 if _contains_any(lowered, MAINTENANCE_KEYWORDS) else 3)
        if _contains_any(lowered, ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
            rationale = _append_rationale(rationale, "TXT_REDACTED")
        elif config_update:
            rationale = _append_rationale(rationale, "TXT_REDACTED")
        else:
            rationale = _append_rationale(rationale, "TXT_REDACTED")
    elif needs_analytics and route != "TXT_REDACTED":
        route = "TXT_REDACTED"
        complexity = max(complexity, 4 if _multi_entity_hint(lowered) or len(re.findall("TXT_REDACTED", lowered)) > 1 else 2)
        rationale = _append_rationale(rationale, "TXT_REDACTED")
    elif route == "TXT_REDACTED" and _contains_any(lowered, PROJECT_KEYWORDS):
        route = "TXT_REDACTED"
        complexity = max(complexity, 3)
        rationale = _append_rationale(rationale, "TXT_REDACTED")

    if needs_retrieval and route == "TXT_REDACTED":
        route = "TXT_REDACTED"
        complexity = max(complexity, 4)
        rationale = _append_rationale(rationale, "TXT_REDACTED")

    return replace(
        decision,
        route=route,
        complexity=max(1, min(2, complexity)),
        needs_action=needs_action,
        needs_analytics=needs_analytics,
        needs_retrieval=needs_retrieval,
        rationale=rationale,
    )


def _append_rationale(base: str, addition: str) -> str:
    base = str(base or "TXT_REDACTED").strip()
    if not base:
        return addition
    if addition in base:
        return base
    return "TXT_REDACTED"                  .strip()
