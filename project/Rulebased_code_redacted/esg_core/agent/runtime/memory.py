# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from typing import Any, Dict

from ..core.models import NormalizedQuery
from ..retrieval.query_memory import QueryMemory
from ..schema.navigator import NavigationResult


def remember_query_path(
    *,
    query_memory: QueryMemory,
    question: str,
    normalized_query: NormalizedQuery,
    navigation: NavigationResult,
    prompt_versions: Dict[str, str],
    semantic_interpretation: Dict[str, Any],
) -> Dict[str, Any]:
    if navigation.selected is None:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
        }
    if not query_memory.enabled:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
        }
    try:
        memory_result = query_memory.add(
            question=question,
            normalized_query=normalized_query.to_dict(),
            selected_path=navigation.selected.path,
            status=navigation.status,
            strategy={
                "TXT_REDACTED": navigation.selected.entry_type,
                "TXT_REDACTED": navigation.selected.source_kind,
                "TXT_REDACTED": semantic_interpretation,
            },
            versions={
                "TXT_REDACTED": prompt_versions.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": prompt_versions.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": prompt_versions.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": prompt_versions.get("TXT_REDACTED", "TXT_REDACTED"),
            },
        )
        if not memory_result.get("TXT_REDACTED", False):
            return {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": {
                    "TXT_REDACTED": str(memory_result.get("TXT_REDACTED") or "TXT_REDACTED"),
                    "TXT_REDACTED": memory_result.get("TXT_REDACTED"),
                    "TXT_REDACTED": memory_result.get("TXT_REDACTED"),
                    "TXT_REDACTED": memory_result.get("TXT_REDACTED"),
                },
            }
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": navigation.selected.path,
                "TXT_REDACTED": navigation.status,
            },
        }
    except Exception as exc:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": str(exc),
                "TXT_REDACTED": navigation.selected.path,
            },
        }
