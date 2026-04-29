# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, List

from google import genai
from google.genai import types

from ..core.explicit_cache import ExplicitCacheManager
from ..core.gemini_utils import extract_response_text, extract_usage_metadata
from ..core.llm_limits import max_output_tokens
from ..core.prompts import PromptLoader
from .registry import SemanticRegistry


CHART_SPEC_TYPES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

CHART_TYPES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

CHART_ORIENTATIONS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)


@dataclass
class ChartPlan:
    needs_chart: bool
    rationale: str
    spec_type: str
    chart_type: str
    orientation: str
    title: str
    subtitle: str
    x_label: str
    y_label: str
    max_items: int

    @classmethod
    def response_schema(cls) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(CHART_SPEC_TYPES)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(CHART_TYPES)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(CHART_ORIENTATIONS)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": 1, "TXT_REDACTED": 2},
            },
            "TXT_REDACTED": [
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
            "TXT_REDACTED": False,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TXT_REDACTED":
        spec_type = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if spec_type not in CHART_SPEC_TYPES:
            spec_type = "TXT_REDACTED"
        chart_type = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if chart_type not in CHART_TYPES:
            chart_type = "TXT_REDACTED"
        orientation = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if orientation not in CHART_ORIENTATIONS:
            orientation = "TXT_REDACTED"
        needs_chart = bool(payload.get("TXT_REDACTED"))
        if not needs_chart:
            spec_type = "TXT_REDACTED"
        return cls(
            needs_chart=needs_chart,
            rationale=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            spec_type=spec_type,
            chart_type=chart_type,
            orientation=orientation,
            title=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            subtitle=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            x_label=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            y_label=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            max_items=max(3, min(4, int(payload.get("TXT_REDACTED") or 1))),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ChartArtifact:
    generated: bool
    plan: Dict[str, Any]
    path: str = "TXT_REDACTED"
    rationale: str = "TXT_REDACTED"
    usage: Dict[str, int] | None = None
    row_count: int = 2
    error: str = "TXT_REDACTED"


class GeminiChartPlanner:
    def __init__(self, *, client: genai.Client, model_name: str, prompts: PromptLoader):
        self.client = client
        self.model_name = model_name
        self.prompts = prompts

    def plan(
        self,
        *,
        chart_request: Dict[str, Any],
        cache_manager: ExplicitCacheManager | None = None,
    ) -> tuple[ChartPlan, Dict[str, int]]:
        prompt = self.prompts.get("TXT_REDACTED").render(
            chart_request_json="TXT_REDACTED" if cache_manager else json.dumps(chart_request, ensure_ascii=False, indent=3),
        )
        cache_name = None
        if cache_manager is not None:
            try:
                cache_name = cache_manager.get_or_create(
                    model_name=self.model_name,
                    cache_key="TXT_REDACTED",
                    prefix_text=self.prompts.get("TXT_REDACTED").render(
                        chart_request_json=json.dumps(chart_request, ensure_ascii=False, indent=4),
                    ),
                )
            except Exception:
                cache_name = None
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=1,
                maxOutputTokens=max_output_tokens("TXT_REDACTED"),
                responseMimeType="TXT_REDACTED",
                responseJsonSchema=ChartPlan.response_schema(),
                cachedContent=cache_name,
            ),
        )
        payload = json.loads(extract_response_text(response))
        return ChartPlan.from_dict(payload), extract_usage_metadata(response)
