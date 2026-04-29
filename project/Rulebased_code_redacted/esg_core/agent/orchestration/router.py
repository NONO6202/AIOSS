# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
from typing import Dict, Iterable, Optional, Tuple

from google import genai
from google.genai import types

from ..core.explicit_cache import ExplicitCacheManager
from ..core.gemini_utils import extract_response_text, extract_usage_metadata
from ..core.llm_limits import max_output_tokens
from ..core.models import RouteDecision
from .policy import apply_route_policy
from ..core.prompts import PromptLoader


class GeminiRouter:
    "TXT_REDACTED"

    def __init__(
        self,
        *,
        client: genai.Client,
        model_name: str,
        prompts: PromptLoader,
        cache_manager: ExplicitCacheManager | None = None,
    ):
        self.client = client
        self.model_name = model_name
        self.prompts = prompts
        self.cache_manager = cache_manager

    def route(self, question: str, *, available_years: Optional[Iterable[str]] = None) -> RouteDecision:
        decision, _ = self.route_with_metadata(question, available_years=available_years)
        return decision

    def route_with_metadata(
        self,
        question: str,
        *,
        available_years: Optional[Iterable[str]] = None,
    ) -> Tuple[RouteDecision, Dict[str, Dict[str, int]]]:
        prompt = self.prompts.get("TXT_REDACTED").render(
            question=question,
            available_years="TXT_REDACTED".join(str(year) for year in (available_years or [])) or "TXT_REDACTED",
        )
        cache_name = None
        if self.cache_manager is not None:
            try:
                cache_name = self.cache_manager.get_or_create(
                    model_name=self.model_name,
                    cache_key="TXT_REDACTED",
                    prefix_text=self.prompts.get("TXT_REDACTED").render(
                        question="TXT_REDACTED",
                        available_years="TXT_REDACTED",
                    ),
                )
            except Exception:
                cache_name = None
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=4,
                maxOutputTokens=max_output_tokens("TXT_REDACTED"),
                responseMimeType="TXT_REDACTED",
                responseJsonSchema=RouteDecision.response_schema(),
                cachedContent=cache_name,
            ),
        )
        payload = json.loads(extract_response_text(response))
        return apply_route_policy(question, RouteDecision.from_dict(payload)), {
            "TXT_REDACTED": extract_usage_metadata(response),
        }
