# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json

from google.genai import types

from ..core.explicit_cache import ExplicitCacheManager
from ..core.gemini_utils import extract_response_text, extract_usage_metadata
from ..core.llm_limits import max_output_tokens
from ..core.models import DerivationRuleUpdate
from ..core.prompts import PromptLoader


class DerivationRuleParser:
    def __init__(self, *, client, model_name: str, prompts: PromptLoader, cache_manager: ExplicitCacheManager | None = None):
        self.client = client
        self.model_name = model_name
        self.prompts = prompts
        self.cache_manager = cache_manager

    def parse(self, question: str) -> tuple[DerivationRuleUpdate, dict[str, int]]:
        prompt = self.prompts.get("TXT_REDACTED").render(question=question)
        cache_name = None
        if self.cache_manager is not None:
            try:
                cache_name = self.cache_manager.get_or_create(
                    model_name=self.model_name,
                    cache_key="TXT_REDACTED",
                    prefix_text=self.prompts.get("TXT_REDACTED").render(question="TXT_REDACTED"),
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
                responseJsonSchema=DerivationRuleUpdate.response_schema(),
                cachedContent=cache_name,
            ),
        )
        usage = extract_usage_metadata(response)
        payload = json.loads(extract_response_text(response))
        return DerivationRuleUpdate.from_dict(payload), usage
