# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from google.genai import types

from ..core.explicit_cache import ExplicitCacheManager
from ..core.gemini_utils import extract_response_text, extract_usage_metadata
from ..core.llm_limits import max_output_tokens
from ..core.models import NormalizedQuery
from ..core.prompts import PromptLoader


class QueryNormalizer:
    def __init__(
        self,
        *,
        client,
        model_name: str,
        prompts: PromptLoader,
        output_schema_md_path: Path,
        cache_manager: ExplicitCacheManager | None = None,
    ):
        self.client = client
        self.model_name = model_name
        self.prompts = prompts
        self.output_schema_md_path = Path(output_schema_md_path)
        self.cache_manager = cache_manager

    def normalize(
        self,
        question: str,
        *,
        available_years: Iterable[str],
    ) -> Tuple[NormalizedQuery, Dict[str, int]]:
        schema_markdown = "TXT_REDACTED"
        if self.output_schema_md_path.exists():
            schema_markdown = self.output_schema_md_path.read_text(encoding="TXT_REDACTED")
        prompt = self.prompts.get("TXT_REDACTED").render(
            question=question,
            available_years="TXT_REDACTED".join(str(item) for item in available_years) or "TXT_REDACTED",
            output_schema_markdown="TXT_REDACTED" if self.cache_manager else schema_markdown,
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
                        output_schema_markdown=schema_markdown,
                    ),
                )
            except Exception:
                cache_name = None
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=3,
                maxOutputTokens=max_output_tokens("TXT_REDACTED"),
                responseMimeType="TXT_REDACTED",
                responseJsonSchema=NormalizedQuery.response_schema(),
                cachedContent=cache_name,
            ),
        )
        usage = extract_usage_metadata(response)
        normalized = NormalizedQuery.from_dict(json.loads(extract_response_text(response)))
        return normalized, usage
