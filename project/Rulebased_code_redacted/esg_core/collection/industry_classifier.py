# REDACTED
"TXT_REDACTED"

import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

KEJI_CATEGORIES = [
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]

SEED_KEYWORDS = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    ],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
}


def _normalize_text(text: str) -> str:
    "TXT_REDACTED"
    text = str(text or "TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text.strip()


class KejiIndustryClassifier:
    "TXT_REDACTED"

    def __init__(self):
        self._is_fitted = False

    def fit(self, krx_company_db: Dict[str, Dict[str, str]]) -> None:
        "TXT_REDACTED"
        self._is_fitted = True
        logger.info("TXT_REDACTED")

    def predict(self, industry_text: str = "TXT_REDACTED", product_text: str = "TXT_REDACTED",
                report_text: str = "TXT_REDACTED") -> Tuple[str, str]:
        "TXT_REDACTED"
        normalized_industry = _normalize_text(industry_text)
        normalized_product = _normalize_text(product_text)
        industry_financial_context = any(
            keyword in normalized_industry
            for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        )
        product_only_financial_allowed = not normalized_industry or industry_financial_context

        # REDACTED
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED"]) or (product_only_financial_allowed and any(
            keyword in normalized_product for keyword in [
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            ]
        )):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or (product_only_financial_allowed and any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        )):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or (product_only_financial_allowed and any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        )):
            return "TXT_REDACTED", "TXT_REDACTED"
        if (
            any(keyword in normalized_product for keyword in ["TXT_REDACTED"])
            and any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"])
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED"]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        ]) and "TXT_REDACTED" not in normalized_industry:
            return "TXT_REDACTED", "TXT_REDACTED"
        if "TXT_REDACTED" in normalized_industry and "TXT_REDACTED" not in normalized_industry:
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED",
        ]):
            if any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                return "TXT_REDACTED", "TXT_REDACTED"
            return "TXT_REDACTED", "TXT_REDACTED"
        if "TXT_REDACTED" in normalized_industry and any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        ]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        ]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if "TXT_REDACTED" in normalized_industry or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in [
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            ]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if "TXT_REDACTED" in normalized_product.replace("TXT_REDACTED", "TXT_REDACTED") or any(
            keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]
        ) or any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or any(
            keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        ):
            return "TXT_REDACTED", "TXT_REDACTED"
        if any(keyword in normalized_industry for keyword in ["TXT_REDACTED", "TXT_REDACTED"]) or any(keyword in normalized_product for keyword in ["TXT_REDACTED", "TXT_REDACTED"]):
            return "TXT_REDACTED", "TXT_REDACTED"
        if "TXT_REDACTED" in normalized_product:
            return "TXT_REDACTED", "TXT_REDACTED"

        # REDACTED
        industry_rule = self._rule_based_category(normalized_industry)
        if industry_rule:
            return industry_rule, "TXT_REDACTED"

        # REDACTED
        krx_text = self._compose_text(normalized_industry, normalized_product, "TXT_REDACTED")
        krx_rule = self._rule_based_category(krx_text)
        if krx_rule:
            return krx_rule, "TXT_REDACTED"

        # REDACTED
        text = self._compose_text(industry_text, product_text, report_text)
        fallback = self._rule_based_category(text)
        return fallback or "TXT_REDACTED", "TXT_REDACTED"

    def _compose_text(self, industry_text: str, product_text: str, report_text: str) -> str:
        "TXT_REDACTED"
        return "TXT_REDACTED".join(filter(None, [
            _normalize_text(industry_text),
            _normalize_text(product_text),
            _normalize_text(report_text)[:2],
        ])).strip()

    def _rule_based_category(self, text: str) -> Optional[str]:
        "TXT_REDACTED"
        normalized = _normalize_text(text)
        scores = {category: 3 for category in KEJI_CATEGORIES}

        for category, keywords in SEED_KEYWORDS.items():
            for keyword in keywords:
                if keyword in normalized:
                    scores[category] += 4

        best_category = max(scores, key=scores.get)
        if scores[best_category] == 1:
            return None
        return best_category
