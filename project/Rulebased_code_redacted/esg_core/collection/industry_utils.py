# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from typing import Any, Mapping


_FINANCIAL_INDUSTRY_TOKENS = (
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

_FINANCIAL_NAME_TOKENS = (
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


def _contains_financial_token(text: str, tokens: tuple[str, ...]) -> bool:
    value = str(text or "TXT_REDACTED").strip()
    if not value:
        return False
    return any(token in value for token in tokens)


def text_looks_financial(*values: Any) -> bool:
    "TXT_REDACTED"
    return any(_contains_financial_token(value, _FINANCIAL_INDUSTRY_TOKENS) for value in values)


def name_looks_financial(company_name: Any) -> bool:
    "TXT_REDACTED"
    return _contains_financial_token(company_name, _FINANCIAL_NAME_TOKENS)


def company_info_is_financial(company_info: Mapping[str, Any]) -> bool:
    "TXT_REDACTED"
    industry_type = str(company_info.get("TXT_REDACTED") or company_info.get("TXT_REDACTED") or "TXT_REDACTED").strip()
    if industry_type == "TXT_REDACTED":
        return True

    # REDACTED
    # REDACTED
    if text_looks_financial(
        company_info.get("TXT_REDACTED"),
        company_info.get("TXT_REDACTED"),
        company_info.get("TXT_REDACTED"),
    ):
        return True

    return name_looks_financial(company_info.get("TXT_REDACTED") or company_info.get("TXT_REDACTED") or company_info.get("TXT_REDACTED"))


def row_is_financial(row: Mapping[str, Any]) -> bool:
    "TXT_REDACTED"
    industry_type = str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
    if industry_type == "TXT_REDACTED":
        return True

    # REDACTED
    # REDACTED
    if text_looks_financial(
        row.get("TXT_REDACTED"),
        row.get("TXT_REDACTED"),
        row.get("TXT_REDACTED"),
        row.get("TXT_REDACTED"),
        row.get("TXT_REDACTED"),
    ):
        return True

    return name_looks_financial(row.get("TXT_REDACTED") or row.get("TXT_REDACTED"))
