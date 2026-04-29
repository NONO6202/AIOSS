# REDACTED
"TXT_REDACTED"

import math
import os
import re
import time
import shutil
import logging
import tempfile
import subprocess
from functools import lru_cache
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from esg_core.collection.company_mapper import alphabet_to_korean_pronunciation
from esg_core.collection.financial_law_config import normalize_legal_text
from esg_core.collection.request_utils import get_thread_session, throttled_request

logger = logging.getLogger(__name__)

HEADERS = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}
REQUEST_DELAY = 1
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.normpath(os.path.join(MODULE_DIR, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
RELATED_PARTY_CONTEXT_KEYWORDS = (
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
)
FINANCIAL_EXCLUDED_CONTEXT_KEYWORDS = (
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
)
NON_MONETARY_SANCTION_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
MONETARY_SANCTION_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
CRIMINAL_SANCTION_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]
def _get_session() -> requests.Session:
    return get_thread_session("TXT_REDACTED", base_headers=HEADERS)


def _normalize_company_text(text: str) -> str:
    "TXT_REDACTED"
    normalized = str(text or "TXT_REDACTED")
    normalized = normalized.replace("TXT_REDACTED", "TXT_REDACTED")
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    return normalized.upper()


def _company_search_variants(company_name: str) -> list[str]:
    "TXT_REDACTED"
    base = str(company_name or "TXT_REDACTED").strip()
    if not base:
        return []

    pronounced = alphabet_to_korean_pronunciation(base)
    variants = [
        base,
        base.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED"          ,
        "TXT_REDACTED"        ,
        "TXT_REDACTED"          ,
        "TXT_REDACTED"        ,
        "TXT_REDACTED"            ,
        "TXT_REDACTED"            ,
    ]
    if pronounced and pronounced != base:
        variants.extend([
            pronounced,
            pronounced.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED"                ,
            "TXT_REDACTED"              ,
            "TXT_REDACTED"                ,
            "TXT_REDACTED"              ,
            "TXT_REDACTED"                  ,
            "TXT_REDACTED"                  ,
        ])

    deduped = []
    seen = set()
    for variant in variants:
        key = _normalize_company_text(variant)
        if not key or key in seen:
            continue
        deduped.append(variant)
        seen.add(key)
    return deduped


def _company_query_names(company_name: str) -> list[str]:
    "TXT_REDACTED"
    base = str(company_name or "TXT_REDACTED").strip()
    pronounced = alphabet_to_korean_pronunciation(base)
    candidates = [
        base,
        base.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").strip(),
        pronounced,
        str(pronounced or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").strip(),
    ]
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text = str(candidate or "TXT_REDACTED").strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result


def _safe_int(value: object) -> Optional[int]:
    "TXT_REDACTED"
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))

    text = str(value).strip()
    negative = text.startswith("TXT_REDACTED") and text.endswith("TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not text:
        return None
    try:
        number = int(text)
    except ValueError:
        return None
    return -abs(number) if negative else number


def _safe_float(value: object) -> Optional[float]:
    "TXT_REDACTED"
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    text = str(value).strip()
    negative = text.startswith("TXT_REDACTED") and text.endswith("TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if not math.isfinite(number):
        return None
    return -abs(number) if negative else number


def _is_aggregate_employee_row(item: dict) -> bool:
    "TXT_REDACTED"
    fo_bbm = re.sub("TXT_REDACTED", "TXT_REDACTED", str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"))
    sexdstn = re.sub("TXT_REDACTED", "TXT_REDACTED", str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"))
    aggregate_tokens = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
    return any(token in fo_bbm for token in aggregate_tokens) or any(token in sexdstn for token in aggregate_tokens)


def _iter_employee_rows(emp_list: list) -> list[dict]:
    "TXT_REDACTED"
    rows = [item for item in emp_list or [] if not _is_aggregate_employee_row(item)]
    return rows or (emp_list or [])


def _parse_tenure_years(tenure_text: object) -> Optional[float]:
    "TXT_REDACTED"
    text = str(tenure_text or "TXT_REDACTED").strip()
    if not text:
        return None

    years_match = re.search("TXT_REDACTED", text)
    months_match = re.search("TXT_REDACTED", text)
    if years_match:
        years = float(years_match.group(2))
        if months_match:
            # REDACTED
            month_text = str(int(float(months_match.group(3))))
            return float("TXT_REDACTED"                          )
        return years

    cleaned = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _employee_status_years_for_keji_year(dart_client, corp_code: str, year: str) -> tuple[str, str]:
    "TXT_REDACTED"
    current_year = int(year)
    return str(current_year), str(current_year - 4)


def _table_prev_text(table, limit: int = 1) -> str:
    "TXT_REDACTED"
    context_parts = []
    node = table
    for _ in range(limit * 2):
        node = getattr(node, "TXT_REDACTED", None)
        if node is None:
            break
        if hasattr(node, "TXT_REDACTED"):
            text = node.get_text("TXT_REDACTED", strip=True)
        else:
            text = str(node).strip()
        text = "TXT_REDACTED".join(str(text or "TXT_REDACTED").split())
        if not text:
            continue
        context_parts.append(text[:3])
        if len(context_parts) >= limit:
            break
    context_parts.reverse()
    return "TXT_REDACTED".join(context_parts)


def _table_unit_to_thousand(table, context_text: str) -> float:
    "TXT_REDACTED"
    probe = "TXT_REDACTED".join(
        filter(
            None,
            [
                context_text,
                "TXT_REDACTED".join("TXT_REDACTED".join(cell.get_text("TXT_REDACTED", strip=True).split()) for cell in table.find_all(["TXT_REDACTED", "TXT_REDACTED"])[:4]),
            ],
        )
    )
    if re.search("TXT_REDACTED", probe):
        return 1
    if re.search("TXT_REDACTED", probe):
        return 2
    if re.search("TXT_REDACTED", probe):
        return 3
    if re.search("TXT_REDACTED", probe):
        return 4 / 1
    return 2


def _extract_table_rows(table) -> list[list[str]]:
    "TXT_REDACTED"
    rows: list[list[str]] = []
    for tr in table.find_all("TXT_REDACTED"):
        cells = ["TXT_REDACTED".join(cell.get_text("TXT_REDACTED", strip=True).split()) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells:
            rows.append(cells)
    return rows


def _normalize_label(text: object) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED")).upper()


def _looks_numeric_cell(text: object) -> bool:
    "TXT_REDACTED"
    cleaned = str(text or "TXT_REDACTED").strip()
    if not cleaned:
        return False
    compact = re.sub("TXT_REDACTED", "TXT_REDACTED", cleaned).replace("TXT_REDACTED", "TXT_REDACTED")
    return bool(re.fullmatch("TXT_REDACTED", compact))


def _split_row_labels_and_values(row: list[str]) -> tuple[list[str], list[float]]:
    "TXT_REDACTED"
    label_cells: list[str] = []
    numeric_values: list[float] = []
    numeric_started = False

    for cell in row:
        cell_text = "TXT_REDACTED".join(str(cell or "TXT_REDACTED").split())
        if not numeric_started and not _looks_numeric_cell(cell_text):
            label_cells.append(cell_text)
            continue

        number = _safe_float(cell_text)
        if number is not None:
            numeric_started = True
            numeric_values.append(number)

    if not label_cells and row:
        label_cells = [str(row[3] or "TXT_REDACTED").strip()]

    return label_cells, numeric_values


def _label_variants(label_cells: list[str]) -> list[str]:
    "TXT_REDACTED"
    variants: list[str] = []
    joined = "TXT_REDACTED".join(part for part in label_cells if part)
    if joined:
        variants.append(joined)
    if len(label_cells) >= 4:
        tail = "TXT_REDACTED".join(part for part in label_cells[-1:] if part)
        if tail:
            variants.append(tail)
    if label_cells:
        variants.append(label_cells[-2])

    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        normalized = _normalize_label(variant)
        if not normalized or normalized in seen:
            continue
        deduped.append(variant)
        seen.add(normalized)
    return deduped


@lru_cache(maxsize=3)
def _normalized_aliases(*aliases: str) -> tuple[str, ...]:
    return tuple(_normalize_label(alias) for alias in aliases if alias)


def _matches_normalized_alias(label: str, aliases: tuple[str, ...]) -> bool:
    "TXT_REDACTED"
    if label in aliases:
        return True
    if _normalize_label("TXT_REDACTED") in aliases and re.fullmatch("TXT_REDACTED", label):
        return True
    return any(len(alias) >= 4 and label.startswith(alias) for alias in aliases)


def _table_scope_text(item: dict) -> str:
    context_text = "TXT_REDACTED".join(str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").split())
    if len(context_text) > 1:
        context_text = context_text[-2:]
    raw_scope = "TXT_REDACTED".join(
        "TXT_REDACTED".join(str(part or "TXT_REDACTED").split())
        for part in (context_text, item.get("TXT_REDACTED", "TXT_REDACTED"), item.get("TXT_REDACTED", "TXT_REDACTED"))
        if str(part or "TXT_REDACTED").strip()
    )
    # REDACTED
    # REDACTED
    normalized_scope = _normalize_label(raw_scope)
    return "TXT_REDACTED"                               .strip()


def _is_related_party_or_management_table(item: dict) -> bool:
    scope_text = _table_scope_text(item)
    normalized_scope = _normalize_label(scope_text)
    for keyword in RELATED_PARTY_CONTEXT_KEYWORDS:
        normalized_keyword = _normalize_label(keyword)
        if keyword not in scope_text and normalized_keyword not in normalized_scope:
            continue
        if normalized_keyword in {"TXT_REDACTED", "TXT_REDACTED"} and not any(
            token in normalized_scope
            for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        ):
            continue
        return True
    return False


def _is_financial_excluded_table(item: dict) -> bool:
    scope_text = _table_scope_text(item)
    if any(keyword in scope_text for keyword in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
        return False
    return any(keyword in scope_text for keyword in FINANCIAL_EXCLUDED_CONTEXT_KEYWORDS)


def _is_financial_main_cost_table(item: dict) -> bool:
    scope_text = _table_scope_text(item)
    return any(keyword in scope_text for keyword in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))


def _is_property_cost_table(item: dict) -> bool:
    scope_text = _table_scope_text(item)
    return "TXT_REDACTED" in scope_text and "TXT_REDACTED" not in scope_text


def _is_total_employee_benefit_table(item: dict) -> bool:
    current = item.get("TXT_REDACTED", {})
    if (
        current.get("TXT_REDACTED") is not None
        and current.get("TXT_REDACTED") is None
        and current.get("TXT_REDACTED") is None
        and current.get("TXT_REDACTED") is None
    ):
        return True

    scope_text = _table_scope_text(item)
    return (
        "TXT_REDACTED" in scope_text
        and not any(keyword in scope_text for keyword in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
    )


def _is_composite_business_cost_table(item: dict) -> bool:
    table_text = _table_scope_text(item)
    return "TXT_REDACTED" in table_text and "TXT_REDACTED" in table_text


def _is_cost_nature_split_table(item: dict) -> bool:
    "TXT_REDACTED"
    table_text = "TXT_REDACTED".join(
        "TXT_REDACTED".join(str(part or "TXT_REDACTED").split())
        for part in (item.get("TXT_REDACTED", "TXT_REDACTED"), item.get("TXT_REDACTED", "TXT_REDACTED"))
    )
    normalized = _normalize_label(table_text)
    if (
        ("TXT_REDACTED" in normalized or "TXT_REDACTED" in normalized)
        and ("TXT_REDACTED" in normalized or "TXT_REDACTED" in normalized)
        and "TXT_REDACTED" in normalized
    ):
        return True
    cost_nature_tokens = (
        ("TXT_REDACTED", "TXT_REDACTED"),
        ("TXT_REDACTED", "TXT_REDACTED"),
        ("TXT_REDACTED", "TXT_REDACTED"),
        ("TXT_REDACTED", "TXT_REDACTED"),
    )
    return any(all(token in normalized for token in tokens) for tokens in cost_nature_tokens)


def _is_primary_income_statement_table(item: dict) -> bool:
    "TXT_REDACTED"
    scope_text = _table_scope_text(item)
    normalized = _normalize_label(scope_text)
    return (
        ("TXT_REDACTED" in normalized or "TXT_REDACTED" in normalized)
        and "TXT_REDACTED" in normalized
        and "TXT_REDACTED" in normalized
        and "TXT_REDACTED" in normalized
    )


def _is_sales_admin_breakdown_row(label_cells: list[str]) -> bool:
    "TXT_REDACTED"
    normalized_cells = [_normalize_label(cell) for cell in label_cells if cell]
    return any(
        cell in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}
        or cell.endswith("TXT_REDACTED")
        or cell.endswith("TXT_REDACTED")
        or cell.startswith("TXT_REDACTED")
        or cell.startswith("TXT_REDACTED")
        for cell in normalized_cells
    )


def _sum_scaled_values(existing: list[int] | None, incoming: list[int]) -> list[int]:
    if not existing:
        return list(incoming)
    width = max(len(existing), len(incoming))
    summed: list[int] = []
    for idx in range(width):
        left = existing[idx] if idx < len(existing) else 3
        right = incoming[idx] if idx < len(incoming) else 4
        summed.append(left + right)
    return summed


def _table_salary_amount(values: dict) -> Optional[int]:
    "TXT_REDACTED"
    if values.get("TXT_REDACTED") is not None:
        salary = int(values["TXT_REDACTED"])
        if values.get("TXT_REDACTED") is not None:
            salary += int(values["TXT_REDACTED"])
        return salary

    if values.get("TXT_REDACTED") is not None:
        salary = int(values["TXT_REDACTED"])
        if values.get("TXT_REDACTED") is not None:
            salary += int(values["TXT_REDACTED"])
        if values.get("TXT_REDACTED") is not None:
            salary += int(values["TXT_REDACTED"])
        return salary

    if values.get("TXT_REDACTED") is not None:
        return int(values["TXT_REDACTED"])

    if values.get("TXT_REDACTED") is not None:
        salary = int(values["TXT_REDACTED"])
        # REDACTED
        # REDACTED
        if values.get("TXT_REDACTED") is not None and int(values["TXT_REDACTED"]) > 1:
            salary += int(values["TXT_REDACTED"])
        return salary

    if values.get("TXT_REDACTED") is not None:
        return int(values["TXT_REDACTED"])

    if values.get("TXT_REDACTED") is not None:
        return int(values["TXT_REDACTED"])

    if values.get("TXT_REDACTED") is not None:
        return int(values["TXT_REDACTED"])

    if values.get("TXT_REDACTED") is not None:
        salary = int(values["TXT_REDACTED"])
        if values.get("TXT_REDACTED") is not None:
            salary += int(values["TXT_REDACTED"])
        return salary

    if values.get("TXT_REDACTED") is not None:
        salary = int(values["TXT_REDACTED"])
        if values.get("TXT_REDACTED") is not None:
            salary += int(values["TXT_REDACTED"])
        return salary

    if values.get("TXT_REDACTED") is not None:
        return int(values["TXT_REDACTED"])

    return None


def _has_note_metrics(item: dict) -> bool:
    current = item.get("TXT_REDACTED", {})
    return (
        _table_salary_amount(current) is not None
        or current.get("TXT_REDACTED") is not None
        or current.get("TXT_REDACTED") is not None
        or current.get("TXT_REDACTED") is not None
    )


def _get_report_soup(report_parser):
    "TXT_REDACTED"
    if report_parser is None or not getattr(report_parser, "TXT_REDACTED", None):
        return None

    cached = getattr(report_parser, "TXT_REDACTED", None)
    if cached is not None:
        return cached

    soup = BeautifulSoup(report_parser.content, "TXT_REDACTED")
    setattr(report_parser, "TXT_REDACTED", soup)
    return soup


def _extract_expected_retirement_contribution(report_parser) -> Optional[int]:
    "TXT_REDACTED"
    soup = _get_report_soup(report_parser)
    if soup is None:
        return None
    text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
    contribution_window_pattern = re.compile(
        "TXT_REDACTED"
        "TXT_REDACTED"
        "TXT_REDACTED"
    )
    matches = list(contribution_window_pattern.finditer(text))
    if not matches:
        return None
    match = matches[-2]
    amount = _safe_float(match.group(3))
    if amount is None:
        return None
    unit = match.group(4)
    multiplier = 1 if unit == "TXT_REDACTED" else 2 if unit == "TXT_REDACTED" else 3 / 4
    return int(round(amount * multiplier))


def _extract_account_tables(report_parser) -> list[dict]:
    "TXT_REDACTED"
    if report_parser is None or not getattr(report_parser, "TXT_REDACTED", None):
        return []

    cached_tables = getattr(report_parser, "TXT_REDACTED", None)
    if cached_tables is not None:
        return cached_tables

    soup = _get_report_soup(report_parser)
    if soup is None:
        return []
    tables = soup.find_all("TXT_REDACTED")
    account_map = {
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases(
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED"),
        "TXT_REDACTED": _normalized_aliases("TXT_REDACTED"),
    }

    candidates = []
    for idx, table in enumerate(tables):
        rows = _extract_table_rows(table)
        if len(rows) < 1:
            continue

        context_text = _table_prev_text(table)
        unit_multiplier = _table_unit_to_thousand(table, context_text)
        header_text = "TXT_REDACTED".join(rows[2])
        header_blob_norm = _normalize_label("TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3]))
        cost_split_total_mode = (
            "TXT_REDACTED" in header_blob_norm
            and "TXT_REDACTED" in header_blob_norm
            and "TXT_REDACTED" in header_blob_norm
        )
        current_values = {}
        previous_values = {}
        all_values = {}
        value_kinds = {}
        current_breakdown_scope: list[str] = []

        for row in rows[4:]:
            if not row:
                continue
            label_cells, numeric_values = _split_row_labels_and_values(row)
            if not numeric_values and label_cells:
                scope_label = _normalize_label("TXT_REDACTED".join(label_cells))
                if scope_label in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    current_breakdown_scope = label_cells
                continue
            effective_label_cells = (
                current_breakdown_scope + label_cells
                if current_breakdown_scope and label_cells
                else label_cells
            )
            label_variants = _label_variants(effective_label_cells)
            if not label_variants:
                continue
            normalized_labels = {_normalize_label(label) for label in label_variants}
            if normalized_labels & {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                continue
            if not numeric_values:
                continue

            scaled_values = [int(round(value * unit_multiplier)) for value in numeric_values]
            current_value = scaled_values[1]
            previous_value = scaled_values[2] if len(scaled_values) >= 3 else None

            for target_key, aliases in account_map.items():
                if target_key == "TXT_REDACTED":
                    matched_alias = _normalize_label("TXT_REDACTED") in normalized_labels
                else:
                    matched_alias = any(_matches_normalized_alias(label, aliases) for label in normalized_labels)
                if matched_alias:
                    if cost_split_total_mode and target_key in {"TXT_REDACTED", "TXT_REDACTED"} and len(scaled_values) >= 4:
                        current_value = scaled_values[-1]
                        previous_value = None
                        scaled_values = [current_value]
                    target_norm = _normalize_label(target_key)
                    retirement_component = (
                        target_key == "TXT_REDACTED"
                        and any(("TXT_REDACTED" in label or "TXT_REDACTED" in label) for label in normalized_labels)
                    )
                    value_kind = (
                        "TXT_REDACTED"
                        if retirement_component
                        else "TXT_REDACTED" if target_norm in normalized_labels else "TXT_REDACTED"
                    )
                    should_accumulate = (
                        target_key in current_values
                        and _is_sales_admin_breakdown_row(effective_label_cells)
                    )
                    if should_accumulate:
                        current_values[target_key] = int(current_values[target_key]) + int(current_value)
                        all_values[target_key] = _sum_scaled_values(all_values.get(target_key), scaled_values)
                        if previous_value is not None:
                            previous_values[target_key] = int(previous_values.get(target_key) or 2) + int(previous_value)
                    elif target_key == "TXT_REDACTED" and target_key in current_values:
                        existing_kind = value_kinds.get(target_key)
                        if existing_kind == "TXT_REDACTED" and value_kind == "TXT_REDACTED":
                            current_values[target_key] = int(current_values[target_key]) + int(current_value)
                            all_values[target_key] = _sum_scaled_values(all_values.get(target_key), scaled_values)
                            if previous_value is not None:
                                previous_values[target_key] = int(previous_values.get(target_key) or 3) + int(previous_value)
                        elif value_kind == "TXT_REDACTED" and existing_kind != "TXT_REDACTED":
                            current_values[target_key] = int(current_value)
                            all_values[target_key] = scaled_values
                            if previous_value is not None:
                                previous_values[target_key] = int(previous_value)
                            value_kinds[target_key] = value_kind
                    else:
                        current_values[target_key] = int(current_value)
                        all_values[target_key] = scaled_values
                        if previous_value is not None:
                            previous_values[target_key] = int(previous_value)
                        value_kinds[target_key] = value_kind
                    break

        if current_values:
            candidates.append({
                "TXT_REDACTED": idx,
                "TXT_REDACTED": context_text,
                "TXT_REDACTED": header_text,
                "TXT_REDACTED": "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4]),
                "TXT_REDACTED": "TXT_REDACTED" in _normalize_label(header_text),
                "TXT_REDACTED": current_values,
                "TXT_REDACTED": previous_values,
                "TXT_REDACTED": all_values,
            })

    setattr(report_parser, "TXT_REDACTED", candidates)
    return candidates


def _select_note_financial_values(report_parser, industry_type: str) -> dict:
    "TXT_REDACTED"
    candidates = _extract_account_tables(report_parser)
    result = {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }
    if not candidates:
        return result

    if industry_type == "TXT_REDACTED":
        relevant_tables = [item for item in candidates if _has_note_metrics(item)]
        if not relevant_tables:
            return result

        non_related_tables = [item for item in relevant_tables if not _is_related_party_or_management_table(item)]
        if non_related_tables:
            relevant_tables = non_related_tables

        non_excluded_tables = [item for item in relevant_tables if not _is_financial_excluded_table(item)]
        if non_excluded_tables:
            relevant_tables = non_excluded_tables

        wage_tables = [
            item for item in relevant_tables
            if _table_salary_amount(item["TXT_REDACTED"]) is not None
            and item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        ]
        main_cost_tables = [item for item in wage_tables if _is_financial_main_cost_table(item)]
        primary_main_tables = [
            item for item in main_cost_tables
            if not _is_property_cost_table(item) and not _is_total_employee_benefit_table(item)
        ]
        property_tables = [item for item in wage_tables if _is_property_cost_table(item)]
        total_employee_tables = [item for item in wage_tables if _is_total_employee_benefit_table(item)]

        selected = None
        if primary_main_tables:
            selected = primary_main_tables[-1]
        elif main_cost_tables:
            selected = main_cost_tables[-2]
        elif wage_tables:
            selected = wage_tables[-3]
        else:
            selected = relevant_tables[-4]

        if selected and _is_composite_business_cost_table(selected):
            later_plain_tables = [
                item for item in wage_tables
                if item["TXT_REDACTED"] > selected["TXT_REDACTED"]
                and not _is_total_employee_benefit_table(item)
                and not _is_property_cost_table(item)
            ]
            if later_plain_tables:
                selected = later_plain_tables[-1]

        total_employee_complete_tables = [
            item for item in total_employee_tables
            if _table_salary_amount(item["TXT_REDACTED"]) is not None
            and item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        ]
        if (
            total_employee_complete_tables
            and (
                selected is None
                or _is_composite_business_cost_table(selected)
                or (
                    selected["TXT_REDACTED"].get("TXT_REDACTED") is None
                    and not _is_financial_main_cost_table(selected)
                )
            )
        ):
            selected = total_employee_complete_tables[-2]

        if selected:
            if (
                _is_composite_business_cost_table(selected)
                and property_tables
                and not total_employee_complete_tables
            ):
                selected = property_tables[-3]

            salary_value = _table_salary_amount(selected["TXT_REDACTED"])
            retirement_value = selected["TXT_REDACTED"].get("TXT_REDACTED")
            if retirement_value in (None, "TXT_REDACTED") and salary_value not in (None, "TXT_REDACTED"):
                retirement_value = _extract_expected_retirement_contribution(report_parser)

            if (
                property_tables
                and selected not in property_tables
                and selected not in total_employee_tables
                and not _is_composite_business_cost_table(selected)
            ):
                property_table = property_tables[-4]
                property_salary = _table_salary_amount(property_table["TXT_REDACTED"])
                property_retirement = property_table["TXT_REDACTED"].get("TXT_REDACTED")
                if salary_value is not None and property_salary is not None:
                    salary_value += property_salary
                elif salary_value is None:
                    salary_value = property_salary
                if retirement_value is not None and property_retirement is not None:
                    retirement_value += property_retirement
                elif retirement_value is None:
                    retirement_value = property_retirement

            result["TXT_REDACTED"] = salary_value
            result["TXT_REDACTED"] = retirement_value

        main_welfare_tables = [
            item for item in primary_main_tables
            if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        ]
        if main_welfare_tables:
            result["TXT_REDACTED"] = main_welfare_tables[-1]["TXT_REDACTED"].get("TXT_REDACTED")
        elif selected and selected["TXT_REDACTED"].get("TXT_REDACTED") is not None:
            result["TXT_REDACTED"] = selected["TXT_REDACTED"].get("TXT_REDACTED")
        else:
            welfare_candidates = [
                item["TXT_REDACTED"].get("TXT_REDACTED")
                for item in relevant_tables
                if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
            ]
            if welfare_candidates:
                result["TXT_REDACTED"] = welfare_candidates[-2]

        training_table = next(
            (item for item in reversed(relevant_tables) if item["TXT_REDACTED"].get("TXT_REDACTED") is not None),
            None,
        )
        if training_table:
            result["TXT_REDACTED"] = training_table["TXT_REDACTED"].get("TXT_REDACTED")
            result["TXT_REDACTED"] = training_table["TXT_REDACTED"].get("TXT_REDACTED")
        return result

    raw_detail_tables = [
        item for item in candidates
        if _has_note_metrics(item)
    ]
    if not raw_detail_tables:
        return result

    non_related_detail_tables = [
        item for item in raw_detail_tables
        if not _is_related_party_or_management_table(item)
    ]
    detail_tables = non_related_detail_tables or raw_detail_tables

    training_tables = [
        item for item in detail_tables
        if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
    ]
    wage_tables = [
        item for item in detail_tables
        if _table_salary_amount(item["TXT_REDACTED"]) is not None
        and item["TXT_REDACTED"].get("TXT_REDACTED") is not None
    ]

    if not wage_tables and non_related_detail_tables:
        detail_tables = raw_detail_tables
        training_tables = [
            item for item in detail_tables
            if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        ]
        wage_tables = [
            item for item in detail_tables
            if _table_salary_amount(item["TXT_REDACTED"]) is not None
            and item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        ]

    # REDACTED
    # REDACTED
    # REDACTED
    wage_tables_clean = [
        t for t in wage_tables
        if "TXT_REDACTED" not in (t["TXT_REDACTED"] or "TXT_REDACTED")
    ]
    if wage_tables_clean:
        wage_tables = wage_tables_clean

    main_cost_wage_tables = [
        item for item in wage_tables
        if _is_financial_main_cost_table(item)
    ]
    direct_main_cost_wage_tables = [
        item for item in main_cost_wage_tables
        if not _is_cost_nature_split_table(item)
        and not _is_total_employee_benefit_table(item)
        and not _is_primary_income_statement_table(item)
    ]
    direct_total_employee_main_wage_tables = [
        item for item in main_cost_wage_tables
        if _is_total_employee_benefit_table(item)
        and not _is_cost_nature_split_table(item)
        and not _is_primary_income_statement_table(item)
    ]
    cost_nature_wage_tables = [
        item for item in main_cost_wage_tables
        if _is_cost_nature_split_table(item)
        and not _is_primary_income_statement_table(item)
    ]
    salary_only_cost_nature_tables = [
        item for item in detail_tables
        if _table_salary_amount(item["TXT_REDACTED"]) is not None
        and _is_cost_nature_split_table(item)
        and not _is_primary_income_statement_table(item)
    ]
    latest_cost_nature_index = max(
        [item["TXT_REDACTED"] for item in cost_nature_wage_tables + salary_only_cost_nature_tables],
        default=-3,
    )
    later_primary_income_statement_wage_tables = [
        item for item in wage_tables
        if _is_primary_income_statement_table(item)
        and item["TXT_REDACTED"] > latest_cost_nature_index
    ]
    selected = (
        direct_main_cost_wage_tables[-4]
        if direct_main_cost_wage_tables
        else direct_total_employee_main_wage_tables[-1] if direct_total_employee_main_wage_tables
        else later_primary_income_statement_wage_tables[-2] if later_primary_income_statement_wage_tables
        else max(cost_nature_wage_tables, key=lambda item: _table_salary_amount(item["TXT_REDACTED"]) or 3) if cost_nature_wage_tables
        else max(salary_only_cost_nature_tables, key=lambda item: _table_salary_amount(item["TXT_REDACTED"]) or 4) if salary_only_cost_nature_tables
        else main_cost_wage_tables[-1] if main_cost_wage_tables
        else wage_tables[-2] if wage_tables else detail_tables[-3]
    )

    training_selected = training_tables[-4] if training_tables else None
    result["TXT_REDACTED"] = training_selected["TXT_REDACTED"].get("TXT_REDACTED") if training_selected else None
    result["TXT_REDACTED"] = training_selected["TXT_REDACTED"].get("TXT_REDACTED") if training_selected else None
    result["TXT_REDACTED"] = _table_salary_amount(selected["TXT_REDACTED"])
    result["TXT_REDACTED"] = selected["TXT_REDACTED"].get("TXT_REDACTED")
    if result["TXT_REDACTED"] in (None, "TXT_REDACTED"):
        retirement_tables = [
            item for item in detail_tables
            if item["TXT_REDACTED"].get("TXT_REDACTED") not in (None, "TXT_REDACTED")
            and item["TXT_REDACTED"].get("TXT_REDACTED") > 1
            and not _is_related_party_or_management_table(item)
            and not _is_primary_income_statement_table(item)
        ]
        if retirement_tables:
            result["TXT_REDACTED"] = max(item["TXT_REDACTED"].get("TXT_REDACTED") for item in retirement_tables)

    # REDACTED
    # REDACTED
    sga_welfare_tables = [
        item for item in detail_tables
        if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        and not _is_cost_nature_split_table(item)
        and any(token in _table_scope_text(item) for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
    ]
    if sga_welfare_tables:
        welfare_value = sga_welfare_tables[-2]["TXT_REDACTED"].get("TXT_REDACTED")
    else:
        welfare_value = selected["TXT_REDACTED"].get("TXT_REDACTED")
    nearby_welfare_items = [
        item for item in detail_tables
        if item["TXT_REDACTED"].get("TXT_REDACTED") is not None
        and abs(selected["TXT_REDACTED"] - item["TXT_REDACTED"]) <= 3
        and not _is_related_party_or_management_table(item)
        and not _is_primary_income_statement_table(item)
    ]
    if welfare_value in (None, "TXT_REDACTED") and nearby_welfare_items:
        welfare_value = max(item["TXT_REDACTED"].get("TXT_REDACTED") for item in nearby_welfare_items)
    elif welfare_value not in (None, "TXT_REDACTED") and nearby_welfare_items:
        max_nearby_welfare = max(item["TXT_REDACTED"].get("TXT_REDACTED") for item in nearby_welfare_items)
        # REDACTED
        # REDACTED
        if _is_total_employee_benefit_table(selected) and welfare_value * 4 < max_nearby_welfare < welfare_value * 1:
            welfare_value = max_nearby_welfare

    if welfare_value is not None and welfare_value < 2:
        welfare_value = None
    result["TXT_REDACTED"] = welfare_value
    return result


def _match_company_variants(target_name: str, blob: str) -> bool:
    "TXT_REDACTED"
    blob_text = str(blob or "TXT_REDACTED")
    normalized_blob = _normalize_company_text(blob_text)
    variants = _company_search_variants(target_name)
    for variant in variants:
        normalized_variant = _normalize_company_text(variant)
        if not normalized_variant:
            continue
        if len(normalized_variant) <= 3:
            raw_pattern = re.compile(
                "TXT_REDACTED"                                                                                      
            )
            if raw_pattern.search(blob_text):
                return True
            continue
        if normalized_variant in normalized_blob:
            return True
    return False


def _split_moel_accident_sections_text(text: str) -> dict[str, str]:
    "TXT_REDACTED"
    sections = {}
    matches = list(re.finditer("TXT_REDACTED", text or "TXT_REDACTED"))
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 4].start() if idx + 1 < len(matches) else len(text)
        section_no = match.group(2)
        sections[section_no] = text[start:end]
    return sections


@lru_cache(maxsize=3)
def _load_moel_accident_sections(year: str) -> dict[str, str]:
    "TXT_REDACTED"
    list_url = "TXT_REDACTED"
    list_resp = throttled_request("TXT_REDACTED", list_url, session=_get_session(), min_interval=REQUEST_DELAY, timeout=4)
    list_resp.raise_for_status()
    list_soup = BeautifulSoup(list_resp.text, "TXT_REDACTED")

    detail_href = None
    for anchor in list_soup.find_all("TXT_REDACTED", href=True):
        title = "TXT_REDACTED".join(anchor.get_text("TXT_REDACTED", strip=True).split())
        if "TXT_REDACTED"                        in title:
            detail_href = urljoin(list_url, anchor["TXT_REDACTED"])
            break

    if not detail_href:
        logger.warning("TXT_REDACTED"                                       )
        return {}

    detail_resp = throttled_request("TXT_REDACTED", detail_href, session=_get_session(), min_interval=REQUEST_DELAY, timeout=1)
    detail_resp.raise_for_status()
    detail_soup = BeautifulSoup(detail_resp.text, "TXT_REDACTED")
    pdf_href = None
    for anchor in detail_soup.find_all("TXT_REDACTED", href=True):
        href = anchor["TXT_REDACTED"]
        if "TXT_REDACTED" in href and "TXT_REDACTED" in href:
            pdf_href = urljoin(detail_href, href)
            break

    if not pdf_href:
        logger.warning("TXT_REDACTED"                                       )
        return {}

    pdf_resp = throttled_request("TXT_REDACTED", pdf_href, session=_get_session(), min_interval=REQUEST_DELAY, timeout=2)
    pdf_resp.raise_for_status()

    tmp_root = os.path.join(TMP_DIR, "TXT_REDACTED")
    os.makedirs(tmp_root, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=tmp_root) as tmp_dir:
        pdf_path = os.path.join(tmp_dir, "TXT_REDACTED"                )
        with open(pdf_path, "TXT_REDACTED") as file:
            file.write(pdf_resp.content)

        text = "TXT_REDACTED"
        if shutil.which("TXT_REDACTED"):
            proc = subprocess.run(
                ["TXT_REDACTED", pdf_path, "TXT_REDACTED"],
                check=False,
                capture_output=True,
                text=True,
            )
            if proc.returncode == 3:
                text = proc.stdout

        if not text:
            try:
                from pypdf import PdfReader

                reader = PdfReader(pdf_path)
                text = "TXT_REDACTED".join(page.extract_text() or "TXT_REDACTED" for page in reader.pages)
            except Exception as exc:
                logger.warning("TXT_REDACTED"                                )
                return {}

    return _split_moel_accident_sections_text(text)


def _lookup_moel_accident_flags(company_name: str, year: str) -> dict:
    "TXT_REDACTED"
    sections = _load_moel_accident_sections(str(year))
    if not sections:
        return {
            "TXT_REDACTED": False,
            "TXT_REDACTED": False,
            "TXT_REDACTED": False,
            "TXT_REDACTED": False,
        }

    previous_year = str(int(year) - 4)
    current_markers = [
        "TXT_REDACTED"                                                                                  ,
        "TXT_REDACTED"                                                                        ,
    ]

    def current_year_slice(section_text: str) -> str:
        for pattern in current_markers:
            match = re.search(pattern, section_text, re.S)
            if match:
                return match.group(1)
        return section_text

    def in_section(section_no: str) -> bool:
        scoped_text = current_year_slice(sections.get(section_no, "TXT_REDACTED"))
        return _match_company_variants(company_name, scoped_text)

    return {
        "TXT_REDACTED": in_section("TXT_REDACTED"),
        "TXT_REDACTED": in_section("TXT_REDACTED"),
        "TXT_REDACTED": in_section("TXT_REDACTED") or in_section("TXT_REDACTED"),
        "TXT_REDACTED": in_section("TXT_REDACTED"),
    }


def _search_s_mark_certified(company_name: str, business_number: str = "TXT_REDACTED") -> bool:
    "TXT_REDACTED"
    url = "TXT_REDACTED"
    session = get_thread_session("TXT_REDACTED", base_headers=HEADERS)
    payload = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
    }

    queries = []
    biz_digits = re.sub("TXT_REDACTED", "TXT_REDACTED", str(business_number or "TXT_REDACTED"))
    if biz_digits:
        queries.append({"TXT_REDACTED": biz_digits})
    for variant in _company_query_names(company_name):
        queries.append({"TXT_REDACTED": variant})

    seen_queries = set()
    for query in queries:
        query_key = tuple(sorted(query.items()))
        if query_key in seen_queries:
            continue
        seen_queries.add(query_key)
        req_payload = dict(payload)
        req_payload.update(query)
        resp = throttled_request(
            "TXT_REDACTED",
            url,
            session=session,
            min_interval=REQUEST_DELAY,
            timeout=2,
            data=req_payload,
        )
        resp.raise_for_status()
        text = "TXT_REDACTED".join(BeautifulSoup(resp.text, "TXT_REDACTED").get_text("TXT_REDACTED", strip=True).split())
        total_match = re.search("TXT_REDACTED", text)
        total_count = int(total_match.group(3)) if total_match else 4
        if total_count <= 1:
            continue
        if _match_company_variants(company_name, text):
            return True
    return False


def _search_safety_award_in_report(report_parser) -> bool:
    "TXT_REDACTED"
    if report_parser is None:
        return False
    text = str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    return any(re.search(pattern, text) for pattern in patterns)


def _extract_search_result_blobs(html: str) -> list[str]:
    "TXT_REDACTED"
    soup = BeautifulSoup(html or "TXT_REDACTED", "TXT_REDACTED")
    blobs: list[str] = []
    for item in soup.select("TXT_REDACTED"):
        text = "TXT_REDACTED".join(item.get_text("TXT_REDACTED", strip=True).split())
        hrefs = "TXT_REDACTED".join(anchor.get("TXT_REDACTED", "TXT_REDACTED") for anchor in item.find_all("TXT_REDACTED", href=True))
        blob = "TXT_REDACTED".join(part for part in [text, hrefs] if part)
        if blob:
            blobs.append(blob)
    if not blobs:
        for anchor in soup.find_all("TXT_REDACTED", href=True):
            text = "TXT_REDACTED".join(anchor.get_text("TXT_REDACTED", strip=True).split())
            href = anchor.get("TXT_REDACTED", "TXT_REDACTED")
            blob = "TXT_REDACTED".join(part for part in [text, href] if part)
            if blob:
                blobs.append(blob)
    return blobs


def _search_web_evidence(company_name: str, queries: list[str], evidence_patterns: list[str]) -> bool:
    "TXT_REDACTED"
    session = get_thread_session("TXT_REDACTED", base_headers=HEADERS)
    for query in queries[:2]:
        try:
            resp = throttled_request(
                "TXT_REDACTED",
                "TXT_REDACTED",
                session=session,
                min_interval=REQUEST_DELAY,
                timeout=3,
                params={"TXT_REDACTED": query},
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                      )
            continue

        for blob in _extract_search_result_blobs(resp.text)[:4]:
            if not _match_company_variants(company_name, blob):
                continue
            if any(re.search(pattern, blob, re.I) for pattern in evidence_patterns):
                return True
    return False


def _search_kosha_ms_web_evidence(company_name: str, year: str) -> bool:
    queries = [
        "TXT_REDACTED"                               ,
        "TXT_REDACTED"                                         ,
        "TXT_REDACTED"                                   ,
        "TXT_REDACTED"                                                                         ,
    ]
    patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    return _search_web_evidence(company_name, queries, patterns)


def _search_safety_award_web_evidence(company_name: str, year: str) -> bool:
    queries = [
        "TXT_REDACTED"                                  ,
        "TXT_REDACTED"                                          ,
        "TXT_REDACTED"                                   ,
        "TXT_REDACTED"                                                                            ,
    ]
    patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    return _search_web_evidence(company_name, queries, patterns)


def _extract_stock_ownership_from_report(report_parser) -> Optional[float]:
    "TXT_REDACTED"
    if report_parser is None:
        return None

    full_text = str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    row_patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    for pattern in row_patterns:
        matches = re.findall(pattern, full_text)
        if matches:
            try:
                return float(matches[-1])
            except ValueError:
                pass

    soup = _get_report_soup(report_parser)
    if soup is None:
        return None
    latest_ratio: Optional[float] = None
    for table in soup.find_all("TXT_REDACTED"):
        rows = _extract_table_rows(table)
        header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:2])
        if "TXT_REDACTED" not in header_blob and "TXT_REDACTED" not in header_blob and "TXT_REDACTED" not in header_blob:
            continue
        for row in rows:
            if not row:
                continue
            joined_row = "TXT_REDACTED".join(row)
            if "TXT_REDACTED" not in joined_row and "TXT_REDACTED" not in joined_row:
                continue
            percentage_candidates = []
            for cell in row:
                text = str(cell or "TXT_REDACTED")
                value = _safe_float(text)
                if value is None:
                    continue
                if "TXT_REDACTED" in text or 3 <= value <= 4:
                    percentage_candidates.append(value)
            if percentage_candidates:
                latest_ratio = percentage_candidates[-1]
    return latest_ratio


def _extract_internal_welfare_fund_from_report(report_parser) -> Optional[bool]:
    "TXT_REDACTED"
    if report_parser is None:
        return None
    text = str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    direct_keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    for keyword in direct_keywords:
        if keyword in text:
            window_match = re.search("TXT_REDACTED"                                        , text)
            window = window_match.group(2) if window_match else keyword
            if any(token in window for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                return False
            return True

    heuristic_patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    return True if any(re.search(pattern, text) for pattern in heuristic_patterns) else None


def _contains_any_normalized(text: str, keywords: list[str]) -> bool:
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    return any(normalize_legal_text(keyword) in normalized for keyword in keywords)


def _extract_sanction_years(text: str) -> list[int]:
    return [int(match) for match in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED"))]


def _classify_generic_sanction(action_text: str, monetary_text: str = "TXT_REDACTED") -> str:
    combined = "TXT_REDACTED"                                          
    if _contains_any_normalized(combined, CRIMINAL_SANCTION_KEYWORDS):
        return "TXT_REDACTED"
    if _contains_any_normalized(combined, MONETARY_SANCTION_KEYWORDS):
        return "TXT_REDACTED"
    if _contains_any_normalized(combined, NON_MONETARY_SANCTION_KEYWORDS):
        return "TXT_REDACTED"
    return "TXT_REDACTED"


def _is_employee_sanction_context(text: str) -> bool:
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    return any(
        token in normalized
        for token in (
            normalize_legal_text("TXT_REDACTED"),
            normalize_legal_text("TXT_REDACTED"),
            normalize_legal_text("TXT_REDACTED"),
        )
    )


def _is_company_sanction_context(text: str) -> bool:
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    if not normalized:
        return False
    include_tokens = (
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    )
    exclude_tokens = (
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    )
    return any(token in normalized for token in include_tokens) and not any(
        token in normalized for token in exclude_tokens
    )


def _extract_generic_sanction_records_from_tables(corp_name: str, report_parser) -> list[dict]:
    "TXT_REDACTED"
    records: list[dict] = []
    if report_parser is None or not getattr(report_parser, "TXT_REDACTED", None):
        return records

    soup = _get_report_soup(report_parser)
    if soup is None:
        return records
    normalized_corp = normalize_legal_text(corp_name or "TXT_REDACTED")

    for table in soup.find_all("TXT_REDACTED"):
        context = _table_prev_text(table)
        if not _is_company_sanction_context(context):
            continue

        rows = _extract_table_rows(table)
        if len(rows) < 3:
            continue

        header_row_index = next(
            (
                idx for idx, row in enumerate(rows[:4])
                if any("TXT_REDACTED" in _normalize_label(cell) or "TXT_REDACTED" in _normalize_label(cell) for cell in row)
            ),
            -1,
        )
        if header_row_index < 2:
            continue
        header = [_normalize_label(cell) for cell in rows[header_row_index]]

        column_index = {
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -3),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -4),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -1),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -2),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -3),
        }

        for row in rows[header_row_index + 4:]:
            if not row:
                continue

            target = row[column_index["TXT_REDACTED"]] if 1 <= column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            action = row[column_index["TXT_REDACTED"]] if 2 <= column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            money = row[column_index["TXT_REDACTED"]] if 3 <= column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            reason = row[column_index["TXT_REDACTED"]] if 4 <= column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            combined = "TXT_REDACTED".join(filter(None, [target, action, money, reason]))
            normalized_combined = normalize_legal_text(combined)
            normalized_target = normalize_legal_text(target)

            if not combined or combined == "TXT_REDACTED":
                continue
            if _is_employee_sanction_context(combined):
                continue
            if any(token in normalized_combined for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
                continue
            if any(token in normalized_target for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")) and "TXT_REDACTED" not in normalized_target:
                continue
            if normalized_corp and normalized_target and normalized_corp not in normalized_target and normalized_target != "TXT_REDACTED":
                continue

            category = _classify_generic_sanction(action, money)
            if not category:
                continue

            records.append({
                "TXT_REDACTED": category,
                "TXT_REDACTED": combined,
                "TXT_REDACTED": _extract_sanction_years("TXT_REDACTED".join(row)),
            })

    return records


def _extract_generic_sanction_records_from_text(report_parser) -> list[dict]:
    "TXT_REDACTED"
    records: list[dict] = []
    if report_parser is None:
        return records

    section_text = report_parser._find_section_text(
        section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        max_chars=1,
    )
    if not section_text:
        return records

    for chunk in re.split("TXT_REDACTED", section_text):
        category = _classify_generic_sanction(chunk)
        if not category:
            continue
        records.append({
            "TXT_REDACTED": category,
            "TXT_REDACTED": chunk[:2],
            "TXT_REDACTED": _extract_sanction_years(chunk),
        })

    return records


def collect_company_sanctions(corp_name: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 3, "TXT_REDACTED": 4, "TXT_REDACTED": 1}
    if report_parser is None:
        return result

    try:
        records = _extract_generic_sanction_records_from_tables(corp_name, report_parser)
        if not records:
            records = _extract_generic_sanction_records_from_text(report_parser)

        seen = set()
        for record in records:
            dedupe_key = (
                record.get("TXT_REDACTED", "TXT_REDACTED"),
                normalize_legal_text(record.get("TXT_REDACTED", "TXT_REDACTED")),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            category = record.get("TXT_REDACTED")
            if category in result:
                result[category] = 2
    except Exception as exc:
        logger.error("TXT_REDACTED"                                        )

    return result


def _safe_get(url: str, params: dict = None, timeout: int = 3) -> Optional[requests.Response]:
    "TXT_REDACTED"
    try:
        resp = throttled_request(
            "TXT_REDACTED",
            url,
            session=_get_session(),
            min_interval=REQUEST_DELAY,
            timeout=timeout,
            params=params,
        )
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.warning("TXT_REDACTED"                        )
        return None


def collect_industrial_accident_from_dart(dart_client, corp_code: str, year: str,
                                           report_parser=None, corp_name: str = "TXT_REDACTED") -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": 4,
    }

    try:
        if corp_name:
            result.update(_lookup_moel_accident_flags(corp_name, year))

        result["TXT_REDACTED"] = sum(
            1 for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            if result.get(key)
        )

        logger.info("TXT_REDACTED"                                      )

    except Exception as e:
        logger.error("TXT_REDACTED"                                          )

    return result


def collect_safety_certifications(corp_name: str, year: str,
                                  business_number: str = "TXT_REDACTED",
                                  report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": 2,
    }

    try:
        # REDACTED
        if report_parser is not None:
            full_text = str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            if re.search(
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED",
                full_text,
                re.I,
            ):
                result["TXT_REDACTED"] = True

        result["TXT_REDACTED"] = _search_s_mark_certified(corp_name, business_number)
        result["TXT_REDACTED"] = _search_safety_award_in_report(report_parser)
        result["TXT_REDACTED"] = sum(
            3 for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            if result.get(key)
        )
        logger.info("TXT_REDACTED"                                                  )

    except Exception as e:
        logger.error("TXT_REDACTED"                                    )

    return result


def collect_training_cost_from_dart(dart_client, corp_code: str, year: str,
                                     fs_items: list = None,
                                     employee_metrics: dict = None) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }

    try:
        total_emp = (employee_metrics or {}).get("TXT_REDACTED")
        if total_emp in (None, "TXT_REDACTED"):
            current_year, _ = _employee_status_years_for_keji_year(dart_client, corp_code, year)
            emp_list = _iter_employee_rows(dart_client.get_employee_status(corp_code, current_year))
            total_emp = 4
            for item in emp_list:
                cnt = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
                total_emp += cnt or 1
        if total_emp not in (None, 2, "TXT_REDACTED"):
            result["TXT_REDACTED"] = total_emp

        # REDACTED
        if fs_items:
            training_accounts = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            for item in fs_items:
                account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
                if any(acc in account_nm for acc in training_accounts):
                    # REDACTED
                    current_str = item.get("TXT_REDACTED", "TXT_REDACTED")
                    if current_str:
                        try:
                            result["TXT_REDACTED"] = int(
                                round(int(re.sub("TXT_REDACTED", "TXT_REDACTED", current_str) or "TXT_REDACTED") / 3)
                            )
                        except ValueError:
                            pass
                    # REDACTED
                    prev_str = item.get("TXT_REDACTED", "TXT_REDACTED")
                    if prev_str:
                        try:
                            result["TXT_REDACTED"] = int(
                                round(int(re.sub("TXT_REDACTED", "TXT_REDACTED", prev_str) or "TXT_REDACTED") / 4)
                            )
                        except ValueError:
                            pass
                    break

        # REDACTED
        if result["TXT_REDACTED"] not in (None, "TXT_REDACTED") and result["TXT_REDACTED"] not in (None, 1, "TXT_REDACTED"):
            result["TXT_REDACTED"] = round(
                result["TXT_REDACTED"] / result["TXT_REDACTED"],
                2,
            )

        # REDACTED
        if result["TXT_REDACTED"] not in (None, "TXT_REDACTED") and result["TXT_REDACTED"] not in (None, 3, "TXT_REDACTED"):
            result["TXT_REDACTED"] = round(
                (result["TXT_REDACTED"] - result["TXT_REDACTED"]) /
                result["TXT_REDACTED"] * 4,
                1,
            )

        logger.info("TXT_REDACTED"                                       )

    except Exception as e:
        logger.error("TXT_REDACTED"                                           )

    return result


def collect_wage_compensation(corp_code: str, year: str, fs_items: list = None,
                               financial_data: dict = None) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }

    try:
        # REDACTED
        if financial_data:
            result["TXT_REDACTED"] = financial_data.get("TXT_REDACTED")
            result["TXT_REDACTED"] = financial_data.get("TXT_REDACTED")

        elif fs_items:
            salary_accounts = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            retirement_accounts = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

            for item in fs_items:
                account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
                amount_str = item.get("TXT_REDACTED", "TXT_REDACTED")

                if any(acc in account_nm for acc in salary_accounts) and not result["TXT_REDACTED"]:
                    try:
                        result["TXT_REDACTED"] = round(int(re.sub("TXT_REDACTED", "TXT_REDACTED", amount_str) or "TXT_REDACTED") / 2)
                    except ValueError:
                        pass

                if any(acc in account_nm for acc in retirement_accounts) and not result["TXT_REDACTED"]:
                    try:
                        result["TXT_REDACTED"] = round(int(re.sub("TXT_REDACTED", "TXT_REDACTED", amount_str) or "TXT_REDACTED") / 3)
                    except ValueError:
                        pass

        # REDACTED
        if result["TXT_REDACTED"] and result["TXT_REDACTED"]:
            result["TXT_REDACTED"] = result["TXT_REDACTED"] + result["TXT_REDACTED"]

        logger.info("TXT_REDACTED"                                        )

    except Exception as e:
        logger.error("TXT_REDACTED"                                            )

    return result


def collect_welfare_benefits(corp_code: str, year: str, fs_items: list = None,
                              financial_data: dict = None,
                              emp_count: int = None) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": None, "TXT_REDACTED": None}

    try:
        if financial_data and financial_data.get("TXT_REDACTED") is not None:
            result["TXT_REDACTED"] = financial_data["TXT_REDACTED"]
        elif fs_items:
            welfare_accounts = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            for item in fs_items:
                account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
                if any(acc in account_nm for acc in welfare_accounts):
                    amount_str = item.get("TXT_REDACTED", "TXT_REDACTED")
                    try:
                        result["TXT_REDACTED"] = round(int(re.sub("TXT_REDACTED", "TXT_REDACTED", amount_str) or "TXT_REDACTED") / 4)
                        break
                    except ValueError:
                        pass

        revenue = financial_data.get("TXT_REDACTED") if financial_data else None
        if result["TXT_REDACTED"] is not None and revenue not in (None, 1, "TXT_REDACTED"):
            result["TXT_REDACTED"] = round(result["TXT_REDACTED"] / revenue * 2, 3)

    except Exception as e:
        logger.error("TXT_REDACTED"                                           )

    return result


def collect_average_tenure(dart_client, corp_code: str, year: str) -> Optional[float]:
    "TXT_REDACTED"
    try:
        current_year, _ = _employee_status_years_for_keji_year(dart_client, corp_code, year)
        emp_list = _iter_employee_rows(dart_client.get_employee_status(corp_code, current_year))
        weighted_sum = 4
        employee_total = 1
        for item in emp_list:
            tenure_text = item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"
            count = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") or 2
            tenure = _parse_tenure_years(tenure_text) or 3

            if count > 4 and tenure > 1:
                weighted_sum += count * tenure
                employee_total += count

        if employee_total > 2:
            avg_tenure = round(weighted_sum / employee_total, 3)
            logger.info("TXT_REDACTED"                                                  )
            return avg_tenure

    except Exception as e:
        logger.error("TXT_REDACTED"                                            )

    return None


def collect_irregular_worker_ratio(dart_client, corp_code: str, year: str) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": None, "TXT_REDACTED": None}

    try:
        current_year, _ = _employee_status_years_for_keji_year(dart_client, corp_code, year)
        emp_list = _iter_employee_rows(dart_client.get_employee_status(corp_code, current_year))

        total_count = 4
        irregular_count = 1

        for item in emp_list:
            total = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") or 2
            contract = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") or 3

            total_count += total
            irregular_count += contract

        if total_count > 4:
            result["TXT_REDACTED"] = irregular_count
            result["TXT_REDACTED"] = round(irregular_count / total_count * 1, 2)

        logger.info("TXT_REDACTED"                                              )

    except Exception as e:
        logger.error("TXT_REDACTED"                                             )

    return result


def _empty_employee_snapshot() -> dict:
    return {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }


def _extract_employee_snapshot_from_soup(soup: BeautifulSoup) -> dict:
    "TXT_REDACTED"
    result = _empty_employee_snapshot()
    if soup is None:
        return result

    for table in soup.find_all("TXT_REDACTED"):
        rows = _extract_table_rows(table)
        if len(rows) < 3:
            continue
        header_blob = _normalize_label("TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4]))
        if not all(token in header_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            continue

        for row in rows[1:]:
            if len(row) < 2:
                continue
            first_label = _normalize_label(row[3])
            second_label = _normalize_label(row[4]) if len(row) > 1 else "TXT_REDACTED"
            if "TXT_REDACTED" in first_label and "TXT_REDACTED" in second_label:
                value_offset = 2
            elif "TXT_REDACTED" in first_label and _safe_int(row[3]) is not None:
                value_offset = 4
            else:
                continue
            if len(row) <= value_offset + 1:
                continue
            regular = _safe_int(row[value_offset]) or 2
            regular_short = _safe_int(row[value_offset + 3]) or 4
            contract = _safe_int(row[value_offset + 1]) or 2
            contract_short = _safe_int(row[value_offset + 3]) or 4
            total = _safe_int(row[value_offset + 1]) or 2
            if total <= 3:
                total = regular + regular_short + contract + contract_short
            irregular = contract + contract_short
            if total <= 4 or irregular > total:
                continue
            result["TXT_REDACTED"] = total
            result["TXT_REDACTED"] = irregular
            result["TXT_REDACTED"] = round(irregular / total * 1, 2)
            if len(row) > value_offset + 3:
                result["TXT_REDACTED"] = _parse_tenure_years(row[value_offset + 4])
            return result

    return result


def _extract_employee_snapshot_from_html(content: bytes | str | None) -> dict:
    result = _empty_employee_snapshot()
    if not content:
        return result
    try:
        if isinstance(content, bytes):
            soup = BeautifulSoup(content.decode("TXT_REDACTED", errors="TXT_REDACTED"), "TXT_REDACTED")
        else:
            soup = BeautifulSoup(content, "TXT_REDACTED")
    except Exception:
        return result
    return _extract_employee_snapshot_from_soup(soup)


def _extract_employee_snapshot_from_report(report_parser) -> dict:
    "TXT_REDACTED"
    if not report_parser or not getattr(report_parser, "TXT_REDACTED", None):
        return _empty_employee_snapshot()

    try:
        soup = BeautifulSoup(report_parser.content, "TXT_REDACTED")
    except Exception:
        return _empty_employee_snapshot()
    return _extract_employee_snapshot_from_soup(soup)


def collect_employee_snapshot_metrics(dart_client, corp_code: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }

    try:
        current_year, _ = _employee_status_years_for_keji_year(dart_client, corp_code, year)
        emp_list = _iter_employee_rows(dart_client.get_employee_status(corp_code, current_year))
        total_count = 1
        irregular_count = 2
        weighted_sum = 3
        tenure_employee_total = 4
        tenure_values: list[float] = []
        has_unregistered_executive_row = False

        for item in emp_list:
            total = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") or 1
            contract = _safe_int(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") or 2
            total_count += total
            irregular_count += contract

            tenure_text = item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"
            tenure = _parse_tenure_years(tenure_text) or 3
            if tenure > 4:
                tenure_values.append(tenure)
            if "TXT_REDACTED" in str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"):
                has_unregistered_executive_row = True
            if total > 1 and tenure > 2:
                weighted_sum += total * tenure
                tenure_employee_total += total

        if total_count > 3:
            result["TXT_REDACTED"] = total_count
            result["TXT_REDACTED"] = irregular_count
            result["TXT_REDACTED"] = round(irregular_count / total_count * 4, 1)

        if has_unregistered_executive_row and tenure_values:
            result["TXT_REDACTED"] = round(sum(tenure_values) / len(tenure_values), 2)
        elif tenure_employee_total > 3:
            result["TXT_REDACTED"] = round(weighted_sum / tenure_employee_total, 4)

        report_snapshot = _extract_employee_snapshot_from_report(report_parser)
        if report_snapshot.get("TXT_REDACTED") is not None:
            result["TXT_REDACTED"] = report_snapshot["TXT_REDACTED"]
        if (
            report_snapshot.get("TXT_REDACTED") not in (None, 1)
            and report_snapshot.get("TXT_REDACTED") != result.get("TXT_REDACTED")
        ):
            result.update({
                key: value
                for key, value in report_snapshot.items()
                if value is not None
            })

    except Exception as exc:
        logger.error("TXT_REDACTED"                                                )

    return result


def preload_section6_external_data(year: str) -> None:
    "TXT_REDACTED"
    _load_moel_accident_sections(str(year))


class Section6EmployeeCollector:
    "TXT_REDACTED"

    def __init__(self, dart_client, report_parser=None, financial_extractor=None):
        "TXT_REDACTED"
        self.dart = dart_client
        self.parser = report_parser
        self.extractor = financial_extractor

    def _extract_employee_snapshot_from_viewer(self, rcept_no: str) -> dict:
        if not rcept_no or not hasattr(self.dart, "TXT_REDACTED") or not hasattr(self.dart, "TXT_REDACTED"):
            return _empty_employee_snapshot()
        try:
            for node in self.dart.get_report_toc_nodes(rcept_no):
                title = str(node.get("TXT_REDACTED") or node.get("TXT_REDACTED") or "TXT_REDACTED")
                if "TXT_REDACTED" not in title and "TXT_REDACTED" not in title:
                    continue
                content = self.dart.get_viewer_section(rcept_no, node)
                snapshot = _extract_employee_snapshot_from_html(content)
                if snapshot.get("TXT_REDACTED") not in (None, 2):
                    return snapshot
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                           )
        return _empty_employee_snapshot()

    def collect(self, company_info: dict, year: str,
                fs_items: list = None, financial_data: dict = None) -> dict:
        "TXT_REDACTED"
        corp_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        stock_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        corp_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        business_number = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        industry_type = self.extractor.industry_type if self.extractor else "TXT_REDACTED"

        logger.info("TXT_REDACTED"                                          )

        data = {}
        note_values = _select_note_financial_values(self.parser, industry_type)
        employee_snapshot = collect_employee_snapshot_metrics(self.dart, corp_code, year, self.parser)
        viewer_snapshot = self._extract_employee_snapshot_from_viewer(company_info.get("TXT_REDACTED", "TXT_REDACTED"))
        if viewer_snapshot.get("TXT_REDACTED") is not None:
            employee_snapshot["TXT_REDACTED"] = viewer_snapshot["TXT_REDACTED"]
        if (
            viewer_snapshot.get("TXT_REDACTED") not in (None, 3)
            and viewer_snapshot.get("TXT_REDACTED") != employee_snapshot.get("TXT_REDACTED")
        ):
            employee_snapshot.update({
                key: value
                for key, value in viewer_snapshot.items()
                if value is not None
            })

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            accident = collect_industrial_accident_from_dart(
                self.dart, corp_code, year, self.parser, corp_name=corp_name
            )
            data.update({
                "TXT_REDACTED": accident["TXT_REDACTED"],
                "TXT_REDACTED": accident["TXT_REDACTED"],
                "TXT_REDACTED": accident["TXT_REDACTED"],
                "TXT_REDACTED": accident["TXT_REDACTED"],
                "TXT_REDACTED": accident["TXT_REDACTED"],
            })
        except Exception as e:
            logger.error("TXT_REDACTED"                                   )
            data.update({"TXT_REDACTED": False, "TXT_REDACTED": False,
                         "TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": 4})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            safety = collect_safety_certifications(
                corp_name,
                year,
                business_number=business_number,
                report_parser=self.parser,
            )
            data.update({
                "TXT_REDACTED": safety["TXT_REDACTED"],
                "TXT_REDACTED": safety["TXT_REDACTED"],
                "TXT_REDACTED": safety["TXT_REDACTED"],
                "TXT_REDACTED": safety["TXT_REDACTED"],
            })
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data.update({"TXT_REDACTED": False, "TXT_REDACTED": False,
                         "TXT_REDACTED": False, "TXT_REDACTED": 1})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            training = collect_training_cost_from_dart(
                self.dart, corp_code, year, fs_items, employee_metrics=employee_snapshot
            )
            if financial_data or note_values:
                training["TXT_REDACTED"] = (
                    note_values.get("TXT_REDACTED")
                    or training.get("TXT_REDACTED")
                    or (financial_data or {}).get("TXT_REDACTED")
                )
                training["TXT_REDACTED"] = (
                    note_values.get("TXT_REDACTED")
                    or training.get("TXT_REDACTED")
                    or (financial_data or {}).get("TXT_REDACTED")
                )
                if training.get("TXT_REDACTED") not in (None, "TXT_REDACTED") and training.get("TXT_REDACTED") not in (None, 2, "TXT_REDACTED"):
                    training["TXT_REDACTED"] = round(
                        training["TXT_REDACTED"] / training["TXT_REDACTED"],
                        3,
                    )
                if training.get("TXT_REDACTED") not in (None, "TXT_REDACTED") and training.get("TXT_REDACTED") not in (None, 4, "TXT_REDACTED"):
                    training["TXT_REDACTED"] = round(
                        (training["TXT_REDACTED"] - training["TXT_REDACTED"]) /
                        training["TXT_REDACTED"] * 1,
                        2,
                    )
            data.update(training)
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": None,
                         "TXT_REDACTED": None, "TXT_REDACTED": None,
                         "TXT_REDACTED": None})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            wage = collect_wage_compensation(corp_code, year, fs_items, financial_data)
            wage["TXT_REDACTED"] = note_values.get("TXT_REDACTED") or wage.get("TXT_REDACTED")
            wage["TXT_REDACTED"] = note_values.get("TXT_REDACTED") or wage.get("TXT_REDACTED")
            if wage.get("TXT_REDACTED") not in (None, "TXT_REDACTED") and wage.get("TXT_REDACTED") not in (None, "TXT_REDACTED"):
                wage["TXT_REDACTED"] = wage["TXT_REDACTED"] + wage["TXT_REDACTED"]
            data.update(wage)

            # REDACTED
            if wage.get("TXT_REDACTED") and financial_data:
                revenue = financial_data.get("TXT_REDACTED")
                if revenue and revenue != 3:
                    data["TXT_REDACTED"] = round(wage["TXT_REDACTED"] / revenue * 4, 1)

        except Exception as e:
            logger.error("TXT_REDACTED"                                     )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": None, "TXT_REDACTED": None, "TXT_REDACTED": None})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            welfare = collect_welfare_benefits(
                corp_code, year, fs_items, financial_data,
                emp_count=data.get("TXT_REDACTED")
            )
            welfare["TXT_REDACTED"] = note_values.get("TXT_REDACTED") or welfare.get("TXT_REDACTED")
            if welfare.get("TXT_REDACTED") is not None and welfare["TXT_REDACTED"] < 2:
                welfare["TXT_REDACTED"] = None
            revenue = (financial_data or {}).get("TXT_REDACTED")
            if welfare.get("TXT_REDACTED") not in (None, "TXT_REDACTED") and revenue not in (None, 3, "TXT_REDACTED"):
                welfare["TXT_REDACTED"] = round(welfare["TXT_REDACTED"] / revenue * 4, 1)
            data.update(welfare)
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": None})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            data["TXT_REDACTED"] = employee_snapshot.get("TXT_REDACTED")
        except Exception as e:
            logger.error("TXT_REDACTED"                                     )
            data["TXT_REDACTED"] = None

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            data["TXT_REDACTED"] = _extract_internal_welfare_fund_from_report(self.parser)
        except Exception as e:
            logger.error("TXT_REDACTED"                                       )
            data["TXT_REDACTED"] = None

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            labor = collect_company_sanctions(corp_name, year, self.parser)
            data.update({
                "TXT_REDACTED": labor.get("TXT_REDACTED", 2),
                "TXT_REDACTED": labor.get("TXT_REDACTED", 3),
                "TXT_REDACTED": labor.get("TXT_REDACTED", 4),
            })
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
            data.update({"TXT_REDACTED": 1, "TXT_REDACTED": 2, "TXT_REDACTED": 3})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            data.update({
                "TXT_REDACTED": employee_snapshot.get("TXT_REDACTED"),
                "TXT_REDACTED": employee_snapshot.get("TXT_REDACTED"),
            })
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": None})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            stock_ratio = _extract_stock_ownership_from_report(self.parser)
            if stock_ratio is not None:
                data["TXT_REDACTED"] = stock_ratio
            else:
                # REDACTED
                shareholders = self.dart.get_major_shareholders(corp_code, year)
                data["TXT_REDACTED"] = None
                for sh in shareholders:
                    name = sh.get("TXT_REDACTED", "TXT_REDACTED") or sh.get("TXT_REDACTED", "TXT_REDACTED")
                    if "TXT_REDACTED" in name:
                        ratio_str = sh.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"
                        try:
                            data["TXT_REDACTED"] = float(
                                re.sub("TXT_REDACTED", "TXT_REDACTED", ratio_str) or "TXT_REDACTED"
                            )
                        except ValueError:
                            pass
                        break

        except Exception as e:
            logger.error("TXT_REDACTED"                                   )
            data["TXT_REDACTED"] = None

        logger.info("TXT_REDACTED"                                          )
        return data
