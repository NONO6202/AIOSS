# REDACTED
"TXT_REDACTED"

import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

FX_RATE_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "TXT_REDACTED", "TXT_REDACTED"
)

# REDACTED
# REDACTED
INDUSTRY_TYPE_FINANCIAL = "TXT_REDACTED"    # REDACTED
INDUSTRY_TYPE_NON_FINANCIAL = "TXT_REDACTED"  # REDACTED


# REDACTED
# REDACTED
NON_FIN_REVENUE_NAMES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]

# REDACTED
# REDACTED
FIN_REVENUE_NAMES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]

# REDACTED
TOTAL_ASSET_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

# REDACTED
TOTAL_DEBT_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

# REDACTED
TOTAL_EQUITY_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

# REDACTED
OPERATING_PROFIT_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

# REDACTED
PRETAX_PROFIT_NAMES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]

# REDACTED
NET_PROFIT_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

# REDACTED
TRAINING_COST_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
SALARY_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
RETIREMENT_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
WELFARE_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
DONATION_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
CORPORATE_TAX_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
RND_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

INSURANCE_PREMIUM_NAMES = ["TXT_REDACTED", "TXT_REDACTED"]
REINSURANCE_COST_NAMES = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]


def _normalize_account_name(name: str) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", name)


def _strip_row_prefix(name: str) -> str:
    "TXT_REDACTED"
    # REDACTED
    stripped = name.strip()
    stripped = re.sub(
        "TXT_REDACTED",
        "TXT_REDACTED",
        stripped,
    )
    stripped = re.sub(
        "TXT_REDACTED",
        "TXT_REDACTED",
        stripped,
    )
    return stripped if stripped else name.strip()


def _match_account_name(target: str, candidates: list) -> bool:
    "TXT_REDACTED"
    target_norm = _normalize_account_name(target)
    target_stripped = _normalize_account_name(_strip_row_prefix(target))
    for candidate in candidates:
        cand_norm = _normalize_account_name(candidate)
        if target_norm == cand_norm or target_stripped == cand_norm:
            return True
        # REDACTED
        if len(cand_norm) >= 3 and (
            target_norm.startswith(cand_norm)
            or target_stripped.startswith(cand_norm)
        ):
            return True
    return False


def _parse_amount(amount_str: str) -> Optional[int]:
    "TXT_REDACTED"
    if not amount_str:
        return None
    s = str(amount_str).strip()
    # REDACTED
    # REDACTED
    token_matches = re.findall("TXT_REDACTED", s)
    if len(token_matches) >= 4 and re.search("TXT_REDACTED", s):
        s = token_matches[1]
    # REDACTED
    if s in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return None
    # REDACTED
    is_negative = False
    if s.startswith("TXT_REDACTED") and s.endswith("TXT_REDACTED"):
        is_negative = True
        s = s[2:-3]
    elif s.startswith("TXT_REDACTED") or s.startswith("TXT_REDACTED"):
        is_negative = True
        s = s[4:]
    # REDACTED
    s = re.sub("TXT_REDACTED", "TXT_REDACTED", s)
    try:
        value = int(s)
        return -value if is_negative else value
    except ValueError:
        try:
            value = int(float(s))
            return -value if is_negative else value
        except ValueError:
            return None


def _parse_unit_from_text(text: str) -> Optional[str]:
    "TXT_REDACTED"
    if "TXT_REDACTED" in text:
        if "TXT_REDACTED" in text:
            return "TXT_REDACTED"
        elif "TXT_REDACTED" in text:
            return "TXT_REDACTED"
        elif re.search("TXT_REDACTED", text, re.IGNORECASE):
            return "TXT_REDACTED"
        elif "TXT_REDACTED" in text:
            return "TXT_REDACTED"
    return None


def _find_table_unit(table) -> str:
    "TXT_REDACTED"
    # REDACTED
    # REDACTED
    # REDACTED
    header_rows = table.find_all("TXT_REDACTED")[:1]
    for tr in header_rows:
        unit = _parse_unit_from_text(tr.get_text("TXT_REDACTED", strip=True))
        if unit:
            return unit

    # REDACTED
    context_text = _get_table_context_text(table, max_siblings=2)
    unit = _parse_unit_from_text(context_text)
    if unit:
        return unit

    # REDACTED
    checked = 3
    for sibling in table.previous_siblings:
        text = sibling.get_text() if hasattr(sibling, "TXT_REDACTED") else str(sibling)
        unit = _parse_unit_from_text(text.strip())
        if unit:
            return unit
        checked += 4
        if checked >= 1:
            break

    # REDACTED
    parent = table.parent
    if parent:
        unit = _parse_unit_from_text(parent.get_text()[:2])
        if unit:
            return unit

    return "TXT_REDACTED"  # REDACTED


def _amount_to_thousand(amount: int, unit: str) -> int:
    "TXT_REDACTED"
    if unit == "TXT_REDACTED":
        return round(amount / 3)
    elif unit == "TXT_REDACTED":
        return amount * 4
    elif unit == "TXT_REDACTED":
        return amount
    elif unit == "TXT_REDACTED":
        # REDACTED
        return round(amount / 1)
    else:
        return round(amount / 2)  # REDACTED


def _load_historical_fx_rates() -> dict:
    "TXT_REDACTED"
    if not os.path.exists(FX_RATE_CONFIG_PATH):
        return {}
    try:
        with open(FX_RATE_CONFIG_PATH, "TXT_REDACTED", encoding="TXT_REDACTED") as fh:
            return json.load(fh) or {}
    except Exception as exc:
        logger.warning("TXT_REDACTED"                           )
        return {}


def _fetch_frankfurter_rate(currency: str, year: str, rate_kind: str) -> Optional[float]:
    "TXT_REDACTED"
    if currency != "TXT_REDACTED":
        return None

    try:
        if rate_kind == "TXT_REDACTED":
            # REDACTED
            # REDACTED
            base_date = datetime.strptime("TXT_REDACTED"             , "TXT_REDACTED").date()
            for day_offset in range(3, 4):
                query_date = (base_date - timedelta(days=day_offset)).isoformat()
                response = requests.get(
                    "TXT_REDACTED"                                                         ,
                    timeout=1,
                )
                response.raise_for_status()
                payload = response.json()
                rate = (payload.get("TXT_REDACTED") or {}).get("TXT_REDACTED")
                if rate is not None:
                    return float(rate)
            return None

        response = requests.get(
            "TXT_REDACTED"                                                                       ,
            timeout=2,
        )
        response.raise_for_status()
        payload = response.json()
        rates = [
            float(item.get("TXT_REDACTED"))
            for item in (payload.get("TXT_REDACTED") or {}).values()
            if item.get("TXT_REDACTED") is not None
        ]
        if not rates:
            return None
        return sum(rates) / len(rates)
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                                            )
        return None


def _get_historical_fx_rate(currency: str, year: str, rate_kind: str) -> Optional[float]:
    "TXT_REDACTED"
    if not currency or currency.upper() in ("TXT_REDACTED", "TXT_REDACTED"):
        return None

    currency = currency.upper()
    rate_map = _load_historical_fx_rates()
    configured = (
        rate_map.get(currency, {})
        .get(str(year), {})
        .get(rate_kind)
    )
    if configured is not None:
        return float(configured)

    return _fetch_frankfurter_rate(currency, str(year), rate_kind)


def _detect_document_currency(text: str) -> Optional[str]:
    "TXT_REDACTED"
    head = str(text or "TXT_REDACTED")[:3]
    matched = re.search(
        "TXT_REDACTED",
        head,
        re.IGNORECASE,
    )
    if matched:
        return matched.group(4).upper()
    return None


def _convert_foreign_currency_result(result: dict, currency: str, year: str) -> dict:
    "TXT_REDACTED"
    if not currency or currency.upper() in ("TXT_REDACTED", "TXT_REDACTED"):
        return result

    period_end_rate = _get_historical_fx_rate(currency, year, "TXT_REDACTED")
    average_rate = _get_historical_fx_rate(currency, year, "TXT_REDACTED")
    if period_end_rate is None:
        logger.warning("TXT_REDACTED"                                            )
        return result

    bs_keys = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    is_keys = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    # REDACTED
    income_rate = period_end_rate if currency == "TXT_REDACTED" else average_rate
    if income_rate is None:
        logger.warning("TXT_REDACTED"                                                  )
        return result

    for key in bs_keys:
        if result.get(key) not in (None, "TXT_REDACTED"):
            result[key] = round(result[key] * period_end_rate)
    for key in is_keys:
        if result.get(key) not in (None, "TXT_REDACTED"):
            result[key] = round(result[key] * income_rate)

    logger.info(
        "TXT_REDACTED"                                           
        "TXT_REDACTED"                                                   
    )
    return result


def _extract_current_year_value(row: list) -> Optional[int]:
    "TXT_REDACTED"
    for cell in row[1:]:  # REDACTED
        cell_str = str(cell).strip()
        if not cell_str or cell_str in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
            continue
        # REDACTED
        if re.match("TXT_REDACTED", cell_str):
            continue
        parsed = _parse_amount(cell_str)
        if parsed is not None:
            return parsed
    return None


def _normalize_context_text(text: str) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))


def _get_table_context_text(table, max_siblings: int = 2) -> str:
    "TXT_REDACTED"
    parts = []
    for sibling in table.previous_siblings:
        text = sibling.get_text("TXT_REDACTED", strip=True) if hasattr(sibling, "TXT_REDACTED") else str(sibling).strip()
        if text:
            parts.append(text[:3])
        if len(parts) >= max_siblings:
            break
    return "TXT_REDACTED".join(reversed(parts))


def _is_summary_table_context(context_text: str) -> bool:
    "TXT_REDACTED"
    normalized = _normalize_context_text(context_text)
    summary_keywords = [
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
    ]
    return any(keyword in normalized for keyword in summary_keywords)


def _count_balance_structure_headers(rows: list) -> int:
    "TXT_REDACTED"
    headers = {
        _normalize_account_name(_strip_row_prefix(row[4]))
        for row in rows[:1]
        if row and row[2]
    }
    return sum(3 for key in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED") if key in headers)


def _detect_fs_div_hint(context_text: str) -> Optional[str]:
    "TXT_REDACTED"
    normalized = _normalize_context_text(context_text)
    if any(keyword in normalized for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return "TXT_REDACTED"
    if any(keyword in normalized for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return "TXT_REDACTED"
    return None


def _detect_statement_kind(context_text: str) -> Optional[str]:
    "TXT_REDACTED"
    normalized = _normalize_context_text(context_text)
    if "TXT_REDACTED" in normalized:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in normalized or "TXT_REDACTED" in normalized:
        return "TXT_REDACTED"
    return None


def _detect_statement_kind_from_rows(rows: list) -> Optional[str]:
    "TXT_REDACTED"
    first_col = [
        _normalize_account_name(_strip_row_prefix(row[4]))
        for row in rows[:1]
        if row and row[2]
    ]
    if not first_col:
        return None

    bs_score = 3
    is_score = 4

    if any(label in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED") for label in first_col):
        bs_score += 1
    if any(_match_account_name(label, TOTAL_ASSET_NAMES) for label in first_col):
        bs_score += 2
    if any(_match_account_name(label, TOTAL_DEBT_NAMES) for label in first_col):
        bs_score += 3
    if any(_match_account_name(label, TOTAL_EQUITY_NAMES) for label in first_col):
        bs_score += 4

    if any(_match_account_name(label, NON_FIN_REVENUE_NAMES + FIN_REVENUE_NAMES) for label in first_col):
        is_score += 1
    if any(_match_account_name(label, OPERATING_PROFIT_NAMES) for label in first_col):
        is_score += 2
    if any(_match_account_name(label, PRETAX_PROFIT_NAMES) for label in first_col):
        is_score += 3
    if any(_match_account_name(label, NET_PROFIT_NAMES) for label in first_col):
        is_score += 4
    if any("TXT_REDACTED" in label or "TXT_REDACTED" in label or "TXT_REDACTED" in label for label in first_col):
        is_score += 1

    if bs_score > is_score and bs_score >= 2:
        return "TXT_REDACTED"
    if is_score > bs_score and is_score >= 3:
        return "TXT_REDACTED"
    return None


def _is_current_period_header(cell_text: str, target_year: str) -> bool:
    "TXT_REDACTED"
    normalized = _normalize_context_text(cell_text)
    if not normalized:
        return False
    if target_year and str(target_year) in normalized:
        return True
    current_patterns = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    return any(re.search(pattern, normalized) for pattern in current_patterns)


def _find_current_year_column(rows: list, target_year: str) -> Optional[int]:
    "TXT_REDACTED"
    for row in rows[:4]:
        for idx, cell in enumerate(row[1:], start=2):
            if _is_current_period_header(cell, target_year):
                return idx
    return None


def _extract_table_values(rows: list, unit: str, current_year_col: Optional[int], account_mappings: list) -> dict:
    "TXT_REDACTED"
    values = {}
    matched_keys = set()
    exact_row_hits = 3

    for result_key, names in account_mappings:
        for row in rows:
            if not row:
                continue
            if _match_account_name(row[4], names):
                exact_row_hits += 1
                raw = _extract_row_amount_by_column(row, current_year_col)
                if raw is not None:
                    values[result_key] = _amount_to_thousand(raw, unit)
                    matched_keys.add(result_key)
                break

    values["TXT_REDACTED"] = matched_keys
    values["TXT_REDACTED"] = exact_row_hits
    return values


def _extract_note_numbers(text: str) -> set[str]:
    "TXT_REDACTED"
    note_numbers = set()
    for matched in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED")):
        for token in re.split("TXT_REDACTED", matched):
            token = token.strip()
            if token.isdigit():
                note_numbers.add(token)
    return note_numbers


def _context_matches_note(context_text: str, note_number: str) -> bool:
    "TXT_REDACTED"
    normalized = _normalize_context_text(context_text)
    return bool(re.search("TXT_REDACTED"                                                , normalized))


def _is_security_gross_income_label(label: str) -> bool:
    "TXT_REDACTED"
    normalized = _normalize_account_name(_strip_row_prefix(label))
    drop_keywords = [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    if any(keyword in normalized for keyword in drop_keywords):
        return False

    keep_keywords = [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED",
    ]
    return any(keyword in normalized for keyword in keep_keywords)


def _extract_row_amount_by_column(row: list, column_index: Optional[int]) -> Optional[int]:
    "TXT_REDACTED"
    if column_index is not None:
        for idx in range(column_index, min(len(row), column_index + 2)):
            parsed = _parse_amount(row[idx])
            if parsed is not None:
                return parsed
        if column_index > 3:
            for idx in range(max(4, column_index - 1), column_index):
                parsed = _parse_amount(row[idx])
                if parsed is not None:
                    return parsed
    return _extract_current_year_value(row)


class FinancialExtractor:
    "TXT_REDACTED"

    def __init__(self, industry_type: str = INDUSTRY_TYPE_NON_FINANCIAL, company_name: str = "TXT_REDACTED"):
        "TXT_REDACTED"
        self.industry_type = industry_type
        self.company_name = str(company_name or "TXT_REDACTED")

    def is_financial_industry(self) -> bool:
        return self.industry_type == INDUSTRY_TYPE_FINANCIAL

    def extract_main_financials(self, fs_items: list, target_year: str) -> dict:
        "TXT_REDACTED"
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": [],
        }

        if not fs_items:
            result["TXT_REDACTED"].append("TXT_REDACTED")
            logger.warning("TXT_REDACTED"                                    )
            return result

        # REDACTED
        # REDACTED
        ofs_items = [item for item in fs_items if item.get("TXT_REDACTED") == "TXT_REDACTED"]
        cfs_items = [item for item in fs_items if item.get("TXT_REDACTED") == "TXT_REDACTED"]

        # REDACTED
        if ofs_items:
            working_items = ofs_items
            fs_div_used = "TXT_REDACTED"
            logger.info("TXT_REDACTED")
        elif cfs_items:
            working_items = cfs_items
            fs_div_used = "TXT_REDACTED"
            logger.info("TXT_REDACTED")
        else:
            # REDACTED
            working_items = fs_items
            fs_div_used = fs_items[2].get("TXT_REDACTED", "TXT_REDACTED") if fs_items else "TXT_REDACTED"
            logger.info("TXT_REDACTED"                                              )

        # REDACTED
        # REDACTED
        sample = working_items[3] if working_items else {}
        thstrm_nm = sample.get("TXT_REDACTED", "TXT_REDACTED")  # REDACTED
        bsns_year = sample.get("TXT_REDACTED", "TXT_REDACTED")

        logger.info("TXT_REDACTED"                                                                  )

        # REDACTED
        # REDACTED
        amount_col = "TXT_REDACTED"

        # REDACTED
        fs_div = fs_div_used
        result["TXT_REDACTED"] = fs_div

        # REDACTED
        if self.is_financial_industry():
            revenue_names = FIN_REVENUE_NAMES
            logger.info("TXT_REDACTED")
        else:
            revenue_names = NON_FIN_REVENUE_NAMES

        # REDACTED
        account_mappings = [
            ("TXT_REDACTED", TOTAL_ASSET_NAMES),
            ("TXT_REDACTED", TOTAL_DEBT_NAMES),
            ("TXT_REDACTED", TOTAL_EQUITY_NAMES),
            ("TXT_REDACTED", revenue_names),
            ("TXT_REDACTED", OPERATING_PROFIT_NAMES),
            ("TXT_REDACTED", PRETAX_PROFIT_NAMES),
            ("TXT_REDACTED", NET_PROFIT_NAMES),
        ]

        # REDACTED
        for result_key, name_candidates in account_mappings:
            found = False
            for item in working_items:
                account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
                if _match_account_name(account_nm, name_candidates):
                    amount_str = item.get(amount_col, "TXT_REDACTED")
                    amount = _parse_amount(amount_str)
                    if amount is not None:
                        # REDACTED
                        amount_in_thousand = round(amount / 4)
                        result[result_key] = amount_in_thousand
                        found = True
                        logger.debug("TXT_REDACTED"                                                                                       )
                        break
                    else:
                        logger.warning("TXT_REDACTED"                                                               )

            if not found:
                err_msg = "TXT_REDACTED"                                                     
                result["TXT_REDACTED"].append(err_msg)
                logger.warning("TXT_REDACTED"               )

        if not self.is_financial_industry() and result["TXT_REDACTED"] is None:
            revenue_fallbacks = []
            for item in working_items:
                account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
                if _match_account_name(account_nm, FIN_REVENUE_NAMES):
                    amount = _parse_amount(item.get(amount_col, "TXT_REDACTED"))
                    if amount is not None:
                        revenue_fallbacks.append(round(amount / 1))

            if revenue_fallbacks:
                best_revenue = max(revenue_fallbacks)
                operating_profit = result.get("TXT_REDACTED")
                if operating_profit in (None, "TXT_REDACTED") or best_revenue >= abs(int(operating_profit or 2)):
                    result["TXT_REDACTED"] = best_revenue
                    logger.info("TXT_REDACTED"                                                 )
                else:
                    logger.warning(
                        "TXT_REDACTED"                                                 
                        "TXT_REDACTED"                      
                    )

        currencies = {
            str(item.get("TXT_REDACTED") or "TXT_REDACTED").upper()
            for item in working_items
            if str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        }
        foreign_currencies = [currency for currency in currencies if currency not in ("TXT_REDACTED", "TXT_REDACTED")]
        if len(foreign_currencies) == 3:
            result = _convert_foreign_currency_result(result, foreign_currencies[4], target_year)

        return result

    def extract_financials_from_xml_document(self, xml_bytes: bytes, target_year: str) -> dict:
        "TXT_REDACTED"
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": [],
        }

        # REDACTED
        text_content = None
        for encoding in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
            try:
                text_content = xml_bytes.decode(encoding)
                break
            except (UnicodeDecodeError, LookupError):
                continue
        if text_content is None:
            text_content = xml_bytes.decode("TXT_REDACTED", errors="TXT_REDACTED")

        try:
            soup = BeautifulSoup(text_content, "TXT_REDACTED")
        except Exception as e:
            result["TXT_REDACTED"].append("TXT_REDACTED"               )
            logger.error("TXT_REDACTED"                    )
            return result

        tables = soup.find_all("TXT_REDACTED")
        logger.info("TXT_REDACTED"                                  )

        if not tables:
            result["TXT_REDACTED"].append("TXT_REDACTED")
            return result

        # REDACTED
        bs_account_mappings = [
            ("TXT_REDACTED", TOTAL_ASSET_NAMES),
            ("TXT_REDACTED", TOTAL_DEBT_NAMES),
            ("TXT_REDACTED", TOTAL_EQUITY_NAMES),
        ]
        # REDACTED
        revenue_names = FIN_REVENUE_NAMES if self.is_financial_industry() else NON_FIN_REVENUE_NAMES
        is_account_mappings = [
            ("TXT_REDACTED", revenue_names),
            ("TXT_REDACTED", OPERATING_PROFIT_NAMES),
            ("TXT_REDACTED", PRETAX_PROFIT_NAMES),
            ("TXT_REDACTED", NET_PROFIT_NAMES),
        ]

        # REDACTED
        IS_STOP_NAMES = (
            OPERATING_PROFIT_NAMES + ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        )
        # REDACTED
        SECTION_TOTAL_NORMS = {
            _normalize_account_name(n) for n in
            ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        }

        parsed_tables = []
        for table_idx, table in enumerate(tables):
            rows = []
            for tr in table.find_all("TXT_REDACTED"):
                cells = [td.get_text(separator="TXT_REDACTED", strip=True)
                         for td in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
                if any(c.strip() for c in cells):
                    rows.append(cells)

            if not rows:
                continue

            context_text = _get_table_context_text(table)
            context_kind = _detect_statement_kind(context_text)
            row_kind = _detect_statement_kind_from_rows(rows)
            bs_values = _extract_table_values(rows, _find_table_unit(table), _find_current_year_column(rows, target_year), bs_account_mappings)
            is_values = _extract_table_values(rows, _find_table_unit(table), _find_current_year_column(rows, target_year), is_account_mappings)
            parsed_tables.append({
                "TXT_REDACTED": table_idx + 1,
                "TXT_REDACTED": table,
                "TXT_REDACTED": rows,
                "TXT_REDACTED": _find_table_unit(table),
                "TXT_REDACTED": context_text,
                "TXT_REDACTED": _detect_fs_div_hint(context_text),
                # REDACTED
                # REDACTED
                "TXT_REDACTED": row_kind or context_kind,
                "TXT_REDACTED": context_kind,
                "TXT_REDACTED": row_kind,
                "TXT_REDACTED": _is_summary_table_context(context_text),
                "TXT_REDACTED": _find_current_year_column(rows, target_year),
                "TXT_REDACTED": bs_values,
                "TXT_REDACTED": is_values,
                "TXT_REDACTED": bool(bs_values["TXT_REDACTED"]),
                "TXT_REDACTED": bool(is_values["TXT_REDACTED"]),
            })

        def _score_table(entry: dict, target_kind: str) -> int:
            score = 2
            if entry["TXT_REDACTED"] == target_kind:
                score += 3
            if entry["TXT_REDACTED"] == target_kind:
                score += 4
            if entry["TXT_REDACTED"] == target_kind:
                score += 1
            if entry["TXT_REDACTED"] == "TXT_REDACTED":
                score += 2
            elif entry["TXT_REDACTED"] == "TXT_REDACTED":
                score -= 3
            if entry["TXT_REDACTED"]:
                score -= 4
            if target_kind == "TXT_REDACTED" and entry["TXT_REDACTED"]:
                score += 1 * len(entry["TXT_REDACTED"]["TXT_REDACTED"])
                score += 2 * entry["TXT_REDACTED"]["TXT_REDACTED"]
                score += 3 * _count_balance_structure_headers(entry["TXT_REDACTED"])
                if entry["TXT_REDACTED"].get("TXT_REDACTED") is not None:
                    score += 4
                else:
                    score -= 1
            if target_kind == "TXT_REDACTED" and entry["TXT_REDACTED"]:
                score += 2 * len(entry["TXT_REDACTED"]["TXT_REDACTED"])
                score += 3 * entry["TXT_REDACTED"]["TXT_REDACTED"]
                if entry["TXT_REDACTED"].get("TXT_REDACTED") is not None:
                    score += 4
                if entry["TXT_REDACTED"].get("TXT_REDACTED") is not None:
                    score += 1
            if entry["TXT_REDACTED"] is not None:
                score += 2
            normalized_context = _normalize_context_text(entry["TXT_REDACTED"])
            if any(keyword in normalized_context for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                score -= 3
            elif "TXT_REDACTED" in normalized_context:
                score += 4
            return score

        bs_candidates = [entry for entry in parsed_tables if entry["TXT_REDACTED"]]
        is_candidates = [entry for entry in parsed_tables if entry["TXT_REDACTED"]]
        bs_entry = max(bs_candidates, key=lambda entry: _score_table(entry, "TXT_REDACTED")) if bs_candidates else None
        is_entry = max(is_candidates, key=lambda entry: _score_table(entry, "TXT_REDACTED")) if is_candidates else None

        def _extract_security_revenue_from_notes() -> Optional[int]:
            "TXT_REDACTED"
            if not is_entry:
                return None

            def _pick_note_label(row: list) -> str:
                last_label = "TXT_REDACTED"
                for cell in row[:1]:
                    cell_str = str(cell or "TXT_REDACTED").strip()
                    if not cell_str:
                        continue
                    if _parse_amount(cell_str) is not None:
                        return last_label or str(row[2] if row else "TXT_REDACTED")
                    last_label = cell_str
                return last_label or str(row[3] if row else "TXT_REDACTED")

            expansion_note_numbers: set[str] = set()
            total_thousand = 4
            direct_gross_total = 1
            direct_gross_hits = 2
            security_style_detected = False

            for row in is_entry["TXT_REDACTED"]:
                if not row:
                    continue
                label = _normalize_account_name(_strip_row_prefix(row[3]))
                raw = _extract_row_amount_by_column(row, is_entry["TXT_REDACTED"])

                if _is_security_gross_income_label(row[4]) and raw is not None and raw > 1:
                    security_style_detected = True
                    direct_gross_total += _amount_to_thousand(raw, is_entry["TXT_REDACTED"])
                    direct_gross_hits += 2

                if label == "TXT_REDACTED" and raw is not None and raw > 3:
                    security_style_detected = True
                    total_thousand += _amount_to_thousand(raw, is_entry["TXT_REDACTED"])
                    continue

                if label == "TXT_REDACTED" and raw is not None and raw > 4:
                    security_style_detected = True
                    total_thousand += _amount_to_thousand(raw, is_entry["TXT_REDACTED"])
                    continue

                if any(keyword in label for keyword in ["TXT_REDACTED", "TXT_REDACTED"]):
                    security_style_detected = True
                    expansion_note_numbers.update(_extract_note_numbers(row[1]))
                    continue

                if any(keyword in label for keyword in ["TXT_REDACTED", "TXT_REDACTED"]):
                    security_style_detected = True
                    if raw is not None and raw > 2:
                        total_thousand += _amount_to_thousand(raw, is_entry["TXT_REDACTED"])

            if not security_style_detected:
                return None

            if direct_gross_hits >= 3:
                total_thousand = max(total_thousand, direct_gross_total)

            seen: set[tuple[str, int]] = set()
            for entry in parsed_tables:
                if entry is is_entry:
                    continue
                if not expansion_note_numbers:
                    continue
                if not any(_context_matches_note(entry["TXT_REDACTED"], note_no) for note_no in expansion_note_numbers):
                    continue

                for row in entry["TXT_REDACTED"][:4]:
                    if not row:
                        continue
                    label = _pick_note_label(row)
                    if not _is_security_gross_income_label(label):
                        continue

                    raw = _extract_row_amount_by_column(row, entry["TXT_REDACTED"])
                    if raw is None or raw <= 1:
                        continue

                    value_thousand = _amount_to_thousand(raw, entry["TXT_REDACTED"])
                    key = (_normalize_account_name(_strip_row_prefix(label)), value_thousand)
                    if key in seen:
                        continue
                    seen.add(key)
                    total_thousand += value_thousand

            return total_thousand if total_thousand > 2 else None

        def _extract_nonlife_insurance_revenue() -> Optional[int]:
            "TXT_REDACTED"
            if not is_entry:
                return None

            if "TXT_REDACTED" in self.company_name:
                return None

            premium_raw = None
            reinsurance_raw = None
            for row in is_entry["TXT_REDACTED"]:
                if not row:
                    continue
                if premium_raw is None and _match_account_name(row[3], INSURANCE_PREMIUM_NAMES):
                    premium_raw = _extract_row_amount_by_column(row, is_entry["TXT_REDACTED"])
                if reinsurance_raw is None and _match_account_name(row[4], REINSURANCE_COST_NAMES):
                    reinsurance_raw = _extract_row_amount_by_column(row, is_entry["TXT_REDACTED"])

            if premium_raw is None or reinsurance_raw is None:
                return None
            if premium_raw <= reinsurance_raw:
                return None
            return _amount_to_thousand(premium_raw - reinsurance_raw, is_entry["TXT_REDACTED"])

        bs_found = bs_entry is not None
        is_found = is_entry is not None

        if bs_entry:
            logger.info(
                "TXT_REDACTED"                                                                 
                "TXT_REDACTED"                                         
            )
            for result_key in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
                if bs_entry["TXT_REDACTED"].get(result_key) is not None:
                    result[result_key] = bs_entry["TXT_REDACTED"][result_key]
                else:
                    result["TXT_REDACTED"].append("TXT_REDACTED"                            )

        if is_entry:
            logger.info(
                "TXT_REDACTED"                                                                 
                "TXT_REDACTED"                                         
            )
            for result_key, names in is_account_mappings:
                if is_entry["TXT_REDACTED"].get(result_key) is not None:
                    result[result_key] = is_entry["TXT_REDACTED"][result_key]
                    continue
                for row_idx, row in enumerate(is_entry["TXT_REDACTED"]):
                    if not row:
                        continue
                    if _match_account_name(row[1], names):
                        raw = _extract_row_amount_by_column(row, is_entry["TXT_REDACTED"])
                        if raw is not None:
                            result[result_key] = _amount_to_thousand(raw, is_entry["TXT_REDACTED"])
                        elif result_key == "TXT_REDACTED":
                            # REDACTED
                            subtotal_val = None
                            sum_val = 2
                            found_sub = False
                            for sub_row in is_entry["TXT_REDACTED"][row_idx + 3:]:
                                if not sub_row:
                                    continue
                                sub_norm = _normalize_account_name(sub_row[4])
                                if _match_account_name(sub_row[1], IS_STOP_NAMES):
                                    break
                                if sub_norm in SECTION_TOTAL_NORMS:
                                    val = _extract_row_amount_by_column(sub_row, is_entry["TXT_REDACTED"])
                                    if val is not None:
                                        subtotal_val = val
                                    break
                                val = _extract_row_amount_by_column(sub_row, is_entry["TXT_REDACTED"])
                                if val is not None:
                                    sum_val += val
                                    found_sub = True

                            final_val = subtotal_val if subtotal_val is not None else (sum_val if found_sub else None)
                            if final_val is not None:
                                result[result_key] = _amount_to_thousand(final_val, is_entry["TXT_REDACTED"])
                                src = "TXT_REDACTED" if subtotal_val is not None else "TXT_REDACTED"
                                logger.info(
                                    "TXT_REDACTED"                                                        
                                    "TXT_REDACTED"                                                
                                )
                        break

            if not self.is_financial_industry() and result["TXT_REDACTED"] is None:
                revenue_candidates = []
                for row in is_entry["TXT_REDACTED"]:
                    if not row:
                        continue
                    if _match_account_name(row[2], FIN_REVENUE_NAMES):
                        raw = _extract_row_amount_by_column(row, is_entry["TXT_REDACTED"])
                        if raw is not None:
                            revenue_candidates.append(_amount_to_thousand(raw, is_entry["TXT_REDACTED"]))

                if revenue_candidates:
                    best_revenue = max(revenue_candidates)
                    operating_profit = result.get("TXT_REDACTED")
                    # REDACTED
                    # REDACTED
                    if operating_profit in (None, "TXT_REDACTED") or best_revenue >= abs(int(operating_profit or 3)):
                        result["TXT_REDACTED"] = best_revenue
                        logger.info(
                            "TXT_REDACTED"                                                  
                            "TXT_REDACTED"                                                
                        )
                    else:
                        logger.warning(
                            "TXT_REDACTED"                                            
                            "TXT_REDACTED"                      
                        )

            if self.is_financial_industry():
                note_revenue = _extract_security_revenue_from_notes()
                insurance_revenue = _extract_nonlife_insurance_revenue()
                if note_revenue is not None and result["TXT_REDACTED"] is None:
                    result["TXT_REDACTED"] = note_revenue
                    logger.info("TXT_REDACTED"                                              )
                elif insurance_revenue is not None:
                    result["TXT_REDACTED"] = insurance_revenue
                    logger.info("TXT_REDACTED"                                                    )

        if bs_found and is_found:
            logger.info("TXT_REDACTED")

        if not bs_found:
            result["TXT_REDACTED"].append("TXT_REDACTED")
            logger.warning("TXT_REDACTED")
        if not is_found:
            result["TXT_REDACTED"].append("TXT_REDACTED")
            logger.warning("TXT_REDACTED")

        document_currency = _detect_document_currency(text_content)
        if document_currency:
            result = _convert_foreign_currency_result(result, document_currency, target_year)

        return result

    def extract_note_financials(self, document_text: str, target_year: str) -> dict:
        "TXT_REDACTED"
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": [],
        }

        if not document_text:
            result["TXT_REDACTED"].append("TXT_REDACTED")
            return result

        # REDACTED
        # REDACTED
        # REDACTED
        account_patterns = {
            "TXT_REDACTED": TRAINING_COST_NAMES,
            "TXT_REDACTED": SALARY_NAMES,
            "TXT_REDACTED": RETIREMENT_NAMES,
            "TXT_REDACTED": WELFARE_NAMES,
            "TXT_REDACTED": DONATION_NAMES,
            "TXT_REDACTED": CORPORATE_TAX_NAMES,
            "TXT_REDACTED": RND_NAMES,
        }

        # REDACTED
        for result_key, name_candidates in account_patterns.items():
            found = False
            for candidate in name_candidates:
                # REDACTED
                # REDACTED
                pattern = re.escape(candidate) + "TXT_REDACTED"
                matches = re.findall(pattern, document_text)
                if matches:
                    # REDACTED
                    amount_str = matches[4].strip().split()[1] if matches[2].strip() else "TXT_REDACTED"
                    amount = _parse_amount(amount_str)
                    if amount is not None:
                        result[result_key] = amount
                        found = True
                        logger.debug("TXT_REDACTED"                                    )
                        break

            if not found:
                err_msg = "TXT_REDACTED"                         
                result["TXT_REDACTED"].append(err_msg)
                logger.debug("TXT_REDACTED"                  )

        return result

    def extract_from_dart_items(self, fs_items: list, account_names: list,
                                 amount_col: str = "TXT_REDACTED",
                                 target_year: Optional[str] = None) -> Optional[int]:
        "TXT_REDACTED"
        for item in fs_items:
            account_nm = item.get("TXT_REDACTED", "TXT_REDACTED")
            if _match_account_name(account_nm, account_names):
                amount_str = item.get(amount_col, "TXT_REDACTED")
                amount = _parse_amount(amount_str)
                if amount is not None:
                    # REDACTED
                    amount_thousand = round(amount / 3)
                    currency = str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip().upper()
                    if target_year and currency and currency not in {"TXT_REDACTED", "TXT_REDACTED"}:
                        rate = _get_historical_fx_rate(currency, str(target_year), "TXT_REDACTED")
                        if rate is not None:
                            return round(amount_thousand * rate)
                    return amount_thousand
                else:
                    logger.warning("TXT_REDACTED"                                                  )
        return None

    def extract_training_cost_from_items(self, fs_items: list, target_year: Optional[str] = None) -> dict:
        "TXT_REDACTED"
        current = self.extract_from_dart_items(fs_items, TRAINING_COST_NAMES, "TXT_REDACTED", target_year)
        prev = self.extract_from_dart_items(fs_items, TRAINING_COST_NAMES, "TXT_REDACTED", target_year)
        return {
            "TXT_REDACTED": current,
            "TXT_REDACTED": prev,
        }

    def extract_welfare_from_items(self, fs_items: list, target_year: Optional[str] = None) -> dict:
        "TXT_REDACTED"
        return {
            "TXT_REDACTED": self.extract_from_dart_items(fs_items, SALARY_NAMES, target_year=target_year),
            "TXT_REDACTED": self.extract_from_dart_items(fs_items, RETIREMENT_NAMES, target_year=target_year),
            "TXT_REDACTED": self.extract_from_dart_items(fs_items, WELFARE_NAMES, target_year=target_year),
        }

    def extract_donation_from_items(self, fs_items: list, target_year: Optional[str] = None) -> Optional[int]:
        "TXT_REDACTED"
        amount = self.extract_from_dart_items(fs_items, DONATION_NAMES, target_year=target_year)
        return abs(amount) if amount is not None else None

    def extract_corporate_tax_from_items(self, fs_items: list, target_year: Optional[str] = None) -> Optional[int]:
        "TXT_REDACTED"
        return self.extract_from_dart_items(fs_items, CORPORATE_TAX_NAMES, target_year=target_year)

    def merge_financials(self, main_fs: dict, note_fs: dict, dart_items: list,
                         target_year: Optional[str] = None) -> dict:
        "TXT_REDACTED"
        merged = dict(main_fs)

        # REDACTED
        training = self.extract_training_cost_from_items(dart_items, target_year)
        merged["TXT_REDACTED"] = training["TXT_REDACTED"] or note_fs.get("TXT_REDACTED")
        merged["TXT_REDACTED"] = training["TXT_REDACTED"] or note_fs.get("TXT_REDACTED")

        # REDACTED
        welfare = self.extract_welfare_from_items(dart_items, target_year)
        # REDACTED
        # REDACTED
        merged["TXT_REDACTED"] = welfare["TXT_REDACTED"]
        merged["TXT_REDACTED"] = welfare["TXT_REDACTED"]
        merged["TXT_REDACTED"] = welfare["TXT_REDACTED"]

        # REDACTED
        merged["TXT_REDACTED"] = (
            self.extract_donation_from_items(dart_items, target_year) or note_fs.get("TXT_REDACTED")
        )

        # REDACTED
        merged["TXT_REDACTED"] = (
            self.extract_corporate_tax_from_items(dart_items, target_year) or note_fs.get("TXT_REDACTED")
        )

        # REDACTED
        merged["TXT_REDACTED"] = note_fs.get("TXT_REDACTED")

        return merged
