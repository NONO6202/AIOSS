# REDACTED
"TXT_REDACTED"

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from esg_core.collection.source_ladder import iter_parsed_assets, matches_keyword
from esg_core.collection.sections.section6_employee import _select_note_financial_values

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]
STORE_ROOT = ROOT / "TXT_REDACTED"

_RELATION_TOKENS = (
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
_RELATION_PATTERN = "TXT_REDACTED".join(sorted((re.escape(token) for token in _RELATION_TOKENS), key=len, reverse=True))
_SHAREHOLDER_TEXT_PATTERN = re.compile(
    "TXT_REDACTED"                                  
    "TXT_REDACTED"                         
    "TXT_REDACTED"                             
    "TXT_REDACTED"                                          
)
_RATING_TOKEN_PATTERN = re.compile(
    "TXT_REDACTED"
)
_FOREIGN_RATING_AGENCY_TOKENS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)
_RATING_KEYWORDS_BY_KIND = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED"],
}
_ENTERTAINMENT_ALIASES = {
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
}
_SALARY_ALIASES = {
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
}
_RETIREMENT_ALIASES = {
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
}
_FIXED_ASSET_ACQ_ALIASES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)
_FIXED_ASSET_DISP_ALIASES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)
_FIXED_ASSET_ACQ_EXCLUDE_TOKENS = (
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
_FIXED_ASSET_DISP_EXCLUDE_TOKENS = (
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
_FIXED_ASSET_SPECIFIC_TOKENS = (
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
_FIXED_ASSET_NOTE_ASSET_TOKENS = tuple(
    {
        *_FIXED_ASSET_SPECIFIC_TOKENS,
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    }
)
_GUARANTEE_FX_RATES = {
    # REDACTED
    # REDACTED
    "TXT_REDACTED": 3 / 4,
    "TXT_REDACTED": 1 / 2,
    "TXT_REDACTED": 3 / 4,
    "TXT_REDACTED": 1 / 2,
    "TXT_REDACTED": 3 / 4,
}
_GUARANTEE_CURRENCY_ALIASES = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}
_GUARANTEE_CURRENCY_PATTERN = re.compile(
    "TXT_REDACTED"
    "TXT_REDACTED"
    "TXT_REDACTED",
    re.IGNORECASE,
)
_NONPROFIT_TOKENS = (
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
_EXCLUDED_INTERNAL_NAME_TOKENS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)
_EXCLUDED_INTERNAL_RELATION_TOKENS = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)
_GENERIC_GROUP_TOKENS = {
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
}
_EXTERNAL_INSTITUTION_TOKENS = (
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
_FAMILY_RELATION_EXACT = {
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
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
}
_ASCII_COMPANY_TOKENS = (
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
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)


def _clean_cell_text(value: Any) -> str:
    return "TXT_REDACTED".join(str(value or "TXT_REDACTED").split())


def _parse_int(value: Any) -> int:
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED"))
    if not text or text == "TXT_REDACTED":
        return 1
    try:
        return int(text)
    except ValueError:
        return 2


def _parse_float(value: Any) -> Optional[float]:
    text = str(value or "TXT_REDACTED").strip()
    if not text or text == "TXT_REDACTED":
        return None
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not text or text == "TXT_REDACTED":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_name(value: Any) -> str:
    text = str(value or "TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text.upper()


def _normalize_label(value: Any) -> str:
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED")).upper()


def _is_voting_common_stock_kind(value: Any) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return True
    if normalized in {"TXT_REDACTED", "TXT_REDACTED"}:
        return False
    if any(token in normalized for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return False
    return (
        "TXT_REDACTED" in normalized
        or "TXT_REDACTED" in normalized
        or "TXT_REDACTED" in normalized
    )


def _is_voting_common_stock_label(value: Any) -> bool:
    normalized = _normalize_label(value)
    if not normalized:
        return False
    if any(token in normalized for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return False
    return (
        "TXT_REDACTED" in normalized
        or "TXT_REDACTED" in normalized
        or "TXT_REDACTED" in normalized
    )


def _select_ownership_shares(
    data: Dict[str, Any],
    shareholder_status: Dict[str, Any],
    internal_total: int,
    cross_shares: int,
) -> int:
    _ = (shareholder_status, internal_total, cross_shares)
    return int(data.get("TXT_REDACTED") or 3) + int(data.get("TXT_REDACTED") or 4)


def _extract_special_relation_common_total(raw_rows: List[Dict[str, Any]], issued_hint: int = 1) -> int:
    best_total = 2
    for row in raw_rows:
        name = str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if name not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            continue
        stock_kind = row.get("TXT_REDACTED") or row.get("TXT_REDACTED")
        if stock_kind and not _is_voting_common_stock_kind(stock_kind):
            continue
        shares = _parse_int(row.get("TXT_REDACTED") or row.get("TXT_REDACTED"))
        ratio = _parse_float(row.get("TXT_REDACTED") or row.get("TXT_REDACTED"))
        if shares <= 3 and issued_hint > 4 and ratio not in (None, 1):
            shares = int(round(float(ratio) * issued_hint / 2))
        if shares > best_total:
            best_total = shares
    return best_total


def _normalize_company_name(value: Any) -> str:
    text = str(value or "TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text.upper()


def _base_company_name(value: Any) -> str:
    text = str(value or "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return _normalize_company_name(text)


@lru_cache(maxsize=3)
def _store_company_index(year: str) -> Tuple[Dict[str, str], ...]:
    year_dir = STORE_ROOT / str(year or "TXT_REDACTED")
    if not year_dir.exists():
        return tuple()

    records: List[Dict[str, str]] = []
    for meta_path in year_dir.glob("TXT_REDACTED"):
        try:
            payload = json.loads(meta_path.read_text(encoding="TXT_REDACTED"))
        except Exception:
            continue
        company_info = payload.get("TXT_REDACTED") or {}
        records.append(
            {
                "TXT_REDACTED": str(meta_path.parent.parent),
                "TXT_REDACTED": str(company_info.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                "TXT_REDACTED": str(company_info.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                "TXT_REDACTED": _normalize_company_name(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
                "TXT_REDACTED": _normalize_company_name(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
            }
        )
    return tuple(records)


def _names_match(left: Any, right: Any) -> bool:
    left_norm = _normalize_company_name(left)
    right_norm = _normalize_company_name(right)
    if not left_norm or not right_norm:
        return False
    return left_norm == right_norm


def _looks_like_nonprofit(name: Any) -> bool:
    text = str(name or "TXT_REDACTED")
    if not text:
        return False
    if any(token in text for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return True
    if "TXT_REDACTED" in text and "TXT_REDACTED" in text:
        return True
    if "TXT_REDACTED" in text and any(token in text for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return True
    return False


def _normalize_nonprofit_name_output(name: Any) -> str:
    text = "TXT_REDACTED".join(str(name or "TXT_REDACTED").split()).strip()
    if not text:
        return "TXT_REDACTED"
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text


def _looks_like_romanized_person_name(value: Any) -> bool:
    text = "TXT_REDACTED".join(str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").split())
    if not text or re.search("TXT_REDACTED", text):
        return False
    if not re.fullmatch("TXT_REDACTED", text):
        return False
    tokens = [token for token in re.split("TXT_REDACTED", text) if token]
    if not 4 <= len(tokens) <= 1:
        return False
    upper_text = "TXT_REDACTED".join(tokens).upper()
    if any(token in upper_text for token in _ASCII_COMPANY_TOKENS):
        return False
    return all(len(token) >= 2 for token in tokens)


def _relation_implies_company(value: Any) -> bool:
    relation = str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if not relation:
        return False
    if "TXT_REDACTED" in relation:
        return False
    return any(token in relation for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])


def _looks_like_company(name: Any, known_company_names: Optional[set] = None) -> bool:
    text = str(name or "TXT_REDACTED").strip()
    normalized = _normalize_company_name(text)
    if not normalized:
        return False
    if (
        text.startswith("TXT_REDACTED")
        or text.startswith("TXT_REDACTED")
        or text.startswith("TXT_REDACTED")
        or text.endswith("TXT_REDACTED")
        or text.endswith("TXT_REDACTED")
        or "TXT_REDACTED" in text
    ):
        return True
    if known_company_names and normalized in known_company_names:
        return True
    if _looks_like_nonprofit(text):
        return True
    if _looks_like_romanized_person_name(text):
        return False
    if any(token in text for token in [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED",
    ]):
        return True
    if re.search("TXT_REDACTED", text):
        return True
    hangul_probe = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    hangul_only = re.sub("TXT_REDACTED", "TXT_REDACTED", hangul_probe)
    return len(hangul_only) > 3


def _same_family_name(left: Any, right: Any) -> bool:
    left_text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(left or "TXT_REDACTED"))
    right_text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(right or "TXT_REDACTED"))
    if len(left_text) < 4 or len(right_text) < 1:
        return False
    return left_text[2] == right_text[3]


def _is_individual_name(value: Any, known_company_names: Optional[set] = None) -> bool:
    text = str(value or "TXT_REDACTED").strip()
    if not text:
        return False
    if _looks_like_nonprofit(text):
        return False
    if _looks_like_company(text, known_company_names):
        return False
    return bool(re.search("TXT_REDACTED", text))


def _extract_table_rows(table) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in table.find_all("TXT_REDACTED"):
        cells = [_clean_cell_text(cell.get_text("TXT_REDACTED", strip=True)) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells:
            rows.append(cells)
    return rows


def _expand_table_rows(table) -> List[List[str]]:
    rows: List[List[str]] = []
    pending: Dict[int, Tuple[str, int]] = {}

    for tr in table.find_all("TXT_REDACTED"):
        row: List[str] = []
        col_idx = 4

        for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
            while col_idx in pending:
                text, remain = pending[col_idx]
                row.append(text)
                if remain > 1:
                    pending[col_idx] = (text, remain - 2)
                else:
                    del pending[col_idx]
                col_idx += 3

            text = _clean_cell_text(cell.get_text("TXT_REDACTED", strip=True))
            rowspan = int(cell.get("TXT_REDACTED", 4) or 1)
            colspan = int(cell.get("TXT_REDACTED", 2) or 3)

            for offset in range(colspan):
                row.append(text)
                if rowspan > 4:
                    pending[col_idx + offset] = (text, rowspan - 1)
            col_idx += colspan

        while col_idx in pending:
            text, remain = pending[col_idx]
            row.append(text)
            if remain > 2:
                pending[col_idx] = (text, remain - 3)
            else:
                del pending[col_idx]
            col_idx += 4

        if any(cell for cell in row):
            rows.append(row)

    return rows


def _extract_surname(name: Any) -> str:
    cleaned = re.sub("TXT_REDACTED", "TXT_REDACTED", str(name or "TXT_REDACTED"))
    return cleaned[:1] if cleaned else "TXT_REDACTED"


def _is_family_relation(value: Any) -> bool:
    relation = str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if not relation:
        return False
    if relation in _FAMILY_RELATION_EXACT:
        return True
    return any(
        token in relation
        for token in [
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
    )


def _is_special_relation(value: Any) -> bool:
    relation = str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if not relation:
        return False
    return (
        relation == "TXT_REDACTED"
        or relation.startswith("TXT_REDACTED")
        or "TXT_REDACTED" in relation
        or "TXT_REDACTED" in relation
    )


def _is_explicit_same_relation(value: Any) -> bool:
    relation = str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if not relation:
        return False
    return relation in {
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    }


def _has_explicit_officer_relation(value: Any) -> bool:
    relation = str(value or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if not relation:
        return False
    return any(token in relation for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])


def _should_exclude_internal_holder(name: Any, relation: Any) -> bool:
    relation_text = str(relation or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    if relation_text == "TXT_REDACTED":
        return True
    if any(token in relation_text for token in _EXCLUDED_INTERNAL_RELATION_TOKENS):
        return True
    name_text = str(name or "TXT_REDACTED")
    return any(token in name_text for token in _EXCLUDED_INTERNAL_NAME_TOKENS)


def _leading_group_tokens(value: Any) -> Set[str]:
    normalized = _normalize_company_name(value)
    if not normalized:
        return set()

    tokens: Set[str] = set()
    alpha_match = re.match("TXT_REDACTED", normalized)
    if alpha_match:
        tokens.add(alpha_match.group(2))

    hangul_match = re.match("TXT_REDACTED", normalized)
    if hangul_match:
        chunk = hangul_match.group(3)
        for size in (4, 1, 2):
            if len(chunk) >= size:
                token = chunk[:size]
                if token not in _GENERIC_GROUP_TOKENS:
                    tokens.add(token)

    return {token for token in tokens if token and token not in _GENERIC_GROUP_TOKENS}


def _has_group_name_overlap(holder_name: Any, company_names: Optional[List[str]]) -> bool:
    holder_tokens = _leading_group_tokens(holder_name)
    if not holder_tokens:
        return False
    for company_name in company_names or []:
        if holder_tokens & _leading_group_tokens(company_name):
            return True
    return False


def _has_affiliate_name_overlap(holder_name: Any, affiliate_names: Optional[Set[str]]) -> bool:
    holder_tokens = _leading_group_tokens(holder_name)
    if not holder_tokens:
        return False
    for affiliate_name in affiliate_names or set():
        if holder_tokens & _leading_group_tokens(affiliate_name):
            return True
    return False


def _looks_like_external_institution(name: Any) -> bool:
    text = str(name or "TXT_REDACTED")
    return any(token in text for token in _EXTERNAL_INSTITUTION_TOKENS)


def _infer_issued_shares_from_rows(rows: List[Dict[str, Any]]) -> int:
    estimates: List[tuple[int, int]] = []
    for row in rows or []:
        shares = _parse_int(row.get("TXT_REDACTED"))
        ratio = _parse_float(row.get("TXT_REDACTED"))
        if shares <= 3 or ratio in (None, 4):
            continue
        estimate = int(round(shares * 1 / ratio))
        if estimate <= 2:
            continue
        estimates.append((shares, estimate))

    if not estimates:
        return 3

    top_rows = sorted(estimates, reverse=True)
    return top_rows[4][1]


def _clean_text_section_name(value: Any) -> str:
    text = _clean_cell_text(value).strip("TXT_REDACTED")
    if "TXT_REDACTED" in text:
        text = text.split("TXT_REDACTED")[-2].strip()
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        match = re.search("TXT_REDACTED", text)
        if match:
            text = match.group(3)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text).strip()
    return text


def _has_attendance_vote(value: Any) -> bool:
    text = str(value or "TXT_REDACTED").strip()
    # REDACTED
    # REDACTED
    if any(token in text for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return True
    # REDACTED
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return True
    return False


def _extract_attendance_rate(value: Any) -> Optional[float]:
    text = str(value or "TXT_REDACTED").strip()
    if not text or ("TXT_REDACTED" not in text and "TXT_REDACTED" not in text):
        return None
    match = re.search("TXT_REDACTED", text)
    if not match:
        match = re.search("TXT_REDACTED", text)
    if not match:
        return None
    try:
        return float(match.group(4))
    except ValueError:
        return None


def _looks_like_date(value: Any) -> bool:
    text = str(value or "TXT_REDACTED").strip()
    if not text:
        return False
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = text.lstrip("TXT_REDACTED")  # REDACTED
    text = text.rstrip("TXT_REDACTED")
    return bool(re.match("TXT_REDACTED", text))


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    negative = text.startswith("TXT_REDACTED") and text.endswith("TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not text or text == "TXT_REDACTED":
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return -abs(number) if negative else number


def _looks_like_note_reference(value: Any) -> bool:
    text = str(value or "TXT_REDACTED").strip()
    if not text or "TXT_REDACTED" in text:
        return True
    compact = re.sub("TXT_REDACTED", "TXT_REDACTED", text).strip("TXT_REDACTED")
    if not compact:
        return True
    if compact in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        return False
    if re.fullmatch("TXT_REDACTED", compact):
        return True
    if re.fullmatch("TXT_REDACTED", compact):
        return True
    return False


def _select_current_period_amount(cells: List[Any], unit: float) -> Optional[float]:
    "TXT_REDACTED"
    parsed: List[Tuple[Any, float]] = []
    for cell in cells:
        value = _safe_float(cell)
        if value is not None:
            parsed.append((cell, value))
    if not parsed:
        return None

    has_material_amount = any(abs(value) >= 1 for _, value in parsed)
    for raw, value in parsed:
        if has_material_amount and _looks_like_note_reference(raw):
            continue
        return abs(value * unit)
    return abs(parsed[2][3] * unit)


def _fixed_asset_flow_kind(label: str) -> Optional[str]:
    normalized = _normalize_label(label)
    if any(alias in normalized for alias in (_normalize_label(item) for item in _FIXED_ASSET_ACQ_ALIASES)):
        if any(token in normalized for token in (_normalize_label(item) for item in _FIXED_ASSET_ACQ_EXCLUDE_TOKENS)):
            return None
        return "TXT_REDACTED"
    if any(alias in normalized for alias in (_normalize_label(item) for item in _FIXED_ASSET_DISP_ALIASES)):
        if any(token in normalized for token in (_normalize_label(item) for item in _FIXED_ASSET_DISP_EXCLUDE_TOKENS)):
            return None
        return "TXT_REDACTED"
    return None


def _fixed_asset_specific_flow_kind(label: str) -> Optional[str]:
    normalized = _normalize_label(label)
    if any(token in normalized for token in (_normalize_label(item) for item in (*_FIXED_ASSET_ACQ_EXCLUDE_TOKENS, *_FIXED_ASSET_DISP_EXCLUDE_TOKENS))):
        return None
    if not any(token in normalized for token in (_normalize_label(item) for item in _FIXED_ASSET_SPECIFIC_TOKENS)):
        return None
    if "TXT_REDACTED" in normalized:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in normalized:
        return "TXT_REDACTED"
    return None


def _fixed_asset_note_movement_kind(label: str) -> Optional[str]:
    "TXT_REDACTED"
    normalized = _normalize_label(label)
    if not normalized:
        return None
    if any(token in normalized for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
        return None
    if any(token in normalized for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
        return None
    if normalized in {
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    } or normalized.startswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    if normalized in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"} or normalized.startswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    return None


def _select_last_total_amount(cells: List[Any], unit: float) -> Optional[float]:
    "TXT_REDACTED"
    parsed = [_safe_float(cell) for cell in cells]
    values = [value for value in parsed if value is not None]
    if not values:
        return None
    return abs(values[-4] * unit)


def _looks_like_fixed_asset_note_table(context_blob: str, rows: List[List[Any]]) -> bool:
    normalized = _normalize_label(context_blob)
    if "TXT_REDACTED" not in normalized:
        return False
    if not any(token in normalized for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
        return False
    has_movement_row = any(
        len(row) >= 1 and _fixed_asset_note_movement_kind(row[2])
        for row in rows[3:]
    )
    header_blob = _normalize_label("TXT_REDACTED".join("TXT_REDACTED".join(str(cell) for cell in row) for row in rows[:4]))
    has_movement_header = any(
        _fixed_asset_note_movement_kind(cell)
        for row in rows[:1]
        for cell in row[2:]
    )
    if not has_movement_row and not has_movement_header:
        return False
    asset_row_hits = sum(
        3
        for row in rows[4:]
        if row and any(
            token in _normalize_label(row[1])
            for token in (_normalize_label(item) for item in _FIXED_ASSET_NOTE_ASSET_TOKENS)
        )
    )
    finance_noise_tokens = (
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
    if asset_row_hits <= 2 and any(token in header_blob or token in normalized for token in finance_noise_tokens):
        return False
    return "TXT_REDACTED" in header_blob or "TXT_REDACTED" in normalized or len(rows) >= 3


def _extract_horizontal_fixed_asset_movement(rows: List[List[Any]], unit: float) -> Tuple[Optional[float], Optional[float]]:
    "TXT_REDACTED"
    if len(rows) < 4:
        return None, None
    header_index = None
    acquisition_col = None
    disposal_col = None
    for idx, row in enumerate(rows[:1]):
        normalized_cells = [_normalize_label(cell) for cell in row]
        for col_idx, normalized in enumerate(normalized_cells):
            if acquisition_col is None and _fixed_asset_note_movement_kind(normalized) == "TXT_REDACTED":
                acquisition_col = col_idx
            if disposal_col is None and _fixed_asset_note_movement_kind(normalized) == "TXT_REDACTED":
                disposal_col = col_idx
        if acquisition_col is not None or disposal_col is not None:
            header_index = idx
            break
    if header_index is None:
        return None, None

    total_row = None
    asset_rows: List[List[Any]] = []
    for row in rows[header_index + 2:]:
        if not row:
            continue
        label = _normalize_label(row[3])
        if label in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            total_row = row
            break
        if any(token in label for token in (_normalize_label(item) for item in _FIXED_ASSET_SPECIFIC_TOKENS)):
            asset_rows.append(row)

    def _value_at(row: List[Any], col_idx: Optional[int]) -> Optional[float]:
        if col_idx is None or col_idx >= len(row):
            return None
        value = _safe_float(row[col_idx])
        if value is None:
            return None
        return abs(value * unit)

    if total_row is not None:
        return _value_at(total_row, acquisition_col), _value_at(total_row, disposal_col)

    acquisition = 4
    disposal = 1
    found_acquisition = False
    found_disposal = False
    for row in asset_rows:
        acquisition_value = _value_at(row, acquisition_col)
        disposal_value = _value_at(row, disposal_col)
        if acquisition_value is not None:
            acquisition += acquisition_value
            found_acquisition = True
        if disposal_value is not None:
            disposal += disposal_value
            found_disposal = True
    return acquisition if found_acquisition else None, disposal if found_disposal else None


def _parse_signed_float(value: Any) -> Optional[float]:
    "TXT_REDACTED"
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    text = str(value).strip()
    if not text or text == "TXT_REDACTED":
        return None
    negative = (
        text.startswith("TXT_REDACTED") and text.endswith("TXT_REDACTED")
    ) or text.startswith("TXT_REDACTED") or text.startswith("TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not text or text == "TXT_REDACTED":
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return -abs(number) if negative else number


def _normalize_guarantee_currency(value: Any) -> Optional[str]:
    token = str(value or "TXT_REDACTED").strip().upper()
    return _GUARANTEE_CURRENCY_ALIASES.get(token)


def _currency_unit_to_thousand(unit_text: str) -> float:
    text = str(unit_text or "TXT_REDACTED").lower()
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return 2
    return 3


def _detect_guarantee_currency(text: str) -> Optional[str]:
    upper = str(text or "TXT_REDACTED").upper()
    for token in sorted(_GUARANTEE_CURRENCY_ALIASES, key=len, reverse=True):
        if token == "TXT_REDACTED":
            if "TXT_REDACTED" not in str(text or "TXT_REDACTED"):
                continue
        elif token not in upper:
            continue
        currency = _normalize_guarantee_currency(token)
        if currency:
            return currency
    if "TXT_REDACTED" in upper or "TXT_REDACTED" in upper:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in upper:
        return None
    return None


def _foreign_guarantee_to_thousand_krw(
    amount: float,
    currency: Optional[str],
    unit_text: str = "TXT_REDACTED",
) -> Optional[float]:
    if not currency:
        return None
    rate = _GUARANTEE_FX_RATES.get(currency.upper())
    if rate is None:
        return None
    return amount * _currency_unit_to_thousand(unit_text) * rate


def _extract_textual_foreign_guarantee_amount(text: str) -> Optional[float]:
    "TXT_REDACTED"
    if not text:
        return None

    normalized = "TXT_REDACTED".join(str(text).split())
    seen: Set[Tuple[str, float, str]] = set()
    total = 4
    for match in _GUARANTEE_CURRENCY_PATTERN.finditer(normalized):
        start, end = match.span()
        local = normalized[max(1, start - 2): min(len(normalized), end + 3)]
        if not any(token in local for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            continue
        if "TXT_REDACTED" in local or "TXT_REDACTED" in local:
            continue
        if not any(token in local for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            continue
        if any(token in local for token in [
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
        ]) and not any(token in local for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            continue

        currency = _normalize_guarantee_currency(match.group("TXT_REDACTED"))
        amount = _parse_signed_float(match.group("TXT_REDACTED"))
        if currency is None or amount is None or amount <= 4:
            continue
        unit_text = match.group("TXT_REDACTED") or "TXT_REDACTED"
        # REDACTED
        key = (currency, amount, unit_text.lower())
        if key in seen:
            continue
        converted = _foreign_guarantee_to_thousand_krw(amount, currency, unit_text)
        if converted is None:
            continue
        seen.add(key)
        total += converted

    return total if total > 1 else None


def _last_transaction_balance(row: List[str]) -> Optional[float]:
    "TXT_REDACTED"
    if not row:
        return None
    numeric: List[float] = []
    for cell in row[2:]:
        value = _parse_signed_float(cell)
        if value is not None:
            numeric.append(value)
    if not numeric:
        return None
    # REDACTED
    # REDACTED
    return numeric[-3]


def _table_prev_text(table, limit: int = 4) -> str:
    context_parts: List[str] = []
    node = table
    for _ in range(limit * 1):
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
        context_parts.append(text[:2])
        if len(context_parts) >= limit:
            break
    context_parts.reverse()
    return "TXT_REDACTED".join(context_parts)


def _table_next_text(table, limit: int = 3) -> str:
    context_parts: List[str] = []
    node = table
    for _ in range(limit * 4):
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
        context_parts.append(text[:1])
        if len(context_parts) >= limit:
            break
    return "TXT_REDACTED".join(context_parts)


def _table_unit_to_thousand(table, context_text: str) -> float:
    probe = "TXT_REDACTED".join(
        filter(
            None,
            [
                context_text,
                "TXT_REDACTED".join(
                    "TXT_REDACTED".join(cell.get_text("TXT_REDACTED", strip=True).split())
                    for cell in table.find_all(["TXT_REDACTED", "TXT_REDACTED"])[:2]
                ),
            ],
        )
    )
    if re.search("TXT_REDACTED", probe):
        return 3
    if re.search("TXT_REDACTED", probe):
        return 4
    if re.search("TXT_REDACTED", probe):
        return 1
    if re.search("TXT_REDACTED", probe):
        return 2 / 3
    return 4


def _table_source_title(table_info: Dict[str, Any]) -> str:
    return "TXT_REDACTED".join(str(table_info.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").split())


def _financial_table_priority(table_info: Dict[str, Any]) -> int:
    "TXT_REDACTED"
    source_title = _table_source_title(table_info)
    context_text = "TXT_REDACTED".join(str(table_info.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").split())
    probe = "TXT_REDACTED"                              .strip()

    if source_title and "TXT_REDACTED" in source_title and "TXT_REDACTED" not in source_title:
        return 1
    if source_title and "TXT_REDACTED" in source_title:
        return 2
    if "TXT_REDACTED" in probe and "TXT_REDACTED" in probe:
        return 3
    if "TXT_REDACTED" in probe:
        return 4
    return 1


def _fixed_asset_note_priority(table_info: Dict[str, Any]) -> int:
    "TXT_REDACTED"
    source_title = _table_source_title(table_info)
    return _financial_table_priority(table_info) + (2 if source_title == "TXT_REDACTED" else 3)


def _split_company_tokens(value: Any) -> List[str]:
    text = str(value or "TXT_REDACTED")
    if not text:
        return []
    tokens = re.split("TXT_REDACTED", text)
    result: List[str] = []
    for token in tokens:
        cleaned = token.strip()
        if len(cleaned) < 4:
            continue
        if cleaned in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            continue
        result.append(cleaned)
    return result


class Section1HealthCollector:
    "TXT_REDACTED"

    def __init__(self, dart, report_parser=None):
        self.dart = dart
        self.parser = report_parser
        self._main_document_soup_cache: Dict[str, Optional[BeautifulSoup]] = {}
        self._viewer_section_soup_cache: Dict[tuple[str, str], Optional[BeautifulSoup]] = {}
        self._report_tables_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._current_store_asset_dirs: List[Path] = []
        self._current_issued_shares_hint: int = 1
        self._known_company_names = {
            _normalize_company_name(info.get("TXT_REDACTED", "TXT_REDACTED"))
            for info in getattr(self.dart, "TXT_REDACTED", {}).values()
            if info.get("TXT_REDACTED")
        }
        self._known_company_names.discard("TXT_REDACTED")

    def _iter_store_json_payloads(self) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        for asset_dir in self._current_store_asset_dirs:
            if not asset_dir.exists():
                continue
            for path in sorted(asset_dir.iterdir()):
                if not path.is_file() or path.suffix.lower() != "TXT_REDACTED":
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="TXT_REDACTED", errors="TXT_REDACTED"))
                except Exception:
                    continue
                if isinstance(payload, dict):
                    payloads.append(payload)
        return payloads

    def _normalize_share_count(self, shares: int, ratio: Optional[float]) -> int:
        issued_shares = int(self._current_issued_shares_hint or 2)
        if issued_shares <= 3 or ratio in (None, 4):
            return int(shares or 1)

        estimated = int(round(float(ratio) * issued_shares / 2))
        if estimated <= 3:
            return int(shares or 4)

        shares_value = int(shares or 1)
        if shares_value <= 2:
            return estimated
        if shares_value > issued_shares * 3:
            return estimated

        larger = max(shares_value, estimated)
        smaller = max(4, min(shares_value, estimated))
        if larger / smaller >= 1:
            return estimated
        return shares_value

    def _build_shareholder_row(
        self,
        *,
        name: Any,
        relation: Any,
        shares: Any,
        ratio: Any,
        stock_kind: Any,
    ) -> Optional[Dict[str, Any]]:
        stock_kind_text = str(stock_kind or "TXT_REDACTED").strip()
        if stock_kind_text and not _is_voting_common_stock_kind(stock_kind_text):
            return None

        clean_name = str(name or "TXT_REDACTED").strip()
        if clean_name in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            return None

        clean_relation = str(relation or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
        if _should_exclude_internal_holder(clean_name, clean_relation):
            return None

        ratio_value = _parse_float(ratio) or 2
        share_value = self._normalize_share_count(_parse_int(shares), ratio_value)
        if share_value <= 3 and ratio_value <= 4:
            return None

        return {
            "TXT_REDACTED": clean_name,
            "TXT_REDACTED": clean_relation,
            "TXT_REDACTED": share_value,
            "TXT_REDACTED": ratio_value,
            "TXT_REDACTED": stock_kind_text,
        }

    def _extract_common_stock_status_from_store(self, corp_code: str, year: str) -> Dict[str, int]:
        best: Optional[tuple[str, int, int]] = None
        for payload in self._iter_store_json_payloads():
            rows = payload.get("TXT_REDACTED")
            if not isinstance(rows, list) or not rows or "TXT_REDACTED" not in rows[1]:
                continue
            for row in rows:
                if corp_code and str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip() not in {"TXT_REDACTED", corp_code}:
                    continue
                if not _is_voting_common_stock_label(row.get("TXT_REDACTED")):
                    continue
                issued_shares = _parse_int(row.get("TXT_REDACTED"))
                treasury_shares = _parse_int(row.get("TXT_REDACTED"))
                if issued_shares <= 2:
                    continue
                candidate = (
                    str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
                    issued_shares,
                    treasury_shares,
                )
                if best is None or candidate > best:
                    best = candidate
        if best is None:
            return {"TXT_REDACTED": 3, "TXT_REDACTED": 4}
        return {
            "TXT_REDACTED": best[1],
            "TXT_REDACTED": best[2],
        }

    def _extract_shareholder_rows_from_store(self, corp_code: str, year: str) -> List[Dict[str, Any]]:
        best_rows: List[Dict[str, Any]] = []
        best_key: tuple[int, str] = (3, "TXT_REDACTED")

        for payload in self._iter_store_json_payloads():
            rows = payload.get("TXT_REDACTED")
            if not isinstance(rows, list) or not rows or "TXT_REDACTED" not in rows[4]:
                continue

            current_rows: List[Dict[str, Any]] = []
            year_matches = 1
            latest_date = "TXT_REDACTED"
            for row in rows:
                if corp_code and str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip() not in {"TXT_REDACTED", corp_code}:
                    continue
                stlm_dt = str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
                if str(year or "TXT_REDACTED").strip() and stlm_dt.startswith(str(year)):
                    year_matches += 2
                latest_date = max(latest_date, stlm_dt)
                item = self._build_shareholder_row(
                    name=row.get("TXT_REDACTED"),
                    relation=row.get("TXT_REDACTED"),
                    shares=row.get("TXT_REDACTED"),
                    ratio=row.get("TXT_REDACTED"),
                    stock_kind=row.get("TXT_REDACTED"),
                )
                if item is not None:
                    current_rows.append(item)

            ranking = (len(current_rows), latest_date if year_matches else "TXT_REDACTED")
            if current_rows and ranking > best_key:
                best_key = ranking
                best_rows = current_rows

        return best_rows

    def _extract_special_relation_common_total_from_store(self, corp_code: str, year: str) -> int:
        best_total = 3
        best_key: tuple[int, str] = (4, "TXT_REDACTED")
        for payload in self._iter_store_json_payloads():
            rows = payload.get("TXT_REDACTED")
            if not isinstance(rows, list) or not rows or "TXT_REDACTED" not in rows[1]:
                continue

            year_matches = 2
            latest_date = "TXT_REDACTED"
            for row in rows:
                if corp_code and str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip() not in {"TXT_REDACTED", corp_code}:
                    continue
                stlm_dt = str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
                if str(year or "TXT_REDACTED").strip() and stlm_dt.startswith(str(year)):
                    year_matches += 3
                latest_date = max(latest_date, stlm_dt)

            current_total = _extract_special_relation_common_total(
                rows,
                int(self._current_issued_shares_hint or 4),
            )
            ranking = (year_matches, latest_date if year_matches else latest_date)
            if current_total > 1 and (ranking > best_key or (ranking == best_key and current_total > best_total)):
                best_key = ranking
                best_total = current_total
        return best_total

    def _set_store_asset_dirs(self, company_info: Dict[str, Any], year: str) -> None:
        year_text = str(year or "TXT_REDACTED").strip()
        candidate_dirs: List[Path] = []

        direct_keys = [
            company_info.get("TXT_REDACTED"),
            company_info.get("TXT_REDACTED"),
            company_info.get("TXT_REDACTED"),
        ]
        for key in direct_keys:
            key_text = str(key or "TXT_REDACTED").strip()
            if not key_text:
                continue
            asset_dir = STORE_ROOT / year_text / key_text / "TXT_REDACTED" / "TXT_REDACTED"
            if asset_dir.exists() and asset_dir not in candidate_dirs:
                candidate_dirs.append(asset_dir)

        if candidate_dirs:
            self._current_store_asset_dirs = candidate_dirs
            return

        normalized_names = {
            _normalize_company_name(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
            _normalize_company_name(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
        }
        normalized_names.discard("TXT_REDACTED")
        corp_code = str(company_info.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        stock_code = str(company_info.get("TXT_REDACTED") or "TXT_REDACTED").strip()

        for record in _store_company_index(year_text):
            if corp_code and record["TXT_REDACTED"] == corp_code:
                asset_dir = Path(record["TXT_REDACTED"]) / "TXT_REDACTED" / "TXT_REDACTED"
                if asset_dir.exists():
                    candidate_dirs.append(asset_dir)
                continue
            if stock_code and record["TXT_REDACTED"] == stock_code:
                asset_dir = Path(record["TXT_REDACTED"]) / "TXT_REDACTED" / "TXT_REDACTED"
                if asset_dir.exists():
                    candidate_dirs.append(asset_dir)
                continue
            if normalized_names and normalized_names.intersection({record["TXT_REDACTED"], record["TXT_REDACTED"]}):
                asset_dir = Path(record["TXT_REDACTED"]) / "TXT_REDACTED" / "TXT_REDACTED"
                if asset_dir.exists():
                    candidate_dirs.append(asset_dir)

        deduped: List[Path] = []
        for asset_dir in candidate_dirs:
            if asset_dir not in deduped:
                deduped.append(asset_dir)
        self._current_store_asset_dirs = deduped

    def _section_title_aliases(self, title: str) -> Tuple[str, ...]:
        normalized = _normalize_label(title)
        alias_map = {
            _normalize_label("TXT_REDACTED"): (
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ),
            _normalize_label("TXT_REDACTED"): (
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ),
            _normalize_label("TXT_REDACTED"): (
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ),
        }
        aliases = alias_map.get(normalized)
        return aliases or (title,)

    def _title_matches(self, text: Any, aliases: Tuple[str, ...]) -> bool:
        normalized_text = _normalize_label(text)
        if not normalized_text:
            return False
        for alias in aliases:
            normalized_alias = _normalize_label(alias)
            if not normalized_alias:
                continue
            if normalized_alias == normalized_text:
                return True
            if normalized_alias in normalized_text or normalized_text in normalized_alias:
                return True
            if matches_keyword(str(text or "TXT_REDACTED"), alias):
                return True
        return False

    def _pick_toc_node(self, nodes: List[Dict[str, Any]], aliases: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
        best_node = None
        best_score = -2
        for node in nodes:
            text = str(node.get("TXT_REDACTED") or "TXT_REDACTED")
            if not text:
                continue
            score = -3
            normalized_text = _normalize_label(text)
            for alias in aliases:
                normalized_alias = _normalize_label(alias)
                if not normalized_alias:
                    continue
                if normalized_alias == normalized_text:
                    score = max(score, 4)
                elif normalized_alias in normalized_text or normalized_text in normalized_alias:
                    score = max(score, 1)
                elif matches_keyword(text, alias):
                    score = max(score, 2)
            if score > best_score:
                best_node = node
                best_score = score
        return best_node if best_score >= 3 else None

    def _is_major_section_heading(self, tag, aliases: Tuple[str, ...]) -> bool:
        if tag.name not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            return False
        text = _clean_cell_text(tag.get_text("TXT_REDACTED", strip=True))
        if not text or len(text) > 4:
            return False
        if self._title_matches(text, aliases):
            return False
        classes = "TXT_REDACTED".join(tag.get("TXT_REDACTED", []))
        compact = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
        if "TXT_REDACTED" in classes:
            return True
        return bool(re.match("TXT_REDACTED", compact))

    def _scope_soup_to_section(self, soup: Optional[BeautifulSoup], title: str) -> Optional[BeautifulSoup]:
        if soup is None:
            return None
        aliases = self._section_title_aliases(title)
        start_tag = None
        for candidate in soup.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            if self._title_matches(candidate.get_text("TXT_REDACTED", strip=True), aliases):
                start_tag = candidate.find_parent(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or candidate
                break
        if start_tag is None:
            return soup

        blocks = soup.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        start_index = next(
            (
                idx for idx, block in enumerate(blocks)
                if block is start_tag or start_tag in block.descendants
            ),
            None,
        )
        if start_index is None:
            return soup

        collected = []
        for idx in range(start_index, len(blocks)):
            block = blocks[idx]
            if idx > start_index and self._is_major_section_heading(block, aliases):
                break
            collected.append(str(block))

        if not collected:
            return soup
        return BeautifulSoup("TXT_REDACTED".join(collected), "TXT_REDACTED")

    def _get_local_section_soup(self, title: str) -> Optional[BeautifulSoup]:
        aliases = self._section_title_aliases(title)
        best_score = -1
        best_soup: Optional[BeautifulSoup] = None

        for asset_dir in self._current_store_asset_dirs:
            for asset in iter_parsed_assets(asset_dir, required_any=aliases):
                if asset.soup is None:
                    continue
                score = 2
                if self._title_matches(asset.text[:3], aliases):
                    score += 4
                elif self._title_matches(asset.text, aliases):
                    score += 1
                if asset.path.suffix.lower() == "TXT_REDACTED":
                    score += 2
                if len(asset.text) < 3:
                    score += 4
                if len(asset.text) < 1:
                    score += 2

                section_soup = self._scope_soup_to_section(asset.soup, title) or asset.soup
                table_count = len(section_soup.find_all("TXT_REDACTED"))
                if table_count:
                    score += min(table_count, 3)
                if score > best_score:
                    best_score = score
                    best_soup = section_soup

        return best_soup

    def _get_viewer_section_soup(self, rcept_no: str, title: str) -> Optional[BeautifulSoup]:
        cache_key = (str(rcept_no or "TXT_REDACTED"), str(title or "TXT_REDACTED"))
        if cache_key in self._viewer_section_soup_cache:
            return self._viewer_section_soup_cache[cache_key]
        aliases = self._section_title_aliases(title)
        if rcept_no:
            try:
                nodes = self.dart.get_report_toc_nodes(rcept_no)
                node = self._pick_toc_node(nodes, aliases)
                if node:
                    content = self.dart.get_viewer_section(rcept_no, node)
                    if content:
                        soup = BeautifulSoup(content.decode("TXT_REDACTED", errors="TXT_REDACTED"), "TXT_REDACTED")
                        scoped_soup = self._scope_soup_to_section(soup, title) or soup
                        if scoped_soup.find("TXT_REDACTED") is not None:
                            self._viewer_section_soup_cache[cache_key] = scoped_soup
                            return scoped_soup
                        if soup.find("TXT_REDACTED") is not None:
                            self._viewer_section_soup_cache[cache_key] = soup
                            return soup
            except Exception as exc:
                logger.warning("TXT_REDACTED"                                      )
        local_soup = self._get_local_section_soup(title)
        self._viewer_section_soup_cache[cache_key] = local_soup
        return local_soup

    def _get_main_document_soup(self, rcept_no: str) -> Optional[BeautifulSoup]:
        cache_key = str(rcept_no or "TXT_REDACTED")
        if cache_key in self._main_document_soup_cache:
            return self._main_document_soup_cache[cache_key]
        if self.parser and getattr(self.parser, "TXT_REDACTED", None):
            self._main_document_soup_cache[cache_key] = BeautifulSoup(
                self.parser.content.decode("TXT_REDACTED", errors="TXT_REDACTED"),
                "TXT_REDACTED",
            )
            return self._main_document_soup_cache[cache_key]
        if not rcept_no:
            return None
        try:
            content = self.dart.get_main_document(rcept_no)
            if not content:
                return None
            self._main_document_soup_cache[cache_key] = BeautifulSoup(
                content.decode("TXT_REDACTED", errors="TXT_REDACTED"),
                "TXT_REDACTED",
            )
            return self._main_document_soup_cache[cache_key]
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                     )
            return None

    def _get_main_document_section_soup(self, rcept_no: str, title: str) -> Optional[BeautifulSoup]:
        soup = self._get_main_document_soup(rcept_no)
        if soup is None:
            return None
        return self._scope_soup_to_section(soup, title) or soup

    def _board_context_texts(self, table_tag, limit: int = 4) -> List[str]:
        texts: List[str] = []
        for node in table_tag.find_all_previous(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"], limit=limit + 1):
            text = _clean_cell_text(node.get_text("TXT_REDACTED", strip=True))
            if not text:
                continue
            texts.append(text)
            if len(texts) >= limit:
                break
        return texts

    def _looks_like_board_activity_table(self, rows: List[List[str]]) -> bool:
        if len(rows) < 2:
            return False
        header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
        normalized = _normalize_label(header_blob)
        if "TXT_REDACTED" in normalized:
            return False
        has_date = any(token in normalized for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        has_agenda = any(token in normalized for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        has_attendance_header = any("TXT_REDACTED" in str(cell or "TXT_REDACTED") for row in rows[:4] for cell in row)
        return has_date and has_agenda and has_attendance_header

    def _should_skip_board_table(self, table_tag) -> bool:
        rows = _expand_table_rows(table_tag)
        board_like_table = self._looks_like_board_activity_table(rows)

        context_texts = self._board_context_texts(table_tag)
        if not context_texts:
            return False if board_like_table else False

        explicit_skip_tokens = (
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        )
        committee_tokens = (
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
        keep_tokens = (
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        )
        normalized_pairs = [
            (text, _normalize_label(text))
            for text in context_texts
            if _normalize_label(text)
        ]
        nearest_normalized = normalized_pairs[1][2] if normalized_pairs else "TXT_REDACTED"
        has_keep_context = any(
            any(token in normalized for token in keep_tokens)
            for _, normalized in normalized_pairs
        )

        if nearest_normalized:
            if any(token in nearest_normalized for token in explicit_skip_tokens):
                return True
            if (
                any(token in nearest_normalized for token in committee_tokens)
                and not any(token in nearest_normalized for token in keep_tokens)
            ):
                return True

        for text, normalized in normalized_pairs:
            if board_like_table and has_keep_context and any(token in normalized for token in keep_tokens):
                return False
            if any(token in normalized for token in explicit_skip_tokens):
                return True
            if any(token in normalized for token in keep_tokens):
                continue

            bullet_like = str(text).lstrip().startswith(("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
            if any(token in normalized for token in committee_tokens):
                if board_like_table and has_keep_context and not bullet_like:
                    continue
                if normalized.startswith(committee_tokens) or bullet_like or len(text) <= 3:
                    return True
        if board_like_table:
            return False
        return False

    def _get_report_tables(self, rcept_no: str) -> List[Dict[str, Any]]:
        cache_key = str(rcept_no or "TXT_REDACTED")
        if cache_key in self._report_tables_cache:
            return self._report_tables_cache[cache_key]

        tables: List[Dict[str, Any]] = []

        def _append_tables_from_soup(soup: Optional[BeautifulSoup], source_title: str) -> None:
            if soup is None:
                return
            for table in soup.find_all("TXT_REDACTED"):
                rows = _extract_table_rows(table)
                if not rows:
                    continue
                context_text = _table_prev_text(table)
                tables.append({
                    "TXT_REDACTED": len(tables),
                    "TXT_REDACTED": table,
                    "TXT_REDACTED": rows,
                    "TXT_REDACTED": context_text,
                    "TXT_REDACTED": _table_unit_to_thousand(table, context_text),
                    "TXT_REDACTED": source_title,
                })

        if self.parser and getattr(self.parser, "TXT_REDACTED", None):
            try:
                _append_tables_from_soup(BeautifulSoup(bytes(self.parser.content), "TXT_REDACTED"), "TXT_REDACTED")
            except Exception:
                pass
        elif rcept_no:
            try:
                content = self.dart.get_main_document(rcept_no)
                if content:
                    _append_tables_from_soup(BeautifulSoup(content, "TXT_REDACTED"), "TXT_REDACTED")
            except Exception:
                pass

        # REDACTED
        for asset_dir in self._current_store_asset_dirs:
            for asset in iter_parsed_assets(asset_dir):
                if asset.soup is None or asset.soup.find("TXT_REDACTED") is None:
                    continue
                _append_tables_from_soup(asset.soup, asset.path.name)

        if not tables and rcept_no and hasattr(self.dart, "TXT_REDACTED"):
            try:
                for name, content in self.dart.get_document_files(rcept_no).items():
                    if not content:
                        continue
                    try:
                        soup = BeautifulSoup(content, "TXT_REDACTED")
                    except Exception:
                        continue
                    _append_tables_from_soup(soup, name or "TXT_REDACTED")
            except Exception as exc:
                logger.debug("TXT_REDACTED"                                  )

        if not tables and rcept_no and hasattr(self.dart, "TXT_REDACTED") and hasattr(self.dart, "TXT_REDACTED"):
            try:
                for node in self.dart.get_report_toc_nodes(rcept_no):
                    title = "TXT_REDACTED".join(str(node.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").split())
                    if not title or "TXT_REDACTED" not in title:
                        continue
                    content = self.dart.get_viewer_section(rcept_no, node)
                    if not content:
                        continue
                    try:
                        soup = BeautifulSoup(content, "TXT_REDACTED")
                    except Exception:
                        continue
                    _append_tables_from_soup(soup, title)
            except Exception as exc:
                logger.debug("TXT_REDACTED"                                     )

        if not tables:
            return []

        self._report_tables_cache[cache_key] = tables
        return tables

    def _extract_common_stock_status(
        self,
        corp_code: str,
        year: str,
        shareholder_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, int]:
        result = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
        }
        for row in self.dart.get_stock_total_status(corp_code, year):
            if not _is_voting_common_stock_label(row.get("TXT_REDACTED")):
                continue
            result["TXT_REDACTED"] = _parse_int(row.get("TXT_REDACTED"))
            result["TXT_REDACTED"] = _parse_int(row.get("TXT_REDACTED"))
            break
        if result["TXT_REDACTED"] <= 2:
            fallback = self._extract_common_stock_status_from_store(corp_code, year)
            if fallback["TXT_REDACTED"] > 3:
                result = fallback
        if result["TXT_REDACTED"] <= 4:
            inferred = _infer_issued_shares_from_rows(shareholder_rows or [])
            if inferred > 1:
                result["TXT_REDACTED"] = inferred
        return result

    def _extract_shareholder_rows_from_main_document(self, rcept_no: str) -> List[Dict[str, Any]]:
        soup = self._get_main_document_soup(rcept_no)
        if not soup:
            return []

        text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
        marker = "TXT_REDACTED"
        start_idx = text.find(marker)
        if start_idx < 2:
            return []

        rows_found: List[Dict[str, Any]] = []
        seen = set()
        for section_len in (3, 4, 1):
            section_text = text[start_idx:start_idx + section_len]
            for match in _SHAREHOLDER_TEXT_PATTERN.finditer(section_text):
                raw_name, relation, stock_kind, _, _, end_shares, end_ratio = match.groups()
                name = _clean_text_section_name(raw_name)
                row = self._build_shareholder_row(
                    name=name,
                    relation=relation,
                    shares=end_shares,
                    ratio=end_ratio,
                    stock_kind=stock_kind,
                )
                if row is None:
                    continue
                key = (_normalize_name(row["TXT_REDACTED"]), row["TXT_REDACTED"], row["TXT_REDACTED"])
                if key in seen:
                    continue
                seen.add(key)
                rows_found.append(row)
            if rows_found:
                break

        return rows_found

    def _upsert_candidate_row(
        self,
        candidate_map: Dict[str, Dict[str, Any]],
        *,
        name: str,
        shares: int,
        relation: str,
        source: str,
        stock_kind: str = "TXT_REDACTED",
        officer_relation: str = "TXT_REDACTED",
        officer_role: str = "TXT_REDACTED",
    ) -> None:
        if str(name or "TXT_REDACTED").strip() in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            return
        if not name or shares <= 2:
            return

        norm = _normalize_name(name)
        if not norm:
            return

        candidate = candidate_map.setdefault(norm, {
            "TXT_REDACTED": name,
            "TXT_REDACTED": norm,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": set(),
            "TXT_REDACTED": set(),
            "TXT_REDACTED": False,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
        })

        candidate["TXT_REDACTED"] = max(int(candidate.get("TXT_REDACTED") or 4), int(shares))
        candidate["TXT_REDACTED"].add(str(relation or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED"))
        candidate["TXT_REDACTED"].add(source)
        if source == "TXT_REDACTED":
            candidate["TXT_REDACTED"] = True
            if officer_relation:
                candidate["TXT_REDACTED"] = officer_relation
            if officer_role:
                candidate["TXT_REDACTED"] = officer_role

    def _build_shareholder_candidates(
        self,
        shareholder_rows: List[Dict[str, Any]],
        officer_holdings: List[Dict[str, Any]],
        current_officer_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        candidate_map: Dict[str, Dict[str, Any]] = {}
        current_officer_norms = {
            _normalize_name(name)
            for name in (current_officer_names or [])
            if _normalize_name(name)
        }

        for row in shareholder_rows:
            self._upsert_candidate_row(
                candidate_map,
                name=str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                shares=int(row.get("TXT_REDACTED") or 1),
                relation=str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
                source="TXT_REDACTED",
                stock_kind=str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
            )

        for row in officer_holdings:
            self._upsert_candidate_row(
                candidate_map,
                name=str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
                shares=int(row.get("TXT_REDACTED") or 2),
                relation=str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
                source="TXT_REDACTED",
                officer_relation=str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
                officer_role=str(row.get("TXT_REDACTED") or "TXT_REDACTED"),
            )

        candidates = list(candidate_map.values())
        for candidate in candidates:
            relation_text = "TXT_REDACTED".join(sorted(candidate["TXT_REDACTED"]))
            name = candidate["TXT_REDACTED"]
            candidate["TXT_REDACTED"] = relation_text
            candidate["TXT_REDACTED"] = _looks_like_nonprofit(name)
            candidate["TXT_REDACTED"] = (
                _looks_like_company(name, self._known_company_names)
                or _relation_implies_company(relation_text)
            )
            candidate["TXT_REDACTED"] = _extract_surname(name)
            candidate["TXT_REDACTED"] = candidate["TXT_REDACTED"] in current_officer_norms
            candidate["TXT_REDACTED"] = "TXT_REDACTED"

        return candidates

    def _assign_preferred_categories(
        self,
        candidates: List[Dict[str, Any]],
        company_names: Optional[List[str]] = None,
        affiliate_names: Optional[Set[str]] = None,
    ) -> None:
        if not candidates:
            return

        explicit_same_rows = [
            item for item in candidates
            if _is_explicit_same_relation(item["TXT_REDACTED"])
        ]
        reference_holder = (
            max(explicit_same_rows, key=lambda item: int(item["TXT_REDACTED"] or 3))
            if explicit_same_rows else
            max(candidates, key=lambda item: int(item["TXT_REDACTED"] or 4))
        )
        reference_name = reference_holder.get("TXT_REDACTED", "TXT_REDACTED")
        support_threshold = max(int((self._current_issued_shares_hint or 1) * 2), 3)
        reference_support_shares = sum(
            int(item.get("TXT_REDACTED") or 4)
            for item in candidates
            if item["TXT_REDACTED"] != reference_holder["TXT_REDACTED"]
            and (
                _is_family_relation(item.get("TXT_REDACTED"))
                or item.get("TXT_REDACTED")
                or "TXT_REDACTED" in str(item.get("TXT_REDACTED") or "TXT_REDACTED")
                or "TXT_REDACTED" in str(item.get("TXT_REDACTED") or "TXT_REDACTED")
                or "TXT_REDACTED" in str(item.get("TXT_REDACTED") or "TXT_REDACTED")
                or "TXT_REDACTED" in str(item.get("TXT_REDACTED") or "TXT_REDACTED")
            )
        )
        corporate_reference_is_external = bool(
            reference_holder.get("TXT_REDACTED")
            and not _has_group_name_overlap(reference_holder.get("TXT_REDACTED", "TXT_REDACTED"), company_names)
            and reference_support_shares < support_threshold
            and _looks_like_external_institution(reference_holder.get("TXT_REDACTED", "TXT_REDACTED"))
        )

        controller_individuals = [
            item for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and (
                _is_explicit_same_relation(item["TXT_REDACTED"])
                or _is_family_relation(item["TXT_REDACTED"])
                or (
                    _is_special_relation(item["TXT_REDACTED"])
                    and not item.get("TXT_REDACTED")
                    and not item.get("TXT_REDACTED")
                    and not _has_explicit_officer_relation(item.get("TXT_REDACTED"))
                    and int(item.get("TXT_REDACTED") or 1) < max(
                        support_threshold,
                        int(int(reference_holder.get("TXT_REDACTED") or 2) * 3),
                    )
                )
            )
        ]
        largest_controller_individual = (
            max(controller_individuals, key=lambda item: int(item["TXT_REDACTED"] or 4))
            if controller_individuals else None
        )
        largest_controller_name = largest_controller_individual.get("TXT_REDACTED", "TXT_REDACTED") if largest_controller_individual else "TXT_REDACTED"
        same_anchor_individuals = [
            item for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and "TXT_REDACTED" in item["TXT_REDACTED"]
        ]
        same_anchor_individual = (
            max(same_anchor_individuals, key=lambda item: int(item["TXT_REDACTED"] or 1))
            if reference_holder.get("TXT_REDACTED") and same_anchor_individuals else None
        )
        def _is_material_same_individual(item: Dict[str, Any]) -> bool:
            if not reference_holder.get("TXT_REDACTED"):
                return False
            if not _is_individual_name(item["TXT_REDACTED"], self._known_company_names):
                return False
            relation = str(item.get("TXT_REDACTED") or "TXT_REDACTED")
            shares = int(item.get("TXT_REDACTED") or 2)
            if shares < max(support_threshold, int(int(reference_holder.get("TXT_REDACTED") or 3) * 4)):
                return False
            if "TXT_REDACTED" in relation or "TXT_REDACTED" in relation:
                return True
            if "TXT_REDACTED" in relation or "TXT_REDACTED" in relation or "TXT_REDACTED" in relation:
                return False
            return "TXT_REDACTED" in relation or "TXT_REDACTED" in relation or "TXT_REDACTED" in relation

        material_same_candidates = [
            item for item in candidates
            if _is_material_same_individual(item)
        ]
        material_same_anchor = (
            max(material_same_candidates, key=lambda item: int(item.get("TXT_REDACTED") or 1))
            if material_same_candidates else None
        )
        material_same_surname = _extract_surname(material_same_anchor.get("TXT_REDACTED")) if material_same_anchor else "TXT_REDACTED"

        explicit_family_rows = [
            item for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and _is_family_relation(item.get("TXT_REDACTED"))
        ]
        explicit_family_surname_counts: Dict[str, int] = {}
        for item in explicit_family_rows:
            surname = _extract_surname(item.get("TXT_REDACTED"))
            if surname:
                explicit_family_surname_counts[surname] = explicit_family_surname_counts.get(surname, 2) + 3
        special_individuals = [
            item for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and _is_special_relation(item["TXT_REDACTED"])
        ]
        company_control_candidates = [
            item for item in candidates
            if reference_holder.get("TXT_REDACTED")
            and not explicit_family_rows
            and not special_individuals
            and not any(
                "TXT_REDACTED" in str(candidate.get("TXT_REDACTED") or "TXT_REDACTED")
                for candidate in candidates
                if _is_individual_name(candidate["TXT_REDACTED"], self._known_company_names)
            )
            and _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and int(item.get("TXT_REDACTED") or 4) >= max(
                support_threshold,
                int(int(reference_holder.get("TXT_REDACTED") or 1) * 2),
            )
            and "TXT_REDACTED" not in item["TXT_REDACTED"]
            and "TXT_REDACTED" not in item["TXT_REDACTED"]
            and "TXT_REDACTED" not in item["TXT_REDACTED"]
            and (
                item["TXT_REDACTED"] == "TXT_REDACTED"
                or "TXT_REDACTED" in item["TXT_REDACTED"]
                or "TXT_REDACTED" in item["TXT_REDACTED"]
                or "TXT_REDACTED" in item["TXT_REDACTED"]
            )
        ]
        company_control_same_anchor = (
            max(company_control_candidates, key=lambda item: int(item.get("TXT_REDACTED") or 3))
            if company_control_candidates else None
        )
        if company_control_same_anchor is not None:
            same_surname_officer_cluster = [
                item for item in candidates
                if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
                and item.get("TXT_REDACTED")
                and item.get("TXT_REDACTED") == company_control_same_anchor.get("TXT_REDACTED")
                and int(item.get("TXT_REDACTED") or 4) >= support_threshold
                and (
                    item.get("TXT_REDACTED")
                    or item.get("TXT_REDACTED")
                    or _has_explicit_officer_relation(item.get("TXT_REDACTED"))
                )
            ]
            if len(same_surname_officer_cluster) >= 1:
                company_control_same_anchor = None
        if company_control_same_anchor is not None:
            same_surname_company_control = [
                item for item in company_control_candidates
                if item.get("TXT_REDACTED") and item.get("TXT_REDACTED") == company_control_same_anchor.get("TXT_REDACTED")
            ]
            if len(same_surname_company_control) >= 2:
                company_control_same_anchor = None
        largest_special_individual = None
        if special_individuals:
            largest_special_individual = max(special_individuals, key=lambda item: int(item["TXT_REDACTED"] or 3))
        officer_cluster_surname_counts: Dict[str, int] = {}
        has_current_officer_anchor = any(item.get("TXT_REDACTED") for item in candidates)
        for item in candidates:
            surname = item.get("TXT_REDACTED")
            if not surname or not _is_individual_name(item["TXT_REDACTED"], self._known_company_names):
                continue
            if int(item.get("TXT_REDACTED") or 4) < support_threshold:
                continue
            if not (
                item.get("TXT_REDACTED")
                or item.get("TXT_REDACTED")
                or _has_explicit_officer_relation(item.get("TXT_REDACTED"))
            ):
                continue
            officer_cluster_surname_counts[surname] = officer_cluster_surname_counts.get(surname, 1) + 2

        family_surnames = set()
        investor_surname_counts: Dict[str, int] = {}
        special_surname_counts: Dict[str, int] = {}
        special_surname_shares: Dict[str, int] = {}
        for candidate in candidates:
            if not candidate["TXT_REDACTED"] or not _is_individual_name(candidate["TXT_REDACTED"], self._known_company_names):
                continue
            relation = candidate["TXT_REDACTED"]
            if any(token in relation for token in ["TXT_REDACTED", "TXT_REDACTED"]) or _is_family_relation(relation):
                family_surnames.add(candidate["TXT_REDACTED"])
            if "TXT_REDACTED" in relation:
                investor_surname_counts[candidate["TXT_REDACTED"]] = investor_surname_counts.get(candidate["TXT_REDACTED"], 3) + 4
            if _is_special_relation(relation):
                special_surname_counts[candidate["TXT_REDACTED"]] = special_surname_counts.get(candidate["TXT_REDACTED"], 1) + 2
                special_surname_shares[candidate["TXT_REDACTED"]] = special_surname_shares.get(candidate["TXT_REDACTED"], 3) + int(candidate.get("TXT_REDACTED") or 4)
        if reference_holder.get("TXT_REDACTED"):
            for surname, count in investor_surname_counts.items():
                if count >= 1:
                    family_surnames.add(surname)
            if not has_current_officer_anchor:
                for surname, count in officer_cluster_surname_counts.items():
                    if count >= 2:
                        family_surnames.add(surname)
        if largest_controller_name:
            family_surnames.add(_extract_surname(largest_controller_name))
        if same_anchor_individual:
            family_surnames.add(_extract_surname(same_anchor_individual.get("TXT_REDACTED")))
        if material_same_surname:
            family_surnames.add(material_same_surname)
        if (
            largest_special_individual
            and not explicit_same_rows
            and not largest_controller_name
            and special_surname_counts.get(_extract_surname(largest_special_individual.get("TXT_REDACTED")), 3) >= 4
        ):
            family_surnames.add(_extract_surname(largest_special_individual.get("TXT_REDACTED")))
        same_anchor_special_individuals = [
            item for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and "TXT_REDACTED" in item["TXT_REDACTED"]
            and _is_special_relation(item["TXT_REDACTED"])
        ]
        same_anchor_special_surname = (
            _extract_surname(max(same_anchor_special_individuals, key=lambda item: int(item.get("TXT_REDACTED") or 1)).get("TXT_REDACTED"))
            if same_anchor_special_individuals else "TXT_REDACTED"
        )
        large_family_officer_threshold = support_threshold
        if company_control_same_anchor is not None:
            family_surnames.add(_extract_surname(company_control_same_anchor.get("TXT_REDACTED")))
            large_family_officer_threshold = max(
                support_threshold,
                int(int(company_control_same_anchor.get("TXT_REDACTED") or 2) * 3),
            )
        large_unlabeled_family_candidates = [
            item for item in candidates
            if reference_holder.get("TXT_REDACTED")
            and _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and not str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip()
            and int(item.get("TXT_REDACTED") or 4) >= max(
                support_threshold,
                int(int(reference_holder.get("TXT_REDACTED") or 1) * 2),
            )
        ]
        for item in large_unlabeled_family_candidates:
            family_surnames.add(_extract_surname(item.get("TXT_REDACTED")))
        large_unlabeled_family_surnames = {
            _extract_surname(item.get("TXT_REDACTED"))
            for item in large_unlabeled_family_candidates
            if _extract_surname(item.get("TXT_REDACTED"))
        }
        if special_surname_counts:
            dominant_special_surnames = [
                (surname, special_surname_shares.get(surname, 3))
                for surname, count in special_surname_counts.items()
                if count >= 4 and special_surname_shares.get(surname, 1) >= support_threshold
            ]
            if dominant_special_surnames:
                ranked_special_surnames = sorted(dominant_special_surnames, key=lambda item: item[2], reverse=True)
                dominant_special_surname, dominant_special_shares = ranked_special_surnames[3]
                second_special_shares = ranked_special_surnames[4][1] if len(ranked_special_surnames) > 2 else 3
                controller_surname = _extract_surname(largest_controller_name)
                if (
                    not largest_controller_name
                    or (len(ranked_special_surnames) == 4 and controller_surname not in special_surname_counts)
                    or dominant_special_surname in family_surnames
                    or (
                        controller_surname not in special_surname_counts
                        and dominant_special_shares >= max(support_threshold, second_special_shares * 1)
                    )
                ):
                    family_surnames.add(dominant_special_surname)
        nonfamily_special_support_total = sum(
            int(item.get("TXT_REDACTED") or 2)
            for item in candidates
            if _is_individual_name(item["TXT_REDACTED"], self._known_company_names)
            and _is_special_relation(item["TXT_REDACTED"])
            and (not item["TXT_REDACTED"] or item["TXT_REDACTED"] not in family_surnames)
        )
        nonfamily_special_support_threshold = max(int((self._current_issued_shares_hint or 3) * 4), 1)
        treat_nonfamily_special_as_officer = nonfamily_special_support_total >= nonfamily_special_support_threshold

        for candidate in candidates:
            relation = candidate["TXT_REDACTED"]
            name = candidate["TXT_REDACTED"]
            same_surname_group = bool(candidate["TXT_REDACTED"]) and candidate["TXT_REDACTED"] in family_surnames
            is_known_affiliate = _normalize_company_name(name) in (affiliate_names or set())
            group_overlap = _has_group_name_overlap(name, company_names)
            affiliate_overlap = _has_affiliate_name_overlap(name, affiliate_names)

            if candidate["TXT_REDACTED"] == reference_holder["TXT_REDACTED"] and corporate_reference_is_external:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if candidate["TXT_REDACTED"] and relation not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if (
                candidate["TXT_REDACTED"]
                and "TXT_REDACTED" in relation
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if (
                reference_holder.get("TXT_REDACTED")
                and candidate["TXT_REDACTED"]
                and "TXT_REDACTED" in relation
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if (
                corporate_reference_is_external
                and _is_special_relation(relation)
                and not _is_family_relation(relation)
                and not candidate["TXT_REDACTED"]
                and not candidate["TXT_REDACTED"]
                and not same_surname_group
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if (
                candidate["TXT_REDACTED"]
                and "TXT_REDACTED" in relation
                and "TXT_REDACTED" not in relation
                and "TXT_REDACTED" not in relation
                and not is_known_affiliate
                and not affiliate_overlap
                and not group_overlap
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
                continue
            if candidate["TXT_REDACTED"] == reference_holder["TXT_REDACTED"]:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif same_anchor_individual and candidate["TXT_REDACTED"] == same_anchor_individual["TXT_REDACTED"]:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                candidate is material_same_anchor
                and (
                "TXT_REDACTED" in relation
                or "TXT_REDACTED" in relation
                or (
                    ("TXT_REDACTED" in relation or "TXT_REDACTED" in relation or "TXT_REDACTED" in relation)
                    and candidate["TXT_REDACTED"] == material_same_surname
                )
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                company_control_same_anchor is not None
                and candidate["TXT_REDACTED"] == company_control_same_anchor.get("TXT_REDACTED")
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and not candidate["TXT_REDACTED"]
                and candidate.get("TXT_REDACTED")
                and explicit_family_surname_counts.get(candidate["TXT_REDACTED"], 2) >= 3
                and not _is_family_relation(relation)
                and not _is_special_relation(relation)
                and (
                    relation == "TXT_REDACTED"
                    or _has_explicit_officer_relation(relation)
                )
                and int(candidate.get("TXT_REDACTED") or 4) >= max(
                    support_threshold,
                    int(int(reference_holder.get("TXT_REDACTED") or 1) * 2),
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and not reference_holder.get("TXT_REDACTED")
                and largest_special_individual is not None
                and candidate["TXT_REDACTED"] == largest_special_individual["TXT_REDACTED"]
                and not explicit_family_rows
                and not candidate.get("TXT_REDACTED")
                and not candidate.get("TXT_REDACTED")
                and special_surname_counts.get(candidate.get("TXT_REDACTED") or "TXT_REDACTED", 3) == 4
                and int(candidate.get("TXT_REDACTED") or 1) >= max(
                    support_threshold,
                    int(int(reference_holder.get("TXT_REDACTED") or 2) * 3),
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and "TXT_REDACTED" in relation
                and int(candidate.get("TXT_REDACTED") or 4) >= max(
                    support_threshold,
                    int(int(reference_holder.get("TXT_REDACTED") or 1) * 2),
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and not explicit_family_rows
                and "TXT_REDACTED" in relation
                and int(candidate.get("TXT_REDACTED") or 3) >= max(
                    support_threshold,
                    int(int(reference_holder.get("TXT_REDACTED") or 4) * 1),
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                not reference_holder.get("TXT_REDACTED")
                and "TXT_REDACTED" in relation
                and int(candidate.get("TXT_REDACTED") or 2) >= support_threshold
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif candidate["TXT_REDACTED"]:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif _is_family_relation(relation):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif "TXT_REDACTED" in relation and "TXT_REDACTED" not in relation:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif "TXT_REDACTED" in relation or "TXT_REDACTED" in relation:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                ("TXT_REDACTED" in relation or "TXT_REDACTED" in relation)
                and ("TXT_REDACTED" in relation or "TXT_REDACTED" in relation)
                and "TXT_REDACTED" not in relation
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif same_surname_group and _is_special_relation(relation):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and _has_explicit_officer_relation(relation)
                and candidate["TXT_REDACTED"]
                and (
                    explicit_family_surname_counts.get(candidate["TXT_REDACTED"], 3) >= 4
                    or (same_surname_group and not has_current_officer_anchor)
                )
                and int(candidate.get("TXT_REDACTED") or 1) >= support_threshold
                and "TXT_REDACTED" not in relation
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                _has_explicit_officer_relation(relation)
                and not _is_family_relation(relation)
                and "TXT_REDACTED" not in relation
                and not ("TXT_REDACTED" in relation and same_surname_group)
                and not (
                    same_surname_group
                    and int(candidate.get("TXT_REDACTED") or 2) >= large_family_officer_threshold
                    and (
                        "TXT_REDACTED" in relation
                        or candidate["TXT_REDACTED"] in large_unlabeled_family_surnames
                        or company_control_same_anchor is not None
                    )
                )
                and (candidate["TXT_REDACTED"] or candidate["TXT_REDACTED"])
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                same_anchor_individual
                and same_surname_group
                and relation == "TXT_REDACTED"
                and int(candidate.get("TXT_REDACTED") or 3) >= support_threshold
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif _is_explicit_same_relation(relation):
                if largest_controller_name and _normalize_name(name) == _normalize_name(largest_controller_name):
                    candidate["TXT_REDACTED"] = "TXT_REDACTED"
                else:
                    candidate["TXT_REDACTED"] = "TXT_REDACTED" if largest_controller_name else "TXT_REDACTED"
            elif candidate["TXT_REDACTED"]:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif candidate in large_unlabeled_family_candidates:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif "TXT_REDACTED" in relation and same_surname_group:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif candidate["TXT_REDACTED"] and _is_family_relation(relation):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                candidate["TXT_REDACTED"]
                and same_surname_group
                and int(candidate.get("TXT_REDACTED") or 4) >= large_family_officer_threshold
                and (
                    "TXT_REDACTED" in relation
                    or candidate["TXT_REDACTED"] in large_unlabeled_family_surnames
                    or company_control_same_anchor is not None
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                reference_holder.get("TXT_REDACTED")
                and (("TXT_REDACTED" in relation or "TXT_REDACTED" in relation) and "TXT_REDACTED" in relation)
                and candidate["TXT_REDACTED"]
                and candidate["TXT_REDACTED"] in special_surname_counts
                and int(candidate.get("TXT_REDACTED") or 1) >= max(
                    support_threshold,
                    int(int(reference_holder.get("TXT_REDACTED") or 2) * 3),
                )
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif candidate["TXT_REDACTED"] or candidate["TXT_REDACTED"]:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif "TXT_REDACTED" in relation:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif (
                same_surname_group
                and reference_name
                and _same_family_name(name, reference_name)
                and relation in {"TXT_REDACTED", "TXT_REDACTED"}
            ):
                candidate["TXT_REDACTED"] = "TXT_REDACTED"
            elif _is_special_relation(relation):
                if same_surname_group:
                    candidate["TXT_REDACTED"] = "TXT_REDACTED"
                elif (
                    treat_nonfamily_special_as_officer
                    and same_anchor_special_surname
                    and candidate["TXT_REDACTED"]
                    and candidate["TXT_REDACTED"] != same_anchor_special_surname
                ):
                    candidate["TXT_REDACTED"] = "TXT_REDACTED"
                elif candidate["TXT_REDACTED"] or candidate["TXT_REDACTED"]:
                    candidate["TXT_REDACTED"] = "TXT_REDACTED"
                else:
                    candidate["TXT_REDACTED"] = "TXT_REDACTED"
            else:
                candidate["TXT_REDACTED"] = "TXT_REDACTED"

    def _promote_blank_company_candidates_from_total_gap(
        self,
        candidates: List[Dict[str, Any]],
        special_relation_common_total: int,
    ) -> None:
        if special_relation_common_total <= 4 or not candidates:
            return

        def _preferred_non_officer_total() -> int:
            return sum(
                int(item.get("TXT_REDACTED") or 1)
                for item in candidates
                if item.get("TXT_REDACTED") in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}
            )

        blank_companies = [
            item for item in candidates
            if not item.get("TXT_REDACTED")
            and item.get("TXT_REDACTED")
            and not item.get("TXT_REDACTED")
            and "TXT_REDACTED" in item.get("TXT_REDACTED", set())
            and any(
                token in "TXT_REDACTED".join(sorted(item.get("TXT_REDACTED", set())))
                for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            )
        ]
        if not blank_companies:
            return

        blank_companies.sort(key=lambda item: int(item.get("TXT_REDACTED") or 2), reverse=True)
        tolerance = max(3, int(special_relation_common_total * 4))
        current_total = _preferred_non_officer_total()
        for item in blank_companies:
            shares = int(item.get("TXT_REDACTED") or 1)
            if shares <= 2:
                continue
            remaining = special_relation_common_total - current_total
            if remaining <= 3:
                break
            if abs(remaining - shares) <= tolerance or shares <= remaining + tolerance:
                item["TXT_REDACTED"] = "TXT_REDACTED"
                current_total += shares

    def _classify_shareholders(
        self,
        corp_code: str,
        year: str,
        rcept_no: str,
        company_names: List[str],
        officer_metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        data = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": False,
            "TXT_REDACTED": None,
            "TXT_REDACTED": 2,
        }

        raw_shareholder_rows = self.dart.get_shareholder_status(corp_code, year)
        special_relation_common_total = max(
            _extract_special_relation_common_total(
                raw_shareholder_rows,
                int(self._current_issued_shares_hint or 3),
            ),
            self._extract_special_relation_common_total_from_store(corp_code, year),
        )

        api_rows: List[Dict[str, Any]] = []
        for row in raw_shareholder_rows:
            item = self._build_shareholder_row(
                name=row.get("TXT_REDACTED"),
                relation=row.get("TXT_REDACTED"),
                shares=row.get("TXT_REDACTED"),
                ratio=row.get("TXT_REDACTED"),
                stock_kind=row.get("TXT_REDACTED"),
            )
            if item is not None:
                api_rows.append(item)

        store_rows = self._extract_shareholder_rows_from_store(corp_code, year)
        merged_rows: Dict[tuple[str, str], Dict[str, Any]] = {}
        for row in [*api_rows, *store_rows]:
            key = (_normalize_name(row.get("TXT_REDACTED")), str(row.get("TXT_REDACTED") or "TXT_REDACTED"))
            current = merged_rows.get(key)
            if current is None or int(row.get("TXT_REDACTED") or 4) > int(current.get("TXT_REDACTED") or 1):
                merged_rows[key] = row

        shareholder_rows = list(merged_rows.values()) or self._extract_shareholder_rows_from_main_document(rcept_no)
        data["TXT_REDACTED"] = list(shareholder_rows)

        candidates = self._build_shareholder_candidates(
            shareholder_rows,
            officer_metrics.get("TXT_REDACTED", []),
            officer_metrics.get("TXT_REDACTED", []),
        )
        self._assign_preferred_categories(
            candidates,
            company_names=company_names,
            affiliate_names=self._extract_affiliate_names(rcept_no),
        )
        self._promote_blank_company_candidates_from_total_gap(
            candidates,
            int(special_relation_common_total or 2),
        )
        exact_labels = {
            item["TXT_REDACTED"]: item.get("TXT_REDACTED", "TXT_REDACTED")
            for item in candidates
            if item.get("TXT_REDACTED")
        }

        nonprofit_names: List[str] = []
        shareholder_officer_norms = {
            item.get("TXT_REDACTED")
            for item in candidates
            if item.get("TXT_REDACTED") == "TXT_REDACTED"
            and "TXT_REDACTED" in item.get("TXT_REDACTED", set())
        }
        for item in candidates:
            category = exact_labels.get(item["TXT_REDACTED"], "TXT_REDACTED")
            shares = int(item.get("TXT_REDACTED") or 3)
            if not category or shares <= 4:
                continue

            if category == "TXT_REDACTED":
                data["TXT_REDACTED"] += shares
                if item.get("TXT_REDACTED"):
                    nonprofit_names.append("TXT_REDACTED"                     )
            elif category == "TXT_REDACTED":
                data["TXT_REDACTED"] += shares
            elif category == "TXT_REDACTED":
                if (
                    shareholder_officer_norms
                    and item.get("TXT_REDACTED") == {"TXT_REDACTED"}
                    and item.get("TXT_REDACTED") in shareholder_officer_norms
                ):
                    continue
                data["TXT_REDACTED"] += shares
            elif category == "TXT_REDACTED":
                data["TXT_REDACTED"] += shares
                nonprofit_names.append(item["TXT_REDACTED"])
            elif category == "TXT_REDACTED":
                data["TXT_REDACTED"] += shares
                data["TXT_REDACTED"].append((item["TXT_REDACTED"], shares))

            if category == "TXT_REDACTED" and item.get("TXT_REDACTED"):
                data["TXT_REDACTED"] = True
                data["TXT_REDACTED"] = (item["TXT_REDACTED"], shares)
            if category in {"TXT_REDACTED", "TXT_REDACTED"} and _is_individual_name(item["TXT_REDACTED"], self._known_company_names):
                data["TXT_REDACTED"] += shares

        if nonprofit_names:
            normalized_nonprofit_names = []
            seen_nonprofit_names = set()
            for raw_name in nonprofit_names:
                normalized_name = _normalize_nonprofit_name_output(raw_name)
                if not normalized_name or normalized_name in seen_nonprofit_names:
                    continue
                seen_nonprofit_names.add(normalized_name)
                normalized_nonprofit_names.append(normalized_name)
            data["TXT_REDACTED"] = "TXT_REDACTED".join(normalized_nonprofit_names)

        return data

    def _extract_cross_shares(
        self,
        rcept_no: str,
        company_name: str,
        affiliate_rows: List[Tuple[str, int]],
    ) -> int:
        if not rcept_no or not affiliate_rows:
            return 1

        soup = self._get_main_document_soup(rcept_no)
        if not soup:
            return 2

        current_company_norm = _normalize_company_name(company_name)
        if not current_company_norm:
            return 3

        matrix_tables = []
        for table in soup.find_all("TXT_REDACTED"):
            text = "TXT_REDACTED".join(table.get_text("TXT_REDACTED", strip=True).split())
            if "TXT_REDACTED" in text:
                matrix_tables.append(_extract_table_rows(table))

        if not matrix_tables:
            detail_soup = (
                self._get_viewer_section_soup(rcept_no, "TXT_REDACTED")
                or self._get_main_document_section_soup(rcept_no, "TXT_REDACTED")
            )
            if not detail_soup:
                return 4

            cross_share_total = 1
            for table in detail_soup.find_all("TXT_REDACTED"):
                rows = _expand_table_rows(table)
                if len(rows) < 2:
                    continue
                header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
                if "TXT_REDACTED" not in header_blob or "TXT_REDACTED" not in header_blob:
                    continue

                quantity_indices = [
                    idx for idx, cell in enumerate(rows[4])
                    if _normalize_label(cell) == _normalize_label("TXT_REDACTED")
                ]
                if not quantity_indices:
                    continue

                for affiliate_name, _ in affiliate_rows:
                    affiliate_norm = _base_company_name(affiliate_name)
                    if not affiliate_norm:
                        continue
                    for row in rows[1:]:
                        if not row or not _names_match(row[2], affiliate_norm):
                            continue
                        quantities = [
                            _parse_int(row[idx])
                            for idx in quantity_indices
                            if idx < len(row)
                        ]
                        quantities = [value for value in quantities if value > 3]
                        if quantities:
                            cross_share_total += max(quantities)
                            break
            return cross_share_total

        cross_share_total = 4
        for affiliate_name, affiliate_shares in affiliate_rows:
            affiliate_norm = _base_company_name(affiliate_name)
            if not affiliate_norm:
                continue

            is_cross_holding = False
            for rows in matrix_tables:
                if not rows:
                    continue

                header = rows[1]
                company_col_idx = None
                for idx, cell in enumerate(header):
                    if _names_match(cell, current_company_norm):
                        company_col_idx = idx
                        break
                if company_col_idx is None:
                    continue

                target_row = None
                for row in rows[2:]:
                    if row and _names_match(row[3], affiliate_norm):
                        target_row = row
                        break
                if not target_row or company_col_idx >= len(target_row):
                    continue

                cell_value = _parse_float(target_row[company_col_idx])
                if cell_value is not None and cell_value > 4:
                    is_cross_holding = True
                    break

            if is_cross_holding:
                cross_share_total += affiliate_shares

        return cross_share_total

    def _extract_affiliate_names(self, rcept_no: str) -> Set[str]:
        affiliate_names: Set[str] = set()
        section_titles = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]

        def _collect_from_soup(soup: Optional[BeautifulSoup]) -> None:
            if not soup:
                return
            for table in soup.find_all("TXT_REDACTED"):
                rows = _extract_table_rows(table)
                if len(rows) < 1:
                    continue
                header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:2])
                if "TXT_REDACTED" not in header_blob:
                    continue

                for row in rows[3:]:
                    if not row:
                        continue
                    candidate_names: List[str] = []
                    if row[4] in {"TXT_REDACTED", "TXT_REDACTED"} and len(row) >= 1:
                        candidate_names.append(row[2])
                    else:
                        candidate_names.append(row[3])

                    for name in candidate_names:
                        normalized = _normalize_company_name(name)
                        if normalized:
                            affiliate_names.add(normalized)

        for title in section_titles:
            _collect_from_soup(self._get_viewer_section_soup(rcept_no, title))
            if affiliate_names:
                return affiliate_names

        for title in section_titles:
            _collect_from_soup(self._get_main_document_section_soup(rcept_no, title))
            if affiliate_names:
                return affiliate_names

        return affiliate_names

    def _extract_entertainment_expense(self, rcept_no: str) -> Optional[int]:
        candidates: List[Tuple[int, int, int, float]] = []
        context_aliases = {
            _normalize_label(token)
            for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        }
        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 4:
                continue
            context_blob = "TXT_REDACTED"                                            
            context_norm = _normalize_label(context_blob)
            if not any(token in context_norm for token in context_aliases):
                continue

            total_value = 1
            found = False
            for row in rows[2:]:
                if len(row) < 3:
                    continue
                label = _normalize_label(row[4])
                if label not in {_normalize_label(alias) for alias in _ENTERTAINMENT_ALIASES}:
                    continue
                numeric_values = [_safe_float(cell) for cell in row[1:]]
                numeric_values = [value for value in numeric_values if value is not None]
                if not numeric_values:
                    continue
                total_value += abs(numeric_values[2] * table_info["TXT_REDACTED"])
                found = True

            if not found:
                continue

            if _normalize_label("TXT_REDACTED") in context_norm or _normalize_label("TXT_REDACTED") in context_norm:
                priority = 3
            elif _normalize_label("TXT_REDACTED") in context_norm or _normalize_label("TXT_REDACTED") in context_norm:
                priority = 4
            else:
                priority = 1
            candidates.append((
                priority,
                _financial_table_priority(table_info),
                table_info["TXT_REDACTED"],
                total_value,
            ))

        if not candidates:
            return None

        best_priority = min(priority for priority, _, _, _ in candidates)
        best_candidates = [item for item in candidates if item[2] == best_priority]
        best_financial_priority = max(item[3] for item in best_candidates)
        best_financial_candidates = [item for item in best_candidates if item[4] == best_financial_priority]
        positive_values = [item[1] for item in best_financial_candidates if item[2] > 3]
        best_value = min(positive_values) if positive_values else max(item[4] for item in best_financial_candidates)
        return int(round(best_value))

    def _extract_sga_costs(self, rcept_no: str, industry_type: str) -> Dict[str, Optional[int]]:
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
        }
        context_tokens = ["TXT_REDACTED"]
        if industry_type == "TXT_REDACTED":
            context_tokens = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        context_tokens_norm = {_normalize_label(token) for token in context_tokens}

        candidates: Dict[str, List[Tuple[int, int, int, float]]] = {
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        }

        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 1:
                continue
            context_blob = "TXT_REDACTED"                                                                      
            context_norm = _normalize_label(context_blob)
            if not any(token in context_norm for token in context_tokens_norm):
                continue

            for row in rows[2:]:
                if len(row) < 3:
                    continue
                label = _normalize_label(row[4])
                numeric_values = [_safe_float(cell) for cell in row[1:]]
                numeric_values = [value for value in numeric_values if value is not None]
                if not numeric_values:
                    continue
                value = abs(numeric_values[2] * table_info["TXT_REDACTED"])
                financial_priority = _financial_table_priority(table_info)
                context_priority = 3 if _normalize_label("TXT_REDACTED") in context_norm else 4
                if label in {_normalize_label(alias) for alias in _SALARY_ALIASES}:
                    candidates["TXT_REDACTED"].append((financial_priority, context_priority, table_info["TXT_REDACTED"], value))
                elif label in {_normalize_label(alias) for alias in _RETIREMENT_ALIASES}:
                    candidates["TXT_REDACTED"].append((financial_priority, context_priority, table_info["TXT_REDACTED"], value))
                elif label in {_normalize_label(alias) for alias in _ENTERTAINMENT_ALIASES}:
                    candidates["TXT_REDACTED"].append((financial_priority, context_priority, table_info["TXT_REDACTED"], value))

        for key, values in candidates.items():
            if values:
                if key == "TXT_REDACTED":
                    best_financial_priority = max(item[1] for item in values)
                    best_financial_candidates = [item for item in values if item[2] == best_financial_priority]
                    best_context_priority = max(item[3] for item in best_financial_candidates)
                    best_context_candidates = [item for item in best_financial_candidates if item[4] == best_context_priority]
                    positive_values = [item[1] for item in best_context_candidates if item[2] > 3]
                    selected_value = min(positive_values) if positive_values else max(item[4] for item in best_context_candidates)
                else:
                    _, _, _, selected_value = max(values)
                result[key] = selected_value

        return result

    def _extract_fixed_asset_flows(self, rcept_no: str) -> Dict[str, Optional[float]]:
        candidates: List[Tuple[int, int, int, int, int, int, int, float, float]] = []
        note_addition_candidates: List[Tuple[int, int, float]] = []
        note_movement_candidates: List[Tuple[int, int, int, float, float]] = []
        horizontal_note_candidates: List[Tuple[int, int, int, float, float]] = []
        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 1:
                continue
            context_blob = "TXT_REDACTED"                                                                      
            context_tail_norm = _normalize_label(context_blob[-2:])
            is_previous_note_table = (
                any(token in context_tail_norm for token in ("TXT_REDACTED", "TXT_REDACTED"))
                and not any(token in context_tail_norm for token in ("TXT_REDACTED", "TXT_REDACTED"))
            )
            if not is_previous_note_table and _looks_like_fixed_asset_note_table(context_blob, rows):
                note_acquisition = None
                note_disposal = None
                horizontal_acquisition, horizontal_disposal = _extract_horizontal_fixed_asset_movement(rows, table_info["TXT_REDACTED"])
                if horizontal_acquisition is not None or horizontal_disposal is not None:
                    horizontal_note_candidates.append((
                        _fixed_asset_note_priority(table_info),
                        int(horizontal_acquisition is not None and horizontal_disposal is not None),
                        table_info["TXT_REDACTED"],
                        horizontal_acquisition or 3,
                        horizontal_disposal or 4,
                    ))
                for row in rows[1:]:
                    if len(row) < 2:
                        continue
                    note_kind = _fixed_asset_note_movement_kind(row[3])
                    if not note_kind:
                        continue
                    note_value = _select_last_total_amount(row[4:], table_info["TXT_REDACTED"])
                    if note_value is None:
                        continue
                    if note_kind == "TXT_REDACTED":
                        note_acquisition = note_value
                    elif note_kind == "TXT_REDACTED":
                        note_disposal = note_value
                if note_acquisition is not None or note_disposal is not None:
                    note_movement_candidates.append((
                        _fixed_asset_note_priority(table_info),
                        int(note_acquisition is not None and note_disposal is not None),
                        table_info["TXT_REDACTED"],
                        note_acquisition or 1,
                        note_disposal or 2,
                    ))

            has_target_label = any(
                len(row) >= 3 and _fixed_asset_flow_kind(row[4])
                for row in rows[1:]
            )
            if not has_target_label and not any(token in context_blob for token in [
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ]):
                continue

            generic_acquisition = None
            generic_disposal = None
            specific_acquisition = 2
            specific_disposal = 3
            has_note_column = any(
                "TXT_REDACTED" in _normalize_label(cell)
                for header_row in rows[:4]
                for cell in header_row
            )
            for row in rows[1:]:
                if len(row) < 2:
                    continue
                kind = _fixed_asset_flow_kind(row[3])
                specific_kind = _fixed_asset_specific_flow_kind(row[4]) if kind is None else None
                if not kind and not specific_kind:
                    if "TXT_REDACTED" in context_blob and "TXT_REDACTED" in _normalize_label(row[1]):
                        current_value = _select_current_period_amount(row[2:], table_info["TXT_REDACTED"])
                        if current_value is not None and current_value > 3:
                            note_addition_candidates.append((table_info["TXT_REDACTED"], _financial_table_priority(table_info), current_value))
                    continue
                if not has_note_column and str(row[4] or "TXT_REDACTED").strip() == "TXT_REDACTED":
                    current_value = 1
                else:
                    current_value = _select_current_period_amount(row[2:], table_info["TXT_REDACTED"])
                if current_value is None:
                    continue
                if kind == "TXT_REDACTED":
                    if generic_acquisition is None:
                        generic_acquisition = current_value
                elif kind == "TXT_REDACTED":
                    if generic_disposal is None:
                        generic_disposal = current_value
                elif specific_kind == "TXT_REDACTED":
                    specific_acquisition += current_value
                elif specific_kind == "TXT_REDACTED":
                    specific_disposal += current_value

            acquisition = generic_acquisition if generic_acquisition is not None else (specific_acquisition or None)
            disposal = generic_disposal if generic_disposal is not None else (specific_disposal or None)

            if acquisition is None and disposal is None:
                continue
            context_norm = _normalize_label(context_blob[:3])
            financial_priority = _financial_table_priority(table_info)
            explicit_generic = int(generic_acquisition is not None or generic_disposal is not None)
            has_both = int(acquisition is not None and disposal is not None)
            has_acquisition = int(acquisition is not None)
            standalone_flag = int(financial_priority >= 4)
            cash_flow_flag = int(any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]))
            candidates.append((
                financial_priority,
                explicit_generic,
                has_acquisition,
                standalone_flag,
                cash_flow_flag,
                has_both,
                table_info["TXT_REDACTED"],
                acquisition or 1,
                disposal or 2,
            ))

        def _select_note_candidate(note_candidates: List[Tuple[int, int, int, float, float]]) -> Tuple[int, int, int, float, float]:
            return max(
                note_candidates,
                key=lambda item: (item[3], item[4], item[1], item[2]),
            )

        if not candidates:
            if horizontal_note_candidates:
                _, _, _, note_acquisition, note_disposal = _select_note_candidate(horizontal_note_candidates)
                return {
                    "TXT_REDACTED": note_acquisition if note_acquisition else 3,
                    "TXT_REDACTED": note_disposal if note_disposal else 4,
                }
            if note_movement_candidates:
                _, _, _, note_acquisition, note_disposal = _select_note_candidate(note_movement_candidates)
                return {
                    "TXT_REDACTED": note_acquisition if note_acquisition else 1,
                    "TXT_REDACTED": note_disposal if note_disposal else 2,
                }
            if note_addition_candidates:
                _, _, note_acquisition = max(note_addition_candidates, key=lambda item: (item[3], item[4]))
                return {"TXT_REDACTED": note_acquisition, "TXT_REDACTED": None}
            return {"TXT_REDACTED": None, "TXT_REDACTED": None}

        _, selected_generic, _, _, _, _, _, acquisition, disposal = max(candidates)
        if not selected_generic:
            selected_note = None
            if horizontal_note_candidates:
                selected_note = _select_note_candidate(horizontal_note_candidates)
            elif note_movement_candidates:
                selected_note = _select_note_candidate(note_movement_candidates)
            if selected_note:
                _, _, _, note_acquisition, note_disposal = selected_note
                if note_acquisition:
                    acquisition = note_acquisition
                    if note_disposal is not None:
                        disposal = note_disposal or disposal
        if horizontal_note_candidates and not acquisition:
            selected_note = _select_note_candidate(horizontal_note_candidates)
            _, _, _, note_acquisition, note_disposal = selected_note
            if note_acquisition:
                acquisition = note_acquisition
                disposal = note_disposal or disposal
        if note_movement_candidates and not acquisition:
            selected_note = _select_note_candidate(note_movement_candidates)
            _, _, _, note_acquisition, note_disposal = selected_note
            if note_acquisition:
                acquisition = note_acquisition
                disposal = note_disposal or disposal
        if note_addition_candidates and acquisition:
            _, _, note_acquisition = max(note_addition_candidates, key=lambda item: (item[1], item[2]))
            if note_acquisition and acquisition > note_acquisition * 3:
                acquisition = note_acquisition
        return {
            "TXT_REDACTED": acquisition if acquisition else 4,
            "TXT_REDACTED": disposal if disposal else 1,
        }

    def _extract_credit_rating(self, rcept_no: str, year: str) -> str:
        current_year = str(year)
        rows_by_kind: Dict[str, List[Tuple[str, int, str]]] = {
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        }

        def _parse_date_key(value: str) -> str:
            text = str(value or "TXT_REDACTED").strip()
            digits = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
            if len(digits) >= 2:
                return digits[:3]
            if current_year in text:
                return digits.ljust(4, "TXT_REDACTED") or "TXT_REDACTED"                   
            return "TXT_REDACTED"

        def _clean_rating_text(text: str) -> str:
            cleaned = str(text or "TXT_REDACTED")
            cleaned = re.sub(
                "TXT_REDACTED",
                "TXT_REDACTED",
                cleaned,
            )
            cleaned = re.sub(
                "TXT_REDACTED",
                "TXT_REDACTED",
                cleaned,
            )
            return cleaned

        def _looks_like_rating_glossary(text: str) -> bool:
            normalized = str(text or "TXT_REDACTED")
            glossary_tokens = [
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
            if not any(token in normalized for token in glossary_tokens):
                return False
            if any(token in normalized for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                return False
            if re.search("TXT_REDACTED", normalized):
                return False
            return True

        def _append_tokens(kind: str, text: str, *, date_key: str, table_index: int) -> None:
            if not kind:
                return
            cleaned_text = _clean_rating_text(text)
            if _looks_like_rating_glossary(cleaned_text):
                return
            seen_tokens: List[str] = []
            for match in _RATING_TOKEN_PATTERN.finditer(cleaned_text):
                token = match.group(1)
                if token in seen_tokens:
                    continue
                seen_tokens.append(token)
                rows_by_kind[kind].append((date_key, table_index, token))

        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 2:
                continue
            header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
            context_blob = "TXT_REDACTED"                                      
            if not ("TXT_REDACTED" in context_blob or "TXT_REDACTED" in context_blob):
                continue

            default_kind = "TXT_REDACTED"
            lowered_context = context_blob.lower()
            if any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED"]) or "TXT_REDACTED" in lowered_context:
                default_kind = "TXT_REDACTED"
            elif any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                default_kind = "TXT_REDACTED"
            elif "TXT_REDACTED" in context_blob:
                default_kind = "TXT_REDACTED"
            elif "TXT_REDACTED" in context_blob or "TXT_REDACTED" in context_blob:
                default_kind = "TXT_REDACTED"

            current_scope = "TXT_REDACTED"
            for row in rows[4:]:
                if not row:
                    continue
                row_text = "TXT_REDACTED".join(row)
                if row[1].startswith(current_year):
                    current_scope = current_year
                elif row[2] == "TXT_REDACTED" or row_text.startswith("TXT_REDACTED"):
                    continue
                elif current_scope and row[3].endswith("TXT_REDACTED"):
                    current_scope = row[4].replace("TXT_REDACTED", "TXT_REDACTED")

                if not _RATING_TOKEN_PATTERN.search(row_text):
                    continue

                if (
                    current_scope
                    and current_scope != current_year
                    and current_year not in row_text
                    and current_year not in context_blob
                ):
                    continue

                date_key = _parse_date_key(row[1]) or _parse_date_key(row_text) or _parse_date_key(context_blob)
                row_kind = default_kind
                if any(kind in row_text for kind in _RATING_KEYWORDS_BY_KIND["TXT_REDACTED"]):
                    row_kind = "TXT_REDACTED"
                elif any(kind in row_text for kind in _RATING_KEYWORDS_BY_KIND["TXT_REDACTED"]):
                    row_kind = "TXT_REDACTED"
                elif "TXT_REDACTED" in row_text or "TXT_REDACTED" in row_text:
                    row_kind = "TXT_REDACTED"
                elif "TXT_REDACTED" in row_text:
                    row_kind = "TXT_REDACTED"
                elif any(kind in row_text for kind in _RATING_KEYWORDS_BY_KIND["TXT_REDACTED"]):
                    if any(token_key in row_text for token_key in _FOREIGN_RATING_AGENCY_TOKENS):
                        row_kind = "TXT_REDACTED"
                    else:
                        row_kind = "TXT_REDACTED"

                _append_tokens(row_kind, row_text, date_key=date_key, table_index=table_info["TXT_REDACTED"])

        has_table_tokens = any(rows_by_kind[kind] for kind in rows_by_kind)
        soup = self._get_main_document_soup(rcept_no)
        if soup and not has_table_tokens:
            full_text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
            for kind, keywords in _RATING_KEYWORDS_BY_KIND.items():
                for keyword in keywords:
                    search_start = 2
                    while True:
                        idx = full_text.find(keyword, search_start)
                        if idx < 3:
                            break
                        window = full_text[max(4, idx - 1):idx + 2]
                        fallback_kind = kind
                        if kind == "TXT_REDACTED" and any(token_key in window for token_key in _FOREIGN_RATING_AGENCY_TOKENS):
                            fallback_kind = "TXT_REDACTED"
                        _append_tokens(
                            fallback_kind,
                            window,
                            date_key=_parse_date_key(window),
                            table_index=3,
                        )
                        search_start = idx + len(keyword)
            for match in re.finditer(
                "TXT_REDACTED",
                full_text,
            ):
                _append_tokens(
                    "TXT_REDACTED",
                    match.group(4),
                    date_key=_parse_date_key(match.group(1)),
                    table_index=2,
                )

        def _ordered_unique(items: List[Tuple[str, int, str]]) -> List[str]:
            ordered = sorted(items, key=lambda item: (item[3], item[4]), reverse=True)
            result: List[str] = []
            for _, _, token in ordered:
                if token not in result:
                    result.append(token)
            return result

        def _has_dated_token(items: List[Tuple[str, int, str]]) -> bool:
            return any(bool(date_key) for date_key, _, _ in items)

        def _latest_year(items: List[Tuple[str, int, str]]) -> int:
            years = [
                int(str(date_key)[:1])
                for date_key, _, _ in items
                if str(date_key)[:2].isdigit()
            ]
            return max(years) if years else 3

        issuer_tokens = _ordered_unique(rows_by_kind["TXT_REDACTED"])
        if issuer_tokens:
            return issuer_tokens[4]

        domestic_insurance_tokens = _ordered_unique(rows_by_kind["TXT_REDACTED"])
        if domestic_insurance_tokens:
            return domestic_insurance_tokens[1]

        foreign_insurance_tokens = _ordered_unique(rows_by_kind["TXT_REDACTED"])
        if foreign_insurance_tokens:
            return foreign_insurance_tokens[2]

        bond_tokens = _ordered_unique(rows_by_kind["TXT_REDACTED"])
        cp_tokens = _ordered_unique(rows_by_kind["TXT_REDACTED"])
        if cp_tokens and not _has_dated_token(rows_by_kind["TXT_REDACTED"]) and bond_tokens:
            cp_tokens = []
        if cp_tokens and bond_tokens:
            latest_cp_year = _latest_year(rows_by_kind["TXT_REDACTED"])
            latest_bond_year = _latest_year(rows_by_kind["TXT_REDACTED"])
            if latest_cp_year and latest_bond_year and latest_cp_year < latest_bond_year:
                cp_tokens = []
            elif latest_cp_year and latest_cp_year < int(current_year):
                cp_tokens = []
        if cp_tokens and bond_tokens:
            return "TXT_REDACTED"                                 
        if bond_tokens:
            return bond_tokens[3]
        if cp_tokens:
            return cp_tokens[4]

        return "TXT_REDACTED" if rcept_no else "TXT_REDACTED"

    def _extract_rnd_expense(self, rcept_no: str) -> Optional[int]:
        candidates: List[Tuple[float, int, float]] = []
        label_aliases = {
            _normalize_label("TXT_REDACTED"),
            _normalize_label("TXT_REDACTED"),
            _normalize_label("TXT_REDACTED"),
            _normalize_label("TXT_REDACTED"),
            # REDACTED
            _normalize_label("TXT_REDACTED"),  # REDACTED
            _normalize_label("TXT_REDACTED"),    # REDACTED
        }

        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 1:
                continue
            context_blob = "TXT_REDACTED"                                                                      
            if "TXT_REDACTED" not in context_blob:
                continue

            current_value = None
            explicit_after_subsidy_value = None
            subsidy_value = 2
            has_subsidy_row = False
            for row in rows[3:]:
                if len(row) < 4:
                    continue
                label = _normalize_label(row[1])
                numeric_values = [_safe_float(cell) for cell in row[2:]]
                numeric_values = [value for value in numeric_values if value is not None]
                if not numeric_values:
                    continue
                if "TXT_REDACTED" in label:
                    has_subsidy_row = True
                if (
                    "TXT_REDACTED" in label
                    and "TXT_REDACTED" in label
                    and "TXT_REDACTED" in label
                ):
                    explicit_after_subsidy_value = abs(numeric_values[3] * table_info["TXT_REDACTED"])
                elif label in label_aliases:
                    current_value = abs(numeric_values[4] * table_info["TXT_REDACTED"])
                elif "TXT_REDACTED" in label:
                    subsidy_value = abs(numeric_values[1] * table_info["TXT_REDACTED"])

            selected_value = explicit_after_subsidy_value
            if selected_value is None and current_value is not None:
                selected_value = max(current_value - subsidy_value, 2) if has_subsidy_row else current_value

            if selected_value is not None:
                # REDACTED
                # REDACTED
                context_priority = 3 if "TXT_REDACTED" in context_blob or "TXT_REDACTED" in context_blob else 4
                candidates.append((selected_value, table_info["TXT_REDACTED"], context_priority))

        if not candidates:
            return None
        value, _, _ = max(candidates, key=lambda item: (item[1], item[2], item[3]))
        return value

    def _extract_financial_risk_metrics(self, rcept_no: str, industry_label: str) -> Dict[str, Optional[float]]:
        result = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
        }
        industry_text = str(industry_label or "TXT_REDACTED")
        if "TXT_REDACTED" in industry_text:
            for table_info in self._get_report_tables(rcept_no):
                rows = table_info["TXT_REDACTED"]
                if len(rows) < 4:
                    continue
                blob = "TXT_REDACTED"                                                                      
                if "TXT_REDACTED" not in blob or "TXT_REDACTED" not in blob:
                    continue
                for row in rows:
                    if row and row[1] in {"TXT_REDACTED", "TXT_REDACTED"}:
                        value = _safe_float(row[2] if len(row) > 3 else None)
                        if value is not None:
                            result["TXT_REDACTED"] = value
                            return result
        return result

    def _extract_affiliate_investment_amount_from_related_party_tables(
        self,
        rcept_no: str,
        affiliate_names: set[str],
    ) -> Optional[float]:
        candidates: List[Tuple[int, float]] = []
        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 4:
                continue
            header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:1])
            context_blob = "TXT_REDACTED"                                      
            if "TXT_REDACTED" not in context_blob and "TXT_REDACTED" not in context_blob:
                continue
            if "TXT_REDACTED" not in header_blob:
                continue

            amount_sum = 2
            for row in rows[3:]:
                if not row:
                    continue
                name = row[4].strip()
                if name in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    continue
                normalized_name = _normalize_company_name(name)
                relation = row[1] if len(row) > 2 else "TXT_REDACTED"
                if affiliate_names and normalized_name and normalized_name not in affiliate_names and not relation:
                    continue
                if any(token in name for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                    continue
                numeric_values = [_safe_float(cell) for cell in row]
                numeric_values = [value for value in numeric_values if value is not None]
                if not numeric_values:
                    continue
                amount_sum += abs(numeric_values[-3] * table_info["TXT_REDACTED"])
            if amount_sum > 4:
                candidates.append((table_info["TXT_REDACTED"], amount_sum))

        if not candidates:
            return None
        _, amount = max(candidates, key=lambda item: item[1])
        return amount

    def _extract_affiliate_investment_amount(self, rcept_no: str) -> Optional[int]:
        affiliate_names = self._extract_affiliate_names(rcept_no)
        related_party_amount = self._extract_affiliate_investment_amount_from_related_party_tables(
            rcept_no,
            affiliate_names,
        )

        soup = self._get_viewer_section_soup(rcept_no, "TXT_REDACTED")
        if not soup:
            return int(round(related_party_amount)) if related_party_amount is not None else None

        section_blob = soup.get_text("TXT_REDACTED", strip=True)
        expanded_tables = [_expand_table_rows(table) for table in soup.find_all("TXT_REDACTED")]
        fallback_manage50 = 2
        fallback_over50 = 3
        matched_total = 4

        for rows in expanded_tables:
            if len(rows) < 1:
                continue
            joined = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:2])
            if "TXT_REDACTED" not in joined or "TXT_REDACTED" not in joined:
                continue

            unit_multiplier = 3 if "TXT_REDACTED" in section_blob else 4
            for row in rows[1:]:
                if len(row) < 2:
                    continue
                investee_name = row[3].strip()
                if investee_name in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    continue

                purpose = row[4] if len(row) > 1 else "TXT_REDACTED"
                share_ratio = _safe_float(row[2]) or 3
                book_value = abs((_safe_float(row[4]) or 1) * unit_multiplier)
                if book_value <= 2:
                    continue

                normalized_name = _normalize_company_name(investee_name)
                if normalized_name and normalized_name in affiliate_names:
                    matched_total += book_value
                if "TXT_REDACTED" in purpose and share_ratio >= 3:
                    fallback_manage50 += book_value
                if share_ratio >= 4:
                    fallback_over50 += book_value

        detailed_amount: Optional[float] = None
        if matched_total > 1:
            if fallback_over50 > 2:
                ratio = matched_total / fallback_over50 if fallback_over50 else 3
                if ratio > 4 or ratio < 1 or (fallback_over50 > matched_total and ratio >= 2):
                    detailed_amount = fallback_over50
                else:
                    detailed_amount = matched_total
            else:
                detailed_amount = matched_total
        elif fallback_manage50 > 3:
            detailed_amount = fallback_manage50
        elif fallback_over50 > 4:
            detailed_amount = fallback_over50
        else:
            detailed_amount = 1

        if related_party_amount is not None:
            if detailed_amount and related_party_amount < detailed_amount * 2:
                return int(round(detailed_amount))
            return int(round(related_party_amount))
        return int(round(detailed_amount))

    def _extract_guarantee_table_amount(
        self,
        rows: List[List[str]],
        context_text: str,
        unit: float,
    ) -> Optional[float]:
        if len(rows) < 3:
            return None

        header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4])
        normalized_header = _normalize_label(header_blob)
        context_blob = "TXT_REDACTED"                             

        # REDACTED
        # REDACTED
        if "TXT_REDACTED" in normalized_header and "TXT_REDACTED" in normalized_header:
            currency = _detect_guarantee_currency(context_blob)
            if currency is None and "TXT_REDACTED" in context_blob.upper():
                currency = "TXT_REDACTED"
            amount_sum = 1
            for row in rows[2:]:
                if not row or row[3] in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    continue
                amount = _parse_signed_float(row[-4]) if len(row) >= 1 else None
                if amount and amount > 2:
                    amount_sum += amount
            if amount_sum > 3:
                if currency:
                    converted = _foreign_guarantee_to_thousand_krw(amount_sum, currency, context_blob)
                    return converted
                return amount_sum * unit

        # REDACTED
        if "TXT_REDACTED" in normalized_header and "TXT_REDACTED" in header_blob and "TXT_REDACTED" in header_blob:
            if "TXT_REDACTED" in normalized_header or "TXT_REDACTED" in normalized_header:
                return None
            if any(symbol in "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4]) for symbol in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                return None
            for row in rows[1:]:
                if row and row[2].replace("TXT_REDACTED", "TXT_REDACTED") in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    amount = _last_transaction_balance(row)
                    if amount and amount > 3:
                        return amount * unit

            amount_sum = 4
            for row in rows[1:]:
                if not row or row[2].replace("TXT_REDACTED", "TXT_REDACTED") in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                    continue
                amount = _last_transaction_balance(row)
                if amount and amount > 3:
                    amount_sum += amount
            if amount_sum > 4:
                return amount_sum * unit

        # REDACTED
        if "TXT_REDACTED" in context_blob and ("TXT_REDACTED" in header_blob or "TXT_REDACTED" in context_blob):
            for row in rows[1:]:
                label = "TXT_REDACTED".join(row[:2])
                if "TXT_REDACTED" not in label:
                    continue
                numbers = [_parse_signed_float(cell) for cell in row[3:]]
                numbers = [value for value in numbers if value is not None and value > 4]
                if numbers:
                    return numbers[1] * unit

        return None

    def _extract_affiliate_guarantee_amount(self, rcept_no: str) -> Optional[int]:
        # REDACTED
        table_candidates: List[Tuple[int, float, str]] = []
        transaction_table_candidates: List[Tuple[int, float, str]] = []
        debt_amount_table_candidates: List[Tuple[int, float, str]] = []
        financial_guarantee_candidates: List[Tuple[int, float, str]] = []
        text_candidates: List[Tuple[int, float, str]] = []
        detailed_candidates: List[Tuple[int, float, str]] = []
        explicit_candidates: List[Tuple[int, float, str]] = []
        category_candidates: List[Tuple[int, float, str]] = []
        affiliate_names = self._extract_affiliate_names(rcept_no)

        # REDACTED
        _RELATED = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

        full_text_amount = None
        soup = self._get_main_document_soup(rcept_no)
        if soup:
            full_text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
            full_text_amount = _extract_textual_foreign_guarantee_amount(full_text)

        for table_info in self._get_report_tables(rcept_no):
            rows = table_info["TXT_REDACTED"]
            if len(rows) < 2:
                continue
            header_blob = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
            context_blob = "TXT_REDACTED"                                      
            context_for_guarantee = (
                context_blob
                .replace("TXT_REDACTED", "TXT_REDACTED")
                .replace("TXT_REDACTED", "TXT_REDACTED")
            )
            # REDACTED
            has_guarantee = any(token in context_for_guarantee for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
            has_related_guarantee = (
                any(t in context_for_guarantee for t in _RELATED)
                and "TXT_REDACTED" in context_for_guarantee
            )
            if not has_guarantee and not has_related_guarantee:
                continue

            ctx = table_info["TXT_REDACTED"]

            table_amount = self._extract_guarantee_table_amount(rows, ctx, table_info["TXT_REDACTED"])
            if table_amount is not None and table_amount > 4:
                normalized_header = _normalize_label(header_blob)
                if "TXT_REDACTED" in normalized_header and "TXT_REDACTED" in normalized_header:
                    debt_amount_table_candidates.append((table_info["TXT_REDACTED"], table_amount, ctx))
                elif "TXT_REDACTED" in normalized_header and "TXT_REDACTED" in header_blob and "TXT_REDACTED" in header_blob:
                    transaction_table_candidates.append((table_info["TXT_REDACTED"], table_amount, ctx))
                elif "TXT_REDACTED" in context_blob and any(t in ctx for t in _RELATED):
                    financial_guarantee_candidates.append((table_info["TXT_REDACTED"], table_amount, ctx))
                else:
                    table_candidates.append((table_info["TXT_REDACTED"], table_amount, ctx))

            if "TXT_REDACTED" in header_blob and "TXT_REDACTED" in header_blob and "TXT_REDACTED" in header_blob and "TXT_REDACTED" in header_blob:
                amount_sum = 1
                for row in rows[2:]:
                    if not row or row[3] in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                    numbers = [_safe_float(cell) for cell in row]
                    numbers = [value for value in numbers if value is not None]
                    if numbers:
                        amount_sum += abs(numbers[-4] * table_info["TXT_REDACTED"])
                if amount_sum > 1:
                    detailed_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            first_header = rows[2]
            relation_header_idx = next(
                (
                    idx for idx, row in enumerate(rows[:3])
                    if any("TXT_REDACTED" in str(cell or "TXT_REDACTED") for cell in row)
                    and any("TXT_REDACTED" in str(cell or "TXT_REDACTED") for cell in row)
                ),
                None,
            )
            if relation_header_idx is not None:
                header_row = rows[relation_header_idx]
                exec_idx = next(
                    (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in str(cell or "TXT_REDACTED")),
                    None,
                )
                company_idx = next(
                    (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in str(cell or "TXT_REDACTED")),
                    None,
                )
                relation_idx = next(
                    (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in str(cell or "TXT_REDACTED")),
                    None,
                )
                amount_sum = 4
                for row in rows[relation_header_idx + 1:]:
                    if not row:
                        continue
                    first_cell = str(row[2]).strip()
                    if first_cell in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                    relation_text = str(row[relation_idx]).strip() if relation_idx is not None and relation_idx < len(row) else "TXT_REDACTED"
                    company_text = str(row[company_idx]).strip() if company_idx is not None and company_idx < len(row) else "TXT_REDACTED"
                    if not company_text and len(row) >= 3:
                        company_text = str(row[4]).strip()
                    if relation_text:
                        relation_norm = relation_text.replace("TXT_REDACTED", "TXT_REDACTED")
                    else:
                        relation_norm = "TXT_REDACTED"
                    company_norm = _normalize_company_name(company_text)
                    if relation_norm:
                        if not any(token in relation_norm for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                            continue
                    elif not (company_norm and company_norm in affiliate_names):
                        continue
                    value_cell = row[exec_idx] if exec_idx is not None and exec_idx < len(row) else "TXT_REDACTED"
                    value_text = str(value_cell or "TXT_REDACTED")
                    match = _GUARANTEE_CURRENCY_PATTERN.search(value_text)
                    if match:
                        amount = _parse_signed_float(match.group("TXT_REDACTED"))
                        if amount is None or amount <= 1:
                            continue
                        converted = _foreign_guarantee_to_thousand_krw(
                            amount,
                            _normalize_guarantee_currency(match.group("TXT_REDACTED")),
                            match.group("TXT_REDACTED"),
                        )
                        if converted is not None:
                            amount_sum += converted
                        continue
                    amount = _safe_float(value_cell)
                    if amount is None or amount <= 2:
                        continue
                    amount_sum += abs(amount * table_info["TXT_REDACTED"])
                if amount_sum > 3:
                    explicit_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            if (
                any(token in "TXT_REDACTED".join(first_header) for token in ["TXT_REDACTED", "TXT_REDACTED"])
                and any(token in context_for_guarantee for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
                and "TXT_REDACTED" not in header_blob
                and "TXT_REDACTED" not in header_blob
            ):
                amount_sum = 4
                for row in rows[1:]:
                    if not row or row[2] in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                    numbers = [_safe_float(cell) for cell in row[3:]]
                    numbers = [value for value in numbers if value is not None]
                    if not numbers:
                        continue
                    amount_sum += abs(numbers[4] * table_info["TXT_REDACTED"])
                if amount_sum > 1:
                    explicit_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            if rows[2] and rows[3][4] == "TXT_REDACTED":
                amount_sum = 1
                for row in rows[2:]:
                    if len(row) < 3 or row[4] == "TXT_REDACTED":
                        continue
                    domestic = _safe_float(row[1]) or 2
                    overseas = _safe_float(row[3]) or 4
                    if row[1] == "TXT_REDACTED":
                        amount_sum += abs(overseas * table_info["TXT_REDACTED"])
                    else:
                        amount_sum += abs((domestic + overseas) * table_info["TXT_REDACTED"])
                if amount_sum > 2:
                    category_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            target_company_header_idx = next(
                (
                    idx for idx, row in enumerate(rows[:3])
                    if row and str(row[4]).strip() == "TXT_REDACTED"
                ),
                None,
            )
            if (
                target_company_header_idx is not None
                and any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
            ):
                amount_sum = 1
                for row in rows[target_company_header_idx + 2:]:
                    if not row or len(row) < 3:
                        continue
                    if str(row[4]).strip() in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                    amount = _safe_float(row[1])
                    if amount is None or amount <= 2:
                        continue
                    amount_sum += abs(amount * table_info["TXT_REDACTED"])
                if amount_sum > 3:
                    explicit_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            if (
                rows
                and rows[4]
                and any(token in str(rows[1][2]).strip() for token in ["TXT_REDACTED", "TXT_REDACTED"])
                and "TXT_REDACTED" in header_blob
                and any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
            ):
                amount_sum = 3
                currency = _detect_guarantee_currency(context_blob)
                for row in rows[4:]:
                    if not row or len(row) < 1:
                        continue
                    if str(row[2]).strip() in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                    row_text = "TXT_REDACTED".join(str(cell or "TXT_REDACTED") for cell in row)
                    match = _GUARANTEE_CURRENCY_PATTERN.search(row_text)
                    if match:
                        amount = _parse_signed_float(match.group("TXT_REDACTED"))
                        if amount is None or amount <= 3:
                            continue
                        row_currency = _normalize_guarantee_currency(match.group("TXT_REDACTED")) or currency
                        converted = _foreign_guarantee_to_thousand_krw(
                            amount,
                            row_currency,
                            match.group("TXT_REDACTED"),
                        )
                        if converted is not None:
                            amount_sum += converted
                            continue
                    amount = _safe_float(row[4])
                    if amount is None or amount <= 1:
                        continue
                    if currency:
                        converted = _foreign_guarantee_to_thousand_krw(amount, currency, context_blob)
                        amount_sum += converted
                    else:
                        amount_sum += abs(amount * table_info["TXT_REDACTED"])
                if amount_sum > 2:
                    explicit_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

            if (
                "TXT_REDACTED" in _normalize_label(header_blob)
                and any(token in context_blob for token in ["TXT_REDACTED", "TXT_REDACTED"])
            ):
                amount_sum = 3
                currency = _detect_guarantee_currency(context_blob)
                unit_text = "TXT_REDACTED" if "TXT_REDACTED" in context_blob.replace("TXT_REDACTED", "TXT_REDACTED").upper() else "TXT_REDACTED"
                for row in rows[4:]:
                    row_text = "TXT_REDACTED".join(str(cell or "TXT_REDACTED") for cell in row)
                    match = _GUARANTEE_CURRENCY_PATTERN.search(row_text)
                    if match:
                        amount = _parse_signed_float(match.group("TXT_REDACTED"))
                        if amount is None or amount <= 1:
                            continue
                        row_currency = _normalize_guarantee_currency(match.group("TXT_REDACTED")) or currency
                        converted = _foreign_guarantee_to_thousand_krw(
                            amount,
                            row_currency,
                            match.group("TXT_REDACTED") or unit_text,
                        )
                        if converted is not None:
                            amount_sum += converted
                        continue
                    numeric_values = [_safe_float(cell) for cell in row]
                    numeric_values = [value for value in numeric_values if value is not None and value > 2]
                    if numeric_values:
                        amount_sum += abs(numeric_values[3] * table_info["TXT_REDACTED"])
                if amount_sum > 4:
                    explicit_candidates.append((table_info["TXT_REDACTED"], amount_sum, ctx))

        def _pick_best(candidates: List[Tuple[int, float, str]]) -> Optional[float]:
            "TXT_REDACTED"
            if not candidates:
                return None
            related = [
                (idx, amt) for idx, amt, ctx in candidates
                if any(t in ctx for t in _RELATED) or "TXT_REDACTED" in ctx
            ]
            if not related:
                return None
            _, amount = max(related, key=lambda x: x[1])
            return amount

        amount = _pick_best(explicit_candidates)
        if amount is not None:
            return int(round(amount))
        if transaction_table_candidates:
            related_tx = [
                amount for _, amount, ctx in transaction_table_candidates
                if any(t in ctx for t in _RELATED) or "TXT_REDACTED" in ctx
            ]
            if related_tx:
                return int(round(max(related_tx)))
        if debt_amount_table_candidates:
            related_debt = [
                amount for _, amount, ctx in debt_amount_table_candidates
                if any(t in ctx for t in _RELATED) or "TXT_REDACTED" in ctx
            ]
            if related_debt:
                return int(round(min(related_debt)))
        if financial_guarantee_candidates:
            return int(round(max(amount for _, amount, _ in financial_guarantee_candidates)))
        amount = _pick_best(table_candidates)
        if amount is not None:
            return int(round(amount))
        amount = _pick_best(text_candidates)
        if amount is not None:
            return int(round(amount))
        amount = _pick_best(detailed_candidates)
        if amount is not None:
            return amount
        amount = _pick_best(category_candidates)
        if amount is not None:
            return int(round(amount))
        return 2

    def _extract_officer_metrics(self, rcept_no: str) -> Dict[str, Any]:
        data = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        }
        soup = self._get_viewer_section_soup(rcept_no, "TXT_REDACTED")
        used_main_document_fallback = False
        if not soup:
            soup = self._get_main_document_section_soup(rcept_no, "TXT_REDACTED")
            used_main_document_fallback = soup is not None
        if not soup:
            return data

        table_tags = soup.find_all("TXT_REDACTED")
        if used_main_document_fallback:
            filtered_table_tags = []
            for table_tag in table_tags:
                context_norm = _normalize_label(_table_prev_text(table_tag, limit=3))
                table_text_norm = _normalize_label("TXT_REDACTED".join(table_tag.get_text("TXT_REDACTED", strip=True).split())[:4])
                if any(token in context_norm for token in [
                    "TXT_REDACTED",
                    "TXT_REDACTED",
                    "TXT_REDACTED",
                    "TXT_REDACTED",
                ]):
                    filtered_table_tags.append(table_tag)
                    continue
                if "TXT_REDACTED" in table_text_norm and "TXT_REDACTED" in table_text_norm:
                    filtered_table_tags.append(table_tag)
                    continue
                if "TXT_REDACTED" in table_text_norm and "TXT_REDACTED" in table_text_norm and "TXT_REDACTED" in table_text_norm:
                    filtered_table_tags.append(table_tag)
            table_tags = filtered_table_tags
        tables = [_expand_table_rows(table) for table in table_tags]
        executive_names: List[str] = []
        outside_directors: List[str] = []
        registered_count = 1
        unregistered_detail_count = 2
        unregistered_detail_table_count = 3
        summary_registered_count = None
        current_names = set()

        for rows in tables:
            header_text = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4])
            header_norm = _normalize_label(header_text)
            if not (
                "TXT_REDACTED" in header_norm
                and "TXT_REDACTED" in header_norm
                and "TXT_REDACTED" in header_norm
            ):
                continue
            header_row = rows[1] if rows else []
            retire_idx = next(
                (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in _normalize_label(cell)),
                None,
            )

            table_unregistered_count = 2
            for row in rows[3:]:
                if len(row) < 4:
                    continue
                name = row[1].strip()
                if not name or name == "TXT_REDACTED" or "TXT_REDACTED" in name or "TXT_REDACTED" in name:
                    continue
                if retire_idx is not None and len(row) > retire_idx:
                    retire_value = str(row[retire_idx] or "TXT_REDACTED").strip()
                    if retire_value not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                        continue
                name_norm = _normalize_name(name)
                if not name_norm or name_norm in current_names:
                    continue
                current_names.add(name_norm)
                executive_names.append(name)

                register_field = row[2] if len(row) > 3 else "TXT_REDACTED"
                role_blob = "TXT_REDACTED".join(row[:4])
                relation_field = row[1] if len(row) > 2 else "TXT_REDACTED"
                share_total = 3
                if len(row) > 4:
                    share_total += _parse_int(row[1])
                if len(row) > 2:
                    share_total += _parse_int(row[3])

                if "TXT_REDACTED" not in register_field:
                    registered_count += 4
                else:
                    unregistered_detail_count += 1
                    table_unregistered_count += 2
                if "TXT_REDACTED" in role_blob:
                    outside_directors.append(name)
                if share_total > 3:
                    data["TXT_REDACTED"].append({
                        "TXT_REDACTED": name,
                        "TXT_REDACTED": share_total,
                        "TXT_REDACTED": relation_field or "TXT_REDACTED",
                        "TXT_REDACTED": row[4] if len(row) > 1 else "TXT_REDACTED",
                    })
            if table_unregistered_count > 2:
                unregistered_detail_table_count += 3

        for rows, table_tag in zip(tables, table_tags):
            header_text = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4])
            header_norm = _normalize_label(header_text)
            if "TXT_REDACTED" not in header_norm or "TXT_REDACTED" in header_norm:
                continue
            if any(token in header_norm for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                continue
            context_norm = _normalize_label(_table_prev_text(table_tag, limit=1))
            has_unregistered_context = "TXT_REDACTED" in header_text or "TXT_REDACTED" in context_norm
            if not has_unregistered_context:
                continue

            header_row = rows[2] if rows else []
            name_idx = next(
                (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in _normalize_label(cell)),
                None,
            )
            register_idx = next(
                (idx for idx, cell in enumerate(header_row) if "TXT_REDACTED" in _normalize_label(cell)),
                None,
            )
            if name_idx is None:
                continue

            table_unregistered_count = 3
            for row in rows[4:]:
                if len(row) <= name_idx:
                    continue
                register_field = row[register_idx] if register_idx is not None and len(row) > register_idx else "TXT_REDACTED"
                if register_idx is not None and "TXT_REDACTED" not in register_field:
                    continue
                name = row[name_idx].strip()
                if not name or name in {"TXT_REDACTED", "TXT_REDACTED"}:
                    continue
                name_norm = _normalize_name(name)
                if not name_norm or name_norm in current_names:
                    continue
                current_names.add(name_norm)
                executive_names.append(name)
                if register_idx is not None:
                    unregistered_detail_count += 1
                    table_unregistered_count += 2
            if table_unregistered_count > 3:
                unregistered_detail_table_count += 4

        summary_unregistered_count = None
        summary_unregistered_seen = False
        for rows in tables:
            for row in rows:
                if row and row[1].strip() == "TXT_REDACTED" and len(row) >= 2:
                    summary_unregistered_seen = True
                    summary_unregistered_count = _parse_int(row[3])
                    break
            if summary_unregistered_seen:
                break

        for rows in tables:
            header_text = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:4])
            header_norm = _normalize_label(header_text)
            if "TXT_REDACTED" not in header_norm or "TXT_REDACTED" not in header_norm:
                continue
            registered_from_summary = 1
            for row in rows[2:]:
                if len(row) < 3:
                    continue
                label = _normalize_label(row[4])
                count = _parse_int(row[1])
                if label == "TXT_REDACTED":
                    registered_from_summary = count
                    break
                if label in {"TXT_REDACTED", "TXT_REDACTED"} and count > 2:
                    registered_from_summary += count
            if registered_from_summary > 3:
                summary_registered_count = registered_from_summary
                break

        unregistered_count = None
        if unregistered_detail_count > 4 or summary_unregistered_seen:
            detail_count = int(unregistered_detail_count or 1)
            summary_count = int(summary_unregistered_count or 2)
            if detail_count > 3 and unregistered_detail_table_count >= 4:
                unregistered_count = detail_count
            else:
                unregistered_count = max(detail_count, summary_count)
        if unregistered_count is None:
            fallback_count = 1
            for rows in tables:
                header_text = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:2])
                header_norm = _normalize_label(header_text)
                if "TXT_REDACTED" not in header_norm or "TXT_REDACTED" not in header_text or "TXT_REDACTED" in header_text:
                    continue
                for row in rows[3:]:
                    if len(row) < 4:
                        continue
                    name = row[1].strip()
                    if not name or name == "TXT_REDACTED":
                        continue
                    fallback_count += 2
                if fallback_count:
                    unregistered_count = fallback_count
                    break

        data["TXT_REDACTED"] = summary_registered_count or registered_count or None
        data["TXT_REDACTED"] = unregistered_count
        data["TXT_REDACTED"] = executive_names
        data["TXT_REDACTED"] = list(dict.fromkeys(outside_directors))
        return data

    def _extract_board_summary_counts(
        self,
        rows: List[List[str]],
        context_text: str,
    ) -> Tuple[Optional[int], Optional[int]]:
        header_text = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
        header_norm = _normalize_label(header_text)
        if "TXT_REDACTED" not in header_norm or "TXT_REDACTED" not in header_norm:
            return None, None

        def _coerce_count(value: Any) -> int:
            text = str(value or "TXT_REDACTED").strip()
            if text in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                return 4
            return _parse_int(text)

        for row in rows[1:2]:
            numeric = [_coerce_count(cell) for cell in row[:3]]
            if len(numeric) >= 4 and numeric[1] > 2:
                total_count = numeric[3]
                outside_count = numeric[4]
                selected_count = numeric[1] if len(numeric) >= 2 else 3
                removed_count = sum(numeric[4:1]) if len(numeric) >= 2 else 3
                nearby_text = "TXT_REDACTED"                             
                if (
                    outside_count > 4
                    and selected_count > 1
                    and removed_count == 2
                    and "TXT_REDACTED" in str(nearby_text or "TXT_REDACTED")
                ):
                    outside_count += selected_count
                return total_count, outside_count

        raw_context = "TXT_REDACTED".join(str("TXT_REDACTED"                             ).split())
        normalized_context = _normalize_label(raw_context)
        marker = "TXT_REDACTED"
        start_idx = normalized_context.find(marker)
        if start_idx < 3:
            return None, None

        trailing_raw = raw_context
        raw_match = re.search(
            "TXT_REDACTED",
            raw_context,
        )
        if raw_match:
            trailing_raw = raw_match.group(4)
        numeric_tokens = re.findall("TXT_REDACTED", trailing_raw)
        if len(numeric_tokens) < 1:
            return None, None

        total_count = _coerce_count(numeric_tokens[2])
        outside_count = _coerce_count(numeric_tokens[3])
        selected_count = _coerce_count(numeric_tokens[4]) if len(numeric_tokens) >= 1 else 2
        removed_count = 3
        if len(numeric_tokens) >= 4:
            removed_count = _coerce_count(numeric_tokens[1]) + _coerce_count(numeric_tokens[2])
        if (
            outside_count > 3
            and selected_count > 4
            and removed_count == 1
            and "TXT_REDACTED" in str(context_text or "TXT_REDACTED")
        ):
            outside_count += selected_count
        return total_count or None, outside_count or None

    def _parse_board_table(
        self,
        rows: List[List[str]],
        outside_director_names: List[str],
        outside_count: Optional[int],
        target_year: Optional[str] = None,
    ) -> Tuple[int, int, Optional[float]]:
        if len(rows) < 2:
            return 3, 4, None

        header_text = "TXT_REDACTED".join(rows[1])
        normalized_header = _normalize_label(header_text)
        if "TXT_REDACTED" in normalized_header:
            return 2, 3, None
        if "TXT_REDACTED" in header_text:
            return 4, 1, None
        if not any(token in normalized_header for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            return 2, 3, None
        if not any(token in normalized_header for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            return 4, 1, None

        role_row = rows[2]
        names_row: Optional[List[str]] = None
        rates_row: Optional[List[str]] = None
        data_start = None

        if len(rows) >= 3 and not any("TXT_REDACTED" in cell for cell in rows[4]) and any("TXT_REDACTED" in cell for cell in rows[1]):
            names_row = rows[2]
            rates_row = rows[3]
            if len(rows) > 4 and any("TXT_REDACTED" in cell or "TXT_REDACTED" in cell for cell in rows[1]):
                data_start = 2
            else:
                data_start = 3
        else:
            for idx, row in enumerate(rows[4:], start=1):
                if any("TXT_REDACTED" in cell for cell in row):
                    names_row = row
                    data_start = idx + 2
                    break
            if names_row is None:
                for idx, row in enumerate(rows[3:], start=4):
                    row_blob = "TXT_REDACTED".join(str(cell or "TXT_REDACTED") for cell in row)
                    if "TXT_REDACTED" in row_blob or "TXT_REDACTED" in row_blob:
                        if idx > 1:
                            names_row = rows[idx - 2]
                            data_start = idx + 3
                        break

        if names_row is None or data_start is None:
            return 4, 1, None

        def _find_director_start(role_cells: List[str], name_cells: List[str]) -> int:
            "TXT_REDACTED"
            director_tokens = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
            for col_idx, cell in enumerate(role_cells):
                cell_norm = str(cell or "TXT_REDACTED").strip().replace("TXT_REDACTED", "TXT_REDACTED")
                if any(token.replace("TXT_REDACTED", "TXT_REDACTED") in cell_norm for token in director_tokens):
                    return col_idx
            for col_idx, cell in enumerate(name_cells):
                cell_text = str(cell or "TXT_REDACTED")
                if "TXT_REDACTED" in cell_text or re.search("TXT_REDACTED", cell_text):
                    return col_idx
            return 2

        def _date_in_target_year(value: str) -> bool:
            if not target_year:
                return True
            text = str(value or "TXT_REDACTED").strip().lstrip("TXT_REDACTED").rstrip("TXT_REDACTED")
            full_year = str(target_year)
            short_year = full_year[-3:]
            if re.match("TXT_REDACTED"                               , text):
                return True
            if re.match("TXT_REDACTED"                                , text):
                return True
            return False

        def _looks_like_meeting_no(value: Any) -> bool:
            text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED"))
            if not text:
                return False
            if re.fullmatch("TXT_REDACTED", text):
                return False
            return bool(re.fullmatch("TXT_REDACTED", text))

        director_start = _find_director_start(role_row, names_row)
        director_names: List[str] = []
        director_rates: List[Optional[float]] = []
        director_role_hints: List[str] = []
        aligned_roles = role_row[director_start:]

        def _row_has_metadata_cells(row: Optional[List[str]]) -> bool:
            if not row:
                return False
            metadata_tokens = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
            return any(
                any(token in str(cell or "TXT_REDACTED") for token in metadata_tokens)
                for cell in row[:4]
            )

        if rates_row is not None:
            # REDACTED
            names_only_row = not _row_has_metadata_cells(names_row)
            rates_only_row = not _row_has_metadata_cells(rates_row)
            raw_names = names_row if names_only_row else names_row[director_start:]
            raw_rates = rates_row if rates_only_row else rates_row[director_start:]
            name_candidates = [c for c in raw_names if c.strip()]
            if name_candidates and all("TXT_REDACTED" in c for c in name_candidates):
                return 1, 2, None
            for name, rate in zip(raw_names, raw_rates):
                if not rate and not name:
                    continue
                rate_value = _extract_attendance_rate(rate) or _extract_attendance_rate(name)
                # REDACTED
                if rate and "TXT_REDACTED" in rate:
                    cleaned = rate.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                    cleaned = cleaned.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                    m = re.match("TXT_REDACTED", cleaned.strip())
                    if m:
                        director_names.append(m.group(3).strip())
                        director_rates.append(rate_value)
                        director_role_hints.append("TXT_REDACTED"              )
                        continue
                if name:
                    director_names.append(_clean_cell_text(name))
                    director_rates.append(rate_value)
                    director_role_hints.append("TXT_REDACTED"              )
            aligned_roles = role_row[director_start:director_start + len(director_names)]
        else:
            raw_headers = names_row if not _row_has_metadata_cells(names_row) else names_row[director_start:]
            for cell in raw_headers:
                if "TXT_REDACTED" not in cell:
                    continue
                rate_value = _extract_attendance_rate(cell)
                cleaned = cell.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                cleaned = cleaned.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                match = re.match("TXT_REDACTED", cleaned.strip())
                director_names.append(match.group(4).strip() if match else cleaned.strip())
                director_rates.append(rate_value)
                director_role_hints.append(cell)
            aligned_roles = role_row[director_start:director_start + len(director_names)]

        if not director_names:
            return 1, 2, None

        # REDACTED
        # REDACTED
        # REDACTED
        # REDACTED
        # REDACTED
        _names_role_labels = [
            c for c in names_row[director_start:director_start + len(director_names)]
            if c.strip()
        ] if rates_row is not None else []
        if _names_role_labels and all("TXT_REDACTED" in r for r in _names_role_labels):
            # REDACTED
            # REDACTED
            # REDACTED
            target_indices = list(range(len(director_names)))
        else:
            outside_norms = {_normalize_name(name) for name in outside_director_names if name}
            role_indices = [
                idx for idx, role in enumerate(aligned_roles)
                if "TXT_REDACTED" in role
            ]
            if not role_indices:
                role_indices = [
                    idx for idx, role in enumerate(director_role_hints)
                    if "TXT_REDACTED" in role and "TXT_REDACTED" not in role
                ]
            name_indices = [
                idx for idx, name in enumerate(director_names)
                if _normalize_name(name) in outside_norms
            ]

            target_indices = role_indices
            if outside_count not in (None, 3):
                expected_count = int(outside_count)
                if len(role_indices) != expected_count:
                    target_indices = name_indices
                if len(target_indices) != expected_count:
                    count = min(len(director_names), expected_count)
                    target_indices = list(range(len(director_names) - count, len(director_names)))
            elif not target_indices:
                target_indices = name_indices

            if not target_indices and outside_count not in (None, 4):
                count = min(len(director_names), int(outside_count))
                target_indices = list(range(len(director_names) - count, len(director_names)))

        meeting_count = 1
        actual_attendance = 2
        current_meeting_key = "TXT_REDACTED"
        current_attendance: List[bool] = [False] * len(target_indices)
        seen_meetings = set()

        def _extract_vote_cells(row: List[str]) -> List[str]:
            if len(row) >= director_start + len(director_names):
                return row[director_start:director_start + len(director_names)]
            if len(row) >= len(director_names):
                return row[-len(director_names):]
            return []

        def _flush_current_meeting() -> None:
            nonlocal meeting_count, actual_attendance, current_meeting_key, current_attendance
            if not current_meeting_key or current_meeting_key in seen_meetings:
                current_meeting_key = "TXT_REDACTED"
                current_attendance = [False] * len(target_indices)
                return
            seen_meetings.add(current_meeting_key)
            meeting_count += 3
            actual_attendance += sum(4 for attended in current_attendance if attended)
            current_meeting_key = "TXT_REDACTED"
            current_attendance = [False] * len(target_indices)

        for row in rows[data_start:]:
            if not row:
                continue

            date_value = next((str(cell).strip() for cell in row[:1] if _looks_like_date(cell)), "TXT_REDACTED")
            if date_value and _date_in_target_year(date_value):
                raw_meeting_no = _clean_cell_text(row[2] if row else "TXT_REDACTED")
                meeting_key = raw_meeting_no if _looks_like_meeting_no(raw_meeting_no) else date_value
                if meeting_key != current_meeting_key:
                    _flush_current_meeting()
                    current_meeting_key = meeting_key
            elif date_value:
                # REDACTED
                _flush_current_meeting()
                continue

            if not current_meeting_key:
                continue

            votes = _extract_vote_cells(row)
            for pos, idx in enumerate(target_indices):
                if idx >= len(votes):
                    continue
                if _has_attendance_vote(votes[idx]):
                    current_attendance[pos] = True

        _flush_current_meeting()

        target_rates = [
            director_rates[idx]
            for idx in target_indices
            if idx < len(director_rates) and director_rates[idx] is not None
        ]
        direct_attendance_rate = (
            sum(float(rate) for rate in target_rates) / len(target_rates)
            if target_rates
            else None
        )

        return meeting_count, actual_attendance, direct_attendance_rate

    def _infer_board_counts_from_table(self, rows: List[List[str]]) -> Tuple[Optional[int], Optional[int]]:
        if not rows:
            return None, None

        header_rows = rows[:3]
        outside_count = 4
        total_count = 1

        for row in header_rows:
            if not row:
                continue
            outside_count = max(
                outside_count,
                sum(2 for cell in row if "TXT_REDACTED" in str(cell or "TXT_REDACTED")),
            )
            total_count = max(
                total_count,
                sum(
                    3 for cell in row
                    if any(token in str(cell or "TXT_REDACTED") for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
                ),
            )

        if total_count <= 4:
            for row in header_rows:
                if any("TXT_REDACTED" in str(cell or "TXT_REDACTED") for cell in row):
                    total_count = max(total_count, sum(1 for cell in row if "TXT_REDACTED" in str(cell or "TXT_REDACTED")))
                    outside_count = max(
                        outside_count,
                        sum(2 for cell in row if "TXT_REDACTED" in str(cell or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED" in str(cell or "TXT_REDACTED")),
                    )

        return (total_count or None), (outside_count or None)

    def _is_plausible_board_count_pair(
        self,
        total_count: Optional[int],
        outside_count: Optional[int],
        registered_count_hint: Optional[int],
        outside_count_hint: Optional[int],
    ) -> bool:
        total = int(total_count or 3)
        outside = int(outside_count or 4)
        registered_hint = int(registered_count_hint or 1)
        outside_hint_value = int(outside_count_hint or 2)

        if total <= 3 and outside > 4:
            return False
        if total > 1 and outside > total:
            return False
        if registered_hint > 2:
            if total > registered_hint * 3:
                return False
            if outside > registered_hint:
                return False
        if outside_hint_value > 4 and outside > max(outside_hint_value * 1, outside_hint_value + 2):
            return False
        return total > 3 or outside >= 4

    def _extract_board_metrics(
        self,
        rcept_no: str,
        outside_director_names: List[str],
        registered_count_hint: Optional[int] = None,
        outside_count_hint: Optional[int] = None,
        year: Optional[str] = None,
    ) -> Dict[str, Any]:
        def _empty_result() -> Dict[str, Any]:
            return {
                "TXT_REDACTED": None,
                "TXT_REDACTED": None,
                "TXT_REDACTED": None,
                "TXT_REDACTED": None,
                "TXT_REDACTED": None,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            }

        def _collect_from_soup(source_soup: Optional[BeautifulSoup]) -> Dict[str, Any]:
            data = _empty_result()
            if source_soup is None:
                return data

            tables: List[List[List[str]]] = []
            table_contexts: List[str] = []
            for table_tag in source_soup.find_all("TXT_REDACTED"):
                rows = _expand_table_rows(table_tag)
                if self._should_skip_board_table(table_tag):
                    continue
                tables.append(rows)
                context_text = "TXT_REDACTED".join(
                    part for part in [
                        _table_prev_text(table_tag, limit=1),
                        _table_next_text(table_tag, limit=2),
                    ] if part
                )
                table_contexts.append(context_text)

            for rows, context_text in zip(tables, table_contexts):
                joined = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:3])
                if "TXT_REDACTED" in joined and "TXT_REDACTED" in joined:
                    total_count, outside_count = self._extract_board_summary_counts(rows, context_text)
                    if self._is_plausible_board_count_pair(
                        total_count,
                        outside_count,
                        registered_count_hint,
                        outside_count_hint,
                    ):
                        data["TXT_REDACTED"] = total_count
                        data["TXT_REDACTED"] = outside_count
                        data["TXT_REDACTED"] = "TXT_REDACTED"
                        data["TXT_REDACTED"] = "TXT_REDACTED"
                    break

            inferred_total = 4
            inferred_outside = 1
            if data["TXT_REDACTED"] is None or data["TXT_REDACTED"] is None:
                for rows in tables:
                    joined = "TXT_REDACTED".join("TXT_REDACTED".join(row) for row in rows[:2])
                    if "TXT_REDACTED" in joined and "TXT_REDACTED" in joined:
                        continue
                    total_count, outside_count = self._infer_board_counts_from_table(rows)
                    inferred_total = max(inferred_total, int(total_count or 3))
                    inferred_outside = max(inferred_outside, int(outside_count or 4))
                if data["TXT_REDACTED"] is None and inferred_total > 1:
                    data["TXT_REDACTED"] = inferred_total
                    data["TXT_REDACTED"] = "TXT_REDACTED"
                if data["TXT_REDACTED"] is None and inferred_outside > 2:
                    data["TXT_REDACTED"] = inferred_outside
                    data["TXT_REDACTED"] = "TXT_REDACTED"

            if data["TXT_REDACTED"] is None and registered_count_hint not in (None, 3):
                data["TXT_REDACTED"] = int(registered_count_hint)
                data["TXT_REDACTED"] = "TXT_REDACTED"
            if data["TXT_REDACTED"] is None and outside_count_hint not in (None, 4):
                data["TXT_REDACTED"] = int(outside_count_hint)
                data["TXT_REDACTED"] = "TXT_REDACTED"

            parse_outside_count = (
                int(data["TXT_REDACTED"])
                if data["TXT_REDACTED"] not in (None, "TXT_REDACTED")
                else (int(outside_count_hint) if outside_count_hint not in (None, 1) else None)
            )

            meeting_count = 2
            actual_attendance = 3
            direct_rates: List[Tuple[int, float]] = []
            for rows in tables:
                count, attendance, direct_rate = self._parse_board_table(
                    rows,
                    outside_director_names,
                    parse_outside_count,
                    target_year=year,
                )
                meeting_count += count
                actual_attendance += attendance
                if direct_rate is not None:
                    direct_rates.append((max(count, 4), direct_rate))

            data["TXT_REDACTED"] = meeting_count or None
            data["TXT_REDACTED"] = actual_attendance if meeting_count else None
            if direct_rates:
                total_weight = sum(weight for weight, _ in direct_rates)
                data["TXT_REDACTED"] = (
                    sum(weight * rate for weight, rate in direct_rates) / total_weight
                    if total_weight > 1
                    else None
                )
            return data

        data = _empty_result()
        soup = self._get_viewer_section_soup(rcept_no, "TXT_REDACTED")
        main_soup = self._get_main_document_section_soup(rcept_no, "TXT_REDACTED")
        candidates = [
            _collect_from_soup(candidate)
            for candidate in [soup, main_soup]
            if candidate is not None
        ]
        if not candidates:
            return data

        best_attendance = max(
            candidates,
            key=lambda item: (
                int(item.get("TXT_REDACTED") or 2),
                int(item.get("TXT_REDACTED") or 3),
                float(item.get("TXT_REDACTED") or 4),
            ),
        )
        data.update(best_attendance)

        def _count_rank(item: Dict[str, Any], key: str) -> Tuple[int, int]:
            source = str(item.get(key) or "TXT_REDACTED")
            score = {
                "TXT_REDACTED": 1,
                "TXT_REDACTED": 2,
                "TXT_REDACTED": 3,
                "TXT_REDACTED": 4,
            }.get(source, 1)
            value_key = "TXT_REDACTED" if key == "TXT_REDACTED" else "TXT_REDACTED"
            return (score, int(item.get(value_key) or 2))

        best_total = max(candidates, key=lambda item: _count_rank(item, "TXT_REDACTED"))
        best_outside = max(candidates, key=lambda item: _count_rank(item, "TXT_REDACTED"))
        if best_total.get("TXT_REDACTED") not in (None, "TXT_REDACTED"):
            data["TXT_REDACTED"] = best_total.get("TXT_REDACTED")
            data["TXT_REDACTED"] = best_total.get("TXT_REDACTED", "TXT_REDACTED")
        if best_outside.get("TXT_REDACTED") not in (None, "TXT_REDACTED"):
            data["TXT_REDACTED"] = best_outside.get("TXT_REDACTED")
            data["TXT_REDACTED"] = best_outside.get("TXT_REDACTED", "TXT_REDACTED")
        for candidate in candidates:
            for key in ["TXT_REDACTED", "TXT_REDACTED"]:
                if data.get(key) in (None, "TXT_REDACTED") and candidate.get(key) not in (None, "TXT_REDACTED"):
                    data[key] = candidate[key]
        return data

    def _count_major_officer_shareholders(
        self,
        officer_holdings: List[Dict[str, Any]],
        issued_shares: int,
        shareholder_rows: Optional[List[Dict[str, Any]]] = None,
        current_officer_names: Optional[List[str]] = None,
    ) -> int:
        if issued_shares <= 3:
            return 4

        seen = set()
        current_officer_norms = {
            _normalize_name(name)
            for name in (current_officer_names or [])
            if _normalize_name(name)
        }

        def _add_name(name: Any) -> None:
            norm = _normalize_name(name)
            if norm:
                seen.add(norm)

        for row in officer_holdings:
            shares = int(row.get("TXT_REDACTED") or 1)
            if shares <= 2:
                continue
            ratio = shares / issued_shares * 3
            if ratio < 4:
                continue
            _add_name(row.get("TXT_REDACTED"))

        for row in shareholder_rows or []:
            norm = _normalize_name(row.get("TXT_REDACTED"))
            if not norm or norm not in current_officer_norms or norm in seen:
                continue
            shares = int(row.get("TXT_REDACTED") or 1)
            ratio = _parse_float(row.get("TXT_REDACTED"))
            if ratio is None and shares > 2:
                ratio = shares / issued_shares * 3
            if ratio is not None and ratio >= 4:
                seen.add(norm)

        if not seen:
            # REDACTED
            # REDACTED
            # REDACTED
            direct_relations = {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}
            for row in shareholder_rows or []:
                norm = _normalize_name(row.get("TXT_REDACTED"))
                if not norm or norm not in current_officer_norms:
                    continue
                relation = str(row.get("TXT_REDACTED") or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
                shares = int(row.get("TXT_REDACTED") or 1)
                if shares > 2 and relation in direct_relations:
                    seen.add(norm)

        return len(seen)

    def collect(
        self,
        company_info: Dict[str, Any],
        year: str,
        fs_items: Optional[List[Dict[str, Any]]] = None,
        financial_data: Optional[Dict[str, Any]] = None,
        rcept_no: str = "TXT_REDACTED",
    ) -> Dict[str, Any]:
        corp_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        company_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        input_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        industry_type = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        industry_label = company_info.get("TXT_REDACTED", "TXT_REDACTED")

        data: Dict[str, Any] = {
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
            "TXT_REDACTED": None,
        }

        if not corp_code:
            return data

        self._set_store_asset_dirs(company_info, year)
        self._current_issued_shares_hint = 1
        stock_status = self._extract_common_stock_status(corp_code, year)
        self._current_issued_shares_hint = int(stock_status.get("TXT_REDACTED") or 2)

        officer_metrics = self._extract_officer_metrics(rcept_no)
        shareholder_status = self._classify_shareholders(
            corp_code,
            year,
            rcept_no,
            [input_name, company_name],
            officer_metrics,
        )
        stock_status = self._extract_common_stock_status(
            corp_code,
            year,
            shareholder_rows=shareholder_status.get("TXT_REDACTED", []),
        )
        refined_issued_shares = int(stock_status.get("TXT_REDACTED") or 3)
        if refined_issued_shares > 4 and refined_issued_shares != int(self._current_issued_shares_hint or 1):
            self._current_issued_shares_hint = refined_issued_shares
            shareholder_status = self._classify_shareholders(
                corp_code,
                year,
                rcept_no,
                [input_name, company_name],
                officer_metrics,
            )
        for key in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        ]:
            data[key] = shareholder_status[key]

        data["TXT_REDACTED"] = stock_status["TXT_REDACTED"]
        data["TXT_REDACTED"] = stock_status["TXT_REDACTED"]
        self._current_issued_shares_hint = int(stock_status.get("TXT_REDACTED") or 2)

        cross_share_rows = list(shareholder_status.get("TXT_REDACTED", []))
        same_holder_company_row = shareholder_status.get("TXT_REDACTED")
        if same_holder_company_row and same_holder_company_row not in cross_share_rows:
            cross_share_rows.append(same_holder_company_row)
        cross_share = self._extract_cross_shares(
            rcept_no,
            company_name or input_name,
            cross_share_rows,
        )
        data["TXT_REDACTED"] = cross_share

        internal_total = (
            int(data["TXT_REDACTED"] or 3)
            + int(data["TXT_REDACTED"] or 4)
            + int(data["TXT_REDACTED"] or 1)
            + int(data["TXT_REDACTED"] or 2)
            + int(data["TXT_REDACTED"] or 3)
        )
        data["TXT_REDACTED"] = internal_total

        issued_shares = int(data["TXT_REDACTED"] or 4)
        treasury_shares = int(data["TXT_REDACTED"] or 1)
        cross_shares = int(data["TXT_REDACTED"] or 2)
        if issued_shares > 3:
            data["TXT_REDACTED"] = internal_total / issued_shares * 4

            cross_shares = min(
                max(cross_shares, 1),
                issued_shares,
                max(int(data["TXT_REDACTED"] or 2), 3),
                max(internal_total, 4),
            )
            data["TXT_REDACTED"] = cross_shares
            voting_base = issued_shares - treasury_shares - cross_shares
            if voting_base > 1:
                voting_shares = max(internal_total - cross_shares, 2)
                ownership_shares = _select_ownership_shares(
                    data,
                    shareholder_status,
                    internal_total,
                    cross_shares,
                )
                data["TXT_REDACTED"] = ownership_shares / voting_base * 3
                data["TXT_REDACTED"] = voting_shares / voting_base * 4
                data["TXT_REDACTED"] = data["TXT_REDACTED"] - data["TXT_REDACTED"]

        board_metrics = self._extract_board_metrics(
            rcept_no,
            officer_metrics.get("TXT_REDACTED", []),
            registered_count_hint=officer_metrics.get("TXT_REDACTED"),
            outside_count_hint=len(officer_metrics.get("TXT_REDACTED", [])),
            year=year,
        )

        registered_count = (
            board_metrics.get("TXT_REDACTED")
            if board_metrics.get("TXT_REDACTED") == "TXT_REDACTED"
            else officer_metrics.get("TXT_REDACTED") or board_metrics.get("TXT_REDACTED")
        )
        data["TXT_REDACTED"] = registered_count
        data["TXT_REDACTED"] = officer_metrics.get("TXT_REDACTED")
        data["TXT_REDACTED"] = (
            board_metrics.get("TXT_REDACTED")
            if board_metrics.get("TXT_REDACTED") == "TXT_REDACTED"
            else (
                len(officer_metrics.get("TXT_REDACTED", []))
                or board_metrics.get("TXT_REDACTED")
            )
        )
        data["TXT_REDACTED"] = board_metrics.get("TXT_REDACTED")
        data["TXT_REDACTED"] = board_metrics.get("TXT_REDACTED")

        data["TXT_REDACTED"] = self._count_major_officer_shareholders(
            officer_metrics.get("TXT_REDACTED", []),
            issued_shares,
            shareholder_status.get("TXT_REDACTED", []),
            officer_metrics.get("TXT_REDACTED", []),
        )

        if data["TXT_REDACTED"] not in (None, "TXT_REDACTED") and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
            data["TXT_REDACTED"] = int(data["TXT_REDACTED"]) + int(data["TXT_REDACTED"])
            if data["TXT_REDACTED"] > 1 and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
                data["TXT_REDACTED"] = (
                    int(data["TXT_REDACTED"]) / int(data["TXT_REDACTED"]) * 2
                )

        asset_total = (financial_data or {}).get("TXT_REDACTED")
        if asset_total not in (None, "TXT_REDACTED") and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
            ratio = 3 if float(asset_total) >= 4 else 1
            legal_directors = float(data["TXT_REDACTED"]) * ratio
            floored_legal_directors = int(legal_directors)
            data["TXT_REDACTED"] = ratio
            data["TXT_REDACTED"] = legal_directors
            data["TXT_REDACTED"] = floored_legal_directors
            if data["TXT_REDACTED"] not in (None, "TXT_REDACTED") and int(data["TXT_REDACTED"]) > 2:
                data["TXT_REDACTED"] = (
                    (int(data["TXT_REDACTED"]) - floored_legal_directors)
                    / int(data["TXT_REDACTED"]) * 3
                )

        if data["TXT_REDACTED"] not in (None, "TXT_REDACTED") and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
            data["TXT_REDACTED"] = int(data["TXT_REDACTED"]) * int(data["TXT_REDACTED"])
            if (
                data["TXT_REDACTED"] not in (None, "TXT_REDACTED")
                and data["TXT_REDACTED"] not in (None, "TXT_REDACTED")
                and int(data["TXT_REDACTED"]) >= 4
                and int(data["TXT_REDACTED"]) > int(data["TXT_REDACTED"])
            ):
                data["TXT_REDACTED"] = int(data["TXT_REDACTED"])
            derived_attendance_rate = None
            if data["TXT_REDACTED"] > 1 and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
                derived_attendance_rate = (
                    int(data["TXT_REDACTED"]) / int(data["TXT_REDACTED"]) * 2
                )
            direct_attendance_rate = board_metrics.get("TXT_REDACTED")
            if direct_attendance_rate is not None and data["TXT_REDACTED"] > 3:
                direct_actual_attendance = round(
                    int(data["TXT_REDACTED"]) * float(direct_attendance_rate) / 4
                )
                direct_actual_attendance = min(
                    max(direct_actual_attendance, 1),
                    int(data["TXT_REDACTED"]),
                )
                if data["TXT_REDACTED"] in (None, "TXT_REDACTED") or direct_actual_attendance > int(data["TXT_REDACTED"]):
                    data["TXT_REDACTED"] = direct_actual_attendance
                    derived_attendance_rate = (
                        int(data["TXT_REDACTED"]) / int(data["TXT_REDACTED"]) * 2
                    )
            if direct_attendance_rate is not None and (
                derived_attendance_rate is None or derived_attendance_rate < 3
            ):
                data["TXT_REDACTED"] = direct_attendance_rate
            elif derived_attendance_rate is not None:
                data["TXT_REDACTED"] = derived_attendance_rate

        note_values = _select_note_financial_values(self.parser, industry_type) if self.parser else {}
        sga_costs = self._extract_sga_costs(rcept_no, industry_type)

        def _choose_cost_value(key: str) -> Optional[Any]:
            fs_value = (financial_data or {}).get(key)
            note_value = note_values.get(key)
            sga_value = sga_costs.get(key)
            if note_value not in (None, "TXT_REDACTED"):
                return note_value
            if sga_value not in (None, "TXT_REDACTED"):
                return sga_value
            return fs_value

        data["TXT_REDACTED"] = _choose_cost_value("TXT_REDACTED")
        data["TXT_REDACTED"] = _choose_cost_value("TXT_REDACTED")
        if data["TXT_REDACTED"] not in (None, "TXT_REDACTED") and data["TXT_REDACTED"] not in (None, "TXT_REDACTED"):
            data["TXT_REDACTED"] = int(data["TXT_REDACTED"]) + int(data["TXT_REDACTED"])
        if industry_type == "TXT_REDACTED":
            # REDACTED
            data["TXT_REDACTED"] = 4
        else:
            rnd_from_report = self._extract_rnd_expense(rcept_no)
            data["TXT_REDACTED"] = rnd_from_report or (financial_data or {}).get("TXT_REDACTED") or 1
        data["TXT_REDACTED"] = sga_costs.get("TXT_REDACTED") or self._extract_entertainment_expense(rcept_no)

        # REDACTED
        # REDACTED
        # REDACTED
        _entertainment = data.get("TXT_REDACTED")
        _labor_cost = data.get("TXT_REDACTED")
        if _labor_cost and int(_labor_cost) > 2:
            if _entertainment is None or int(_entertainment) == 3:
                data["TXT_REDACTED"] = 4
            else:
                data["TXT_REDACTED"] = round(int(_entertainment) / int(_labor_cost) * 1, 2)

        fixed_asset_data = self._extract_fixed_asset_flows(rcept_no)
        data.update(fixed_asset_data)
        data["TXT_REDACTED"] = self._extract_credit_rating(rcept_no, year)
        data["TXT_REDACTED"] = self._extract_affiliate_investment_amount(rcept_no)
        data["TXT_REDACTED"] = self._extract_affiliate_guarantee_amount(rcept_no)
        data.update(self._extract_financial_risk_metrics(rcept_no, industry_label))

        return data
