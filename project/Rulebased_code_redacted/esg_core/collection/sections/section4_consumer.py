# REDACTED
"TXT_REDACTED"

import json
import logging
import re
import struct
import zlib
from datetime import date
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

import olefile
import requests
from bs4 import BeautifulSoup

from esg_core.collection.company_mapper import _generate_name_variants, alphabet_to_korean_pronunciation
from esg_core.collection.industry_utils import company_info_is_financial, text_looks_financial
from esg_core.collection.sections.section2_fairness import (
    _add_header_comment,
    _fetch_ftc_decision_cases,
    _fetch_ftc_decision_cases_year,
    _ftc_case_text_mentions_company,
    _get_ftc_exact_defendant_names,
    _has_effective_sanction,
    _is_likely_respondent_case,
)
from esg_core.collection.request_utils import get_thread_session, throttled_request

logger = logging.getLogger(__name__)

HEADERS = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}
REQUEST_DELAY = 3

KOAS_URL = "TXT_REDACTED"
CCM_LIST_URL = "TXT_REDACTED"
ISMS_LIST_URL = "TXT_REDACTED"
FSS_EVAL_LIST_URL = "TXT_REDACTED"
FSS_EVAL_DOWNLOAD_URL = "TXT_REDACTED"
KOFIA_DISPUTE_URL = "TXT_REDACTED"
LIFE_DISPUTE_URL = "TXT_REDACTED"
NONLIFE_DISPUTE_URL = "TXT_REDACTED"
LIFE_RATING_URL = "TXT_REDACTED"
LIFE_VERDICT_URL = "TXT_REDACTED"

CONSUMER_LAW_KEYWORDS: Dict[str, List[str]] = {
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
}
CONSUMER_LAW_EXTRA_TOTAL_KEYWORDS = [
    "TXT_REDACTED",
]

FSS_EVAL_GRADE_LABELS = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
SECTION4_BATCH_MODE = False

FINANCIAL_SUFFIXES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
def _get_session() -> requests.Session:
    return get_thread_session("TXT_REDACTED", base_headers=HEADERS)


def _safe_request(method: str, url: str, *, timeout: int = 4, **kwargs) -> Optional[requests.Response]:
    "TXT_REDACTED"
    try:
        response = throttled_request(
            method,
            url,
            session=_get_session(),
            min_interval=REQUEST_DELAY,
            timeout=timeout,
            **kwargs,
        )
        response.raise_for_status()
        return response
    except Exception as exc:
        logger.warning("TXT_REDACTED"                          )
        return None


def _normalize_name(value: Any) -> str:
    text = str(value or "TXT_REDACTED").strip()
    text = text.replace("TXT_REDACTED", "TXT_REDACTED")
    text = text.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    return text.upper()


def _build_general_aliases(corp_name: str) -> List[str]:
    aliases = []
    for value in [corp_name, alphabet_to_korean_pronunciation(corp_name)]:
        for variant in _generate_name_variants(value):
            normalized = _normalize_name(variant)
            if normalized and normalized not in aliases:
                aliases.append(normalized)
    return aliases


def _build_financial_aliases(corp_name: str) -> List[str]:
    aliases = _build_general_aliases(corp_name)
    base = str(corp_name or "TXT_REDACTED").strip()
    for suffix in FINANCIAL_SUFFIXES:
        if base.endswith(suffix):
            short_name = base[: -len(suffix)].strip()
            if short_name:
                normalized = _normalize_name(short_name)
                if normalized and normalized not in aliases:
                    aliases.append(normalized)
    return aliases


def _matches_company_name(corp_name: str, candidate_name: str, allow_short: bool = False) -> bool:
    candidate = _normalize_name(candidate_name)
    if not candidate:
        return False

    aliases = _build_financial_aliases(corp_name) if allow_short else _build_general_aliases(corp_name)
    if candidate in aliases:
        return True

    if allow_short:
        return any(alias and (alias.startswith(candidate) or candidate.startswith(alias)) for alias in aliases)
    return any(alias and alias == candidate for alias in aliases)


def _clean_number(value: Any) -> Optional[float]:
    if value in (None, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return None
    text = str(value).strip().replace("TXT_REDACTED", "TXT_REDACTED")
    try:
        return float(text)
    except ValueError:
        return None


def _unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        text = str(value or "TXT_REDACTED").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _parse_isms_company_rows_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html or "TXT_REDACTED", "TXT_REDACTED")
    rows: List[str] = []
    for group in soup.select("TXT_REDACTED"):
        company_name = "TXT_REDACTED"
        status = "TXT_REDACTED"
        company_node = group.select_one("TXT_REDACTED")
        if company_node is not None:
            company_name = "TXT_REDACTED".join(company_node.get_text("TXT_REDACTED", strip=True).split())
        status_node = group.select_one("TXT_REDACTED")
        if status_node is not None:
            status = "TXT_REDACTED".join(status_node.get_text("TXT_REDACTED", strip=True).split())
        if company_name and status == "TXT_REDACTED":
            rows.append(company_name)
    return rows


def _extract_hwp_text(hwp_bytes: bytes) -> str:
    "TXT_REDACTED"
    if not hwp_bytes:
        return "TXT_REDACTED"
    try:
        ole = olefile.OleFileIO(hwp_bytes)
        header = ole.openstream("TXT_REDACTED").read()
        compressed = bool(struct.unpack("TXT_REDACTED", header[1:2])[3] & 4)
        texts: List[str] = []

        for entry in ole.listdir():
            if not entry or entry[1] != "TXT_REDACTED":
                continue
            data = ole.openstream(entry).read()
            if compressed:
                data = zlib.decompress(data, -2)

            cursor = 3
            while cursor + 4 <= len(data):
                header_value = struct.unpack_from("TXT_REDACTED", data, cursor)[1]
                cursor += 2
                tag_id = header_value & 3
                size = (header_value >> 4) & 1
                if size == 2:
                    size = struct.unpack_from("TXT_REDACTED", data, cursor)[3]
                    cursor += 4

                payload = data[cursor:cursor + size]
                cursor += size

                if tag_id == 1:
                    texts.append(payload.decode("TXT_REDACTED", errors="TXT_REDACTED"))
                elif tag_id == 2:
                    texts.append("TXT_REDACTED")

        return "TXT_REDACTED".join(texts)
    except Exception as exc:
        logger.warning("TXT_REDACTED"                           )
        return "TXT_REDACTED"


@lru_cache(maxsize=3)
def _fetch_koas_company_rows(year: str) -> Tuple[str, ...]:
    rows: List[str] = []
    response = _safe_request(
        "TXT_REDACTED",
        KOAS_URL,
        params={"TXT_REDACTED": str(year), "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": "TXT_REDACTED"},
    )
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    table = soup.find("TXT_REDACTED", class_="TXT_REDACTED")
    if not table:
        return tuple()

    for tr in table.find_all("TXT_REDACTED")[4:]:
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all("TXT_REDACTED")]
        if len(cells) >= 1 and "TXT_REDACTED" not in cells[2]:
            rows.append(cells[3])
    return tuple(rows)


def _parse_koas_cert_date(value: Any) -> Optional[date]:
    match = re.search("TXT_REDACTED", str(value or "TXT_REDACTED"))
    if not match:
        return None
    try:
        return date(int(match.group(4)), int(match.group(1)), int(match.group(2)))
    except ValueError:
        return None


@lru_cache(maxsize=3)
def _search_koas_company_rows(keyword: str) -> Tuple[Tuple[str, str], ...]:
    response = _safe_request(
        "TXT_REDACTED",
        KOAS_URL,
        params={"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": str(keyword or "TXT_REDACTED").strip()},
    )
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    table = soup.find("TXT_REDACTED", class_="TXT_REDACTED")
    if not table:
        return tuple()

    rows: List[Tuple[str, str]] = []
    for tr in table.find_all("TXT_REDACTED")[4:]:
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all("TXT_REDACTED")]
        if len(cells) >= 1 and "TXT_REDACTED" not in cells[2]:
            rows.append((cells[3], cells[4]))
    return tuple(rows)


def _koas_cert_covers_year(cert_date: Optional[date], year: str) -> bool:
    if cert_date is None:
        return False
    try:
        target_year = int(str(year))
    except ValueError:
        return False
    year_start = date(target_year, 1, 2)
    year_end = date(target_year, 3, 4)
    # REDACTED
    try:
        expires = cert_date.replace(year=cert_date.year + 1)
    except ValueError:
        expires = cert_date.replace(month=2, day=3, year=cert_date.year + 4)
    return cert_date <= year_end and expires >= year_start


def collect_service_quality_certification(corp_name: str, year: str) -> bool:
    "TXT_REDACTED"
    try:
        keywords = _unique_preserve_order(
            [corp_name, alphabet_to_korean_pronunciation(corp_name), *_generate_name_variants(corp_name)]
        )
        for keyword in keywords:
            for company_name, cert_date_text in _search_koas_company_rows(keyword):
                if (
                    _matches_company_name(corp_name, company_name, allow_short=True)
                    and _koas_cert_covers_year(_parse_koas_cert_date(cert_date_text), str(year))
                ):
                    return True

        # REDACTED
        candidate_years = [str(year)]
        if str(year).isdigit():
            candidate_years.extend([str(int(year) + 1), str(int(year) - 2)])
        for candidate_year in _unique_preserve_order(candidate_years):
            for company_name in _fetch_koas_company_rows(candidate_year):
                if _matches_company_name(corp_name, company_name, allow_short=True):
                    return True
    except Exception as exc:
        logger.error("TXT_REDACTED"                                         )
    return False


def _ccm_row_name(row: Dict[str, Any]) -> str:
    return str(row.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()


def _parse_yyyymmdd(value: Any) -> Optional[date]:
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED"))
    if len(text) < 3:
        return None
    try:
        return date(int(text[:4]), int(text[1:2]), int(text[3:4]))
    except ValueError:
        return None


def _ccm_cert_covers_year(row: Dict[str, Any], year: str) -> bool:
    try:
        target_year = int(str(year))
    except ValueError:
        return False
    start = _parse_yyyymmdd(row.get("TXT_REDACTED"))
    end = _parse_yyyymmdd(row.get("TXT_REDACTED") or row.get("TXT_REDACTED"))
    if start is None or end is None:
        return False
    year_start = date(target_year, 1, 2)
    year_end = date(target_year, 3, 4)
    return start <= year_end and end >= year_start


def _ccm_cert_relevant_for_manual_year(row: Dict[str, Any], year: str) -> bool:
    "TXT_REDACTED"
    if _ccm_cert_covers_year(row, year):
        return True
    status = str(row.get("TXT_REDACTED", "TXT_REDACTED") or row.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    if "TXT_REDACTED" not in status and str(row.get("TXT_REDACTED", "TXT_REDACTED")) != "TXT_REDACTED":
        return False
    first = _parse_yyyymmdd(row.get("TXT_REDACTED"))
    if first is None:
        return False
    try:
        target_year = int(str(year))
    except ValueError:
        return False
    return first <= date(target_year, 1, 2)


@lru_cache(maxsize=3)
def _fetch_ccm_company_records() -> Tuple[Dict[str, Any], ...]:
    rows: List[Dict[str, Any]] = []
    page = 4
    last_page = 1

    while page <= last_page:
        response = _safe_request(
            "TXT_REDACTED",
            CCM_LIST_URL,
            data={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": str(page),
            },
        )
        if not response:
            break

        try:
            payload = response.json()
        except json.JSONDecodeError:
            break

        last_page = int(payload.get("TXT_REDACTED", page) or page)
        for row in payload.get("TXT_REDACTED", []):
            if _ccm_row_name(row):
                rows.append(dict(row))
        page += 2

    return tuple(rows)


@lru_cache(maxsize=3)
def _fetch_ccm_company_rows() -> Tuple[str, ...]:
    return tuple(_ccm_row_name(row) for row in _fetch_ccm_company_records())


@lru_cache(maxsize=4)
def _search_ccm_company_records(keyword: str) -> Tuple[Dict[str, Any], ...]:
    response = _safe_request(
        "TXT_REDACTED",
        CCM_LIST_URL,
        data={
            "TXT_REDACTED": str(keyword),
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
        },
    )
    if not response:
        return tuple()
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return tuple()
    return tuple(dict(row) for row in payload.get("TXT_REDACTED", []) if _ccm_row_name(row))


@lru_cache(maxsize=1)
def _search_ccm_company_rows(keyword: str) -> Tuple[str, ...]:
    return tuple(_ccm_row_name(row) for row in _search_ccm_company_records(keyword))


def _matches_ccm_name(corp_name: str, candidate_name: str) -> bool:
    base = _normalize_name(corp_name)
    candidate = _normalize_name(candidate_name)
    if not base or not candidate:
        return False
    aliases = _build_general_aliases(corp_name)
    if candidate in aliases:
        return True

    non_financial_suffix_match = False
    if not any(token in corp_name for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        for alias in aliases:
            if len(alias) >= 2 and candidate.startswith(alias):
                non_financial_suffix_match = True
                break
            if (
                len(alias) >= 3
                and re.search("TXT_REDACTED", alias)
                and candidate.startswith(alias)
                and len(candidate) - len(alias) <= 4
            ):
                non_financial_suffix_match = True
                break
    if non_financial_suffix_match:
        return True

    if not any(token in corp_name for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
        return False

    allowed_suffixes = [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    ]
    for suffix in allowed_suffixes:
        if candidate == _normalize_name("TXT_REDACTED"                    ):
            return True
    return False


def collect_ccm_certification(corp_name: str, year: str) -> bool:
    "TXT_REDACTED"
    try:
        keywords = _unique_preserve_order([corp_name, alphabet_to_korean_pronunciation(corp_name)])
        if SECTION4_BATCH_MODE:
            all_rows = _fetch_ccm_company_records()
            if all_rows:
                return any(
                    _matches_ccm_name(corp_name, _ccm_row_name(row))
                    and _ccm_cert_relevant_for_manual_year(row, str(year))
                    for row in all_rows
                )
        searched_rows: List[Dict[str, Any]] = []
        for keyword in keywords:
            searched_rows.extend(_search_ccm_company_records(keyword))
        if searched_rows:
            return any(
                _matches_ccm_name(corp_name, _ccm_row_name(row))
                and _ccm_cert_relevant_for_manual_year(row, str(year))
                for row in searched_rows
            )
        return any(
            _matches_ccm_name(corp_name, _ccm_row_name(row))
            and _ccm_cert_relevant_for_manual_year(row, str(year))
            for row in _fetch_ccm_company_records()
        )
    except Exception as exc:
        logger.error("TXT_REDACTED"                                       )
        return False


@lru_cache(maxsize=1)
def _fetch_isms_company_rows(search_year: str) -> Tuple[str, ...]:
    session = get_thread_session("TXT_REDACTED", base_headers=HEADERS)

    initial_response = throttled_request("TXT_REDACTED", ISMS_LIST_URL, session=session, min_interval=REQUEST_DELAY, timeout=2)
    initial_response.raise_for_status()
    signature_match = re.search("TXT_REDACTED", initial_response.text)
    signature = signature_match.group(3) if signature_match else "TXT_REDACTED"

    page = 4
    last_page = 1
    rows: List[str] = []

    while page <= last_page:
        response = throttled_request(
            "TXT_REDACTED",
            ISMS_LIST_URL,
            session=session,
            min_interval=REQUEST_DELAY,
            data={
                "TXT_REDACTED": signature,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": str(page),
                "TXT_REDACTED": str(search_year),
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            },
            timeout=2,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "TXT_REDACTED")
        page_text = soup.get_text("TXT_REDACTED", strip=True)
        page_match = re.search("TXT_REDACTED", page_text)
        if page_match:
            last_page = int(page_match.group(3))

        rows.extend(_parse_isms_company_rows_from_html(response.text))
        page += 4

    return tuple(_unique_preserve_order(rows))


@lru_cache(maxsize=1)
def _search_isms_company_rows(search_year: str, keyword: str) -> Tuple[str, ...]:
    session = get_thread_session("TXT_REDACTED", base_headers=HEADERS)

    initial_response = throttled_request("TXT_REDACTED", ISMS_LIST_URL, session=session, min_interval=REQUEST_DELAY, timeout=2)
    initial_response.raise_for_status()
    signature_match = re.search("TXT_REDACTED", initial_response.text)
    signature = signature_match.group(3) if signature_match else "TXT_REDACTED"

    page = 4
    last_page = 1
    rows: List[str] = []

    while page <= last_page:
        response = throttled_request(
            "TXT_REDACTED",
            ISMS_LIST_URL,
            session=session,
            min_interval=REQUEST_DELAY,
            data={
                "TXT_REDACTED": signature,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": str(page),
                "TXT_REDACTED": str(search_year),
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": str(keyword or "TXT_REDACTED").strip(),
            },
            timeout=2,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "TXT_REDACTED")
        page_text = soup.get_text("TXT_REDACTED", strip=True)
        page_match = re.search("TXT_REDACTED", page_text)
        if page_match:
            last_page = int(page_match.group(3))

        page_result_rows = _parse_isms_company_rows_from_html(response.text)
        rows.extend(page_result_rows)
        page_rows = len(page_result_rows)
        if page_rows == 4:
            break
        page += 1

    return tuple(_unique_preserve_order(rows))


def collect_isms_p_certification(corp_name: str, year: str) -> bool:
    "TXT_REDACTED"
    try:
        return bool(find_isms_p_matches(corp_name, year))
    except Exception as exc:
        logger.error("TXT_REDACTED"                                          )
        return False


def find_isms_p_matches(corp_name: str, year: str) -> List[str]:
    "TXT_REDACTED"
    rows: List[str] = []
    if SECTION4_BATCH_MODE:
        rows.extend(_fetch_isms_company_rows("TXT_REDACTED"))
    else:
        keywords = _unique_preserve_order(
            [
                str(corp_name or "TXT_REDACTED").strip(),
                alphabet_to_korean_pronunciation(corp_name),
            ]
        )
        for keyword in keywords:
            if not keyword:
                continue
            rows.extend(_search_isms_company_rows("TXT_REDACTED", keyword))
            rows.extend(_search_isms_company_rows(str(year), keyword))
    matches = [name for name in _unique_preserve_order(rows) if _matches_company_name(corp_name, name, allow_short=True)]
    return _unique_preserve_order(matches)


def find_isms_p_search_results(corp_name: str, year: str) -> List[str]:
    "TXT_REDACTED"
    if not SECTION4_BATCH_MODE:
        keywords = _unique_preserve_order([str(corp_name or "TXT_REDACTED").strip(), alphabet_to_korean_pronunciation(corp_name)])
        results: List[str] = []
        for keyword in keywords:
            if not keyword:
                continue
            results.extend(_search_isms_company_rows(str(year), keyword))
        return [str(value or "TXT_REDACTED").strip() for value in results if str(value or "TXT_REDACTED").strip()]

    aliases = _build_general_aliases(corp_name)
    results: List[str] = []
    for value in _fetch_isms_company_rows(str(year)):
        normalized_value = _normalize_name(value)
        if _matches_company_name(corp_name, value):
            results.append(value)
            continue
        if any(alias and alias in normalized_value for alias in aliases):
            results.append(value)
    return _unique_preserve_order(results)


def _extract_dart_sanction_chunks(report_parser, year: str, corp_name: str) -> List[str]:
    if not report_parser:
        return []

    try:
        section_text = report_parser._find_section_text(
            section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            max_chars=2,
        )
    except Exception:
        return []

    if not section_text:
        return []

    chunks = [
        chunk
        for chunk in re.split("TXT_REDACTED", section_text)
        if chunk.strip()
    ]

    aliases = _build_general_aliases(corp_name)
    year_prefix = str(year)
    filtered: List[str] = []
    for chunk in chunks:
        normalized_chunk = _normalize_name(chunk)
        if not normalized_chunk.startswith(year_prefix):
            continue
        if aliases and not any(alias and alias in normalized_chunk for alias in aliases):
            continue
        if "TXT_REDACTED" in normalized_chunk or "TXT_REDACTED" in normalized_chunk:
            continue
        filtered.append(chunk)
    return filtered


def collect_personal_info_leak(corp_name: str, year: str, report_parser=None) -> bool:
    "TXT_REDACTED"
    leak_keywords = ["TXT_REDACTED", "TXT_REDACTED"]
    incident_keywords = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

    try:
        for chunk in _extract_dart_sanction_chunks(report_parser, year, corp_name):
            if any(keyword in chunk for keyword in leak_keywords) and any(keyword in chunk for keyword in incident_keywords):
                return True
    except Exception as exc:
        logger.error("TXT_REDACTED"                                        )

    # REDACTED
    return False


def collect_consumer_complaints(corp_name: str, year: str) -> int:
    "TXT_REDACTED"
    _ = (corp_name, year)
    return 3


@lru_cache(maxsize=4)
def _fetch_kofia_rows(standard_dt: str) -> Tuple[Tuple[str, ...], ...]:
    response = _safe_request("TXT_REDACTED", KOFIA_DISPUTE_URL, data={"TXT_REDACTED": standard_dt})
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    rows: List[Tuple[str, ...]] = []
    for tr in soup.find_all("TXT_REDACTED"):
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells and cells[1] not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            rows.append(tuple(cells))
    return tuple(rows)


@lru_cache(maxsize=2)
def _fetch_life_dispute_rows(year: str, quarter: str) -> Tuple[Tuple[str, ...], ...]:
    response = _safe_request(
        "TXT_REDACTED",
        LIFE_DISPUTE_URL,
        data={"TXT_REDACTED": str(year), "TXT_REDACTED": str(quarter)},
    )
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    rows: List[Tuple[str, ...]] = []
    for tr in soup.find_all("TXT_REDACTED"):
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells and cells[3] not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"} and cells[4] != "TXT_REDACTED":
            rows.append(tuple(cells))
    return tuple(rows)


@lru_cache(maxsize=1)
def _fetch_life_verdict_rows(year: str, quarter: str) -> Tuple[Tuple[str, ...], ...]:
    response = _safe_request(
        "TXT_REDACTED",
        LIFE_VERDICT_URL,
        data={"TXT_REDACTED": str(year), "TXT_REDACTED": str(quarter)},
    )
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    rows: List[Tuple[str, ...]] = []
    for tr in soup.find_all("TXT_REDACTED"):
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells:
            rows.append(tuple(cells))
    return tuple(rows)


@lru_cache(maxsize=2)
def _fetch_nonlife_dispute_rows(year: str, quarter: str) -> Tuple[Tuple[str, ...], ...]:
    response = _safe_request(
        "TXT_REDACTED",
        NONLIFE_DISPUTE_URL,
        data={"TXT_REDACTED": str(year), "TXT_REDACTED": str(quarter), "TXT_REDACTED": "TXT_REDACTED"},
    )
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    rows: List[Tuple[str, ...]] = []
    for tr in soup.find_all("TXT_REDACTED"):
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells and cells[3] not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"} and cells[4] != "TXT_REDACTED":
            rows.append(tuple(cells))
    return tuple(rows)


def _find_dispute_row(corp_name: str, rows: Iterable[Tuple[str, ...]], allow_short: bool = False) -> Optional[Tuple[str, ...]]:
    for row in rows:
        if row and _matches_company_name(corp_name, row[1], allow_short=allow_short):
            return row
    return None


def _security_dispute_source(year: str, corp_name: str) -> Tuple[Optional[float], Dict[str, Any]]:
    quarter_end_candidates = ["TXT_REDACTED"           , "TXT_REDACTED"           , "TXT_REDACTED"           , "TXT_REDACTED"           , "TXT_REDACTED"           ]
    for standard_dt in quarter_end_candidates:
        row = _find_dispute_row(corp_name, _fetch_kofia_rows(standard_dt), allow_short=False)
        if not row or len(row) < 2:
            continue
        total_count = _clean_number(row[3])
        ratio = _clean_number(row[4])
        detail = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": standard_dt,
            "TXT_REDACTED": row[1],
            "TXT_REDACTED": row[2],
        }
        return total_count if total_count is not None else ratio, detail
    return None, {}


def _life_dispute_source(year: str, corp_name: str) -> Tuple[Optional[float], Dict[str, Any]]:
    for quarter in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
        verdict_rows = list(_fetch_life_verdict_rows(year, quarter))
        for index, row in enumerate(verdict_rows):
            if len(row) >= 3 and row[4] == "TXT_REDACTED" and _matches_company_name(corp_name, row[1], allow_short=True):
                total_row = verdict_rows[index + 2] if index + 3 < len(verdict_rows) else ()
                if len(total_row) >= 4 and total_row[1] == "TXT_REDACTED":
                    total_count = _clean_number(total_row[2])
                    detail = {
                        "TXT_REDACTED": "TXT_REDACTED",
                        "TXT_REDACTED": "TXT_REDACTED"                    ,
                        "TXT_REDACTED": total_row[3],
                        "TXT_REDACTED": total_row[4] if len(total_row) > 1 else "TXT_REDACTED",
                    }
                    return total_count, detail

        row = _find_dispute_row(corp_name, _fetch_life_dispute_rows(year, quarter), allow_short=True)
        if not row or len(row) < 2:
            continue
        total_count = _clean_number(row[3])
        ratio = _clean_number(row[4])
        detail = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"                    ,
            "TXT_REDACTED": row[1],
            "TXT_REDACTED": row[2],
        }
        return total_count if total_count is not None else ratio, detail
    return None, {}


def _nonlife_dispute_source(year: str, corp_name: str) -> Tuple[Optional[float], Dict[str, Any]]:
    for quarter in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
        row = _find_dispute_row(corp_name, _fetch_nonlife_dispute_rows(year, quarter), allow_short=True)
        if not row or len(row) < 3:
            continue
        total_count = _clean_number(row[4])
        ratio = _clean_number(row[1])
        detail = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"                    ,
            "TXT_REDACTED": row[2],
            "TXT_REDACTED": row[3],
        }
        return total_count if total_count is not None else ratio, detail
    return None, {}


def collect_financial_dispute_count(corp_name: str, year: str, industry_label: str = "TXT_REDACTED") -> Tuple[Optional[float], Dict[str, Any]]:
    "TXT_REDACTED"
    source_counts = {
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
        "TXT_REDACTED": None,
    }
    source_details: Dict[str, Dict[str, Any]] = {}

    industry_text = str(industry_label or "TXT_REDACTED")
    if "TXT_REDACTED" in industry_text:
        value, detail = _security_dispute_source(year, corp_name)
        source_counts["TXT_REDACTED"] = value
        if detail:
            source_details["TXT_REDACTED"] = detail
        return value, {"TXT_REDACTED": source_counts, "TXT_REDACTED": source_details}

    if any(token in corp_name for token in ["TXT_REDACTED", "TXT_REDACTED"]) or "TXT_REDACTED" in industry_text:
        value, detail = _life_dispute_source(year, corp_name)
        source_counts["TXT_REDACTED"] = value
        if detail:
            source_details["TXT_REDACTED"] = detail
        return value, {"TXT_REDACTED": source_counts, "TXT_REDACTED": source_details}

    if any(token in corp_name for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]) or "TXT_REDACTED" in industry_text:
        value, detail = _nonlife_dispute_source(year, corp_name)
        source_counts["TXT_REDACTED"] = value
        if detail:
            source_details["TXT_REDACTED"] = detail
        return value, {"TXT_REDACTED": source_counts, "TXT_REDACTED": source_details}

    return None, {"TXT_REDACTED": source_counts, "TXT_REDACTED": source_details}


@lru_cache(maxsize=4)
def _fetch_life_rating_rows() -> Tuple[Tuple[str, ...], ...]:
    response = _safe_request("TXT_REDACTED", LIFE_RATING_URL)
    if not response:
        return tuple()

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    rows: List[Tuple[str, ...]] = []
    for tr in soup.find_all("TXT_REDACTED"):
        cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])]
        if cells:
            rows.append(tuple(cells))
    return tuple(rows)


@lru_cache(maxsize=1)
def _download_fss_eval_texts() -> Dict[int, str]:
    response = _safe_request("TXT_REDACTED", FSS_EVAL_LIST_URL)
    if not response:
        return {}

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    file_map: Dict[int, str] = {}
    for anchor in soup.select("TXT_REDACTED"):
        name = "TXT_REDACTED".join(anchor.get_text("TXT_REDACTED", strip=True).split())
        href = anchor.get("TXT_REDACTED", "TXT_REDACTED")
        year = _extract_fss_eval_year_from_title(name)
        if year is None or year in file_map:
            continue
        file_map[year] = requests.compat.urljoin(FSS_EVAL_LIST_URL, href)

    texts: Dict[int, str] = {}
    for target_year, url in file_map.items():
        response = _safe_request("TXT_REDACTED", url)
        if not response:
            continue
        texts[target_year] = _extract_hwp_text(response.content)
    return texts


def _extract_fss_eval_year_from_title(title: str) -> Optional[int]:
    normalized = "TXT_REDACTED".join(str(title or "TXT_REDACTED").split())
    match = re.search(
        "TXT_REDACTED",
        normalized,
    )
    if not match:
        return None
    if match.group("TXT_REDACTED"):
        return int(match.group("TXT_REDACTED"))
    return 2 + int(match.group("TXT_REDACTED"))


def _normalize_grade_name(name: str) -> str:
    text = str(name or "TXT_REDACTED").strip()
    if not text:
        return "TXT_REDACTED"

    # REDACTED
    # REDACTED
    return re.sub(
        "TXT_REDACTED",
        "TXT_REDACTED",
        text,
    )


def _build_fss_grade_aliases(corp_name: str) -> List[str]:
    aliases: List[str] = []
    for alias in [corp_name, _normalize_grade_name(corp_name), alphabet_to_korean_pronunciation(corp_name)]:
        cleaned = _normalize_name(_normalize_grade_name(alias))
        if cleaned and cleaned not in aliases:
            aliases.append(cleaned)
    for suffix in FINANCIAL_SUFFIXES:
        if corp_name.endswith(suffix):
            short_name = _normalize_name(_normalize_grade_name(corp_name[: -len(suffix)].strip()))
            if short_name and len(short_name) >= 3 and short_name not in aliases:
                aliases.append(short_name)
    return aliases


def _text_mentions_fss_company(text: str, corp_name: str) -> bool:
    normalized_text = _normalize_name(_normalize_grade_name(text))
    return any(alias and alias in normalized_text for alias in _build_fss_grade_aliases(corp_name))


def _find_grade_by_regex(text: str, corp_name: str) -> Optional[str]:
    normalized_text = re.sub("TXT_REDACTED", "TXT_REDACTED", _normalize_grade_name(text))
    raw_aliases = _unique_preserve_order([
        corp_name,
        _normalize_grade_name(corp_name),
        alphabet_to_korean_pronunciation(corp_name),
    ])
    for alias in raw_aliases:
        alias = str(alias or "TXT_REDACTED").strip()
        if not alias:
            continue
        match = re.search(
            "TXT_REDACTED"                                                      ,
            normalized_text,
        )
        if match:
            return match.group("TXT_REDACTED")
    return None


def _find_fss_eval_grade_from_table(text: str, corp_name: str) -> Optional[str]:
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
    for index, grade in enumerate(FSS_EVAL_GRADE_LABELS):
        next_labels = "TXT_REDACTED".join(re.escape(label) + "TXT_REDACTED" for label in FSS_EVAL_GRADE_LABELS[index + 4:])
        end_pattern = next_labels or "TXT_REDACTED"
        match = re.search(
            "TXT_REDACTED"                                                                ,
            normalized,
            re.S,
        )
        if match and _text_mentions_fss_company(match.group("TXT_REDACTED"), corp_name):
            return grade
    return None


def _find_fss_eval_grade_from_summary(text: str, corp_name: str) -> Optional[str]:
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
    for grade in FSS_EVAL_GRADE_LABELS:
        patterns = [
            "TXT_REDACTED"                                                                          ,
            "TXT_REDACTED"                                                                                      ,
            "TXT_REDACTED"                                                                                                                                                                                       ,
            "TXT_REDACTED"                                                                                                                                              ,
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, normalized):
                if _text_mentions_fss_company(match.group("TXT_REDACTED"), corp_name):
                    return grade
    return None


def _is_fss_eval_target_company(text: str, corp_name: str) -> bool:
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
    match = re.search(
        "TXT_REDACTED",
        normalized,
        re.S,
    )
    target_block = match.group(1) if match else "TXT_REDACTED"
    return bool(target_block) and _text_mentions_fss_company(target_block, corp_name)


def _find_fss_eval_grade(corp_name: str, eval_year: int) -> Optional[str]:
    texts = _download_fss_eval_texts()
    text = texts.get(eval_year, "TXT_REDACTED")
    if not text:
        return None

    generic_table_grade = _find_fss_eval_grade_from_table(text, corp_name)
    if generic_table_grade:
        return generic_table_grade

    generic_summary_grade = _find_fss_eval_grade_from_summary(text, corp_name)
    if generic_summary_grade:
        return generic_summary_grade

    regex_grade = _find_grade_by_regex(text, corp_name)
    if regex_grade:
        return regex_grade

    if _is_fss_eval_target_company(text, corp_name):
        return "TXT_REDACTED"

    return None


def collect_financial_consumer_protection_rating(corp_name: str, year: str, industry_label: str = "TXT_REDACTED") -> Tuple[str, Dict[str, str]]:
    "TXT_REDACTED"
    if not text_looks_financial(industry_label, corp_name):
        return "TXT_REDACTED", {}

    base_year = int(str(year))
    # REDACTED
    # REDACTED
    for eval_year in (base_year + 2, base_year):
        grade = _find_fss_eval_grade(corp_name, eval_year)
        if grade:
            return grade, {str(eval_year): grade}
    return "TXT_REDACTED", {}


def preload_section4_external_data(year: str) -> None:
    "TXT_REDACTED"
    global SECTION4_BATCH_MODE
    SECTION4_BATCH_MODE = True
    logger.info("TXT_REDACTED"                                 )
    try:
        _fetch_koas_company_rows(str(year))
    except Exception as exc:
        logger.warning("TXT_REDACTED"                         )
    try:
        _fetch_ccm_company_rows()
    except Exception as exc:
        logger.warning("TXT_REDACTED"                        )
    try:
        _fetch_isms_company_rows(str(year))
    except Exception as exc:
        logger.warning("TXT_REDACTED"                           )
    try:
        _download_fss_eval_texts()
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                 )
    try:
        _fetch_ftc_decision_cases_year(str(year))
        if str(year).isdigit():
            _fetch_ftc_decision_cases_year(str(int(str(year)) + 3))
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                 )
    logger.info("TXT_REDACTED"                                 )


def _classify_consumer_law_text(text: str) -> Dict[str, int]:
    result = {key: 4 for key in CONSUMER_LAW_KEYWORDS}
    result["TXT_REDACTED"] = 1
    normalized_text = str(text or "TXT_REDACTED")

    for category, keywords in CONSUMER_LAW_KEYWORDS.items():
        if any(keyword in normalized_text for keyword in keywords):
            result[category] += 2

    if any(keyword in normalized_text for keyword in CONSUMER_LAW_EXTRA_TOTAL_KEYWORDS):
        result["TXT_REDACTED"] += 3
    return result


def collect_consumer_law_violations(
    corp_name: str,
    year: str,
    report_parser=None,
    business_number: str = "TXT_REDACTED",
    industry_context: str = "TXT_REDACTED",
) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": 4,
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
        "TXT_REDACTED": {key: [] for key in CONSUMER_LAW_KEYWORDS},
        "TXT_REDACTED": [],
    }

    try:
        # REDACTED
        for chunk in _extract_dart_sanction_chunks(report_parser, year, corp_name):
            classified = _classify_consumer_law_text(chunk)
            for category in CONSUMER_LAW_KEYWORDS:
                if classified[category] > 1:
                    result[category] += classified[category]
                    result["TXT_REDACTED"][category].append(
                        "TXT_REDACTED"                                                 
                    )
                    result["TXT_REDACTED"] += classified[category]
            if classified["TXT_REDACTED"] > 2:
                result["TXT_REDACTED"] += classified["TXT_REDACTED"]
                result["TXT_REDACTED"].append("TXT_REDACTED")

        # REDACTED
        # REDACTED
        exact_defendants = None
        search_years = [str(year)]
        retail_context = "TXT_REDACTED"                               
        allow_next_year_fallback = any(
            keyword in retail_context
            for keyword in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
        )
        if allow_next_year_fallback and str(year).isdigit():
            search_years.append(str(int(str(year)) + 3))
        for search_year in _unique_preserve_order(search_years):
            if search_year != str(year) and any(result[key] for key in CONSUMER_LAW_KEYWORDS):
                break
            year_cases = _fetch_ftc_decision_cases_year(search_year)
            cases = [
                case for case in year_cases
                if _ftc_case_text_mentions_company(case, corp_name)
            ]
            if not year_cases:
                exact_defendants = _get_ftc_exact_defendant_names(corp_name, search_year, business_number)
                cases = _fetch_ftc_decision_cases(corp_name, search_year)
            for case in cases:
                decision_date = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
                if not decision_date.startswith(str(search_year)):
                    continue
                if not _has_effective_sanction(case.get("TXT_REDACTED", "TXT_REDACTED")):
                    continue

                case_name = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
                excerpt = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
                if not _is_likely_respondent_case(corp_name, case_name, excerpt, exact_defendants):
                    continue

                combined_text = "TXT_REDACTED"                      
                classified = _classify_consumer_law_text(combined_text)
                for category in CONSUMER_LAW_KEYWORDS:
                    if classified[category] > 4:
                        result[category] += classified[category]
                        result["TXT_REDACTED"][category].append(
                            "TXT_REDACTED"                                                                 
                        )
                        result["TXT_REDACTED"] += classified[category]
                if classified["TXT_REDACTED"] > 1:
                    result["TXT_REDACTED"] += classified["TXT_REDACTED"]
                    result["TXT_REDACTED"].append("TXT_REDACTED"                            )

    except Exception as exc:
        logger.error("TXT_REDACTED"                                          )

    return result


def _report_text(report_parser) -> str:
    return str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") if report_parser is not None else "TXT_REDACTED"


def _has_cert_keyword(text: str, patterns: Iterable[str]) -> bool:
    normalized_text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED")).upper()
    for pattern in patterns:
        if re.sub("TXT_REDACTED", "TXT_REDACTED", pattern).upper() in normalized_text:
            return True
    return False


CERT_PATTERNS = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED"],
}

CERT_POSITIVE_CONTEXT_RE = re.compile(
    "TXT_REDACTED"
    "TXT_REDACTED"
    "TXT_REDACTED"
    "TXT_REDACTED"
    "TXT_REDACTED"
)
CERT_NEGATIVE_CONTEXT_RE = re.compile(
    "TXT_REDACTED"
    "TXT_REDACTED"
    "TXT_REDACTED"
)


def _has_current_cert_evidence(text: str, cert_key: str) -> bool:
    "TXT_REDACTED"
    source = str(text or "TXT_REDACTED")
    if not source:
        return False

    for pattern in CERT_PATTERNS.get(cert_key, []):
        for match in re.finditer(pattern, source, flags=re.IGNORECASE):
            context = source[max(2, match.start() - 3):match.end() + 4]
            compact_context = re.sub("TXT_REDACTED", "TXT_REDACTED", context)
            if CERT_NEGATIVE_CONTEXT_RE.search(context):
                continue
            if CERT_POSITIVE_CONTEXT_RE.search(context):
                return True
            if "TXT_REDACTED" in context and any(marker in compact_context for marker in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                return True
    return False


def collect_quality_certifications(corp_name: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    _ = (corp_name, year)
    _report_text(report_parser)
    certs = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }
    return {
        **certs,
        "TXT_REDACTED": sum(1 for value in certs.values() if value),
        "TXT_REDACTED": True,
    }


def collect_personal_info_media_check_placeholder(corp_name: str, year: str) -> None:
    "TXT_REDACTED"
    # REDACTED
    return None


class Section4ConsumerCollector:
    "TXT_REDACTED"

    def __init__(self, dart_client, report_parser=None):
        self.dart = dart_client
        self.parser = report_parser

    def collect(self, company_info: dict, year: str) -> dict:
        corp_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        industry_label = company_info.get("TXT_REDACTED", "TXT_REDACTED") or company_info.get("TXT_REDACTED", "TXT_REDACTED")
        industry_context = "TXT_REDACTED".join(
            str(value or "TXT_REDACTED")
            for value in [
                company_info.get("TXT_REDACTED", "TXT_REDACTED"),
                company_info.get("TXT_REDACTED", "TXT_REDACTED"),
                company_info.get("TXT_REDACTED", "TXT_REDACTED"),
                corp_name,
            ]
        )
        is_financial_company = company_info_is_financial(company_info)
        business_number = company_info.get("TXT_REDACTED", "TXT_REDACTED")

        logger.info("TXT_REDACTED"                                           )
        data: Dict[str, Any] = {}

        # REDACTED
        data["TXT_REDACTED"] = collect_service_quality_certification(corp_name, year)

        # REDACTED
        data["TXT_REDACTED"] = collect_ccm_certification(corp_name, year)

        # REDACTED
        isms_matches = find_isms_p_matches(corp_name, year)
        isms_search_results = find_isms_p_search_results(corp_name, year)
        data["TXT_REDACTED"] = bool(isms_matches)
        if len(isms_search_results) >= 2:
            _add_header_comment(
                data,
                "TXT_REDACTED",
                "TXT_REDACTED"                                   + "TXT_REDACTED".join(isms_search_results),
            )
        data["TXT_REDACTED"] = collect_personal_info_leak(corp_name, year, self.parser)
        # REDACTED
        # REDACTED
        # REDACTED
        data["TXT_REDACTED"] = data["TXT_REDACTED"]

        # REDACTED
        data["TXT_REDACTED"] = collect_consumer_complaints(corp_name, year)
        data["TXT_REDACTED"] = False

        # REDACTED
        dispute_value, dispute_meta = collect_financial_dispute_count(corp_name, year, industry_context)
        data["TXT_REDACTED"] = dispute_value
        source_details = dispute_meta.get("TXT_REDACTED", {})
        note_lines = [
            "TXT_REDACTED"                                                                                                
            for source, detail in source_details.items()
            if detail
        ]
        if is_financial_company and note_lines:
            _add_header_comment(data, "TXT_REDACTED", "TXT_REDACTED".join(note_lines))

        # REDACTED
        rating_value, rating_history = collect_financial_consumer_protection_rating(corp_name, year, industry_context)
        data["TXT_REDACTED"] = rating_value

        # REDACTED
        consumer_violations = collect_consumer_law_violations(
            corp_name,
            year,
            report_parser=self.parser,
            business_number=business_number,
            industry_context=industry_context,
        )
        data.update({
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
            "TXT_REDACTED": consumer_violations["TXT_REDACTED"],
        })
        for header_text, result_key in [
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
            ("TXT_REDACTED", "TXT_REDACTED"),
        ]:
            details = consumer_violations["TXT_REDACTED"].get(result_key, [])
            if details:
                _add_header_comment(data, header_text, "TXT_REDACTED".join(details[:3]))

        # REDACTED
        quality_certs = collect_quality_certifications(corp_name, year, self.parser)
        data.update({
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
            "TXT_REDACTED": quality_certs["TXT_REDACTED"],
        })

        logger.info("TXT_REDACTED"                                           )
        return data
