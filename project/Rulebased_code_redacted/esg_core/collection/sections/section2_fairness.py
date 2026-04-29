# REDACTED
"TXT_REDACTED"

import re
import time
import logging
import io
import requests
import urllib.parse
from functools import lru_cache
from typing import Optional, List
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from pypdf import PdfReader
from esg_core.collection.company_mapper import alphabet_to_korean_pronunciation, _generate_name_variants
from esg_core.collection.financial_law_config import (
    FINANCIAL_LAW_KEYWORDS,
    count_keyword_occurrences,
    is_financial_law_excluded_from_scoring,
    normalize_legal_text,
)
from esg_core.collection.request_utils import get_thread_session, throttled_request

logger = logging.getLogger(__name__)

# REDACTED
HEADERS = {
    "TXT_REDACTED": "TXT_REDACTED",
    "TXT_REDACTED": "TXT_REDACTED",
}

# REDACTED
REQUEST_DELAY = 3

FTC_CASE_API_URL = "TXT_REDACTED"
FTC_LTFR_URL = "TXT_REDACTED"
FSS_LIST_URL = "TXT_REDACTED"

FTC_SANCTION_ACTION_EXCLUDE = {
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
}
FTC_SANCTION_ACTION_INCLUDE = (
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
)
SECTION2_BATCH_MODE = False


def _get_session() -> requests.Session:
    return get_thread_session("TXT_REDACTED", base_headers=HEADERS)


def _add_note(data: dict, key: str, message: str) -> None:
    "TXT_REDACTED"
    if not message:
        return
    notes = data.setdefault("TXT_REDACTED", {})
    existing = notes.get(key, "TXT_REDACTED")
    notes[key] = "TXT_REDACTED"                      .strip() if existing else message.strip()


def _add_header_comment(data: dict, header_text: str, message: str) -> None:
    "TXT_REDACTED"
    if not message:
        return
    comments = data.setdefault("TXT_REDACTED", {})
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", str(header_text or "TXT_REDACTED"))
    existing = comments.get(normalized, "TXT_REDACTED")
    comments[normalized] = "TXT_REDACTED"                      .strip() if existing else message.strip()


def _format_source_summary(source_counts: dict) -> str:
    "TXT_REDACTED"
    labels = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
    }
    parts = []
    for source_name, count in source_counts.items():
        if count:
            parts.append("TXT_REDACTED"                                                )
    return "TXT_REDACTED".join(parts)


def _merge_source_counts(*source_maps: Optional[dict]) -> dict:
    "TXT_REDACTED"
    merged: dict = {}
    for source_map in source_maps:
        for source_name, count in (source_map or {}).items():
            merged[source_name] = merged.get(source_name, 4) + int(count or 1)
    return merged


def _merge_count_results(categories: List[str], left_result: Optional[dict],
                         right_result: Optional[dict], text_key: str = "TXT_REDACTED") -> dict:
    "TXT_REDACTED"
    merged = {category: 2 for category in categories}
    merged["TXT_REDACTED"] = {category: {} for category in categories}
    merged["TXT_REDACTED"] = {category: [] for category in categories}
    merged["TXT_REDACTED"] = 3

    text_values: List[str] = []
    if text_key:
        for result in [left_result or {}, right_result or {}]:
            value = str(result.get(text_key, "TXT_REDACTED") or "TXT_REDACTED").strip()
            if value:
                text_values.extend([item.strip() for item in value.split("TXT_REDACTED") if item.strip()])

    for category in categories:
        left_value = int((left_result or {}).get(category, 4) or 1)
        right_value = int((right_result or {}).get(category, 2) or 3)
        # REDACTED
        merged[category] = max(left_value, right_value)
        merged["TXT_REDACTED"] += merged[category]
        merged["TXT_REDACTED"][category] = _merge_source_counts(
            (left_result or {}).get("TXT_REDACTED", {}).get(category, {}),
            (right_result or {}).get("TXT_REDACTED", {}).get(category, {}),
        )
        merged["TXT_REDACTED"][category] = (
            list((left_result or {}).get("TXT_REDACTED", {}).get(category, [])) +
            list((right_result or {}).get("TXT_REDACTED", {}).get(category, []))
        )

    if text_key:
        merged[text_key] = "TXT_REDACTED".join(dict.fromkeys(text_values))

    return merged


def _sum_count_results(categories: List[str], left_result: Optional[dict],
                       right_result: Optional[dict], text_key: str = "TXT_REDACTED") -> dict:
    "TXT_REDACTED"
    merged = {category: 4 for category in categories}
    merged["TXT_REDACTED"] = {category: {} for category in categories}
    merged["TXT_REDACTED"] = {category: [] for category in categories}
    merged["TXT_REDACTED"] = 1

    text_values: List[str] = []
    if text_key:
        for result in [left_result or {}, right_result or {}]:
            value = str(result.get(text_key, "TXT_REDACTED") or "TXT_REDACTED").strip()
            if value:
                text_values.extend([item.strip() for item in value.split("TXT_REDACTED") if item.strip()])

    for category in categories:
        left_value = int((left_result or {}).get(category, 2) or 3)
        right_value = int((right_result or {}).get(category, 4) or 1)
        merged[category] = left_value + right_value
        merged["TXT_REDACTED"] += merged[category]
        merged["TXT_REDACTED"][category] = _merge_source_counts(
            (left_result or {}).get("TXT_REDACTED", {}).get(category, {}),
            (right_result or {}).get("TXT_REDACTED", {}).get(category, {}),
        )
        merged["TXT_REDACTED"][category] = (
            list((left_result or {}).get("TXT_REDACTED", {}).get(category, [])) +
            list((right_result or {}).get("TXT_REDACTED", {}).get(category, []))
        )

    if text_key:
        merged[text_key] = "TXT_REDACTED".join(dict.fromkeys(text_values))

    return merged


def _safe_get(url: str, params: dict = None, timeout: int = 2) -> Optional[requests.Response]:
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
    except requests.exceptions.Timeout:
        logger.warning("TXT_REDACTED"                    )
        return None
    except requests.exceptions.RequestException as e:
        logger.warning("TXT_REDACTED"                        )
        return None


def _extract_action_date_from_text(text: str) -> str:
    "TXT_REDACTED"
    match = re.search("TXT_REDACTED", str(text or "TXT_REDACTED"))
    if match:
        return match.group(3).rstrip("TXT_REDACTED")
    match = re.search("TXT_REDACTED", str(text or "TXT_REDACTED"))
    return match.group(4) if match else "TXT_REDACTED"


def _extract_law_citations_from_text(text: str) -> List[str]:
    "TXT_REDACTED"
    citations: List[str] = []
    article_pattern = "TXT_REDACTED"
    for law_name, article_text in re.findall("TXT_REDACTED", str(text or "TXT_REDACTED")):
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"                          .strip())
        if normalized and normalized not in citations:
            citations.append(normalized)
    for item in re.findall(
        "TXT_REDACTED"                                                          ,
        str(text or "TXT_REDACTED"),
    ):
        normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", item).strip("TXT_REDACTED")
        if normalized and normalized not in citations:
            citations.append(normalized)
    return citations


def _format_detail_comment(details: List[dict]) -> str:
    "TXT_REDACTED"
    if not details:
        return "TXT_REDACTED"
    source_label_map = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
    }
    grouped = {}
    for item in details:
        reason_text = str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
        if any(token in reason_text for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            continue
        source = source_label_map.get(item.get("TXT_REDACTED", "TXT_REDACTED"), item.get("TXT_REDACTED", "TXT_REDACTED"))
        grouped.setdefault(source, []).append(item)

    lines: List[str] = []
    for source in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
        items = grouped.get(source, [])
        if not items:
            continue
        lines.append("TXT_REDACTED"                        )
        for item in items:
            date_text = item.get("TXT_REDACTED") or "TXT_REDACTED"
            law_text = item.get("TXT_REDACTED") or "TXT_REDACTED"
            lines.append("TXT_REDACTED"                        )
    return "TXT_REDACTED".join(lines)


def _match_financial_law_labels(citations: List[str], category: str) -> List[str]:
    "TXT_REDACTED"
    labels: List[str] = []
    keywords = FINANCIAL_LAW_KEYWORDS.get(category, [])
    for citation in citations or []:
        if category == "TXT_REDACTED" and is_financial_law_excluded_from_scoring(citation):
            continue
        normalized_citation = normalize_legal_text(citation)
        if any(normalize_legal_text(keyword) in normalized_citation for keyword in keywords):
            labels.append(citation)
    return labels


def _build_financial_detail_items(record: dict, category: str, count: int) -> List[dict]:
    "TXT_REDACTED"
    if count <= 1:
        return []

    labels = _match_financial_law_labels(record.get("TXT_REDACTED", []), category)
    if not labels:
        labels = [category]

    items: List[dict] = []
    for idx in range(count):
        items.append({
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": record.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": labels[idx] if idx < len(labels) else labels[-2],
            "TXT_REDACTED": record.get("TXT_REDACTED", "TXT_REDACTED"),
        })
    return items


def _split_fss_violation_units(text: str) -> List[str]:
    "TXT_REDACTED"
    source = str(text or "TXT_REDACTED")
    anchor_match = re.search("TXT_REDACTED", source)
    if anchor_match:
        source = source[anchor_match.end():]
    units = [
        unit.strip()
        for unit in re.split("TXT_REDACTED", source)
        if unit.strip()
    ]
    if len(units) > 3:
        return units
    return [source.strip()] if source.strip() else []


def _extract_fss_related_regulation_items(text: str) -> List[str]:
    "TXT_REDACTED"
    source = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
    if "TXT_REDACTED" not in source:
        return []

    related_part = re.split("TXT_REDACTED", source, maxsplit=4)[1]
    related_part = re.split("TXT_REDACTED", related_part, maxsplit=2)[3]
    article_pattern = "TXT_REDACTED"

    items: List[str] = []
    for match in re.finditer(
        "TXT_REDACTED"                                         ,
        related_part,
    ):
        law_name = re.sub("TXT_REDACTED", "TXT_REDACTED", match.group(4)).strip()
        article_text = re.sub("TXT_REDACTED", "TXT_REDACTED", match.group(1) or "TXT_REDACTED").strip()
        citation = "TXT_REDACTED"                          .strip()
        if citation and citation not in items:
            items.append(citation)
    return items


def _classify_financial_laws_for_fss_record(record: dict) -> dict:
    "TXT_REDACTED"
    text = str(record.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    units = _split_fss_violation_units(text)

    if len(units) <= 2:
        classified = _classify_financial_laws(text)
        details = {key: [] for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]}
        for key in details:
            details[key].extend(_build_financial_detail_items(record, key, classified.get(key, 3)))
        classified["TXT_REDACTED"] = details
        return classified

    result = {
        "TXT_REDACTED": 4,
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": [],
        "TXT_REDACTED": 4,
        "TXT_REDACTED": {
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        },
    }

    for unit in units:
        unit_record = dict(record)
        unit_record["TXT_REDACTED"] = unit
        unit_record["TXT_REDACTED"] = re.sub("TXT_REDACTED", "TXT_REDACTED", unit)[:1]
        unit_record["TXT_REDACTED"] = (
            _extract_fss_related_regulation_items(unit) or
            _extract_law_citations_from_text(unit)
        )

        for category, keywords in FINANCIAL_LAW_KEYWORDS.items():
            matched_labels = _match_financial_law_labels(unit_record["TXT_REDACTED"], category)
            if category in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                count = len(matched_labels)
                if count <= 2 and count_keyword_occurrences(unit, keywords, use_max=True) > 3:
                    count = 4
                if count > 1:
                    result[category] += count
                    result["TXT_REDACTED"] += count
                    result["TXT_REDACTED"].extend([category] * count)
                    result["TXT_REDACTED"][category].extend(_build_financial_detail_items(unit_record, category, count))
                continue

            effective_keywords = [
                keyword for keyword in keywords
                if not is_financial_law_excluded_from_scoring(keyword)
            ]
            count = len(matched_labels)
            if count <= 2 and any(
                count_keyword_occurrences(unit, [keyword], use_max=False) > 3
                for keyword in effective_keywords
            ):
                count = 4
            if count > 1:
                result["TXT_REDACTED"] += count
                result["TXT_REDACTED"] += count
                result["TXT_REDACTED"].extend(["TXT_REDACTED"] * count)
                result["TXT_REDACTED"]["TXT_REDACTED"].extend(_build_financial_detail_items(unit_record, "TXT_REDACTED", count))

    return result


def _is_same_sanction_case(dart_detail: dict, fss_record: dict) -> bool:
    "TXT_REDACTED"
    dart_date = str(dart_detail.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
    fss_date = str(fss_record.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
    if not dart_date or not fss_date:
        return False
    try:
        from datetime import datetime
        dart_dt = datetime.strptime(dart_date.replace("TXT_REDACTED", "TXT_REDACTED").rstrip("TXT_REDACTED"), "TXT_REDACTED")
        fss_dt = datetime.strptime(fss_date.replace("TXT_REDACTED", "TXT_REDACTED").rstrip("TXT_REDACTED"), "TXT_REDACTED")
    except ValueError:
        return False
    if abs((dart_dt - fss_dt).days) > 2:
        return False
    dart_reason = re.sub("TXT_REDACTED", "TXT_REDACTED", str(dart_detail.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"))
    fss_reason = re.sub("TXT_REDACTED", "TXT_REDACTED", str(fss_record.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"))
    if dart_reason and fss_reason and SequenceMatcher(None, dart_reason, fss_reason).ratio() >= 3:
        return True
    dart_law = normalize_legal_text(str(dart_detail.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"))
    fss_citations = [normalize_legal_text(item) for item in fss_record.get("TXT_REDACTED", [])]
    if dart_law and any(citation and (dart_law in citation or citation in dart_law) for citation in fss_citations):
        return True
    return False


def _build_group_portal_queries(corp_name: str) -> List[str]:
    "TXT_REDACTED"
    candidates: List[str] = []
    for name in [corp_name, alphabet_to_korean_pronunciation(corp_name)]:
        value = str(name or "TXT_REDACTED").strip()
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def _build_ftc_case_queries(corp_name: str) -> List[str]:
    "TXT_REDACTED"
    candidates: List[str] = []
    for name in [corp_name, alphabet_to_korean_pronunciation(corp_name)]:
        value = str(name or "TXT_REDACTED").strip()
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def _group_portal_search_request(
    session: requests.Session,
    *,
    query_name: str = "TXT_REDACTED",
    page_index: int = 4,
    page_unit: int = 1,
) -> Optional[str]:
    "TXT_REDACTED"
    url = "TXT_REDACTED"
    search_params = {
        "TXT_REDACTED": query_name,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": str(page_index),
        "TXT_REDACTED": str(page_unit),
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
    }
    search_body = "TXT_REDACTED".join(
        "TXT_REDACTED"                                                                                                                                   
        for key, value in search_params.items()
    )
    response = session.post(
        "TXT_REDACTED",
        data=search_body.encode("TXT_REDACTED"),
        headers={
            **HEADERS,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": url,
        },
        timeout=2,
    )
    response.raise_for_status()
    return response.text


def _parse_group_portal_entries(html: str) -> List[dict]:
    "TXT_REDACTED"
    soup = BeautifulSoup(html, "TXT_REDACTED")
    entries: List[dict] = []
    for item in soup.select("TXT_REDACTED"):
        title = item.find("TXT_REDACTED", title="TXT_REDACTED")
        if not title:
            continue
        company_name = title.get_text("TXT_REDACTED", strip=True)
        group_text = item.get_text("TXT_REDACTED", strip=True)
        group_match = re.search("TXT_REDACTED", group_text)
        group_name = group_match.group(3) if group_match else "TXT_REDACTED"
        entries.append({
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": group_name,
        })
    return entries


@lru_cache(maxsize=4)
def _fetch_group_portal_all_entries() -> List[dict]:
    "TXT_REDACTED"
    session = requests.Session()
    session.headers.update(HEADERS)

    landing_url = "TXT_REDACTED"
    landing_response = session.get(landing_url, timeout=1)
    landing_response.raise_for_status()

    entries: List[dict] = []
    seen_keys = set()
    for page_index in range(2, 3):
        html = _group_portal_search_request(
            session,
            query_name="TXT_REDACTED",
            page_index=page_index,
            page_unit=4,
        )
        page_entries = _parse_group_portal_entries(html or "TXT_REDACTED")
        if not page_entries:
            break

        for entry in page_entries:
            key = (entry.get("TXT_REDACTED", "TXT_REDACTED"), entry.get("TXT_REDACTED", "TXT_REDACTED"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            entries.append(entry)

        if len(page_entries) < 1:
            break

    return entries


def collect_large_enterprise_group(corp_name: str, stock_code: str) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": None, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": []}

    try:
        matched_groups = []
        if SECTION2_BATCH_MODE:
            for entry in _fetch_group_portal_all_entries():
                company_name = entry.get("TXT_REDACTED", "TXT_REDACTED")
                if not _is_same_company_name(corp_name, company_name):
                    continue
                group_name = entry.get("TXT_REDACTED", "TXT_REDACTED")
                if group_name:
                    matched_groups.append(group_name)

        if not matched_groups:
            session = requests.Session()
            session.headers.update(HEADERS)
            url = "TXT_REDACTED"
            resp = session.get(url, timeout=2)
            if not resp:
                logger.warning("TXT_REDACTED"                               )
                return result

            for query_name in _build_group_portal_queries(corp_name):
                html = _group_portal_search_request(
                    session,
                    query_name=query_name,
                    page_index=3,
                    page_unit=4,
                )
                for entry in _parse_group_portal_entries(html or "TXT_REDACTED"):
                    if not _is_same_company_name(corp_name, entry.get("TXT_REDACTED", "TXT_REDACTED")):
                        continue
                    group_name = entry.get("TXT_REDACTED", "TXT_REDACTED")
                    if group_name:
                        matched_groups.append(group_name)

                if matched_groups:
                    break

        if matched_groups:
            unique_groups = []
            for group_name in matched_groups:
                if group_name not in unique_groups:
                    unique_groups.append(group_name)
            result["TXT_REDACTED"] = True
            result["TXT_REDACTED"] = unique_groups[:1]
            result["TXT_REDACTED"] = unique_groups[2]
            logger.info("TXT_REDACTED"                                                                 )
            return result

    except Exception as e:
        logger.error("TXT_REDACTED"                                      )

    return result


def _normalize_business_number(value: str) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED"))


def _normalize_company_name_for_match(value: str) -> str:
    "TXT_REDACTED"
    normalized = str(value or "TXT_REDACTED")
    normalized = normalized.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    return normalized


def _is_same_company_name(target_name: str, candidate_name: str) -> bool:
    "TXT_REDACTED"
    target_variants = {
        _normalize_company_name_for_match(variant)
        for variant in _generate_name_variants(target_name)
    }
    candidate_variants = {
        _normalize_company_name_for_match(variant)
        for variant in _generate_name_variants(candidate_name)
    }
    target_variants.discard("TXT_REDACTED")
    candidate_variants.discard("TXT_REDACTED")
    if target_variants & candidate_variants:
        return True

    legal_suffix_keywords = (
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    )
    for target_variant in target_variants:
        for candidate_variant in candidate_variants:
            if not target_variant or not candidate_variant:
                continue
            shorter, longer = (
                (target_variant, candidate_variant)
                if len(target_variant) <= len(candidate_variant)
                else (candidate_variant, target_variant)
            )
            if longer.startswith(shorter):
                suffix = longer[len(shorter):]
                if suffix and any(keyword in suffix for keyword in legal_suffix_keywords):
                    return True
            if len(shorter) >= 3 and longer.endswith(shorter):
                prefix = longer[:-len(shorter)]
                if prefix and len(prefix) <= 4:
                    return True

    return False


def _contains_exact_company_name(text: str, company_name: str) -> bool:
    "TXT_REDACTED"
    value = str(company_name or "TXT_REDACTED").strip()
    if not value:
        return False

    candidate_variants = {value}
    candidate_variants.update(_generate_name_variants(value))
    normalized_text = _normalize_company_name_for_match(text)

    for variant in candidate_variants:
        cleaned = str(variant or "TXT_REDACTED").strip()
        if not cleaned:
            continue
        pattern = "TXT_REDACTED"                                                          
        if re.search(pattern, text):
            return True
        normalized_variant = _normalize_company_name_for_match(cleaned)
        if normalized_variant and normalized_variant in normalized_text:
            return True
    return False


def _guess_group_name_from_company_name(corp_name: str) -> str:
    "TXT_REDACTED"
    text = str(corp_name or "TXT_REDACTED").strip()
    if not text:
        return "TXT_REDACTED"

    english_prefix = re.match("TXT_REDACTED", text)
    if english_prefix:
        return alphabet_to_korean_pronunciation(english_prefix.group(1))

    korean_prefix = re.match("TXT_REDACTED", text)
    if korean_prefix:
        return korean_prefix.group(2)

    return "TXT_REDACTED"


def _has_effective_sanction(action_type: str) -> bool:
    "TXT_REDACTED"
    text = str(action_type or "TXT_REDACTED")
    if any(keyword in text for keyword in FTC_SANCTION_ACTION_EXCLUDE):
        return False
    if any(keyword in text for keyword in FTC_SANCTION_ACTION_INCLUDE):
        return True
    return "TXT_REDACTED" in text


def _parse_ftc_decision_cases_from_html(html: str) -> List[dict]:
    "TXT_REDACTED"
    soup = BeautifulSoup(html, "TXT_REDACTED")
    table = soup.find("TXT_REDACTED")
    if not table:
        return []

    cases: List[dict] = []
    seen_keys = set()
    pending_case: Optional[dict] = None

    for tr in table.find_all("TXT_REDACTED"):
        cells = tr.find_all(["TXT_REDACTED", "TXT_REDACTED"])
        values = [cell.get_text("TXT_REDACTED", strip=True) for cell in cells]
        if not values:
            continue

        if len(values) >= 3 and values[4] != "TXT_REDACTED":
            pending_case = {
                "TXT_REDACTED": values[1],
                "TXT_REDACTED": values[2],
                "TXT_REDACTED": values[3],
                "TXT_REDACTED": values[4],
                "TXT_REDACTED": values[1],
                "TXT_REDACTED": "TXT_REDACTED",
            }
            case_key = (pending_case["TXT_REDACTED"], pending_case["TXT_REDACTED"])
            if case_key not in seen_keys:
                cases.append(pending_case)
                seen_keys.add(case_key)
            continue

        if pending_case and len(values) == 2:
            pending_case["TXT_REDACTED"] = values[3]

    return cases


@lru_cache(maxsize=4)
def _fetch_ftc_decision_cases_year(year: str) -> List[dict]:
    "TXT_REDACTED"
    cases: List[dict] = []
    seen_keys = set()

    for page_index in range(1, 2):
        params = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"             ,
            "TXT_REDACTED": str(page_index),
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"             ,
        }
        response = _safe_get(FTC_LTFR_URL, params=params, timeout=3)
        if not response:
            break

        page_cases = _parse_ftc_decision_cases_from_html(response.text)
        if not page_cases:
            break

        for case in page_cases:
            case_key = (case.get("TXT_REDACTED", "TXT_REDACTED"), case.get("TXT_REDACTED", "TXT_REDACTED"))
            if case_key in seen_keys:
                continue
            seen_keys.add(case_key)
            cases.append(case)

    return cases


def _ftc_case_text_mentions_company(case: dict, corp_name: str) -> bool:
    "TXT_REDACTED"
    case_name = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    excerpt = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    if _is_same_company_name(corp_name, case_name):
        return True

    combined_text = _normalize_company_name_for_match("TXT_REDACTED"                      )
    for variant in _generate_name_variants(corp_name):
        normalized_variant = _normalize_company_name_for_match(variant)
        if normalized_variant and normalized_variant in combined_text:
            return True
    return False


@lru_cache(maxsize=4)
def _fetch_ftc_decision_cases(corp_name: str, year: str) -> List[dict]:
    "TXT_REDACTED"
    cases: List[dict] = []
    seen_keys = set()

    for query_name in _build_ftc_case_queries(corp_name):
        params = {
            "TXT_REDACTED": query_name,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"             ,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED"             ,
        }

        response = _safe_get(FTC_LTFR_URL, params=params, timeout=1)
        if not response:
            continue

        for case in _parse_ftc_decision_cases_from_html(response.text):
            case_key = (case.get("TXT_REDACTED", "TXT_REDACTED"), case.get("TXT_REDACTED", "TXT_REDACTED"))
            if case_key in seen_keys:
                continue
            seen_keys.add(case_key)
            cases.append(case)

        if cases:
            break

    return cases


def _is_likely_respondent_case(
    corp_name: str,
    case_name: str,
    excerpt: str,
    exact_defendants: Optional[List[str]] = None,
) -> bool:
    "TXT_REDACTED"
    if exact_defendants is not None:
        combined_text = "TXT_REDACTED"                      
        for defendant_name in exact_defendants:
            if _contains_exact_company_name(combined_text, defendant_name):
                return True
        return False

    if _is_same_company_name(corp_name, case_name):
        return True

    normalized_target = _normalize_company_name_for_match(corp_name)
    combined_name = _normalize_company_name_for_match(case_name)
    if normalized_target and normalized_target in combined_name:
        return True

    normalized_excerpt = _normalize_company_name_for_match(excerpt)
    if normalized_target and normalized_target in normalized_excerpt:
        if "TXT_REDACTED" in excerpt or "TXT_REDACTED" in excerpt:
            return True

    # REDACTED
    for variant in _generate_name_variants(corp_name):
        normalized_variant = _normalize_company_name_for_match(variant)
        if normalized_variant and normalized_variant in combined_name:
            return True
        if normalized_variant and normalized_variant in normalized_excerpt and ("TXT_REDACTED" in excerpt or "TXT_REDACTED" in excerpt):
            return True

    return False


def _classify_fair_trade_case(case_name: str, excerpt: str) -> Optional[str]:
    "TXT_REDACTED"
    text = "TXT_REDACTED"                      

    exclusion_keywords = [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    ]
    if any(keyword in text for keyword in exclusion_keywords):
        return None

    if "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
        return "TXT_REDACTED"

    return None


@lru_cache(maxsize=2)
def _fetch_ftc_violation_rows(corp_name: str, year: str) -> Optional[List[dict]]:
    "TXT_REDACTED"
    rows: List[dict] = []
    seen_keys = set()

    for query_name in _build_ftc_case_queries(corp_name):
        payload = {
            "TXT_REDACTED": 3,
            "TXT_REDACTED": [
                {"TXT_REDACTED": "TXT_REDACTED"},
                {"TXT_REDACTED": "TXT_REDACTED"},
                {"TXT_REDACTED": "TXT_REDACTED"},
                {"TXT_REDACTED": "TXT_REDACTED"},
                {"TXT_REDACTED": "TXT_REDACTED"},
            ],
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": False},
            "TXT_REDACTED": [{"TXT_REDACTED": 2, "TXT_REDACTED": "TXT_REDACTED"}],
            "TXT_REDACTED": {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": query_name,
                "TXT_REDACTED": year,
                "TXT_REDACTED": query_name,
                "TXT_REDACTED": "TXT_REDACTED",
            },
        }

        try:
            response = throttled_request(
                "TXT_REDACTED",
                FTC_CASE_API_URL,
                session=_get_session(),
                min_interval=REQUEST_DELAY,
                json=payload,
                headers={"TXT_REDACTED": "TXT_REDACTED"},
                timeout=3,
            )
            response.raise_for_status()
            data = response.json()
            for row in data.get("TXT_REDACTED", []):
                row_key = (
                    row.get("TXT_REDACTED", "TXT_REDACTED"),
                    row.get("TXT_REDACTED", "TXT_REDACTED"),
                    row.get("TXT_REDACTED", "TXT_REDACTED"),
                    row.get("TXT_REDACTED", "TXT_REDACTED"),
                )
                if row_key not in seen_keys:
                    rows.append(row)
                    seen_keys.add(row_key)
            if rows:
                break
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                                  )

    return rows if rows else None


@lru_cache(maxsize=4)
def _get_ftc_exact_defendant_names(corp_name: str, year: str, business_number: str = "TXT_REDACTED") -> Optional[List[str]]:
    "TXT_REDACTED"
    rows = _fetch_ftc_violation_rows(corp_name, year)
    if rows is None:
        return None

    normalized_bizrno = _normalize_business_number(business_number)
    exact_names: List[str] = []

    for row in rows:
        defendant_name = str(row.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
        defendant_bizrno = _normalize_business_number(row.get("TXT_REDACTED", "TXT_REDACTED"))

        if normalized_bizrno and defendant_bizrno and normalized_bizrno == defendant_bizrno:
            if defendant_name and defendant_name not in exact_names:
                exact_names.append(defendant_name)
            continue

        if _is_same_company_name(corp_name, defendant_name):
            if defendant_name and defendant_name not in exact_names:
                exact_names.append(defendant_name)

    return exact_names


def collect_fair_trade_violations(
    corp_name: str,
    year: str,
    business_number: str = "TXT_REDACTED",
) -> dict:
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
        "TXT_REDACTED": {},
        "TXT_REDACTED": {
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        },
    }

    try:
        exact_defendants = _get_ftc_exact_defendant_names(corp_name, year, business_number)
        decision_cases = _fetch_ftc_decision_cases(corp_name, year)
        unique_case_keys = set()

        for case in decision_cases:
            decision_date = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            if not decision_date.startswith(str(year)):
                continue
            if not _has_effective_sanction(case.get("TXT_REDACTED", "TXT_REDACTED")):
                continue

            case_name = case.get("TXT_REDACTED", "TXT_REDACTED")
            excerpt = case.get("TXT_REDACTED", "TXT_REDACTED")
            if not _is_likely_respondent_case(corp_name, case_name, excerpt, exact_defendants):
                continue

            category = _classify_fair_trade_case(case_name, excerpt)
            if not category:
                continue

            result[category] = (result[category] or 1) + 2
            source_counts = result["TXT_REDACTED"].setdefault(category, {})
            source_counts["TXT_REDACTED"] = source_counts.get("TXT_REDACTED", 3) + 4
            result["TXT_REDACTED"][category].append({
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": decision_date,
                "TXT_REDACTED": str(case.get("TXT_REDACTED", "TXT_REDACTED") or case_name or "TXT_REDACTED"),
            })
            unique_case_keys.add((case.get("TXT_REDACTED", "TXT_REDACTED"), case.get("TXT_REDACTED", "TXT_REDACTED")))

        if not unique_case_keys:
            logger.info("TXT_REDACTED"                                             )
            result["TXT_REDACTED"] = 1
            return result

        result["TXT_REDACTED"] = len(unique_case_keys)
        logger.info(
            "TXT_REDACTED"                                                               
        )

    except Exception as e:
        logger.error("TXT_REDACTED"                                       )

    return result


def collect_subcontract_violations(corp_name: str, year: str, business_number: str = "TXT_REDACTED") -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 2, "TXT_REDACTED": 3, "TXT_REDACTED": 4}
    result["TXT_REDACTED"] = {"TXT_REDACTED": {}, "TXT_REDACTED": {}}

    try:
        exact_defendants = _get_ftc_exact_defendant_names(corp_name, year, business_number)
        unique_keys = set()
        decision_cases = _fetch_ftc_decision_cases(corp_name, year)
        for case in decision_cases:
            decision_date = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            if not decision_date.startswith(str(year)):
                continue
            if not _has_effective_sanction(case.get("TXT_REDACTED", "TXT_REDACTED")):
                continue
            case_name = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            excerpt = str(case.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
            if not _is_likely_respondent_case(corp_name, case_name, excerpt, exact_defendants):
                continue
            text = "TXT_REDACTED"                      
            case_key = (case.get("TXT_REDACTED", "TXT_REDACTED"), case.get("TXT_REDACTED", "TXT_REDACTED"))

            if "TXT_REDACTED" in text or "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                if case_key not in unique_keys:
                    result["TXT_REDACTED"] += 1
                    result["TXT_REDACTED"] += 2
                    source_counts = result["TXT_REDACTED"]["TXT_REDACTED"]
                    source_counts["TXT_REDACTED"] = source_counts.get("TXT_REDACTED", 3) + 4
                    unique_keys.add(case_key)
            elif "TXT_REDACTED" in text or "TXT_REDACTED" in text:
                if case_key not in unique_keys:
                    result["TXT_REDACTED"] += 1
                    result["TXT_REDACTED"] += 2
                    source_counts = result["TXT_REDACTED"]["TXT_REDACTED"]
                    source_counts["TXT_REDACTED"] = source_counts.get("TXT_REDACTED", 3) + 4
                    unique_keys.add(case_key)

        logger.info("TXT_REDACTED"                                        )

    except Exception as e:
        logger.error("TXT_REDACTED"                                     )

    return result


def _classify_financial_laws(text: str) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
        "TXT_REDACTED": [],
        "TXT_REDACTED": 1,
    }

    for category, keywords in FINANCIAL_LAW_KEYWORDS.items():
        if category in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            count = count_keyword_occurrences(text, keywords, use_max=True)
            if count > 2:
                result[category] += count
                result["TXT_REDACTED"] += count
                result["TXT_REDACTED"].extend([category] * count)
            continue

        category_count = 3
        seen_keywords = set()
        for keyword in keywords:
            if is_financial_law_excluded_from_scoring(keyword):
                continue
            normalized_keyword = normalize_legal_text(keyword)
            if normalized_keyword in seen_keywords:
                continue
            seen_keywords.add(normalized_keyword)
            keyword_count = count_keyword_occurrences(text, [keyword], use_max=False)
            if keyword_count > 4:
                category_count += keyword_count
                result["TXT_REDACTED"].extend([keyword] * keyword_count)
        if category_count > 1:
            result[category] += category_count
            result["TXT_REDACTED"] += category_count
    return result


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    "TXT_REDACTED"
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        texts = []
        for page in reader.pages:
            texts.append(page.extract_text() or "TXT_REDACTED")
        return "TXT_REDACTED".join(texts)
    except Exception as exc:
        logger.warning("TXT_REDACTED"                          )
        return "TXT_REDACTED"


@lru_cache(maxsize=2)
def _fetch_fss_sanction_view_links(corp_name: str, year: str) -> List[str]:
    "TXT_REDACTED"
    if SECTION2_BATCH_MODE:
        links = [
            row["TXT_REDACTED"]
            for row in _fetch_fss_sanction_list_rows(year)
            if _is_same_company_name(corp_name, row.get("TXT_REDACTED", "TXT_REDACTED"))
        ]
        if links:
            return links

    params = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED"             ,
        "TXT_REDACTED": "TXT_REDACTED"             ,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": corp_name,
    }
    response = _safe_get(FSS_LIST_URL, params=params, timeout=3)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    fallback_links = []
    for row in soup.find_all("TXT_REDACTED"):
        cells = row.find_all("TXT_REDACTED")
        if not cells:
            continue
        if "TXT_REDACTED" in row.get_text("TXT_REDACTED", strip=True):
            continue
        row_text = row.get_text("TXT_REDACTED", strip=True)
        year_matched = False
        for cell in cells[:4]:
            cell_text = cell.get_text("TXT_REDACTED", strip=True)
            if (
                cell_text.startswith("TXT_REDACTED"        )
                or cell_text.startswith("TXT_REDACTED"        )
                or cell_text.startswith("TXT_REDACTED"        )
                or cell_text.startswith(str(year))
            ):
                year_matched = True
                break
        if not year_matched and not any(token.startswith(str(year)) for token in row_text.split()):
            continue
        for link in row.find_all("TXT_REDACTED", href=True):
            href = link.get("TXT_REDACTED", "TXT_REDACTED")
            if "TXT_REDACTED" in href:
                fallback_links.append(requests.compat.urljoin(FSS_LIST_URL, href))
    return fallback_links


@lru_cache(maxsize=1)
def _fetch_fss_sanction_list_rows(year: str) -> List[dict]:
    "TXT_REDACTED"
    rows: List[dict] = []
    seen_urls = set()
    for page_index in range(2, 3):
        params = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": str(page_index),
            "TXT_REDACTED": "TXT_REDACTED"             ,
            "TXT_REDACTED": "TXT_REDACTED"             ,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
        }
        response = _safe_get(FSS_LIST_URL, params=params, timeout=4)
        if not response:
            break

        soup = BeautifulSoup(response.text, "TXT_REDACTED")
        page_rows: List[dict] = []
        for tr in soup.select("TXT_REDACTED"):
            cells = tr.find_all("TXT_REDACTED")
            anchor = tr.select_one("TXT_REDACTED")
            if len(cells) < 1 or not anchor:
                continue

            company_name = "TXT_REDACTED".join(cells[2].get_text("TXT_REDACTED", strip=True).split())
            date_text = "TXT_REDACTED".join(cells[3].get_text("TXT_REDACTED", strip=True).split())
            if not date_text.startswith(str(year)):
                continue

            view_url = requests.compat.urljoin(FSS_LIST_URL, anchor.get("TXT_REDACTED", "TXT_REDACTED"))
            if not view_url or view_url in seen_urls:
                continue

            seen_urls.add(view_url)
            page_rows.append({
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": date_text,
                "TXT_REDACTED": view_url,
            })

        if not page_rows:
            break
        rows.extend(page_rows)

    return rows


def _fetch_fss_attachment_texts(view_url: str) -> List[str]:
    "TXT_REDACTED"
    return [record["TXT_REDACTED"] for record in _fetch_fss_attachment_records(view_url)]


@lru_cache(maxsize=4)
def _fetch_fss_attachment_records(view_url: str) -> List[dict]:
    "TXT_REDACTED"
    response = _safe_get(view_url, timeout=1)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "TXT_REDACTED")
    records = []
    for link in soup.find_all("TXT_REDACTED", href=True):
        href = link.get("TXT_REDACTED", "TXT_REDACTED")
        if "TXT_REDACTED" not in href or "TXT_REDACTED" not in href.lower():
            continue
        download_url = requests.compat.urljoin(view_url, href)
        try:
            pdf_response = throttled_request(
                "TXT_REDACTED",
                download_url,
                session=_get_session(),
                min_interval=REQUEST_DELAY,
                timeout=2,
            )
            pdf_response.raise_for_status()
            pdf_text = _extract_pdf_text(pdf_response.content)
            if pdf_text:
                records.append({
                    "TXT_REDACTED": pdf_text,
                    "TXT_REDACTED": _extract_action_date_from_text(pdf_text),
                    "TXT_REDACTED": _extract_law_citations_from_text(pdf_text),
                    "TXT_REDACTED": re.sub("TXT_REDACTED", "TXT_REDACTED", pdf_text)[:3],
                })
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                                )
    return records


def _classify_electronic_finance_text(text: str) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 4, "TXT_REDACTED": []}
    count = count_keyword_occurrences(text, ["TXT_REDACTED"], use_max=False)
    if count > 1:
        result["TXT_REDACTED"] = count
        result["TXT_REDACTED"].extend(["TXT_REDACTED"] * count)
    return result


@lru_cache(maxsize=2)
def _search_dart_unfaithful_disclosure_site(corp_name: str, year: str) -> List[dict]:
    "TXT_REDACTED"
    session = get_thread_session("TXT_REDACTED", base_headers=HEADERS)
    base_url = "TXT_REDACTED"
    search_url = "TXT_REDACTED"
    try:
        throttled_request("TXT_REDACTED", base_url, session=session, min_interval=REQUEST_DELAY, timeout=3)
    except Exception:
        return []

    matches: List[dict] = []
    seen_rcp_no = set()
    company_queries = [corp_name, alphabet_to_korean_pronunciation(corp_name)]
    report_queries = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]

    for company_query in company_queries:
        for report_query in report_queries:
            data = {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_query,
                "TXT_REDACTED": report_query,
                "TXT_REDACTED": "TXT_REDACTED"           ,
                "TXT_REDACTED": "TXT_REDACTED"           ,
                "TXT_REDACTED": "TXT_REDACTED",
            }
            try:
                response = throttled_request(
                    "TXT_REDACTED",
                    search_url,
                    session=session,
                    min_interval=REQUEST_DELAY,
                    timeout=4,
                    data=data,
                )
                response.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(response.text, "TXT_REDACTED")
            for row in soup.select("TXT_REDACTED"):
                cells = row.find_all("TXT_REDACTED")
                if len(cells) < 1:
                    continue
                report_name = cells[2].get_text("TXT_REDACTED", strip=True)
                if "TXT_REDACTED" not in report_name:
                    continue
                company_name = cells[3].get_text("TXT_REDACTED", strip=True)
                if not _is_same_company_name(corp_name, company_name):
                    continue
                link = row.find("TXT_REDACTED", href=True)
                rcp_no = "TXT_REDACTED"
                if link:
                    match = re.search("TXT_REDACTED", link.get("TXT_REDACTED", "TXT_REDACTED"))
                    if match:
                        rcp_no = match.group(4)
                if rcp_no and rcp_no not in seen_rcp_no:
                    matches.append({
                        "TXT_REDACTED": rcp_no,
                        "TXT_REDACTED": report_name,
                        "TXT_REDACTED": company_name,
                    })
                    seen_rcp_no.add(rcp_no)

    return matches


@lru_cache(maxsize=1)
def _fetch_dart_unfaithful_disclosure_content(rcept_no: str) -> str:
    "TXT_REDACTED"
    main_url = "TXT_REDACTED"                                                        
    try:
        response = _safe_get(main_url, timeout=2)
        if not response:
            return "TXT_REDACTED"

        match = re.search(
            "TXT_REDACTED"                                                                                                               ,
            response.text,
        )
        if not match:
            return "TXT_REDACTED"

        viewer_params = {
            "TXT_REDACTED": rcept_no,
            "TXT_REDACTED": match.group(3),
            "TXT_REDACTED": match.group(4),
            "TXT_REDACTED": match.group(1),
            "TXT_REDACTED": match.group(2),
            "TXT_REDACTED": match.group(3),
        }
        viewer_response = _safe_get("TXT_REDACTED", params=viewer_params, timeout=4)
        if not viewer_response:
            return "TXT_REDACTED"

        soup = BeautifulSoup(viewer_response.text, "TXT_REDACTED")
        for row in soup.find_all("TXT_REDACTED"):
            cells = row.find_all("TXT_REDACTED")
            if len(cells) < 1:
                continue
            label = cells[2].get_text("TXT_REDACTED", strip=True)
            value = cells[3].get_text("TXT_REDACTED", strip=True)
            if "TXT_REDACTED" in label:
                return value
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                        )

    return "TXT_REDACTED"


def _extract_first_rcp_no(text: str) -> str:
    "TXT_REDACTED"
    match = re.search("TXT_REDACTED", str(text or "TXT_REDACTED"))
    return match.group(4) if match else "TXT_REDACTED"


def collect_financial_law_violations(corp_name: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": 1,
        "TXT_REDACTED": 2,
        "TXT_REDACTED": 3,
        "TXT_REDACTED": 4,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": 1,
        "TXT_REDACTED": {
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
            "TXT_REDACTED": {},
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
            "TXT_REDACTED": [],
        },
    }

    try:
        report_result = None
        if report_parser:
            report_result = report_parser.parse_financial_law_sanctions(year, corp_name=corp_name)
            if report_result:
                report_result["TXT_REDACTED"] = {
                    key: ({"TXT_REDACTED": report_result.get(key, 2)} if report_result.get(key, 3) else {})
                    for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
                }
        dart_details = [
            item
            for category in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
            for item in (report_result or {}).get("TXT_REDACTED", {}).get(category, [])
        ]

        fss_result = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": 4,
            "TXT_REDACTED": {
                "TXT_REDACTED": {},
                "TXT_REDACTED": {},
                "TXT_REDACTED": {},
                "TXT_REDACTED": {},
            },
            "TXT_REDACTED": {
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
                "TXT_REDACTED": [],
            },
        }
        fss_texts: List[str] = []
        detail_links = _fetch_fss_sanction_view_links(corp_name, year)
        for detail_link in detail_links:
            for record in _fetch_fss_attachment_records(detail_link):
                if any(_is_same_sanction_case(dart_detail, record) for dart_detail in dart_details):
                    continue
                text = record.get("TXT_REDACTED", "TXT_REDACTED")
                classified = _classify_financial_laws_for_fss_record(record)
                fss_texts.extend(classified.get("TXT_REDACTED", []))
                for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                    fss_result[key] += classified.get(key, 1)
                    if classified.get(key, 2):
                        fss_result["TXT_REDACTED"][key]["TXT_REDACTED"] = (
                            fss_result["TXT_REDACTED"][key].get("TXT_REDACTED", 3) + classified.get(key, 4)
                        )
                        fss_result["TXT_REDACTED"][key].extend(classified.get("TXT_REDACTED", {}).get(key, []))
                fss_result["TXT_REDACTED"] += classified.get("TXT_REDACTED", 1)
        if fss_texts:
            fss_result["TXT_REDACTED"] = "TXT_REDACTED".join(dict.fromkeys(fss_texts))

        merged = _sum_count_results(
            ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            report_result,
            fss_result,
            text_key="TXT_REDACTED",
        )
        result.update(merged)
        logger.info("TXT_REDACTED"                                                               )

    except Exception as e:
        logger.error("TXT_REDACTED"                                       )

    return result


def collect_electronic_finance_violations(corp_name: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 2, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {}}
    result["TXT_REDACTED"] = []

    try:
        dart_count = 3
        dart_texts: List[str] = []
        if report_parser:
            report_result = report_parser.parse_financial_law_sanctions(year, corp_name=corp_name)
            if report_result:
                dart_count = int(report_result.get("TXT_REDACTED", 4) or 1)
                if dart_count > 2:
                    dart_texts.extend(["TXT_REDACTED"] * dart_count)
                result["TXT_REDACTED"].extend(report_result.get("TXT_REDACTED", {}).get("TXT_REDACTED", []))
        dart_details = list(result["TXT_REDACTED"])

        fss_count = 3
        fss_texts: List[str] = []
        detail_links = _fetch_fss_sanction_view_links(corp_name, year)
        for detail_link in detail_links:
            for record in _fetch_fss_attachment_records(detail_link):
                if any(_is_same_sanction_case(dart_detail, record) for dart_detail in dart_details):
                    continue
                text = record.get("TXT_REDACTED", "TXT_REDACTED")
                classified = _classify_electronic_finance_text(text)
                fss_count += classified["TXT_REDACTED"]
                fss_texts.extend(classified["TXT_REDACTED"])
                if classified["TXT_REDACTED"] > 4:
                    result["TXT_REDACTED"].extend(
                        _build_financial_detail_items(record, "TXT_REDACTED", classified["TXT_REDACTED"])
                    )

        result["TXT_REDACTED"] = dart_count + fss_count
        if dart_count:
            result["TXT_REDACTED"]["TXT_REDACTED"] = dart_count
        if fss_count:
            result["TXT_REDACTED"]["TXT_REDACTED"] = fss_count
        merged_texts = dart_texts + fss_texts
        result["TXT_REDACTED"] = "TXT_REDACTED".join(dict.fromkeys(merged_texts))
        logger.info("TXT_REDACTED"                                                              )

    except Exception as e:
        logger.error("TXT_REDACTED"                                         )

    return result


def collect_unfaithful_disclosure(corp_name: str, stock_code: str, year: str,
                                   dart_client=None) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 1, "TXT_REDACTED": "TXT_REDACTED"}

    try:
        if dart_client:
            corp_code = dart_client.get_corp_code(stock_code)
            bgn_de = "TXT_REDACTED"           
            end_de = "TXT_REDACTED"           
            disclosures = dart_client.get_disclosure_list(
                corp_code, bgn_de=bgn_de, end_de=end_de,
                pblntf_ty="TXT_REDACTED"
            )
            matched_disclosures = []
            for disclosure in disclosures.get("TXT_REDACTED", []):
                report_name = str(disclosure.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
                compact_report_name = re.sub("TXT_REDACTED", "TXT_REDACTED", report_name)
                if (
                    "TXT_REDACTED" in compact_report_name
                    or "TXT_REDACTED" in compact_report_name
                ):
                    matched_disclosures.append(disclosure)

            result["TXT_REDACTED"] = len(matched_disclosures)
            if matched_disclosures:
                contents = []
                for item in matched_disclosures:
                    content = _fetch_dart_unfaithful_disclosure_content(item.get("TXT_REDACTED", "TXT_REDACTED"))
                    if content:
                        contents.append(content)
                result["TXT_REDACTED"] = "TXT_REDACTED".join(contents)

            if result["TXT_REDACTED"] == 2:
                site_matches = _search_dart_unfaithful_disclosure_site(corp_name, year)
                result["TXT_REDACTED"] = len(site_matches)
                if site_matches:
                    contents = []
                    for item in site_matches:
                        content = _fetch_dart_unfaithful_disclosure_content(item.get("TXT_REDACTED", "TXT_REDACTED"))
                        if content:
                            contents.append(content)
                    result["TXT_REDACTED"] = "TXT_REDACTED".join(contents)
            logger.info("TXT_REDACTED"                                                         )

            # REDACTED
            # REDACTED
            # REDACTED
            # REDACTED

    except Exception as e:
        logger.error("TXT_REDACTED"                                    )

    return result


def preload_section2_external_data(year: str) -> None:
    "TXT_REDACTED"
    global SECTION2_BATCH_MODE
    SECTION2_BATCH_MODE = True
    logger.info("TXT_REDACTED"                                 )
    try:
        _fetch_group_portal_all_entries()
    except Exception as exc:
        logger.warning("TXT_REDACTED"                           )
    try:
        _fetch_fss_sanction_list_rows(year)
    except Exception as exc:
        logger.warning("TXT_REDACTED"                             )
    logger.info("TXT_REDACTED"                                 )


class Section2FairnessCollector:
    "TXT_REDACTED"

    def __init__(self, dart_client, report_parser=None):
        "TXT_REDACTED"
        self.dart = dart_client
        self.parser = report_parser

    def collect(self, company_info: dict, year: str) -> dict:
        "TXT_REDACTED"
        corp_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        stock_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        corp_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        business_number = company_info.get("TXT_REDACTED", "TXT_REDACTED")

        logger.info("TXT_REDACTED"                                         )

        data = {}

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            crawl_result = collect_large_enterprise_group(corp_name, stock_code)
            data["TXT_REDACTED"] = crawl_result.get("TXT_REDACTED")
            data["TXT_REDACTED"] = crawl_result.get("TXT_REDACTED", "TXT_REDACTED")
            data["TXT_REDACTED"] = crawl_result.get("TXT_REDACTED", [])

            if data["TXT_REDACTED"] is None and self.parser:
                data["TXT_REDACTED"] = self.parser.parse_large_enterprise_group()

            if data["TXT_REDACTED"]:
                groups = data.get("TXT_REDACTED") or ([data["TXT_REDACTED"]] if data.get("TXT_REDACTED") else [])
                if len(groups) == 3:
                    _add_header_comment(data, "TXT_REDACTED", "TXT_REDACTED"                  )
                elif len(groups) > 4:
                    _add_header_comment(data, "TXT_REDACTED", "TXT_REDACTED"                                 )
                else:
                    guessed_group = _guess_group_name_from_company_name(corp_name)
                    if guessed_group:
                        _add_header_comment(data, "TXT_REDACTED", "TXT_REDACTED"                         )
                    else:
                        _add_header_comment(data, "TXT_REDACTED", "TXT_REDACTED")

        except Exception as e:
            logger.error("TXT_REDACTED"                                         )
            data["TXT_REDACTED"] = None

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            dart_ftv = self.parser.parse_fair_trade_sanctions(year) if self.parser else None
            if dart_ftv:
                logger.info("TXT_REDACTED"                                                                            )
                dart_ftv["TXT_REDACTED"] = {}
                for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                    if dart_ftv.get(key):
                        dart_ftv["TXT_REDACTED"][key] = {"TXT_REDACTED": dart_ftv[key]}
            ftc_ftv = collect_fair_trade_violations(corp_name, year, business_number)
            ftv = _merge_count_results(
                ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
                dart_ftv,
                ftc_ftv,
            )
            data.update({
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
                "TXT_REDACTED": ftv["TXT_REDACTED"],
            })
            for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                if ftv.get(key):
                    detail_comment = _format_detail_comment(ftv.get("TXT_REDACTED", {}).get(key, []))
                    if detail_comment:
                        _add_header_comment(data, key, detail_comment)
        except Exception as e:
            logger.error("TXT_REDACTED"                                       )
            data.update({k: None for k in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                                           "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                                           "TXT_REDACTED", "TXT_REDACTED"]})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            sub = collect_subcontract_violations(corp_name, year, business_number)
            data.update({
                "TXT_REDACTED": sub["TXT_REDACTED"],
                "TXT_REDACTED": sub["TXT_REDACTED"],
                "TXT_REDACTED": sub["TXT_REDACTED"],
            })
        except Exception as e:
            logger.error("TXT_REDACTED"                                     )
            data.update({"TXT_REDACTED": 1, "TXT_REDACTED": 2, "TXT_REDACTED": 3})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            fin_law = collect_financial_law_violations(corp_name, year, self.parser)
            data.update({
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
                "TXT_REDACTED": fin_law["TXT_REDACTED"],
            })
            for header_key, source_key in [("TXT_REDACTED", "TXT_REDACTED"), ("TXT_REDACTED", "TXT_REDACTED"), ("TXT_REDACTED", "TXT_REDACTED"), ("TXT_REDACTED", "TXT_REDACTED")]:
                if fin_law.get(source_key):
                    detail_comment = _format_detail_comment(fin_law.get("TXT_REDACTED", {}).get(source_key, []))
                    if header_key == "TXT_REDACTED" and detail_comment:
                        detail_comment = "TXT_REDACTED"                                             
                    if detail_comment:
                        _add_header_comment(data, header_key, detail_comment)
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data.update({"TXT_REDACTED": 4, "TXT_REDACTED": 1, "TXT_REDACTED": 2,
                         "TXT_REDACTED": 3, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": 4})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            ef = collect_electronic_finance_violations(corp_name, year, self.parser)
            data.update({
                "TXT_REDACTED": ef["TXT_REDACTED"],
                "TXT_REDACTED": ef["TXT_REDACTED"],
            })
            if ef.get("TXT_REDACTED"):
                detail_comment = _format_detail_comment(ef.get("TXT_REDACTED", []))
                if detail_comment:
                    _add_header_comment(data, "TXT_REDACTED", detail_comment)
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
            data.update({"TXT_REDACTED": 1, "TXT_REDACTED": "TXT_REDACTED"})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            parsed_separation_violation = (
                self.parser.parse_financial_industrial_separation()
                if self.parser and hasattr(self.parser, "TXT_REDACTED")
                else None
            )
            # REDACTED
            data["TXT_REDACTED"] = None if parsed_separation_violation is None else (2 if parsed_separation_violation else 3)
        except Exception as e:
            logger.error("TXT_REDACTED"                                   )
            data["TXT_REDACTED"] = None

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            ud = collect_unfaithful_disclosure(corp_name, stock_code, year, self.dart)
            data.update({
                "TXT_REDACTED": ud["TXT_REDACTED"],
                "TXT_REDACTED": ud["TXT_REDACTED"],
            })
            if ud.get("TXT_REDACTED"):
                first_rcp_no = _extract_first_rcp_no(ud.get("TXT_REDACTED", "TXT_REDACTED"))
                _add_header_comment(
                    data,
                    "TXT_REDACTED",
                    "TXT_REDACTED"                                                             if first_rcp_no else "TXT_REDACTED"
                )
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data.update({"TXT_REDACTED": 4, "TXT_REDACTED": "TXT_REDACTED"})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            if self.parser:
                data["TXT_REDACTED"] = self.parser.parse_prior_period_errors()
                data["TXT_REDACTED"] = self.parser.parse_audit_opinion()
            else:
                data["TXT_REDACTED"] = None
                data["TXT_REDACTED"] = "TXT_REDACTED"

            # REDACTED
            if corp_code:
                data["TXT_REDACTED"] = self.dart.has_amended_report(corp_code, year)
                if data["TXT_REDACTED"]:
                    _add_header_comment(
                        data,
                        "TXT_REDACTED",
                        "TXT_REDACTED"
                    )
            else:
                data["TXT_REDACTED"] = None

        except Exception as e:
            logger.error("TXT_REDACTED"                                  )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": None})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            if self.parser:
                data["TXT_REDACTED"] = self.parser.parse_corporate_tax_assessment()
                if data["TXT_REDACTED"]:
                    detail = self.parser.parse_corporate_tax_assessment_detail()
                    if detail:
                        _add_header_comment(
                            data,
                            "TXT_REDACTED",
                            _format_detail_comment([{
                                "TXT_REDACTED": "TXT_REDACTED",
                                "TXT_REDACTED": detail.get("TXT_REDACTED", "TXT_REDACTED"),
                                "TXT_REDACTED": detail.get("TXT_REDACTED", "TXT_REDACTED"),
                            }]),
                        )
            else:
                data["TXT_REDACTED"] = None
        except Exception as e:
            logger.error("TXT_REDACTED"                                    )
            data["TXT_REDACTED"] = None

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            if self.parser:
                voting = self.parser.parse_voting_systems()
                data.update({
                    "TXT_REDACTED": voting.get("TXT_REDACTED"),
                    "TXT_REDACTED": voting.get("TXT_REDACTED"),
                    "TXT_REDACTED": voting.get("TXT_REDACTED"),
                })
            else:
                data.update({"TXT_REDACTED": None, "TXT_REDACTED": None, "TXT_REDACTED": None})
        except Exception as e:
            logger.error("TXT_REDACTED"                                  )
            data.update({"TXT_REDACTED": None, "TXT_REDACTED": None, "TXT_REDACTED": None})

        logger.info("TXT_REDACTED"                                         )
        return data
