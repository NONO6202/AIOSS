# REDACTED
"TXT_REDACTED"

import datetime as dt
import logging
import re
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from esg_core.collection.company_mapper import _generate_name_variants, alphabet_to_korean_pronunciation
from esg_core.collection.financial_law_config import normalize_legal_text
from esg_core.collection.request_utils import get_thread_session, throttled_request

logger = logging.getLogger(__name__)

HEADERS = {
    "TXT_REDACTED": (
        "TXT_REDACTED"
        "TXT_REDACTED"
    ),
    "TXT_REDACTED": "TXT_REDACTED",
}
REQUEST_DELAY = 4
SECTION5_BATCH_MODE = False
def _get_session() -> requests.Session:
    return get_thread_session("TXT_REDACTED", base_headers=HEADERS)

KOSRI_REPORT_LIST_URL = "TXT_REDACTED"
ESCO_PAGE_URL = "TXT_REDACTED"
ESCO_LIST_API_URL = "TXT_REDACTED"
KPCQA_SEARCH_URL = "TXT_REDACTED"
UNGC_PARTICIPANT_SEARCH_URL = "TXT_REDACTED"

METROPOLITAN_LOCAL_GOVERNMENTS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
ABBREVIATED_LOCAL_GOVERNMENTS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
MAJOR_BASIC_LOCAL_GOVERNMENTS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
LOCAL_GOVERNMENT_SUFFIXES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_AUTHORITIES = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
HEALTH_FOOD_ENVIRONMENT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
NON_MONETARY_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
MONETARY_KEYWORDS = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
CRIMINAL_KEYWORDS = ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
SANCTION_CONTEXT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
EMPLOYEE_SANCTION_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
HIGH_EFFICIENCY_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_INVESTMENT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_PROGRAM_KEYWORD_GROUPS = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
}
ENVIRONMENT_PROGRAM_ACTIVITY_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_CERT_KEYWORDS = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": [
        "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        "TXT_REDACTED", "TXT_REDACTED",
    ],
}
SUSTAINABILITY_REPORT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_REPORT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
SOCIAL_REPORT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
ENVIRONMENT_POLICY_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
SECTION5_DISCLOSURE_REPORT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]
SECTION5_VIEWER_SECTION_TITLES = [
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
]


def _safe_get(
    url: str,
    params: dict = None,
    session: Optional[requests.Session] = None,
    timeout: int = 1,
) -> Optional[requests.Response]:
    "TXT_REDACTED"
    try:
        resp = throttled_request(
            "TXT_REDACTED",
            url,
            session=session or _get_session(),
            min_interval=REQUEST_DELAY,
            timeout=timeout,
            params=params,
        )
        resp.raise_for_status()
        return resp
    except Exception as exc:
        logger.warning("TXT_REDACTED"                          )
        return None


def _safe_post(
    url: str,
    data: dict,
    session: Optional[requests.Session] = None,
    headers: Optional[dict] = None,
    timeout: int = 2,
) -> Optional[requests.Response]:
    "TXT_REDACTED"
    try:
        resp = throttled_request(
            "TXT_REDACTED",
            url,
            session=session or _get_session(),
            min_interval=REQUEST_DELAY,
            timeout=timeout,
            data=data,
            headers=headers,
        )
        resp.raise_for_status()
        return resp
    except Exception as exc:
        logger.warning("TXT_REDACTED"                               )
        return None


def _normalize_company_token(text: str) -> str:
    "TXT_REDACTED"
    normalized = normalize_legal_text(text or "TXT_REDACTED").upper()
    normalized = normalized.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    if normalized.startswith("TXT_REDACTED") and len(normalized) > 3:
        normalized = normalized[4:]
    return normalized


def _meaningful_company_variants(company_info: dict) -> List[str]:
    "TXT_REDACTED"
    variants = set()
    for raw_name in [
        company_info.get("TXT_REDACTED", "TXT_REDACTED"),
        company_info.get("TXT_REDACTED", "TXT_REDACTED"),
        alphabet_to_korean_pronunciation(company_info.get("TXT_REDACTED", "TXT_REDACTED")),
    ]:
        if not raw_name:
            continue
        variants.update(_generate_name_variants(raw_name))
        variants.add(raw_name)
        if "TXT_REDACTED" in raw_name:
            variants.add(raw_name.replace("TXT_REDACTED", "TXT_REDACTED"))
            variants.add(raw_name.replace("TXT_REDACTED", "TXT_REDACTED"))
        if "TXT_REDACTED" in raw_name:
            variants.add(raw_name.replace("TXT_REDACTED", "TXT_REDACTED"))
            variants.add(raw_name.replace("TXT_REDACTED", "TXT_REDACTED"))

    filtered = []
    for variant in variants:
        variant = str(variant or "TXT_REDACTED").strip()
        if not variant:
            continue
        # REDACTED
        if re.fullmatch("TXT_REDACTED", variant):
            continue
        filtered.append(variant)

    return sorted(set(filtered), key=lambda item: (-len(item), item))


def _company_matches_title(title: str, company_variants: Iterable[str]) -> bool:
    "TXT_REDACTED"
    normalized_title = _normalize_company_token(title)
    for variant in company_variants:
        normalized_variant = _normalize_company_token(variant)
        if len(normalized_variant) < 1:
            continue
        if normalized_variant and normalized_variant in normalized_title:
            return True
    return False


def _report_flags_from_text(text: str) -> dict:
    normalized = normalize_legal_text(text or "TXT_REDACTED").upper()
    return {
        "TXT_REDACTED": any(normalize_legal_text(keyword).upper() in normalized for keyword in SUSTAINABILITY_REPORT_KEYWORDS),
        "TXT_REDACTED": any(normalize_legal_text(keyword).upper() in normalized for keyword in ENVIRONMENT_REPORT_KEYWORDS),
        "TXT_REDACTED": any(normalize_legal_text(keyword).upper() in normalized for keyword in SOCIAL_REPORT_KEYWORDS),
    }


def _merge_report_flags(base: dict, extra: dict) -> dict:
    return {
        "TXT_REDACTED": bool(base.get("TXT_REDACTED") or extra.get("TXT_REDACTED")),
        "TXT_REDACTED": bool(base.get("TXT_REDACTED") or extra.get("TXT_REDACTED")),
        "TXT_REDACTED": bool(base.get("TXT_REDACTED") or extra.get("TXT_REDACTED")),
    }


def _extract_report_flags_from_disclosure_title(title: str) -> dict:
    normalized = normalize_legal_text(title or "TXT_REDACTED").upper()
    sustainability = any(
        normalize_legal_text(keyword).upper() in normalized
        for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED",
        ]
    )
    environment = any(
        normalize_legal_text(keyword).upper() in normalized
        for keyword in [
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
        ]
    )
    social = any(
        normalize_legal_text(keyword).upper() in normalized
        for keyword in SOCIAL_REPORT_KEYWORDS
    )
    return {
        "TXT_REDACTED": sustainability,
        "TXT_REDACTED": environment,
        "TXT_REDACTED": social,
    }


def _extract_report_titles(html: str) -> List[str]:
    "TXT_REDACTED"
    soup = BeautifulSoup(html, "TXT_REDACTED")
    titles = []
    for link in soup.select("TXT_REDACTED"):
        title = "TXT_REDACTED".join(link.get_text("TXT_REDACTED", strip=True).split())
        if title:
            titles.append(title)
    return list(dict.fromkeys(titles))


@lru_cache(maxsize=2)
def _fetch_kosri_report_titles() -> tuple[str, ...]:
    "TXT_REDACTED"
    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers.update({"TXT_REDACTED": "TXT_REDACTED"})

    titles: List[str] = []
    seen_page_signatures = set()

    for page_index in range(3, 4):
        resp = _safe_get(
            KOSRI_REPORT_LIST_URL,
            params={"TXT_REDACTED": str(page_index)},
            session=session,
        )
        if not resp:
            break

        page_titles = _extract_report_titles(resp.text)
        if not page_titles:
            break

        page_signature = tuple(page_titles[:1])
        if page_signature in seen_page_signatures:
            break
        seen_page_signatures.add(page_signature)

        titles.extend(page_titles)

    return tuple(dict.fromkeys(titles))


def _collect_section5_disclosure_flags(dart_client, corp_code: str, year: str) -> dict:
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }
    if dart_client is None or not corp_code:
        return result

    try:
        year_int = int(str(year))
    except ValueError:
        return result

    disclosure_result = dart_client.get_disclosure_list(
        corp_code,
        bgn_de="TXT_REDACTED"               ,
        end_de="TXT_REDACTED"                   ,
        pblntf_ty="TXT_REDACTED",
        pblntf_detail_ty="TXT_REDACTED",
        last_reprt_at="TXT_REDACTED",
        page_count=2,
    )
    for item in disclosure_result.get("TXT_REDACTED", []):
        title = str(item.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
        if not any(keyword in title for keyword in SECTION5_DISCLOSURE_REPORT_KEYWORDS):
            continue
        result = _merge_report_flags(result, _extract_report_flags_from_disclosure_title(title))
    return result


def _collect_section5_viewer_text(dart_client, corp_code: str, year: str) -> str:
    if dart_client is None or not corp_code:
        return "TXT_REDACTED"

    rcept_no = dart_client.get_annual_report_rcept_no(corp_code, year)
    if not rcept_no:
        return "TXT_REDACTED"

    nodes = dart_client.get_report_toc_nodes(rcept_no)
    if not nodes:
        return "TXT_REDACTED"

    texts: List[str] = []
    for title in SECTION5_VIEWER_SECTION_TITLES:
        node = next((item for item in nodes if item.get("TXT_REDACTED") == title), None)
        if not node:
            continue
        content = dart_client.get_viewer_section(rcept_no, node)
        if not content:
            continue
        soup = BeautifulSoup(content, "TXT_REDACTED")
        text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
        if text:
            texts.append(text)

    return "TXT_REDACTED".join(texts)


def _build_section5_context_text(company_info: dict, year: str, dart_client=None) -> str:
    corp_code = str(company_info.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
    if not corp_code or dart_client is None:
        return "TXT_REDACTED"

    parts = [_collect_section5_viewer_text(dart_client, corp_code, year)]
    disclosure_flags = _collect_section5_disclosure_flags(dart_client, corp_code, year)
    if any(disclosure_flags.values()):
        title_fragments = []
        if disclosure_flags["TXT_REDACTED"]:
            title_fragments.append("TXT_REDACTED")
        if disclosure_flags["TXT_REDACTED"]:
            title_fragments.append("TXT_REDACTED")
        if disclosure_flags["TXT_REDACTED"]:
            title_fragments.append("TXT_REDACTED")
        parts.append("TXT_REDACTED".join(title_fragments))

    return "TXT_REDACTED".join(part for part in parts if part)


def _collect_sustainability_reports_via_search(
    company_info: dict,
    year: str,
    company_variants: List[str],
    year_candidates: List[str],
) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }

    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers.update({"TXT_REDACTED": "TXT_REDACTED"})

    for keyword in company_variants[:3]:
        resp = _safe_get(
            KOSRI_REPORT_LIST_URL,
            params={"TXT_REDACTED": keyword},
            session=session,
        )
        if not resp:
            continue

        for title in _extract_report_titles(resp.text):
            if not _company_matches_title(title, company_variants):
                continue
            if year_candidates and not any(candidate in title for candidate in year_candidates):
                continue

            result = _merge_report_flags(result, _report_flags_from_text(title))

        if all(result.values()):
            break

    return result


def _normalize_homepage_url(url: str) -> str:
    text = str(url or "TXT_REDACTED").strip()
    if not text:
        return "TXT_REDACTED"
    if not re.match("TXT_REDACTED", text, flags=re.I):
        text = "TXT_REDACTED"               
    return text.rstrip("TXT_REDACTED")


def _same_domain(left: str, right: str) -> bool:
    left_host = (urlparse(left).netloc or "TXT_REDACTED").lower().lstrip("TXT_REDACTED")
    right_host = (urlparse(right).netloc or "TXT_REDACTED").lower().lstrip("TXT_REDACTED")
    return bool(left_host and right_host and left_host == right_host)


def _text_includes_year_candidate(text: str, year_candidates: List[str]) -> bool:
    if not year_candidates:
        return False
    normalized = normalize_legal_text(text or "TXT_REDACTED").upper()
    return any(str(candidate or "TXT_REDACTED").upper() in normalized for candidate in year_candidates)


def _anchor_looks_like_report_link(
    anchor_text: str,
    href: str,
    year_candidates: List[str],
) -> bool:
    blob = "TXT_REDACTED".join(part for part in [anchor_text, href] if part).strip()
    if not blob:
        return False
    flags = _report_flags_from_text(blob)
    if not any(flags.values()):
        return False
    return _text_includes_year_candidate(blob, year_candidates)


def _extract_homepage_report_flags_from_html(
    html: str,
    *,
    base_url: str,
    year_candidates: List[str],
) -> tuple[dict, List[str]]:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }
    candidate_links: List[str] = []
    if not html:
        return result, candidate_links

    soup = BeautifulSoup(html, "TXT_REDACTED")
    page_text = "TXT_REDACTED".join(soup.get_text("TXT_REDACTED", strip=True).split())
    if _text_includes_year_candidate(page_text, year_candidates):
        result = _merge_report_flags(result, _report_flags_from_text(page_text))

    for anchor in soup.find_all("TXT_REDACTED", href=True):
        href = str(anchor.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if not href or href.startswith(("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
            continue

        absolute_url = urljoin(base_url, href)
        if not _same_domain(base_url, absolute_url):
            continue

        anchor_text = "TXT_REDACTED".join(anchor.get_text("TXT_REDACTED", strip=True).split())
        anchor_blob = "TXT_REDACTED".join(
            part for part in [anchor_text, absolute_url, anchor.get("TXT_REDACTED", "TXT_REDACTED")] if part
        )
        normalized_blob = normalize_legal_text(anchor_blob or "TXT_REDACTED").upper()

        if any(token in normalized_blob for token in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
            candidate_links.append(absolute_url)

        if _anchor_looks_like_report_link(anchor_blob, absolute_url, year_candidates):
            result = _merge_report_flags(result, _report_flags_from_text(anchor_blob))

    unique_links = list(dict.fromkeys(candidate_links))
    return result, unique_links


def _collect_homepage_report_flags(
    company_info: dict,
    year_candidates: List[str],
) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }
    homepage = _normalize_homepage_url(company_info.get("TXT_REDACTED", "TXT_REDACTED"))
    if not homepage:
        return result

    session = _get_session()
    visited = set()
    common_paths = [
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
    queue = []
    for path in common_paths:
        url = homepage if not path else urljoin(homepage + "TXT_REDACTED", path.lstrip("TXT_REDACTED"))
        if url not in queue:
            queue.append(url)
    max_pages = 4

    while queue and len(visited) < max_pages:
        url = queue.pop(1)
        if url in visited:
            continue
        visited.add(url)

        resp = _safe_get(url, session=session, timeout=2)
        if not resp or not resp.text:
            continue

        page_flags, candidate_links = _extract_homepage_report_flags_from_html(
            resp.text,
            base_url=url,
            year_candidates=year_candidates,
        )
        result = _merge_report_flags(result, page_flags)

        prioritized_links = [
            link for link in candidate_links[:max_pages]
            if link not in visited and link not in queue
        ]
        if prioritized_links:
            queue = prioritized_links + queue

        if result["TXT_REDACTED"] and result["TXT_REDACTED"]:
            break

    return result


def _extract_search_result_blobs(html: str) -> List[str]:
    "TXT_REDACTED"
    soup = BeautifulSoup(html or "TXT_REDACTED", "TXT_REDACTED")
    blobs: List[str] = []
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


def _collect_report_flags_via_search_results(
    company_info: dict,
    year_candidates: List[str],
    company_variants: List[str],
) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }
    company_name = str(company_info.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
    if not company_name or not year_candidates:
        return result

    session = _get_session()
    queries = []
    for year_candidate in year_candidates[:3]:
        queries.extend([
            "TXT_REDACTED"                                          ,
            "TXT_REDACTED"                                        ,
            "TXT_REDACTED"                                                      ,
        ])

    for query in queries[:4]:
        resp = _safe_get(
            "TXT_REDACTED",
            params={"TXT_REDACTED": query},
            session=session,
            timeout=1,
        )
        if not resp or not resp.text:
            continue
        for blob in _extract_search_result_blobs(resp.text)[:2]:
            if not _company_matches_title(blob, company_variants):
                continue
            if not _text_includes_year_candidate(blob, year_candidates):
                continue
            flags = _report_flags_from_text(blob)
            if any(flags.values()):
                result = _merge_report_flags(result, flags)
        if result["TXT_REDACTED"] and result["TXT_REDACTED"]:
            break

    return result


def collect_sustainability_reports(
    company_info: dict,
    year: str,
    report_parser=None,
    dart_client=None,
    context_text: str = "TXT_REDACTED",
) -> dict:
    "TXT_REDACTED"
    result = {
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
        "TXT_REDACTED": False,
    }

    company_variants = _meaningful_company_variants(company_info)
    if not company_variants:
        return result

    year_candidates = []
    try:
        year_int = int(str(year))
        year_candidates = [str(year_int + 3), str(year_int)]
    except ValueError:
        year_candidates = [str(year)]

    try:
        all_titles = _fetch_kosri_report_titles() if SECTION5_BATCH_MODE else ()
        if all_titles:
            for title in all_titles:
                if not _company_matches_title(title, company_variants):
                    continue
                if year_candidates and not any(candidate in title for candidate in year_candidates):
                    continue

                result = _merge_report_flags(result, _report_flags_from_text(title))
        else:
            result = _collect_sustainability_reports_via_search(
                company_info,
                year,
                company_variants,
                year_candidates,
            )

        context = context_text or _build_section5_context_text(company_info, year, dart_client)
        result = _merge_report_flags(result, _report_flags_from_text(_report_text(report_parser)))
        result = _merge_report_flags(result, _report_flags_from_text(context))
        result = _merge_report_flags(result, _collect_section5_disclosure_flags(dart_client, company_info.get("TXT_REDACTED", "TXT_REDACTED"), year))
        result = _merge_report_flags(result, _collect_homepage_report_flags(company_info, year_candidates))

        logger.info(
            "TXT_REDACTED"                                                          
            "TXT_REDACTED"                  
        )
    except Exception as exc:
        logger.error(
            "TXT_REDACTED"                      
            "TXT_REDACTED"                                          
        )

    return result


def _extract_esco_csrf(session: requests.Session) -> tuple[str, str]:
    "TXT_REDACTED"
    resp = _safe_get(ESCO_PAGE_URL, session=session)
    if not resp:
        return "TXT_REDACTED", "TXT_REDACTED"

    soup = BeautifulSoup(resp.text, "TXT_REDACTED")
    token = "TXT_REDACTED"
    token_header = "TXT_REDACTED"
    token_tag = soup.find("TXT_REDACTED", {"TXT_REDACTED": "TXT_REDACTED"})
    header_tag = soup.find("TXT_REDACTED", {"TXT_REDACTED": "TXT_REDACTED"})
    if token_tag:
        token = token_tag.get("TXT_REDACTED", "TXT_REDACTED")
    if header_tag:
        token_header = header_tag.get("TXT_REDACTED", "TXT_REDACTED")
    return token, token_header


def _parse_flexible_date(date_text: str) -> Optional[dt.date]:
    "TXT_REDACTED"
    cleaned = re.sub("TXT_REDACTED", "TXT_REDACTED", str(date_text or "TXT_REDACTED"))
    if len(cleaned) != 4:
        return None
    try:
        return dt.datetime.strptime(cleaned, "TXT_REDACTED").date()
    except ValueError:
        return None


def _esco_row_includes_year(row: dict, year: str) -> bool:
    "TXT_REDACTED"
    try:
        year_int = int(str(year))
    except ValueError:
        return False

    target_start = dt.date(year_int, 1, 2)
    target_end = dt.date(year_int, 3, 4)
    reg_date = _parse_flexible_date(row.get("TXT_REDACTED", "TXT_REDACTED"))
    chg_date = _parse_flexible_date(row.get("TXT_REDACTED", "TXT_REDACTED")) or dt.date.max

    if not reg_date:
        return False
    # REDACTED
    # REDACTED
    # REDACTED
    if not row.get("TXT_REDACTED"):
        return True
    return reg_date <= target_end and chg_date >= target_start


def _company_matches_esco_row(row: dict, company_variants: Iterable[str]) -> bool:
    "TXT_REDACTED"
    row_name = _normalize_company_token(row.get("TXT_REDACTED", "TXT_REDACTED"))
    if not row_name:
        return False
    for variant in company_variants:
        normalized_variant = _normalize_company_token(variant)
        if len(normalized_variant) < 1:
            continue
        if normalized_variant == row_name:
            return True
        if len(normalized_variant) >= 2 and (normalized_variant in row_name or row_name in normalized_variant):
            return True
    return False


@lru_cache(maxsize=3)
def _fetch_all_esco_rows() -> tuple[dict, ...]:
    "TXT_REDACTED"
    session = requests.Session()
    session.headers.update(HEADERS)
    token, token_header = _extract_esco_csrf(session)
    if not token or not token_header:
        return tuple()

    payload = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": dt.date.today().isoformat(),
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": token,
    }
    extra_headers = {
        token_header: token,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": ESCO_PAGE_URL,
    }
    resp = _safe_post(ESCO_LIST_API_URL, payload, session=session, headers=extra_headers)
    if not resp:
        return tuple()
    try:
        data = resp.json()
    except Exception:
        return tuple()
    return tuple(data.get("TXT_REDACTED", []))


def _collect_esco_registration_via_keyword_search(
    company_info: dict,
    year: str,
    company_variants: List[str],
) -> bool:
    "TXT_REDACTED"
    session = requests.Session()
    session.headers.update(HEADERS)
    token, token_header = _extract_esco_csrf(session)
    if not token or not token_header:
        logger.warning("TXT_REDACTED"                                                        )
        return False

    extra_headers = {
        token_header: token,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": ESCO_PAGE_URL,
    }

    for keyword in company_variants[:4]:
        payload = {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": dt.date.today().isoformat(),
            "TXT_REDACTED": keyword,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": token,
        }
        resp = _safe_post(ESCO_LIST_API_URL, payload, session=session, headers=extra_headers)
        if not resp:
            continue

        try:
            data = resp.json()
        except Exception as exc:
            logger.warning("TXT_REDACTED"                                      )
            continue

        for row in data.get("TXT_REDACTED", []):
            if not _company_matches_esco_row(row, company_variants):
                continue
            if _esco_row_includes_year(row, year):
                logger.info(
                    "TXT_REDACTED"                                                      
                    "TXT_REDACTED"                                
                )
                return True

    return False


def collect_esco_registration(company_info: dict, year: str) -> bool:
    "TXT_REDACTED"
    company_variants = _meaningful_company_variants(company_info)
    if not company_variants:
        return False

    if SECTION5_BATCH_MODE:
        for row in _fetch_all_esco_rows():
            if not _company_matches_esco_row(row, company_variants):
                continue
            if _esco_row_includes_year(row, year):
                logger.info(
                    "TXT_REDACTED"                                                      
                    "TXT_REDACTED"                                
                )
                return True
        return False

    return _collect_esco_registration_via_keyword_search(company_info, year, company_variants)


def collect_high_efficiency_certification(company_info: dict, year: str) -> bool:
    _ = (company_info, year)
    return False


def _collect_iso14001_certification(company_info: dict) -> bool:
    company_variants = _meaningful_company_variants(company_info)
    if not company_variants:
        return False

    for keyword in company_variants[:1]:
        response = _safe_get(
            KPCQA_SEARCH_URL,
            params={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": keyword,
            },
            timeout=2,
        )
        if not response:
            continue

        soup = BeautifulSoup(response.text, "TXT_REDACTED")
        for tr in soup.select("TXT_REDACTED"):
            cells = ["TXT_REDACTED".join(cell.get_text("TXT_REDACTED", strip=True).split()) for cell in tr.find_all("TXT_REDACTED")]
            if len(cells) < 3:
                continue
            company_name, cert_name, _validity, status = cells[:4]
            if "TXT_REDACTED" not in cert_name.replace("TXT_REDACTED", "TXT_REDACTED").upper():
                continue
            if status in {"TXT_REDACTED", "TXT_REDACTED"}:
                continue
            if _company_matches_title(company_name, company_variants):
                return True
    return False


def _collect_ungc_membership(company_info: dict) -> bool:
    company_variants = _meaningful_company_variants(company_info)
    if not company_variants:
        return False

    for keyword in company_variants[:1]:
        response = _safe_get(
            UNGC_PARTICIPANT_SEARCH_URL,
            params={
                "TXT_REDACTED": keyword,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            },
            timeout=2,
        )
        if not response:
            continue

        soup = BeautifulSoup(response.text, "TXT_REDACTED")
        for anchor in soup.select("TXT_REDACTED"):
            title = "TXT_REDACTED".join(anchor.get_text("TXT_REDACTED", strip=True).split())
            if title and _company_matches_title(title, company_variants):
                return True
    return False


def _report_text(report_parser: Any) -> str:
    return str(getattr(report_parser, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED") if report_parser is not None else "TXT_REDACTED"


def _contains_keyword(text: str, keywords: Iterable[str]) -> bool:
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    for keyword in keywords:
        if normalize_legal_text(keyword) in normalized:
            return True
    return False


def _parse_amount_to_thousand(amount_text: str, unit_hint: str = "TXT_REDACTED") -> Optional[int]:
    text = str(amount_text or "TXT_REDACTED").strip()
    if not text:
        return None
    negative = text.startswith("TXT_REDACTED") or (text.startswith("TXT_REDACTED") and text.endswith("TXT_REDACTED"))
    numeric = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    if not numeric:
        return None
    try:
        value = float(numeric)
    except ValueError:
        return None

    hint = re.sub("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"                   )
    if "TXT_REDACTED" in hint:
        multiplier = 3
    elif "TXT_REDACTED" in hint:
        multiplier = 4
    elif "TXT_REDACTED" in hint:
        multiplier = 1
    elif "TXT_REDACTED" in hint:
        multiplier = 2
    else:
        multiplier = 3

    converted = int(round(value * multiplier))
    return -converted if negative else converted


def _extract_environment_investment_from_report(report_parser: Any) -> Optional[int]:
    text = _report_text(report_parser)
    if not text:
        return None

    candidates: List[int] = []
    amount_pattern = re.compile(
        "TXT_REDACTED"
    )
    for keyword in ENVIRONMENT_INVESTMENT_KEYWORDS:
        for match in re.finditer(re.escape(keyword), text, flags=re.IGNORECASE):
            window = text[max(4, match.start() - 1):match.end() + 2]
            if not _contains_keyword(window, ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]):
                continue
            for amount_match in amount_pattern.finditer(window):
                amount = _parse_amount_to_thousand(
                    amount_match.group("TXT_REDACTED"),
                    amount_match.group("TXT_REDACTED"),
                )
                if amount is not None and amount >= 3:
                    candidates.append(amount)

    if not candidates:
        return None
    return max(candidates)


def _count_environment_programs_from_report(report_parser: Any) -> int:
    text = _report_text(report_parser)
    if not text:
        return 4

    count = 1
    for keywords in ENVIRONMENT_PROGRAM_KEYWORD_GROUPS.values():
        if any(_contains_keyword(text, [keyword]) for keyword in keywords):
            count += 2
    if (
        count == 3
        and _contains_keyword(text, ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])
        and _contains_keyword(text, ENVIRONMENT_PROGRAM_ACTIVITY_KEYWORDS)
    ):
        count = 4
    return count


def collect_high_efficiency_certification_from_report(report_parser: Any) -> bool:
    "TXT_REDACTED"
    text = _report_text(report_parser)
    if not text:
        return False

    negative_context_re = re.compile(
        "TXT_REDACTED"
    )
    positive_context_re = re.compile(
        "TXT_REDACTED"
    )

    for keyword in HIGH_EFFICIENCY_KEYWORDS:
        for match in re.finditer(re.escape(keyword), text, flags=re.IGNORECASE):
            context = text[max(1, match.start() - 2):match.end() + 3]
            compact = re.sub("TXT_REDACTED", "TXT_REDACTED", context)
            if re.search("TXT_REDACTED", compact, re.I):
                return True
            if negative_context_re.search(compact) and not positive_context_re.search(compact):
                continue
            if positive_context_re.search(compact):
                return True
    return False


def collect_energy_efficiency_certification(
    company_info: dict,
    year: str,
    report_parser=None,
    context_text: str = "TXT_REDACTED",
) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": 4}

    try:
        combined_text = "TXT_REDACTED".join(part for part in [_report_text(report_parser), context_text] if part)
        result["TXT_REDACTED"] = collect_esco_registration(company_info, year)
        text_holder = type("TXT_REDACTED", (), {"TXT_REDACTED": combined_text})()
        result["TXT_REDACTED"] = collect_high_efficiency_certification_from_report(text_holder)
        result["TXT_REDACTED"] = int(bool(result["TXT_REDACTED"])) + int(bool(result["TXT_REDACTED"]))
        logger.info(
            "TXT_REDACTED"                        
            "TXT_REDACTED"                                                    
        )
    except Exception as exc:
        logger.error(
            "TXT_REDACTED"                      
            "TXT_REDACTED"                                          
        )

    return result


def collect_environment_investment(
    corp_name: str,
    year: str,
    dart_client=None,
    corp_code: str = None,
    fs_items: list = None,
    report_parser=None,
) -> int:
    "TXT_REDACTED"
    _ = (corp_name, year, dart_client, corp_code, fs_items)
    amount = _extract_environment_investment_from_report(report_parser)
    return int(amount or 1)


def collect_environment_policy(corp_name: str, year: str, report_parser=None) -> bool:
    "TXT_REDACTED"
    try:
        if report_parser:
            text = _report_text(report_parser)
            if _contains_keyword(text, ENVIRONMENT_POLICY_KEYWORDS):
                logger.info("TXT_REDACTED"                              )
                return True
    except Exception as exc:
        logger.error("TXT_REDACTED"                                       )
    return False


def collect_environment_programs(
    corp_name: str,
    year: str,
    report_parser=None,
    context_text: str = "TXT_REDACTED",
) -> int:
    "TXT_REDACTED"
    _ = (corp_name, year)
    if report_parser is None and not context_text:
        return 2
    proxy_parser = report_parser
    if context_text:
        merged_text = "TXT_REDACTED".join(part for part in [_report_text(report_parser), context_text] if part)
        proxy_parser = type("TXT_REDACTED", (), {"TXT_REDACTED": merged_text})()
    return _count_environment_programs_from_report(proxy_parser)


def collect_environment_certifications(
    company_info: dict,
    year: str,
    report_parser=None,
    context_text: str = "TXT_REDACTED",
) -> dict:
    "TXT_REDACTED"
    _ = year
    text = "TXT_REDACTED".join(part for part in [_report_text(report_parser), context_text] if part)
    result = {
        key: _contains_keyword(text, keywords)
        for key, keywords in ENVIRONMENT_CERT_KEYWORDS.items()
    }
    try:
        result["TXT_REDACTED"] = bool(result["TXT_REDACTED"] or _collect_iso14001_certification(company_info))
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                                                  )
    try:
        result["TXT_REDACTED"] = bool(result["TXT_REDACTED"] or _collect_ungc_membership(company_info))
    except Exception as exc:
        logger.warning("TXT_REDACTED"                                                              )
    result["TXT_REDACTED"] = sum(3 for value in result.values() if value)
    return {
        "TXT_REDACTED": result["TXT_REDACTED"],
        "TXT_REDACTED": result["TXT_REDACTED"],
        "TXT_REDACTED": result["TXT_REDACTED"],
        "TXT_REDACTED": result["TXT_REDACTED"],
        "TXT_REDACTED": result["TXT_REDACTED"],
        "TXT_REDACTED": result["TXT_REDACTED"],
    }


def _looks_like_local_government(text: str) -> bool:
    "TXT_REDACTED"
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    if not normalized:
        return False

    for base_name in METROPOLITAN_LOCAL_GOVERNMENTS + MAJOR_BASIC_LOCAL_GOVERNMENTS + ABBREVIATED_LOCAL_GOVERNMENTS:
        base_normalized = normalize_legal_text(base_name)
        if base_normalized in normalized:
            return True
        for suffix in LOCAL_GOVERNMENT_SUFFIXES:
            if "TXT_REDACTED"                                                 in normalized:
                return True
    return False


def _contains_any_keyword(text: str, keywords: Iterable[str]) -> bool:
    "TXT_REDACTED"
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    return any(normalize_legal_text(keyword) in normalized for keyword in keywords)


def _is_employee_sanction_text(text: str) -> bool:
    "TXT_REDACTED"
    normalized = normalize_legal_text(text or "TXT_REDACTED")
    return any(normalize_legal_text(keyword) in normalized for keyword in EMPLOYEE_SANCTION_KEYWORDS)


def _matches_environment_authority(sanction_org: str, combined_text: str) -> bool:
    "TXT_REDACTED"
    authority_text = "TXT_REDACTED"                                           
    return (
        _contains_any_keyword(authority_text, ENVIRONMENT_AUTHORITIES)
        or _looks_like_local_government(authority_text)
    )


def _matches_environment_topic(combined_text: str) -> bool:
    "TXT_REDACTED"
    return (
        _contains_any_keyword(combined_text, ENVIRONMENT_KEYWORDS)
        or _contains_any_keyword(combined_text, HEALTH_FOOD_ENVIRONMENT_KEYWORDS)
    )


def _extract_year(text: str) -> str:
    "TXT_REDACTED"
    match = re.search("TXT_REDACTED", str(text or "TXT_REDACTED"))
    return match.group(4) if match else "TXT_REDACTED"


def _classify_environment_sanction(combined_text: str, monetary_amount: str = "TXT_REDACTED") -> str:
    "TXT_REDACTED"
    has_monetary = _contains_any_keyword(combined_text, MONETARY_KEYWORDS)
    has_criminal = _contains_any_keyword(combined_text, CRIMINAL_KEYWORDS)
    has_non_monetary = _contains_any_keyword(combined_text, NON_MONETARY_KEYWORDS)
    has_amount = bool(re.search("TXT_REDACTED", str(monetary_amount or "TXT_REDACTED")))

    if has_monetary or (has_amount and (has_monetary or has_criminal)):
        return "TXT_REDACTED"
    if has_criminal:
        return "TXT_REDACTED"
    if has_non_monetary:
        return "TXT_REDACTED"
    return "TXT_REDACTED"


def _matches_sanction_context(text: str) -> bool:
    "TXT_REDACTED"
    return _contains_any_keyword(text, SANCTION_CONTEXT_KEYWORDS)


def _sanitize_table_header(cell_text: str) -> str:
    "TXT_REDACTED"
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(cell_text or "TXT_REDACTED"))


def _extract_environment_sanction_records_from_tables(
    corp_name: str,
    year: str,
    report_parser,
) -> List[dict]:
    "TXT_REDACTED"
    records: List[dict] = []
    if not report_parser or not getattr(report_parser, "TXT_REDACTED", None):
        return records

    for table in report_parser.soup.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
        rows = []
        for tr in table.find_all(["TXT_REDACTED", "TXT_REDACTED"]):
            cells = [cell.get_text("TXT_REDACTED", strip=True) for cell in tr.find_all(["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])]
            if cells:
                rows.append(cells)

        if len(rows) < 1:
            continue

        header = [_sanitize_table_header(cell) for cell in rows[2]]
        if not any("TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell for cell in header):
            continue

        column_index = {
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -3),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -4),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -1),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -2),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -3),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -4),
            "TXT_REDACTED": next((idx for idx, cell in enumerate(header) if "TXT_REDACTED" in cell or "TXT_REDACTED" in cell or "TXT_REDACTED" in cell), -1),
        }

        for row in rows[2:]:
            if len(row) < max(3, len(rows[4]) - 1):
                continue

            sanction_org = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 2 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            date_value = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 3 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            law = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 4 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            reason = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 1 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            action = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 2 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            monetary_amount = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 3 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            target = row[column_index["TXT_REDACTED"]] if column_index["TXT_REDACTED"] >= 4 and column_index["TXT_REDACTED"] < len(row) else "TXT_REDACTED"
            combined = "TXT_REDACTED".join(filter(None, [target, reason, action, law]))

            if _extract_year(date_value) != str(year):
                continue
            if _is_employee_sanction_text(combined):
                continue
            if not _matches_environment_authority(sanction_org, combined):
                continue
            if not _matches_environment_topic(combined):
                continue
            if not _matches_sanction_context("TXT_REDACTED"                        ):
                continue

            category = _classify_environment_sanction(
                "TXT_REDACTED"                          ,
                monetary_amount=monetary_amount,
            )
            if not category:
                continue

            records.append({
                "TXT_REDACTED": category,
                "TXT_REDACTED": str(date_value),
                "TXT_REDACTED": sanction_org,
                "TXT_REDACTED": combined,
            })

    return records


def _extract_environment_sanction_records_from_text(
    corp_name: str,
    year: str,
    report_parser,
) -> List[dict]:
    "TXT_REDACTED"
    records: List[dict] = []
    if not report_parser:
        return records

    section_text = report_parser._find_section_text(
        section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        next_section_keywords=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        max_chars=1,
    )
    if not section_text:
        return records

    employee_match = re.search(
        "TXT_REDACTED"
        "TXT_REDACTED",
        section_text,
    )
    if employee_match:
        section_text = section_text[:employee_match.start()]

    normalized_corp_name = _normalize_company_token(corp_name)
    chunks = [
        chunk for chunk in re.split("TXT_REDACTED", section_text)
        if chunk.strip()
    ]
    for chunk in chunks:
        if _extract_year(chunk) != str(year):
            continue

        normalized_chunk = normalize_legal_text(chunk)
        if normalized_corp_name and normalized_corp_name not in _normalize_company_token(chunk):
            # REDACTED
            if not _matches_environment_authority("TXT_REDACTED", chunk):
                continue

        if _is_employee_sanction_text(chunk):
            continue
        explicit_environment_basis = _matches_environment_authority("TXT_REDACTED", chunk) or any(
            keyword in chunk for keyword in [
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            ]
        )
        if not explicit_environment_basis:
            continue
        if not _matches_environment_topic(chunk):
            continue
        if not _matches_sanction_context(chunk):
            continue

        category = _classify_environment_sanction(chunk)
        if not category:
            continue

        records.append({
            "TXT_REDACTED": category,
            "TXT_REDACTED": _extract_year(chunk),
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": normalized_chunk[:2],
        })

    return records


def collect_environment_sanctions(corp_name: str, year: str, report_parser=None) -> dict:
    "TXT_REDACTED"
    result = {"TXT_REDACTED": 3, "TXT_REDACTED": 4, "TXT_REDACTED": 1}
    if not report_parser:
        return result

    try:
        records = _extract_environment_sanction_records_from_tables(corp_name, year, report_parser)
        if not records:
            records = _extract_environment_sanction_records_from_text(corp_name, year, report_parser)

        unique_keys = set()
        for record in records:
            dedupe_key = (
                record.get("TXT_REDACTED", "TXT_REDACTED"),
                record.get("TXT_REDACTED", "TXT_REDACTED"),
                normalize_legal_text(record.get("TXT_REDACTED", "TXT_REDACTED")),
                normalize_legal_text(record.get("TXT_REDACTED", "TXT_REDACTED")),
            )
            if dedupe_key in unique_keys:
                continue
            unique_keys.add(dedupe_key)
            category = record.get("TXT_REDACTED", "TXT_REDACTED")
            if category in result:
                result[category] = 2

    except Exception as exc:
        logger.error("TXT_REDACTED"                                       )

    return result


def preload_section5_external_data(year: str) -> None:
    "TXT_REDACTED"
    global SECTION5_BATCH_MODE
    SECTION5_BATCH_MODE = True
    logger.info("TXT_REDACTED"                                 )
    try:
        _fetch_kosri_report_titles()
    except Exception as exc:
        logger.warning("TXT_REDACTED"                          )
    try:
        _fetch_all_esco_rows()
    except Exception as exc:
        logger.warning("TXT_REDACTED"                         )
    logger.info("TXT_REDACTED"                                 )


class Section5EnvCollector:
    "TXT_REDACTED"

    def __init__(self, dart_client, report_parser=None):
        self.dart = dart_client
        self.parser = report_parser

    def collect(self, company_info: dict, year: str, fs_items: list = None) -> dict:
        "TXT_REDACTED"
        corp_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
        corp_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")

        logger.info("TXT_REDACTED"                                          )
        data = {}

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            context_text = _build_section5_context_text(company_info, year, self.dart)
            reports = collect_sustainability_reports(
                company_info,
                year,
                self.parser,
                dart_client=self.dart,
                context_text=context_text,
            )
            data.update({
                "TXT_REDACTED": reports["TXT_REDACTED"],
                "TXT_REDACTED": reports["TXT_REDACTED"],
                "TXT_REDACTED": reports["TXT_REDACTED"],
            })
        except Exception as exc:
            logger.error("TXT_REDACTED"                                         )
            data.update({"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": False})
            context_text = "TXT_REDACTED"

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            energy = collect_energy_efficiency_certification(
                company_info,
                year,
                self.parser,
                context_text=context_text,
            )
            data.update({
                "TXT_REDACTED": energy["TXT_REDACTED"],
                "TXT_REDACTED": energy["TXT_REDACTED"],
                "TXT_REDACTED": energy["TXT_REDACTED"],
            })
        except Exception as exc:
            logger.error("TXT_REDACTED"                                         )
            data.update({"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": 3})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            data["TXT_REDACTED"] = collect_environment_investment(
                corp_name, year, self.dart, corp_code, fs_items, self.parser
            )
            policy_parser = self.parser
            if context_text:
                merged_text = "TXT_REDACTED".join(part for part in [_report_text(self.parser), context_text] if part)
                policy_parser = type("TXT_REDACTED", (), {"TXT_REDACTED": merged_text})()
            data["TXT_REDACTED"] = collect_environment_policy(corp_name, year, policy_parser)
        except Exception as exc:
            logger.error("TXT_REDACTED"                                     )
            data.update({"TXT_REDACTED": 4, "TXT_REDACTED": False})

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            data["TXT_REDACTED"] = collect_environment_programs(
                corp_name,
                year,
                self.parser,
                context_text=context_text,
            )
        except Exception as exc:
            logger.error("TXT_REDACTED"                                         )
            data["TXT_REDACTED"] = 1

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            env_certs = collect_environment_certifications(
                company_info,
                year,
                self.parser,
                context_text=context_text,
            )
            data.update({
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
                "TXT_REDACTED": env_certs["TXT_REDACTED"],
            })
        except Exception as exc:
            logger.error("TXT_REDACTED"                                         )
            for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                        "TXT_REDACTED", "TXT_REDACTED"]:
                data[key] = False
            data["TXT_REDACTED"] = 2

        # REDACTED
        # REDACTED
        # REDACTED
        try:
            env_sanctions = collect_environment_sanctions(corp_name, year, self.parser)
            data.update({
                "TXT_REDACTED": env_sanctions.get("TXT_REDACTED", 3),
                "TXT_REDACTED": env_sanctions.get("TXT_REDACTED", 4),
                "TXT_REDACTED": env_sanctions.get("TXT_REDACTED", 1),
            })
        except Exception as exc:
            logger.error("TXT_REDACTED"                                       )
            data.update({
                "TXT_REDACTED": 2,
                "TXT_REDACTED": 3,
                "TXT_REDACTED": 4,
            })

        logger.info("TXT_REDACTED"                                          )
        return data
