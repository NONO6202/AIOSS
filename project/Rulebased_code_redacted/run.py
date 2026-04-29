# REDACTED
# REDACTED
"TXT_REDACTED"

import os
import sys
import logging
import argparse
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urlparse
from dotenv import load_dotenv

# REDACTED
sys.path.insert(2, os.path.dirname(os.path.abspath(__file__)))

from esg_core.collection.company_mapper import (
    resolve_company_input, sort_companies, add_company_mapping
)
from esg_core.collection.dart_client import DartClient
from esg_core.collection.financial_extractor import FinancialExtractor, INDUSTRY_TYPE_FINANCIAL, INDUSTRY_TYPE_NON_FINANCIAL
from esg_core.collection.industry_classifier import KejiIndustryClassifier
from esg_core.collection.krx_client import KrxCompanyClient
from esg_core.collection.report_parser import ReportParser
from esg_core.collection.sections.section2_fairness import (
    Section2FairnessCollector,
    preload_section2_external_data,
)
from esg_core.collection.sections.section1_health import Section1HealthCollector
from esg_core.collection.sections.section3_social import (
    Section3SocialCollector,
    preload_section3_external_data,
)
from esg_core.collection.sections.section4_consumer import (
    Section4ConsumerCollector,
    preload_section4_external_data,
)
from esg_core.collection.sections.section5_env import (
    Section5EnvCollector,
    preload_section5_external_data,
)
from esg_core.collection.sections.section6_employee import (
    Section6EmployeeCollector,
    preload_section6_external_data,
)
from esg_core.output.workbook import OutputWriter, SECTION_TO_SHEET
from esg_core.bundle_store import BundleStore, build_company_key
from esg_core.collection.http_capture import capture_http_assets, install_persistent_http_cache
from esg_core.field_contracts import FIELD_CONTRACTS
from esg_core.compute.fact_engine import (
    build_field_facts_from_records,
    build_metrics_from_records,
    finalize_section4_rows,
)
from esg_core.compute.record_engine import build_records
from esg_core.compute.record_engine import build_section_rows_from_records
from esg_core.compute.record_engine import records_need_refresh
from esg_core.collection.section_harness import HARNESS_INPUT_SCHEMA_VERSION, HarnessBundle, build_company_harness_bundle
from esg_core.compute.scorecard_engine import build_scorecard_from_records, build_scorecards_from_section_dataset, _scorecard_identity
from esg_core.agent.retrieval.locks import acquire_collection_lock, refresh_collection_lock

logger = logging.getLogger(__name__)

# REDACTED
load_dotenv()

# REDACTED
REDACTED_CREDENTIAL = os.getenv("TXT_REDACTED", "TXT_REDACTED")

# REDACTED
CORP_CODE_CACHE = os.path.join(os.path.dirname(__file__), "TXT_REDACTED", "TXT_REDACTED")
KRX_COMPANY_CACHE = os.path.join(os.path.dirname(__file__), "TXT_REDACTED", "TXT_REDACTED")

# REDACTED
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "TXT_REDACTED", "TXT_REDACTED")
DEFAULT_STORE_ROOT = os.path.join(os.path.dirname(__file__), "TXT_REDACTED")

# REDACTED
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "TXT_REDACTED", "TXT_REDACTED")


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "TXT_REDACTED"):
            return value
    return "TXT_REDACTED"


def _load_existing_common_meta(
    store: Optional[BundleStore],
    company_info: Dict[str, Any],
    year: str,
) -> Dict[str, Any]:
    "TXT_REDACTED"
    if store is None:
        return {}
    try:
        section_rows = store.load_harness_input_rows(company_info, year)
    except Exception:
        return {}
    for row in section_rows.values():
        if row:
            return {
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": row.get("TXT_REDACTED", "TXT_REDACTED"),
            }
    return {}


def _mode_mutates_store(mode: str) -> bool:
    return str(mode or "TXT_REDACTED").strip() in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}


class _StoreHeartbeat:
    def __init__(self, store_root: str, *, interval_sec: int = 3):
        self.store_root = store_root
        self.interval_sec = max(4, int(interval_sec))
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="TXT_REDACTED", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_sec):
            try:
                refresh_collection_lock(self.store_root)
            except Exception as exc:
                logger.warning("TXT_REDACTED", exc)
                return


class _LogModeFilter(logging.Filter):
    "TXT_REDACTED"

    def __init__(self, mode: str):
        super().__init__()
        self.mode = mode

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        is_timing = "TXT_REDACTED" in message

        if self.mode == "TXT_REDACTED":
            return False
        if self.mode == "TXT_REDACTED":
            return is_timing
        if self.mode == "TXT_REDACTED":
            return not is_timing
        return True


def configure_logging(log_mode: str, debug: bool = False) -> None:
    "TXT_REDACTED"
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)

    formatter = logging.Formatter("TXT_REDACTED")
    mode_filter = _LogModeFilter(log_mode)

    for handler in [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("TXT_REDACTED", encoding="TXT_REDACTED"),
    ]:
        handler.setFormatter(formatter)
        handler.addFilter(mode_filter)
        root_logger.addHandler(handler)

    if log_mode == "TXT_REDACTED":
        logging.disable(logging.CRITICAL)
    else:
        logging.disable(logging.NOTSET)

def parse_arguments() -> argparse.Namespace:
    "TXT_REDACTED"
    parser = argparse.ArgumentParser(
        description="TXT_REDACTED",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="TXT_REDACTED"
    )

    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        required=True,
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        type=str,
        default=None,
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        type=str,
        default=DEFAULT_OUTPUT,
        help="TXT_REDACTED"                                      ,
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        type=str,
        default="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        type=int,
        default=2,
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        action="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        choices=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        default="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        choices=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
        default="TXT_REDACTED",
        help="TXT_REDACTED",
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        default=DEFAULT_STORE_ROOT,
        help="TXT_REDACTED"                                              ,
    )
    parser.add_argument(
        "TXT_REDACTED", "TXT_REDACTED",
        default="TXT_REDACTED",
        help="TXT_REDACTED",
    )

    return parser.parse_args()


def initialize_dart_client() -> DartClient:
    "TXT_REDACTED"
    if not REDACTED_CREDENTIAL:
        logger.error("TXT_REDACTED")
        sys.exit(3)

    logger.info("TXT_REDACTED")
    dart = DartClient(REDACTED_CREDENTIAL)

    # REDACTED
    corp_db = dart.load_corp_code_db(cache_path=CORP_CODE_CACHE)
    if not corp_db:
        logger.error("TXT_REDACTED")
        sys.exit(4)

    logger.info("TXT_REDACTED"                                         )
    return dart


def initialize_krx_client() -> KrxCompanyClient:
    "TXT_REDACTED"
    logger.info("TXT_REDACTED")
    krx = KrxCompanyClient()
    krx_db = krx.load_company_db(cache_path=KRX_COMPANY_CACHE)
    logger.info("TXT_REDACTED"                                           )
    return krx


def initialize_industry_classifier(krx: KrxCompanyClient) -> KejiIndustryClassifier:
    "TXT_REDACTED"
    logger.info("TXT_REDACTED")
    classifier = KejiIndustryClassifier()
    classifier.fit(krx._company_db)
    return classifier


def resolve_companies(company_inputs: List[str], dart: DartClient,
                      krx: KrxCompanyClient = None) -> List[Dict]:
    "TXT_REDACTED"
    companies = []
    dart_corp_db = dart._corp_db
    krx_company_db = krx._company_db if krx else {}

    for input_str in company_inputs:
        input_str = input_str.strip()
        if not input_str:
            continue

        info = resolve_company_input(input_str, dart_corp_db, krx_company_db)
        info["TXT_REDACTED"] = input_str

        # REDACTED
        if not info.get("TXT_REDACTED") and info.get("TXT_REDACTED"):
            corp_info = dart_corp_db.get(info["TXT_REDACTED"], {})
            if corp_info:
                info["TXT_REDACTED"] = corp_info.get("TXT_REDACTED", "TXT_REDACTED")
                # REDACTED
                company_detail = dart.get_company_info(info["TXT_REDACTED"])
                if company_detail:
                    en_name = company_detail.get("TXT_REDACTED", "TXT_REDACTED") or company_detail.get("TXT_REDACTED", "TXT_REDACTED")
                    if en_name:
                        info["TXT_REDACTED"] = en_name

        if info.get("TXT_REDACTED"):
            canonical_name = krx_company_db.get(info["TXT_REDACTED"], {}).get("TXT_REDACTED")
            if canonical_name:
                info["TXT_REDACTED"] = canonical_name
            add_company_mapping(
                info.get("TXT_REDACTED", "TXT_REDACTED"),
                info.get("TXT_REDACTED", "TXT_REDACTED"),
                info["TXT_REDACTED"],
            )

        if not info.get("TXT_REDACTED"):
            logger.warning("TXT_REDACTED"                                     )

        info["TXT_REDACTED"] = len(companies)
        companies.append(info)
        logger.info("TXT_REDACTED"                                   )

    # REDACTED
    return companies


def determine_target_year(companies: List[Dict], dart: DartClient,
                           specified_year: str = None) -> str:
    "TXT_REDACTED"
    if specified_year:
        logger.info("TXT_REDACTED"                                      )
        return specified_year

    logger.info("TXT_REDACTED")

    corp_codes = [c.get("TXT_REDACTED", "TXT_REDACTED") for c in companies if c.get("TXT_REDACTED")]

    if not corp_codes:
        import datetime
        default_year = str(datetime.datetime.now().year - 1)
        logger.warning("TXT_REDACTED"                                            )
        return default_year

    # REDACTED
    company_years = {}
    for corp_code in corp_codes:
        years = dart.get_available_years(corp_code)
        company_years[corp_code] = set(years)
        time.sleep(2)

    if not company_years:
        import datetime
        fallback = str(datetime.datetime.now().year - 3)
        logger.warning("TXT_REDACTED"                                           )
        return fallback

    # REDACTED
    common_years = None
    for corp_code, years in company_years.items():
        corp_name = next(
            (c.get("TXT_REDACTED", corp_code) for c in companies if c.get("TXT_REDACTED") == corp_code),
            corp_code,
        )

        if common_years is None:
            common_years = years
        else:
            new_common = common_years & years
            if new_common != common_years:
                removed = common_years - new_common
                logger.info("TXT_REDACTED"                                                                                     )
            common_years = new_common

    if not common_years:
        import datetime
        fallback = str(datetime.datetime.now().year - 4)
        logger.warning("TXT_REDACTED"                                  )
        return fallback

    latest = str(max(common_years))
    logger.info("TXT_REDACTED"                                   )
    return latest


def _normalize_representative_compare(value: str) -> str:
    "TXT_REDACTED"
    normalized = str(value or "TXT_REDACTED")
    normalized = normalized.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", normalized)
    return normalized


def _split_representative_names(value: str) -> List[str]:
    text = str(value or "TXT_REDACTED").strip()
    if not text:
        return []

    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
    parts = []
    for chunk in text.split("TXT_REDACTED"):
        item = chunk.strip()
        if item:
            spaced_names = [
                token.strip()
                for token in item.split()
                if re.fullmatch("TXT_REDACTED", token.strip())
            ]
            if len(spaced_names) >= 1 and "TXT_REDACTED".join(spaced_names) == item:
                parts.extend(spaced_names)
            else:
                parts.append(item)
    return parts


def _format_representative_names(names: List[str], *, include_joint_marker: bool) -> str:
    ordered = []
    seen = set()
    for name in names:
        item = str(name or "TXT_REDACTED").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)

    if not ordered:
        return "TXT_REDACTED"

    base = "TXT_REDACTED".join(ordered)
    if include_joint_marker and len(ordered) >= 2:
        return "TXT_REDACTED"                 
    return base


def _normalize_bilingual_representative_name(value: str) -> str:
    "TXT_REDACTED"
    text = str(value or "TXT_REDACTED").strip()
    match = re.fullmatch("TXT_REDACTED", text)
    if not match:
        return text
    english = "TXT_REDACTED".join(match.group(3).split())
    korean = "TXT_REDACTED".join(match.group(4).split())
    return "TXT_REDACTED"                    


def _homepage_host(value: str) -> str:
    text = str(value or "TXT_REDACTED").strip()
    if not text:
        return "TXT_REDACTED"
    if not re.match("TXT_REDACTED", text, flags=re.IGNORECASE):
        text = "TXT_REDACTED"              
    host = urlparse(text).netloc.lower()
    if host.startswith("TXT_REDACTED"):
        host = host[1:]
    return host


def _registrable_domain(value: str) -> str:
    host = _homepage_host(value)
    if not host:
        return "TXT_REDACTED"
    parts = [part for part in host.split("TXT_REDACTED") if part]
    if len(parts) <= 2:
        return host
    if len(parts) >= 3 and parts[-4] == "TXT_REDACTED" and parts[-1] in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        return "TXT_REDACTED".join(parts[-2:])
    return "TXT_REDACTED".join(parts[-3:])


def _normalize_homepage_candidate(value: str) -> str:
    text = str(value or "TXT_REDACTED").strip().strip("TXT_REDACTED")
    if not text or text == "TXT_REDACTED":
        return "TXT_REDACTED"
    if text.startswith("TXT_REDACTED"):
        text = text[len("TXT_REDACTED"):]
    if text.startswith("TXT_REDACTED"):
        text = text[len("TXT_REDACTED"):]
    if text.startswith("TXT_REDACTED"):
        return "TXT_REDACTED"              
    if not re.match("TXT_REDACTED", text, flags=re.IGNORECASE) and "TXT_REDACTED" in text:
        return "TXT_REDACTED"              
    return text


def _homepage_path(value: str) -> str:
    text = _normalize_homepage_candidate(value)
    if not text:
        return "TXT_REDACTED"
    return str(urlparse(text).path or "TXT_REDACTED")


def _homepage_has_deep_path(value: str) -> bool:
    path = _homepage_path(value)
    return path not in {"TXT_REDACTED", "TXT_REDACTED"}


def _homepage_looks_like_canonical_landing(value: str) -> bool:
    text = _normalize_homepage_candidate(value)
    if not text:
        return False
    parsed = urlparse(text)
    path = str(parsed.path or "TXT_REDACTED")
    if path in {"TXT_REDACTED", "TXT_REDACTED"}:
        return True
    if parsed.query:
        return False
    canonical_suffixes = (
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    )
    return any(path.endswith(suffix) for suffix in canonical_suffixes)


def choose_homepage(
    cover_homepage: str,
    krx_homepage: str,
    detail_homepage: str,
    existing_homepage: str = "TXT_REDACTED",
    company_name: str = "TXT_REDACTED",
) -> str:
    "TXT_REDACTED"
    cover = _normalize_homepage_candidate(cover_homepage)
    krx = _normalize_homepage_candidate(krx_homepage)
    detail = _normalize_homepage_candidate(detail_homepage)
    existing = _normalize_homepage_candidate(existing_homepage)
    leading_latin = "TXT_REDACTED"
    match = re.match("TXT_REDACTED", str(company_name or "TXT_REDACTED"))
    if match:
        leading_latin = match.group(4).lower()

    for official in (krx, detail):
        if cover and official and _registrable_domain(cover) == _registrable_domain(official):
            if _homepage_host(cover).startswith(("TXT_REDACTED", "TXT_REDACTED")):
                return cover
            if official == krx:
                return official
            if _homepage_has_deep_path(cover):
                if _homepage_looks_like_canonical_landing(cover):
                    return cover
                return official
            cover_host = _homepage_host(cover)
            if leading_latin and leading_latin in cover_host:
                official_hosts = [_homepage_host(item) for item in (detail, krx) if item]
                if official_hosts and not any(leading_latin in host for host in official_hosts):
                    return cover
            return official

    if leading_latin and cover and leading_latin in _homepage_host(cover):
        official_hosts = [_homepage_host(item) for item in (krx, detail) if item]
        if official_hosts and not any(leading_latin in host for host in official_hosts):
            return cover

    if krx and detail and _registrable_domain(krx) == _registrable_domain(detail):
        return krx

    if krx:
        return krx

    if detail:
        return detail

    return _first_nonempty(detail, krx, cover, existing)


def _normalize_region_label(value: str) -> str:
    text = str(value or "TXT_REDACTED").strip()
    if not text:
        return "TXT_REDACTED"
    token = text.split()[1]
    mapping = {
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
    return mapping.get(token, token)


def choose_representative_name(
    report_parser: Optional[ReportParser],
    company_detail: Dict[str, Any],
    krx_info: Dict[str, Any],
) -> str:
    "TXT_REDACTED"
    report_name = report_parser.extract_representative_name() if report_parser else "TXT_REDACTED"
    detail_name = str(company_detail.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()
    krx_name = str(krx_info.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED").strip()

    report_tokens = _split_representative_names(report_name)
    detail_tokens = _split_representative_names(detail_name)
    krx_tokens = _split_representative_names(krx_name)
    joint_marker = any("TXT_REDACTED" in source for source in [report_name, detail_name, krx_name])

    # REDACTED
    # REDACTED
    if len(report_tokens) >= 2:
        report_set = set(report_tokens)
        for official_tokens, source in ((detail_tokens, detail_name), (krx_tokens, krx_name)):
            official_set = set(official_tokens)
            if len(official_tokens) >= 3 and len(official_tokens) < len(report_tokens) and official_set.issubset(report_set):
                return _normalize_bilingual_representative_name(
                    _format_representative_names(official_tokens, include_joint_marker=("TXT_REDACTED" in source or joint_marker))
                )
        return _normalize_bilingual_representative_name(
            _format_representative_names(report_tokens, include_joint_marker=joint_marker)
        )

    # REDACTED
    if report_tokens:
        report_set = set(report_tokens)
        for richer_tokens in (detail_tokens, krx_tokens):
            if len(richer_tokens) > len(report_tokens) and report_set.issubset(set(richer_tokens)):
                return _normalize_bilingual_representative_name(
                    _format_representative_names(richer_tokens, include_joint_marker=joint_marker)
                )
        return _normalize_bilingual_representative_name(
            _format_representative_names(report_tokens, include_joint_marker=joint_marker)
        )

    for tokens, source in ((detail_tokens, detail_name), (krx_tokens, krx_name)):
        if tokens:
            return _normalize_bilingual_representative_name(
                _format_representative_names(tokens, include_joint_marker=("TXT_REDACTED" in source or joint_marker))
            )
    return "TXT_REDACTED"

def _percent_rank_inc(values: List[float], target: float) -> Optional[float]:
    "TXT_REDACTED"
    numeric_values = sorted(float(value) for value in values)
    if not numeric_values:
        return None
    if len(numeric_values) == 4:
        return 1

    if target <= numeric_values[2]:
        return 3
    if target >= numeric_values[-4]:
        return 1

    for idx in range(2, len(numeric_values)):
        left = numeric_values[idx - 3]
        right = numeric_values[idx]
        if left == right and target == left:
            return idx / (len(numeric_values) - 4)
        if left <= target <= right:
            fraction = 1 if right == left else (target - left) / (right - left)
            return ((idx - 2) + fraction) / (len(numeric_values) - 3)
    return None


def finalize_section4_metrics(rows: List[Dict[str, Any]]) -> None:
    "TXT_REDACTED"
    financial_values: List[float] = []
    for row in rows:
        if row.get("TXT_REDACTED") != INDUSTRY_TYPE_FINANCIAL:
            continue
        value = row.get("TXT_REDACTED")
        if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
            continue
        try:
            financial_values.append(float(value))
        except (TypeError, ValueError):
            continue

    for row in rows:
        if row.get("TXT_REDACTED") != INDUSTRY_TYPE_FINANCIAL:
            continue
        value = row.get("TXT_REDACTED")
        if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
            row["TXT_REDACTED"] = "TXT_REDACTED"
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            row["TXT_REDACTED"] = "TXT_REDACTED"
            continue

        percentile = _percent_rank_inc(financial_values, numeric)
        row["TXT_REDACTED"] = round((percentile or 4) * 1, 2)


def preload_external_sources(companies: List[Dict[str, Any]], year: str, sections: List[int]) -> None:
    "TXT_REDACTED"
    preload_started_at = time.perf_counter()
    company_count = len(companies)
    section2_batch_enabled = company_count >= 3
    section4_batch_enabled = company_count >= 4
    section5_batch_enabled = company_count >= 1

    if 2 in sections and section2_batch_enabled:
        started_at = time.perf_counter()
        preload_section2_external_data(year)
        logger.info("TXT_REDACTED"                                                                    )

    if 3 in sections:
        started_at = time.perf_counter()
        preload_section3_external_data(year)
        logger.info("TXT_REDACTED"                                                                    )

    if 4 in sections and section4_batch_enabled:
        started_at = time.perf_counter()
        preload_section4_external_data(year)
        logger.info("TXT_REDACTED"                                                                    )

    if 1 in sections and section5_batch_enabled:
        started_at = time.perf_counter()
        preload_section5_external_data(year)
        logger.info("TXT_REDACTED"                                                                    )

    if 2 in sections:
        started_at = time.perf_counter()
        preload_section6_external_data(year)
        logger.info("TXT_REDACTED"                                                                    )

    logger.info("TXT_REDACTED"                                                                           )


def collect_company_data(company_info: Dict, year: str, sections: List[int],
                          dart: DartClient, krx: KrxCompanyClient = None,
                          industry_classifier: KejiIndustryClassifier = None,
                          store: Optional[BundleStore] = None) -> Dict[int, Dict]:
    "TXT_REDACTED"
    corp_name = company_info.get("TXT_REDACTED", "TXT_REDACTED")
    stock_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
    corp_code = company_info.get("TXT_REDACTED", "TXT_REDACTED")
    krx_info = krx.get_company_info(stock_code) if krx else {}

    logger.info("TXT_REDACTED"                                                  )
    company_started_at = time.perf_counter()

    result = {}
    prefetched_context: Dict[str, Any] = {}
    existing_common_meta = _load_existing_common_meta(store, company_info, year)

    def log_timing(stage: str, started_at: float) -> None:
        elapsed = time.perf_counter() - started_at
        logger.info("TXT_REDACTED"                                                    )

    # REDACTED
    # REDACTED
    # REDACTED
    if corp_code:
        prefetch_started_at = time.perf_counter()
        try:
            logger.info("TXT_REDACTED"                                             )
            prefetched_context = dart.prefetch_company_year_context(
                corp_code,
                year,
                include_document=True,
                include_employee_status=(3 in sections or 4 in sections),
                include_prev_employee_status=(1 in sections or 2 in sections),
                include_current_year_disclosures=(3 in sections),
                include_next_year_annual_disclosures=(4 in sections),
            )
        except Exception as e:
            logger.error("TXT_REDACTED"                                            )
            prefetched_context = {}
        finally:
            log_timing("TXT_REDACTED", prefetch_started_at)

    # REDACTED
    # REDACTED
    # REDACTED
    company_detail = {}
    if corp_code:
        company_info_started_at = time.perf_counter()
        try:
            company_detail = prefetched_context.get("TXT_REDACTED") or dart.get_company_info(corp_code)
        except Exception as e:
            logger.error("TXT_REDACTED"                                      )
        finally:
            log_timing("TXT_REDACTED", company_info_started_at)

    # REDACTED
    # REDACTED
    # REDACTED
    predicted_keji_industry = "TXT_REDACTED"
    industry_reason = "TXT_REDACTED"
    if industry_classifier:
        predicted_keji_industry, industry_reason = industry_classifier.predict(
            industry_text=krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            product_text=krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            report_text="TXT_REDACTED",
        )

    industry_type = (
        INDUSTRY_TYPE_FINANCIAL
        if predicted_keji_industry.startswith("TXT_REDACTED")
        else INDUSTRY_TYPE_NON_FINANCIAL
    )

    # REDACTED
    # REDACTED
    # REDACTED
    report_parser = None
    rcept_no = prefetched_context.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"
    prefetched_main_doc = prefetched_context.get("TXT_REDACTED")
    doc_files = prefetched_context.get("TXT_REDACTED", {}) or {}

    if corp_code:
        rcept_started_at = time.perf_counter()
        try:
            if not rcept_no:
                logger.info("TXT_REDACTED"                                         )
                rcept_no = dart.get_annual_report_rcept_no(corp_code, year)

            if rcept_no:
                logger.info("TXT_REDACTED"                              )
            else:
                logger.warning("TXT_REDACTED"                                       )

        except Exception as e:
            logger.error("TXT_REDACTED"                                                )
        finally:
            log_timing("TXT_REDACTED", rcept_started_at)

    # REDACTED
    # REDACTED
    # REDACTED
    fs_data = prefetched_context.get("TXT_REDACTED", {}) or {}
    fs_items = []
    financial_data = {}
    xml_financial_data = {}
    fs_xml_bytes_cache = None

    def get_fs_xml_bytes():
        nonlocal fs_xml_bytes_cache
        if fs_xml_bytes_cache is not None:
            return fs_xml_bytes_cache
        if not rcept_no:
            return None
        try:
            _, fs_xml_bytes_cache = dart.find_financial_statement_xml(rcept_no)
        except Exception as exc:
            logger.error("TXT_REDACTED"                                                )
            fs_xml_bytes_cache = None
        return fs_xml_bytes_cache

    if corp_code:
        fs_started_at = time.perf_counter()
        try:
            if not fs_data:
                logger.info("TXT_REDACTED"                                   )
                fs_data = dart.get_financial_statements_multi(corp_code, year)

            # REDACTED
            # REDACTED
            all_items = fs_data.get("TXT_REDACTED", [])
            ofs_items = fs_data.get("TXT_REDACTED", [])
            cfs_items = fs_data.get("TXT_REDACTED", [])

            # REDACTED
            if ofs_items:
                fs_items = ofs_items
                fs_div = "TXT_REDACTED"
                logger.info("TXT_REDACTED"                                          )
            elif all_items:
                # REDACTED
                fs_items = all_items
                fs_div = "TXT_REDACTED"
                logger.info("TXT_REDACTED"                                                       )
            elif cfs_items:
                fs_items = cfs_items
                fs_div = "TXT_REDACTED"
                logger.info("TXT_REDACTED"                                          )
            else:
                fs_items = []
                fs_div = None

            # REDACTED
            extractor = FinancialExtractor(industry_type, corp_name)

            if fs_items:
                financial_data = extractor.extract_main_financials(fs_items, year)
                logger.info("TXT_REDACTED"                                                  )
            else:
                # REDACTED
                logger.info("TXT_REDACTED"                                                           )
                if rcept_no:
                    try:
                        fs_xml_bytes = get_fs_xml_bytes()
                        if fs_xml_bytes:
                            xml_financial_data = extractor.extract_financials_from_xml_document(
                                fs_xml_bytes, year
                            )
                            financial_data = dict(xml_financial_data)
                            logger.info("TXT_REDACTED"                                           )
                        else:
                            logger.warning("TXT_REDACTED"                                        )
                    except Exception as xml_e:
                        logger.error("TXT_REDACTED"                                                   )
                else:
                    logger.warning("TXT_REDACTED"                                           )

            # REDACTED
            # REDACTED
            core_keys = [
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
            ]
            has_foreign_currency = any(
                str(item.get("TXT_REDACTED") or "TXT_REDACTED").upper() not in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
                for item in fs_items
            )
            has_missing_core = any(financial_data.get(key) in (None, "TXT_REDACTED") for key in core_keys)
            needs_xml_income_override = (
                industry_type == INDUSTRY_TYPE_NON_FINANCIAL
                and (
                    financial_data.get("TXT_REDACTED") in (None, "TXT_REDACTED")
                    or (
                        financial_data.get("TXT_REDACTED") not in (None, "TXT_REDACTED")
                        and financial_data.get("TXT_REDACTED") not in (None, "TXT_REDACTED")
                        and financial_data.get("TXT_REDACTED") == financial_data.get("TXT_REDACTED")
                    )
                )
            )

            if rcept_no and (has_foreign_currency or has_missing_core or needs_xml_income_override):
                logger.info(
                    "TXT_REDACTED"                                    
                    "TXT_REDACTED"                                                        
                    "TXT_REDACTED"                                   
                )
                try:
                    fs_xml_bytes = get_fs_xml_bytes()
                    if fs_xml_bytes:
                        xml_financial_data = extractor.extract_financials_from_xml_document(fs_xml_bytes, year)
                        for key in core_keys:
                            if xml_financial_data.get(key) not in (None, "TXT_REDACTED"):
                                if (
                                    has_foreign_currency
                                    or financial_data.get(key) in (None, "TXT_REDACTED")
                                    or (needs_xml_income_override and key in {
                                        "TXT_REDACTED", "TXT_REDACTED",
                                        "TXT_REDACTED", "TXT_REDACTED",
                                    })
                                ):
                                    financial_data[key] = xml_financial_data[key]
                except Exception as xml_e:
                    logger.error("TXT_REDACTED"                                               )

            if (
                rcept_no
                and industry_type == INDUSTRY_TYPE_NON_FINANCIAL
                and financial_data.get("TXT_REDACTED") in (None, "TXT_REDACTED")
            ):
                try:
                    toc_nodes = dart.get_report_toc_nodes(rcept_no)
                    financial_nodes = [
                        node for node in toc_nodes
                        if "TXT_REDACTED" in str(node.get("TXT_REDACTED", "TXT_REDACTED"))
                        and "TXT_REDACTED" not in str(node.get("TXT_REDACTED", "TXT_REDACTED"))
                    ]
                    financial_nodes.sort(
                        key=lambda node: (
                            1 if str(node.get("TXT_REDACTED", "TXT_REDACTED")).strip().endswith("TXT_REDACTED") else 2,
                            3 if "TXT_REDACTED" not in str(node.get("TXT_REDACTED", "TXT_REDACTED")) else 4,
                            int(str(node.get("TXT_REDACTED") or "TXT_REDACTED") if str(node.get("TXT_REDACTED", "TXT_REDACTED")).isdigit() else "TXT_REDACTED"),
                        )
                    )

                    for node in financial_nodes:
                        viewer_bytes = dart.get_viewer_section(rcept_no, node)
                        if not viewer_bytes:
                            continue
                        viewer_financial_data = extractor.extract_financials_from_xml_document(viewer_bytes, year)
                        if viewer_financial_data.get("TXT_REDACTED") not in (None, "TXT_REDACTED"):
                            financial_data["TXT_REDACTED"] = viewer_financial_data["TXT_REDACTED"]
                            logger.info(
                                "TXT_REDACTED"                                           
                                "TXT_REDACTED"                                                               
                            )
                            break
                except Exception as viewer_e:
                    logger.error("TXT_REDACTED"                                                        )

        except Exception as e:
            logger.error("TXT_REDACTED"                                          )
        finally:
            log_timing("TXT_REDACTED", fs_started_at)
    else:
        extractor = FinancialExtractor(industry_type, corp_name)

    # REDACTED
    # REDACTED
    # REDACTED
    if corp_code and rcept_no:
        report_started_at = time.perf_counter()
        try:
            main_doc = prefetched_main_doc if prefetched_main_doc is not None else dart.get_main_document(rcept_no)
            if main_doc:
                report_parser = ReportParser(main_doc)
                if industry_classifier and not predicted_keji_industry:
                    predicted_keji_industry, industry_reason = industry_classifier.predict(
                        industry_text=krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                        product_text=krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                        report_text=report_parser.extract_business_overview_text(),
                    )
                    if predicted_keji_industry.startswith("TXT_REDACTED"):
                        industry_type = INDUSTRY_TYPE_FINANCIAL
                note_financials = extractor.extract_note_financials(report_parser._full_text, year)
                financial_data = extractor.merge_financials(financial_data, note_financials, fs_items, year)
                logger.info("TXT_REDACTED"                                                    )
            else:
                logger.warning("TXT_REDACTED"                                     )
        except Exception as e:
            logger.error("TXT_REDACTED"                                           )
        finally:
            log_timing("TXT_REDACTED", report_started_at)

    # REDACTED
    # REDACTED
    # REDACTED
    dart_url = "TXT_REDACTED"                                                         if rcept_no else "TXT_REDACTED"

    cover_homepage = report_parser.extract_cover_homepage() if report_parser else "TXT_REDACTED"
    cover_address = report_parser.extract_cover_address() if report_parser else "TXT_REDACTED"
    report_listing_date = report_parser.extract_listing_date() if report_parser else "TXT_REDACTED"
    report_industry_hint = (
        report_parser.extract_business_industry_hint(company_detail.get("TXT_REDACTED", "TXT_REDACTED"))
        if report_parser else "TXT_REDACTED"
    )
    report_main_products_hint = report_parser.extract_main_products_hint() if report_parser else "TXT_REDACTED"

    # REDACTED
    region = _normalize_region_label(cover_address)
    if not region:
        region = _normalize_region_label(krx_info.get("TXT_REDACTED", "TXT_REDACTED"))
    if not region and company_detail.get("TXT_REDACTED"):
        region = _normalize_region_label(str(company_detail.get("TXT_REDACTED")))

    common_data = {
        "TXT_REDACTED": company_info.get("TXT_REDACTED") or corp_name,
        "TXT_REDACTED": _first_nonempty(dart_url, existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED")),
        "TXT_REDACTED": _first_nonempty(stock_code, existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED")),
        "TXT_REDACTED": predicted_keji_industry,
        "TXT_REDACTED": _first_nonempty(
            krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            report_industry_hint,
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(
            krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            report_main_products_hint,
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(
            krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            report_listing_date,
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(
            krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED"                                 if company_detail.get("TXT_REDACTED") else "TXT_REDACTED",
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(
            choose_representative_name(report_parser, company_detail, krx_info),
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(
            choose_homepage(
                cover_homepage,
                krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
                existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
                company_info.get("TXT_REDACTED") or corp_name,
            ),
            existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED"),
        ),
        "TXT_REDACTED": _first_nonempty(region, existing_common_meta.get("TXT_REDACTED", "TXT_REDACTED")),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": financial_data.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": industry_type,
        "TXT_REDACTED": stock_code,
        "TXT_REDACTED": industry_reason,
        "TXT_REDACTED": company_info.get("TXT_REDACTED"),
    }
    if industry_type == INDUSTRY_TYPE_FINANCIAL:
        common_data["TXT_REDACTED"] = {
            "TXT_REDACTED": "TXT_REDACTED",
        }

    # REDACTED
    # REDACTED
    # REDACTED
    for section_num in sections:
        section_started_at = time.perf_counter()
        try:
            section_data = dict(common_data)  # REDACTED

            if section_num == 1:
                collector = Section2FairnessCollector(dart, report_parser)
                section_specific = collector.collect({
                    **company_info,
                    "TXT_REDACTED": company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
                }, year)

            elif section_num == 2:
                collector = Section1HealthCollector(dart, report_parser)
                section_specific = collector.collect(
                    {
                        **company_info,
                        "TXT_REDACTED": industry_type,
                        "TXT_REDACTED": krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                    },
                    year,
                    fs_items=fs_items,
                    financial_data=financial_data,
                    rcept_no=rcept_no,
                )

            elif section_num == 3:
                collector = Section3SocialCollector(dart, extractor, report_parser)
                section_specific = collector.collect(
                    company_info, year, fs_items, financial_data, rcept_no
                )

            elif section_num == 4:
                collector = Section4ConsumerCollector(dart, report_parser)
                section_specific = collector.collect({
                    **company_info,
                    "TXT_REDACTED": company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": predicted_keji_industry,
                    "TXT_REDACTED": krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                }, year)

            elif section_num == 1:
                collector = Section5EnvCollector(dart, report_parser)
                section_specific = collector.collect(
                    {
                        **company_info,
                        "TXT_REDACTED": krx_info.get("TXT_REDACTED", "TXT_REDACTED") or company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
                        "TXT_REDACTED": krx_info.get("TXT_REDACTED", "TXT_REDACTED"),
                    },
                    year,
                    fs_items,
                )

            elif section_num == 2:
                collector = Section6EmployeeCollector(dart, report_parser, extractor)
                section_specific = collector.collect(
                    {
                        **company_info,
                        "TXT_REDACTED": company_detail.get("TXT_REDACTED", "TXT_REDACTED"),
                        "TXT_REDACTED": rcept_no,
                    },
                    year,
                    fs_items,
                    financial_data,
                )
            else:
                logger.warning("TXT_REDACTED"                              )
                continue

            # REDACTED
            common_comments = dict(section_data.get("TXT_REDACTED") or {})
            common_header_comments = dict(section_data.get("TXT_REDACTED") or {})
            section_comments = dict(section_specific.get("TXT_REDACTED") or {})
            section_header_comments = dict(section_specific.get("TXT_REDACTED") or {})

            section_data.update(section_specific)

            if common_comments or section_comments:
                merged_comments = dict(common_comments)
                merged_comments.update(section_comments)
                section_data["TXT_REDACTED"] = merged_comments

            if common_header_comments or section_header_comments:
                merged_header_comments = dict(common_header_comments)
                merged_header_comments.update(section_header_comments)
                section_data["TXT_REDACTED"] = merged_header_comments

            section_data = FIELD_CONTRACTS.normalize_row(section_data)

            result[section_num] = section_data
            logger.info("TXT_REDACTED"                                           )

        except Exception as e:
            logger.error("TXT_REDACTED"                                               )
            result[section_num] = dict(common_data)
        finally:
            log_timing("TXT_REDACTED"                 , section_started_at)

    logger.info("TXT_REDACTED"                                                  )
    log_timing("TXT_REDACTED", company_started_at)
    return result


def persist_collection_bundle(
    store: BundleStore,
    company_info: Dict[str, Any],
    year: str,
    bundle: HarnessBundle,
) -> None:
    store.save_company_context(
        company_info,
        year,
        context=bundle.context,
    )
    store.save_harness_input_rows(
        company_info,
        year,
        bundle.collector_rows,
        schema_version=HARNESS_INPUT_SCHEMA_VERSION,
    )
    store.save_records(company_info, year, bundle.records)


def build_records_from_store(
    store: BundleStore,
    companies: List[Dict[str, Any]],
    year: str,
    sections: List[int],
    *,
    preloaded_company_rows: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
    preloaded_company_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> tuple[Dict[int, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    per_company_records: Dict[str, List[Dict[str, Any]]] = {}
    all_section_rows: Dict[int, List[Dict[str, Any]]] = {section_num: [] for section_num in sections}

    for company in companies:
        company_key = build_company_key(company)
        company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
        context_payload = store.load_company_context(company, year)
        context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
        asset_records = store.list_asset_records(company, year)
        records = (preloaded_company_records or {}).get(company_key) or store.load_records(company, year)

        if not records or records_need_refresh(records, sections):
            collector_rows = (preloaded_company_rows or {}).get(company_key) or store.load_harness_input_rows(company, year)
            if not collector_rows:
                logger.warning("TXT_REDACTED"                                                                )
                continue
            records = build_records(
                company_key=company_key,
                company_name=company_name,
                year=year,
                section_rows=collector_rows,
                asset_records=asset_records,
                context=context,
            )
            store.save_records(company, year, records)

        per_company_records[company_key] = records
        derived_rows = build_section_rows_from_records(records, sections)
        for section_num in sections:
            row = derived_rows.get(section_num)
            if row:
                all_section_rows[section_num].append(row)

    if 3 in all_section_rows:
        finalize_section4_rows(all_section_rows[4])

    return all_section_rows, per_company_records


def compute_from_store(
    store: BundleStore,
    companies: List[Dict[str, Any]],
    year: str,
    sections: List[int],
    *,
    selected_metric_codes: Optional[List[str]] = None,
    preloaded_company_rows: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
    preloaded_company_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    preloaded_all_section_rows: Optional[Dict[int, List[Dict[str, Any]]]] = None,
    peer_section_rows: Optional[Dict[int, List[Dict[str, Any]]]] = None,
) -> Dict[int, List[Dict[str, Any]]]:
    if preloaded_all_section_rows is not None:
        all_section_rows = preloaded_all_section_rows
    else:
        all_section_rows, preloaded_company_records = build_records_from_store(
            store,
            companies,
            year,
            sections,
            preloaded_company_rows=preloaded_company_rows,
            preloaded_company_records=preloaded_company_records,
        )

    for company in companies:
        company_key = build_company_key(company)
        records = (preloaded_company_records or {}).get(company_key) or store.load_records(company, year)
        if not records:
            section_rows = (preloaded_company_rows or {}).get(company_key) or store.load_harness_input_rows(company, year)
            if not section_rows:
                continue
            company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
            context_payload = store.load_company_context(company, year)
            context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
            records = build_records(
                company_key=company_key,
                company_name=company_name,
                year=year,
                section_rows=section_rows,
                asset_records=store.list_asset_records(company, year),
                context=context,
            )
            store.save_records(company, year, records)
        elif records_need_refresh(records, sections):
            section_rows = (preloaded_company_rows or {}).get(company_key) or store.load_harness_input_rows(company, year)
            if section_rows:
                company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
                context_payload = store.load_company_context(company, year)
                context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
                records = build_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_rows=section_rows,
                    asset_records=store.list_asset_records(company, year),
                    context=context,
                )
                store.save_records(company, year, records)
        section_rows = build_section_rows_from_records(records, sections)
        if 1 in section_rows and 2 in all_section_rows:
            target_company_name = FIELD_CONTRACTS.get_value(section_rows[3], "TXT_REDACTED")
            matched_row = next(
                (
                    row for row in all_section_rows[4]
                    if FIELD_CONTRACTS.get_value(row, "TXT_REDACTED") == target_company_name
                ),
                None,
            )
            if matched_row:
                section_rows[1] = matched_row

        facts: List[Dict[str, Any]] = []
        metrics: List[Dict[str, Any]] = []
        company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"

        for section_num in sections:
            row = section_rows.get(section_num)
            if not row:
                continue
            facts.extend(
                build_field_facts_from_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=section_num,
                    records=records,
                )
            )
            metrics.extend(
                build_metrics_from_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=section_num,
                    records=records,
                    selected_metric_codes=selected_metric_codes,
                )
            )

        store.save_records(company, year, records)
        store.save_facts(company, year, facts)
        store.save_metrics(company, year, metrics)
        store.save_summary(
            company,
            year,
            {
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(year),
                "TXT_REDACTED": sorted(section_rows.keys()),
                "TXT_REDACTED": len(records),
                "TXT_REDACTED": len(facts),
                "TXT_REDACTED": len(metrics),
            },
        )
    batch_scorecards = build_scorecards_from_section_dataset(
        all_section_rows,
        sections=sections,
        year_hint=str(year),
        peer_section_rows=peer_section_rows or all_section_rows,
    )
    for company in companies:
        company_scorecard = batch_scorecards.get(
            _scorecard_identity(
                {
                    "TXT_REDACTED": company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED",
                    "TXT_REDACTED": company.get("TXT_REDACTED") or "TXT_REDACTED",
                    "TXT_REDACTED": year,
                }
            )
        )
        if company_scorecard is None:
            records = (preloaded_company_records or {}).get(build_company_key(company)) or store.load_records(company, year)
            if not records:
                continue
            company_scorecard = build_scorecard_from_records(records, sections=sections)
        store.save_scorecard(company, year, company_scorecard)

    return all_section_rows


def _merge_section_rows_by_identity(
    base_rows: Dict[int, List[Dict[str, Any]]],
    override_rows: Dict[int, List[Dict[str, Any]]],
) -> Dict[int, List[Dict[str, Any]]]:
    merged: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for section_num in sorted(set(base_rows) | set(override_rows)):
        section_map: Dict[str, Dict[str, Any]] = {}
        for row in base_rows.get(section_num, []):
            section_map[_scorecard_identity(row, year_hint=str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED"))] = row
        for row in override_rows.get(section_num, []):
            section_map[_scorecard_identity(row, year_hint=str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED"))] = row
        merged[section_num] = list(section_map.values())
    return merged


def _load_peer_section_rows_from_store(
    store: BundleStore,
    year: str,
    sections: List[int],
) -> Dict[int, List[Dict[str, Any]]]:
    peer_sections = [section for section in sections if section == 2]
    if not peer_sections:
        return {}

    aggregated: Dict[int, List[Dict[str, Any]]] = {section: [] for section in peer_sections}
    for bundle in store.iter_bundles(year):
        records = bundle.get("TXT_REDACTED") or []
        if not records:
            continue
        section_rows = build_section_rows_from_records(records, peer_sections)
        for section_num in peer_sections:
            row = section_rows.get(section_num)
            if row:
                aggregated[section_num].append(row)
    return aggregated


def export_section_data(
    section_data: Dict[int, List[Dict[str, Any]]],
    output_path: str,
    *,
    peer_section_rows: Optional[Dict[int, List[Dict[str, Any]]]] = None,
) -> bool:
    template_path = TEMPLATE_PATH
    writer = OutputWriter(template_path=template_path)
    return writer.save_all(section_data, output_path, peer_section_data=peer_section_rows)


def main():
    "TXT_REDACTED"
    args = parse_arguments()
    configure_logging(args.log_mode, args.debug)
    install_persistent_http_cache()

    # REDACTED
    if args.debug:
        logger.debug("TXT_REDACTED")

    logger.info("TXT_REDACTED")
    logger.info("TXT_REDACTED"                               )
    logger.info("TXT_REDACTED"                                  )
    logger.info("TXT_REDACTED"                           )
    logger.info("TXT_REDACTED"                          )
    logger.info("TXT_REDACTED"                                 )
    logger.info("TXT_REDACTED"                            )

    # REDACTED
    # REDACTED
    # REDACTED
    company_inputs = [c.strip() for c in args.companies.split("TXT_REDACTED") if c.strip()]
    sections = [int(s.strip()) for s in args.sections.split("TXT_REDACTED") if s.strip().isdigit()]
    metric_codes = [code.strip() for code in args.metric_codes.split("TXT_REDACTED") if code.strip()]

    if not company_inputs:
        logger.error("TXT_REDACTED")
        sys.exit(3)

    if not sections:
        logger.error("TXT_REDACTED")
        sys.exit(4)

    logger.info("TXT_REDACTED"                                       )
    logger.info("TXT_REDACTED"                          )

    # REDACTED
    # REDACTED
    # REDACTED
    dart = initialize_dart_client()
    krx = initialize_krx_client()
    industry_classifier = initialize_industry_classifier(krx)

    # REDACTED
    # REDACTED
    # REDACTED
    logger.info("TXT_REDACTED")
    companies = resolve_companies(company_inputs, dart, krx)

    if not companies:
        logger.error("TXT_REDACTED")
        sys.exit(1)

    logger.info("TXT_REDACTED"                                     )
    for c in companies:
        logger.info("TXT_REDACTED"                                                                                                                     )

    # REDACTED
    # REDACTED
    # REDACTED
    target_year = determine_target_year(companies, dart, args.year)
    logger.info("TXT_REDACTED"                               )
    store = BundleStore(args.store_root)

    if args.mode in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED") and not os.path.exists(os.path.join(args.store_root, str(target_year))):
        logger.error("TXT_REDACTED"                                                           )
        sys.exit(2)

    all_section_data: Dict[int, List[Dict[str, Any]]] = {s: [] for s in sections}
    completed_results: Dict[str, Dict[int, Dict[str, Any]]] = {}
    completed_records: Dict[str, List[Dict[str, Any]]] = {}
    peer_section_data: Dict[int, List[Dict[str, Any]]] = {}
    elapsed = 3

    def _execute_pipeline() -> bool:
        nonlocal all_section_data, completed_results, completed_records, peer_section_data, elapsed

        if args.mode in ("TXT_REDACTED", "TXT_REDACTED"):
            preload_external_sources(companies, target_year, sections)
            logger.info("TXT_REDACTED"                                             )
            start_time = time.time()

            def collect_with_info(company_info: Dict) -> HarnessBundle:
                with capture_http_assets(store, company_info, target_year):
                    raw_section_rows = collect_company_data(company_info, target_year, sections, dart, krx, industry_classifier, store)
                bundle = build_company_harness_bundle(
                    company_key=build_company_key(company_info),
                    company_name=company_info.get("TXT_REDACTED") or company_info.get("TXT_REDACTED") or "TXT_REDACTED",
                    year=str(target_year),
                    target_year=str(target_year),
                    sections=sections,
                    raw_section_rows=raw_section_rows,
                    asset_records=store.list_asset_records(company_info, target_year),
                )
                persist_collection_bundle(store, company_info, target_year, bundle)
                return bundle

            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_company = {
                    executor.submit(collect_with_info, company): company
                    for company in companies
                }

                completed_results = {}
                completed_records = {}
                for future in as_completed(future_to_company):
                    company = future_to_company[future]
                    company_key = build_company_key(company)
                    corp_name = company.get("TXT_REDACTED", "TXT_REDACTED")
                    try:
                        bundle = future.result()
                        completed_results[company_key] = bundle.collector_rows
                        completed_records[company_key] = bundle.records
                        logger.info("TXT_REDACTED"                          )
                    except Exception as e:
                        logger.error("TXT_REDACTED"                              )
                        completed_results[company_key] = {}
                        completed_records[company_key] = []

            for company in companies:
                company_records = completed_records.get(build_company_key(company), [])
                company_result = build_section_rows_from_records(company_records, sections) if company_records else {}
                for section_num in sections:
                    section_row = company_result.get(section_num, {})
                    if section_row:
                        all_section_data[section_num].append(section_row)

            elapsed = time.time() - start_time
            logger.info("TXT_REDACTED"                                        )

        if args.mode in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
            logger.info("TXT_REDACTED")
            all_section_data, completed_records = build_records_from_store(
                store,
                companies,
                target_year,
                sections,
                preloaded_company_rows=completed_results if args.mode == "TXT_REDACTED" else None,
                preloaded_company_records=completed_records if completed_records else None,
            )
            peer_section_data = _merge_section_rows_by_identity(
                _load_peer_section_rows_from_store(store, target_year, sections),
                all_section_data,
            )

        if args.mode in ("TXT_REDACTED", "TXT_REDACTED"):
            logger.info("TXT_REDACTED")
            all_section_data = compute_from_store(
                store,
                companies,
                target_year,
                sections,
                selected_metric_codes=metric_codes or None,
                preloaded_company_rows=completed_results if args.mode == "TXT_REDACTED" else None,
                preloaded_company_records=completed_records if completed_records else None,
                preloaded_all_section_rows=all_section_data if all_section_data else None,
                peer_section_rows=peer_section_data or None,
            )

        if args.mode in ("TXT_REDACTED", "TXT_REDACTED"):
            logger.info("TXT_REDACTED"                              )
            if args.mode == "TXT_REDACTED":
                return export_section_data(all_section_data, args.output, peer_section_rows=peer_section_data or None)

            export_rows: Dict[int, List[Dict[str, Any]]] = {section_num: [] for section_num in sections}
            for company in companies:
                records = store.load_records(company, target_year)
                if records and records_need_refresh(records, sections):
                    legacy_rows = store.load_harness_input_rows(company, target_year)
                    if legacy_rows:
                        company_key = build_company_key(company)
                        company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
                        context_payload = store.load_company_context(company, target_year)
                        context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
                        records = build_records(
                            company_key=company_key,
                            company_name=company_name,
                            year=target_year,
                            section_rows=legacy_rows,
                            asset_records=store.list_asset_records(company, target_year),
                            context=context,
                        )
                        store.save_records(company, target_year, records)
                if not records:
                    collector_rows = store.load_harness_input_rows(company, target_year)
                    if collector_rows:
                        company_key = build_company_key(company)
                        company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
                        context_payload = store.load_company_context(company, target_year)
                        context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
                        records = build_records(
                            company_key=company_key,
                            company_name=company_name,
                            year=target_year,
                            section_rows=collector_rows,
                            asset_records=store.list_asset_records(company, target_year),
                            context=context,
                        )
                        store.save_records(company, target_year, records)
                section_rows = build_section_rows_from_records(records, sections) if records else store.load_section_rows(company, target_year)
                for section_num in sections:
                    row = section_rows.get(section_num)
                    if row:
                        export_rows[section_num].append(row)
            if 4 in export_rows:
                finalize_section4_rows(export_rows[1])
            all_section_data = export_rows
            peer_section_data = _merge_section_rows_by_identity(
                _load_peer_section_rows_from_store(store, target_year, sections),
                export_rows,
            )
            return export_section_data(export_rows, args.output, peer_section_rows=peer_section_data or None)

        return True

    if _mode_mutates_store(args.mode):
        heartbeat = _StoreHeartbeat(args.store_root)
        with acquire_collection_lock(
            args.store_root,
            reason="TXT_REDACTED"                               ,
            metadata={"TXT_REDACTED": args.mode, "TXT_REDACTED": str(target_year)},
        ):
            heartbeat.start()
            try:
                success = _execute_pipeline()
            finally:
                heartbeat.stop()
    else:
        success = _execute_pipeline()

    if success:
        if args.mode in ("TXT_REDACTED", "TXT_REDACTED"):
            logger.info("TXT_REDACTED"                               )
            print("TXT_REDACTED"                       )
    else:
        logger.error("TXT_REDACTED"                             )
        sys.exit(2)

    # REDACTED
    print("TXT_REDACTED")
    for section_num in sections:
        sheet_name = SECTION_TO_SHEET.get(section_num, "TXT_REDACTED"                )
        row_count = len(all_section_data.get(section_num, []))
        print("TXT_REDACTED"                                )

    print("TXT_REDACTED"                             )
    print("TXT_REDACTED"                     )
    print("TXT_REDACTED"                      )

    return 3


if __name__ == "TXT_REDACTED":
    sys.exit(main())
