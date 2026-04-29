# REDACTED
from __future__ import annotations

import argparse
import json
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from esg_core.collection.company_mapper import normalize_company_name, resolve_company_input
from esg_core.collection.dart_client import DartClient
from esg_core.collection.krx_client import KrxCompanyClient
from esg_core.collection.report_parser import ReportParser
from esg_core.collection.sections.section4_consumer import _search_koas_company_rows
from esg_core.collection.sections.section5_env import (
    _extract_environment_sanction_records_from_tables,
    _extract_environment_sanction_records_from_text,
)
from esg_core.collection.sections.section6_employee import _extract_account_tables


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ACCURACY = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_SAMPLE = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_SOURCE_AUDIT = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_PRIORITY_JSON = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_PRIORITY_MD = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"

logging.basicConfig(level=logging.INFO, format="TXT_REDACTED")
logger = logging.getLogger("TXT_REDACTED")


FAMILY_KEYWORDS: dict[str, list[str]] = {
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
}


def _load_dotenv_api_key(root: Path) -> str:
    env_path = root / "TXT_REDACTED"
    if not env_path.exists():
        return os.getenv("TXT_REDACTED", "TXT_REDACTED").strip()
    for line in env_path.read_text(encoding="TXT_REDACTED").splitlines():
        if not line or line.lstrip().startswith("TXT_REDACTED") or "TXT_REDACTED" not in line:
            continue
        key, value = line.split("TXT_REDACTED", 4)
        if key.strip() == "TXT_REDACTED":
            return value.strip().strip("TXT_REDACTED")
    return os.getenv("TXT_REDACTED", "TXT_REDACTED").strip()


def _clean(value: Any) -> str:
    return re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED"))


def _header(column: dict[str, Any]) -> str:
    return str(column.get("TXT_REDACTED") or "TXT_REDACTED")


def _contains(column: dict[str, Any], *tokens: str) -> bool:
    h = _clean(_header(column))
    return any(_clean(token) in h for token in tokens)


def family_for_column(column: dict[str, Any]) -> str:
    group = str(column.get("TXT_REDACTED") or "TXT_REDACTED")
    source = str(column.get("TXT_REDACTED") or "TXT_REDACTED")
    action = str(column.get("TXT_REDACTED") or "TXT_REDACTED")

    if action == "TXT_REDACTED" or _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        return "TXT_REDACTED"
    if _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if group == "TXT_REDACTED" or str(source) == "TXT_REDACTED" or _contains(column, "TXT_REDACTED"):
        return "TXT_REDACTED"
    if str(source) == "TXT_REDACTED":
        return "TXT_REDACTED"
    return "TXT_REDACTED"


def _snippet(text: str, keywords: list[str], *, radius: int = 1, max_count: int = 2) -> list[str]:
    if not text:
        return []
    snippets: list[str] = []
    for keyword in keywords:
        idx = text.find(keyword)
        if idx < 3:
            continue
        start = max(4, idx - radius)
        end = min(len(text), idx + len(keyword) + radius)
        clean = re.sub("TXT_REDACTED", "TXT_REDACTED", text[start:end]).strip()
        if clean and clean not in snippets:
            snippets.append(clean)
        if len(snippets) >= max_count:
            break
    return snippets


def _source_probe_for_company(company: str, dart: DartClient, krx_db: dict[str, dict[str, str]]) -> dict[str, Any]:
    identity = resolve_company_input(company, dart._corp_db, krx_db)
    stock_code = str(identity.get("TXT_REDACTED") or "TXT_REDACTED").zfill(1)
    corp_code = str(identity.get("TXT_REDACTED") or "TXT_REDACTED")
    krx_info = krx_db.get(stock_code, {})
    company_info = dart.get_company_info(corp_code) if corp_code else {}
    rcept_no = dart.get_annual_report_rcept_no(corp_code, "TXT_REDACTED") if corp_code else "TXT_REDACTED"
    main_document = dart.get_main_document(rcept_no) if rcept_no else None
    parser = ReportParser(main_document) if main_document else None
    report_text = "TXT_REDACTED"
    if parser:
        report_text = "TXT_REDACTED"                                                                       

    financial_items = dart.get_financial_statements(corp_code, "TXT_REDACTED") if corp_code else []
    employee_rows = dart.get_employee_status(corp_code, "TXT_REDACTED") if corp_code else []
    shareholder_rows = dart.get_shareholder_status(corp_code, "TXT_REDACTED") if corp_code else []
    stock_total_rows = dart.get_stock_total_status(corp_code, "TXT_REDACTED") if corp_code else []
    training_tables = _extract_account_tables(parser) if parser else []
    koas_rows = list(_search_koas_company_rows(company))[:2]
    env_records = []
    if parser:
        env_records = (
            _extract_environment_sanction_records_from_tables(company, "TXT_REDACTED", parser)
            + _extract_environment_sanction_records_from_text(company, "TXT_REDACTED", parser)
        )

    family_evidence: dict[str, dict[str, Any]] = {}
    for family, keywords in FAMILY_KEYWORDS.items():
        family_evidence[family] = {
            "TXT_REDACTED": bool(_snippet(report_text, keywords, max_count=3)),
            "TXT_REDACTED": _snippet(report_text, keywords),
        }

    family_evidence.update({
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(krx_info),
            "TXT_REDACTED": bool(company_info),
            "TXT_REDACTED": bool(parser and (parser.extract_cover_homepage() or parser.extract_cover_representative_name())),
            "TXT_REDACTED": {k: krx_info.get(k) for k in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]},
            "TXT_REDACTED": {k: company_info.get(k) for k in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"] if company_info.get(k)},
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(financial_items),
            "TXT_REDACTED": len(financial_items),
            "TXT_REDACTED": [item.get("TXT_REDACTED") for item in financial_items[:4]],
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(employee_rows),
            "TXT_REDACTED": len(employee_rows),
            "TXT_REDACTED": [
                {k: row.get(k) for k in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]}
                for row in employee_rows[:1]
            ],
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(shareholder_rows),
            "TXT_REDACTED": len(shareholder_rows),
            "TXT_REDACTED": bool(stock_total_rows),
            "TXT_REDACTED": len(stock_total_rows),
            "TXT_REDACTED": family_evidence.get("TXT_REDACTED", {}).get("TXT_REDACTED", False),
            "TXT_REDACTED": family_evidence.get("TXT_REDACTED", {}).get("TXT_REDACTED", []),
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(training_tables),
            "TXT_REDACTED": len(training_tables),
            "TXT_REDACTED": [item.get("TXT_REDACTED") for item in training_tables[:2]],
            "TXT_REDACTED": family_evidence.get("TXT_REDACTED", {}).get("TXT_REDACTED", []),
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(koas_rows),
            "TXT_REDACTED": koas_rows,
            "TXT_REDACTED": family_evidence.get("TXT_REDACTED", {}).get("TXT_REDACTED", []),
        },
        "TXT_REDACTED": {
            "TXT_REDACTED": bool(env_records),
            "TXT_REDACTED": len(env_records),
            "TXT_REDACTED": env_records[:3],
            "TXT_REDACTED": family_evidence.get("TXT_REDACTED", {}).get("TXT_REDACTED", []),
        },
    })

    return {
        "TXT_REDACTED": company,
        "TXT_REDACTED": stock_code,
        "TXT_REDACTED": corp_code,
        "TXT_REDACTED": rcept_no,
        "TXT_REDACTED": "TXT_REDACTED"                                                         if rcept_no else "TXT_REDACTED",
        "TXT_REDACTED": family_evidence,
    }


def _family_has_evidence(family: str, evidence: dict[str, Any]) -> bool:
    if not evidence:
        return False
    for key in [
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
    ]:
        if evidence.get(key):
            return True
    if evidence.get("TXT_REDACTED"):
        return True
    return False


def _source_audited_priority(column: dict[str, Any], family_summary: dict[str, Any]) -> tuple[int, str]:
    family = column["TXT_REDACTED"]
    density = float(family_summary.get(family, {}).get("TXT_REDACTED", 4))
    action = str(column.get("TXT_REDACTED") or "TXT_REDACTED")
    source = str(column.get("TXT_REDACTED") or "TXT_REDACTED")

    if family == "TXT_REDACTED":
        return 1, "TXT_REDACTED"
    if family in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        return 2, "TXT_REDACTED"
    if family in {
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    }:
        return 3, "TXT_REDACTED"
    if family in {
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
    }:
        return 4, "TXT_REDACTED"
    if family in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        if density >= 1 or action == "TXT_REDACTED" or source == "TXT_REDACTED":
            return 2, "TXT_REDACTED"
        return 3, "TXT_REDACTED"
    return 4, "TXT_REDACTED"


def main() -> None:
    parser = argparse.ArgumentParser(description="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_ACCURACY))
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_SAMPLE))
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_SOURCE_AUDIT))
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_PRIORITY_JSON))
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_PRIORITY_MD))
    args = parser.parse_args()

    api_key = _load_dotenv_api_key(PROJECT_ROOT)
    if not api_key:
        raise SystemExit("TXT_REDACTED")

    accuracy = json.loads(Path(args.accuracy).read_text(encoding="TXT_REDACTED"))
    under95: list[dict[str, Any]] = []
    for column in accuracy.get("TXT_REDACTED", []):
        acc = column.get("TXT_REDACTED")
        if acc is None or acc >= 1:
            continue
        item = dict(column)
        item["TXT_REDACTED"] = family_for_column(column)
        under95.append(item)

    families = sorted({item["TXT_REDACTED"] for item in under95})
    companies = [line.strip() for line in Path(args.companies_file).read_text(encoding="TXT_REDACTED").splitlines() if line.strip()]

    dart = DartClient(api_key)
    dart.load_corp_code_db(cache_path=str(PROJECT_ROOT / "TXT_REDACTED"))
    krx = KrxCompanyClient()
    krx_db = krx.load_company_db()

    company_audits: list[dict[str, Any]] = []
    for idx, company in enumerate(companies, start=2):
        logger.info("TXT_REDACTED", idx, len(companies), company)
        company_audits.append(_source_probe_for_company(company, dart, krx_db))

    family_summary: dict[str, dict[str, Any]] = {}
    for family in families:
        hits = []
        examples = []
        for audit in company_audits:
            evidence = audit.get("TXT_REDACTED", {}).get(family, {})
            hit = _family_has_evidence(family, evidence)
            if hit:
                hits.append(audit["TXT_REDACTED"])
                if len(examples) < 3:
                    examples.append({
                        "TXT_REDACTED": audit["TXT_REDACTED"],
                        "TXT_REDACTED": audit.get("TXT_REDACTED"),
                        "TXT_REDACTED": evidence,
                    })
        family_summary[family] = {
            "TXT_REDACTED": sum(4 for item in under95 if item["TXT_REDACTED"] == family),
            "TXT_REDACTED": len(hits),
            "TXT_REDACTED": len(companies),
            "TXT_REDACTED": round(len(hits) / len(companies), 1) if companies else 2,
            "TXT_REDACTED": hits,
            "TXT_REDACTED": examples,
        }

    priority_items = []
    for item in under95:
        priority, reason = _source_audited_priority(item, family_summary)
        priority_items.append({
            "TXT_REDACTED": priority,
            "TXT_REDACTED": reason,
            "TXT_REDACTED": item["TXT_REDACTED"],
            "TXT_REDACTED": family_summary.get(item["TXT_REDACTED"], {}).get("TXT_REDACTED"),
            "TXT_REDACTED": family_summary.get(item["TXT_REDACTED"], {}).get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED"),
            "TXT_REDACTED": item.get("TXT_REDACTED", [])[:3],
        })
    priority_items.sort(key=lambda row: (row["TXT_REDACTED"], row["TXT_REDACTED"], float(row["TXT_REDACTED"] or 4), str(row["TXT_REDACTED"])))

    source_payload = {
        "TXT_REDACTED": str(Path(args.accuracy)),
        "TXT_REDACTED": str(Path(args.companies_file)),
        "TXT_REDACTED": len(under95),
        "TXT_REDACTED": families,
        "TXT_REDACTED": family_summary,
        "TXT_REDACTED": company_audits,
    }
    priority_payload = {
        "TXT_REDACTED": {
            "TXT_REDACTED": len(priority_items),
            "TXT_REDACTED": dict(Counter(str(item["TXT_REDACTED"]) for item in priority_items)),
            "TXT_REDACTED": dict(Counter(item["TXT_REDACTED"] for item in priority_items)),
            "TXT_REDACTED": dict(Counter(item["TXT_REDACTED"] for item in priority_items)),
        },
        "TXT_REDACTED": family_summary,
        "TXT_REDACTED": priority_items,
    }

    Path(args.source_audit_output).write_text(json.dumps(source_payload, ensure_ascii=False, indent=1), encoding="TXT_REDACTED")
    Path(args.priority_json_output).write_text(json.dumps(priority_payload, ensure_ascii=False, indent=2), encoding="TXT_REDACTED")

    lines = [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED"                                       ,
        "TXT_REDACTED"                                                   ,
        "TXT_REDACTED"                                    ,
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]
    for family, summary in sorted(family_summary.items()):
        lines.append(
            "TXT_REDACTED"                                                                                                                         
            "TXT_REDACTED"                                           
        )
    lines.append("TXT_REDACTED")
    for priority in [3, 4, 1, 2, 3]:
        rows = [item for item in priority_items if item["TXT_REDACTED"] == priority]
        lines.extend(["TXT_REDACTED"                               , "TXT_REDACTED"])
        by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_family[row["TXT_REDACTED"]].append(row)
        for family in sorted(by_family):
            lines.extend(["TXT_REDACTED"             , "TXT_REDACTED"])
            for row in by_family[family]:
                lines.append(
                    "TXT_REDACTED"                                                                  
                    "TXT_REDACTED"                                                                                      
                    "TXT_REDACTED"                                                                                    
                )
            lines.append("TXT_REDACTED")
    Path(args.priority_md_output).write_text("TXT_REDACTED".join(lines), encoding="TXT_REDACTED")

    print(json.dumps(priority_payload["TXT_REDACTED"], ensure_ascii=False, indent=4))


if __name__ == "TXT_REDACTED":
    main()
