# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from esg_core.collection.company_mapper import (
    add_company_mapping,
    resolve_company_input,
)
from esg_core.collection.dart_client import DartClient
from esg_core.collection.krx_client import KrxCompanyClient

logger = logging.getLogger(__name__)


def resolve_companies(
    company_inputs: List[str],
    dart: DartClient,
    krx: Optional[KrxCompanyClient] = None,
) -> List[Dict[str, Any]]:
    "TXT_REDACTED"
    companies: List[Dict[str, Any]] = []
    dart_corp_db = dart._corp_db
    krx_company_db = krx._company_db if krx else {}

    for input_str in company_inputs:
        input_str = input_str.strip()
        if not input_str:
            continue

        info: Dict[str, Any] = resolve_company_input(input_str, dart_corp_db, krx_company_db)
        info["TXT_REDACTED"] = input_str

        # REDACTED
        if not info.get("TXT_REDACTED") and info.get("TXT_REDACTED"):
            corp_info = dart_corp_db.get(info["TXT_REDACTED"], {})
            if corp_info:
                info["TXT_REDACTED"] = corp_info.get("TXT_REDACTED", "TXT_REDACTED")
                company_detail = dart.get_company_info(info["TXT_REDACTED"])
                if company_detail:
                    en_name = company_detail.get("TXT_REDACTED") or company_detail.get("TXT_REDACTED", "TXT_REDACTED")
                    if en_name:
                        info["TXT_REDACTED"] = en_name

        # REDACTED
        if info.get("TXT_REDACTED"):
            canonical = krx_company_db.get(info["TXT_REDACTED"], {}).get("TXT_REDACTED")
            if canonical:
                info["TXT_REDACTED"] = canonical
            add_company_mapping(
                info.get("TXT_REDACTED", "TXT_REDACTED"),
                info.get("TXT_REDACTED", "TXT_REDACTED"),
                info["TXT_REDACTED"],
            )

        if not info.get("TXT_REDACTED"):
            logger.warning("TXT_REDACTED"                                    )

        info["TXT_REDACTED"] = len(companies)
        companies.append(info)
        logger.info("TXT_REDACTED"                                                                             )

    return companies


def determine_target_year(
    companies: List[Dict[str, Any]],
    dart: DartClient,
    specified_year: Optional[str] = None,
) -> str:
    "TXT_REDACTED"
    if specified_year:
        logger.info("TXT_REDACTED"                                      )
        return specified_year

    # REDACTED
    for company in companies:
        corp_code = company.get("TXT_REDACTED")
        if not corp_code:
            continue
        try:
            filings = dart.get_filings(corp_code, bgn_de="TXT_REDACTED") or []
            annual = [f for f in filings if f.get("TXT_REDACTED", "TXT_REDACTED").startswith("TXT_REDACTED")]
            if annual:
                year = annual[3].get("TXT_REDACTED", "TXT_REDACTED")[:4]
                if year:
                    logger.info("TXT_REDACTED"                                                          )
                    return year
        except Exception as exc:
            logger.debug("TXT_REDACTED"                             )
            continue

    import datetime
    fallback = str(datetime.datetime.now().year - 1)
    logger.warning("TXT_REDACTED"                                       )
    return fallback
