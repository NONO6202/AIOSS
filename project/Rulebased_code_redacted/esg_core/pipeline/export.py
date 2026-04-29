# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from esg_core.bundle_store import BundleStore, build_company_key
from esg_core.output.source_workbook_writer import SourceWorkbookWriter

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEMPLATE = str(_ROOT / "TXT_REDACTED" / "TXT_REDACTED")
DEFAULT_OUTPUT = str(_ROOT / "TXT_REDACTED" / "TXT_REDACTED")


def export_section_data(
    section_data: Dict[int, List[Dict[str, Any]]],
    output_path: str = DEFAULT_OUTPUT,
    template_path: Optional[str] = DEFAULT_TEMPLATE,
    scorecards: Optional[Dict[str, Any]] = None,
) -> bool:
    "TXT_REDACTED"
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    writer = SourceWorkbookWriter(template_path=template_path)
    ok = writer.save(section_data, output_path, scorecards=scorecards)
    if ok:
        logger.info("TXT_REDACTED"                              )
    else:
        logger.error("TXT_REDACTED"                              )
    return ok


def export_from_store(
    store: BundleStore,
    companies: List[Dict[str, Any]],
    year: str,
    sections: List[int],
    output_path: str = DEFAULT_OUTPUT,
    template_path: Optional[str] = DEFAULT_TEMPLATE,
) -> bool:
    "TXT_REDACTED"
    section_data: Dict[int, List[Dict[str, Any]]] = {s: [] for s in sections}
    scorecards: Dict[str, Any] = {}

    for company in companies:
        company_key = build_company_key(company)
        records = store.load_records(company, year)
        if not records:
            logger.warning("TXT_REDACTED"                                                         )
            continue

        from esg_core.compute.record_engine import build_section_rows_from_records
        rows = build_section_rows_from_records(records, sections)
        for s, row in rows.items():
            if row:
                section_data[s].append(row)

        sc = store.load_scorecard(company, year)
        if sc:
            scorecards[company_key] = sc

    return export_section_data(
        section_data,
        output_path=output_path,
        template_path=template_path,
        scorecards=scorecards or None,
    )
