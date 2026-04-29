# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from esg_core.compute.record_engine import RECORD_SCHEMA_VERSION, build_records, build_section_rows_from_records

HARNESS_INPUT_SCHEMA_VERSION = 3


@dataclass
class HarnessBundle:
    company_key: str
    company_name: str
    year: str
    context: Dict[str, Any]
    collector_rows: Dict[int, Dict[str, Any]]
    records: List[Dict[str, Any]]
    section_rows: Dict[int, Dict[str, Any]]


def build_company_harness_bundle(
    *,
    company_key: str,
    company_name: str,
    year: str,
    target_year: str,
    sections: List[int],
    raw_section_rows: Dict[int, Dict[str, Any]],
    asset_records: List[Dict[str, Any]],
) -> HarnessBundle:
    asset_ids = [record.get("TXT_REDACTED") for record in asset_records if record.get("TXT_REDACTED")]
    section_asset_ids = {
        str(section_num): list(asset_ids)
        for section_num in raw_section_rows.keys()
    }
    context = {
        "TXT_REDACTED": str(target_year),
        "TXT_REDACTED": sorted(raw_section_rows.keys()),
        "TXT_REDACTED": asset_ids,
        "TXT_REDACTED": section_asset_ids,
        "TXT_REDACTED": HARNESS_INPUT_SCHEMA_VERSION,
        "TXT_REDACTED": RECORD_SCHEMA_VERSION,
    }
    records = build_records(
        company_key=company_key,
        company_name=company_name,
        year=year,
        section_rows=raw_section_rows,
        asset_records=asset_records,
        context=context,
    )
    derived_rows = build_section_rows_from_records(records, sections)
    return HarnessBundle(
        company_key=company_key,
        company_name=company_name,
        year=str(year),
        context=context,
        collector_rows=raw_section_rows,
        records=records,
        section_rows=derived_rows,
    )
