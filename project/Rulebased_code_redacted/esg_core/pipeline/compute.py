# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from esg_core.bundle_store import BundleStore, build_company_key
from esg_core.compute.fact_engine import (
    build_field_facts_from_records,
    build_metrics_from_records,
    finalize_section4_rows,
)
from esg_core.compute.record_engine import (
    build_records,
    build_section_rows_from_records,
    records_need_refresh,
)
from esg_core.compute.scorecard_engine import (
    _scorecard_identity,
    build_scorecard_from_records,
    build_scorecards_from_section_dataset,
)
from esg_core.field_contracts import FIELD_CONTRACTS

logger = logging.getLogger(__name__)


def build_records_from_store(
    store: BundleStore,
    companies: List[Dict[str, Any]],
    year: str,
    sections: List[int],
    *,
    preloaded_company_rows: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
    preloaded_company_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> tuple[Dict[int, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    "TXT_REDACTED"
    per_company_records: Dict[str, List[Dict[str, Any]]] = {}
    all_section_rows: Dict[int, List[Dict[str, Any]]] = {s: [] for s in sections}

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
                logger.warning("TXT_REDACTED"                                                  )
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
        for s in sections:
            row = derived_rows.get(s)
            if row:
                all_section_rows[s].append(row)

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
) -> Dict[int, List[Dict[str, Any]]]:
    "TXT_REDACTED"
    if preloaded_all_section_rows is not None:
        all_section_rows = preloaded_all_section_rows
    else:
        all_section_rows, preloaded_company_records = build_records_from_store(
            store, companies, year, sections,
            preloaded_company_rows=preloaded_company_rows,
            preloaded_company_records=preloaded_company_records,
        )

    for company in companies:
        company_key = build_company_key(company)
        company_name = company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED"
        records = (preloaded_company_records or {}).get(company_key) or store.load_records(company, year)

        if not records:
            section_rows_raw = (preloaded_company_rows or {}).get(company_key) or store.load_harness_input_rows(company, year)
            if not section_rows_raw:
                continue
            context_payload = store.load_company_context(company, year)
            context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
            records = build_records(
                company_key=company_key,
                company_name=company_name,
                year=year,
                section_rows=section_rows_raw,
                asset_records=store.list_asset_records(company, year),
                context=context,
            )
            store.save_records(company, year, records)
        elif records_need_refresh(records, sections):
            section_rows_raw = (preloaded_company_rows or {}).get(company_key) or store.load_harness_input_rows(company, year)
            if section_rows_raw:
                context_payload = store.load_company_context(company, year)
                context = context_payload.get("TXT_REDACTED", {}) if isinstance(context_payload, dict) else {}
                records = build_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_rows=section_rows_raw,
                    asset_records=store.list_asset_records(company, year),
                    context=context,
                )
                store.save_records(company, year, records)

        section_rows = build_section_rows_from_records(records, sections)

        # REDACTED
        if 1 in section_rows and 2 in all_section_rows:
            target_name = FIELD_CONTRACTS.get_value(section_rows[3], "TXT_REDACTED")
            matched = next(
                (r for r in all_section_rows[4] if FIELD_CONTRACTS.get_value(r, "TXT_REDACTED") == target_name),
                None,
            )
            if matched:
                section_rows[1] = matched

        facts: List[Dict[str, Any]] = []
        metrics: List[Dict[str, Any]] = []

        for s in sections:
            if not section_rows.get(s):
                continue
            facts.extend(
                build_field_facts_from_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=s,
                    records=records,
                )
            )
            metrics.extend(
                build_metrics_from_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=s,
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

    # REDACTED
    batch_scorecards = build_scorecards_from_section_dataset(
        all_section_rows, sections=sections, year_hint=str(year)
    )
    for company in companies:
        identity = _scorecard_identity(
            {
                "TXT_REDACTED": company.get("TXT_REDACTED") or company.get("TXT_REDACTED") or "TXT_REDACTED",
                "TXT_REDACTED": company.get("TXT_REDACTED") or "TXT_REDACTED",
                "TXT_REDACTED": year,
            }
        )
        company_scorecard = batch_scorecards.get(identity)
        if company_scorecard is None:
            records = (preloaded_company_records or {}).get(build_company_key(company)) or store.load_records(company, year)
            if not records:
                continue
            company_scorecard = build_scorecard_from_records(records, sections=sections)
        store.save_scorecard(company, year, company_scorecard)

    return all_section_rows
