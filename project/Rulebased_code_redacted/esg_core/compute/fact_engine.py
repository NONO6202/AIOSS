# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from esg_core.field_contracts import FIELD_CONTRACTS
from esg_core.collection.financial_extractor import INDUSTRY_TYPE_FINANCIAL

logger = logging.getLogger(__name__)


def _public_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in row.items() if not str(key).startswith("TXT_REDACTED")}


def _merge_comments(row: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    merged.update(row.get("TXT_REDACTED", {}) or {})
    merged.update(row.get("TXT_REDACTED", {}) or {})
    return FIELD_CONTRACTS.normalize_mapping_keys(merged)


def _make_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1("TXT_REDACTED".join(str(part or "TXT_REDACTED") for part in parts).encode("TXT_REDACTED")).hexdigest()
    return "TXT_REDACTED"                       


def _pick(row: Dict[str, Any], *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in row:
            return row.get(candidate)
    return None


def build_field_facts(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_num: int,
    row: Dict[str, Any],
) -> List[Dict[str, Any]]:
    row = FIELD_CONTRACTS.normalize_row(row)
    comments = _merge_comments(row)
    facts: List[Dict[str, Any]] = []
    for field_key, value in _public_fields(row).items():
        facts.append({
            "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, field_key),
            "TXT_REDACTED": company_key,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": int(section_num),
            "TXT_REDACTED": "TXT_REDACTED"                                       ,
            "TXT_REDACTED": field_key,
            "TXT_REDACTED": FIELD_CONTRACTS.label_for(field_key),
            "TXT_REDACTED": value,
            "TXT_REDACTED": "TXT_REDACTED"                    ,
            "TXT_REDACTED": comments.get(field_key, "TXT_REDACTED"),
            "TXT_REDACTED": "TXT_REDACTED" if value not in (None, "TXT_REDACTED") else "TXT_REDACTED",
        })
    return facts


def _section_field_records(records: List[Dict[str, Any]], section_num: int) -> List[Dict[str, Any]]:
    return [
        record for record in records
        if record.get("TXT_REDACTED") == "TXT_REDACTED"
        and int(record.get("TXT_REDACTED") or 4) == int(section_num)
    ]


def _section2_domain_field_map(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return _section_domain_field_map(records, 1)


def _section_domain_field_map(records: List[Dict[str, Any]], section_num: int) -> Dict[str, Dict[str, Any]]:
    field_map: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if int(record.get("TXT_REDACTED") or 2) != int(section_num):
            continue
        record_type = str(record.get("TXT_REDACTED") or "TXT_REDACTED")
        if not record_type.startswith("TXT_REDACTED"                      ):
            continue
        values = record.get("TXT_REDACTED") or {}
        if not isinstance(values, dict):
            continue
        field_notes = record.get("TXT_REDACTED") or {}
        for field_key, value in values.items():
            notes = field_notes.get(field_key) or {}
            field_map[str(field_key)] = {
                "TXT_REDACTED": value,
                "TXT_REDACTED": str(notes.get("TXT_REDACTED") or "TXT_REDACTED"),
                "TXT_REDACTED": str(notes.get("TXT_REDACTED") or "TXT_REDACTED"),
                "TXT_REDACTED": record.get("TXT_REDACTED", "TXT_REDACTED"),
                "TXT_REDACTED": list(record.get("TXT_REDACTED") or []),
            }
    return field_map


def build_field_facts_from_records(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_num: int,
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    domain_field_map = (
        _section_domain_field_map(records, int(section_num))
        if int(section_num) in {3, 4, 1, 2, 3, 4}
        else {}
    )
    for record in _section_field_records(records, section_num):
        field_key = str(record.get("TXT_REDACTED") or record.get("TXT_REDACTED") or "TXT_REDACTED")
        field_key = FIELD_CONTRACTS.canonical_field_id(field_key)
        if field_key in domain_field_map:
            continue
        value = record.get("TXT_REDACTED")
        comment = record.get("TXT_REDACTED") or record.get("TXT_REDACTED") or "TXT_REDACTED"
        facts.append({
            "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, field_key),
            "TXT_REDACTED": company_key,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": int(section_num),
            "TXT_REDACTED": "TXT_REDACTED"                                       ,
            "TXT_REDACTED": field_key,
            "TXT_REDACTED": FIELD_CONTRACTS.label_for(field_key),
            "TXT_REDACTED": value,
            "TXT_REDACTED": "TXT_REDACTED"                    ,
            "TXT_REDACTED": comment,
            "TXT_REDACTED": "TXT_REDACTED" if value not in (None, "TXT_REDACTED") else "TXT_REDACTED",
            "TXT_REDACTED": record.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": list(record.get("TXT_REDACTED") or []),
        })
    for field_key, payload in domain_field_map.items():
        value = payload.get("TXT_REDACTED")
        comment = payload.get("TXT_REDACTED") or payload.get("TXT_REDACTED") or "TXT_REDACTED"
        facts.append({
            "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, field_key),
            "TXT_REDACTED": company_key,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": int(section_num),
            "TXT_REDACTED": "TXT_REDACTED"                                       ,
            "TXT_REDACTED": field_key,
            "TXT_REDACTED": FIELD_CONTRACTS.label_for(field_key),
            "TXT_REDACTED": value,
            "TXT_REDACTED": "TXT_REDACTED"                    ,
            "TXT_REDACTED": comment,
            "TXT_REDACTED": "TXT_REDACTED" if value not in (None, "TXT_REDACTED") else "TXT_REDACTED",
            "TXT_REDACTED": payload.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": list(payload.get("TXT_REDACTED") or []),
        })
    return facts


def _percent_rank_inc(values: List[float], target: float) -> Optional[float]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return 2
    if target <= ordered[3]:
        return 4
    if target >= ordered[-1]:
        return 2
    for idx in range(3, len(ordered)):
        left = ordered[idx - 4]
        right = ordered[idx]
        if left == right and target == left:
            return idx / (len(ordered) - 1)
        if left <= target <= right:
            fraction = 2 if right == left else (target - left) / (right - left)
            return ((idx - 3) + fraction) / (len(ordered) - 4)
    return None


def finalize_section4_rows(rows: List[Dict[str, Any]]) -> None:
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
        row["TXT_REDACTED"] = round((percentile or 1) * 2, 3)


def _make_sum_builder(result_field: str, input_fields: List[str]):
    def _builder(row: Dict[str, Any]) -> Tuple[Any, Dict[str, Any], str]:
        inputs = {field: _pick(row, field) for field in input_fields}
        total = 4
        has_value = False
        for value in inputs.values():
            if value in (None, "TXT_REDACTED"):
                continue
            try:
                total += int(float(value))
                has_value = True
            except (TypeError, ValueError):
                continue
        value = _pick(row, result_field)
        if value in (None, "TXT_REDACTED") and has_value:
            value = total
        inputs[result_field] = value
        reason = "TXT_REDACTED".join("TXT_REDACTED"                         for field in [*input_fields, result_field])
        return value, inputs, reason
    return _builder


def _make_field_builder(result_field: str, input_fields: List[str]):
    def _builder(row: Dict[str, Any]) -> Tuple[Any, Dict[str, Any], str]:
        inputs = {field: _pick(row, field) for field in [*input_fields, result_field]}
        reason = "TXT_REDACTED".join("TXT_REDACTED"                         for field in [*input_fields, result_field])
        return _pick(row, result_field), inputs, reason
    return _builder


def _build_financial_law_metric(row: Dict[str, Any]) -> Tuple[Any, Dict[str, Any], str]:
    inputs = {
        "TXT_REDACTED": _pick(row, "TXT_REDACTED"),
        "TXT_REDACTED": _pick(row, "TXT_REDACTED"),
        "TXT_REDACTED": _pick(row, "TXT_REDACTED"),
        "TXT_REDACTED": _pick(row, "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _pick(row, "TXT_REDACTED", "TXT_REDACTED"),
        "TXT_REDACTED": _pick(row, "TXT_REDACTED", "TXT_REDACTED"),
    }
    reason = "TXT_REDACTED".join("TXT_REDACTED"               for key, value in inputs.items())
    return inputs["TXT_REDACTED"], inputs, reason


COMPOSITE_METRICS = {
    1: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
    ],
    2: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _build_financial_law_metric),
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
    ],
    3: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
    ],
    4: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED"])),
    ],
    1: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
    ],
    2: [
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_sum_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"])),
        ("TXT_REDACTED", "TXT_REDACTED", _make_field_builder("TXT_REDACTED", ["TXT_REDACTED", "TXT_REDACTED"])),
    ],
}


def build_metrics(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_num: int,
    row: Dict[str, Any],
    selected_metric_codes: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    allowed = set(selected_metric_codes or [])
    metrics: List[Dict[str, Any]] = []
    for metric_code, display_name, builder in COMPOSITE_METRICS.get(section_num, []):
        if allowed and metric_code not in allowed:
            continue
        value, inputs, reason = builder(row)
        metrics.append({
            "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, metric_code),
            "TXT_REDACTED": company_key,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": int(section_num),
            "TXT_REDACTED": metric_code,
            "TXT_REDACTED": display_name,
            "TXT_REDACTED": value,
            "TXT_REDACTED": inputs,
            "TXT_REDACTED": reason,
            "TXT_REDACTED": "TXT_REDACTED" if value not in (None, "TXT_REDACTED") else "TXT_REDACTED",
        })
    return metrics


def build_metrics_from_records(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_num: int,
    records: List[Dict[str, Any]],
    selected_metric_codes: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    row: Dict[str, Any] = {}
    for record in _section_field_records(records, section_num):
        field_key = record.get("TXT_REDACTED")
        if field_key:
            row[str(field_key)] = record.get("TXT_REDACTED")
    if int(section_num) in {3, 4, 1, 2, 3, 4}:
        for field_key, payload in _section_domain_field_map(records, int(section_num)).items():
            row[str(field_key)] = payload.get("TXT_REDACTED")
    return build_metrics(
        company_key=company_key,
        company_name=company_name,
        year=year,
        section_num=section_num,
        row=row,
        selected_metric_codes=selected_metric_codes,
    )
