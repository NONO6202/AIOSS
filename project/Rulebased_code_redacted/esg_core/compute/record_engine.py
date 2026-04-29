# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from esg_core.field_contracts import FIELD_CONTRACTS

RECORD_SCHEMA_VERSION = 1


def _make_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1("TXT_REDACTED".join(str(part or "TXT_REDACTED") for part in parts).encode("TXT_REDACTED")).hexdigest()
    return "TXT_REDACTED"                       


def _public_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in row.items() if not str(key).startswith("TXT_REDACTED")}


def _private_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in row.items() if str(key).startswith("TXT_REDACTED")}


SECTION2_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION2_HEADER_COMMENT_ALIASES = {
    "TXT_REDACTED": ["TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED"],
    "TXT_REDACTED": ["TXT_REDACTED"],
}

SECTION1_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION3_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION4_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION5_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION6_RECORD_GROUPS = [
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
    (
        "TXT_REDACTED",
        "TXT_REDACTED",
        ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ),
]

SECTION_FIELD_COMMENT_ALIASES = {
    2: SECTION2_HEADER_COMMENT_ALIASES,
    3: {
        "TXT_REDACTED": ["TXT_REDACTED"],
    },
}


def records_need_refresh(records: List[Dict[str, Any]], sections: List[int]) -> bool:
    context_record = next(
        (record for record in records if record.get("TXT_REDACTED") == "TXT_REDACTED"),
        None,
    )
    context_value = context_record.get("TXT_REDACTED") if isinstance(context_record, dict) else {}
    schema_version = context_value.get("TXT_REDACTED") if isinstance(context_value, dict) else None
    if schema_version != RECORD_SCHEMA_VERSION:
        return True
    present_meta_sections = {
        int(record.get("TXT_REDACTED") or 4)
        for record in records
        if record.get("TXT_REDACTED") == "TXT_REDACTED"
    }
    for section_num in sections:
        if int(section_num) not in present_meta_sections:
            return True
    return False


def _pick_comment(section_num: int, field_key: str, comments: Dict[str, Any], header_comments: Dict[str, Any]) -> tuple[str, str]:
    comment = str(comments.get(field_key) or "TXT_REDACTED")
    header_comment = str(header_comments.get(field_key) or "TXT_REDACTED")
    if header_comment:
        return comment, header_comment
    aliases = SECTION_FIELD_COMMENT_ALIASES.get(int(section_num), {}).get(field_key, [])
    if not isinstance(aliases, list):
        aliases = [aliases]
    for alias in aliases:
        alias_value = header_comments.get(alias)
        if alias_value:
            return comment, str(alias_value)
        alias_comment = comments.get(alias)
        if alias_comment:
            return str(alias_comment), header_comment
    return comment, header_comment


def _build_section2_domain_records(
    *,
    company_key: str,
    company_name: str,
    year: str,
    row: Dict[str, Any],
    row_asset_ids: List[str],
    comments: Dict[str, Any],
    header_comments: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for group_code, display_name, field_keys in SECTION2_RECORD_GROUPS:
        values = {field_key: row.get(field_key) for field_key in field_keys}
        field_notes = {}
        for field_key in field_keys:
            comment, header_comment = _pick_comment(1, field_key, comments, header_comments)
            if comment or header_comment:
                field_notes[field_key] = {
                    "TXT_REDACTED": comment,
                    "TXT_REDACTED": header_comment,
                }
        records.append(
            {
                "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, 2, group_code),
                "TXT_REDACTED": "TXT_REDACTED"                      ,
                "TXT_REDACTED": "TXT_REDACTED"                             ,
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(year),
                "TXT_REDACTED": 3,
                "TXT_REDACTED": display_name,
                "TXT_REDACTED": values,
                "TXT_REDACTED": "TXT_REDACTED" if any(value not in (None, "TXT_REDACTED", [], {}) for value in values.values()) else "TXT_REDACTED",
                "TXT_REDACTED": row_asset_ids,
                "TXT_REDACTED": field_notes,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            }
        )
    return records


def _build_grouped_domain_records(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_num: int,
    row: Dict[str, Any],
    row_asset_ids: List[str],
    comments: Dict[str, Any],
    header_comments: Dict[str, Any],
    groups: List[tuple[str, str, List[str]]],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for group_code, display_name, field_keys in groups:
        values = {field_key: row.get(field_key) for field_key in field_keys}
        field_notes = {}
        for field_key in field_keys:
            comment, header_comment = _pick_comment(section_num, field_key, comments, header_comments)
            if comment or header_comment:
                field_notes[field_key] = {
                    "TXT_REDACTED": comment,
                    "TXT_REDACTED": header_comment,
                }
        records.append(
            {
                "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, group_code),
                "TXT_REDACTED": "TXT_REDACTED"                                  ,
                "TXT_REDACTED": "TXT_REDACTED"                                         ,
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(year),
                "TXT_REDACTED": int(section_num),
                "TXT_REDACTED": display_name,
                "TXT_REDACTED": values,
                "TXT_REDACTED": "TXT_REDACTED" if any(value not in (None, "TXT_REDACTED", [], {}) for value in values.values()) else "TXT_REDACTED",
                "TXT_REDACTED": row_asset_ids,
                "TXT_REDACTED": field_notes,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            }
        )
    return records


def build_records(
    *,
    company_key: str,
    company_name: str,
    year: str,
    section_rows: Dict[int, Dict[str, Any]],
    asset_records: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    context = context or {}
    section_asset_ids = context.get("TXT_REDACTED", {}) or {}
    all_asset_ids = list(context.get("TXT_REDACTED", []) or [])

    records: List[Dict[str, Any]] = [
        {
            "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, "TXT_REDACTED"),
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": company_key,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": 4,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": sorted(int(section_num) for section_num in section_rows.keys()),
                "TXT_REDACTED": all_asset_ids,
                "TXT_REDACTED": context.get("TXT_REDACTED", str(year)),
                "TXT_REDACTED": RECORD_SCHEMA_VERSION,
            },
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": all_asset_ids,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
        }
    ]

    for asset in asset_records:
        asset_id = asset.get("TXT_REDACTED", "TXT_REDACTED")
        asset_type = asset.get("TXT_REDACTED", "TXT_REDACTED")
        records.append(
            {
                "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, "TXT_REDACTED", asset_id),
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED"                                ,
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(year),
                "TXT_REDACTED": 1,
                "TXT_REDACTED": asset.get("TXT_REDACTED") or asset_type or asset_id,
                "TXT_REDACTED": {
                    "TXT_REDACTED": asset_id,
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": asset_type,
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": asset.get("TXT_REDACTED", 2),
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                    "TXT_REDACTED": asset.get("TXT_REDACTED", "TXT_REDACTED"),
                },
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": [asset_id] if asset_id else [],
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            }
        )

    for section_num, row in sorted(section_rows.items()):
        row = FIELD_CONTRACTS.normalize_row(row)
        comments = row.get("TXT_REDACTED", {}) or {}
        header_comments = row.get("TXT_REDACTED", {}) or {}
        row_asset_ids = list(section_asset_ids.get(str(section_num), all_asset_ids))
        records.append(
            {
                "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, "TXT_REDACTED"),
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED"                                 ,
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": str(year),
                "TXT_REDACTED": int(section_num),
                "TXT_REDACTED": "TXT_REDACTED"                          ,
                "TXT_REDACTED": _private_fields(row),
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": row_asset_ids,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
            }
        )
        if int(section_num) == 3:
            records.extend(
                _build_section2_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                )
            )
        elif int(section_num) == 4:
            records.extend(
                _build_grouped_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=1,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                    groups=SECTION1_RECORD_GROUPS,
                )
            )
        elif int(section_num) == 2:
            records.extend(
                _build_grouped_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=3,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                    groups=SECTION4_RECORD_GROUPS,
                )
            )
        elif int(section_num) == 4:
            records.extend(
                _build_grouped_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=1,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                    groups=SECTION5_RECORD_GROUPS,
                )
            )
        elif int(section_num) == 2:
            records.extend(
                _build_grouped_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=3,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                    groups=SECTION3_RECORD_GROUPS,
                )
            )
        elif int(section_num) == 4:
            records.extend(
                _build_grouped_domain_records(
                    company_key=company_key,
                    company_name=company_name,
                    year=year,
                    section_num=1,
                    row=row,
                    row_asset_ids=row_asset_ids,
                    comments=comments,
                    header_comments=header_comments,
                    groups=SECTION6_RECORD_GROUPS,
                )
            )
        for field_key, value in _public_fields(row).items():
            records.append(
                {
                    "TXT_REDACTED": _make_id("TXT_REDACTED", company_key, year, section_num, field_key),
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED"                                        ,
                    "TXT_REDACTED": company_key,
                    "TXT_REDACTED": company_name,
                    "TXT_REDACTED": str(year),
                    "TXT_REDACTED": int(section_num),
                    "TXT_REDACTED": field_key,
                    "TXT_REDACTED": FIELD_CONTRACTS.label_for(field_key),
                    "TXT_REDACTED": value,
                    "TXT_REDACTED": "TXT_REDACTED" if value not in (None, "TXT_REDACTED") else "TXT_REDACTED",
                    "TXT_REDACTED": row_asset_ids,
                    "TXT_REDACTED": comments.get(field_key, "TXT_REDACTED"),
                    "TXT_REDACTED": header_comments.get(field_key, "TXT_REDACTED"),
                }
            )
    return records


def build_section_rows_from_records(
    records: List[Dict[str, Any]],
    sections: Optional[List[int]] = None,
) -> Dict[int, Dict[str, Any]]:
    allowed = set(int(section) for section in (sections or []))
    section_rows: Dict[int, Dict[str, Any]] = {}
    section_comments: Dict[int, Dict[str, Any]] = {}
    section_header_comments: Dict[int, Dict[str, Any]] = {}

    for record in records:
        section_num = int(record.get("TXT_REDACTED") or 2)
        if section_num <= 3:
            continue
        if allowed and section_num not in allowed:
            continue

        row = section_rows.setdefault(section_num, {})
        comments = section_comments.setdefault(section_num, {})
        header_comments = section_header_comments.setdefault(section_num, {})
        record_type = str(record.get("TXT_REDACTED") or "TXT_REDACTED")

        if record_type == "TXT_REDACTED":
            payload = record.get("TXT_REDACTED") or {}
            if isinstance(payload, dict):
                existing_comments = payload.get("TXT_REDACTED") or {}
                existing_header_comments = payload.get("TXT_REDACTED") or {}
                if isinstance(existing_comments, dict):
                    comments.update(existing_comments)
                if isinstance(existing_header_comments, dict):
                    header_comments.update(existing_header_comments)
                row.update(payload)
            continue

        if record_type == "TXT_REDACTED":
            field_key = str(record.get("TXT_REDACTED") or record.get("TXT_REDACTED") or "TXT_REDACTED")
            if field_key:
                row[field_key] = record.get("TXT_REDACTED")
                if record.get("TXT_REDACTED"):
                    comments[field_key] = record.get("TXT_REDACTED")
                if record.get("TXT_REDACTED"):
                    header_comments[field_key] = record.get("TXT_REDACTED")
            continue

        if record_type.startswith("TXT_REDACTED"                      ):
            payload = record.get("TXT_REDACTED") or {}
            if isinstance(payload, dict):
                row.update(payload)
            field_notes = record.get("TXT_REDACTED") or {}
            if isinstance(field_notes, dict):
                for field_key, note in field_notes.items():
                    if not isinstance(note, dict):
                        continue
                    aliases = SECTION_FIELD_COMMENT_ALIASES.get(section_num, {}).get(str(field_key), [])
                    if not isinstance(aliases, list):
                        aliases = [aliases]
                    target_keys = [str(alias) for alias in aliases if alias] or [str(field_key)]
                    if note.get("TXT_REDACTED"):
                        for target_key in target_keys:
                            comments[target_key] = note.get("TXT_REDACTED")
                    if note.get("TXT_REDACTED"):
                        for target_key in target_keys:
                            header_comments[target_key] = note.get("TXT_REDACTED")

    for section_num, row in section_rows.items():
        if section_comments.get(section_num):
            row["TXT_REDACTED"] = FIELD_CONTRACTS.normalize_mapping_keys(section_comments[section_num])
        if section_header_comments.get(section_num):
            row["TXT_REDACTED"] = FIELD_CONTRACTS.normalize_mapping_keys(section_header_comments[section_num])
        section_rows[section_num] = FIELD_CONTRACTS.normalize_row(row)
    return section_rows
