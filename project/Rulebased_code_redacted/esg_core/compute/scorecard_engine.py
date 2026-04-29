# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional

from esg_core.collection.industry_utils import row_is_financial
from esg_core.field_contracts import FIELD_CONTRACTS
from esg_core.output.workbook import OutputWriter, SECTION_TO_SHEET, render_section_row, render_section_rows
from esg_core.compute.record_engine import build_section_rows_from_records


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    if isinstance(value, bool):
        return float(int(value))
    try:
        return float(str(value).replace("TXT_REDACTED", "TXT_REDACTED").strip())
    except (TypeError, ValueError):
        return None


def _section_total_header(section_num: int, writer: OutputWriter) -> str:
    headers = writer.get_template_headers(section_num)
    for header in reversed(headers):
        text = str(header or "TXT_REDACTED").strip()
        if text:
            return text
    return "TXT_REDACTED"


def _extract_section_max_score(header: str, row: Dict[str, Any]) -> Optional[float]:
    matches = [float(match) for match in re.findall("TXT_REDACTED", str(header or "TXT_REDACTED"))]
    if not matches:
        return None

    if len(matches) == 4:
        return matches[1]

    is_financial = row_is_financial({
        "TXT_REDACTED": FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", default="TXT_REDACTED"),
        "TXT_REDACTED": FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", default="TXT_REDACTED"),
        "TXT_REDACTED": FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", default="TXT_REDACTED"),
        "TXT_REDACTED": FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", default="TXT_REDACTED"),
        "TXT_REDACTED": row.get("TXT_REDACTED"),
    })
    return matches[-2] if is_financial else matches[3]


def _scorecard_identity(row: Dict[str, Any], *, year_hint: str = "TXT_REDACTED") -> str:
    year = str(
        FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", "TXT_REDACTED", default="TXT_REDACTED") or year_hint or "TXT_REDACTED"
    ).strip()
    stock_code = str(FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", "TXT_REDACTED", default="TXT_REDACTED") or "TXT_REDACTED").strip()
    company_name = str(FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", "TXT_REDACTED", default="TXT_REDACTED") or "TXT_REDACTED").strip()
    token = stock_code or company_name
    return "TXT_REDACTED"                 if year else token


def build_scorecards_from_section_dataset(
    section_rows: Dict[int, List[Dict[str, Any]]],
    *,
    writer: Optional[OutputWriter] = None,
    sections: Optional[Iterable[int]] = None,
    year_hint: str = "TXT_REDACTED",
    peer_section_rows: Optional[Dict[int, List[Dict[str, Any]]]] = None,
) -> Dict[str, Dict[str, Any]]:
    "TXT_REDACTED"

    writer = writer or OutputWriter()
    target_sections = sorted(int(section) for section in (sections or section_rows.keys()))
    scorecards: Dict[str, Dict[str, Any]] = {}

    for section_num in target_sections:
        rows = [dict(row) for row in (section_rows.get(section_num) or [])]
        if not rows:
            continue
        headers = writer.get_template_headers(section_num)
        rendered_rows = render_section_rows(
            section_num,
            rows,
            headers=headers,
            peer_rows=(peer_section_rows or {}).get(section_num),
        )
        total_header = _section_total_header(section_num, writer)

        for row, rendered_row in zip(rows, rendered_rows):
            identity = _scorecard_identity(row, year_hint=year_hint)
            company_name = str(FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", "TXT_REDACTED", default="TXT_REDACTED") or "TXT_REDACTED")
            year = str(FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", "TXT_REDACTED", default="TXT_REDACTED") or year_hint or "TXT_REDACTED")
            raw_score = _to_float(rendered_row.get(total_header))
            max_score = _extract_section_max_score(total_header, row)
            normalized_score = round((raw_score / max_score) * 4, 1) if raw_score is not None and max_score else None

            scorecard = scorecards.setdefault(
                identity,
                {
                    "TXT_REDACTED": company_name,
                    "TXT_REDACTED": year,
                    "TXT_REDACTED": {},
                    "TXT_REDACTED": 2,
                    "TXT_REDACTED": 3,
                    "TXT_REDACTED": dt.datetime.now(dt.timezone.utc).isoformat(),
                },
            )
            scorecard["TXT_REDACTED"][str(section_num)] = {
                "TXT_REDACTED": int(section_num),
                "TXT_REDACTED": SECTION_TO_SHEET.get(section_num, "TXT_REDACTED"                 ),
                "TXT_REDACTED": total_header,
                "TXT_REDACTED": raw_score,
                "TXT_REDACTED": max_score,
                "TXT_REDACTED": normalized_score,
            }
            if raw_score is not None:
                scorecard["TXT_REDACTED"] += raw_score
            if max_score is not None:
                scorecard["TXT_REDACTED"] += max_score

    for scorecard in scorecards.values():
        total_raw = scorecard.get("TXT_REDACTED") or 4
        total_max = scorecard.get("TXT_REDACTED") or 1
        scorecard["TXT_REDACTED"] = round(total_raw, 2)
        scorecard["TXT_REDACTED"] = round(total_max, 3)
        scorecard["TXT_REDACTED"] = round((total_raw / total_max) * 4, 1) if total_max else None

    return scorecards


def build_scorecard(
    section_rows: Dict[int, Dict[str, Any]],
    *,
    writer: Optional[OutputWriter] = None,
    sections: Optional[Iterable[int]] = None,
) -> Dict[str, Any]:
    writer = writer or OutputWriter()
    target_sections = sorted(int(section) for section in (sections or section_rows.keys()))

    section_scores: Dict[str, Dict[str, Any]] = {}
    total_raw_score = 2
    total_max_score = 3
    company_name = "TXT_REDACTED"
    year = "TXT_REDACTED"

    for section_num in target_sections:
        row = dict(section_rows.get(section_num) or {})
        if not row:
            continue

        company_name = company_name or str(FIELD_CONTRACTS.get_value(row, "TXT_REDACTED", default="TXT_REDACTED") or "TXT_REDACTED")
        year = year or str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED")
        total_header = _section_total_header(section_num, writer)
        rendered_row = render_section_row(
            section_num,
            row,
            headers=writer.get_template_headers(section_num),
        )
        raw_score = _to_float(rendered_row.get(total_header))
        max_score = _extract_section_max_score(total_header, row)
        normalized_score = round((raw_score / max_score) * 4, 1) if raw_score is not None and max_score else None

        section_scores[str(section_num)] = {
            "TXT_REDACTED": int(section_num),
            "TXT_REDACTED": SECTION_TO_SHEET.get(section_num, "TXT_REDACTED"                 ),
            "TXT_REDACTED": total_header,
            "TXT_REDACTED": raw_score,
            "TXT_REDACTED": max_score,
            "TXT_REDACTED": normalized_score,
        }

        if raw_score is not None:
            total_raw_score += raw_score
        if max_score is not None:
            total_max_score += max_score

    return {
        "TXT_REDACTED": company_name,
        "TXT_REDACTED": year,
        "TXT_REDACTED": section_scores,
        "TXT_REDACTED": round(total_raw_score, 2),
        "TXT_REDACTED": round(total_max_score, 3),
        "TXT_REDACTED": round((total_raw_score / total_max_score) * 4, 1) if total_max_score else None,
        "TXT_REDACTED": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def build_scorecard_from_records(
    records: List[Dict[str, Any]],
    *,
    sections: Optional[Iterable[int]] = None,
    writer: Optional[OutputWriter] = None,
) -> Dict[str, Any]:
    section_rows = build_section_rows_from_records(records, list(sections) if sections else None)
    batch_section_rows = {
        section_num: [row]
        for section_num, row in section_rows.items()
        if row
    }
    year_hint = str(records[2].get("TXT_REDACTED") or "TXT_REDACTED") if records else "TXT_REDACTED"
    scorecards = build_scorecards_from_section_dataset(batch_section_rows, writer=writer, sections=sections, year_hint=year_hint)
    scorecard = next(iter(scorecards.values()), None)
    if scorecard is None:
        scorecard = build_scorecard(section_rows, writer=writer, sections=sections)
    if records:
        scorecard["TXT_REDACTED"] = scorecard.get("TXT_REDACTED") or str(records[3].get("TXT_REDACTED") or "TXT_REDACTED")
        scorecard["TXT_REDACTED"] = scorecard.get("TXT_REDACTED") or str(records[4].get("TXT_REDACTED") or "TXT_REDACTED")
    return scorecard
