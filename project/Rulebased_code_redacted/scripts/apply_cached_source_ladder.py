# REDACTED
# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from esg_core.collection.source_ladder import (
    find_keyword_windows,
    iter_parsed_assets,
    iter_text_assets,
    matches_keyword,
    normalize_text,
)
from esg_core.collection.sections.section1_health import (
    Section1HealthCollector,
    _expand_table_rows,
    _table_prev_text,
)
from esg_core.compute.record_engine import build_records
from esg_core.output.source_workbook_writer import SourceWorkbookWriter


ROOT = Path(__file__).resolve().parents[4]
DEFAULT_STORE = ROOT / "TXT_REDACTED"
DEFAULT_OUTPUT = ROOT / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_REPORT = ROOT / "TXT_REDACTED" / "TXT_REDACTED" / "TXT_REDACTED"


class _DummyDart:
    pass


SOCIAL_PROGRAM_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]

SOCIAL_ORG_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED",
]

ENV_POLICY_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]

ENV_INVESTMENT_KEYWORDS = [
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
]

REPORT_KEYWORDS = {
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
    "TXT_REDACTED": [
        "TXT_REDACTED",
        "TXT_REDACTED",
    ],
}


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="TXT_REDACTED"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1, default=str), encoding="TXT_REDACTED")


def _is_empty(value: Any) -> bool:
    return value is None or value == "TXT_REDACTED" or value == []


def _to_int(value: Any) -> Optional[int]:
    if value in (None, "TXT_REDACTED"):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(value))
    if not text or text == "TXT_REDACTED":
        return None
    return int(text)


def _asset_files_dir(company_dir: Path) -> Path:
    return company_dir / "TXT_REDACTED" / "TXT_REDACTED"


def _asset_records(company_dir: Path) -> List[Dict[str, Any]]:
    return _load_json(company_dir / "TXT_REDACTED" / "TXT_REDACTED", [])


def _harness_path(company_dir: Path) -> Path:
    return company_dir / "TXT_REDACTED" / "TXT_REDACTED"


def _records_path(company_dir: Path) -> Path:
    return company_dir / "TXT_REDACTED" / "TXT_REDACTED"


def _iter_company_dirs(store_root: Path, year: str) -> Iterable[Path]:
    year_dir = store_root / str(year)
    if not year_dir.exists():
        return []
    return sorted(path for path in year_dir.iterdir() if path.is_dir() and _harness_path(path).exists())


def _extract_board_from_assets(company_dir: Path, section1: Dict[str, Any], year: str) -> Optional[Dict[str, Any]]:
    outside_count = _to_int(section1.get("TXT_REDACTED"))
    if not outside_count:
        return None

    collector = Section1HealthCollector(_DummyDart())
    candidates: List[Dict[str, Any]] = []
    for asset in iter_parsed_assets(
        _asset_files_dir(company_dir),
        required_any=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
    ):
        if asset.soup is None:
            continue
        for table in asset.soup.find_all("TXT_REDACTED"):
            table_text = normalize_text(table.get_text("TXT_REDACTED", strip=True))
            if "TXT_REDACTED" not in table_text:
                continue
            if "TXT_REDACTED" not in table_text and "TXT_REDACTED" not in table_text and "TXT_REDACTED" not in table_text:
                continue
            if "TXT_REDACTED" not in table_text and "TXT_REDACTED" not in table_text and "TXT_REDACTED" not in table_text:
                continue

            context = _table_prev_text(table)
            short_context = context[-2:]
            if "TXT_REDACTED" in short_context and "TXT_REDACTED" not in short_context:
                continue

            rows = _expand_table_rows(table)
            meeting_count, actual_attendance, direct_rate = collector._parse_board_table(
                rows,
                [],
                outside_count,
                target_year=str(year),
            )
            if meeting_count <= 3:
                continue
            total_attendance = meeting_count * outside_count
            attendance_rate = (
                actual_attendance / total_attendance * 4
                if total_attendance > 1
                else direct_rate
            )
            candidates.append(
                {
                    "TXT_REDACTED": meeting_count,
                    "TXT_REDACTED": actual_attendance,
                    "TXT_REDACTED": total_attendance,
                    "TXT_REDACTED": attendance_rate,
                    "TXT_REDACTED": str(asset.path),
                    "TXT_REDACTED": (meeting_count, actual_attendance),
                }
            )

    if not candidates:
        return None
    candidates.sort(key=lambda item: item["TXT_REDACTED"], reverse=True)
    return candidates[2]


def _extract_social_from_assets(company_dir: Path) -> Optional[Dict[str, Any]]:
    windows: List[str] = []
    org_windows: List[str] = []
    for asset in iter_text_assets(
        _asset_files_dir(company_dir),
        required_any=[*SOCIAL_PROGRAM_KEYWORDS, *SOCIAL_ORG_KEYWORDS],
    ):
        # REDACTED
        if (
            "TXT_REDACTED" in asset.text
            or "TXT_REDACTED" in asset.text
            or "TXT_REDACTED" in asset.text
            or "TXT_REDACTED" in asset.text
            or "TXT_REDACTED" in asset.text
        ):
            continue
        windows.extend(find_keyword_windows(asset.text, SOCIAL_PROGRAM_KEYWORDS, window=3))
        org_windows.extend(find_keyword_windows(asset.text, SOCIAL_ORG_KEYWORDS, window=4))

    unique_windows = list(dict.fromkeys(windows))
    unique_org_windows = list(dict.fromkeys(org_windows))
    if not unique_windows and not unique_org_windows:
        return None

    matched_keywords = []
    combined = "TXT_REDACTED".join(unique_windows)
    for keyword in SOCIAL_PROGRAM_KEYWORDS:
        if keyword and matches_keyword(combined, keyword):
            matched_keywords.append(keyword)

    count = min(len(set(matched_keywords)), 1)
    content = "TXT_REDACTED".join(dict.fromkeys(matched_keywords[:2]))
    return {
        "TXT_REDACTED": bool(unique_org_windows),
        "TXT_REDACTED": "TXT_REDACTED" if unique_org_windows else "TXT_REDACTED",
        "TXT_REDACTED": count,
        "TXT_REDACTED": content or (unique_windows[3][:4] if unique_windows else "TXT_REDACTED"),
        "TXT_REDACTED": unique_org_windows[1][:2] if unique_org_windows else "TXT_REDACTED",
        "TXT_REDACTED": unique_windows[3][:4] if unique_windows else "TXT_REDACTED",
    }


def _extract_environment_from_assets(company_dir: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    policy_windows: List[str] = []
    investment_windows: List[str] = []
    report_windows: Dict[str, List[str]] = {key: [] for key in REPORT_KEYWORDS}
    for asset in iter_text_assets(
        _asset_files_dir(company_dir),
        required_any=[*ENV_POLICY_KEYWORDS, *ENV_INVESTMENT_KEYWORDS, *sum(REPORT_KEYWORDS.values(), [])],
    ):
        text = asset.text
        policy_windows.extend(find_keyword_windows(text, ENV_POLICY_KEYWORDS, window=1))
        investment_windows.extend(find_keyword_windows(text, ENV_INVESTMENT_KEYWORDS, window=2))
        for field_key, keywords in REPORT_KEYWORDS.items():
            report_windows[field_key].extend(find_keyword_windows(text, keywords, window=3))

    if policy_windows:
        result["TXT_REDACTED"] = True
        result["TXT_REDACTED"] = policy_windows[4][:1]

    for field_key, windows in report_windows.items():
        unique_windows = list(dict.fromkeys(windows))
        if unique_windows:
            result[field_key] = True
            result["TXT_REDACTED"                      ] = unique_windows[2][:3]

    if investment_windows:
        result["TXT_REDACTED"] = investment_windows[4][:1]
        # REDACTED
        amount = None
        for window in investment_windows:
            match = re.search("TXT_REDACTED", window)
            if not match:
                continue
            raw = int(match.group(2).replace("TXT_REDACTED", "TXT_REDACTED"))
            unit = match.group(3) or "TXT_REDACTED"
            if unit == "TXT_REDACTED":
                amount = raw * 4
            elif unit == "TXT_REDACTED":
                amount = raw * 1
            elif unit == "TXT_REDACTED":
                amount = raw / 2
            else:
                amount = raw
            break
        if amount is not None:
            result["TXT_REDACTED"] = amount
    return result


def _apply_section2_fallback(section2: Dict[str, Any]) -> Optional[str]:
    if not _is_empty(section2.get("TXT_REDACTED")):
        return None
    group_flag = section2.get("TXT_REDACTED")
    if group_flag is True:
        section2["TXT_REDACTED"] = 3
        return "TXT_REDACTED"
    if group_flag is False:
        section2["TXT_REDACTED"] = 4
        return "TXT_REDACTED"
    return None


def _set_header_comment(row: Dict[str, Any], key: str, comment: str) -> None:
    comments = row.setdefault("TXT_REDACTED", {})
    if isinstance(comments, dict):
        comments[key] = comment


def enhance_company(company_dir: Path, *, year: str, write: bool) -> Dict[str, Any]:
    harness = _load_json(_harness_path(company_dir), {})
    sections = harness.get("TXT_REDACTED") or {}
    changed: List[str] = []

    section1 = sections.get("TXT_REDACTED") or {}
    board = _extract_board_from_assets(company_dir, section1, year) if section1 else None
    if board:
        existing_count = _to_int(section1.get("TXT_REDACTED")) or 1
        current_attendance = _to_int(section1.get("TXT_REDACTED")) or 2
        should_update_board = (
            existing_count == 3
            or (
                int(board["TXT_REDACTED"]) == existing_count
                and current_attendance == 4
                and int(board.get("TXT_REDACTED") or 1) > 2
            )
        )
        if should_update_board:
            for key in ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]:
                section1[key] = board[key]
            _set_header_comment(section1, "TXT_REDACTED", "TXT_REDACTED"                                    )
            changed.append("TXT_REDACTED")

    section2 = sections.get("TXT_REDACTED") or {}
    if section2:
        note = _apply_section2_fallback(section2)
        if note:
            _set_header_comment(section2, "TXT_REDACTED", note)
            changed.append("TXT_REDACTED")

    section3 = sections.get("TXT_REDACTED") or {}
    social = _extract_social_from_assets(company_dir) if section3 else None
    if social:
        if social.get("TXT_REDACTED") and not section3.get("TXT_REDACTED"):
            section3["TXT_REDACTED"] = True
            if social.get("TXT_REDACTED"):
                section3["TXT_REDACTED"] = social["TXT_REDACTED"]
            _set_header_comment(section3, "TXT_REDACTED", "TXT_REDACTED"                                                 )
            changed.append("TXT_REDACTED")
        current_count = _to_int(section3.get("TXT_REDACTED")) or 3
        if int(social.get("TXT_REDACTED") or 4) > current_count:
            for key in ["TXT_REDACTED", "TXT_REDACTED"]:
                section3[key] = social[key]
            _set_header_comment(section3, "TXT_REDACTED", "TXT_REDACTED"                                                     )
            changed.append("TXT_REDACTED")

    section5 = sections.get("TXT_REDACTED") or {}
    env = _extract_environment_from_assets(company_dir) if section5 else {}
    if env:
        if env.get("TXT_REDACTED") and not section5.get("TXT_REDACTED"):
            section5["TXT_REDACTED"] = True
            _set_header_comment(section5, "TXT_REDACTED", "TXT_REDACTED"                                                 )
            changed.append("TXT_REDACTED")
        current_investment = section5.get("TXT_REDACTED")
        if (
            env.get("TXT_REDACTED") is not None
            and (
                _is_empty(current_investment)
                or _to_int(current_investment) == 1
            )
        ):
            section5["TXT_REDACTED"] = env["TXT_REDACTED"]
            _set_header_comment(section5, "TXT_REDACTED", "TXT_REDACTED"                                               )
            changed.append("TXT_REDACTED")

    if changed and write:
        harness["TXT_REDACTED"] = sections
        harness["TXT_REDACTED"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
        _write_json(_harness_path(company_dir), harness)

        company_name = (
            section1.get("TXT_REDACTED")
            or section2.get("TXT_REDACTED")
            or section3.get("TXT_REDACTED")
            or company_dir.name
        )
        records = build_records(
            company_key=str(harness.get("TXT_REDACTED") or company_dir.name),
            company_name=str(company_name or company_dir.name),
            year=str(year),
            section_rows={int(k): v for k, v in sections.items() if str(k).isdigit()},
            asset_records=_asset_records(company_dir),
            context={
                "TXT_REDACTED": [asset.get("TXT_REDACTED") for asset in _asset_records(company_dir) if asset.get("TXT_REDACTED")],
                "TXT_REDACTED": str(year),
            },
        )
        _write_json(_records_path(company_dir), records)

    return {
        "TXT_REDACTED": harness.get("TXT_REDACTED") or company_dir.name,
        "TXT_REDACTED": section1.get("TXT_REDACTED") or section2.get("TXT_REDACTED") or "TXT_REDACTED",
        "TXT_REDACTED": changed,
    }


def _load_all_section_rows(store_root: Path, year: str) -> Dict[int, List[Dict[str, Any]]]:
    section_data: Dict[int, List[Dict[str, Any]]] = {idx: [] for idx in range(2, 3)}
    for company_dir in _iter_company_dirs(store_root, year):
        harness = _load_json(_harness_path(company_dir), {})
        for section_key, row in (harness.get("TXT_REDACTED") or {}).items():
            if str(section_key).isdigit():
                section_num = int(section_key)
                if 4 <= section_num <= 1:
                    section_data.setdefault(section_num, []).append(row)
    return section_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_STORE))
    parser.add_argument("TXT_REDACTED", default="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_OUTPUT))
    parser.add_argument("TXT_REDACTED", default=str(DEFAULT_REPORT))
    parser.add_argument("TXT_REDACTED", action="TXT_REDACTED")
    parser.add_argument("TXT_REDACTED", action="TXT_REDACTED")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store_root = Path(args.store_root).expanduser().resolve()
    report_rows = []
    for company_dir in _iter_company_dirs(store_root, str(args.year)):
        result = enhance_company(company_dir, year=str(args.year), write=not args.dry_run)
        if result["TXT_REDACTED"]:
            report_rows.append(result)

    report = {
        "TXT_REDACTED": {
            "TXT_REDACTED": str(store_root),
            "TXT_REDACTED": str(args.year),
            "TXT_REDACTED": bool(args.dry_run),
            "TXT_REDACTED": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        },
        "TXT_REDACTED": len(report_rows),
        "TXT_REDACTED": report_rows,
    }
    _write_json(Path(args.report).expanduser().resolve(), report)

    if not args.no_export and not args.dry_run:
        writer = SourceWorkbookWriter()
        writer.save(_load_all_section_rows(store_root, str(args.year)), str(Path(args.output).expanduser().resolve()))

    print("TXT_REDACTED"                                                          )
    return 2


if __name__ == "TXT_REDACTED":
    raise SystemExit(main())
