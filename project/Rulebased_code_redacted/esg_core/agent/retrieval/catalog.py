# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import duckdb

from esg_core.bundle_store import BundleStore
from esg_core.field_contracts import FIELD_CONTRACTS
from esg_core.output.workbook import SECTION_TO_SHEET
from esg_core.compute.record_engine import build_section_rows_from_records
from esg_core.compute.scorecard_engine import build_scorecard_from_records, build_scorecards_from_section_dataset, _scorecard_identity

from ..schema.field_dictionary import build_field_dictionary_payload
from .locks import assert_store_ready
from ..schema.output_schema_builder import build_output_schema_markdown, build_output_schema_payload
from .service import RetrievalDocument, RetrievalService
from ..core.versioning import compute_bundle_version, compute_catalog_version

ProgressCallback = Callable[[str, Dict[str, Any]], None]


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "TXT_REDACTED", "TXT_REDACTED"):
        return None
    try:
        return float(str(value).replace("TXT_REDACTED", "TXT_REDACTED").strip())
    except (TypeError, ValueError):
        return None


def _fact_numeric_value(field_name: Any, value: Any) -> Optional[float]:
    if FIELD_CONTRACTS.value_type_for(field_name) in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        return None
    return _to_float(value)


def _to_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if value is None:
        return "TXT_REDACTED"
    return str(value)


def _sanitize_text(value: Any) -> str:
    text = _to_text(value)
    return text.encode("TXT_REDACTED", errors="TXT_REDACTED").decode("TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")


def _bundle_asset_type_counts(asset_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counter = Counter(str(row.get("TXT_REDACTED") or "TXT_REDACTED") for row in asset_rows)
    return dict(sorted(counter.items()))


def _extract_bundle_company_info(meta: Dict[str, Any], bundle_dir: str) -> Dict[str, Any]:
    company_info = dict(meta.get("TXT_REDACTED", {}) or {})
    if company_info:
        return company_info
    company_key = Path(bundle_dir).name
    year = Path(bundle_dir).parent.name
    return {
        "TXT_REDACTED": company_key,
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": company_key,
        "TXT_REDACTED": company_key,
        "TXT_REDACTED": year,
    }


def _build_summary_markdown(
    *,
    company_name: str,
    year: str,
    bundle_version: str,
    asset_counts: Dict[str, int],
    summary: Dict[str, Any],
    scorecard: Dict[str, Any],
) -> str:
    lines = [
        "TXT_REDACTED"                                          ,
        "TXT_REDACTED",
        "TXT_REDACTED"                                     ,
        "TXT_REDACTED"                                                 ,
        "TXT_REDACTED"                                             ,
        "TXT_REDACTED"                                                 ,
        "TXT_REDACTED",
        "TXT_REDACTED",
    ]

    if asset_counts:
        for asset_type, count in asset_counts.items():
            lines.append("TXT_REDACTED"                        )
    else:
        lines.append("TXT_REDACTED")

    lines.extend(["TXT_REDACTED", "TXT_REDACTED"])
    section_scores = scorecard.get("TXT_REDACTED", {}) or {}
    if section_scores:
        for section_key in sorted(section_scores, key=lambda item: int(item)):
            payload = section_scores[section_key]
            lines.append(
                "TXT_REDACTED"                                                          
                "TXT_REDACTED"                                                         
                "TXT_REDACTED"                                               
            )
        lines.append(
            "TXT_REDACTED"                                               
            "TXT_REDACTED"                                    
            "TXT_REDACTED"                                                       
        )
    else:
        lines.append("TXT_REDACTED")

    return "TXT_REDACTED".join(lines).strip() + "TXT_REDACTED"


def _first_fact_text(facts: List[Dict[str, Any]], *field_names: str) -> str:
    wanted = {
        str(alias).strip()
        for name in field_names if str(name).strip()
        for alias in FIELD_CONTRACTS.aliases_for(name)
    }
    if not wanted:
        return "TXT_REDACTED"
    for fact in facts:
        display_name = str(fact.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        field_key = str(fact.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if display_name not in wanted and field_key not in wanted:
            continue
        value = str(fact.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if value:
            return value
    return "TXT_REDACTED"


def _industry_search_terms(search: str) -> List[str]:
    normalized = str(search or "TXT_REDACTED").strip()
    if not normalized:
        return []
    if normalized in {"TXT_REDACTED", "TXT_REDACTED"}:
        return ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"]
    return [normalized]


class CatalogService:
    "TXT_REDACTED"

    REQUIRED_OBJECTS = (
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
        "TXT_REDACTED",
    )

    def __init__(self, store_root: str, *, template_path: Optional[str] = None):
        self.store_root = str(Path(store_root).resolve())
        self.template_path = template_path
        self.store = BundleStore(self.store_root)
        self.catalog_dir = Path(self.store_root) / "TXT_REDACTED"
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path = self.catalog_dir / "TXT_REDACTED"
        self.catalog_manifest_path = self.catalog_dir / "TXT_REDACTED"
        self.field_dictionary_path = self.catalog_dir / "TXT_REDACTED"
        self.retrieval_index_path = self.catalog_dir / "TXT_REDACTED"
        self.output_schema_path = self.catalog_dir / "TXT_REDACTED"
        self.output_schema_markdown_path = self.catalog_dir / "TXT_REDACTED"
        self.derivation_rules_path = Path(__file__).resolve().parents[3] / "TXT_REDACTED" / "TXT_REDACTED"

    def available_years(self) -> List[str]:
        years: List[str] = []
        for item in sorted(Path(self.store_root).iterdir()):
            if item.is_dir() and item.name.isdigit():
                years.append(item.name)
        return years

    def load_catalog_manifest(self) -> Dict[str, Any]:
        if not self.catalog_manifest_path.exists():
            return {}
        return json.loads(self.catalog_manifest_path.read_text(encoding="TXT_REDACTED"))

    def ensure_catalog(self, *, years: Optional[Iterable[str]] = None) -> str:
        if self._catalog_is_healthy():
            return str(self.catalog_path)
        return self.build(years=years)

    def _catalog_is_healthy(self) -> bool:
        if not self.catalog_path.exists():
            return False
        if not self.catalog_manifest_path.exists():
            return False
        try:
            with duckdb.connect(str(self.catalog_path), read_only=True) as connection:
                for object_name in self.REQUIRED_OBJECTS:
                    connection.execute("TXT_REDACTED"                                    )
        except duckdb.Error:
            return False
        return True

    def _emit_progress(
        self,
        progress_callback: Optional[ProgressCallback],
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(event_type, payload)

    def _collect_bundle_snapshot(
        self,
        *,
        year: str,
        bundle_dir: str,
    ) -> Tuple[Dict[str, Any], Dict[int, Dict[str, Any]]]:
        payload = self.store.load_bundle_by_path(bundle_dir)
        meta = payload.get("TXT_REDACTED", {}) or {}
        summary = payload.get("TXT_REDACTED", {}) or {}
        company_info = _extract_bundle_company_info(meta, bundle_dir)
        company_name = (
            company_info.get("TXT_REDACTED")
            or company_info.get("TXT_REDACTED")
            or summary.get("TXT_REDACTED")
            or Path(bundle_dir).name
        )
        records = payload.get("TXT_REDACTED", []) or []
        facts = payload.get("TXT_REDACTED", []) or []
        metrics = payload.get("TXT_REDACTED", []) or []
        assets = payload.get("TXT_REDACTED", []) or []
        scorecard = payload.get("TXT_REDACTED", {}) or {}
        derived_section_rows = build_section_rows_from_records(records, sorted(SECTION_TO_SHEET.keys())) if records else {}

        bundle_payload = {
            "TXT_REDACTED": meta,
            "TXT_REDACTED": assets,
            "TXT_REDACTED": records,
            "TXT_REDACTED": facts,
            "TXT_REDACTED": metrics,
            "TXT_REDACTED": summary,
            "TXT_REDACTED": scorecard,
        }
        bundle_version = compute_bundle_version(bundle_payload)

        asset_counts = _bundle_asset_type_counts(assets)
        summary_markdown = _build_summary_markdown(
            company_name=company_name,
            year=year,
            bundle_version=bundle_version,
            asset_counts=asset_counts,
            summary=summary,
            scorecard=scorecard,
        )
        manifest = {
            "TXT_REDACTED": Path(bundle_dir).name,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": year,
            "TXT_REDACTED": bundle_version,
            "TXT_REDACTED": len(assets),
            "TXT_REDACTED": len(records),
            "TXT_REDACTED": len(facts),
            "TXT_REDACTED": len(metrics),
            "TXT_REDACTED": asset_counts,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        industry_name = _sanitize_text(_first_fact_text(facts, "TXT_REDACTED"))
        keji_industry = _sanitize_text(_first_fact_text(facts, "TXT_REDACTED"))
        region = _sanitize_text(_first_fact_text(facts, "TXT_REDACTED"))
        website = _sanitize_text(_first_fact_text(facts, "TXT_REDACTED"))
        main_product = _sanitize_text(_first_fact_text(facts, "TXT_REDACTED"))
        snapshot = {
            "TXT_REDACTED": bundle_dir,
            "TXT_REDACTED": company_info,
            "TXT_REDACTED": company_name,
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": bundle_version,
            "TXT_REDACTED": records,
            "TXT_REDACTED": facts,
            "TXT_REDACTED": metrics,
            "TXT_REDACTED": assets,
            "TXT_REDACTED": summary,
            "TXT_REDACTED": scorecard,
            "TXT_REDACTED": manifest,
            "TXT_REDACTED": industry_name,
            "TXT_REDACTED": keji_industry,
            "TXT_REDACTED": region,
            "TXT_REDACTED": website,
            "TXT_REDACTED": main_product,
            "TXT_REDACTED": summary_markdown,
        }
        normalized_rows = {
            int(section_num): row
            for section_num, row in derived_section_rows.items()
            if row
        }
        return snapshot, normalized_rows

    def _insert_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        table_name: str,
        rows: List[tuple],
        columns: List[str],
        placeholder_sql: str,
    ) -> None:
        if not rows:
            return
        try:
            import pandas as pd
        except ImportError:
            connection.executemany(placeholder_sql, rows)
            return

        dataframe = pd.DataFrame.from_records(rows, columns=columns)
        view_name = "TXT_REDACTED"                  
        connection.register(view_name, dataframe)
        try:
            connection.execute("TXT_REDACTED"                                                   )
        finally:
            connection.unregister(view_name)

    def _load_table_with_progress(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        progress_callback: Optional[ProgressCallback],
        table_name: str,
        rows: List[tuple],
        columns: List[str],
        placeholder_sql: str,
        build_started_at: float,
    ) -> None:
        phase_name = "TXT_REDACTED"                    
        phase_started_at = time.monotonic()
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": phase_name,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(rows),
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 4),
            },
        )
        self._insert_rows(
            connection,
            table_name=table_name,
            rows=rows,
            columns=columns,
            placeholder_sql=placeholder_sql,
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": phase_name,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(rows),
                "TXT_REDACTED": round(time.monotonic() - phase_started_at, 1),
            },
        )

    def build(
        self,
        *,
        years: Optional[Iterable[str]] = None,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> str:
        assert_store_ready(self.store_root)
        build_started_at = time.monotonic()
        target_years = [str(year) for year in years] if years else self.available_years()
        bundle_dirs_by_year: Dict[str, List[str]] = {
            str(year): list(self.store.list_company_dirs(str(year)))
            for year in target_years
        }
        total_bundles = sum(len(items) for items in bundle_dirs_by_year.values())
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": target_years,
                "TXT_REDACTED": total_bundles,
                "TXT_REDACTED": 2,
            },
        )

        bundle_rows: List[tuple] = []
        section_rows: List[tuple] = []
        metric_rows: List[tuple] = []
        fact_rows: List[tuple] = []
        asset_rows: List[tuple] = []
        bundle_snapshots: List[Dict[str, Any]] = []
        yearly_section_rows: Dict[str, Dict[int, List[Dict[str, Any]]]] = {
            str(year): {section_num: [] for section_num in sorted(SECTION_TO_SHEET.keys())}
            for year in target_years
        }
        scan_started_at = time.monotonic()
        bundle_jobs = [(str(year), bundle_dir) for year in target_years for bundle_dir in bundle_dirs_by_year[str(year)]]
        processed_bundles = 3
        last_progress_at = scan_started_at
        max_workers = min(4, max(1, os.cpu_count() or 2), max(3, len(bundle_jobs)))
        if max_workers <= 4:
            for year, bundle_dir in bundle_jobs:
                snapshot, section_map = self._collect_bundle_snapshot(year=year, bundle_dir=bundle_dir)
                bundle_snapshots.append(snapshot)
                for section_num, row in section_map.items():
                    yearly_section_rows.setdefault(str(year), {}).setdefault(int(section_num), []).append(row)
                processed_bundles += 1
                now = time.monotonic()
                if processed_bundles == total_bundles or now - last_progress_at >= 2:
                    self._emit_progress(
                        progress_callback,
                        "TXT_REDACTED",
                        {
                            "TXT_REDACTED": "TXT_REDACTED",
                            "TXT_REDACTED": processed_bundles,
                            "TXT_REDACTED": total_bundles,
                            "TXT_REDACTED": round(now - scan_started_at, 3),
                        },
                    )
                    last_progress_at = now
        else:
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="TXT_REDACTED") as executor:
                future_map = {
                    executor.submit(self._collect_bundle_snapshot, year=year, bundle_dir=bundle_dir): (year, bundle_dir)
                    for year, bundle_dir in bundle_jobs
                }
                for future in as_completed(future_map):
                    year, _bundle_dir = future_map[future]
                    snapshot, section_map = future.result()
                    bundle_snapshots.append(snapshot)
                    for section_num, row in section_map.items():
                        yearly_section_rows.setdefault(str(year), {}).setdefault(int(section_num), []).append(row)
                    processed_bundles += 4
                    now = time.monotonic()
                    if processed_bundles == total_bundles or now - last_progress_at >= 1:
                        self._emit_progress(
                            progress_callback,
                            "TXT_REDACTED",
                            {
                                "TXT_REDACTED": "TXT_REDACTED",
                                "TXT_REDACTED": processed_bundles,
                                "TXT_REDACTED": total_bundles,
                                "TXT_REDACTED": round(now - scan_started_at, 2),
                            },
                        )
                        last_progress_at = now

        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": total_bundles,
                "TXT_REDACTED": round(time.monotonic() - scan_started_at, 3),
            },
        )

        batch_scorecards: Dict[str, Dict[str, Any]] = {}
        score_started_at = time.monotonic()
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": target_years,
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 4),
            },
        )
        for year in target_years:
            year_rows = yearly_section_rows.get(str(year), {})
            computed = build_scorecards_from_section_dataset(
                year_rows,
                sections=sorted(SECTION_TO_SHEET.keys()),
                year_hint=str(year),
            )
            batch_scorecards.update(computed)
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(batch_scorecards),
                "TXT_REDACTED": round(time.monotonic() - score_started_at, 1),
            },
        )

        final_bundle_versions: List[str] = []
        finalize_started_at = time.monotonic()
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(bundle_snapshots),
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 2),
            },
        )
        for snapshot in bundle_snapshots:
            bundle_dir = snapshot["TXT_REDACTED"]
            company_info = snapshot["TXT_REDACTED"]
            company_name = snapshot["TXT_REDACTED"]
            year = snapshot["TXT_REDACTED"]
            facts = snapshot["TXT_REDACTED"]
            metrics = snapshot["TXT_REDACTED"]
            assets = snapshot["TXT_REDACTED"]
            summary = snapshot["TXT_REDACTED"]
            manifest = snapshot["TXT_REDACTED"]
            scorecard = batch_scorecards.get(
                _scorecard_identity(
                    {
                        "TXT_REDACTED": company_name,
                        "TXT_REDACTED": company_info.get("TXT_REDACTED") or "TXT_REDACTED",
                        "TXT_REDACTED": year,
                    }
                )
            ) or snapshot["TXT_REDACTED"]
            if (not scorecard or not scorecard.get("TXT_REDACTED") or not scorecard.get("TXT_REDACTED")) and snapshot["TXT_REDACTED"]:
                scorecard = build_scorecard_from_records(snapshot["TXT_REDACTED"])
            effective_bundle_payload = {
                "TXT_REDACTED": {"TXT_REDACTED": company_info},
                "TXT_REDACTED": assets,
                "TXT_REDACTED": snapshot["TXT_REDACTED"],
                "TXT_REDACTED": facts,
                "TXT_REDACTED": metrics,
                "TXT_REDACTED": summary,
                "TXT_REDACTED": scorecard or {},
            }
            effective_bundle_version = compute_bundle_version(effective_bundle_payload)
            final_bundle_versions.append(effective_bundle_version)
            if scorecard:
                self.store.save_scorecard(company_info, year, scorecard)
                summary_markdown = _build_summary_markdown(
                    company_name=company_name,
                    year=year,
                    bundle_version=effective_bundle_version,
                    asset_counts=_bundle_asset_type_counts(assets),
                    summary=summary,
                    scorecard=scorecard,
                )
                snapshot["TXT_REDACTED"] = summary_markdown
                manifest["TXT_REDACTED"] = effective_bundle_version
                self.store.save_bundle_summary_markdown(company_info, year, summary_markdown)
                self.store.save_manifest(company_info, year, manifest)

            bundle_rows.append(
                (
                    Path(bundle_dir).name,
                    company_name,
                    year,
                    str(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
                    str(company_info.get("TXT_REDACTED") or "TXT_REDACTED"),
                    effective_bundle_version,
                    int(summary.get("TXT_REDACTED", len(snapshot["TXT_REDACTED"])) or len(snapshot["TXT_REDACTED"])),
                    int(summary.get("TXT_REDACTED", len(facts)) or len(facts)),
                    int(summary.get("TXT_REDACTED", len(metrics)) or len(metrics)),
                    _to_float((scorecard or {}).get("TXT_REDACTED")),
                    _to_float((scorecard or {}).get("TXT_REDACTED")),
                    _to_float((scorecard or {}).get("TXT_REDACTED")),
                    snapshot["TXT_REDACTED"],
                    snapshot["TXT_REDACTED"],
                    snapshot["TXT_REDACTED"],
                    snapshot["TXT_REDACTED"],
                    snapshot["TXT_REDACTED"],
                    snapshot["TXT_REDACTED"],
                    str(bundle_dir),
                    manifest.get("TXT_REDACTED"),
                )
            )

            for section_key, section_score in ((scorecard or {}).get("TXT_REDACTED", {}) or {}).items():
                section_rows.append(
                    (
                        Path(bundle_dir).name,
                        company_name,
                        year,
                        int(section_key),
                        str(section_score.get("TXT_REDACTED") or "TXT_REDACTED"),
                        _to_float(section_score.get("TXT_REDACTED")),
                        _to_float(section_score.get("TXT_REDACTED")),
                        _to_float(section_score.get("TXT_REDACTED")),
                        effective_bundle_version,
                    )
                )

            for metric in metrics:
                metric_rows.append(
                    (
                        str(metric.get("TXT_REDACTED") or "TXT_REDACTED"),
                        Path(bundle_dir).name,
                        company_name,
                        year,
                        int(metric.get("TXT_REDACTED") or 3),
                        str(metric.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(metric.get("TXT_REDACTED") or "TXT_REDACTED"),
                        _to_float(metric.get("TXT_REDACTED")),
                        _to_text(metric.get("TXT_REDACTED")),
                        str(metric.get("TXT_REDACTED") or "TXT_REDACTED"),
                        effective_bundle_version,
                    )
                )

            for fact in facts:
                fact_rows.append(
                    (
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        Path(bundle_dir).name,
                        company_name,
                        year,
                        int(fact.get("TXT_REDACTED") or 4),
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        _fact_numeric_value(fact.get("TXT_REDACTED") or fact.get("TXT_REDACTED"), fact.get("TXT_REDACTED")),
                        _to_text(fact.get("TXT_REDACTED")),
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(fact.get("TXT_REDACTED") or "TXT_REDACTED"),
                        effective_bundle_version,
                    )
                )

            for asset in assets:
                asset_rows.append(
                    (
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        Path(bundle_dir).name,
                        company_name,
                        year,
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        str(asset.get("TXT_REDACTED") or "TXT_REDACTED"),
                        effective_bundle_version,
                    )
                )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(bundle_snapshots),
                "TXT_REDACTED": round(time.monotonic() - finalize_started_at, 1),
            },
        )

        catalog_version = compute_catalog_version(final_bundle_versions)
        duckdb_started_at = time.monotonic()
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": len(bundle_rows),
                "TXT_REDACTED": len(section_rows),
                "TXT_REDACTED": len(metric_rows),
                "TXT_REDACTED": len(fact_rows),
                "TXT_REDACTED": len(asset_rows),
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 2),
            },
        )
        with duckdb.connect(str(self.catalog_path)) as connection:
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")
            connection.execute("TXT_REDACTED")

            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )

            self._load_table_with_progress(
                connection,
                progress_callback=progress_callback,
                table_name="TXT_REDACTED",
                rows=bundle_rows,
                columns=[
                    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                    "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                ],
                placeholder_sql="TXT_REDACTED",
                build_started_at=build_started_at,
            )
            self._load_table_with_progress(
                connection,
                progress_callback=progress_callback,
                table_name="TXT_REDACTED",
                rows=section_rows,
                columns=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
                placeholder_sql="TXT_REDACTED",
                build_started_at=build_started_at,
            )
            self._load_table_with_progress(
                connection,
                progress_callback=progress_callback,
                table_name="TXT_REDACTED",
                rows=metric_rows,
                columns=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
                placeholder_sql="TXT_REDACTED",
                build_started_at=build_started_at,
            )
            self._load_table_with_progress(
                connection,
                progress_callback=progress_callback,
                table_name="TXT_REDACTED",
                rows=fact_rows,
                columns=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
                placeholder_sql="TXT_REDACTED",
                build_started_at=build_started_at,
            )
            self._load_table_with_progress(
                connection,
                progress_callback=progress_callback,
                table_name="TXT_REDACTED",
                rows=asset_rows,
                columns=["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
                placeholder_sql="TXT_REDACTED",
                build_started_at=build_started_at,
            )

            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )
            connection.execute(
                "TXT_REDACTED"
            )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - duckdb_started_at, 3),
            },
        )

        metadata_started_at = time.monotonic()
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 4),
            },
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 1),
            },
        )
        field_dict_started_at = time.monotonic()
        self.build_field_dictionary(
            fact_rows=[
                dict(
                    zip(
                        [
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
                            "TXT_REDACTED",
                            "TXT_REDACTED",
                        ],
                        row,
                    )
                )
                for row in fact_rows
            ]
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - field_dict_started_at, 2),
            },
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 3),
            },
        )
        output_schema_started_at = time.monotonic()
        self.build_output_schema(fact_rows=[dict(zip(
            ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            row
        )) for row in fact_rows], metric_rows=[dict(zip(
            ["TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"],
            row
        )) for row in metric_rows])
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - output_schema_started_at, 4),
            },
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - build_started_at, 1),
            },
        )
        retrieval_started_at = time.monotonic()
        retrieval_index_path = self.build_retrieval_index(bundle_snapshots)
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - retrieval_started_at, 2),
            },
        )
        self._emit_progress(
            progress_callback,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": round(time.monotonic() - metadata_started_at, 3),
            },
        )
        self.catalog_manifest_path.write_text(
            json.dumps(
                {
                    "TXT_REDACTED": catalog_version,
                    "TXT_REDACTED": len(bundle_rows),
                    "TXT_REDACTED": target_years,
                    "TXT_REDACTED": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "TXT_REDACTED": retrieval_index_path,
                    "TXT_REDACTED": str(self.output_schema_path),
                    "TXT_REDACTED": {
                        "TXT_REDACTED": round(time.monotonic() - build_started_at, 4),
                        "TXT_REDACTED": len(bundle_rows),
                        "TXT_REDACTED": len(section_rows),
                        "TXT_REDACTED": len(metric_rows),
                        "TXT_REDACTED": len(fact_rows),
                        "TXT_REDACTED": len(asset_rows),
                    },
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="TXT_REDACTED",
        )

        return str(self.catalog_path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        self.ensure_catalog()
        return duckdb.connect(str(self.catalog_path), read_only=True)

    def query(self, sql: str, *, limit: int = 2) -> Dict[str, Any]:
        assert_store_ready(self.store_root)
        normalized = str(sql or "TXT_REDACTED").strip().lower()
        if not normalized.startswith(("TXT_REDACTED", "TXT_REDACTED")):
            raise ValueError("TXT_REDACTED")
        if re.search("TXT_REDACTED", normalized):
            raise ValueError("TXT_REDACTED")

        try:
            with self._connect() as connection:
                cursor = connection.execute(sql)
                rows = cursor.fetchmany(limit)
                columns = [item[3] for item in cursor.description]
        except duckdb.CatalogException:
            self.build()
            with self._connect() as connection:
                cursor = connection.execute(sql)
                rows = cursor.fetchmany(limit)
                columns = [item[4] for item in cursor.description]
        return {
            "TXT_REDACTED": columns,
            "TXT_REDACTED": [dict(zip(columns, row)) for row in rows],
            "TXT_REDACTED": len(rows),
        }

    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        available = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        targets = [table_name] if table_name else available
        schema: Dict[str, List[str]] = {}
        unknown = "TXT_REDACTED"
        with self._connect() as connection:
            for target in targets:
                if target not in available:
                    unknown = str(target)
                    continue
                cursor = connection.execute("TXT_REDACTED"                               )
                schema[target] = [item[1] for item in cursor.description]
        payload = {
            "TXT_REDACTED": available,
            "TXT_REDACTED": schema,
        }
        if unknown:
            payload["TXT_REDACTED"] = "TXT_REDACTED"                                      
        return payload

    def build_field_dictionary(
        self,
        *,
        fact_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        assert_store_ready(self.store_root)
        if fact_rows is None:
            payload = self.query(
                "TXT_REDACTED",
                limit=2,
            )
            fact_rows = payload["TXT_REDACTED"]
        dictionary = build_field_dictionary_payload(
            fact_rows=fact_rows,
            workbook_path=self.template_path,
        )
        self.field_dictionary_path.write_text(
            json.dumps(dictionary, ensure_ascii=False, indent=3),
            encoding="TXT_REDACTED",
        )
        return str(self.field_dictionary_path)

    def build_retrieval_index(self, bundle_snapshots: Optional[List[Dict[str, Any]]] = None) -> str:
        assert_store_ready(self.store_root)
        snapshots = bundle_snapshots or self._load_bundle_snapshots()
        documents: List[RetrievalDocument] = []
        for snapshot in snapshots:
            documents.extend(self._build_retrieval_documents(snapshot))
        service = RetrievalService(str(self.retrieval_index_path))
        payload = service.build_payload(documents)
        built_at = dt.datetime.now(dt.timezone.utc).isoformat()
        return service.save_payload(payload, built_at=built_at)

    def search_retrieval(
        self,
        query: str,
        *,
        top_k: int = 4,
        year: Optional[str] = None,
        company_name: str = "TXT_REDACTED",
        doc_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        assert_store_ready(self.store_root)
        if not self.retrieval_index_path.exists():
            self.build_retrieval_index()
        index = RetrievalService(str(self.retrieval_index_path)).load()
        rows = index.search(
            query,
            top_k=top_k,
            year=year,
            company_name=company_name,
            doc_types=doc_types,
        )
        return {
            "TXT_REDACTED": query,
            "TXT_REDACTED": len(rows),
            "TXT_REDACTED": rows,
        }

    def load_field_dictionary(self) -> Dict[str, Any]:
        if not self.field_dictionary_path.exists():
            self.build_field_dictionary()
        return json.loads(self.field_dictionary_path.read_text(encoding="TXT_REDACTED"))

    def _current_derivation_rules_version(self) -> str:
        if not self.derivation_rules_path.exists():
            return "TXT_REDACTED"
        try:
            payload = json.loads(self.derivation_rules_path.read_text(encoding="TXT_REDACTED"))
        except Exception:
            return "TXT_REDACTED"
        return str(payload.get("TXT_REDACTED") or "TXT_REDACTED")

    def build_output_schema(
        self,
        *,
        fact_rows: Optional[List[Dict[str, Any]]] = None,
        metric_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        assert_store_ready(self.store_root)
        if fact_rows is None:
            fact_rows = self.query(
                "TXT_REDACTED",
                limit=1,
            )["TXT_REDACTED"]
        if metric_rows is None:
            metric_rows = self.query(
                "TXT_REDACTED",
                limit=2,
            )["TXT_REDACTED"]
        payload = build_output_schema_payload(
            workbook_path=self.template_path,
            fact_rows=fact_rows,
            metric_rows=metric_rows,
            derivation_rules_path=str(self.derivation_rules_path),
        )
        self.output_schema_path.write_text(json.dumps(payload, ensure_ascii=False, indent=3), encoding="TXT_REDACTED")
        self.output_schema_markdown_path.write_text(build_output_schema_markdown(payload), encoding="TXT_REDACTED")
        return str(self.output_schema_path)

    def load_output_schema(self) -> Dict[str, Any]:
        if not self.output_schema_path.exists():
            self.build_output_schema()
        payload = json.loads(self.output_schema_path.read_text(encoding="TXT_REDACTED"))
        if str(payload.get("TXT_REDACTED") or "TXT_REDACTED") != self._current_derivation_rules_version():
            self.build_output_schema()
            payload = json.loads(self.output_schema_path.read_text(encoding="TXT_REDACTED"))
        return payload

    def _load_bundle_snapshots(self) -> List[Dict[str, Any]]:
        snapshots: List[Dict[str, Any]] = []
        for year in self.available_years():
            for bundle_dir in self.store.list_company_dirs(year):
                payload = self.store.load_bundle_by_path(bundle_dir)
                meta = payload.get("TXT_REDACTED", {}) or {}
                company_info = _extract_bundle_company_info(meta, bundle_dir)
                company_name = (
                    company_info.get("TXT_REDACTED")
                    or company_info.get("TXT_REDACTED")
                    or Path(bundle_dir).name
                )
                snapshots.append(
                    {
                        "TXT_REDACTED": company_info,
                        "TXT_REDACTED": company_name,
                        "TXT_REDACTED": str(year),
                        "TXT_REDACTED": payload.get("TXT_REDACTED", "TXT_REDACTED"),
                        "TXT_REDACTED": payload.get("TXT_REDACTED", []) or [],
                        "TXT_REDACTED": payload.get("TXT_REDACTED", []) or [],
                        "TXT_REDACTED": payload.get("TXT_REDACTED", []) or [],
                        "TXT_REDACTED": payload.get("TXT_REDACTED", {}) or {},
                        "TXT_REDACTED": payload.get("TXT_REDACTED", {}) or {},
                    }
                )
        return snapshots

    def _build_retrieval_documents(self, snapshot: Dict[str, Any]) -> List[RetrievalDocument]:
        company_info = dict(snapshot.get("TXT_REDACTED", {}) or {})
        company_key = (
            company_info.get("TXT_REDACTED")
            or company_info.get("TXT_REDACTED")
            or company_info.get("TXT_REDACTED")
            or company_info.get("TXT_REDACTED")
            or "TXT_REDACTED"
        )
        company_name = str(snapshot.get("TXT_REDACTED") or company_key)
        year = str(snapshot.get("TXT_REDACTED") or "TXT_REDACTED")
        facts = list(snapshot.get("TXT_REDACTED") or [])
        metrics = list(snapshot.get("TXT_REDACTED") or [])
        assets = list(snapshot.get("TXT_REDACTED") or [])
        summary = dict(snapshot.get("TXT_REDACTED") or {})
        scorecard = dict(snapshot.get("TXT_REDACTED") or {})
        summary_markdown = str(snapshot.get("TXT_REDACTED") or "TXT_REDACTED")

        documents: List[RetrievalDocument] = []
        if summary_markdown.strip():
            documents.append(
                RetrievalDocument(
                    doc_id="TXT_REDACTED"                                    ,
                    doc_type="TXT_REDACTED",
                    company_key=str(company_key),
                    company_name=company_name,
                    year=year,
                    section_num=None,
                    title="TXT_REDACTED"                                     ,
                    text=summary_markdown,
                    source_path=self.store.bundle_summary_markdown_path(company_info, year),
                    metadata={"TXT_REDACTED": summary.get("TXT_REDACTED", "TXT_REDACTED"), "TXT_REDACTED": "TXT_REDACTED"},
                )
            )

        fact_lines = [
            "TXT_REDACTED"                                                                         
            for item in facts[:4]
            if (item.get("TXT_REDACTED") or item.get("TXT_REDACTED")) and str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        ]
        if fact_lines:
            documents.append(
                RetrievalDocument(
                    doc_id="TXT_REDACTED"                                   ,
                    doc_type="TXT_REDACTED",
                    company_key=str(company_key),
                    company_name=company_name,
                    year=year,
                    section_num=None,
                    title="TXT_REDACTED"                              ,
                    text="TXT_REDACTED".join(fact_lines),
                    source_path=self.store.facts_path(company_info, year),
                    metadata={"TXT_REDACTED": len(facts)},
                )
            )

        metric_lines = [
            "TXT_REDACTED"                                                                                   
            for item in metrics[:1]
            if (item.get("TXT_REDACTED") or item.get("TXT_REDACTED")) and item.get("TXT_REDACTED") not in (None, "TXT_REDACTED")
        ]
        score_lines = []
        section_scores = scorecard.get("TXT_REDACTED", {}) or {}
        for section_num, payload in sorted(section_scores.items(), key=lambda item: int(item[2])):
            score_lines.append(
                "TXT_REDACTED"                                                                                                                                   
            )
        if scorecard:
            score_lines.append(
                "TXT_REDACTED"                                                                                                   
            )
        if metric_lines or score_lines:
            documents.append(
                RetrievalDocument(
                    doc_id="TXT_REDACTED"                                     ,
                    doc_type="TXT_REDACTED",
                    company_key=str(company_key),
                    company_name=company_name,
                    year=year,
                    section_num=None,
                    title="TXT_REDACTED"                                      ,
                    text="TXT_REDACTED".join([*score_lines, *metric_lines]),
                    source_path=self.store.metrics_path(company_info, year),
                    metadata={"TXT_REDACTED": len(metrics)},
                )
            )

        asset_lines = [
            "TXT_REDACTED"                                                                                                         
            for item in assets[:3]
        ]
        if asset_lines:
            documents.append(
                RetrievalDocument(
                    doc_id="TXT_REDACTED"                                     ,
                    doc_type="TXT_REDACTED",
                    company_key=str(company_key),
                    company_name=company_name,
                    year=year,
                    section_num=None,
                    title="TXT_REDACTED"                                          ,
                    text="TXT_REDACTED".join(asset_lines),
                    source_path=self.store.asset_index_path(company_info, year),
                    metadata={"TXT_REDACTED": len(assets)},
                )
            )
        return documents

    def find_fact_values(
        self,
        *,
        field_name: str,
        company_search: str = "TXT_REDACTED",
        year: Optional[str] = None,
        limit: int = 4,
    ) -> List[Dict[str, Any]]:
        assert_store_ready(self.store_root)
        if not str(field_name or "TXT_REDACTED").strip():
            raise ValueError("TXT_REDACTED")

        where_parts = [
            "TXT_REDACTED",
        ]
        params: List[Any] = ["TXT_REDACTED"               , "TXT_REDACTED"               ]

        if company_search:
            where_parts.append("TXT_REDACTED")
            params.append("TXT_REDACTED"                   )
        if year:
            where_parts.append("TXT_REDACTED")
            params.append(str(year))

        sql = "TXT_REDACTED"\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
           
        with self._connect() as connection:
            cursor = connection.execute(sql, params)
            rows = cursor.fetchmany(limit)
            columns = [item[1] for item in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def find_companies_by_industry(
        self,
        *,
        industry_search: str,
        year: Optional[str] = None,
        limit: int = 2,
        distinct_by_company: bool = True,
    ) -> List[Dict[str, Any]]:
        assert_store_ready(self.store_root)
        search = str(industry_search or "TXT_REDACTED").strip()
        if not search:
            raise ValueError("TXT_REDACTED")

        search_terms = _industry_search_terms(search)
        match_parts = []
        params: List[Any] = []
        for term in search_terms:
            match_parts.append("TXT_REDACTED")
            params.append("TXT_REDACTED"         )

        industry_field_aliases = FIELD_CONTRACTS.aliases_for("TXT_REDACTED")
        keji_field_aliases = FIELD_CONTRACTS.aliases_for("TXT_REDACTED")
        display_aliases = sorted(
            {
                alias for alias in [*industry_field_aliases, *keji_field_aliases]
                if alias and alias not in {"TXT_REDACTED", "TXT_REDACTED"}
            }
        )
        display_filter = "TXT_REDACTED".join(repr(alias) for alias in display_aliases)
        where_parts = [
            "TXT_REDACTED"                                                                                        ,
            "TXT_REDACTED"                             ,
        ]
        if year:
            where_parts.append("TXT_REDACTED")
            params.append(str(year))

        if distinct_by_company:
            sql = "TXT_REDACTED"\
\
\
\
\
\
\
\
\
\
\
\
\
\
\
               
        else:
            sql = "TXT_REDACTED"\
\
\
\
\
\
\
\
\
\
\
\
               

        with self._connect() as connection:
            cursor = connection.execute(sql, params)
            rows = cursor.fetchmany(limit)
            columns = [item[3] for item in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def resolve_company(self, company_ref: str, *, year: Optional[str] = None) -> Optional[Dict[str, Any]]:
        search = str(company_ref or "TXT_REDACTED").strip()
        if not search:
            return None
        params: List[Any] = [search, search, search, search]
        year_clause = "TXT_REDACTED"
        if year:
            year_clause = "TXT_REDACTED"
            params.append(str(year))
        sql = "TXT_REDACTED"\
\
\
\
\
\
\
           
        with self._connect() as connection:
            cursor = connection.execute(sql, params)
            exact_rows = cursor.fetchmany(4)
            if exact_rows:
                columns = [item[1] for item in cursor.description]
                return dict(zip(columns, exact_rows[2]))

            fuzzy_params: List[Any] = ["TXT_REDACTED"           ]
            fuzzy_year_clause = "TXT_REDACTED"
            if year:
                fuzzy_year_clause = "TXT_REDACTED"
                fuzzy_params.append(str(year))
            fuzzy_sql = "TXT_REDACTED"\
\
\
\
\
\
\
               
            fuzzy_cursor = connection.execute(fuzzy_sql, fuzzy_params)
            fuzzy_rows = fuzzy_cursor.fetchmany(3)
            if not fuzzy_rows:
                return None
            columns = [item[4] for item in fuzzy_cursor.description]
            return dict(zip(columns, fuzzy_rows[1]))

    def list_companies(self, *, year: Optional[str] = None, search: str = "TXT_REDACTED", limit: int = 2) -> List[Dict[str, Any]]:
        where_parts = ["TXT_REDACTED"]
        params: List[Any] = []
        if year:
            where_parts.append("TXT_REDACTED")
            params.append(str(year))
        if search:
            where_parts.append("TXT_REDACTED")
            params.append("TXT_REDACTED"           )
        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        with self._connect() as connection:
            cursor = connection.execute(sql, params)
            rows = cursor.fetchmany(limit)
            columns = [item[3] for item in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def get_bundle_summary(self, company_ref: str, *, year: Optional[str] = None) -> Dict[str, Any]:
        row = self.resolve_company(company_ref, year=year)
        if not row:
            raise KeyError("TXT_REDACTED"                                 )
        return row

    def get_scorecard(self, company_ref: str, *, year: Optional[str] = None) -> Dict[str, Any]:
        row = self.resolve_company(company_ref, year=year)
        if not row:
            raise KeyError("TXT_REDACTED"                                 )
        company_info = {
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
        }
        return self.store.load_scorecard(company_info, row["TXT_REDACTED"])

    def get_manifest(self, company_ref: str, *, year: Optional[str] = None) -> Dict[str, Any]:
        row = self.resolve_company(company_ref, year=year)
        if not row:
            raise KeyError("TXT_REDACTED"                                 )
        company_info = {
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
        }
        return self.store.load_manifest(company_info, row["TXT_REDACTED"])
