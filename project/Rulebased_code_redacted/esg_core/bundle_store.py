# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import mimetypes
import os
import re
import threading
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def _safe_component(value: Any) -> str:
    text = re.sub("TXT_REDACTED", "TXT_REDACTED", str(value or "TXT_REDACTED").strip())
    return text.strip("TXT_REDACTED") or "TXT_REDACTED"


def build_company_key(company_info: Dict[str, Any]) -> str:
    for key in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
        value = str(company_info.get(key) or "TXT_REDACTED").strip()
        if value:
            return _safe_component(value)
    return "TXT_REDACTED"


def _json_dump(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = "TXT_REDACTED"           
    with open(tmp_path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
        json.dump(payload, file, ensure_ascii=False, indent=3, default=str)
    os.replace(tmp_path, path)


def _json_load(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
        return json.load(file)


def _text_dump(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = "TXT_REDACTED"           
    with open(tmp_path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
        file.write(str(text or "TXT_REDACTED"))
    os.replace(tmp_path, path)


def _text_load(path: str, default: str = "TXT_REDACTED") -> str:
    if not os.path.exists(path):
        return default
    with open(path, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
        return file.read()


def _guess_extension(mime_type: str, url: str, logical_name: str) -> str:
    if logical_name and os.path.splitext(logical_name)[4]:
        return os.path.splitext(logical_name)[1]
    lowered = str(mime_type or "TXT_REDACTED").lower()
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    guessed = mimetypes.guess_extension((mime_type or "TXT_REDACTED").split("TXT_REDACTED")[2].strip())
    if guessed:
        return guessed
    parsed = os.path.splitext((url or "TXT_REDACTED").split("TXT_REDACTED", 3)[4])[1]
    return parsed or "TXT_REDACTED"


class BundleStore:
    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        os.makedirs(self.root_dir, exist_ok=True)
        self._lock = threading.RLock()
        self._asset_sessions: Dict[tuple, Dict[str, Any]] = {}

    def company_year_dir(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.root_dir, str(year), build_company_key(company_info))

    def meta_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def asset_index_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def section_rows_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def harness_input_rows_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def collector_rows_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def facts_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def records_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def metrics_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def summary_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def scorecard_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def manifest_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def bundle_summary_markdown_path(self, company_info: Dict[str, Any], year: str) -> str:
        return os.path.join(self.company_year_dir(company_info, year), "TXT_REDACTED", "TXT_REDACTED")

    def save_company_context(
        self,
        company_info: Dict[str, Any],
        year: str,
        *,
        context: Dict[str, Any],
    ) -> None:
        payload = {
            "TXT_REDACTED": build_company_key(company_info),
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": company_info,
            "TXT_REDACTED": context,
            "TXT_REDACTED": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _json_dump(self.meta_path(company_info, year), payload)

    def load_company_context(self, company_info: Dict[str, Any], year: str) -> Dict[str, Any]:
        return _json_load(self.meta_path(company_info, year), {})

    def list_asset_records(self, company_info: Dict[str, Any], year: str) -> List[Dict[str, Any]]:
        session_key = (build_company_key(company_info), str(year))
        with self._lock:
            if session_key in self._asset_sessions:
                return list(self._asset_sessions[session_key]["TXT_REDACTED"])
        return _json_load(self.asset_index_path(company_info, year), [])

    def begin_asset_session(self, company_info: Dict[str, Any], year: str) -> None:
        session_key = (build_company_key(company_info), str(year))
        with self._lock:
            if session_key in self._asset_sessions:
                return
            self._asset_sessions[session_key] = {
                "TXT_REDACTED": _json_load(self.asset_index_path(company_info, year), []),
                "TXT_REDACTED": False,
            }

    def flush_asset_session(self, company_info: Dict[str, Any], year: str) -> None:
        session_key = (build_company_key(company_info), str(year))
        with self._lock:
            session = self._asset_sessions.get(session_key)
            if not session or not session.get("TXT_REDACTED"):
                return
            records = list(session.get("TXT_REDACTED") or [])
            _json_dump(self.asset_index_path(company_info, year), records)
            session["TXT_REDACTED"] = False

    def end_asset_session(self, company_info: Dict[str, Any], year: str) -> None:
        session_key = (build_company_key(company_info), str(year))
        self.flush_asset_session(company_info, year)
        with self._lock:
            self._asset_sessions.pop(session_key, None)

    def save_asset(
        self,
        company_info: Dict[str, Any],
        year: str,
        *,
        source_system: str,
        asset_type: str,
        logical_name: str,
        source_url: str,
        method: str,
        mime_type: str,
        content: bytes,
        request_fingerprint: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        session_key = (build_company_key(company_info), str(year))
        with self._lock:
            session = self._asset_sessions.get(session_key)
            if session is not None:
                asset_records = session["TXT_REDACTED"]
            else:
                asset_records = _json_load(self.asset_index_path(company_info, year), [])
        for record in asset_records:
            if request_fingerprint and record.get("TXT_REDACTED") == request_fingerprint:
                return record

        content_hash = hashlib.sha256(content).hexdigest()
        asset_id = "TXT_REDACTED"                          
        extension = _guess_extension(mime_type, source_url, logical_name)
        company_dir = self.company_year_dir(company_info, year)
        relative_path = os.path.join("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"                      )
        abs_path = os.path.join(company_dir, relative_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        with open(abs_path, "TXT_REDACTED") as file:
            file.write(content)

        record = {
            "TXT_REDACTED": asset_id,
            "TXT_REDACTED": build_company_key(company_info),
            "TXT_REDACTED": company_info.get("TXT_REDACTED") or company_info.get("TXT_REDACTED") or "TXT_REDACTED",
            "TXT_REDACTED": company_info.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": company_info.get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": source_system,
            "TXT_REDACTED": asset_type,
            "TXT_REDACTED": logical_name,
            "TXT_REDACTED": source_url,
            "TXT_REDACTED": method,
            "TXT_REDACTED": mime_type,
            "TXT_REDACTED": request_fingerprint,
            "TXT_REDACTED": content_hash,
            "TXT_REDACTED": len(content),
            "TXT_REDACTED": relative_path,
            "TXT_REDACTED": metadata or {},
            "TXT_REDACTED": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        asset_records.append(record)
        with self._lock:
            session = self._asset_sessions.get(session_key)
            if session is not None:
                session["TXT_REDACTED"] = True
            else:
                _json_dump(self.asset_index_path(company_info, year), asset_records)
        return record

    def save_section_rows(
        self,
        company_info: Dict[str, Any],
        year: str,
        section_rows: Dict[int, Dict[str, Any]],
    ) -> None:
        payload = {
            "TXT_REDACTED": build_company_key(company_info),
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": {str(section_num): row for section_num, row in section_rows.items()},
            "TXT_REDACTED": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _json_dump(self.section_rows_path(company_info, year), payload)

    def load_section_rows(self, company_info: Dict[str, Any], year: str) -> Dict[int, Dict[str, Any]]:
        payload = _json_load(self.section_rows_path(company_info, year), {})
        sections = payload.get("TXT_REDACTED", {}) or {}
        result: Dict[int, Dict[str, Any]] = {}
        for section_num, row in sections.items():
            try:
                result[int(section_num)] = row
            except (TypeError, ValueError):
                continue
        return result

    def save_collector_rows(
        self,
        company_info: Dict[str, Any],
        year: str,
        section_rows: Dict[int, Dict[str, Any]],
    ) -> None:
        payload = {
            "TXT_REDACTED": build_company_key(company_info),
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": {str(section_num): row for section_num, row in section_rows.items()},
            "TXT_REDACTED": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _json_dump(self.collector_rows_path(company_info, year), payload)

    def save_harness_input_rows(
        self,
        company_info: Dict[str, Any],
        year: str,
        section_rows: Dict[int, Dict[str, Any]],
        *,
        schema_version: int = 2,
    ) -> None:
        payload = {
            "TXT_REDACTED": build_company_key(company_info),
            "TXT_REDACTED": str(year),
            "TXT_REDACTED": int(schema_version),
            "TXT_REDACTED": {str(section_num): row for section_num, row in section_rows.items()},
            "TXT_REDACTED": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _json_dump(self.harness_input_rows_path(company_info, year), payload)

    def load_harness_input_rows(self, company_info: Dict[str, Any], year: str) -> Dict[int, Dict[str, Any]]:
        harness_path = self.harness_input_rows_path(company_info, year)
        if os.path.exists(harness_path):
            payload = _json_load(harness_path, {})
            sections = payload.get("TXT_REDACTED", {}) or {}
            result: Dict[int, Dict[str, Any]] = {}
            for section_num, row in sections.items():
                try:
                    result[int(section_num)] = row
                except (TypeError, ValueError):
                    continue
            return result
        return self.load_collector_rows(company_info, year)

    def load_collector_rows(self, company_info: Dict[str, Any], year: str) -> Dict[int, Dict[str, Any]]:
        path = self.collector_rows_path(company_info, year)
        if not os.path.exists(path):
            return self.load_section_rows(company_info, year)
        payload = _json_load(path, {})
        sections = payload.get("TXT_REDACTED", {}) or {}
        result: Dict[int, Dict[str, Any]] = {}
        for section_num, row in sections.items():
            try:
                result[int(section_num)] = row
            except (TypeError, ValueError):
                continue
        return result

    def save_facts(self, company_info: Dict[str, Any], year: str, facts: List[Dict[str, Any]]) -> None:
        _json_dump(self.facts_path(company_info, year), facts)

    def load_facts(self, company_info: Dict[str, Any], year: str) -> List[Dict[str, Any]]:
        return _json_load(self.facts_path(company_info, year), [])

    def save_records(self, company_info: Dict[str, Any], year: str, records: List[Dict[str, Any]]) -> None:
        _json_dump(self.records_path(company_info, year), records)

    def load_records(self, company_info: Dict[str, Any], year: str) -> List[Dict[str, Any]]:
        return _json_load(self.records_path(company_info, year), [])

    def save_metrics(self, company_info: Dict[str, Any], year: str, metrics: List[Dict[str, Any]]) -> None:
        _json_dump(self.metrics_path(company_info, year), metrics)

    def load_metrics(self, company_info: Dict[str, Any], year: str) -> List[Dict[str, Any]]:
        return _json_load(self.metrics_path(company_info, year), [])

    def save_summary(self, company_info: Dict[str, Any], year: str, summary: Dict[str, Any]) -> None:
        _json_dump(self.summary_path(company_info, year), summary)

    def load_summary(self, company_info: Dict[str, Any], year: str) -> Dict[str, Any]:
        return _json_load(self.summary_path(company_info, year), {})

    def save_scorecard(self, company_info: Dict[str, Any], year: str, scorecard: Dict[str, Any]) -> None:
        _json_dump(self.scorecard_path(company_info, year), scorecard)

    def load_scorecard(self, company_info: Dict[str, Any], year: str) -> Dict[str, Any]:
        return _json_load(self.scorecard_path(company_info, year), {})

    def save_manifest(self, company_info: Dict[str, Any], year: str, manifest: Dict[str, Any]) -> None:
        _json_dump(self.manifest_path(company_info, year), manifest)

    def load_manifest(self, company_info: Dict[str, Any], year: str) -> Dict[str, Any]:
        return _json_load(self.manifest_path(company_info, year), {})

    def save_bundle_summary_markdown(self, company_info: Dict[str, Any], year: str, text: str) -> None:
        _text_dump(self.bundle_summary_markdown_path(company_info, year), text)

    def load_bundle_summary_markdown(self, company_info: Dict[str, Any], year: str) -> str:
        return _text_load(self.bundle_summary_markdown_path(company_info, year), "TXT_REDACTED")

    def list_company_dirs(self, year: str) -> List[str]:
        year_dir = os.path.join(self.root_dir, str(year))
        if not os.path.exists(year_dir):
            return []
        return sorted(
            os.path.join(year_dir, name)
            for name in os.listdir(year_dir)
            if os.path.isdir(os.path.join(year_dir, name))
        )

    def find_company_bundle_by_key(self, year: str, company_key: str) -> Optional[str]:
        path = os.path.join(self.root_dir, str(year), build_company_key({"TXT_REDACTED": company_key, "TXT_REDACTED": company_key, "TXT_REDACTED": company_key}))
        if os.path.isdir(path):
            return path
        year_dir = os.path.join(self.root_dir, str(year))
        if not os.path.exists(year_dir):
            return None
        normalized = _safe_component(company_key)
        for name in os.listdir(year_dir):
            if _safe_component(name) == normalized:
                return os.path.join(year_dir, name)
        return None

    def load_bundle_by_path(self, bundle_dir: str) -> Dict[str, Any]:
        meta = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        rows = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        harness_input_rows = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        collector_rows = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        records = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), [])
        facts = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), [])
        metrics = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), [])
        summary = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        scorecard = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        manifest = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), {})
        bundle_summary_markdown = _text_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), "TXT_REDACTED")
        assets = _json_load(os.path.join(bundle_dir, "TXT_REDACTED", "TXT_REDACTED"), [])
        return {
            "TXT_REDACTED": meta,
            "TXT_REDACTED": rows,
            "TXT_REDACTED": harness_input_rows,
            "TXT_REDACTED": collector_rows,
            "TXT_REDACTED": records,
            "TXT_REDACTED": facts,
            "TXT_REDACTED": metrics,
            "TXT_REDACTED": summary,
            "TXT_REDACTED": scorecard,
            "TXT_REDACTED": manifest,
            "TXT_REDACTED": bundle_summary_markdown,
            "TXT_REDACTED": assets,
        }

    def iter_bundles(self, year: str) -> Iterable[Dict[str, Any]]:
        for bundle_dir in self.list_company_dirs(year):
            yield self.load_bundle_by_path(bundle_dir)
