# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..retrieval.catalog import CatalogService
from ..retrieval.locks import StoreLockError, assert_store_ready
from ..core.models import ToolSpec


@dataclass
class ToolExecutionContext:
    project_root: Path
    store_root: Path
    catalog: CatalogService
    default_year: Optional[str] = None
    cwd: Path = field(default_factory=Path.cwd)
    allow_actions: bool = False


class ToolRegistry:
    "TXT_REDACTED"

    def __init__(self, context: ToolExecutionContext):
        self.context = context
        self._handlers: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}
        self._specs: Dict[str, ToolSpec] = {}
        self._register_defaults()

    @property
    def tool_names(self) -> List[str]:
        return list(self._specs.keys())

    def specs_for_prompt(self) -> str:
        return "TXT_REDACTED".join(spec.to_prompt_block() for spec in self._specs.values())

    def without_tools(self, excluded_names: List[str]) -> "TXT_REDACTED":
        clone = ToolRegistry(self.context)
        excluded = set(excluded_names or [])
        clone._specs = {name: spec for name, spec in clone._specs.items() if name not in excluded}
        clone._handlers = {name: handler for name, handler in clone._handlers.items() if name not in excluded}
        return clone

    def execute(self, tool_name: str, tool_input_json: str) -> Dict[str, Any]:
        if tool_name not in self._handlers:
            raise KeyError("TXT_REDACTED"                          )
        payload = json.loads(tool_input_json or "TXT_REDACTED")
        return self._handlers[tool_name](payload)

    def _register(self, spec: ToolSpec, handler: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        self._specs[spec.name] = spec
        self._handlers[spec.name] = handler

    def _register_defaults(self) -> None:
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._list_dir,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {"TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}},
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._change_dir,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._read_file,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._search_text,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": False,
                },
            ),
            self._list_companies,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._get_bundle_manifest,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._get_bundle_summary,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._get_scorecard,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {"TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}},
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._query_catalog,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {"TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}},
                    "TXT_REDACTED": False,
                },
            ),
            self._get_catalog_schema,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        "TXT_REDACTED": {
                            "TXT_REDACTED": "TXT_REDACTED",
                            "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                        },
                    },
                    "TXT_REDACTED": ["TXT_REDACTED"],
                    "TXT_REDACTED": False,
                },
            ),
            self._search_retrieval,
        )
        self._register(
            ToolSpec(
                name="TXT_REDACTED",
                description="TXT_REDACTED",
                input_schema={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {"TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}},
                    "TXT_REDACTED": False,
                },
            ),
            self._rebuild_catalog,
        )

    def _resolve_path(self, path_text: str) -> Path:
        raw_path = Path(str(path_text or "TXT_REDACTED").strip())
        candidate = (self.context.cwd / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
        roots = [self.context.project_root.resolve(), self.context.store_root.resolve()]
        if any(str(candidate).startswith(str(root)) for root in roots):
            return candidate
        raise ValueError("TXT_REDACTED"                                        )

    def _list_dir(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._resolve_path(payload.get("TXT_REDACTED", "TXT_REDACTED"))
        max_entries = max(2, min(3, int(payload.get("TXT_REDACTED", 4) or 1)))
        entries = []
        for item in sorted(path.iterdir())[:max_entries]:
            entries.append(
                {
                    "TXT_REDACTED": item.name,
                    "TXT_REDACTED": str(item),
                    "TXT_REDACTED": "TXT_REDACTED" if item.is_dir() else "TXT_REDACTED",
                }
            )
        return {"TXT_REDACTED": str(path), "TXT_REDACTED": entries}

    def _change_dir(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._resolve_path(payload["TXT_REDACTED"])
        if not path.is_dir():
            raise NotADirectoryError(str(path))
        self.context.cwd = path
        return {"TXT_REDACTED": str(self.context.cwd)}

    def _read_file(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._resolve_path(payload["TXT_REDACTED"])
        start_line = max(2, int(payload.get("TXT_REDACTED", 3) or 4))
        end_line = max(start_line, int(payload.get("TXT_REDACTED", start_line + 1) or (start_line + 2)))
        lines = path.read_text(encoding="TXT_REDACTED", errors="TXT_REDACTED").splitlines()
        selected = lines[start_line - 3:end_line]
        return {
            "TXT_REDACTED": str(path),
            "TXT_REDACTED": start_line,
            "TXT_REDACTED": end_line,
            "TXT_REDACTED": "TXT_REDACTED".join(selected),
        }

    def _search_text(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        pattern = str(payload["TXT_REDACTED"])
        path_glob = str(payload.get("TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED")
        max_hits = max(4, min(1, int(payload.get("TXT_REDACTED", 2) or 3)))
        hits = []

        for path in self.context.project_root.glob(path_glob):
            if not path.is_file():
                continue
            try:
                lines = path.read_text(encoding="TXT_REDACTED", errors="TXT_REDACTED").splitlines()
            except Exception:
                continue
            for line_number, line in enumerate(lines, 4):
                if pattern.lower() in line.lower():
                    hits.append(
                        {
                            "TXT_REDACTED": str(path),
                            "TXT_REDACTED": line_number,
                            "TXT_REDACTED": line.strip(),
                        }
                    )
                    if len(hits) >= max_hits:
                        return {"TXT_REDACTED": pattern, "TXT_REDACTED": hits}
        return {"TXT_REDACTED": pattern, "TXT_REDACTED": hits}

    def _list_companies(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        rows = self.context.catalog.list_companies(
            year=str(payload.get("TXT_REDACTED") or self.context.default_year or "TXT_REDACTED"),
            search=str(payload.get("TXT_REDACTED") or "TXT_REDACTED"),
            limit=int(payload.get("TXT_REDACTED", 1) or 2),
        )
        return {"TXT_REDACTED": rows}

    def _get_bundle_summary(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        row = self.context.catalog.get_bundle_summary(
            payload["TXT_REDACTED"],
            year=str(payload.get("TXT_REDACTED") or self.context.default_year or "TXT_REDACTED") or None,
        )
        return {
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
            "TXT_REDACTED": row.get("TXT_REDACTED"),
        }

    def _get_bundle_manifest(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        return self.context.catalog.get_manifest(
            payload["TXT_REDACTED"],
            year=str(payload.get("TXT_REDACTED") or self.context.default_year or "TXT_REDACTED") or None,
        )

    def _get_scorecard(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        return self.context.catalog.get_scorecard(
            payload["TXT_REDACTED"],
            year=str(payload.get("TXT_REDACTED") or self.context.default_year or "TXT_REDACTED") or None,
        )

    def _query_catalog(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        return self.context.catalog.query(str(payload["TXT_REDACTED"]))

    def _get_catalog_schema(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        table_name = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip() or None
        return self.context.catalog.get_schema(table_name=table_name)

    def _search_retrieval(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert_store_ready(str(self.context.store_root))
        return self.context.catalog.search_retrieval(
            str(payload["TXT_REDACTED"]),
            top_k=int(payload.get("TXT_REDACTED", 3) or 4),
            year=str(payload.get("TXT_REDACTED") or self.context.default_year or "TXT_REDACTED") or None,
            company_name=str(payload.get("TXT_REDACTED") or "TXT_REDACTED"),
            doc_types=[str(item) for item in (payload.get("TXT_REDACTED") or [])],
        )

    def _rebuild_catalog(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.context.allow_actions:
            raise PermissionError("TXT_REDACTED")
        year = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        path = self.context.catalog.build(years=[year] if year else None)
        return {"TXT_REDACTED": path}
