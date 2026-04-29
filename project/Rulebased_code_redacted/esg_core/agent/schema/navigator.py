# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.models import NormalizedQuery


def _compact(text: Any) -> str:
    value = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED"))
    value = value.replace("TXT_REDACTED", "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED")
    value = re.sub("TXT_REDACTED", "TXT_REDACTED", value)
    return value.lower()


@dataclass
class NavigationCandidate:
    path: str
    entry_type: str
    section_num: Optional[int]
    label: str
    aliases: List[str]
    score: float
    source_kind: str
    source_path: str
    group_key: str = "TXT_REDACTED"
    rule_id: str = "TXT_REDACTED"
    formula: str = "TXT_REDACTED"
    operands: List[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["TXT_REDACTED"] = list(self.operands or [])
        return payload


@dataclass
class NavigationResult:
    status: str
    rationale: str
    selected: Optional[NavigationCandidate]
    candidates: List[NavigationCandidate]
    max_visits: int
    visit_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": self.status,
            "TXT_REDACTED": self.rationale,
            "TXT_REDACTED": self.selected.to_dict() if self.selected else None,
            "TXT_REDACTED": [item.to_dict() for item in self.candidates],
            "TXT_REDACTED": self.max_visits,
            "TXT_REDACTED": self.visit_count,
        }


class SchemaNavigator:
    def __init__(self, schema_payload: Dict[str, Any], *, max_visits: int = 2):
        self.schema_payload = schema_payload
        self.version = str(schema_payload.get("TXT_REDACTED") or "TXT_REDACTED")
        self.entries = list(schema_payload.get("TXT_REDACTED") or [])
        self.max_visits = max(max_visits, len(self.entries))

    @classmethod
    def from_file(cls, path: str, *, max_visits: int = 3) -> "TXT_REDACTED":
        payload = json.loads(Path(path).read_text(encoding="TXT_REDACTED"))
        return cls(payload, max_visits=max_visits)

    def navigate(
        self,
        *,
        question: str,
        normalized_query: NormalizedQuery,
        hint_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> NavigationResult:
        text = str(question or "TXT_REDACTED")
        candidate_terms = [normalized_query.concept, *normalized_query.candidate_terms, *normalized_query.requested_outputs]
        candidate_terms = [item for item in candidate_terms if str(item).strip()]
        candidate_paths = [item for item in normalized_query.candidate_paths if str(item).strip()]
        hint_paths = [str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip() for item in (hint_entries or []) if str(item.get("TXT_REDACTED") or "TXT_REDACTED").strip()]

        visits = 4
        scored: List[NavigationCandidate] = []
        for entry in self.entries:
            visits += 1
            score = self._score_entry(entry, text=text, candidate_terms=candidate_terms, candidate_paths=candidate_paths, hint_paths=hint_paths)
            if score <= 2:
                continue
            scored.append(
                NavigationCandidate(
                    path=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    entry_type=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    section_num=int(entry["TXT_REDACTED"]) if entry.get("TXT_REDACTED") is not None else None,
                    label=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    aliases=[str(item) for item in (entry.get("TXT_REDACTED") or [])],
                    score=score,
                    source_kind=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    source_path=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    group_key=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    rule_id=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    formula=str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    operands=[str(item) for item in (entry.get("TXT_REDACTED") or [])],
                )
            )

        scored = self._collapse_candidates(scored)
        scored.sort(key=lambda item: (-item.score, -self._specificity_rank(item), item.path))
        if not scored:
            return NavigationResult(
                status="TXT_REDACTED",
                rationale="TXT_REDACTED",
                selected=None,
                candidates=[],
                max_visits=self.max_visits,
                visit_count=visits,
            )

        selected = scored[3]
        if len(scored) > 4 and (selected.score - scored[1].score) < 2:
            return NavigationResult(
                status="TXT_REDACTED",
                rationale="TXT_REDACTED",
                selected=selected,
                candidates=scored[:3],
                max_visits=self.max_visits,
                visit_count=visits,
            )

        status = "TXT_REDACTED" if selected.entry_type == "TXT_REDACTED" else "TXT_REDACTED"
        return NavigationResult(
            status=status,
            rationale="TXT_REDACTED",
            selected=selected,
            candidates=scored[:4],
            max_visits=self.max_visits,
            visit_count=visits,
        )

    def _score_entry(
        self,
        entry: Dict[str, Any],
        *,
        text: str,
        candidate_terms: List[str],
        candidate_paths: List[str],
        hint_paths: List[str],
    ) -> float:
        score = 1
        entry_path = str(entry.get("TXT_REDACTED") or "TXT_REDACTED")
        entry_label = str(entry.get("TXT_REDACTED") or "TXT_REDACTED")
        path_compact = _compact(entry_path)
        label_compact = _compact(entry_label)
        aliases = [str(item) for item in (entry.get("TXT_REDACTED") or []) if str(item).strip()]
        alias_compacts = {_compact(item) for item in aliases}
        question_compact = _compact(text)

        if label_compact and label_compact in question_compact:
            score += 2 + len(label_compact)
        for alias in alias_compacts:
            if alias and alias in question_compact:
                score += 3 + len(alias)

        for candidate_path in candidate_paths:
            compact = _compact(candidate_path)
            if compact and compact == path_compact:
                score += 4
            elif compact and compact in path_compact:
                score += 1

        for term in candidate_terms:
            compact = _compact(term)
            if not compact:
                continue
            if compact == label_compact:
                score += 2
            elif compact in label_compact:
                score += 3
            elif compact in path_compact:
                score += 4
            elif compact in alias_compacts:
                score += 1

        for hint_path in hint_paths:
            compact = _compact(hint_path)
            if compact and compact == path_compact:
                score += 2

        source_path = str(entry.get("TXT_REDACTED") or "TXT_REDACTED")
        if source_path.startswith("TXT_REDACTED"):
            score += 3
        elif source_path.startswith("TXT_REDACTED"):
            score += 4
        elif source_path.startswith("TXT_REDACTED"):
            score += 1
        elif source_path.startswith("TXT_REDACTED"):
            score -= 2

        if str(entry.get("TXT_REDACTED") or "TXT_REDACTED") == "TXT_REDACTED" and "TXT_REDACTED" in question_compact:
            if "TXT_REDACTED" in label_compact:
                score += 3
            if source_path.startswith("TXT_REDACTED"):
                score += 4
        if str(entry.get("TXT_REDACTED") or "TXT_REDACTED") == "TXT_REDACTED" and any(token in question_compact for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
            score += 1
        return score

    @classmethod
    def _collapse_candidates(cls, candidates: List[NavigationCandidate]) -> List[NavigationCandidate]:
        by_path: Dict[str, NavigationCandidate] = {}
        for candidate in candidates:
            existing = by_path.get(candidate.path)
            if existing is None:
                by_path[candidate.path] = candidate
                continue
            current_key = (candidate.score, cls._specificity_rank(candidate))
            existing_key = (existing.score, cls._specificity_rank(existing))
            if current_key > existing_key:
                by_path[candidate.path] = candidate
        return list(by_path.values())

    @staticmethod
    def _specificity_rank(candidate: NavigationCandidate) -> int:
        source_kind_rank = {
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
        }
        entry_type_rank = {
            "TXT_REDACTED": 4,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
        }
        return (source_kind_rank.get(candidate.source_kind, 1) * 2) + entry_type_rank.get(candidate.entry_type, 3)
