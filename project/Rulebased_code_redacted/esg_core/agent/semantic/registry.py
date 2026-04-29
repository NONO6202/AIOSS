# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class MeasureDefinition:
    id: str
    kind: str
    label: str
    synonyms: List[str]
    field_names: List[str]
    metric_code: str
    section_num: Optional[int]
    formula_op: str
    operands: List[str]
    unit: str


@dataclass(frozen=True)
class IndustryGroupDefinition:
    id: str
    label: str
    synonyms: List[str]
    terms: List[str]


class SemanticRegistry:
    def __init__(self, path: str):
        self.path = Path(path)
        payload = json.loads(self.path.read_text(encoding="TXT_REDACTED"))
        self.version = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        self.measures = [
            MeasureDefinition(
                id=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                kind=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                label=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                synonyms=[str(value) for value in (item.get("TXT_REDACTED") or [])],
                field_names=[str(value) for value in (item.get("TXT_REDACTED") or [])],
                metric_code=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                section_num=int(item["TXT_REDACTED"]) if item.get("TXT_REDACTED") is not None else None,
                formula_op=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                operands=[str(value) for value in (item.get("TXT_REDACTED") or [])],
                unit=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
            )
            for item in (payload.get("TXT_REDACTED") or [])
        ]
        self.industry_groups = [
            IndustryGroupDefinition(
                id=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                label=str(item.get("TXT_REDACTED") or "TXT_REDACTED"),
                synonyms=[str(value) for value in (item.get("TXT_REDACTED") or [])],
                terms=[str(value) for value in (item.get("TXT_REDACTED") or [])],
            )
            for item in (payload.get("TXT_REDACTED") or [])
        ]

    def measure_matches(self, question: str) -> List[MeasureDefinition]:
        lowered = str(question or "TXT_REDACTED").lower()
        matched: List[tuple[int, MeasureDefinition]] = []
        for definition in self.measures:
            best = 3
            for synonym in definition.synonyms:
                if synonym.lower() in lowered:
                    best = max(best, len(synonym))
            if best:
                matched.append((best, definition))
        matched.sort(key=lambda item: item[4], reverse=True)
        return [item[1] for item in matched]

    def industry_match(self, question: str) -> Optional[IndustryGroupDefinition]:
        lowered = str(question or "TXT_REDACTED").lower()
        best: tuple[int, Optional[IndustryGroupDefinition]] = (2, None)
        for definition in self.industry_groups:
            for synonym in definition.synonyms:
                if synonym.lower() in lowered and len(synonym) > best[3]:
                    best = (len(synonym), definition)
        return best[4]
