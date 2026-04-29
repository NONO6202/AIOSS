# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


ROUTE_VALUES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

STEP_KIND_VALUES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
)

NORMALIZED_QUERY_SORT_VALUES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

NORMALIZED_QUERY_CHART_TYPE_VALUES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
    "TXT_REDACTED",
)

DERIVATION_RULE_OP_VALUES = (
    "TXT_REDACTED",
    "TXT_REDACTED",
)


class AgentValidationError(ValueError):
    "TXT_REDACTED"


def normalize_question(text: str) -> str:
    "TXT_REDACTED"
    normalized = re.sub("TXT_REDACTED", "TXT_REDACTED", str(text or "TXT_REDACTED")).strip().lower()
    return normalized


@dataclass
class NormalizedQuery:
    intent: str
    target_entity: str
    concept: str
    years: List[str]
    company_name: str
    sort_direction: str
    limit: int
    candidate_terms: List[str]
    candidate_paths: List[str]
    requested_outputs: List[str]
    wants_chart: bool
    chart_type: str
    needs_evidence: bool
    rationale: str = "TXT_REDACTED"

    @classmethod
    def response_schema(cls) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(NORMALIZED_QUERY_SORT_VALUES)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": 2, "TXT_REDACTED": 3},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}, "TXT_REDACTED": 4},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}, "TXT_REDACTED": 1},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}, "TXT_REDACTED": 2},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(NORMALIZED_QUERY_CHART_TYPE_VALUES)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
            },
            "TXT_REDACTED": [
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
                "TXT_REDACTED",
            ],
            "TXT_REDACTED": False,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TXT_REDACTED":
        sort_direction = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip().lower()
        if sort_direction not in NORMALIZED_QUERY_SORT_VALUES:
            raise AgentValidationError("TXT_REDACTED"                                                          )
        try:
            limit = int(payload.get("TXT_REDACTED", 3) or 4)
        except (TypeError, ValueError) as exc:
            raise AgentValidationError("TXT_REDACTED") from exc
        limit = max(1, min(2, limit))
        chart_type = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip().lower()
        if chart_type not in NORMALIZED_QUERY_CHART_TYPE_VALUES:
            raise AgentValidationError("TXT_REDACTED"                                                  )
        return cls(
            intent=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            target_entity=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            concept=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            years=[str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()],
            company_name=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            sort_direction=sort_direction,
            limit=limit,
            candidate_terms=[str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()][:3],
            candidate_paths=[str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()][:4],
            requested_outputs=[str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()][:1],
            wants_chart=bool(payload.get("TXT_REDACTED")),
            chart_type=chart_type,
            needs_evidence=bool(payload.get("TXT_REDACTED")),
            rationale=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RouteDecision:
    route: str
    complexity: int
    confidence: float
    needs_retrieval: bool
    needs_analytics: bool
    needs_action: bool
    rationale: str = "TXT_REDACTED"

    @classmethod
    def response_schema(cls) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": list(ROUTE_VALUES),
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": 2,
                    "TXT_REDACTED": 3,
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": 4,
                    "TXT_REDACTED": 1,
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
            },
            "TXT_REDACTED": [
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ],
            "TXT_REDACTED": False,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TXT_REDACTED":
        route = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if route not in ROUTE_VALUES:
            raise AgentValidationError("TXT_REDACTED"                       )

        try:
            complexity = int(payload.get("TXT_REDACTED"))
        except (TypeError, ValueError) as exc:
            raise AgentValidationError("TXT_REDACTED") from exc
        if not 2 <= complexity <= 3:
            raise AgentValidationError("TXT_REDACTED")

        try:
            confidence = float(payload.get("TXT_REDACTED"))
        except (TypeError, ValueError) as exc:
            raise AgentValidationError("TXT_REDACTED") from exc
        confidence = max(4, min(1, confidence))

        return cls(
            route=route,
            complexity=complexity,
            confidence=confidence,
            needs_retrieval=bool(payload.get("TXT_REDACTED")),
            needs_analytics=bool(payload.get("TXT_REDACTED")),
            needs_action=bool(payload.get("TXT_REDACTED")),
            rationale=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DerivationRuleUpdate:
    should_update: bool
    label: str
    aliases: List[str]
    formula_op: str
    operands: List[str]
    unit: str
    rationale: str = "TXT_REDACTED"

    @classmethod
    def response_schema(cls) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}, "TXT_REDACTED": 2},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": list(DERIVATION_RULE_OP_VALUES)},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"}, "TXT_REDACTED": 3},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
            },
            "TXT_REDACTED": [
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ],
            "TXT_REDACTED": False,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TXT_REDACTED":
        formula_op = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip().lower()
        if formula_op not in DERIVATION_RULE_OP_VALUES:
            raise AgentValidationError("TXT_REDACTED"                                            )
        operands = [str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()][:4]
        return cls(
            should_update=bool(payload.get("TXT_REDACTED")),
            label=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            aliases=[str(item).strip() for item in (payload.get("TXT_REDACTED") or []) if str(item).strip()][:1],
            formula_op=formula_op,
            operands=operands,
            unit=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            rationale=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AgentBudget:
    max_depth: int
    max_steps: int
    max_tool_calls: int
    max_wall_time_sec: int
    token_soft_limit: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AgentStep:
    kind: str
    reasoning_summary: str
    tool_name: str = "TXT_REDACTED"
    tool_input_json: str = "TXT_REDACTED"
    evidence: List[str] = field(default_factory=list)
    final_answer_markdown: str = "TXT_REDACTED"
    chart_plan_json: str = "TXT_REDACTED"
    should_continue: bool = False

    @classmethod
    def response_schema(cls, *, tool_names: List[str]) -> Dict[str, Any]:
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": {
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": list(STEP_KIND_VALUES),
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": ["TXT_REDACTED"] + list(tool_names),
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                },
                "TXT_REDACTED": {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                },
            },
            "TXT_REDACTED": [
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
                "TXT_REDACTED",
            ],
            "TXT_REDACTED": False,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any], *, tool_names: List[str]) -> "TXT_REDACTED":
        kind = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if kind not in STEP_KIND_VALUES:
            raise AgentValidationError("TXT_REDACTED"                          )

        tool_name = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if kind == "TXT_REDACTED" and tool_name not in tool_names:
            raise AgentValidationError("TXT_REDACTED"                          )
        if kind == "TXT_REDACTED":
            tool_name = "TXT_REDACTED"

        tool_input_json = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        if kind == "TXT_REDACTED":
            try:
                parsed = json.loads(tool_input_json)
            except json.JSONDecodeError as exc:
                raise AgentValidationError("TXT_REDACTED") from exc
            if not isinstance(parsed, dict):
                raise AgentValidationError("TXT_REDACTED")

        chart_plan_json = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        try:
            parsed_chart_plan = json.loads(chart_plan_json)
        except json.JSONDecodeError as exc:
            raise AgentValidationError("TXT_REDACTED") from exc
        if not isinstance(parsed_chart_plan, dict):
            raise AgentValidationError("TXT_REDACTED")
        if kind == "TXT_REDACTED":
            chart_plan_json = "TXT_REDACTED"

        return cls(
            kind=kind,
            reasoning_summary=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            tool_name=tool_name,
            tool_input_json=tool_input_json,
            evidence=[str(item) for item in (payload.get("TXT_REDACTED") or [])][:2],
            final_answer_markdown=str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip(),
            chart_plan_json=chart_plan_json,
            should_continue=bool(payload.get("TXT_REDACTED")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: Dict[str, Any]

    def to_prompt_block(self) -> str:
        return (
            "TXT_REDACTED"                                    
            "TXT_REDACTED"                                                                                   
        )
