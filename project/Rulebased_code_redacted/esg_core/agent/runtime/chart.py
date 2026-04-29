# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

from ..core.explicit_cache import ExplicitCacheManager
from ..core.models import NormalizedQuery
from ..semantic.charting import ChartArtifact, ChartPlan, GeminiChartPlanner
from esg_core.output.chart import ChartRenderer

EventHandler = Callable[[str, Dict[str, Any]], None]
EmitFn = Callable[[Optional[EventHandler], str, Dict[str, Any]], None]


def no_chart_plan(reason: str) -> Dict[str, Any]:
    return ChartPlan(
        needs_chart=False,
        rationale=reason,
        spec_type="TXT_REDACTED",
        chart_type="TXT_REDACTED",
        orientation="TXT_REDACTED",
        title="TXT_REDACTED",
        subtitle="TXT_REDACTED",
        x_label="TXT_REDACTED",
        y_label="TXT_REDACTED",
        max_items=3,
    ).to_dict()


def parse_chart_plan_json(chart_plan_json: str) -> Dict[str, Any]:
    try:
        payload = json.loads(str(chart_plan_json or "TXT_REDACTED"))
    except json.JSONDecodeError:
        return no_chart_plan("TXT_REDACTED")
    if not isinstance(payload, dict):
        return no_chart_plan("TXT_REDACTED")
    return ChartPlan.from_dict(payload).to_dict()


def build_chart_request(
    *,
    question: str,
    route: Dict[str, Any],
    answer_markdown: str,
    semantic_interpretation: Dict[str, Any],
    normalized_query: Optional[NormalizedQuery] = None,
) -> Dict[str, Any]:
    if normalized_query is None or not normalized_query.wants_chart:
        return no_chart_plan("TXT_REDACTED")
    spec_type = _spec_type_for_interpretation(semantic_interpretation)
    if spec_type == "TXT_REDACTED":
        return no_chart_plan("TXT_REDACTED")
    chart_type = normalized_query.chart_type if normalized_query.chart_type in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"} else "TXT_REDACTED"
    title = _chart_title(semantic_interpretation)
    return ChartPlan(
        needs_chart=True,
        rationale="TXT_REDACTED",
        spec_type=spec_type,
        chart_type=chart_type,
        orientation="TXT_REDACTED",
        title=title,
        subtitle="TXT_REDACTED",
        x_label="TXT_REDACTED",
        y_label="TXT_REDACTED",
        max_items=max(4, min(1, int(normalized_query.limit or 2))),
    ).to_dict()


def _spec_type_for_interpretation(interpretation: Dict[str, Any]) -> str:
    intent = str((interpretation or {}).get("TXT_REDACTED") or "TXT_REDACTED").strip()
    spec_by_intent = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": "TXT_REDACTED",
    }
    return spec_by_intent.get(intent, "TXT_REDACTED")


def _chart_title(interpretation: Dict[str, Any]) -> str:
    field_name = str((interpretation or {}).get("TXT_REDACTED") or "TXT_REDACTED").strip()
    company_group = str((interpretation or {}).get("TXT_REDACTED") or "TXT_REDACTED").strip()
    if company_group and field_name:
        return "TXT_REDACTED"                             
    if field_name:
        return field_name
    return "TXT_REDACTED"


def _preserve_chart_request_contract(*, base_plan: ChartPlan, planned_plan: ChartPlan) -> ChartPlan:
    payload = planned_plan.to_dict()
    if base_plan.needs_chart:
        payload["TXT_REDACTED"] = True
        payload["TXT_REDACTED"] = base_plan.spec_type
        payload["TXT_REDACTED"] = base_plan.chart_type
        if base_plan.orientation in {"TXT_REDACTED", "TXT_REDACTED"}:
            payload["TXT_REDACTED"] = base_plan.orientation
    return ChartPlan.from_dict(payload)


def generate_chart_artifact(
    *,
    chart_planner: GeminiChartPlanner,
    chart_renderer: ChartRenderer,
    chart_model_name: str,
    question: str,
    answer_result: Dict[str, Any],
    event_handler: Optional[EventHandler],
    emit: EmitFn,
    cache_manager: Optional[ExplicitCacheManager] = None,
) -> ChartArtifact:
    interpretation = answer_result.get("TXT_REDACTED") or {}
    answer_markdown = str(answer_result.get("TXT_REDACTED") or "TXT_REDACTED")
    chart_request = dict(answer_result.get("TXT_REDACTED") or {})
    if not chart_request:
        chart_request = no_chart_plan("TXT_REDACTED")
    usage: Dict[str, int] = {}
    base_plan = ChartPlan.from_dict(chart_request)
    if not base_plan.needs_chart:
        artifact = ChartArtifact(generated=False, plan=base_plan.to_dict(), rationale=base_plan.rationale)
        emit(
            event_handler,
            "TXT_REDACTED",
            {"TXT_REDACTED": artifact.plan, "TXT_REDACTED": artifact.rationale or artifact.error},
        )
        return artifact

    emit(
        event_handler,
        "TXT_REDACTED",
        {"TXT_REDACTED": chart_model_name},
    )
    try:
        planned_plan, usage = chart_planner.plan(
            chart_request=chart_request,
            cache_manager=cache_manager,
        )
        plan = _preserve_chart_request_contract(base_plan=base_plan, planned_plan=planned_plan)
    except Exception as exc:
        plan = base_plan
        emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": str(exc),
                "TXT_REDACTED": plan.to_dict(),
                "TXT_REDACTED": chart_model_name,
            },
        )

    emit(
        event_handler,
        "TXT_REDACTED",
        {
            "TXT_REDACTED": plan.to_dict(),
            "TXT_REDACTED": usage,
            "TXT_REDACTED": chart_model_name,
        },
    )
    try:
        artifact = chart_renderer.render(
            plan=plan,
            semantic_interpretation=interpretation,
            question=question,
            answer_markdown=answer_markdown,
        )
    except Exception as exc:
        artifact = ChartArtifact(
            generated=False,
            plan=plan.to_dict(),
            rationale=plan.rationale,
            error=str(exc),
        )
    artifact.usage = usage
    if artifact.generated:
        emit(
            event_handler,
            "TXT_REDACTED",
            {"TXT_REDACTED": artifact.path, "TXT_REDACTED": artifact.row_count, "TXT_REDACTED": artifact.plan},
        )
    else:
        emit(
            event_handler,
            "TXT_REDACTED",
            {"TXT_REDACTED": artifact.plan, "TXT_REDACTED": artifact.rationale or artifact.error},
        )
    return artifact
