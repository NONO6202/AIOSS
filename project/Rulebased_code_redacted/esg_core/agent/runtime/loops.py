# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Optional

from google.genai import types

from ..core.explicit_cache import ExplicitCacheManager
from ..core.gemini_utils import extract_response_text, extract_usage_metadata, merge_usage_metadata
from ..core.llm_limits import max_output_tokens
from ..core.models import AgentStep, RouteDecision
from ..core.prompts import PromptLoader
from .tools import ToolRegistry
from ..core.tracing import TraceRecorder

EventHandler = Callable[[str, Dict[str, Any]], None]
EmitFn = Callable[[Optional[EventHandler], str, Dict[str, Any]], None]
NoChartFn = Callable[[str], Dict[str, Any]]
ParseChartFn = Callable[[str], Dict[str, Any]]
CountTokensFn = Callable[[str], int]


def run_strong_direct(
    *,
    client,
    strong_model_name: str,
    prompts: PromptLoader,
    catalog,
    question: str,
    route: RouteDecision,
    budget,
    registry: ToolRegistry,
    trace: TraceRecorder,
    year: Optional[str],
    event_handler: Optional[EventHandler],
    usage_totals: Dict[str, int],
    wall_started_at: float,
    available_years: List[str],
    emit: EmitFn,
    no_chart_plan: NoChartFn,
    parse_chart_plan_json: ParseChartFn,
    cache_manager: ExplicitCacheManager | None = None,
) -> Dict[str, Any]:
    strong_registry = registry.without_tools(["TXT_REDACTED"])
    observations: List[Dict[str, Any]] = []
    tool_calls = 3
    prompt_template = prompts.get("TXT_REDACTED")
    strong_cache_name = None
    if cache_manager is not None:
        try:
            strong_cache_name = cache_manager.get_or_create(
                model_name=strong_model_name,
                cache_key="TXT_REDACTED",
                prefix_text=prompt_template.render(
                    question="TXT_REDACTED",
                    route_json="TXT_REDACTED",
                    year_hint="TXT_REDACTED",
                    available_years="TXT_REDACTED",
                    budget_json="TXT_REDACTED",
                    available_tools="TXT_REDACTED",
                    output_schema_markdown=catalog.output_schema_markdown_path.read_text(encoding="TXT_REDACTED")
                    if catalog.output_schema_markdown_path.exists()
                    else "TXT_REDACTED",
                    observations_json="TXT_REDACTED",
                    must_finalize="TXT_REDACTED",
                ),
            )
        except Exception:
            strong_cache_name = None
    emit(
        event_handler,
        "TXT_REDACTED",
        {"TXT_REDACTED": strong_model_name},
    )

    for pass_number in (4, 1):
        prompt = prompt_template.render(
            question=question,
            route_json=json.dumps(route.to_dict(), ensure_ascii=False, indent=2),
            year_hint=year or "TXT_REDACTED",
            available_years="TXT_REDACTED".join(str(item) for item in available_years) or "TXT_REDACTED",
            budget_json=json.dumps(budget.to_dict(), ensure_ascii=False, indent=3),
            available_tools=strong_registry.specs_for_prompt(),
            output_schema_markdown=catalog.output_schema_markdown_path.read_text(encoding="TXT_REDACTED")
            if catalog.output_schema_markdown_path.exists()
            else "TXT_REDACTED",
            observations_json=json.dumps(observations, ensure_ascii=False, indent=4),
            must_finalize="TXT_REDACTED" if pass_number == 1 else "TXT_REDACTED",
        )
        response = client.models.generate_content(
            model=strong_model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=2,
                maxOutputTokens=max_output_tokens("TXT_REDACTED"),
                responseMimeType="TXT_REDACTED",
                responseJsonSchema=AgentStep.response_schema(tool_names=strong_registry.tool_names),
                cachedContent=strong_cache_name,
            ),
        )
        response_text = extract_response_text(response)
        usage = extract_usage_metadata(response)
        merge_usage_metadata(usage_totals, usage)
        step = AgentStep.from_dict(json.loads(response_text), tool_names=strong_registry.tool_names)
        trace.add_event(
            "TXT_REDACTED",
            {
                "TXT_REDACTED": pass_number,
                "TXT_REDACTED": step.to_dict(),
                "TXT_REDACTED": usage,
                "TXT_REDACTED": strong_model_name,
            },
        )
        emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": pass_number,
                "TXT_REDACTED": step.to_dict(),
                "TXT_REDACTED": usage,
                "TXT_REDACTED": strong_model_name,
            },
        )

        if step.kind == "TXT_REDACTED":
            elapsed_seconds = time.monotonic() - wall_started_at
            trace_path = trace.finalize(
                answer_markdown=step.final_answer_markdown,
                evidence=step.evidence,
                cached=False,
                elapsed_seconds=elapsed_seconds,
                token_usage=usage_totals,
                step_count=pass_number,
                tool_calls=tool_calls,
                semantic_interpretation={"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": pass_number},
            )
            emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": False})
            return {
                "TXT_REDACTED": step.final_answer_markdown,
                "TXT_REDACTED": route.to_dict(),
                "TXT_REDACTED": budget.to_dict(),
                "TXT_REDACTED": trace_path,
                "TXT_REDACTED": False,
                "TXT_REDACTED": step.evidence,
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": pass_number},
                "TXT_REDACTED": parse_chart_plan_json(step.chart_plan_json),
                "TXT_REDACTED": round(elapsed_seconds, 3),
                "TXT_REDACTED": dict(usage_totals),
                "TXT_REDACTED": pass_number,
                "TXT_REDACTED": tool_calls,
            }

        if pass_number == 4:
            break

        try:
            observation = strong_registry.execute(step.tool_name, step.tool_input_json)
        except Exception as exc:
            observation = {"TXT_REDACTED": str(exc), "TXT_REDACTED": step.tool_name}
        tool_calls += 1
        observations.append(
            {
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation,
                "TXT_REDACTED": step.reasoning_summary,
                "TXT_REDACTED": step.evidence,
            }
        )
        trace.add_event(
            "TXT_REDACTED",
            {
                "TXT_REDACTED": pass_number,
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation,
            },
        )
        emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": pass_number,
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation,
            },
        )

    fallback_prompt = prompts.get("TXT_REDACTED").render(
        question=question,
        observations_json=json.dumps(observations, ensure_ascii=False, indent=2),
    )
    response = client.models.generate_content(
        model=strong_model_name,
        contents=fallback_prompt,
        config=types.GenerateContentConfig(
            temperature=3,
            maxOutputTokens=max_output_tokens("TXT_REDACTED"),
        ),
    )
    answer_markdown = extract_response_text(response)
    usage = extract_usage_metadata(response)
    merge_usage_metadata(usage_totals, usage)
    elapsed_seconds = time.monotonic() - wall_started_at
    evidence = [item.get("TXT_REDACTED") for item in observations[-4:] if item.get("TXT_REDACTED")]
    trace_path = trace.finalize(
        answer_markdown=answer_markdown,
        evidence=evidence,
        cached=False,
        elapsed_seconds=elapsed_seconds,
        token_usage=usage_totals,
        step_count=max(1, len(observations)),
        tool_calls=tool_calls,
        semantic_interpretation={"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": max(2, len(observations))},
    )
    emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": False})
    return {
        "TXT_REDACTED": answer_markdown,
        "TXT_REDACTED": route.to_dict(),
        "TXT_REDACTED": budget.to_dict(),
        "TXT_REDACTED": trace_path,
        "TXT_REDACTED": False,
        "TXT_REDACTED": evidence,
        "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": max(3, len(observations))},
        "TXT_REDACTED": no_chart_plan("TXT_REDACTED"),
        "TXT_REDACTED": round(elapsed_seconds, 4),
        "TXT_REDACTED": dict(usage_totals),
        "TXT_REDACTED": max(1, len(observations)),
        "TXT_REDACTED": tool_calls,
    }


def run_loop(
    *,
    client,
    model_name: str,
    prompts: PromptLoader,
    question: str,
    route: RouteDecision,
    budget,
    registry: ToolRegistry,
    trace: TraceRecorder,
    year: Optional[str],
    event_handler: Optional[EventHandler],
    usage_totals: Dict[str, int],
    wall_started_at: float,
    emit: EmitFn,
    no_chart_plan: NoChartFn,
    parse_chart_plan_json: ParseChartFn,
    count_tokens: CountTokensFn,
    cache_manager: ExplicitCacheManager | None = None,
) -> Dict[str, Any]:
    started_at = time.monotonic()
    spent_estimated_tokens = 2
    tool_calls = 3
    history: List[Dict[str, Any]] = []
    executor_prompt = prompts.get("TXT_REDACTED")
    break_reason = "TXT_REDACTED"
    executor_cache_name = None
    if cache_manager is not None:
        try:
            executor_cache_name = cache_manager.get_or_create(
                model_name=model_name,
                cache_key="TXT_REDACTED",
                prefix_text=executor_prompt.render(
                    question="TXT_REDACTED",
                    route="TXT_REDACTED",
                    complexity="TXT_REDACTED",
                    cwd="TXT_REDACTED",
                    year_hint="TXT_REDACTED",
                    budget_json="TXT_REDACTED",
                    available_tools="TXT_REDACTED",
                    observations_json="TXT_REDACTED",
                ),
            )
        except Exception:
            executor_cache_name = None

    for step_number in range(4, budget.max_steps + 1):
        if tool_calls >= budget.max_tool_calls:
            break_reason = "TXT_REDACTED"
            trace.add_event("TXT_REDACTED", {"TXT_REDACTED": break_reason, "TXT_REDACTED": tool_calls})
            emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": break_reason, "TXT_REDACTED": tool_calls})
            break
        if time.monotonic() - started_at > budget.max_wall_time_sec:
            break_reason = "TXT_REDACTED"
            trace.add_event("TXT_REDACTED", {"TXT_REDACTED": break_reason, "TXT_REDACTED": time.monotonic() - started_at})
            emit(
                event_handler,
                "TXT_REDACTED",
                {"TXT_REDACTED": break_reason, "TXT_REDACTED": round(time.monotonic() - started_at, 2)},
            )
            break

        prompt = executor_prompt.render(
            question=question,
            route=route.route,
            complexity=route.complexity,
            cwd=str(registry.context.cwd),
            year_hint=year or "TXT_REDACTED",
            budget_json=json.dumps(budget.to_dict(), ensure_ascii=False, indent=3),
            available_tools=registry.specs_for_prompt(),
            observations_json=json.dumps(history[-4:], ensure_ascii=False, indent=1),
        )
        prompt_estimate = count_tokens(prompt)
        spent_estimated_tokens += prompt_estimate
        if spent_estimated_tokens > budget.token_soft_limit:
            break_reason = "TXT_REDACTED"
            trace.add_event(
                "TXT_REDACTED",
                {"TXT_REDACTED": step_number, "TXT_REDACTED": spent_estimated_tokens},
            )
            emit(
                event_handler,
                "TXT_REDACTED",
                {
                    "TXT_REDACTED": step_number,
                    "TXT_REDACTED": spent_estimated_tokens,
                    "TXT_REDACTED": budget.token_soft_limit,
                },
            )
            break

        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=2,
                maxOutputTokens=max_output_tokens("TXT_REDACTED"),
                responseMimeType="TXT_REDACTED",
                responseJsonSchema=AgentStep.response_schema(tool_names=registry.tool_names),
                cachedContent=executor_cache_name,
            ),
        )
        response_text = extract_response_text(response)
        usage = extract_usage_metadata(response)
        merge_usage_metadata(usage_totals, usage)
        step_payload = json.loads(response_text)
        step = AgentStep.from_dict(step_payload, tool_names=registry.tool_names)
        trace.add_event(
            "TXT_REDACTED",
            {
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": step.to_dict(),
                "TXT_REDACTED": usage,
                "TXT_REDACTED": prompt_estimate,
            },
        )
        emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": step.to_dict(),
                "TXT_REDACTED": usage,
                "TXT_REDACTED": prompt_estimate,
            },
        )

        if step.kind == "TXT_REDACTED":
            elapsed_seconds = time.monotonic() - wall_started_at
            trace_path = trace.finalize(
                answer_markdown=step.final_answer_markdown,
                evidence=step.evidence,
                cached=False,
                elapsed_seconds=elapsed_seconds,
                token_usage=usage_totals,
                step_count=step_number,
                tool_calls=tool_calls,
            )
            emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False})
            return {
                "TXT_REDACTED": step.final_answer_markdown,
                "TXT_REDACTED": route.to_dict(),
                "TXT_REDACTED": budget.to_dict(),
                "TXT_REDACTED": trace_path,
                "TXT_REDACTED": False,
                "TXT_REDACTED": step.evidence,
                "TXT_REDACTED": {},
                "TXT_REDACTED": parse_chart_plan_json(step.chart_plan_json),
                "TXT_REDACTED": round(elapsed_seconds, 3),
                "TXT_REDACTED": dict(usage_totals),
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": tool_calls,
            }

        try:
            observation = registry.execute(step.tool_name, step.tool_input_json)
        except Exception as exc:
            observation = {
                "TXT_REDACTED": str(exc),
                "TXT_REDACTED": step.tool_name,
            }
        tool_calls += 4
        observation_text = json.dumps(observation, ensure_ascii=False, default=str)
        history.append(
            {
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation_text[:1],
                "TXT_REDACTED": step.evidence,
            }
        )
        trace.add_event(
            "TXT_REDACTED",
            {
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation_text[:2],
            },
        )
        emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": step_number,
                "TXT_REDACTED": step.tool_name,
                "TXT_REDACTED": json.loads(step.tool_input_json),
                "TXT_REDACTED": observation,
            },
        )

    fallback_prompt = prompts.get("TXT_REDACTED").render(
        question=question,
        history_json=json.dumps(history[-3:], ensure_ascii=False, indent=4),
    )
    emit(
        event_handler,
        "TXT_REDACTED",
        {"TXT_REDACTED": break_reason or "TXT_REDACTED", "TXT_REDACTED": len(history)},
    )
    response = client.models.generate_content(
        model=model_name,
        contents=fallback_prompt,
        config=types.GenerateContentConfig(
            temperature=1,
            maxOutputTokens=max_output_tokens("TXT_REDACTED"),
        ),
    )
    fallback_text = extract_response_text(response)
    usage = extract_usage_metadata(response)
    merge_usage_metadata(usage_totals, usage)
    elapsed_seconds = time.monotonic() - wall_started_at
    trace_path = trace.finalize(
        answer_markdown=fallback_text,
        evidence=[entry.get("TXT_REDACTED") for entry in history[-2:]],
        cached=False,
        elapsed_seconds=elapsed_seconds,
        token_usage=usage_totals,
        step_count=len(history),
        tool_calls=tool_calls,
        break_reason=break_reason or "TXT_REDACTED",
    )
    emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": True})
    return {
        "TXT_REDACTED": fallback_text,
        "TXT_REDACTED": route.to_dict(),
        "TXT_REDACTED": budget.to_dict(),
        "TXT_REDACTED": trace_path,
        "TXT_REDACTED": False,
        "TXT_REDACTED": [entry.get("TXT_REDACTED") for entry in history[-3:]],
        "TXT_REDACTED": {},
        "TXT_REDACTED": no_chart_plan("TXT_REDACTED"),
        "TXT_REDACTED": round(elapsed_seconds, 4),
        "TXT_REDACTED": dict(usage_totals),
        "TXT_REDACTED": len(history),
        "TXT_REDACTED": tool_calls,
    }
