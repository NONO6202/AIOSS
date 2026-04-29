# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv
from google import genai
from google.genai import types

from .budget import derive_budget
from ..retrieval.catalog import CatalogService
from ..semantic.charting import ChartArtifact, GeminiChartPlanner
from ..schema.field_dictionary import FieldDictionary
from esg_core.field_contracts import FIELD_CONTRACTS
from esg_core.output.chart import ChartRenderer
from ..core.gemini_utils import (
    empty_usage_metadata,
    extract_response_text,
    extract_usage_metadata,
    merge_usage_metadata,
)
from ..core.explicit_cache import ExplicitCacheManager
from ..core.models import AgentStep, NormalizedQuery, RouteDecision
from ..core.models import DerivationRuleUpdate
from ..retrieval.query_memory import QueryMemory
from .query_normalizer import QueryNormalizer
from .derivation_rule_parser import DerivationRuleParser
from ..core.prompts import PromptLoader
from .router import GeminiRouter
from ..schema.navigator import NavigationResult, SchemaNavigator
from ..semantic.engine import SemanticQueryEngine
from ..semantic.registry import SemanticRegistry
from ..runtime.tools import ToolExecutionContext, ToolRegistry
from ..core.tracing import TraceRecorder
from ..runtime.chart import (
    build_chart_request as build_chart_request_helper,
    generate_chart_artifact as generate_chart_artifact_helper,
    no_chart_plan as no_chart_plan_helper,
    parse_chart_plan_json as parse_chart_plan_json_helper,
)
from ..runtime.loops import run_loop as run_loop_helper, run_strong_direct as run_strong_direct_helper
from ..runtime.memory import remember_query_path as remember_query_path_helper
from ..runtime.modes import derive_execution_mode as derive_execution_mode_helper, is_collection_request as is_collection_request_helper

EventHandler = Callable[[str, Dict[str, Any]], None]
SEMANTIC_ENGINE_VERSION = "TXT_REDACTED"
STRONG_ANALYSIS_VERSION = "TXT_REDACTED"


class GeminiAgentRunner:
    "TXT_REDACTED"

    def __init__(
        self,
        *,
        project_root: str,
        store_root: str,
        model_name: Optional[str] = None,
    ):
        load_dotenv()

        api_key = os.getenv("TXT_REDACTED", "TXT_REDACTED").strip()
        if not api_key:
            raise RuntimeError("TXT_REDACTED")

        self.project_root = Path(project_root).resolve()
        self.store_root = Path(store_root).resolve()
        self.model_name = model_name or os.getenv("TXT_REDACTED", "TXT_REDACTED")
        self.strong_model_name = (
            os.getenv("TXT_REDACTED", "TXT_REDACTED").strip()
            or os.getenv("TXT_REDACTED", "TXT_REDACTED").strip()
            or self.model_name
        )
        self.chart_model_name = os.getenv("TXT_REDACTED", "TXT_REDACTED").strip() or self.model_name
        self.client = genai.Client(api_key=api_key)
        self.prompts = PromptLoader(self.project_root / "TXT_REDACTED" / "TXT_REDACTED")
        self.explicit_cache_manager = ExplicitCacheManager(client=self.client, store_root=str(self.store_root), ttl_seconds=1)
        self.router = GeminiRouter(
            client=self.client,
            model_name=self.model_name,
            prompts=self.prompts,
            cache_manager=self.explicit_cache_manager,
        )
        self.catalog = CatalogService(str(self.store_root), template_path=str(self.project_root / "TXT_REDACTED" / "TXT_REDACTED"))
        self.semantic_registry = SemanticRegistry(str(self.project_root / "TXT_REDACTED" / "TXT_REDACTED" / "TXT_REDACTED"))
        self.field_dictionary: Optional[FieldDictionary] = None
        self.schema_navigator: Optional[SchemaNavigator] = None
        self.semantic_engine: Optional[SemanticQueryEngine] = None
        self.query_normalizer = QueryNormalizer(
            client=self.client,
            model_name=self.model_name,
            prompts=self.prompts,
            output_schema_md_path=self.catalog.output_schema_markdown_path,
            cache_manager=self.explicit_cache_manager,
        )
        self.derivation_rule_parser = DerivationRuleParser(
            client=self.client,
            model_name=self.model_name,
            prompts=self.prompts,
            cache_manager=self.explicit_cache_manager,
        )
        self.query_memory = QueryMemory(store_root=str(self.store_root), project_root=str(self.project_root))
        self.chart_planner = GeminiChartPlanner(client=self.client, model_name=self.chart_model_name, prompts=self.prompts)
        self.chart_renderer = ChartRenderer(
            catalog=self.catalog,
            registry=self.semantic_registry,
            output_dir=str(self.store_root / "TXT_REDACTED" / "TXT_REDACTED"),
        )

    def answer(
        self,
        question: str,
        *,
        year: Optional[str] = None,
        rebuild_catalog: bool = False,
        event_handler: Optional[EventHandler] = None,
    ) -> Dict[str, Any]:
        wall_started_at = time.monotonic()
        usage_totals = empty_usage_metadata()
        self._emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": question,
                "TXT_REDACTED": year,
                "TXT_REDACTED": self.model_name,
            },
        )
        if rebuild_catalog or not self.catalog.catalog_path.exists():
            self._emit(
                event_handler,
                "TXT_REDACTED",
                {"TXT_REDACTED": [year] if year else self.catalog.available_years()},
            )
            try:
                self.catalog.build(
                    years=[year] if year else None,
                    progress_callback=lambda event_type, payload: self._emit(event_handler, event_type, payload),
                )
            except Exception as exc:
                error_text = str(exc)
                self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": error_text})
                self._raise_catalog_error(exc)
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": str(self.catalog.catalog_path)})
        try:
            if self.field_dictionary is None or not self.catalog.field_dictionary_path.exists():
                self.field_dictionary = FieldDictionary(self.catalog.load_field_dictionary())
                self.semantic_engine = SemanticQueryEngine(self.catalog, self.semantic_registry, self.field_dictionary)
                self.schema_navigator = SchemaNavigator(self.catalog.load_output_schema())
            elif self.semantic_engine is None:
                self.semantic_engine = SemanticQueryEngine(self.catalog, self.semantic_registry, self.field_dictionary)
            elif self.schema_navigator is None:
                self.schema_navigator = SchemaNavigator(self.catalog.load_output_schema())
        except Exception as exc:
            self._raise_catalog_error(exc)

        trace = TraceRecorder(str(self.store_root), question=question)
        available_years = self.catalog.available_years()
        self.explicit_cache_manager.clear_request_ttl_seconds()
        route, route_meta = self.router.route_with_metadata(question, available_years=available_years)
        merge_usage_metadata(usage_totals, route_meta.get("TXT_REDACTED"))
        budget = derive_budget(route)
        self.explicit_cache_manager.set_request_ttl_seconds(budget.max_wall_time_sec)
        trace.set_context(route=route.to_dict(), budget=budget.to_dict(), model_name=self.model_name)
        trace.add_event("TXT_REDACTED", {"TXT_REDACTED": route.to_dict(), "TXT_REDACTED": route_meta.get("TXT_REDACTED", {})})
        trace.add_event("TXT_REDACTED", {"TXT_REDACTED": budget.to_dict()})
        self._emit(
            event_handler,
            "TXT_REDACTED",
            {
                "TXT_REDACTED": route.to_dict(),
                "TXT_REDACTED": route_meta.get("TXT_REDACTED", {}),
            },
        )
        self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": budget.to_dict()})

        derivation_update = self._maybe_parse_derivation_rule_update(
            question,
            route=route,
            usage_totals=usage_totals,
            trace=trace,
            event_handler=event_handler,
        )
        if derivation_update and derivation_update.should_update:
            result = self._apply_derivation_rule_update(
                update=derivation_update,
                route=route,
                budget=budget,
                trace=trace,
                usage_totals=usage_totals,
                wall_started_at=wall_started_at,
                event_handler=event_handler,
            )
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False})
            return result

        if route.needs_action and self._is_collection_request(question):
            answer = (
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
            )
            elapsed_seconds = time.monotonic() - wall_started_at
            trace_path = trace.finalize(
                answer_markdown=answer,
                cached=False,
                elapsed_seconds=elapsed_seconds,
                token_usage=usage_totals,
            )
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False})
            return {
                "TXT_REDACTED": answer,
                "TXT_REDACTED": route.to_dict(),
                "TXT_REDACTED": budget.to_dict(),
                "TXT_REDACTED": trace_path,
                "TXT_REDACTED": False,
                "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED"},
                "TXT_REDACTED": self._no_chart_plan("TXT_REDACTED"),
                "TXT_REDACTED": round(elapsed_seconds, 2),
                "TXT_REDACTED": dict(usage_totals),
                "TXT_REDACTED": 3,
                "TXT_REDACTED": 4,
            }

        if route.route in {"TXT_REDACTED", "TXT_REDACTED"}:
            answer = self.prompts.get("TXT_REDACTED").render(question=question)
            elapsed_seconds = time.monotonic() - wall_started_at
            trace_path = trace.finalize(
                answer_markdown=answer,
                cached=False,
                elapsed_seconds=elapsed_seconds,
                token_usage=usage_totals,
            )
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False})
            return {
                "TXT_REDACTED": answer,
                "TXT_REDACTED": route.to_dict(),
                "TXT_REDACTED": budget.to_dict(),
                "TXT_REDACTED": trace_path,
                "TXT_REDACTED": False,
                "TXT_REDACTED": {},
                "TXT_REDACTED": self._no_chart_plan("TXT_REDACTED"),
                "TXT_REDACTED": round(elapsed_seconds, 1),
                "TXT_REDACTED": dict(usage_totals),
                "TXT_REDACTED": 2,
                "TXT_REDACTED": 3,
            }

        prompt_versions = {
            "TXT_REDACTED": self.prompts.get("TXT_REDACTED").version,
            "TXT_REDACTED": self.prompts.get("TXT_REDACTED").version,
            "TXT_REDACTED": self.prompts.get("TXT_REDACTED").version,
            "TXT_REDACTED": self.prompts.get("TXT_REDACTED").version,
            "TXT_REDACTED": self.prompts.get("TXT_REDACTED").version,
            "TXT_REDACTED": self.semantic_registry.version,
            "TXT_REDACTED": FIELD_CONTRACTS.version,
            "TXT_REDACTED": self.field_dictionary.version if self.field_dictionary else "TXT_REDACTED",
            "TXT_REDACTED": self.schema_navigator.version if self.schema_navigator else "TXT_REDACTED",
            "TXT_REDACTED": self.catalog.load_output_schema().get("TXT_REDACTED", "TXT_REDACTED"),
            "TXT_REDACTED": SEMANTIC_ENGINE_VERSION,
            "TXT_REDACTED": STRONG_ANALYSIS_VERSION,
        }
        normalized_query: Optional[NormalizedQuery] = None
        navigation: Optional[NavigationResult] = None
        memory_hints: List[Dict[str, Any]] = []
        query_memory_event_emitted = False

        def emit_query_memory_event(event_name: str, payload: Dict[str, Any]) -> None:
            nonlocal query_memory_event_emitted
            trace.add_event(event_name, payload)
            self._emit(event_handler, event_name, payload)
            query_memory_event_emitted = True

        if route.route in {"TXT_REDACTED", "TXT_REDACTED"}:
            try:
                normalized_query, normalizer_usage = self.query_normalizer.normalize(
                    question,
                    available_years=available_years,
                )
                merge_usage_metadata(usage_totals, normalizer_usage)
                trace.add_event("TXT_REDACTED", {"TXT_REDACTED": normalized_query.to_dict(), "TXT_REDACTED": normalizer_usage})
                self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": normalized_query.to_dict(), "TXT_REDACTED": normalizer_usage})
            except Exception as exc:
                trace.add_event("TXT_REDACTED", {"TXT_REDACTED": str(exc)})
                self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": str(exc)})

            if normalized_query is None:
                emit_query_memory_event("TXT_REDACTED", {"TXT_REDACTED": "TXT_REDACTED"})
            elif self.schema_navigator is None:
                emit_query_memory_event("TXT_REDACTED", {"TXT_REDACTED": "TXT_REDACTED"})
            elif self.schema_navigator is not None:
                try:
                    memory_hints = self.query_memory.lookup(
                        question,
                        versions={
                            "TXT_REDACTED": prompt_versions["TXT_REDACTED"],
                            "TXT_REDACTED": prompt_versions["TXT_REDACTED"],
                            "TXT_REDACTED": prompt_versions["TXT_REDACTED"],
                            "TXT_REDACTED": prompt_versions["TXT_REDACTED"],
                        },
                    )
                except Exception as exc:
                    trace.add_event("TXT_REDACTED", {"TXT_REDACTED": str(exc)})
                    self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": str(exc)})
                if memory_hints:
                    trace.add_event("TXT_REDACTED", {"TXT_REDACTED": memory_hints})
                    self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": memory_hints})
                navigation = self.schema_navigator.navigate(
                    question=question,
                    normalized_query=normalized_query,
                    hint_entries=memory_hints,
                )
                trace.add_event("TXT_REDACTED", navigation.to_dict())
                self._emit(event_handler, "TXT_REDACTED", navigation.to_dict())

                navigation_result = self.semantic_engine.resolve_from_navigation(
                    question,
                    normalized_query=normalized_query,
                    navigation=navigation,
                    year_hint=year,
                    available_years=available_years,
                )
                if navigation_result is not None:
                    answer_markdown = navigation_result.answer_markdown
                    elapsed_seconds = time.monotonic() - wall_started_at
                    chart_request = self._build_chart_request(
                        question=question,
                        route=route.to_dict(),
                        answer_markdown=answer_markdown,
                        semantic_interpretation=navigation_result.interpretation,
                        normalized_query=normalized_query,
                    )
                    trace.add_event("TXT_REDACTED", navigation_result.to_dict())
                    self._emit(event_handler, "TXT_REDACTED", navigation_result.to_dict())
                    trace_path = trace.finalize(
                        answer_markdown=answer_markdown,
                        evidence=navigation_result.evidence,
                        cached=False,
                        elapsed_seconds=elapsed_seconds,
                        token_usage=usage_totals,
                        step_count=4,
                        tool_calls=1,
                        semantic_interpretation=navigation_result.interpretation,
                    )
                    memory_event = self._remember_query_path(
                        question=question,
                        normalized_query=normalized_query,
                        navigation=navigation,
                        prompt_versions=prompt_versions,
                        semantic_interpretation=navigation_result.interpretation,
                    )
                    emit_query_memory_event(memory_event["TXT_REDACTED"], memory_event["TXT_REDACTED"])
                    self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": True})
                    return {
                        "TXT_REDACTED": answer_markdown,
                        "TXT_REDACTED": route.to_dict(),
                        "TXT_REDACTED": budget.to_dict(),
                        "TXT_REDACTED": trace_path,
                        "TXT_REDACTED": False,
                        "TXT_REDACTED": navigation_result.evidence,
                        "TXT_REDACTED": navigation_result.interpretation,
                        "TXT_REDACTED": chart_request,
                        "TXT_REDACTED": round(elapsed_seconds, 2),
                        "TXT_REDACTED": dict(usage_totals),
                        "TXT_REDACTED": 3,
                        "TXT_REDACTED": 4,
                    }
                emit_query_memory_event(
                    "TXT_REDACTED",
                    {
                        "TXT_REDACTED": (
                            "TXT_REDACTED"
                            "TXT_REDACTED"
                            if navigation.status == "TXT_REDACTED"
                            else "TXT_REDACTED"
                        )
                    },
                )

        if route.route in {"TXT_REDACTED", "TXT_REDACTED"}:
            semantic_result = self.semantic_engine.resolve(
                question,
                year_hint=year,
                available_years=available_years,
            )
            if semantic_result is not None:
                if not query_memory_event_emitted:
                    emit_query_memory_event(
                        "TXT_REDACTED",
                        {"TXT_REDACTED": "TXT_REDACTED"},
                    )
                answer_markdown = semantic_result.answer_markdown
                elapsed_seconds = time.monotonic() - wall_started_at
                chart_request = self._build_chart_request(
                    question=question,
                    route=route.to_dict(),
                    answer_markdown=answer_markdown,
                    semantic_interpretation=semantic_result.interpretation,
                    normalized_query=normalized_query,
                )
                trace.add_event("TXT_REDACTED", semantic_result.to_dict())
                self._emit(
                    event_handler,
                    "TXT_REDACTED",
                    semantic_result.to_dict(),
                )
                trace_path = trace.finalize(
                    answer_markdown=answer_markdown,
                    evidence=semantic_result.evidence,
                    cached=False,
                    elapsed_seconds=elapsed_seconds,
                    token_usage=usage_totals,
                    step_count=1,
                    tool_calls=2,
                    semantic_interpretation=semantic_result.interpretation,
                )
                result = {
                    "TXT_REDACTED": answer_markdown,
                    "TXT_REDACTED": route.to_dict(),
                    "TXT_REDACTED": budget.to_dict(),
                    "TXT_REDACTED": trace_path,
                    "TXT_REDACTED": False,
                    "TXT_REDACTED": semantic_result.evidence,
                    "TXT_REDACTED": semantic_result.interpretation,
                    "TXT_REDACTED": chart_request,
                    "TXT_REDACTED": round(elapsed_seconds, 3),
                    "TXT_REDACTED": dict(usage_totals),
                    "TXT_REDACTED": 4,
                    "TXT_REDACTED": 1,
                }
                self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": False, "TXT_REDACTED": False, "TXT_REDACTED": True})
                return result

        context = ToolExecutionContext(
            project_root=self.project_root,
            store_root=self.store_root,
            catalog=self.catalog,
            default_year=year,
            cwd=self.project_root,
            allow_actions=route.route == "TXT_REDACTED",
        )
        registry = ToolRegistry(context)
        execution_mode = self._derive_execution_mode(question, route)
        trace.add_event("TXT_REDACTED", execution_mode)
        self._emit(event_handler, "TXT_REDACTED", execution_mode)
        if route.route in {"TXT_REDACTED", "TXT_REDACTED"} and not query_memory_event_emitted:
            emit_query_memory_event(
                "TXT_REDACTED",
                {"TXT_REDACTED": "TXT_REDACTED"},
            )
        if execution_mode["TXT_REDACTED"] == "TXT_REDACTED":
            try:
                result = self._run_strong_direct(
                    question=question,
                    route=route,
                    budget=budget,
                    registry=registry,
                    trace=trace,
                    year=year,
                    event_handler=event_handler,
                    usage_totals=usage_totals,
                    wall_started_at=wall_started_at,
                    available_years=available_years,
                )
            except Exception as exc:
                trace.add_event("TXT_REDACTED", {"TXT_REDACTED": str(exc)})
                self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": str(exc)})
                result = self._run_loop(
                    question=question,
                    route=route,
                    budget=budget,
                    registry=registry.without_tools(["TXT_REDACTED"]),
                    trace=trace,
                    year=year,
                    event_handler=event_handler,
                    usage_totals=usage_totals,
                    wall_started_at=wall_started_at,
                )
        else:
            result = self._run_loop(
                question=question,
                route=route,
                budget=budget,
                registry=registry.without_tools(["TXT_REDACTED"]),
                trace=trace,
                year=year,
                event_handler=event_handler,
                usage_totals=usage_totals,
                wall_started_at=wall_started_at,
            )
        return result

    @staticmethod
    def _is_collection_request(question: str) -> bool:
        return is_collection_request_helper(question)

    def generate_chart_artifact(
        self,
        *,
        question: str,
        answer_result: Dict[str, Any],
        event_handler: Optional[EventHandler] = None,
    ) -> ChartArtifact:
        return generate_chart_artifact_helper(
            chart_planner=self.chart_planner,
            chart_renderer=self.chart_renderer,
            chart_model_name=self.chart_model_name,
            question=question,
            answer_result=answer_result,
            event_handler=event_handler,
            emit=self._emit,
            cache_manager=self.explicit_cache_manager,
        )

    @staticmethod
    def _no_chart_plan(reason: str) -> Dict[str, Any]:
        return no_chart_plan_helper(reason)

    @staticmethod
    def _parse_chart_plan_json(chart_plan_json: str) -> Dict[str, Any]:
        return parse_chart_plan_json_helper(chart_plan_json)

    def _build_chart_request(
        self,
        *,
        question: str,
        route: Dict[str, Any],
        answer_markdown: str,
        semantic_interpretation: Dict[str, Any],
        normalized_query: Optional[NormalizedQuery] = None,
    ) -> Dict[str, Any]:
        return build_chart_request_helper(
            question=question,
            route=route,
            answer_markdown=answer_markdown,
            semantic_interpretation=semantic_interpretation,
            normalized_query=normalized_query,
        )

    def _remember_query_path(
        self,
        *,
        question: str,
        normalized_query: NormalizedQuery,
        navigation: NavigationResult,
        prompt_versions: Dict[str, str],
        semantic_interpretation: Dict[str, Any],
    ) -> Dict[str, Any]:
        return remember_query_path_helper(
            query_memory=self.query_memory,
            question=question,
            normalized_query=normalized_query,
            navigation=navigation,
            prompt_versions=prompt_versions,
            semantic_interpretation=semantic_interpretation,
        )

    @staticmethod
    def _derive_execution_mode(question: str, route: RouteDecision) -> Dict[str, Any]:
        return derive_execution_mode_helper(question, route)

    def _run_strong_direct(
        self,
        *,
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
    ) -> Dict[str, Any]:
        return run_strong_direct_helper(
            client=self.client,
            strong_model_name=self.strong_model_name,
            prompts=self.prompts,
            catalog=self.catalog,
            question=question,
            route=route,
            budget=budget,
            registry=registry,
            trace=trace,
            year=year,
            event_handler=event_handler,
            usage_totals=usage_totals,
            wall_started_at=wall_started_at,
            available_years=available_years,
            emit=self._emit,
            no_chart_plan=self._no_chart_plan,
            parse_chart_plan_json=self._parse_chart_plan_json,
            cache_manager=self.explicit_cache_manager,
        )

    def _run_loop(
        self,
        *,
        question: str,
        route: RouteDecision,
        budget,
        registry: ToolRegistry,
        trace: TraceRecorder,
        year: Optional[str],
        event_handler: Optional[EventHandler],
        usage_totals: Dict[str, int],
        wall_started_at: float,
    ) -> Dict[str, Any]:
        return run_loop_helper(
            client=self.client,
            model_name=self.model_name,
            prompts=self.prompts,
            question=question,
            route=route,
            budget=budget,
            registry=registry,
            trace=trace,
            year=year,
            event_handler=event_handler,
            usage_totals=usage_totals,
            wall_started_at=wall_started_at,
            emit=self._emit,
            no_chart_plan=self._no_chart_plan,
            parse_chart_plan_json=self._parse_chart_plan_json,
            count_tokens=self._count_tokens,
            cache_manager=self.explicit_cache_manager,
        )

    def _count_tokens(self, prompt: str) -> int:
        try:
            result = self.client.models.count_tokens(
                model=self.model_name,
                contents=prompt,
            )
            return int(getattr(result, "TXT_REDACTED", 2) or 3)
        except Exception:
            return max(4, len(prompt) // 1)


    @staticmethod
    def _emit(handler: Optional[EventHandler], event_type: str, payload: Dict[str, Any]) -> None:
        if handler is None:
            return
        handler(event_type, payload)

    @staticmethod
    def _raise_catalog_error(exc: Exception) -> None:
        error_text = str(exc)
        if "TXT_REDACTED" in error_text:
            raise RuntimeError(
                "TXT_REDACTED"
                "TXT_REDACTED"
            ) from exc
        raise exc

    def _maybe_parse_derivation_rule_update(
        self,
        question: str,
        *,
        route: RouteDecision,
        usage_totals: Dict[str, int],
        trace: TraceRecorder,
        event_handler: Optional[EventHandler],
    ) -> Optional[DerivationRuleUpdate]:
        lowered = str(question or "TXT_REDACTED").lower()
        if route.route != "TXT_REDACTED" and not any(token in lowered for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
            return None
        try:
            update, usage = self.derivation_rule_parser.parse(question)
        except Exception as exc:
            trace.add_event("TXT_REDACTED", {"TXT_REDACTED": str(exc)})
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": str(exc)})
            return None
        merge_usage_metadata(usage_totals, usage)
        trace.add_event("TXT_REDACTED", {"TXT_REDACTED": update.to_dict(), "TXT_REDACTED": usage})
        self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": update.to_dict(), "TXT_REDACTED": usage})
        return update

    def _apply_derivation_rule_update(
        self,
        *,
        update: DerivationRuleUpdate,
        route: RouteDecision,
        budget,
        trace: TraceRecorder,
        usage_totals: Dict[str, int],
        wall_started_at: float,
        event_handler: Optional[EventHandler],
    ) -> Dict[str, Any]:
        if update.formula_op != "TXT_REDACTED" or len(update.operands) != 2 or not update.label:
            answer = (
                "TXT_REDACTED"
                "TXT_REDACTED"                                    
                "TXT_REDACTED"                                      
                "TXT_REDACTED"                                                   
            )
        else:
            payload = {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": {}}
            rules_path = self.catalog.derivation_rules_path
            if rules_path.exists():
                payload = json.loads(rules_path.read_text(encoding="TXT_REDACTED"))
            rule_id = self._rule_id(update.label)
            payload.setdefault("TXT_REDACTED", {})[rule_id] = {
                "TXT_REDACTED": rule_id,
                "TXT_REDACTED": update.label,
                "TXT_REDACTED": [update.label, *[alias for alias in update.aliases if alias != update.label]],
                "TXT_REDACTED": update.operands[:3],
                "TXT_REDACTED": "TXT_REDACTED"                                            ,
                "TXT_REDACTED": update.unit,
            }
            payload["TXT_REDACTED"] = str(int(str(payload.get("TXT_REDACTED") or "TXT_REDACTED")) + 4)
            rules_path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="TXT_REDACTED")
            try:
                self.catalog.build_output_schema()
            except Exception:
                pass
            self.schema_navigator = SchemaNavigator(self.catalog.load_output_schema())
            trace.add_event("TXT_REDACTED", {"TXT_REDACTED": rule_id, "TXT_REDACTED": update.label})
            self._emit(event_handler, "TXT_REDACTED", {"TXT_REDACTED": rule_id, "TXT_REDACTED": update.label})
            answer = (
                "TXT_REDACTED"
                "TXT_REDACTED"                          
                "TXT_REDACTED"                                                       
                "TXT_REDACTED"                       
                "TXT_REDACTED"
            )
        elapsed_seconds = time.monotonic() - wall_started_at
        trace_path = trace.finalize(
            answer_markdown=answer,
            cached=False,
            elapsed_seconds=elapsed_seconds,
            token_usage=usage_totals,
        )
        return {
            "TXT_REDACTED": answer,
            "TXT_REDACTED": route.to_dict(),
            "TXT_REDACTED": budget.to_dict(),
            "TXT_REDACTED": trace_path,
            "TXT_REDACTED": False,
            "TXT_REDACTED": {"TXT_REDACTED": "TXT_REDACTED", "TXT_REDACTED": update.label},
            "TXT_REDACTED": self._no_chart_plan("TXT_REDACTED"),
            "TXT_REDACTED": round(elapsed_seconds, 2),
            "TXT_REDACTED": dict(usage_totals),
            "TXT_REDACTED": 3,
            "TXT_REDACTED": 4,
        }

    @staticmethod
    def _rule_id(label: str) -> str:
        compact = re.sub("TXT_REDACTED", "TXT_REDACTED", str(label or "TXT_REDACTED").strip().lower())
        compact = re.sub("TXT_REDACTED", "TXT_REDACTED", compact).strip("TXT_REDACTED")
        return compact or "TXT_REDACTED"
