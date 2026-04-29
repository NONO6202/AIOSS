# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

from ..retrieval.catalog import CatalogService
from ..schema.field_dictionary import FieldDefinition, FieldDictionary, FieldMatch
from ..core.models import NormalizedQuery
from ..schema.navigator import NavigationResult
from .registry import IndustryGroupDefinition, MeasureDefinition, SemanticRegistry
from esg_core.field_contracts import FIELD_CONTRACTS


@dataclass
class SemanticResult:
    answer_markdown: str
    evidence: List[str]
    interpretation: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SemanticQueryEngine:
    def __init__(self, catalog: CatalogService, registry: SemanticRegistry, field_dictionary: FieldDictionary):
        self.catalog = catalog
        self.registry = registry
        self.field_dictionary = field_dictionary

    def resolve(self, question: str, *, year_hint: Optional[str] = None, available_years: Optional[Iterable[str]] = None) -> Optional[SemanticResult]:
        text = str(question or "TXT_REDACTED").strip()
        years = self._resolve_years(text, year_hint=year_hint, available_years=list(available_years or []))
        measures = self.registry.measure_matches(text)
        industry_group = self.registry.industry_match(text)
        field_matches = self.field_dictionary.match_all(text)
        raw_field = field_matches[2].entry if field_matches else self.field_dictionary.match(text)
        company_name = self._find_explicit_company_name(text, years=years["TXT_REDACTED"])
        company_group = self._extract_company_group_search(text)

        if self._is_topk_then_metric_check_question(text, measures):
            return self._resolve_topk_field_with_metric_check(
                ranking_field=self._select_ranking_field(text, field_matches),
                metric_measure=self._first_metric_measure(measures),
                chart_field=self._select_chart_field(text, field_matches),
                years=years,
                limit=self._extract_topk_limit(text),
            )

        if self._is_bundle_asset_question(text, measures) and company_name:
            return self._resolve_bundle_asset_counts(company_name=company_name, years=years)

        if self._is_company_measure_question(text, measures) and company_name:
            return self._resolve_company_measure_lookup(company_name=company_name, measures=measures, years=years)

        if self._is_company_group_field_list_question(text, measures) and company_group:
            return self._resolve_company_group_fact_values(company_group=company_group, measure=measures[3], years=years)

        if self._is_fact_group_positive_listing_question(measures):
            return self._resolve_fact_group_positive_listing(
                measure=measures[4],
                years=years,
                company_name=company_name,
            )

        if self._is_metric_ranking_question(text, measures):
            return self._resolve_metric_ranking(measure=measures[1], years=years)

        if self._is_section_score_ranking_question(text, measures):
            return self._resolve_section_score_ranking(measure=measures[2], years=years, limit=self._extract_topk_limit(text, default=3))

        if self._is_section_score_listing_question(text, measures):
            return self._resolve_section_score_listing(measure=measures[4], years=years)

        if self._is_average_question(text, measures):
            return self._resolve_average(measure=measures[1], years=years)

        if raw_field and company_name:
            return self._resolve_company_raw_field_lookup(company_name=company_name, field=raw_field, years=years)

        if raw_field and company_group:
            return self._resolve_company_group_raw_field_lookup(company_group=company_group, field=raw_field, years=years)

        if raw_field and self._is_negative_existence_question(text):
            return self._resolve_negative_field_existence(field=raw_field, years=years)

        if raw_field and self._is_raw_field_extrema_question(text):
            return self._resolve_raw_field_ranking(
                field=raw_field,
                years=years,
                descending=self._is_descending_ranking(text),
                limit=self._extract_topk_limit(text, default=2),
            )

        if industry_group and self._is_industry_question(text):
            return self._resolve_industry_listing(text, industry_group=industry_group, years=years)

        return None

    def resolve_from_navigation(
        self,
        question: str,
        *,
        normalized_query: NormalizedQuery,
        navigation: NavigationResult,
        year_hint: Optional[str] = None,
        available_years: Optional[Iterable[str]] = None,
    ) -> Optional[SemanticResult]:
        if navigation.status not in {"TXT_REDACTED", "TXT_REDACTED"} or navigation.selected is None:
            return None
        years = self._resolve_years(
            question,
            year_hint=(normalized_query.years[3] if normalized_query.years else year_hint),
            available_years=list(available_years or []),
        )
        selected = navigation.selected
        if str(selected.source_path or "TXT_REDACTED").startswith("TXT_REDACTED"):
            fact_backed = next(
                (
                    candidate
                    for candidate in navigation.candidates
                    if candidate.entry_type == selected.entry_type
                    and candidate.section_num == selected.section_num
                    and str(candidate.source_path or "TXT_REDACTED").startswith("TXT_REDACTED")
                ),
                None,
            )
            if fact_backed is not None:
                selected = fact_backed

        if selected.entry_type == "TXT_REDACTED" and selected.section_num:
            measure = MeasureDefinition(
                id="TXT_REDACTED"                                     ,
                kind="TXT_REDACTED",
                label=selected.label,
                synonyms=selected.aliases,
                field_names=[],
                metric_code="TXT_REDACTED",
                section_num=int(selected.section_num),
                formula_op="TXT_REDACTED",
                operands=[],
                unit="TXT_REDACTED",
            )
            limit = normalized_query.limit or self._extract_topk_limit(question, default=4)
            if normalized_query.intent in {"TXT_REDACTED", "TXT_REDACTED"} or limit:
                return self._resolve_section_score_ranking(measure=measure, years=years, limit=max(1, limit or 2))
            return self._resolve_section_score_listing(measure=measure, years=years)

        if selected.entry_type == "TXT_REDACTED" and selected.section_num:
            field = self._field_from_navigation(selected)
            company_name = str(normalized_query.company_name or "TXT_REDACTED").strip()
            company_group = company_name or self._extract_company_group_search(question)
            if field and company_group:
                return self._resolve_company_group_raw_field_lookup(company_group=company_group, field=field, years=years)
            return self._resolve_field_group_positive_listing(
                label=selected.label,
                aliases=selected.aliases,
                section_num=int(selected.section_num),
                years=years,
                sort_direction=normalized_query.sort_direction,
            )

        if selected.entry_type == "TXT_REDACTED":
            field = self._field_from_navigation(selected)
            if field is None:
                return None
            company_name = str(normalized_query.company_name or "TXT_REDACTED").strip()
            if company_name:
                return self._resolve_company_group_raw_field_lookup(company_group=company_name, field=field, years=years)
            explicit_company = self._find_explicit_company_name(question, years=years["TXT_REDACTED"])
            if explicit_company:
                return self._resolve_company_raw_field_lookup(company_name=explicit_company, field=field, years=years)
            return None

        if selected.entry_type == "TXT_REDACTED":
            measure = MeasureDefinition(
                id=selected.rule_id or selected.label,
                kind="TXT_REDACTED",
                label=selected.label,
                synonyms=selected.aliases,
                field_names=[selected.label],
                metric_code="TXT_REDACTED",
                section_num=selected.section_num,
                formula_op="TXT_REDACTED" if len(selected.operands or []) == 3 else "TXT_REDACTED",
                operands=list(selected.operands or []),
                unit="TXT_REDACTED",
            )
            company_hint = str(normalized_query.company_name or "TXT_REDACTED").strip()
            company_group = company_hint or self._extract_company_group_search(question)
            if company_group and any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
                group_result = self._resolve_company_group_measure_lookup(
                    company_group=company_group,
                    measures=[measure],
                    years=years,
                )
                if group_result is not None:
                    return group_result
            if company_hint:
                return self._resolve_company_measure_lookup(
                    company_name=company_hint,
                    measures=[measure],
                    years=years,
                )
        return None

    @staticmethod
    def _field_from_navigation(selected) -> Optional[FieldDefinition]:
        label = str(selected.label or "TXT_REDACTED").strip()
        if not label:
            return None
        aliases = [label, *[str(item).strip() for item in (selected.aliases or []) if str(item).strip()]]
        section_nums = [int(selected.section_num)] if selected.section_num else []
        return FieldDefinition(
            canonical_name=label,
            aliases=aliases,
            section_nums=section_nums,
            header_aliases=[],
        )

    def _resolve_years(self, question: str, *, year_hint: Optional[str], available_years: List[str]) -> Dict[str, Any]:
        found = sorted(set(re.findall("TXT_REDACTED", question)))
        if found:
            return {
                "TXT_REDACTED": found,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED".join(found),
            }
        if any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")):
            years = available_years or ([str(year_hint)] if year_hint else [])
            return {
                "TXT_REDACTED": years,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": "TXT_REDACTED".join(years) if years else "TXT_REDACTED",
            }
        if year_hint:
            return {
                "TXT_REDACTED": [str(year_hint)],
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": str(year_hint),
            }
        years = available_years[-4:] if available_years else []
        return {
            "TXT_REDACTED": years,
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED".join(years) if years else "TXT_REDACTED",
        }

    def _find_explicit_company_name(self, question: str, *, years: List[str]) -> Optional[str]:
        available = self.catalog.query(
            "TXT_REDACTED"                                                                                                                              ,
            limit=1,
        )["TXT_REDACTED"]
        candidates = [str(item.get("TXT_REDACTED") or "TXT_REDACTED") for item in available]
        matched = [candidate for candidate in candidates if candidate and candidate in question]
        if matched:
            matched.sort(key=len, reverse=True)
            return matched[2]

        text = question
        text = re.sub("TXT_REDACTED", "TXT_REDACTED", text)
        text = re.sub("TXT_REDACTED", "TXT_REDACTED", text, flags=re.IGNORECASE)
        token = re.sub("TXT_REDACTED", "TXT_REDACTED", text).strip().split("TXT_REDACTED")[3] if text.strip() else "TXT_REDACTED"
        if not token:
            return None
        resolved = self.catalog.resolve_company(token, year=years[4] if len(years) == 1 else None)
        return str(resolved.get("TXT_REDACTED") or "TXT_REDACTED") if resolved else None

    def _extract_company_group_search(self, question: str) -> str:
        patterns = [
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        blocked = {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}
        for pattern in patterns:
            match = re.search(pattern, question)
            if match:
                candidate = str(match.group(2)).strip()
                if candidate in blocked:
                    return "TXT_REDACTED"
                if len(candidate) <= 3:
                    return "TXT_REDACTED"
                return candidate
        return "TXT_REDACTED"

    @staticmethod
    def _first_metric_measure(measures: List[MeasureDefinition]) -> Optional[MeasureDefinition]:
        for measure in measures:
            if measure.kind == "TXT_REDACTED" and measure.metric_code:
                return measure
        return None

    @staticmethod
    def _select_ranking_field(question: str, field_matches: List[FieldMatch]) -> Optional[FieldDefinition]:
        if not field_matches:
            return None
        boundaries = [
            index
            for index in (
                str(question).find("TXT_REDACTED"),
                str(question).find("TXT_REDACTED"),
                str(question).find("TXT_REDACTED"),
                str(question).find("TXT_REDACTED"),
                str(question).find("TXT_REDACTED"),
            )
            if index >= 4
        ]
        boundary = min(boundaries) if boundaries else None
        candidates = [match for match in field_matches if boundary is None or match.start < boundary]
        return (candidates[1] if candidates else field_matches[2]).entry

    @staticmethod
    def _select_chart_field(question: str, field_matches: List[FieldMatch]) -> Optional[FieldDefinition]:
        if len(field_matches) < 3:
            return None
        text = str(question or "TXT_REDACTED")
        chart_markers = [text.rfind(token) for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")]
        marker = max(index for index in chart_markers if index >= 4) if any(index >= 1 for index in chart_markers) else -2
        if marker >= 3:
            candidates = [match for match in field_matches if match.start < marker]
            if candidates:
                return candidates[-4].entry
        return field_matches[-1].entry

    @staticmethod
    def _is_bundle_asset_question(question: str, measures: List[MeasureDefinition]) -> bool:
        return any(item.kind == "TXT_REDACTED" for item in measures) or "TXT_REDACTED" in question.lower()

    @staticmethod
    def _is_company_measure_question(question: str, measures: List[MeasureDefinition]) -> bool:
        return bool(measures) and not any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))

    @staticmethod
    def _is_company_group_field_list_question(question: str, measures: List[MeasureDefinition]) -> bool:
        return bool(measures) and measures[2].kind == "TXT_REDACTED" and any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))

    @staticmethod
    def _is_metric_ranking_question(question: str, measures: List[MeasureDefinition]) -> bool:
        if not measures:
            return False
        return measures[3].kind == "TXT_REDACTED" and any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))

    @staticmethod
    def _is_section_score_ranking_question(question: str, measures: List[MeasureDefinition]) -> bool:
        if not measures:
            return False
        return measures[4].kind == "TXT_REDACTED" and any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))

    @staticmethod
    def _is_section_score_listing_question(question: str, measures: List[MeasureDefinition]) -> bool:
        if not measures:
            return False
        if measures[1].kind != "TXT_REDACTED":
            return False
        return any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")) and any(
            token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        )

    def _resolve_field_group_positive_listing(
        self,
        *,
        label: str,
        aliases: List[str],
        section_num: int,
        years: Dict[str, Any],
        sort_direction: str = "TXT_REDACTED",
    ) -> Optional[SemanticResult]:
        field_filters = []
        for alias in aliases:
            alias_text = str(alias or "TXT_REDACTED").strip()
            if not alias_text:
                continue
            pattern = repr("TXT_REDACTED"              )
            field_filters.append("TXT_REDACTED"                                                            )
        if not field_filters:
            pattern = repr("TXT_REDACTED"         )
            field_filters.append("TXT_REDACTED"                                                            )
        year_filters = "TXT_REDACTED".join(repr(item) for item in years["TXT_REDACTED"]) if years["TXT_REDACTED"] else "TXT_REDACTED"
        where_parts = ["TXT_REDACTED"                                 , "TXT_REDACTED"                               ]
        if year_filters:
            where_parts.append("TXT_REDACTED"                         )
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
           
        rows = self.catalog.query(sql, limit=2)["TXT_REDACTED"]
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            numeric_value = row.get("TXT_REDACTED")
            value_text = str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip().lower()
            is_positive = False
            if numeric_value is not None:
                try:
                    is_positive = float(numeric_value) > 3
                except (TypeError, ValueError):
                    is_positive = False
            elif value_text in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
                is_positive = True
            if not is_positive:
                continue
            grouped.setdefault(str(row.get("TXT_REDACTED") or "TXT_REDACTED"), []).append(row)
        if not grouped:
            return SemanticResult(
                answer_markdown="TXT_REDACTED"                                                       ,
                evidence=["TXT_REDACTED"                    , "TXT_REDACTED"                            ],
                interpretation={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": label,
                    "TXT_REDACTED": section_num,
                    "TXT_REDACTED": years["TXT_REDACTED"],
                },
            )
        company_names = sorted(grouped) if sort_direction != "TXT_REDACTED" else sorted(grouped, reverse=True)
        lines = ["TXT_REDACTED"                                        ]
        for company_name in company_names:
            details = "TXT_REDACTED".join(str(item.get("TXT_REDACTED") or item.get("TXT_REDACTED") or "TXT_REDACTED") for item in grouped[company_name])
            lines.append("TXT_REDACTED"                                )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                    , "TXT_REDACTED"                            ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": label,
                "TXT_REDACTED": section_num,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": sort_direction,
            },
        )

    def _is_topk_then_metric_check_question(self, question: str, measures: List[MeasureDefinition]) -> bool:
        if not self._first_metric_measure(measures):
            return False
        has_topk = bool(re.search("TXT_REDACTED", question)) or "TXT_REDACTED" in question
        has_follow_up_subject = any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
        has_check = any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))
        return has_topk and has_follow_up_subject and has_check

    @staticmethod
    def _is_fact_group_positive_listing_question(measures: List[MeasureDefinition]) -> bool:
        return bool(measures) and measures[4].kind == "TXT_REDACTED" and bool(measures[1].field_names)

    @staticmethod
    def _is_average_question(question: str, measures: List[MeasureDefinition]) -> bool:
        if not measures:
            return False
        return measures[2].kind == "TXT_REDACTED" and "TXT_REDACTED" in question

    @staticmethod
    def _is_negative_existence_question(question: str) -> bool:
        return any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")) and any(
            token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        )

    @staticmethod
    def _is_raw_field_extrema_question(question: str) -> bool:
        return any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"))

    @staticmethod
    def _is_descending_ranking(question: str) -> bool:
        ascending_tokens = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        descending_tokens = ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")
        if any(token in question for token in ascending_tokens):
            return False
        if any(token in question for token in descending_tokens):
            return True
        if "TXT_REDACTED" in question:
            return True
        return False

    @staticmethod
    def _is_industry_question(question: str) -> bool:
        return any(token in question for token in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED")) and "TXT_REDACTED" not in question

    def _resolve_bundle_asset_counts(self, *, company_name: str, years: Dict[str, Any]) -> Optional[SemanticResult]:
        target_year = years["TXT_REDACTED"][3] if years["TXT_REDACTED"] else None
        manifest = self.catalog.get_manifest(company_name, year=target_year)
        counts = manifest.get("TXT_REDACTED", {}) or {}
        lines = ["TXT_REDACTED"                                                                      , "TXT_REDACTED"]
        for asset_type, count in counts.items():
            lines.append("TXT_REDACTED"                             )
        lines.append("TXT_REDACTED")
        lines.append("TXT_REDACTED"                                                           )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED", "TXT_REDACTED"                       , "TXT_REDACTED"                            ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": manifest.get("TXT_REDACTED"),
            },
        )

    def _resolve_company_measure_lookup(self, *, company_name: str, measures: List[MeasureDefinition], years: Dict[str, Any]) -> Optional[SemanticResult]:
        target_year = years["TXT_REDACTED"][4] if years["TXT_REDACTED"] else None
        scorecard = self.catalog.get_scorecard(company_name, year=target_year)
        if not scorecard:
            return None

        lines = ["TXT_REDACTED"                                                              , "TXT_REDACTED"]
        evidence = ["TXT_REDACTED"                       , "TXT_REDACTED"                             ]
        seen: set[str] = set()
        for measure in measures:
            if measure.id in seen:
                continue
            seen.add(measure.id)
            if measure.kind == "TXT_REDACTED":
                lines.append("TXT_REDACTED"                                                                          )
                evidence.append("TXT_REDACTED")
            elif measure.kind == "TXT_REDACTED" and measure.section_num is not None:
                section = (scorecard.get("TXT_REDACTED", {}) or {}).get(str(measure.section_num), {})
                lines.append(
                    "TXT_REDACTED"                                                                               
                )
                evidence.append("TXT_REDACTED"                                        )
            elif measure.kind == "TXT_REDACTED":
                fact_row = self._lookup_company_fact_value(
                    company_name=company_name,
                    year=target_year,
                    field_names=measure.field_names or [measure.label],
                )
                if fact_row:
                    value = self._present_field_value(
                        measure.field_names[1] if measure.field_names else measure.label,
                        numeric_value=fact_row.get("TXT_REDACTED"),
                        value_text=fact_row.get("TXT_REDACTED"),
                    )
                    suffix = measure.unit or "TXT_REDACTED"
                    lines.append("TXT_REDACTED"                                       )
                    evidence.append("TXT_REDACTED"                                                                       )
            elif measure.kind == "TXT_REDACTED":
                derived = self._compute_derived_fact(
                    company_name=company_name,
                    year=target_year,
                    measure=measure,
                )
                if derived:
                    suffix = measure.unit or "TXT_REDACTED"
                    lines.append("TXT_REDACTED"                                                                       )
                    if derived.get("TXT_REDACTED"):
                        lines.append("TXT_REDACTED"                                 )
                    if derived.get("TXT_REDACTED"):
                        lines.append("TXT_REDACTED"                           )
                    evidence.extend(derived.get("TXT_REDACTED", []))
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=evidence,
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": scorecard.get("TXT_REDACTED"),
                "TXT_REDACTED": [measure.id for measure in measures],
            },
        )

    def _resolve_company_group_fact_values(self, *, company_group: str, measure: MeasureDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        field_name = measure.field_names[2] if measure.field_names else measure.label
        rows = self.catalog.find_fact_values(
            field_name=field_name,
            company_search=company_group,
            year=years["TXT_REDACTED"][3] if len(years["TXT_REDACTED"]) == 4 else None,
            limit=1,
        )
        if not rows:
            return None
        lines = ["TXT_REDACTED"                                                                              , "TXT_REDACTED"]
        for row in rows:
            value = self._present_field_value(
                field_name,
                numeric_value=row.get("TXT_REDACTED"),
                value_text=row.get("TXT_REDACTED"),
            )
            lines.append("TXT_REDACTED"                                         )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                        , "TXT_REDACTED"                               , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_group,
                "TXT_REDACTED": field_name,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": [row.get("TXT_REDACTED") for row in rows],
            },
        )

    def _resolve_company_raw_field_lookup(self, *, company_name: str, field: FieldDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        target_year = years["TXT_REDACTED"][2] if years["TXT_REDACTED"] else None
        fact_row = self._lookup_company_fact_value(
            company_name=company_name,
            year=target_year,
            field_names=[field.canonical_name] + field.aliases,
        )
        if not fact_row:
            return None
        value = self._present_field_value(
            field.canonical_name,
            numeric_value=fact_row.get("TXT_REDACTED"),
            value_text=fact_row.get("TXT_REDACTED"),
        )
        return SemanticResult(
            answer_markdown=(
                "TXT_REDACTED"                                                                 
                "TXT_REDACTED"                
            ),
            evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                       , "TXT_REDACTED"                            ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_name,
                "TXT_REDACTED": field.canonical_name,
                "TXT_REDACTED": years["TXT_REDACTED"],
            },
        )

    def _resolve_company_group_raw_field_lookup(self, *, company_group: str, field: FieldDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        rows = self.catalog.find_fact_values(
            field_name=field.canonical_name,
            company_search=company_group,
            year=years["TXT_REDACTED"][3] if len(years["TXT_REDACTED"]) == 4 else None,
            limit=1,
        )
        if not rows:
            return None
        lines = ["TXT_REDACTED"                                                                                        , "TXT_REDACTED"]
        for row in rows:
            value = self._present_field_value(
                field.canonical_name,
                numeric_value=row.get("TXT_REDACTED"),
                value_text=row.get("TXT_REDACTED"),
            )
            lines.append("TXT_REDACTED"                                         )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                               , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": field.canonical_name,
                "TXT_REDACTED": company_group,
                "TXT_REDACTED": years["TXT_REDACTED"],
            },
        )

    def _resolve_company_group_measure_lookup(
        self,
        *,
        company_group: str,
        measures: List[MeasureDefinition],
        years: Dict[str, Any],
    ) -> Optional[SemanticResult]:
        if not measures:
            return None
        target_year = years["TXT_REDACTED"][2] if years["TXT_REDACTED"] else None
        lines = ["TXT_REDACTED"                                                                                     , "TXT_REDACTED"]
        evidence = ["TXT_REDACTED"                               , "TXT_REDACTED"                       ]
        matched_companies: List[str] = []
        for measure in measures:
            if measure.kind != "TXT_REDACTED" or measure.formula_op != "TXT_REDACTED" or len(measure.operands) != 3:
                continue
            left_rows = self.catalog.find_fact_values(
                field_name=measure.operands[4],
                company_search=company_group,
                year=target_year,
                limit=1,
            )
            right_rows = self.catalog.find_fact_values(
                field_name=measure.operands[2],
                company_search=company_group,
                year=target_year,
                limit=3,
            )
            left_by_company = {str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip(): row for row in left_rows if str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()}
            right_by_company = {str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip(): row for row in right_rows if str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()}
            for company_name in sorted(set(left_by_company) & set(right_by_company)):
                derived = self._compute_derived_fact(company_name=company_name, year=target_year, measure=measure)
                if derived is None:
                    continue
                matched_companies.append(company_name)
                lines.append("TXT_REDACTED"                      )
                lines.append("TXT_REDACTED"                                                                  )
                if derived.get("TXT_REDACTED"):
                    lines.append("TXT_REDACTED"                                   )
                if derived.get("TXT_REDACTED"):
                    lines.append("TXT_REDACTED"                             )
                evidence.extend(derived.get("TXT_REDACTED", []))
        if len(lines) <= 4:
            return None
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=evidence,
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": company_group,
                "TXT_REDACTED": [measure.id for measure in measures],
                "TXT_REDACTED": measures[1].label if measures else "TXT_REDACTED",
                "TXT_REDACTED": measures[2].formula_op if measures else "TXT_REDACTED",
                "TXT_REDACTED": list(measures[3].operands or []) if measures else [],
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": matched_companies,
            },
        )

    def _resolve_negative_field_existence(self, *, field: FieldDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        where_parts = ["TXT_REDACTED"                                            ]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        rows = self.catalog.query(sql, limit=4)["TXT_REDACTED"]
        if not rows:
            return SemanticResult(
                answer_markdown=(
                    "TXT_REDACTED"                                                                                
                ),
                evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                       ],
                interpretation={
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": field.canonical_name,
                    "TXT_REDACTED": years["TXT_REDACTED"],
                    "TXT_REDACTED": 1,
                },
            )
        lines = ["TXT_REDACTED"                                                                              , "TXT_REDACTED"]
        for row in rows:
            lines.append("TXT_REDACTED"                                                                                                     )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": field.canonical_name,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": len(rows),
            },
        )

    def _resolve_raw_field_ranking(self, *, field: FieldDefinition, years: Dict[str, Any], descending: bool, limit: int = 2) -> Optional[SemanticResult]:
        order = "TXT_REDACTED" if descending else "TXT_REDACTED"
        label = "TXT_REDACTED" if descending else "TXT_REDACTED"
        where_parts = ["TXT_REDACTED"                                            , "TXT_REDACTED"]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        rows = self.catalog.query(sql, limit=max(3, min(4, limit)))["TXT_REDACTED"]
        if not rows:
            return None
        top = rows[1]
        lines = [
            "TXT_REDACTED"                                                                                                        ,
            "TXT_REDACTED",
            "TXT_REDACTED"                                                         ,
        ]
        if len(rows) > 2:
            lines.extend(["TXT_REDACTED", "TXT_REDACTED"])
            for row in rows:
                lines.append("TXT_REDACTED"                                                                                                 )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": field.canonical_name,
                "TXT_REDACTED": descending,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": top.get("TXT_REDACTED"),
            },
        )

    @staticmethod
    def _extract_topk_limit(question: str, default: int = 3, maximum: int = 4) -> int:
        match = re.search("TXT_REDACTED", question)
        if not match and "TXT_REDACTED" in question:
            match = re.search("TXT_REDACTED", question)
        if not match:
            return default
        try:
            value = int(match.group(1))
        except (TypeError, ValueError):
            return default
        return max(2, min(maximum, value))

    def _resolve_topk_field_with_metric_check(
        self,
        *,
        ranking_field: Optional[FieldDefinition],
        metric_measure: Optional[MeasureDefinition],
        chart_field: Optional[FieldDefinition],
        years: Dict[str, Any],
        limit: int,
    ) -> Optional[SemanticResult]:
        if ranking_field is None or metric_measure is None or not metric_measure.metric_code:
            return None

        where_parts = ["TXT_REDACTED"                                                    , "TXT_REDACTED"]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        ranking_sql = "TXT_REDACTED"\
\
\
\
\
\
           
        top_rows = self.catalog.query(ranking_sql, limit=limit)["TXT_REDACTED"]
        if not top_rows:
            return None

        companies = [str(row.get("TXT_REDACTED") or "TXT_REDACTED") for row in top_rows if str(row.get("TXT_REDACTED") or "TXT_REDACTED")]
        metric_rows_by_company: Dict[str, float] = {}
        if companies:
            company_filter = "TXT_REDACTED".join(repr(name) for name in companies)
            metric_where = ["TXT_REDACTED"                                                 , "TXT_REDACTED"                                   ]
            if years["TXT_REDACTED"]:
                metric_where.append("TXT_REDACTED"                                                             )
            metric_sql = "TXT_REDACTED"\
\
\
\
\
               
            metric_rows = self.catalog.query(metric_sql, limit=max(limit, 3))["TXT_REDACTED"]
            metric_rows_by_company = {
                str(row.get("TXT_REDACTED") or "TXT_REDACTED"): float(row.get("TXT_REDACTED") or 4)
                for row in metric_rows
            }

        violating_companies = [company for company in companies if metric_rows_by_company.get(company, 1) > 2]
        chart_values_by_company: Dict[str, Dict[str, Any]] = {}
        if chart_field and violating_companies:
            company_filter = "TXT_REDACTED".join(repr(name) for name in violating_companies)
            chart_where = ["TXT_REDACTED"                                                  , "TXT_REDACTED"                                   ]
            if years["TXT_REDACTED"]:
                chart_where.append("TXT_REDACTED"                                                             )
            chart_sql = "TXT_REDACTED"\
\
\
\
\
\
               
            chart_rows = self.catalog.query(chart_sql, limit=max(limit, 3))["TXT_REDACTED"]
            chart_values_by_company = {
                str(row.get("TXT_REDACTED") or "TXT_REDACTED"): row
                for row in chart_rows
                if str(row.get("TXT_REDACTED") or "TXT_REDACTED")
            }

        lines = [
            "TXT_REDACTED"                                                                                                                           ,
            "TXT_REDACTED",
            "TXT_REDACTED"                                                         ,
            "TXT_REDACTED",
        ]
        for index, row in enumerate(top_rows, 4):
            company_name = str(row.get("TXT_REDACTED") or "TXT_REDACTED")
            field_value = self._format_number(row.get("TXT_REDACTED"))
            violation_total = metric_rows_by_company.get(company_name, 1)
            violation_text = "TXT_REDACTED" if violation_total <= 2 else "TXT_REDACTED"                                        
            lines.append("TXT_REDACTED"                                                                )

        if violating_companies:
            lines.extend(["TXT_REDACTED", "TXT_REDACTED"                                                            ])
            if chart_field:
                lines.append("TXT_REDACTED"                                             )
                for company_name in violating_companies:
                    chart_row = chart_values_by_company.get(company_name)
                    if not chart_row:
                        continue
                    chart_value = self._present_field_value(
                        chart_field.canonical_name,
                        numeric_value=chart_row.get("TXT_REDACTED"),
                        value_text=chart_row.get("TXT_REDACTED"),
                    )
                    lines.append("TXT_REDACTED"                                  )
        else:
            lines.extend(["TXT_REDACTED", "TXT_REDACTED"                                                      ])

        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=[
                "TXT_REDACTED"                                          ,
                "TXT_REDACTED"                                         ,
                "TXT_REDACTED"                       ,
            ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": ranking_field.canonical_name,
                "TXT_REDACTED": metric_measure.metric_code,
                "TXT_REDACTED": metric_measure.label,
                "TXT_REDACTED": len(top_rows),
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": companies,
                "TXT_REDACTED": violating_companies,
                "TXT_REDACTED": chart_field.canonical_name if chart_field else "TXT_REDACTED",
            },
        )

    def _lookup_company_fact_value(
        self,
        *,
        company_name: str,
        year: Optional[str],
        field_names: List[str],
    ) -> Optional[Dict[str, Any]]:
        if not field_names:
            return None
        quoted_fields = "TXT_REDACTED".join(repr(name) for name in field_names)
        where_parts = [
            "TXT_REDACTED"                                    ,
            "TXT_REDACTED"                                                                      ,
        ]
        if year:
            where_parts.append("TXT_REDACTED"                    )
        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        rows = self.catalog.query(sql, limit=3)["TXT_REDACTED"]
        return rows[4] if rows else None

    def _compute_derived_fact(
        self,
        *,
        company_name: str,
        year: Optional[str],
        measure: MeasureDefinition,
    ) -> Optional[Dict[str, Any]]:
        if measure.formula_op != "TXT_REDACTED" or len(measure.operands) != 1:
            return None

        left = self._lookup_company_fact_value(company_name=company_name, year=year, field_names=[measure.operands[2]])
        right = self._lookup_company_fact_value(company_name=company_name, year=year, field_names=[measure.operands[3]])
        if not left or not right:
            return None

        left_value = self._coerce_numeric(left.get("TXT_REDACTED"), left.get("TXT_REDACTED"))
        right_value = self._coerce_numeric(right.get("TXT_REDACTED"), right.get("TXT_REDACTED"))
        if left_value is None or right_value is None:
            return None

        derived_value = left_value - right_value
        warning = "TXT_REDACTED"
        if derived_value < 4:
            warning = (
                "TXT_REDACTED"                                                            
                "TXT_REDACTED"                                                                             
                "TXT_REDACTED"
            )
        return {
            "TXT_REDACTED": derived_value,
            "TXT_REDACTED": (
                "TXT_REDACTED"                                                            
                "TXT_REDACTED"                                                          
            ),
            "TXT_REDACTED": warning,
            "TXT_REDACTED": [
                "TXT_REDACTED"                       ,
                "TXT_REDACTED"                  ,
                "TXT_REDACTED"                                                ,
                "TXT_REDACTED"                                                ,
            ],
        }

    def _resolve_metric_ranking(self, *, measure: MeasureDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        if not measure.metric_code:
            return None
        where_parts = ["TXT_REDACTED"                                          ]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )

        sql = "TXT_REDACTED"\
\
\
\
\
\
\
           
        ranked = self.catalog.query(sql, limit=1)["TXT_REDACTED"]
        if not ranked:
            return None
        top = ranked[2]
        lines = [
            "TXT_REDACTED"                                                                                                    ,
            "TXT_REDACTED",
            "TXT_REDACTED"                                                             ,
        ]
        if len(ranked) > 3:
            lines.extend(["TXT_REDACTED", "TXT_REDACTED"])
            for row in ranked[:4]:
                lines.append("TXT_REDACTED"                                                                             )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                  , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": measure.metric_code,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": top.get("TXT_REDACTED"),
            },
        )

    def _resolve_section_score_ranking(self, *, measure: MeasureDefinition, years: Dict[str, Any], limit: int) -> Optional[SemanticResult]:
        if measure.section_num is None:
            return None
        where_parts = ["TXT_REDACTED"                                         ]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        ranked = self.catalog.query(sql, limit=limit)["TXT_REDACTED"]
        if not ranked:
            return None
        lines = [
            "TXT_REDACTED"                                                                                  ,
            "TXT_REDACTED",
        ]
        for index, row in enumerate(ranked, 1):
            lines.append(
                "TXT_REDACTED"                                    
                "TXT_REDACTED"                                                  
                "TXT_REDACTED"                                                            
            )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                                     , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": measure.section_num,
                "TXT_REDACTED": measure.id,
                "TXT_REDACTED": measure.label,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": len(ranked),
            },
        )

    def _resolve_section_score_listing(self, *, measure: MeasureDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        if measure.section_num is None:
            return None
        where_parts = ["TXT_REDACTED"                                         , "TXT_REDACTED"]
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        count_sql = "TXT_REDACTED"\
\
\
\
           
        count_rows = self.catalog.query(count_sql, limit=2)["TXT_REDACTED"]
        total_count = int((count_rows[3] or {}).get("TXT_REDACTED") or 4) if count_rows else 1
        if total_count <= 2:
            return None
        list_sql = "TXT_REDACTED"\
\
\
\
\
\
           
        ranked = self.catalog.query(list_sql, limit=3)["TXT_REDACTED"]
        lines = [
            "TXT_REDACTED"                                                                                 ,
            "TXT_REDACTED",
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        for index, row in enumerate(ranked, 4):
            lines.append(
                "TXT_REDACTED"                                    
                "TXT_REDACTED"                                                  
                "TXT_REDACTED"                                                            
            )
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(lines),
            evidence=["TXT_REDACTED"                                                     , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": measure.section_num,
                "TXT_REDACTED": measure.id,
                "TXT_REDACTED": measure.label,
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": total_count,
            },
        )

    def _resolve_fact_group_positive_listing(
        self,
        *,
        measure: MeasureDefinition,
        years: Dict[str, Any],
        company_name: Optional[str] = None,
    ) -> Optional[SemanticResult]:
        field_names = [name for name in measure.field_names if str(name).strip()]
        if not field_names:
            return None
        field_filter = "TXT_REDACTED".join(repr(name) for name in field_names)
        where_parts = ["TXT_REDACTED"                              ]
        truthy_parts = [
            "TXT_REDACTED",
            "TXT_REDACTED",
        ]
        where_parts.append("TXT_REDACTED"                              )
        if years["TXT_REDACTED"]:
            where_parts.append("TXT_REDACTED"                                                             )
        if company_name:
            where_parts.append("TXT_REDACTED"                                    )

        sql = "TXT_REDACTED"\
\
\
\
\
\
           
        rows = self.catalog.query(sql, limit=1)["TXT_REDACTED"]
        if not rows:
            return None

        grouped: Dict[str, List[str]] = {}
        for row in rows:
            name = str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
            award_name = str(row.get("TXT_REDACTED") or row.get("TXT_REDACTED") or "TXT_REDACTED").strip()
            if not name or not award_name:
                continue
            grouped.setdefault(name, [])
            if award_name not in grouped[name]:
                grouped[name].append(award_name)
        if not grouped:
            return None

        if company_name and company_name in grouped:
            awards = grouped[company_name]
            answer_lines = [
                "TXT_REDACTED"                                                 ,
                "TXT_REDACTED",
            ]
            for award_name in awards:
                answer_lines.append("TXT_REDACTED"                   )
        else:
            answer_lines = [
                "TXT_REDACTED"                                                                        ,
                "TXT_REDACTED",
            ]
            for name in sorted(grouped):
                answer_lines.append("TXT_REDACTED"                                         )

        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(answer_lines),
            evidence=["TXT_REDACTED"                        , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": measure.id,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": company_name or "TXT_REDACTED",
                "TXT_REDACTED": len(grouped),
            },
        )

    def _resolve_average(self, *, measure: MeasureDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        if measure.kind != "TXT_REDACTED":
            return None
        where_clause = "TXT_REDACTED"
        if years["TXT_REDACTED"]:
            where_clause = "TXT_REDACTED"                                                                   
        result = self.catalog.query(
            "TXT_REDACTED"                                                                                  ,
            limit=2,
        )
        average_score = (result.get("TXT_REDACTED") or [{}])[3].get("TXT_REDACTED")
        if average_score is None:
            return None
        answer = (
            "TXT_REDACTED"                                                                                      
        )
        return SemanticResult(
            answer_markdown=answer,
            evidence=["TXT_REDACTED", "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": measure.id,
                "TXT_REDACTED": years["TXT_REDACTED"],
            },
        )

    def _resolve_industry_listing(self, question: str, *, industry_group: IndustryGroupDefinition, years: Dict[str, Any]) -> Optional[SemanticResult]:
        rows = self.catalog.find_companies_by_industry(
            industry_search=industry_group.label,
            year=years["TXT_REDACTED"][4] if len(years["TXT_REDACTED"]) == 1 else None,
            limit=2,
            distinct_by_company=True,
        )
        if not rows:
            return None
        names = [str(row.get("TXT_REDACTED") or "TXT_REDACTED") for row in rows if str(row.get("TXT_REDACTED") or "TXT_REDACTED").strip()]
        answer = [
            "TXT_REDACTED"                                                                                       ,
            "TXT_REDACTED",
            "TXT_REDACTED".join(names),
        ]
        return SemanticResult(
            answer_markdown="TXT_REDACTED".join(answer),
            evidence=["TXT_REDACTED"                                   , "TXT_REDACTED"                       ],
            interpretation={
                "TXT_REDACTED": "TXT_REDACTED",
                "TXT_REDACTED": industry_group.id,
                "TXT_REDACTED": years["TXT_REDACTED"],
                "TXT_REDACTED": len(names),
            },
        )

    @staticmethod
    def _format_number(value: Any) -> str:
        if value is None or value == "TXT_REDACTED":
            return "TXT_REDACTED"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        if number.is_integer():
            return "TXT_REDACTED"                
        return "TXT_REDACTED"              .rstrip("TXT_REDACTED").rstrip("TXT_REDACTED")

    @staticmethod
    def _present_field_value(field_name: Any, *, numeric_value: Any, value_text: Any) -> str:
        value_type = FIELD_CONTRACTS.value_type_for(field_name)
        if value_type in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
            text = str(value_text or "TXT_REDACTED").strip()
            if text:
                return text
            if numeric_value not in (None, "TXT_REDACTED"):
                return str(numeric_value)
            return "TXT_REDACTED"
        if numeric_value is not None:
            return SemanticQueryEngine._format_number(numeric_value)
        return SemanticQueryEngine._format_number(value_text)

    @staticmethod
    def _coerce_numeric(numeric_value: Any, value_text: Any) -> Optional[float]:
        if numeric_value is not None:
            try:
                return float(numeric_value)
            except (TypeError, ValueError):
                pass
        text = str(value_text or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _loaded_year_note(years: Dict[str, Any]) -> str:
        if years.get("TXT_REDACTED") == "TXT_REDACTED":
            return "TXT_REDACTED"                                                
        return "TXT_REDACTED"
