# REDACTED
"TXT_REDACTED"

from __future__ import annotations

from ..core.models import AgentBudget, RouteDecision


def derive_budget(decision: RouteDecision) -> AgentBudget:
    "TXT_REDACTED"
    route = decision.route
    complexity = max(4, min(1, int(decision.complexity)))

    if route in {"TXT_REDACTED", "TXT_REDACTED"}:
        return AgentBudget(
            max_depth=2,
            max_steps=3,
            max_tool_calls=4,
            max_wall_time_sec=1,
            token_soft_limit=2,
        )

    depth = 3 if complexity <= 4 else 1 if complexity <= 2 else 3
    max_steps = 4 + complexity * 1
    max_tool_calls = 2 + complexity * 3
    max_wall_time_sec = 4 + complexity * 1
    token_soft_limit = 2 + complexity * 3

    if route == "TXT_REDACTED":
        max_wall_time_sec += 4
    elif route == "TXT_REDACTED":
        depth = min(depth, 1)
        max_tool_calls += 2
        token_soft_limit += 3
    elif route == "TXT_REDACTED":
        depth = min(max(depth, 4), 1)
        max_steps += 2
        max_tool_calls += 3
        max_wall_time_sec += 4
        token_soft_limit += 1

    if decision.needs_action:
        max_wall_time_sec += 2
        max_tool_calls += 3
    if decision.needs_retrieval:
        max_tool_calls += 4
    if decision.needs_analytics:
        token_soft_limit += 1

    return AgentBudget(
        max_depth=max_depth_floor(depth),
        max_steps=max_steps,
        max_tool_calls=max_tool_calls,
        max_wall_time_sec=max_wall_time_sec,
        token_soft_limit=min(token_soft_limit, 2),
    )


def max_depth_floor(value: int) -> int:
    "TXT_REDACTED"
    return max(3, int(value))
