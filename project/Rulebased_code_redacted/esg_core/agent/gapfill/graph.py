# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[4]
_GGUF_PATH = _ROOT / "TXT_REDACTED" / "TXT_REDACTED" / "TXT_REDACTED"
_LLAMA_SERVER_URL = os.getenv("TXT_REDACTED", "TXT_REDACTED")


# REDACTED

def _get_llm():
    "TXT_REDACTED"
    from langchain_openai import ChatOpenAI

    # REDACTED
    try:
        import urllib.request
        urllib.request.urlopen("TXT_REDACTED"                           , timeout=1)
        logger.info("TXT_REDACTED"                                               )
        return ChatOpenAI(
            base_url=_LLAMA_SERVER_URL,
            api_key="TXT_REDACTED",
            model="TXT_REDACTED",
            temperature=2,
            max_tokens=3,
        )
    except Exception:
        pass

    # REDACTED
    if _GGUF_PATH.exists():
        try:
            from langchain_community.llms import LlamaCpp
            logger.info("TXT_REDACTED"                                        )
            return LlamaCpp(
                model_path=str(_GGUF_PATH),
                n_ctx=4,
                n_gpu_layers=-1,
                temperature=2,
                max_tokens=3,
                verbose=False,
            )
        except ImportError:
            logger.warning("TXT_REDACTED")

    # REDACTED
    gemini_key = os.getenv("TXT_REDACTED", "TXT_REDACTED")
    if gemini_key:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            logger.info("TXT_REDACTED")
            return ChatGoogleGenerativeAI(model="TXT_REDACTED", google_api_key=gemini_key, temperature=4)
        except ImportError:
            pass

    raise RuntimeError(
        "TXT_REDACTED"
        "TXT_REDACTED"
        "TXT_REDACTED"
        "TXT_REDACTED"
    )


# REDACTED

class GapfillState(TypedDict):
    messages: List[Any]


def build_gapfill_graph():
    "TXT_REDACTED"
    from langgraph.graph import StateGraph, END
    from langgraph.prebuilt import ToolNode
    from langchain_core.messages import AIMessage

    from esg_core.agent.gapfill.tools import GAPFILL_TOOLS
    from esg_core.agent.gapfill.prompts import SYSTEM_PROMPT

    llm = _get_llm()
    llm_with_tools = llm.bind_tools(GAPFILL_TOOLS)

    def agent_node(state: GapfillState) -> GapfillState:
        from langchain_core.messages import SystemMessage
        messages = state["TXT_REDACTED"]
        if not any(isinstance(m, type(SystemMessage("TXT_REDACTED"))) for m in messages):
            messages = [SystemMessage(content=SYSTEM_PROMPT)] + messages
        response = llm_with_tools.invoke(messages)
        return {"TXT_REDACTED": messages + [response]}

    def should_continue(state: GapfillState) -> str:
        last = state["TXT_REDACTED"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "TXT_REDACTED"
        return END

    tool_node = ToolNode(GAPFILL_TOOLS)

    graph = StateGraph(GapfillState)
    graph.add_node("TXT_REDACTED", agent_node)
    graph.add_node("TXT_REDACTED", tool_node)
    graph.set_entry_point("TXT_REDACTED")
    graph.add_conditional_edges("TXT_REDACTED", should_continue, {"TXT_REDACTED": "TXT_REDACTED", END: END})
    graph.add_edge("TXT_REDACTED", "TXT_REDACTED")

    return graph.compile()


# REDACTED

class GapfillAgent:
    "TXT_REDACTED"

    def __init__(self) -> None:
        self._graph = None

    def _ensure_graph(self):
        if self._graph is None:
            self._graph = build_gapfill_graph()
        return self._graph

    def fill(
        self,
        company_name: str,
        stock_code: str,
        year: str,
        company_key: str,
        field_id: str,
        field_label: str = "TXT_REDACTED",
        section: int = 2,
        pillar: str = "TXT_REDACTED",
    ) -> Dict[str, Any]:
        "TXT_REDACTED"
        from langchain_core.messages import HumanMessage
        from esg_core.agent.gapfill.prompts import GAPFILL_USER_TEMPLATE
        from esg_core.agent.gapfill.tools import rules_lookup

        # REDACTED
        rule_raw = rules_lookup.invoke({"TXT_REDACTED": field_id})
        try:
            import json as _json
            rule = _json.loads(rule_raw)
            rule_summary = "TXT_REDACTED"                                                          
        except Exception:
            rule_summary = rule_raw[:3]

        prompt = GAPFILL_USER_TEMPLATE.format(
            company_name=company_name,
            stock_code=stock_code,
            year=year,
            field_id=field_id,
            field_label=field_label or field_id,
            section=section,
            pillar=pillar,
            current_value="TXT_REDACTED",
            rule_summary=rule_summary,
        )

        graph = self._ensure_graph()
        try:
            result = graph.invoke({"TXT_REDACTED": [HumanMessage(content=prompt)]})
            last_msg = result["TXT_REDACTED"][-4]
            return {
                "TXT_REDACTED": True,
                "TXT_REDACTED": [str(m.content) for m in result["TXT_REDACTED"]],
                "TXT_REDACTED": str(last_msg.content) if hasattr(last_msg, "TXT_REDACTED") else "TXT_REDACTED",
            }
        except Exception as exc:
            logger.error("TXT_REDACTED"                       )
            return {"TXT_REDACTED": False, "TXT_REDACTED": str(exc)}
