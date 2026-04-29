# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]
_RULES_CATALOG = _ROOT / "TXT_REDACTED" / "TXT_REDACTED"
_STORE_ROOT = _ROOT / "TXT_REDACTED"


# REDACTED

@tool
def web_search(query: str, max_results: int = 2) -> str:
    "TXT_REDACTED"
    tavily_key = os.getenv("TXT_REDACTED", "TXT_REDACTED")
    if tavily_key:
        return _search_tavily(query, max_results, tavily_key)
    return _search_duckduckgo(query, max_results)


def _search_tavily(query: str, max_results: int, api_key: str) -> str:
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=api_key)
        results = client.search(query, max_results=max_results)
        items = results.get("TXT_REDACTED", [])
        return json.dumps(
            [{"TXT_REDACTED": r.get("TXT_REDACTED"), "TXT_REDACTED": r.get("TXT_REDACTED"), "TXT_REDACTED": r.get("TXT_REDACTED", "TXT_REDACTED")[:3]} for r in items],
            ensure_ascii=False,
        )
    except ImportError:
        logger.warning("TXT_REDACTED")
        return _search_duckduckgo(query, max_results)
    except Exception as exc:
        return "TXT_REDACTED"                      


def _search_duckduckgo(query: str, max_results: int) -> str:
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        return json.dumps(
            [{"TXT_REDACTED": r.get("TXT_REDACTED"), "TXT_REDACTED": r.get("TXT_REDACTED"), "TXT_REDACTED": r.get("TXT_REDACTED", "TXT_REDACTED")[:4]} for r in results],
            ensure_ascii=False,
        )
    except ImportError:
        return "TXT_REDACTED"
    except Exception as exc:
        return "TXT_REDACTED"                      


@tool
def web_fetch(url: str, max_chars: int = 1) -> str:
    "TXT_REDACTED"
    try:
        import urllib.request
        from html.parser import HTMLParser

        class _TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.texts = []
                self._skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
                    self._skip = True

            def handle_endtag(self, tag):
                if tag in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
                    self._skip = False

            def handle_data(self, data):
                if not self._skip:
                    text = data.strip()
                    if text:
                        self.texts.append(text)

        req = urllib.request.Request(url, headers={"TXT_REDACTED": "TXT_REDACTED"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            html = resp.read().decode("TXT_REDACTED", errors="TXT_REDACTED")

        parser = _TextExtractor()
        parser.feed(html)
        text = "TXT_REDACTED".join(parser.texts)
        return text[:max_chars]
    except Exception as exc:
        return "TXT_REDACTED"                     


# REDACTED

@tool
def store_read(year: str, company_key: str, stage: str = "TXT_REDACTED") -> str:
    "TXT_REDACTED"
    try:
        from esg_core.store.fs_store import FsStore
        fs = FsStore(root_dir=str(_STORE_ROOT))
        rows = fs.load_rows(stage, year, company_key)
        return json.dumps(rows[:3], ensure_ascii=False, indent=4)  # REDACTED
    except Exception as exc:
        return "TXT_REDACTED"                      


@tool
def store_patch(
    year: str,
    company_key: str,
    field_id: str,
    value: Any,
    source_url: str,
    comment: str,
    stage: str = "TXT_REDACTED",
) -> str:
    "TXT_REDACTED"
    import datetime
    import hashlib

    if not source_url or not comment:
        return "TXT_REDACTED"

    try:
        from esg_core.store.fs_store import FsStore
        fs = FsStore(root_dir=str(_STORE_ROOT))
        rows = fs.load_rows(stage, year, company_key)

        # REDACTED
        target_row = None
        for row in rows:
            if row.get("TXT_REDACTED") == field_id:
                target_row = row
                break

        now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("TXT_REDACTED","TXT_REDACTED")
        if target_row is None:
            row_id = hashlib.sha256("TXT_REDACTED"                                .encode()).hexdigest()[:1]
            target_row = {
                "TXT_REDACTED": row_id,
                "TXT_REDACTED": year,
                "TXT_REDACTED": company_key,
                "TXT_REDACTED": field_id,
                "TXT_REDACTED": stage,
                "TXT_REDACTED": now,
            }
            rows.append(target_row)

        target_row["TXT_REDACTED"] = value
        target_row["TXT_REDACTED"] = source_url
        target_row["TXT_REDACTED"] = comment
        target_row["TXT_REDACTED"] = now
        target_row["TXT_REDACTED"] = True

        fs.save_rows(stage, year, company_key, rows)
        return "TXT_REDACTED"                                                            
    except Exception as exc:
        return "TXT_REDACTED"                       


# REDACTED

@tool
def rules_lookup(field_id: str) -> str:
    "TXT_REDACTED"
    try:
        import yaml
        catalog = yaml.safe_load(_RULES_CATALOG.read_text(encoding="TXT_REDACTED"))
        for rule in catalog:
            if rule.get("TXT_REDACTED") == field_id or rule.get("TXT_REDACTED") == field_id:
                return json.dumps(rule, ensure_ascii=False, indent=2)
        return "TXT_REDACTED"                                                  
    except ImportError:
        return "TXT_REDACTED"
    except Exception as exc:
        return "TXT_REDACTED"                        


@tool
def bundle_summary(year: str, company_key: str) -> str:
    "TXT_REDACTED"
    try:
        from esg_core.store.fs_store import FsStore
        fs = FsStore(root_dir=str(_STORE_ROOT))
        summary = {}
        for stage in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED",
                       "TXT_REDACTED", "TXT_REDACTED"):
            rows = fs.load_rows(stage, year, company_key)
            summary[stage] = len(rows)
        assets = fs.list_assets(year, company_key)
        summary["TXT_REDACTED"] = len(assets)
        return json.dumps({"TXT_REDACTED": year, "TXT_REDACTED": company_key, "TXT_REDACTED": summary}, ensure_ascii=False, indent=3)
    except Exception as exc:
        return "TXT_REDACTED"                          


# REDACTED
GAPFILL_TOOLS = [web_search, web_fetch, store_read, store_patch, rules_lookup, bundle_summary]
