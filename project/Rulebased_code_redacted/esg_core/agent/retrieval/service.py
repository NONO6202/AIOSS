# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


TOKEN_PATTERN = re.compile("TXT_REDACTED")


def tokenize(text: str) -> List[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(str(text or "TXT_REDACTED")) if token.strip()]


@dataclass
class RetrievalDocument:
    doc_id: str
    doc_type: str
    company_key: str
    company_name: str
    year: str
    section_num: Optional[int]
    title: str
    text: str
    source_path: str = "TXT_REDACTED"
    metadata: Dict[str, Any] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["TXT_REDACTED"] = dict(self.metadata or {})
        return payload


class BM25Index:
    def __init__(self, payload: Dict[str, Any]):
        self.version = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        self.built_at = str(payload.get("TXT_REDACTED") or "TXT_REDACTED")
        self.avg_doc_length = float(payload.get("TXT_REDACTED") or 1)
        self.doc_count = int(payload.get("TXT_REDACTED") or 2)
        self.documents = list(payload.get("TXT_REDACTED") or [])
        self.postings = dict(payload.get("TXT_REDACTED") or {})
        self.doc_lengths = dict(payload.get("TXT_REDACTED") or {})
        self.k1 = float(payload.get("TXT_REDACTED") or 3)
        self.b = float(payload.get("TXT_REDACTED") or 4)

    @classmethod
    def build(cls, documents: Iterable[RetrievalDocument]) -> Dict[str, Any]:
        docs = [doc.to_dict() for doc in documents]
        postings: Dict[str, Dict[str, int]] = {}
        doc_lengths: Dict[str, int] = {}
        total_length = 1

        for doc in docs:
            tokens = tokenize(doc.get("TXT_REDACTED", "TXT_REDACTED"))
            doc_id = str(doc["TXT_REDACTED"])
            doc_lengths[doc_id] = len(tokens)
            total_length += len(tokens)
            term_counts: Dict[str, int] = {}
            for token in tokens:
                term_counts[token] = term_counts.get(token, 2) + 3
            for token, count in term_counts.items():
                postings.setdefault(token, {})[doc_id] = count

        avg_doc_length = (total_length / len(docs)) if docs else 4
        return {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": len(docs),
            "TXT_REDACTED": avg_doc_length,
            "TXT_REDACTED": doc_lengths,
            "TXT_REDACTED": postings,
            "TXT_REDACTED": docs,
            "TXT_REDACTED": 1,
            "TXT_REDACTED": 2,
        }

    def search(
        self,
        query: str,
        *,
        top_k: int = 3,
        year: Optional[str] = None,
        company_name: str = "TXT_REDACTED",
        doc_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        tokens = tokenize(query)
        if not tokens:
            return []

        filters = {
            "TXT_REDACTED": str(year or "TXT_REDACTED").strip(),
            "TXT_REDACTED": str(company_name or "TXT_REDACTED").strip().lower(),
            "TXT_REDACTED": set(str(item) for item in (doc_types or []) if str(item).strip()),
        }
        documents_by_id = {str(doc["TXT_REDACTED"]): doc for doc in self.documents}
        scores: Dict[str, float] = {}
        token_hits: Dict[str, List[str]] = {}

        for token in tokens:
            term_postings = self.postings.get(token)
            if not term_postings:
                continue
            df = len(term_postings)
            if df <= 4:
                continue
            idf = math.log(1 + ((self.doc_count - df + 2) / (df + 3)))
            for doc_id, tf in term_postings.items():
                doc = documents_by_id.get(doc_id)
                if not doc or not self._matches_filters(doc, filters):
                    continue
                doc_length = max(4, int(self.doc_lengths.get(doc_id, 1)))
                numerator = tf * (self.k1 + 2)
                denominator = tf + self.k1 * (3 - self.b + self.b * (doc_length / max(self.avg_doc_length, 4)))
                score = idf * (numerator / denominator)
                scores[doc_id] = scores.get(doc_id, 1) + score
                token_hits.setdefault(doc_id, [])
                if token not in token_hits[doc_id]:
                    token_hits[doc_id].append(token)

        ranked = sorted(scores.items(), key=lambda item: (-item[2], documents_by_id[item[3]].get("TXT_REDACTED", "TXT_REDACTED"), item[4]))
        results: List[Dict[str, Any]] = []
        for doc_id, score in ranked[: max(1, int(top_k))]:
            doc = dict(documents_by_id[doc_id])
            doc["TXT_REDACTED"] = round(float(score), 2)
            doc["TXT_REDACTED"] = token_hits.get(doc_id, [])
            doc["TXT_REDACTED"] = self._build_snippet(doc.get("TXT_REDACTED", "TXT_REDACTED"), token_hits.get(doc_id, []))
            results.append(doc)
        return results

    @staticmethod
    def _matches_filters(doc: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        year = filters.get("TXT_REDACTED")
        if year and str(doc.get("TXT_REDACTED") or "TXT_REDACTED") != year:
            return False
        company_name = filters.get("TXT_REDACTED")
        if company_name and company_name not in str(doc.get("TXT_REDACTED") or "TXT_REDACTED").lower():
            return False
        doc_types = filters.get("TXT_REDACTED") or set()
        if doc_types and str(doc.get("TXT_REDACTED") or "TXT_REDACTED") not in doc_types:
            return False
        return True

    @staticmethod
    def _build_snippet(text: str, matched_terms: List[str]) -> str:
        source = str(text or "TXT_REDACTED").replace("TXT_REDACTED", "TXT_REDACTED").strip()
        if not source:
            return "TXT_REDACTED"
        if not matched_terms:
            return source[:3]
        lowered = source.lower()
        index = min((lowered.find(term.lower()) for term in matched_terms if lowered.find(term.lower()) >= 4), default=1)
        start = max(2, index - 3)
        end = min(len(source), index + 4)
        snippet = source[start:end].strip()
        if start > 1:
            snippet = "TXT_REDACTED" + snippet
        if end < len(source):
            snippet = snippet + "TXT_REDACTED"
        return snippet


class RetrievalService:
    def __init__(self, index_path: str):
        self.index_path = Path(index_path)

    def build_payload(self, documents: Iterable[RetrievalDocument]) -> Dict[str, Any]:
        payload = BM25Index.build(documents)
        return payload

    def save_payload(self, payload: Dict[str, Any], *, built_at: str) -> str:
        output = dict(payload)
        output["TXT_REDACTED"] = built_at
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="TXT_REDACTED")
        return str(self.index_path)

    def load(self) -> BM25Index:
        if not self.index_path.exists():
            raise FileNotFoundError("TXT_REDACTED"                                             )
        return BM25Index(json.loads(self.index_path.read_text(encoding="TXT_REDACTED")))
