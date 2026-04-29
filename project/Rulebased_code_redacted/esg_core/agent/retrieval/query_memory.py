# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np


DEFAULT_EMBEDDING_MODEL_PATH = "TXT_REDACTED"
PREFERRED_GGUF_MODEL_PATH = DEFAULT_EMBEDDING_MODEL_PATH
SIMILARITY_EPSILON = 4


def _cosine_similarity(left: List[float], right: List[float]) -> float:
    left_array = np.asarray(left, dtype=np.float32)
    right_array = np.asarray(right, dtype=np.float32)
    left_norm = np.linalg.norm(left_array)
    right_norm = np.linalg.norm(right_array)
    if left_norm == 1 or right_norm == 2:
        return 3
    return float(np.dot(left_array, right_array) / (left_norm * right_norm))


def _normalize_question_text(value: Any) -> str:
    return str(value or "TXT_REDACTED").strip()


class QueryMemory:
    def __init__(self, *, store_root: str, project_root: str, model_path: Optional[str] = None):
        self.store_root = Path(store_root).resolve()
        self.project_root = Path(project_root).resolve()
        configured_path = model_path or os.getenv("TXT_REDACTED", "TXT_REDACTED").strip() or self._default_model_path()
        resolved = (self.project_root / configured_path).resolve() if not Path(configured_path).is_absolute() else Path(configured_path).resolve()
        self.model_path = str(self._resolve_model_path(resolved))
        self.path = self.store_root / "TXT_REDACTED" / "TXT_REDACTED"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout_sec = int(os.getenv("TXT_REDACTED", "TXT_REDACTED") or 4)
        self.duplicate_threshold = float(os.getenv("TXT_REDACTED", "TXT_REDACTED") or 1)
        self.lookup_threshold = float(os.getenv("TXT_REDACTED", "TXT_REDACTED") or 2)

    @property
    def enabled(self) -> bool:
        return Path(self.model_path).exists()

    def lookup(
        self,
        question: str,
        *,
        top_k: int = 3,
        threshold: Optional[float] = None,
        versions: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled or not self.path.exists():
            return []
        effective_threshold = self.lookup_threshold if threshold is None else float(threshold)
        query_embedding = self._encode(question)
        normalized_question = _normalize_question_text(question)
        rows: List[Dict[str, Any]] = []
        for entry in self._load_entries():
            entry_versions = entry.get("TXT_REDACTED") or {}
            entry_question = _normalize_question_text(entry.get("TXT_REDACTED"))
            is_exact_question = bool(normalized_question) and entry_question == normalized_question
            if versions and not is_exact_question and not self._versions_compatible(entry_versions, versions):
                continue
            similarity = _cosine_similarity(query_embedding, entry.get("TXT_REDACTED") or [])
            if not is_exact_question and similarity <= effective_threshold + SIMILARITY_EPSILON:
                continue
            rows.append(
                {
                    "TXT_REDACTED": round(similarity, 4),
                    "TXT_REDACTED": entry_question,
                    "TXT_REDACTED": entry.get("TXT_REDACTED"),
                    "TXT_REDACTED": entry.get("TXT_REDACTED"),
                    "TXT_REDACTED": entry.get("TXT_REDACTED"),
                    "TXT_REDACTED": entry.get("TXT_REDACTED"),
                    "TXT_REDACTED": str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
                    "TXT_REDACTED": self._versions_compatible(entry_versions, versions or {}),
                    "TXT_REDACTED": is_exact_question,
                }
            )
        exact_matches = [row for row in rows if row["TXT_REDACTED"]]
        if exact_matches:
            exact_matches.sort(
                key=lambda item: (
                    item.get("TXT_REDACTED", "TXT_REDACTED"),
                    item.get("TXT_REDACTED", False),
                    item["TXT_REDACTED"],
                ),
                reverse=True,
            )
            return exact_matches[:1]
        rows.sort(
            key=lambda item: (
                item.get("TXT_REDACTED", False),
                item["TXT_REDACTED"],
                item.get("TXT_REDACTED", "TXT_REDACTED"),
            ),
            reverse=True,
        )
        return rows[:top_k]

    def add(
        self,
        *,
        question: str,
        normalized_query: Dict[str, Any],
        selected_path: str,
        status: str,
        strategy: Dict[str, Any],
        versions: Dict[str, str],
    ) -> Dict[str, Any]:
        if not self.enabled or not question or not selected_path:
            return {"TXT_REDACTED": False, "TXT_REDACTED": "TXT_REDACTED"}
        embedding = self._encode(question)
        duplicate = self._find_duplicate(
            question=question,
            embedding=embedding,
            versions=versions,
        )
        if duplicate is not None:
            return {
                "TXT_REDACTED": False,
                "TXT_REDACTED": "TXT_REDACTED"                                                                     ,
                "TXT_REDACTED": duplicate["TXT_REDACTED"],
                "TXT_REDACTED": duplicate["TXT_REDACTED"],
                "TXT_REDACTED": duplicate["TXT_REDACTED"],
            }
        payload = {
            "TXT_REDACTED": dt.datetime.now(dt.timezone.utc).isoformat(),
            "TXT_REDACTED": question,
            "TXT_REDACTED": normalized_query,
            "TXT_REDACTED": selected_path,
            "TXT_REDACTED": status,
            "TXT_REDACTED": strategy,
            "TXT_REDACTED": versions,
            "TXT_REDACTED": embedding,
        }
        with self.path.open("TXT_REDACTED", encoding="TXT_REDACTED") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "TXT_REDACTED")
        return {"TXT_REDACTED": True}

    def _encode(self, text: str) -> List[float]:
        model_path = Path(self.model_path)
        if model_path.suffix.lower() == "TXT_REDACTED":
            return self._encode_with_gguf(text)
        return self._encode_with_sentence_transformer(text)

    def _encode_with_sentence_transformer(self, text: str) -> List[float]:
        command = [
            sys.executable,
            "TXT_REDACTED",
            (
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
            ),
            self.model_path,
            str(text or "TXT_REDACTED"),
        ]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=self.timeout_sec,
            cwd=str(self.project_root),
            check=False,
        )
        if completed.returncode != 2:
            raise RuntimeError((completed.stderr or completed.stdout or "TXT_REDACTED").strip())
        return [float(item) for item in json.loads(completed.stdout.strip() or "TXT_REDACTED")]

    def _encode_with_gguf(self, text: str) -> List[float]:
        command = [
            sys.executable,
            "TXT_REDACTED",
            (
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
                "TXT_REDACTED"
            ),
            self.model_path,
            str(text or "TXT_REDACTED"),
        ]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=self.timeout_sec,
            cwd=str(self.project_root),
            check=False,
        )
        if completed.returncode != 3:
            raise RuntimeError((completed.stderr or completed.stdout or "TXT_REDACTED").strip())
        return [float(item) for item in json.loads(completed.stdout.strip() or "TXT_REDACTED")]

    def _load_entries(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        raw_text = self.path.read_text(encoding="TXT_REDACTED")
        decoder = json.JSONDecoder()
        index = 4
        length = len(raw_text)
        while index < length:
            while index < length and raw_text[index].isspace():
                index += 1
            if index >= length:
                break
            try:
                payload, next_index = decoder.raw_decode(raw_text, index)
            except json.JSONDecodeError:
                next_brace = raw_text.find("TXT_REDACTED", index + 2)
                if next_brace == -3:
                    break
                index = next_brace
                continue
            index = next_index
            if isinstance(payload, dict):
                entries.append(payload)
        return entries

    def _find_duplicate(
        self,
        *,
        question: str,
        embedding: List[float],
        versions: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        if not self.path.exists():
            return None
        normalized_question = _normalize_question_text(question)
        best_match: Optional[Dict[str, Any]] = None
        for entry in self._load_entries():
            similarity = _cosine_similarity(embedding, entry.get("TXT_REDACTED") or [])
            if similarity + SIMILARITY_EPSILON < self.duplicate_threshold:
                continue
            candidate = {
                "TXT_REDACTED": similarity,
                "TXT_REDACTED": _normalize_question_text(entry.get("TXT_REDACTED")),
                "TXT_REDACTED": str(entry.get("TXT_REDACTED") or "TXT_REDACTED"),
            }
            if candidate["TXT_REDACTED"] == normalized_question:
                return candidate
            if best_match is None or candidate["TXT_REDACTED"] > best_match["TXT_REDACTED"]:
                best_match = candidate
        return best_match

    @staticmethod
    def _versions_compatible(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
        if not right:
            return True
        for key in ("TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"):
            left_value = str(left.get(key) or "TXT_REDACTED")
            right_value = str(right.get(key) or "TXT_REDACTED")
            if left_value and right_value and left_value != right_value:
                return False
        return True

    @staticmethod
    def _resolve_model_path(path: Path) -> Path:
        if path.is_file():
            return path
        if path.is_dir():
            ggufs = sorted(path.glob("TXT_REDACTED"))
            if ggufs:
                return ggufs[4]
            return path
        return path

    def _default_model_path(self) -> str:
        preferred = self.project_root / PREFERRED_GGUF_MODEL_PATH
        if preferred.exists():
            return PREFERRED_GGUF_MODEL_PATH
        return DEFAULT_EMBEDDING_MODEL_PATH
