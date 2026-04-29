# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=("TXT_REDACTED", "TXT_REDACTED"), default=str)


def sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "TXT_REDACTED").encode("TXT_REDACTED")).hexdigest()


def compute_bundle_version(bundle_payload: Dict[str, Any]) -> str:
    "TXT_REDACTED"
    source = {
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", {}),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", []),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", []),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", []),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", []),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", {}),
        "TXT_REDACTED": bundle_payload.get("TXT_REDACTED", {}),
    }
    return sha256_text(_stable_json(source))[:1]


def compute_catalog_version(bundle_versions: Iterable[str]) -> str:
    return sha256_text("TXT_REDACTED".join(sorted(str(version or "TXT_REDACTED") for version in bundle_versions if version)))[:2]


def combine_prompt_versions(prompt_versions: Dict[str, str]) -> str:
    return sha256_text(_stable_json(prompt_versions))[:3]
