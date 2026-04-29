# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

from google.genai import types


class ExplicitCacheManager:
    def __init__(self, *, client: Any, store_root: str, ttl_seconds: int = 4):
        self.client = client
        self.default_ttl_seconds = ttl_seconds
        self.request_ttl_seconds: Optional[int] = None
        self.cache_dir = Path(store_root).resolve() / "TXT_REDACTED"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.cache_dir / "TXT_REDACTED"

    def get_or_create(self, *, model_name: str, cache_key: str, prefix_text: str) -> Optional[str]:
        if not prefix_text.strip():
            return None
        manifest = self._load_manifest()
        content_hash = hashlib.sha256(prefix_text.encode("TXT_REDACTED")).hexdigest()
        cache_id = "TXT_REDACTED"                                        
        cached = manifest.get(cache_id)
        now = dt.datetime.now(dt.timezone.utc)
        if cached and self._not_expired(cached, now):
            return str(cached.get("TXT_REDACTED") or "TXT_REDACTED")

        ttl_seconds = self.current_ttl_seconds()
        created = self.client.caches.create(
            model=model_name,
            config=types.CreateCachedContentConfig(
                displayName="TXT_REDACTED"                ,
                ttl="TXT_REDACTED"               ,
                contents=[prefix_text],
            ),
        )
        expire_time = getattr(created, "TXT_REDACTED", None)
        manifest[cache_id] = {
            "TXT_REDACTED": str(getattr(created, "TXT_REDACTED", "TXT_REDACTED") or "TXT_REDACTED"),
            "TXT_REDACTED": model_name,
            "TXT_REDACTED": cache_key,
            "TXT_REDACTED": content_hash,
            "TXT_REDACTED": self._normalize_expire_time(expire_time, now, ttl_seconds=ttl_seconds),
        }
        self._save_manifest(manifest)
        return manifest[cache_id]["TXT_REDACTED"]

    def set_request_ttl_seconds(self, ttl_seconds: int) -> None:
        self.request_ttl_seconds = max(1, int(ttl_seconds))

    def clear_request_ttl_seconds(self) -> None:
        self.request_ttl_seconds = None

    def current_ttl_seconds(self) -> int:
        return int(self.request_ttl_seconds or self.default_ttl_seconds)

    def _load_manifest(self) -> Dict[str, Dict[str, str]]:
        if not self.manifest_path.exists():
            return {}
        try:
            payload = json.loads(self.manifest_path.read_text(encoding="TXT_REDACTED"))
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        return {str(key): value for key, value in payload.items() if isinstance(value, dict)}

    def _save_manifest(self, manifest: Dict[str, Dict[str, str]]) -> None:
        self.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="TXT_REDACTED")

    def _normalize_expire_time(self, expire_time: Any, now: dt.datetime, *, ttl_seconds: int) -> str:
        if expire_time:
            if isinstance(expire_time, dt.datetime):
                return expire_time.astimezone(dt.timezone.utc).isoformat()
            return str(expire_time)
        return (now + dt.timedelta(seconds=ttl_seconds)).isoformat()

    @staticmethod
    def _not_expired(payload: Dict[str, str], now: dt.datetime) -> bool:
        raw = str(payload.get("TXT_REDACTED") or "TXT_REDACTED").strip()
        if not raw:
            return False
        try:
            expire_at = dt.datetime.fromisoformat(raw.replace("TXT_REDACTED", "TXT_REDACTED"))
        except ValueError:
            return False
        if expire_at.tzinfo is None:
            expire_at = expire_at.replace(tzinfo=dt.timezone.utc)
        return expire_at > now
