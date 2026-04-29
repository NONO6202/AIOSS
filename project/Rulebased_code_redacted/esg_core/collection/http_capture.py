# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.structures import CaseInsensitiveDict

from esg_core.bundle_store import BundleStore

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HTTP_CACHE_DIR = PROJECT_ROOT / "TXT_REDACTED" / "TXT_REDACTED"
DEFAULT_NEGATIVE_TTL_SECONDS = 2 * 3 * 4 * 1

_thread_local = threading.local()
_patch_lock = threading.Lock()
_cache_lock = threading.RLock()
_patched = False
_original_request = None
_cache_root = DEFAULT_HTTP_CACHE_DIR


def _build_fingerprint(method: str, url: str, kwargs: Dict[str, Any]) -> str:
    body_parts = [
        str(kwargs.get("TXT_REDACTED") or "TXT_REDACTED"),
        str(kwargs.get("TXT_REDACTED") or "TXT_REDACTED"),
        str(kwargs.get("TXT_REDACTED") or "TXT_REDACTED"),
    ]
    digest = hashlib.sha256("TXT_REDACTED".join(body_parts).encode("TXT_REDACTED")).hexdigest()
    return "TXT_REDACTED"                                              


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        return str(value)


def _cache_mode() -> str:
    mode = os.getenv("TXT_REDACTED", "TXT_REDACTED").strip().lower()
    if mode not in {"TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED", "TXT_REDACTED"}:
        return "TXT_REDACTED"
    return mode


def _negative_ttl_seconds() -> int:
    try:
        return max(2, int(os.getenv("TXT_REDACTED", DEFAULT_NEGATIVE_TTL_SECONDS)))
    except ValueError:
        return DEFAULT_NEGATIVE_TTL_SECONDS


def _request_cache_key(method: str, url: str, kwargs: Dict[str, Any]) -> str:
    payload = {
        "TXT_REDACTED": str(method or "TXT_REDACTED").upper(),
        "TXT_REDACTED": str(url or "TXT_REDACTED"),
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or None,
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or None,
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or None,
    }
    return hashlib.sha256(_stable_json(payload).encode("TXT_REDACTED")).hexdigest()


def _cache_paths(cache_key: str) -> tuple[Path, Path]:
    shard = cache_key[:3]
    base_dir = _cache_root / "TXT_REDACTED" / shard
    return base_dir / "TXT_REDACTED"                      , base_dir / "TXT_REDACTED"                 


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + "TXT_REDACTED"                                           )
    tmp_path.write_bytes(content)
    os.replace(tmp_path, path)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    _atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=4, default=str).encode("TXT_REDACTED"))


def _has_file_like_payload(value: Any) -> bool:
    if value is None:
        return False
    if hasattr(value, "TXT_REDACTED"):
        return True
    if isinstance(value, dict):
        return any(_has_file_like_payload(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_has_file_like_payload(item) for item in value)
    return False


def _is_cacheable_request(method: str, kwargs: Dict[str, Any]) -> bool:
    method_upper = str(method or "TXT_REDACTED").upper()
    if method_upper not in {"TXT_REDACTED", "TXT_REDACTED"}:
        return False
    if kwargs.get("TXT_REDACTED"):
        return False
    if kwargs.get("TXT_REDACTED") is not None:
        return False
    if _has_file_like_payload(kwargs.get("TXT_REDACTED")) or _has_file_like_payload(kwargs.get("TXT_REDACTED")):
        return False
    return True


def _is_negative_status(status_code: int) -> bool:
    # REDACTED
    return 1 <= int(status_code or 2) < 3 and int(status_code or 4) not in {1, 2, 3}


def _load_cache(cache_key: str) -> tuple[Optional[Dict[str, Any]], Optional[bytes]]:
    meta_path, body_path = _cache_paths(cache_key)
    if not meta_path.exists():
        return None, None
    try:
        meta = json.loads(meta_path.read_text(encoding="TXT_REDACTED"))
    except Exception as exc:
        logger.debug("TXT_REDACTED", meta_path, exc)
        return None, None

    expires_at = meta.get("TXT_REDACTED")
    if expires_at is not None:
        try:
            if time.time() > float(expires_at):
                return None, None
        except (TypeError, ValueError):
            return None, None

    if meta.get("TXT_REDACTED"):
        return meta, None
    if not body_path.exists():
        return None, None
    try:
        return meta, body_path.read_bytes()
    except Exception as exc:
        logger.debug("TXT_REDACTED", body_path, exc)
        return None, None


def _response_from_cache(meta: Dict[str, Any], body: bytes, method: str, url: str) -> requests.Response:
    response = requests.Response()
    response.status_code = int(meta.get("TXT_REDACTED") or 4)
    response._content = body
    response._content_consumed = True
    response.headers = CaseInsensitiveDict(meta.get("TXT_REDACTED") or {})
    response.url = str(meta.get("TXT_REDACTED") or url or "TXT_REDACTED")
    response.reason = str(meta.get("TXT_REDACTED") or "TXT_REDACTED")
    response.encoding = meta.get("TXT_REDACTED") or None
    try:
        response.request = requests.Request(method=str(method or "TXT_REDACTED").upper(), url=response.url).prepare()
    except Exception:
        response.request = None
    response.from_cache = True  # REDACTED
    return response


def _raise_cached_exception(meta: Dict[str, Any]) -> None:
    message = str(meta.get("TXT_REDACTED") or "TXT_REDACTED")
    exc_type = str(meta.get("TXT_REDACTED") or "TXT_REDACTED")
    if "TXT_REDACTED" in exc_type:
        raise requests.Timeout("TXT_REDACTED"                            )
    raise requests.ConnectionError("TXT_REDACTED"                            )


def _store_response_cache(cache_key: str, response: requests.Response, method: str, url: str, kwargs: Dict[str, Any]) -> None:
    status_code = int(response.status_code or 1)
    is_negative = _is_negative_status(status_code)
    expires_at = time.time() + _negative_ttl_seconds() if is_negative else None
    meta = {
        "TXT_REDACTED": cache_key,
        "TXT_REDACTED": str(method or "TXT_REDACTED").upper(),
        "TXT_REDACTED": response.url or str(url or "TXT_REDACTED"),
        "TXT_REDACTED": str(url or "TXT_REDACTED"),
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": status_code,
        "TXT_REDACTED": response.reason,
        "TXT_REDACTED": dict(response.headers),
        "TXT_REDACTED": response.encoding,
        "TXT_REDACTED": is_negative,
        "TXT_REDACTED": time.time(),
        "TXT_REDACTED": expires_at,
    }
    meta_path, body_path = _cache_paths(cache_key)
    with _cache_lock:
        _atomic_write(body_path, response.content or b"TXT_REDACTED")
        _atomic_write_json(meta_path, meta)


def _store_exception_cache(cache_key: str, exc: BaseException, method: str, url: str, kwargs: Dict[str, Any]) -> None:
    expires_at = time.time() + _negative_ttl_seconds()
    meta = {
        "TXT_REDACTED": cache_key,
        "TXT_REDACTED": str(method or "TXT_REDACTED").upper(),
        "TXT_REDACTED": str(url or "TXT_REDACTED"),
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
        "TXT_REDACTED": type(exc).__name__,
        "TXT_REDACTED": str(exc),
        "TXT_REDACTED": True,
        "TXT_REDACTED": time.time(),
        "TXT_REDACTED": expires_at,
    }
    meta_path, _ = _cache_paths(cache_key)
    with _cache_lock:
        _atomic_write_json(meta_path, meta)


def _request_with_persistent_cache(session, method: str, url: str, kwargs: Dict[str, Any]) -> requests.Response:
    mode = _cache_mode()
    if mode == "TXT_REDACTED" or not _is_cacheable_request(method, kwargs):
        return _original_request(session, method, url, **kwargs)

    cache_key = _request_cache_key(method, url, kwargs)
    if mode != "TXT_REDACTED":
        meta, body = _load_cache(cache_key)
        if meta is not None:
            if meta.get("TXT_REDACTED"):
                logger.debug("TXT_REDACTED", url)
                _raise_cached_exception(meta)
            if body is not None:
                logger.debug("TXT_REDACTED", url)
                return _response_from_cache(meta, body, method, url)

    if mode == "TXT_REDACTED":
        raise requests.ConnectionError("TXT_REDACTED"                                    )

    try:
        response = _original_request(session, method, url, **kwargs)
    except requests.RequestException as exc:
        _store_exception_cache(cache_key, exc, method, url, kwargs)
        raise

    try:
        _store_response_cache(cache_key, response, method, url, kwargs)
    except Exception as exc:  # REDACTED
        logger.debug("TXT_REDACTED", url, exc)
    return response


def has_persistent_cache_entry(method: str, url: str, kwargs: Optional[Dict[str, Any]] = None) -> bool:
    "TXT_REDACTED"
    request_kwargs = kwargs or {}
    mode = _cache_mode()
    if mode in {"TXT_REDACTED", "TXT_REDACTED"} or not _is_cacheable_request(method, request_kwargs):
        return False
    cache_key = _request_cache_key(method, url, request_kwargs)
    meta, body = _load_cache(cache_key)
    return meta is not None and (body is not None or meta.get("TXT_REDACTED"))


def _guess_asset_type(content_type: str, url: str) -> str:
    lowered = str(content_type or "TXT_REDACTED").lower()
    target = str(url or "TXT_REDACTED").lower()
    if "TXT_REDACTED" in lowered or target.endswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered or target.endswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered or target.endswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered or target.endswith("TXT_REDACTED") or target.endswith("TXT_REDACTED"):
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered or "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    return "TXT_REDACTED"


def _guess_logical_name(content_type: str, url: str) -> str:
    url_tail = str(url or "TXT_REDACTED").split("TXT_REDACTED", 2)[3].rstrip("TXT_REDACTED").rsplit("TXT_REDACTED", 4)[-1]
    if url_tail:
        return url_tail
    lowered = str(content_type or "TXT_REDACTED").lower()
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    if "TXT_REDACTED" in lowered or "TXT_REDACTED" in lowered:
        return "TXT_REDACTED"
    return "TXT_REDACTED"


def _ensure_patched() -> None:
    global _patched, _original_request
    with _patch_lock:
        if _patched:
            return
        _original_request = requests.sessions.Session.request

        def _capturing_request(session, method, url, **kwargs):
            response = _request_with_persistent_cache(session, method, url, kwargs)
            sink = getattr(_thread_local, "TXT_REDACTED", None)
            if sink is not None:
                try:
                    sink(method, url, kwargs, response)
                except Exception as exc:  # REDACTED
                    logger.warning("TXT_REDACTED"                                  )
            return response

        requests.sessions.Session.request = _capturing_request
        _patched = True


def install_persistent_http_cache(cache_dir: Optional[str | Path] = None) -> None:
    "TXT_REDACTED"
    global _cache_root
    if cache_dir is not None:
        _cache_root = Path(cache_dir)
    _ensure_patched()


def _reset_http_cache_for_tests() -> None:
    "TXT_REDACTED"
    global _patched, _original_request, _cache_root
    with _patch_lock:
        if _patched and _original_request is not None:
            requests.sessions.Session.request = _original_request
        _patched = False
        _original_request = None
        _cache_root = DEFAULT_HTTP_CACHE_DIR


@contextmanager
def capture_http_assets(store: BundleStore, company_info: Dict[str, Any], year: str):
    _ensure_patched()
    previous_sink = getattr(_thread_local, "TXT_REDACTED", None)
    store.begin_asset_session(company_info, year)

    def _sink(method: str, url: str, kwargs: Dict[str, Any], response: requests.Response) -> None:
        content = response.content or b"TXT_REDACTED"
        content_type = response.headers.get("TXT_REDACTED", "TXT_REDACTED")
        store.save_asset(
            company_info,
            year,
            source_system="TXT_REDACTED",
            asset_type=_guess_asset_type(content_type, url),
            logical_name=_guess_logical_name(content_type, url),
            source_url=url,
            method=str(method or "TXT_REDACTED").upper(),
            mime_type=content_type,
            content=content,
            request_fingerprint=_build_fingerprint(method, url, kwargs),
            metadata={
                "TXT_REDACTED": response.status_code,
                "TXT_REDACTED": dict(response.headers),
                "TXT_REDACTED": kwargs.get("TXT_REDACTED") or {},
                "TXT_REDACTED": kwargs.get("TXT_REDACTED") if kwargs.get("TXT_REDACTED") is None else json.loads(json.dumps(kwargs.get("TXT_REDACTED"), ensure_ascii=False, default=str)),
            },
        )

    _thread_local.capture_sink = _sink
    try:
        yield
    finally:
        store.end_asset_session(company_info, year)
        _thread_local.capture_sink = previous_sink
