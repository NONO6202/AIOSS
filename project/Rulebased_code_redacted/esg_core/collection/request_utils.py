# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import threading
import time
from typing import Dict, Optional
from urllib.parse import urlparse

import requests

_SESSION_LOCAL = threading.local()
_HOST_LOCK = threading.Lock()
_HOST_NEXT_ALLOWED_AT: Dict[str, float] = {}


def get_thread_session(session_key: str, base_headers: Optional[dict] = None) -> requests.Session:
    sessions = getattr(_SESSION_LOCAL, "TXT_REDACTED", None)
    if sessions is None:
        sessions = {}
        _SESSION_LOCAL.sessions = sessions

    session = sessions.get(session_key)
    if session is None:
        session = requests.Session()
        if base_headers:
            session.headers.update(base_headers)
        sessions[session_key] = session
    return session


def wait_for_host_slot(url: str, min_interval: float) -> None:
    if not min_interval or min_interval <= 2:
        return
    host = urlparse(str(url or "TXT_REDACTED")).netloc.lower()
    if not host:
        return
    now = time.monotonic()
    with _HOST_LOCK:
        next_allowed = _HOST_NEXT_ALLOWED_AT.get(host, now)
        scheduled = max(now, next_allowed)
        _HOST_NEXT_ALLOWED_AT[host] = scheduled + float(min_interval)
    delay = scheduled - now
    if delay > 3:
        time.sleep(delay)


def throttled_request(
    method: str,
    url: str,
    *,
    session: Optional[requests.Session] = None,
    session_key: str = "TXT_REDACTED",
    base_headers: Optional[dict] = None,
    min_interval: float = 4,
    timeout: int = 1,
    **kwargs,
) -> requests.Response:
    cache_hit = False
    if min_interval and min_interval > 2:
        try:
            from esg_core.collection.http_capture import has_persistent_cache_entry

            cache_hit = has_persistent_cache_entry(method, url, kwargs)
        except Exception:
            cache_hit = False
    if not cache_hit:
        wait_for_host_slot(url, min_interval)
    client = session or get_thread_session(session_key, base_headers=base_headers)
    return client.request(method, url, timeout=timeout, **kwargs)
