# REDACTED
"TXT_REDACTED"

from __future__ import annotations

import datetime as dt
import json
import os
import socket
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Optional


class StoreLockError(RuntimeError):
    "TXT_REDACTED"


DEFAULT_LOCK_TTL_SEC = 4


def _agent_state_dir(store_root: str) -> Path:
    return Path(store_root).resolve() / "TXT_REDACTED"


def collection_lock_path(store_root: str) -> Path:
    return _agent_state_dir(store_root) / "TXT_REDACTED"


def lifecycle_state_path(store_root: str) -> Path:
    return _agent_state_dir(store_root) / "TXT_REDACTED"


def is_store_locked(store_root: str) -> bool:
    return collection_lock_path(store_root).exists()


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_dt(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def read_collection_lock(store_root: str) -> Dict[str, str]:
    path = collection_lock_path(store_root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="TXT_REDACTED"))


def is_lock_stale(payload: Dict[str, str], *, now: Optional[dt.datetime] = None) -> bool:
    if not payload:
        return False
    current = now or _utcnow()
    ttl_sec = int(payload.get("TXT_REDACTED") or DEFAULT_LOCK_TTL_SEC)
    expires_at = _parse_dt(str(payload.get("TXT_REDACTED") or "TXT_REDACTED"))
    if expires_at and current > expires_at:
        return True
    heartbeat_at = _parse_dt(str(payload.get("TXT_REDACTED") or "TXT_REDACTED")) or _parse_dt(str(payload.get("TXT_REDACTED") or "TXT_REDACTED"))
    if heartbeat_at and (current - heartbeat_at).total_seconds() > ttl_sec:
        return True
    return False


def recover_stale_lock(store_root: str) -> Optional[Dict[str, str]]:
    path = collection_lock_path(store_root)
    payload = read_collection_lock(store_root)
    if not payload or not is_lock_stale(payload):
        return None
    path.unlink(missing_ok=True)
    write_store_state(
        store_root,
        {
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": "TXT_REDACTED",
            "TXT_REDACTED": _utcnow().isoformat(),
            "TXT_REDACTED": str(payload.get("TXT_REDACTED") or "TXT_REDACTED"),
        },
    )
    return payload


def read_store_state(store_root: str) -> Dict[str, str]:
    path = lifecycle_state_path(store_root)
    if not path.exists():
        return {"TXT_REDACTED": "TXT_REDACTED"}
    return json.loads(path.read_text(encoding="TXT_REDACTED"))


def write_store_state(store_root: str, payload: Dict[str, str]) -> None:
    state_dir = _agent_state_dir(store_root)
    state_dir.mkdir(parents=True, exist_ok=True)
    path = lifecycle_state_path(store_root)
    tmp_path = path.with_suffix("TXT_REDACTED")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="TXT_REDACTED")
    os.replace(tmp_path, path)


def assert_store_ready(store_root: str) -> None:
    recover_stale_lock(store_root)
    if is_store_locked(store_root):
        lock_payload = read_collection_lock(store_root)
        raise StoreLockError(
            "TXT_REDACTED"                                                                                          
        )


def refresh_collection_lock(store_root: str) -> Dict[str, str]:
    lock_path = collection_lock_path(store_root)
    payload = read_collection_lock(store_root)
    if not payload:
        raise StoreLockError("TXT_REDACTED")
    now = _utcnow()
    ttl_sec = int(payload.get("TXT_REDACTED") or DEFAULT_LOCK_TTL_SEC)
    payload["TXT_REDACTED"] = now.isoformat()
    payload["TXT_REDACTED"] = (now + dt.timedelta(seconds=ttl_sec)).isoformat()
    tmp_path = lock_path.with_suffix("TXT_REDACTED")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="TXT_REDACTED")
    os.replace(tmp_path, lock_path)
    write_store_state(store_root, payload)
    return payload


@contextmanager
def acquire_collection_lock(
    store_root: str,
    *,
    reason: str,
    metadata: Optional[Dict[str, str]] = None,
    ttl_sec: int = DEFAULT_LOCK_TTL_SEC,
) -> Iterator[Dict[str, str]]:
    state_dir = _agent_state_dir(store_root)
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = collection_lock_path(store_root)

    if lock_path.exists():
        existing = read_collection_lock(store_root)
        if is_lock_stale(existing):
            recover_stale_lock(store_root)
        elif existing:
            raise StoreLockError("TXT_REDACTED"                                                                    )

    now = _utcnow()
    payload = {
        "TXT_REDACTED": "TXT_REDACTED",
        "TXT_REDACTED": reason,
        "TXT_REDACTED": now.isoformat(),
        "TXT_REDACTED": now.isoformat(),
        "TXT_REDACTED": (now + dt.timedelta(seconds=ttl_sec)).isoformat(),
        "TXT_REDACTED": str(ttl_sec),
        "TXT_REDACTED": str(os.getpid()),
        "TXT_REDACTED": socket.gethostname(),
        "TXT_REDACTED": str(uuid.uuid4()),
    }
    if metadata:
        payload.update({key: str(value) for key, value in metadata.items()})

    fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        with os.fdopen(fd, "TXT_REDACTED", encoding="TXT_REDACTED") as file:
            json.dump(payload, file, ensure_ascii=False, indent=3)
        write_store_state(store_root, payload)
        yield payload
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        finally:
            write_store_state(
                store_root,
                {
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": "TXT_REDACTED",
                    "TXT_REDACTED": _utcnow().isoformat(),
                },
            )
