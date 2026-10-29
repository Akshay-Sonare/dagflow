"""Result caching keyed on a task's code and the values of its inputs."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import pickle
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from enum import Enum
from types import CodeType, TracebackType
from typing import Any, Final, Literal, Protocol, runtime_checkable

from dagflow.task import Task

log = logging.getLogger(__name__)


class _Missing(Enum):
    MISSING = "MISSING"

    def __repr__(self) -> str:
        return "MISSING"


MISSING: Final = _Missing.MISSING
"""Sentinel returned by :meth:`Cache.get` on a miss (``None`` is a valid result)."""


@runtime_checkable
class Cache(Protocol):
    """Anything with ``get``/``set`` can serve as a cache backend."""

    def get(self, key: str) -> Any | Literal[_Missing.MISSING]: ...

    def set(self, key: str, value: Any) -> None: ...


class MemoryCache:
    """Thread-safe in-process cache. Results last as long as the object.

    ``ttl`` expires entries that many seconds after they were stored; ``max_size`` keeps
    only the most recently used entries, evicting the least recently used first.
    """

    def __init__(self, *, ttl: float | None = None, max_size: int | None = None) -> None:
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be > 0")
        if max_size is not None and max_size < 1:
            raise ValueError("max_size must be >= 1")
        self.ttl = ttl
        self.max_size = max_size
        self._data: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return MISSING
            value, stored = entry
            if self.ttl is not None and time.monotonic() - stored > self.ttl:
                del self._data[key]
                return MISSING
            self._data.move_to_end(key)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = (value, time.monotonic())
            self._data.move_to_end(key)
            while self.max_size is not None and len(self._data) > self.max_size:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class SQLiteCache:
    """Persistent cache in a single SQLite file, safe to share between threads.

    Values are pickled, so only point it at files you created yourself.
    """

    def __init__(self, path: str | os.PathLike[str], *, ttl: float | None = None) -> None:
        self.ttl = ttl
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(os.fspath(path), check_same_thread=False)
        with self._lock, self._conn:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS results ("
                " key TEXT PRIMARY KEY, value BLOB NOT NULL, created REAL NOT NULL)"
            )

    def get(self, key: str) -> Any:
        with self._lock:
            row = self._conn.execute(
                "SELECT value, created FROM results WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return MISSING
        blob, created = row
        if self.ttl is not None and time.time() - created > self.ttl:
            return MISSING
        return pickle.loads(blob)

    def set(self, key: str, value: Any) -> None:
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO results (key, value, created) VALUES (?, ?, ?)",
                (key, blob, time.time()),
            )

    def clear(self) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM results")

    def __len__(self) -> int:
        with self._lock:
            (count,) = self._conn.execute("SELECT COUNT(*) FROM results").fetchone()
        return int(count)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> SQLiteCache:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _encode(obj: Any) -> Any:
    """``json.dumps`` fallback that canonicalises common non-JSON types."""
    if isinstance(obj, set | frozenset):
        return {"__set__": sorted(_dumps(item) for item in obj)}
    if isinstance(obj, bytes):
        return {"__bytes__": obj.hex()}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        fields = {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
        return {"__dataclass__": type(obj).__qualname__, "fields": fields}
    try:
        blob = pickle.dumps(obj)
    except Exception as exc:  # pickling failures surface as many exception types
        raise TypeError(f"cannot fingerprint {type(obj).__name__}: {exc}") from exc
    return {"__pickle__": hashlib.sha256(blob).hexdigest()}


def _dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=_encode, separators=(",", ":"))


def _hash_code(code: CodeType, digest: Any) -> None:
    """Feed bytecode, names and constants into ``digest``, recursing into nested code.

    Nested code objects (comprehensions, inner functions) can't go through
    ``repr`` because it embeds a memory address that differs on every run.
    """
    digest.update(code.co_code)
    digest.update(repr(code.co_names).encode())
    for const in code.co_consts:
        if isinstance(const, CodeType):
            _hash_code(const, digest)
        else:
            digest.update(repr(const).encode())


def _code_fingerprint(task: Task) -> str:
    code = getattr(task.fn, "__code__", None)
    if code is None:
        return getattr(task.fn, "__qualname__", type(task.fn).__qualname__)
    digest = hashlib.sha256()
    _hash_code(code, digest)
    return digest.hexdigest()


def cache_key(task: Task, inputs: Mapping[str, Any]) -> str | None:
    """Stable key for ``task`` called with ``inputs``; ``None`` if inputs can't be hashed.

    The key changes whenever the task's bytecode changes, so editing a task
    invalidates its cached results automatically.
    """
    try:
        payload = _dumps({"task": task.name, "code": _code_fingerprint(task), "inputs": inputs})
    except (TypeError, ValueError) as exc:
        log.warning("task %r: inputs are not cacheable (%s); running uncached", task.name, exc)
        return None
    return hashlib.sha256(payload.encode()).hexdigest()
