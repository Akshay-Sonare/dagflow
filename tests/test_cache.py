from dataclasses import dataclass
from pathlib import Path

import pytest

from dagflow import MISSING, Cache, MemoryCache, SQLiteCache, Workflow, cache_key, task


@task
def add(a: int, b: int) -> int:
    return a + b


@dataclass
class Point:
    x: int
    y: int


def test_backends_satisfy_the_protocol(tmp_path: Path) -> None:
    assert isinstance(MemoryCache(), Cache)
    with SQLiteCache(tmp_path / "c.db") as cache:
        assert isinstance(cache, Cache)


def test_memory_cache_roundtrip() -> None:
    cache = MemoryCache()
    assert cache.get("k") is MISSING
    cache.set("k", None)
    assert cache.get("k") is None


def test_sqlite_cache_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "results.db"
    with SQLiteCache(path) as cache:
        cache.set("k", {"rows": [1, 2, 3]})
    with SQLiteCache(path) as cache:
        assert cache.get("k") == {"rows": [1, 2, 3]}
        assert len(cache) == 1
        cache.clear()
        assert cache.get("k") is MISSING


def test_sqlite_cache_ttl(tmp_path: Path) -> None:
    with SQLiteCache(tmp_path / "c.db", ttl=-1) as cache:
        cache.set("k", 1)
        assert cache.get("k") is MISSING


def test_workflow_uses_sqlite_cache_between_runs(tmp_path: Path) -> None:
    calls = 0
    wf = Workflow()

    @wf.task(cache=True)
    def expensive() -> str:
        nonlocal calls
        calls += 1
        return "result"

    for _ in range(3):
        with SQLiteCache(tmp_path / "c.db") as cache:
            assert wf.run(cache=cache)["expensive"] == "result"
    assert calls == 1


def test_key_is_stable_and_order_independent() -> None:
    assert cache_key(add, {"a": 1, "b": 2}) == cache_key(add, {"b": 2, "a": 1})
    assert cache_key(add, {"a": 1, "b": 2}) != cache_key(add, {"a": 2, "b": 1})


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ({1, 2, 3}, {3, 2, 1}),
        (frozenset("ab"), frozenset("ba")),
        (Point(1, 2), Point(1, 2)),
        (b"\x00\x01", b"\x00\x01"),
    ],
)
def test_key_canonicalises_values(first: object, second: object) -> None:
    assert cache_key(add, {"a": first}) == cache_key(add, {"a": second})


def test_key_changes_when_code_changes() -> None:
    @task(name="f")
    def v1() -> int:
        return 1

    @task(name="f")
    def v2() -> int:
        return 2

    assert cache_key(v1, {}) != cache_key(v2, {})


def test_key_is_stable_across_separately_compiled_code() -> None:
    # Nested code objects (here a comprehension) must not leak memory addresses
    # into the key, or results cached by one process are never hit by the next.
    source = "def f(xs):\n    return [x * 2 for x in xs if (lambda: x)()]\n"
    keys = set()
    for _ in range(2):
        namespace: dict[str, object] = {}
        exec(compile(source, "<test>", "exec"), namespace)
        keys.add(cache_key(task(namespace["f"]), {"xs": [1]}))  # type: ignore[call-overload]
    assert len(keys) == 1


def test_unhashable_inputs_disable_caching() -> None:
    assert cache_key(add, {"a": lambda: None}) is None
