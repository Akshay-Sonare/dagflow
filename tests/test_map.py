"""Dynamic fan-out: ``@task(map="param")`` runs a task once per item of an input."""

import asyncio
import time
from typing import Any

import pytest

from dagflow import (
    Context,
    Event,
    EventType,
    MapError,
    MemoryCache,
    Skip,
    TaskState,
    Workflow,
    task,
)


def numbers_workflow(count: int = 5, delay: float = 0.0) -> Workflow:
    wf = Workflow("mapped")

    @wf.task
    def numbers() -> list[int]:
        return list(range(count))

    @wf.task(map="numbers")
    async def square(numbers: int) -> int:
        await asyncio.sleep(delay)
        return numbers * numbers

    @wf.task
    def total(square: list[int]) -> int:
        return sum(square)

    return wf


def test_results_are_gathered_in_input_order() -> None:
    result = numbers_workflow().run()
    assert result["square"] == [0, 1, 4, 9, 16]
    assert result["total"] == 30
    items = result.results["square"].items
    assert items is not None
    assert [i.name for i in items] == [f"square[{n}]" for n in range(5)]
    assert all(i.state is TaskState.SUCCESS for i in items)


def test_items_run_concurrently() -> None:
    start = time.perf_counter()
    numbers_workflow(count=8, delay=0.15).run(max_concurrency=8)
    assert time.perf_counter() - start < 0.5  # 8 x 0.15s one after another would be 1.2s


def test_items_share_the_concurrency_limit() -> None:
    wf = Workflow()
    active = peak = 0

    @wf.task(map="xs")
    async def work(xs: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return xs

    wf.run({"xs": list(range(12))}, max_concurrency=3)
    assert peak == 3


def test_items_respect_the_task_pool() -> None:
    wf = Workflow()
    active = peak = 0

    @wf.task(map="xs", pool="api")
    async def call(xs: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return xs

    wf.run({"xs": list(range(10))}, max_concurrency=8, pools={"api": 2})
    assert peak == 2


def test_map_over_a_run_param_and_a_default() -> None:
    wf = Workflow()

    @wf.task(map="names")
    def greet(names: str, greeting: str = "hi") -> str:
        return f"{greeting} {names}"

    assert wf.run({"names": ["ada", "bo"]})["greet"] == ["hi ada", "hi bo"]

    wf2 = Workflow()

    @wf2.task(map="xs")
    def double(xs: int = (1, 2)) -> int:  # type: ignore[assignment]
        return xs * 2

    assert wf2.run()["double"] == [2, 4]


def test_other_inputs_are_passed_whole_to_every_item() -> None:
    wf = Workflow()

    @wf.task
    def rates() -> dict[str, float]:
        return {"EUR": 1.0, "USD": 0.5}

    @wf.task(map="amounts")
    def convert(amounts: tuple[float, str], rates: dict[str, float]) -> float:
        value, currency = amounts
        return value * rates[currency]

    result = wf.run({"amounts": [(10, "EUR"), (10, "USD")]})
    assert result["convert"] == [10.0, 5.0]


def test_empty_input_gives_an_empty_list_and_downstream_still_runs() -> None:
    result = numbers_workflow(count=0).run()
    assert result["square"] == []
    assert result["total"] == 0
    assert result.results["square"].items == []


@pytest.mark.parametrize("bad", ["abc", b"abc", 42, None])
def test_non_list_input_fails_clearly(bad: object) -> None:
    wf = Workflow()

    @wf.task(map="xs")
    def each(xs: Any) -> Any:
        return xs

    result = wf.run({"xs": bad})
    failed = result.results["each"]
    assert failed.state is TaskState.FAILED
    assert isinstance(failed.error, TypeError)
    assert "must be a list or other iterable" in str(failed.error)


def test_each_item_retries_on_its_own_and_sees_its_index() -> None:
    wf = Workflow()
    seen: list[tuple[int | None, int]] = []

    @wf.task(map="xs", retries=2)
    def flaky(xs: int, ctx: Context) -> int:
        seen.append((ctx.index, ctx.attempt))
        if xs == 2 and ctx.attempt < 3:
            raise ConnectionError("blip")
        return xs

    result = wf.run({"xs": [0, 1, 2, 3]})
    assert result["flaky"] == [0, 1, 2, 3]
    items = result.results["flaky"].items
    assert items is not None
    assert [i.attempts for i in items] == [1, 1, 3, 1]
    assert result.results["flaky"].attempts == 6
    assert sorted(seen) == [(0, 1), (1, 1), (2, 1), (2, 2), (2, 3), (3, 1)]


def test_failed_items_fail_the_task_and_skip_downstream() -> None:
    wf = Workflow()
    ran: list[int] = []

    @wf.task(map="xs")
    def picky(xs: int) -> int:
        ran.append(xs)
        if xs % 3 == 0:
            raise ValueError(f"bad {xs}")
        return xs

    @wf.task
    def after(picky: list[int]) -> int:
        return len(picky)

    result = wf.run({"xs": list(range(7))})
    assert sorted(ran) == list(range(7))  # one failure doesn't stop the other items
    failed = result.results["picky"]
    assert failed.state is TaskState.FAILED
    assert isinstance(failed.error, MapError)
    assert sorted(failed.error.failures) == [0, 3, 6]
    assert "3 of 7 items of 'picky' failed" in str(failed.error)
    assert result.results["after"].state is TaskState.SKIPPED
    assert not result.ok


def test_map_error_message_is_truncated() -> None:
    error = MapError("t", 10, {i: ValueError(str(i)) for i in range(5)})
    assert str(error).endswith("(+2 more)")


def test_skipped_items_are_dropped_from_the_result() -> None:
    wf = Workflow()

    @wf.task(map="xs")
    def evens(xs: int) -> int:
        if xs % 2:
            raise Skip("odd")
        return xs

    result = wf.run({"xs": list(range(6))})
    assert result.ok
    assert result["evens"] == [0, 2, 4]
    items = result.results["evens"].items
    assert items is not None
    assert [i.state for i in items].count(TaskState.SKIPPED) == 3


def test_each_item_is_cached_separately() -> None:
    wf = Workflow()
    calls: list[int] = []

    @wf.task(map="xs", cache=True)
    def slow_square(xs: int) -> int:
        calls.append(xs)
        return xs * xs

    cache = MemoryCache()
    wf.run({"xs": [1, 2, 3]}, cache=cache)
    second = wf.run({"xs": [1, 2, 3, 4]}, cache=cache)
    assert second["slow_square"] == [1, 4, 9, 16]
    assert sorted(calls) == [1, 2, 3, 4]  # items run concurrently; only the new item was computed
    assert second.cache_stats() == {"hits": 3, "misses": 1, "hit_rate": 0.75}
    third = wf.run({"xs": [1, 2]}, cache=cache)
    assert third.results["slow_square"].state is TaskState.CACHED


def test_events_describe_the_fan_out() -> None:
    wf = Workflow()

    @wf.task(map="xs")
    def each(xs: int) -> int:
        return xs

    events: list[Event] = []
    wf.run({"xs": [10, 20]}, listeners=[events.append])
    mine = [(e.type, e.index) for e in events if e.task == "each"]
    assert mine[:2] == [(EventType.TASK_STARTED, None), (EventType.TASK_MAPPED, None)]
    assert mine[-1] == (EventType.TASK_SUCCEEDED, None)
    per_item = sorted((e.index, e.type) for e in events if e.task == "each" and e.index is not None)
    assert per_item == [
        (0, EventType.TASK_STARTED),
        (0, EventType.TASK_SUCCEEDED),
        (1, EventType.TASK_STARTED),
        (1, EventType.TASK_SUCCEEDED),
    ]
    mapped = next(e for e in events if e.type is EventType.TASK_MAPPED)
    assert mapped.items == 2


def test_fail_fast_cancels_the_remaining_items() -> None:
    wf = Workflow()

    @wf.task(map="xs")
    async def slow_or_bad(xs: int) -> int:
        if xs == 0:
            raise ValueError("boom")
        await asyncio.sleep(5)
        return xs

    @wf.task
    async def bad() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("elsewhere")

    start = time.perf_counter()
    result = wf.run({"xs": [0, 1, 2]}, fail_fast=True)
    assert time.perf_counter() - start < 1
    assert not result.ok


def test_to_dict_includes_items() -> None:
    summary = numbers_workflow(count=2).run().to_dict()
    square = summary["tasks"]["square"]
    assert square["value"] == [0, 1]
    assert [i["state"] for i in square["items"]] == ["success", "success"]
    assert "items" not in summary["tasks"]["total"]


def test_map_must_name_an_input() -> None:
    def fn(xs: list[int], ctx: Context) -> None: ...

    with pytest.raises(ValueError, match="map='nope' must name one of its inputs"):
        task(map="nope")(fn)
    with pytest.raises(ValueError, match="map='ctx'"):
        task(map="ctx")(fn)  # the Context parameter isn't a data input
