import asyncio
import threading
import time

import pytest

from dagflow import (
    Event,
    EventType,
    MemoryCache,
    TaskFailedError,
    TaskState,
    TaskTimeoutError,
    Workflow,
)


def test_independent_tasks_run_concurrently() -> None:
    wf = Workflow()

    @wf.task
    async def left() -> str:
        await asyncio.sleep(0.2)
        return "L"

    @wf.task
    async def right() -> str:
        await asyncio.sleep(0.2)
        return "R"

    @wf.task
    def joined(left: str, right: str) -> str:
        return left + right

    start = time.perf_counter()
    result = wf.run()
    assert time.perf_counter() - start < 0.35
    assert result["joined"] == "LR"


def test_max_concurrency_is_respected() -> None:
    wf = Workflow()
    active = 0
    peak = 0

    async def probe() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1

    for i in range(10):
        wf.task(name=f"t{i}")(probe)

    wf.run(max_concurrency=3)
    assert peak == 3


def test_downstream_starts_without_waiting_for_slow_siblings() -> None:
    wf = Workflow()
    finished: list[str] = []

    @wf.task
    async def slow() -> None:
        await asyncio.sleep(0.2)
        finished.append("slow")

    @wf.task
    async def fast() -> None:
        finished.append("fast")

    @wf.task
    async def after_fast(fast: None) -> None:
        finished.append("after_fast")

    wf.run()
    assert finished == ["fast", "after_fast", "slow"]


def test_sync_tasks_run_in_worker_threads() -> None:
    wf = Workflow()

    @wf.task
    def which_thread() -> str:
        return threading.current_thread().name

    assert wf.run()["which_thread"] != threading.main_thread().name


def test_retries_until_success() -> None:
    wf = Workflow()
    calls = 0

    @wf.task(retries=3, backoff=0.01)
    def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("try again")
        return "ok"

    result = wf.run()
    assert result["flaky"] == "ok"
    assert result.results["flaky"].attempts == 3


def test_retries_exhausted_marks_failure() -> None:
    wf = Workflow()

    @wf.task(retries=2)
    def broken() -> None:
        raise RuntimeError("nope")

    result = wf.run()
    assert not result.ok
    failed = result.results["broken"]
    assert failed.state is TaskState.FAILED
    assert failed.attempts == 3
    assert isinstance(failed.error, RuntimeError)


def test_non_matching_errors_are_not_retried() -> None:
    wf = Workflow()

    @wf.task(retries=5, retry_on=(ConnectionError,))
    def wrong_kind() -> None:
        raise ValueError("bad input")

    assert wf.run().results["wrong_kind"].attempts == 1


def test_timeout() -> None:
    wf = Workflow()

    @wf.task(timeout=0.05)
    async def sleepy() -> None:
        await asyncio.sleep(5)

    result = wf.run()
    assert isinstance(result.results["sleepy"].error, TaskTimeoutError)


def test_user_timeout_error_is_not_mistaken_for_ours() -> None:
    wf = Workflow()

    @wf.task(timeout=5)
    async def raises_own() -> None:
        raise TimeoutError("from the task itself")

    error = wf.run().results["raises_own"].error
    assert type(error) is TimeoutError


def test_failure_skips_descendants_but_not_siblings() -> None:
    wf = Workflow()

    @wf.task
    def bad() -> int:
        raise ValueError("boom")

    @wf.task
    def child(bad: int) -> int:
        return bad

    @wf.task
    def grandchild(child: int) -> int:
        return child

    @wf.task
    async def sibling() -> str:
        await asyncio.sleep(0.05)
        return "fine"

    result = wf.run()
    assert result.results["bad"].state is TaskState.FAILED
    assert result.by_state(TaskState.SKIPPED) == ["child", "grandchild"]
    assert result["sibling"] == "fine"
    with pytest.raises(TaskFailedError, match="skipped"):
        result["child"]


def test_fail_fast_cancels_running_tasks() -> None:
    wf = Workflow()

    @wf.task
    async def bad() -> None:
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    @wf.task
    async def slow() -> None:
        await asyncio.sleep(5)

    @wf.task
    def after_slow(slow: None) -> None: ...

    start = time.perf_counter()
    result = wf.run(fail_fast=True)
    assert time.perf_counter() - start < 1
    assert result.results["slow"].state is TaskState.CANCELLED
    assert result.results["after_slow"].state is TaskState.SKIPPED


def test_cache_skips_recomputation_and_keys_on_inputs() -> None:
    wf = Workflow()
    calls: list[int] = []

    @wf.task(cache=True)
    def square(n: int) -> int:
        calls.append(n)
        return n * n

    cache = MemoryCache()
    assert wf.run({"n": 4}, cache=cache)["square"] == 16
    second = wf.run({"n": 4}, cache=cache)
    assert second["square"] == 16
    assert second.results["square"].state is TaskState.CACHED
    wf.run({"n": 5}, cache=cache)
    assert calls == [4, 5]


def test_cache_is_ignored_for_tasks_without_cache_flag() -> None:
    wf = Workflow()
    calls = 0

    @wf.task
    def uncached() -> int:
        nonlocal calls
        calls += 1
        return calls

    cache = MemoryCache()
    wf.run(cache=cache)
    wf.run(cache=cache)
    assert calls == 2
    assert len(cache) == 0


def test_events_are_emitted_in_order() -> None:
    wf = Workflow("events")

    @wf.task(retries=1)
    def once_flaky() -> int:
        if not seen_retry:
            raise ConnectionError
        return 1

    seen: list[tuple[EventType, str | None]] = []
    seen_retry: list[bool] = []

    def listener(event: Event) -> None:
        seen.append((event.type, event.task))
        if event.type is EventType.TASK_RETRYING:
            seen_retry.append(True)

    wf.run(listeners=[listener])
    assert seen == [
        (EventType.RUN_STARTED, None),
        (EventType.TASK_STARTED, "once_flaky"),
        (EventType.TASK_RETRYING, "once_flaky"),
        (EventType.TASK_STARTED, "once_flaky"),
        (EventType.TASK_SUCCEEDED, "once_flaky"),
        (EventType.RUN_FINISHED, None),
    ]


def test_broken_listener_does_not_break_the_run() -> None:
    wf = Workflow()
    wf.task(name="fine")(lambda: 1)

    def explode(event: Event) -> None:
        raise RuntimeError("listener bug")

    assert wf.run(listeners=[explode])["fine"] == 1


def test_arun_inside_existing_loop() -> None:
    wf = Workflow()
    wf.task(name="value")(lambda: 42)

    async def main() -> int:
        result = await wf.arun()
        return int(result["value"])

    assert asyncio.run(main()) == 42


def test_run_result_to_dict() -> None:
    wf = Workflow("summary")
    wf.task(name="answer")(lambda: 42)
    summary = wf.run().to_dict()
    assert summary["ok"] is True
    assert summary["tasks"]["answer"]["value"] == 42
    assert summary["tasks"]["answer"]["state"] == "success"
