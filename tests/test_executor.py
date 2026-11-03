import asyncio
import threading
import time
from typing import Any

import pytest

from dagflow import (
    Context,
    Event,
    EventType,
    MemoryCache,
    Skip,
    TaskFailedError,
    TaskState,
    TaskTimeoutError,
    Workflow,
    task,
)
from dagflow.executor import Executor
from dagflow.task import Task


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


def test_cache_read_failure_is_treated_as_a_miss() -> None:
    class BrokenReads(MemoryCache):
        def get(self, key: str) -> Any:
            raise OSError("disk unavailable")

    wf = Workflow()

    @wf.task(cache=True)
    def value() -> int:
        return 7

    cache = BrokenReads()
    result = wf.run(cache=cache)
    assert result.ok
    assert result["value"] == 7
    assert result.results["value"].state is TaskState.SUCCESS
    assert len(cache) == 1  # writes still work


def test_context_injection_reports_attempt_and_run_id() -> None:
    wf = Workflow("ctx")
    seen: list[Context] = []

    @wf.task(retries=2)
    def flaky(c: Context, seed: int = 1) -> int:
        seen.append(c)
        if c.attempt < 3:
            raise RuntimeError("boom")
        return seed

    @wf.task
    def after(flaky: int, info: Context) -> str:
        return f"{info.task}:{info.run_id}"

    result = wf.run(params={"seed": 5})
    assert result["flaky"] == 5
    assert [c.attempt for c in seen] == [1, 2, 3]
    assert seen[0].max_attempts == 3
    assert not seen[0].is_retry
    assert seen[1].is_retry
    assert result["after"] == f"after:{seen[0].run_id}"
    assert wf["flaky"].inputs == ("seed",)
    assert wf.required_params() == {}


def test_context_param_is_not_a_cache_input() -> None:
    wf = Workflow("ctx-cache")
    calls: list[str] = []

    @wf.task(cache=True)
    def work(ctx: Context) -> int:
        calls.append(ctx.run_id)
        return 1

    cache = MemoryCache()
    wf.run(cache=cache)
    wf.run(cache=cache)
    assert len(calls) == 1


def test_two_context_params_rejected() -> None:
    def bad(a: Context, b: Context) -> None: ...

    with pytest.raises(TypeError, match="several Context"):
        task(bad)


def test_critical_path_follows_slowest_chain() -> None:
    wf = Workflow()

    @wf.task
    async def root() -> int:
        return 1

    @wf.task
    async def slow(root: int) -> int:
        await asyncio.sleep(0.15)
        return root

    @wf.task
    async def quick(root: int) -> int:
        return root

    @wf.task
    async def final(slow: int, quick: int) -> int:
        return slow + quick

    result = wf.run()
    assert result.critical_path() == ["root", "slow", "final"]
    assert result.to_dict()["critical_path"] == ["root", "slow", "final"]


def test_critical_path_ignores_tasks_that_never_ran() -> None:
    wf = Workflow()

    @wf.task
    def boom() -> int:
        raise RuntimeError("nope")

    @wf.task
    def after(boom: int) -> int:
        return boom

    result = wf.run()
    assert result.results["after"].state is TaskState.SKIPPED
    assert result.critical_path() == ["boom"]


def _pool_workflow(pool: str | None) -> tuple[Workflow, dict[str, int]]:
    state = {"now": 0, "peak": 0}
    wf = Workflow("pooled")

    def make(i: int) -> None:
        async def fn() -> int:
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
            await asyncio.sleep(0.01)
            state["now"] -= 1
            return i

        wf.add(Task(fn=fn, name=f"t{i}", pool=pool))

    for i in range(6):
        make(i)
    return wf, state


def test_pool_limits_concurrency() -> None:
    wf, state = _pool_workflow("api")
    result = wf.run(max_concurrency=6, pools={"api": 2})
    assert result.ok
    assert state["peak"] == 2


def test_pool_unlimited_without_pool() -> None:
    wf, state = _pool_workflow(None)
    assert wf.run(max_concurrency=6, pools={"api": 2}).ok
    assert state["peak"] == 6


def test_pool_without_limit_is_an_error() -> None:
    wf, _ = _pool_workflow("api")
    with pytest.raises(ValueError, match="pool 'api'"):
        wf.run()


def test_pool_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="limit"):
        Executor(pools={"api": 0})


def test_skip_exception_skips_task_and_descendants_without_failing_run() -> None:
    wf = Workflow()
    calls: list[str] = []

    @wf.task(retries=3)
    def maybe() -> int:
        calls.append("maybe")
        raise Skip("nothing to do")

    @wf.task
    def after(maybe: int) -> int:
        return maybe

    @wf.task
    def other() -> int:
        return 1

    events: list[Event] = []
    result = wf.run(listeners=[events.append])
    assert calls == ["maybe"]  # no retries
    assert result.results["maybe"].state is TaskState.SKIPPED
    assert result.results["after"].state is TaskState.SKIPPED
    assert result["other"] == 1
    assert result.ok
    assert any(e.type is EventType.TASK_SKIPPED and e.detail == "nothing to do" for e in events)
    with pytest.raises(TaskFailedError):
        result["maybe"]


def test_skip_does_not_hide_real_failures() -> None:
    wf = Workflow()

    @wf.task
    def a() -> None:
        raise Skip

    @wf.task
    def b() -> None:
        raise RuntimeError("boom")

    assert not wf.run().ok
