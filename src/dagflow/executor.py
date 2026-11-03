"""The asynchronous, dependency-aware scheduler that actually runs tasks.

Scheduling is event driven rather than layer by layer: a task starts the moment
its last upstream dependency succeeds, so a slow branch never holds up an
unrelated fast one. A semaphore bounds how many attempts run at once; retry
back-off sleeps happen outside it so waiting tasks don't hog a slot.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from dagflow.cache import MISSING, Cache, cache_key
from dagflow.errors import Skip, TaskFailedError, TaskTimeoutError
from dagflow.events import Event, EventBus, EventType
from dagflow.graph import DAG
from dagflow.task import Context, Task

log = logging.getLogger(__name__)


class TaskState(StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    CACHED = "cached"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def ok(self) -> bool:
        return self in (TaskState.SUCCESS, TaskState.CACHED)


@dataclass(slots=True)
class TaskResult:
    name: str
    state: TaskState = TaskState.PENDING
    value: Any = None
    error: BaseException | None = None
    attempts: int = 0
    duration: float = 0.0
    upstream: tuple[str, ...] = ()
    cache_lookup: bool = False
    """True if a cache lookup was made for this task (it is cached and its inputs hash)."""


@dataclass(slots=True)
class RunResult:
    """Outcome of a workflow run. ``result["task"]`` returns that task's value."""

    workflow: str
    results: dict[str, TaskResult] = field(default_factory=dict)
    duration: float = 0.0

    @property
    def ok(self) -> bool:
        """True unless a task failed or was cancelled (deliberate ``Skip`` is fine)."""
        return not any(
            r.state in (TaskState.FAILED, TaskState.CANCELLED) for r in self.results.values()
        )

    def __getitem__(self, name: str) -> Any:
        result = self.results[name]
        if not result.state.ok:
            raise TaskFailedError(name, result.state, result.error)
        return result.value

    def __contains__(self, name: object) -> bool:
        return name in self.results

    def by_state(self, state: TaskState) -> list[str]:
        return [name for name, r in self.results.items() if r.state is state]

    def critical_path(self) -> list[str]:
        """The dependency chain of executed tasks with the largest total duration.

        Speeding up a task on this path is what shortens the run. Skipped and
        cancelled tasks never ran, so they are ignored; empty if nothing ran.
        """
        best: dict[str, tuple[float, list[str]]] = {}

        def visit(name: str) -> tuple[float, list[str]]:
            if name not in best:
                r = self.results[name]
                if r.state in (TaskState.SKIPPED, TaskState.CANCELLED, TaskState.PENDING):
                    best[name] = (0.0, [])
                else:
                    total, path = max(
                        (visit(u) for u in r.upstream if u in self.results),
                        key=lambda item: item[0],
                        default=(0.0, []),
                    )
                    best[name] = (total + r.duration, [*path, name])
            return best[name]

        ranked = [visit(name) for name in self.results]
        return max(ranked, key=lambda item: item[0], default=(0.0, []))[1]

    def cache_stats(self) -> dict[str, Any]:
        """Cache ``hits``, ``misses`` and ``hit_rate`` (``None`` if no lookups happened).

        Only tasks that consulted the cache count; tasks skipped or cancelled before
        running never looked, and uncacheable inputs bypass the cache entirely.
        """
        lookups = [r for r in self.results.values() if r.cache_lookup]
        hits = sum(r.state is TaskState.CACHED for r in lookups)
        return {
            "hits": hits,
            "misses": len(lookups) - hits,
            "hit_rate": round(hits / len(lookups), 4) if lookups else None,
        }

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly summary (values are left as-is; serialise with ``default=repr``)."""
        return {
            "workflow": self.workflow,
            "ok": self.ok,
            "duration": round(self.duration, 4),
            "critical_path": self.critical_path(),
            "cache": self.cache_stats(),
            "tasks": {
                name: {
                    "state": str(r.state),
                    "attempts": r.attempts,
                    "duration": round(r.duration, 4),
                    "value": r.value if r.state.ok else None,
                    "error": f"{type(r.error).__name__}: {r.error}" if r.error else None,
                }
                for name, r in self.results.items()
            },
        }


class Executor:
    """Runs the tasks of a :class:`~dagflow.graph.DAG` on the current event loop.

    Args:
        max_concurrency: Upper bound on task attempts running at the same time.
        cache: Backend for tasks declared with ``cache=True``.
        fail_fast: Cancel everything still running as soon as one task fails.
            Otherwise only the failed task's descendants are skipped.
        pools: Concurrency limits for named task pools (``@task(pool=...)``), applied on top
            of ``max_concurrency``. A task naming a pool that has no limit is an error.
        bus: Where lifecycle events are published.
    """

    def __init__(
        self,
        *,
        max_concurrency: int = 8,
        cache: Cache | None = None,
        fail_fast: bool = False,
        pools: Mapping[str, int] | None = None,
        bus: EventBus | None = None,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self.cache = cache
        self.fail_fast = fail_fast
        self.pools = dict(pools or {})
        for pool, limit in self.pools.items():
            if limit < 1:
                raise ValueError(f"pool {pool!r} limit must be >= 1")
        self.bus = bus or EventBus()

    async def execute(
        self,
        workflow: str,
        tasks: Mapping[str, Task],
        dag: DAG,
        params: Mapping[str, Any],
    ) -> RunResult:
        started = time.perf_counter()
        run_id = uuid.uuid4().hex[:12]
        order = dag.topological_order()  # also rejects cycles before anything runs
        run = RunResult(workflow, {name: TaskResult(name) for name in order})
        waiting_on = {name: len(dag.upstream(name)) for name in order}
        for t in (tasks[n] for n in order):
            if t.pool is not None and t.pool not in self.pools:
                raise ValueError(f"task {t.name!r} uses pool {t.pool!r} but the run sets no limit")
        semaphore = asyncio.Semaphore(self.max_concurrency)
        pool_gates = {n: asyncio.Semaphore(limit) for n, limit in self.pools.items()}
        running: dict[asyncio.Task[TaskResult], str] = {}

        def launch(name: str) -> None:
            inputs = self._inputs(tasks[name], run.results, params)
            coro = self._run_task(
                tasks[name], inputs, semaphore, run_id, pool_gates.get(tasks[name].pool or "")
            )
            running[asyncio.create_task(coro, name=f"dagflow:{name}")] = name

        self.bus.emit(Event(EventType.RUN_STARTED, detail=workflow))
        for name in order:
            if waiting_on[name] == 0:
                launch(name)

        try:
            while running:
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for finished in done:
                    name = running.pop(finished)
                    result = run.results[name] = finished.result()
                    result.upstream = tuple(sorted(dag.upstream(name)))
                    if result.state.ok:
                        for child in sorted(dag.downstream(name)):
                            waiting_on[child] -= 1
                            if waiting_on[child] == 0:
                                launch(child)
                    else:
                        self._skip(dag.descendants(name), run.results, cause=name)
                if self.fail_fast and any(
                    r.state is TaskState.FAILED for r in run.results.values()
                ):
                    await self._cancel(running, run.results, dag)
        finally:
            # Reached with tasks still running only if we ourselves were cancelled.
            for pending in running:
                pending.cancel()
            await asyncio.gather(*running, return_exceptions=True)

        run.duration = time.perf_counter() - started
        self.bus.emit(
            Event(
                EventType.RUN_FINISHED, duration=run.duration, detail="ok" if run.ok else "failed"
            )
        )
        return run

    @staticmethod
    def _inputs(
        task: Task, results: Mapping[str, TaskResult], params: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Upstream results win over params of the same name; defaults fill the rest."""
        inputs: dict[str, Any] = {}
        for param in task.inputs:
            if param in results:
                inputs[param] = results[param].value
            elif param in params:
                inputs[param] = params[param]
        return inputs

    def _skip(self, names: Iterable[str], results: dict[str, TaskResult], cause: str) -> None:
        for name in sorted(names):
            if results[name].state is TaskState.PENDING:
                results[name].state = TaskState.SKIPPED
                self.bus.emit(
                    Event(
                        EventType.TASK_SKIPPED, name, detail=f"upstream {cause!r} did not succeed"
                    )
                )

    async def _cancel(
        self,
        running: dict[asyncio.Task[TaskResult], str],
        results: dict[str, TaskResult],
        dag: DAG,
    ) -> None:
        for pending in running:
            pending.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        for name in sorted(running.values()):
            results[name].state = TaskState.CANCELLED
            self.bus.emit(Event(EventType.TASK_CANCELLED, name, detail="fail-fast"))
        running.clear()
        for name in list(results):
            if results[name].state is TaskState.PENDING:
                self._skip([name], results, cause="fail-fast")

    async def _run_task(
        self,
        task: Task,
        inputs: dict[str, Any],
        semaphore: asyncio.Semaphore,
        run_id: str,
        gate: asyncio.Semaphore | None = None,
    ) -> TaskResult:
        result = TaskResult(task.name)
        started = time.perf_counter()
        cache = self.cache if task.cache else None
        key = cache_key(task, inputs) if cache is not None else None

        if cache is not None and key is not None:
            result.cache_lookup = True
            try:
                hit = await asyncio.to_thread(cache.get, key)
            except Exception:
                log.warning("task %r: could not read cache; running it", task.name, exc_info=True)
                hit = MISSING
            if hit is not MISSING:
                result.state, result.value = TaskState.CACHED, hit
                result.duration = time.perf_counter() - started
                self.bus.emit(Event(EventType.TASK_CACHED, task.name, duration=result.duration))
                return result

        while True:
            result.attempts += 1
            self.bus.emit(Event(EventType.TASK_STARTED, task.name, attempt=result.attempts))
            try:
                call_inputs = inputs
                if task.context_param is not None:
                    ctx = Context(run_id, task.name, result.attempts, task.retry.max_attempts)
                    call_inputs = {**inputs, task.context_param: ctx}
                # Pool first, so a task waiting on a full pool doesn't hold a global slot.
                async with gate or contextlib.nullcontext(), semaphore:
                    value = await self._invoke(task, call_inputs)
            except Skip as skip:
                result.state = TaskState.SKIPPED
                result.duration = time.perf_counter() - started
                self.bus.emit(
                    Event(EventType.TASK_SKIPPED, task.name, detail=skip.reason or "skipped")
                )
                return result
            except Exception as exc:
                if task.retry.should_retry(exc, result.attempts):
                    delay = task.retry.delay(result.attempts)
                    self.bus.emit(
                        Event(
                            EventType.TASK_RETRYING,
                            task.name,
                            attempt=result.attempts,
                            error=exc,
                            detail=f"retrying in {delay:.2f}s",
                        )
                    )
                    await asyncio.sleep(delay)
                    continue
                result.state, result.error = TaskState.FAILED, exc
                result.duration = time.perf_counter() - started
                self.bus.emit(
                    Event(
                        EventType.TASK_FAILED,
                        task.name,
                        attempt=result.attempts,
                        duration=result.duration,
                        error=exc,
                    )
                )
                return result
            break

        result.state, result.value = TaskState.SUCCESS, value
        result.duration = time.perf_counter() - started
        if cache is not None and key is not None:
            try:
                await asyncio.to_thread(cache.set, key, value)
            except Exception:
                log.warning("task %r: could not cache result", task.name, exc_info=True)
        self.bus.emit(
            Event(
                EventType.TASK_SUCCEEDED,
                task.name,
                attempt=result.attempts,
                duration=result.duration,
            )
        )
        return result

    @staticmethod
    async def _invoke(task: Task, inputs: dict[str, Any]) -> Any:
        """Await async tasks directly; push sync ones onto a worker thread.

        A timed-out sync task can't be interrupted: its thread finishes in the
        background while the run moves on.
        """
        awaitable = task.fn(**inputs) if task.is_async else asyncio.to_thread(task.fn, **inputs)
        if task.timeout is None:
            return await awaitable
        deadline = asyncio.timeout(task.timeout)
        try:
            async with deadline:
                return await awaitable
        except TimeoutError:
            if deadline.expired():
                raise TaskTimeoutError(task.name, task.timeout) from None
            raise
