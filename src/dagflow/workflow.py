"""The :class:`Workflow` container: register tasks, validate the graph, run it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any, overload

from dagflow.cache import Cache
from dagflow.errors import DuplicateTaskError, MissingInputError, UnknownTaskError
from dagflow.events import EventBus, Listener
from dagflow.executor import Executor, RunResult
from dagflow.graph import DAG
from dagflow.task import Task, TaskFn
from dagflow.task import task as make_task


class Workflow:
    """A named collection of tasks whose parameters define a dependency graph.

    >>> wf = Workflow("demo")
    >>> @wf.task
    ... def numbers():
    ...     return [1, 2, 3]
    >>> @wf.task
    ... def total(numbers):
    ...     return sum(numbers)
    >>> wf.run()["total"]
    6
    """

    def __init__(self, name: str = "workflow") -> None:
        self.name = name
        self._tasks: dict[str, Task] = {}

    def __repr__(self) -> str:
        return f"Workflow({self.name!r}, tasks={len(self)})"

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self) -> Iterator[Task]:
        return iter(self._tasks.values())

    def __contains__(self, name: object) -> bool:
        return name in self._tasks

    def __getitem__(self, name: str) -> Task:
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTaskError(name) from None

    @property
    def tasks(self) -> Mapping[str, Task]:
        return MappingProxyType(self._tasks)

    def add(self, *tasks: Task) -> None:
        for item in tasks:
            if item.name in self._tasks:
                raise DuplicateTaskError(item.name)
            self._tasks[item.name] = item

    @overload
    def task(self, fn: TaskFn, /) -> Task: ...

    @overload
    def task(
        self,
        /,
        *,
        name: str | None = None,
        retries: int = 0,
        backoff: float = 0.0,
        jitter: float = 0.0,
        retry_on: Iterable[type[BaseException]] = (Exception,),
        timeout: float | None = None,
        cache: bool = False,
        after: Iterable[str | Task] = (),
        tags: Iterable[str] = (),
    ) -> Callable[[TaskFn], Task]: ...

    def task(
        self,
        fn: TaskFn | None = None,
        /,
        *,
        name: str | None = None,
        retries: int = 0,
        backoff: float = 0.0,
        jitter: float = 0.0,
        retry_on: Iterable[type[BaseException]] = (Exception,),
        timeout: float | None = None,
        cache: bool = False,
        after: Iterable[str | Task] = (),
        tags: Iterable[str] = (),
    ) -> Task | Callable[[TaskFn], Task]:
        """Decorator form of :func:`dagflow.task` that also registers the task."""

        def register(func: TaskFn) -> Task:
            created = make_task(
                name=name,
                retries=retries,
                backoff=backoff,
                jitter=jitter,
                retry_on=retry_on,
                timeout=timeout,
                cache=cache,
                after=after,
                tags=tags,
            )(func)
            self.add(created)
            return created

        return register(fn) if fn is not None else register

    def dag(self) -> DAG:
        """Build the dependency graph from task parameters and ``after=`` links."""
        graph = DAG(self._tasks)
        for item in self._tasks.values():
            for param in item.inputs:
                if param in self._tasks:
                    graph.add_edge(param, item.name)
            for upstream in item.after:
                if upstream not in self._tasks:
                    raise UnknownTaskError(upstream, referenced_by=item.name)
                graph.add_edge(upstream, item.name)
        return graph

    def required_params(self) -> dict[str, list[str]]:
        """Run params with no default, mapped to the tasks that need them."""
        needed: dict[str, list[str]] = {}
        for item in self._tasks.values():
            for param in item.inputs:
                if param not in self._tasks and not item.has_default(param):
                    needed.setdefault(param, []).append(item.name)
        return needed

    def validate(self, params: Iterable[str] = (), *, targets: Iterable[str] | None = None) -> DAG:
        """Check the workflow can run with ``params`` and return its (sub)graph.

        Raises:
            UnknownTaskError: an ``after=`` link or target names no task.
            CycleError: the dependencies are circular.
            MissingInputError: a task has an input nobody provides.
        """
        graph = self.dag()
        targets = list(targets or ())
        if targets:
            graph = graph.subgraph(targets)
        graph.topological_order()
        provided = set(params)
        for name in graph:
            item = self._tasks[name]
            missing = [
                p
                for p in item.inputs
                if p not in self._tasks and p not in provided and not item.has_default(p)
            ]
            if missing:
                raise MissingInputError(name, missing)
        return graph

    def plan(self, *, targets: Iterable[str] | None = None) -> list[list[str]]:
        """Execution layers: tasks in the same layer may run concurrently."""
        graph = self.dag()
        return (graph.subgraph(targets) if targets else graph).layers()

    async def arun(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        targets: Iterable[str] | None = None,
        max_concurrency: int = 8,
        cache: Cache | None = None,
        fail_fast: bool = False,
        listeners: Iterable[Listener] = (),
    ) -> RunResult:
        """Run the workflow on the current event loop. See :meth:`run`."""
        params = dict(params or {})
        graph = self.validate(params, targets=targets)
        bus = EventBus()
        for listener in listeners:
            bus.subscribe(listener)
        executor = Executor(
            max_concurrency=max_concurrency, cache=cache, fail_fast=fail_fast, bus=bus
        )
        return await executor.execute(self.name, self._tasks, graph, params)

    def run(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        targets: Iterable[str] | None = None,
        max_concurrency: int = 8,
        cache: Cache | None = None,
        fail_fast: bool = False,
        listeners: Iterable[Listener] = (),
    ) -> RunResult:
        """Validate and run the workflow, blocking until every task has settled.

        Args:
            params: Values for task parameters that aren't other tasks.
            targets: Run only these tasks and what they depend on.
            max_concurrency: Maximum task attempts in flight at once.
            cache: Backend for tasks declared with ``cache=True``.
            fail_fast: Cancel running tasks as soon as any task fails.
            listeners: Callables that receive every :class:`~dagflow.events.Event`.
        """
        return asyncio.run(
            self.arun(
                params,
                targets=targets,
                max_concurrency=max_concurrency,
                cache=cache,
                fail_fast=fail_fast,
                listeners=listeners,
            )
        )
