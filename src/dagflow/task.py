"""Task definitions, retry policies and the :func:`task` decorator.

A task is a plain Python function (sync or ``async``). Its parameters declare its
inputs: a parameter named after another task receives that task's result, any
other parameter is filled from the run's params or its default value. This is
the same dependency-injection style pytest uses for fixtures.
"""

from __future__ import annotations

import inspect
import random
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, overload

TaskFn = Callable[..., Any]

_KEYWORD_KINDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential-backoff retry policy.

    ``max_attempts`` counts the first try, so ``max_attempts=1`` means no retries.
    ``jitter`` (0-1) randomly shortens each delay by up to that fraction, so many tasks
    failing together don't all retry at the same instant; ``max_backoff`` stays an upper bound.
    """

    max_attempts: int = 1
    backoff: float = 0.0
    multiplier: float = 2.0
    max_backoff: float = 60.0
    jitter: float = 0.0
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.backoff < 0 or self.max_backoff < 0:
            raise ValueError("backoff values must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay(self, attempt: int) -> float:
        """Seconds to wait after failed ``attempt`` (1-based) before the next one."""
        base = min(self.backoff * self.multiplier ** (attempt - 1), self.max_backoff)
        return float(base * (1 - self.jitter * random.random()))

    def should_retry(self, error: BaseException, attempt: int) -> bool:
        return attempt < self.max_attempts and isinstance(error, self.retry_on)


@dataclass(frozen=True, slots=True, eq=False)
class Task:
    """A unit of work in a workflow. Calling it calls the wrapped function directly."""

    fn: TaskFn
    name: str
    after: tuple[str, ...] = ()
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: float | None = None
    cache: bool = False
    tags: frozenset[str] = frozenset()
    signature: inspect.Signature = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError(f"task name {self.name!r} must be a valid identifier")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        signature = inspect.signature(self.fn)
        for param in signature.parameters.values():
            if param.kind not in _KEYWORD_KINDS:
                raise TypeError(
                    f"task {self.name!r}: parameter {param.name!r} must be passable by keyword"
                )
        object.__setattr__(self, "signature", signature)

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    @property
    def inputs(self) -> tuple[str, ...]:
        """Names of every parameter the task accepts."""
        return tuple(self.signature.parameters)

    def has_default(self, param: str) -> bool:
        return self.signature.parameters[param].default is not inspect.Parameter.empty

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)


def _names(items: Iterable[str | Task]) -> tuple[str, ...]:
    return tuple(item.name if isinstance(item, Task) else item for item in items)


@overload
def task(fn: TaskFn, /) -> Task: ...


@overload
def task(
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
    """Turn a function into a :class:`Task`. Usable bare (``@task``) or with options.

    Args:
        name: Task name; defaults to the function name.
        retries: Extra attempts after the first failure.
        backoff: Initial delay between attempts, doubled after each failure.
        jitter: Fraction (0-1) of each backoff delay to randomly shave off.
        retry_on: Exception types that trigger a retry.
        timeout: Per-attempt timeout in seconds.
        cache: Reuse results for identical inputs when the run has a cache.
        after: Tasks that must finish first even though no data flows from them.
        tags: Free-form labels, used for filtering and display.
    """

    def wrap(func: TaskFn) -> Task:
        return Task(
            fn=func,
            name=name or func.__name__,
            after=_names(after),
            retry=RetryPolicy(
                max_attempts=retries + 1,
                backoff=backoff,
                jitter=jitter,
                retry_on=tuple(retry_on),
            ),
            timeout=timeout,
            cache=cache,
            tags=frozenset(tags),
        )

    return wrap(fn) if fn is not None else wrap
