"""Lifecycle events emitted while a workflow runs, plus a tiny pub/sub bus."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

log = logging.getLogger(__name__)


class EventType(StrEnum):
    RUN_STARTED = "run_started"
    RUN_FINISHED = "run_finished"
    TASK_STARTED = "task_started"
    TASK_RETRYING = "task_retrying"
    TASK_SUCCEEDED = "task_succeeded"
    TASK_CACHED = "task_cached"
    TASK_FAILED = "task_failed"
    TASK_SKIPPED = "task_skipped"
    TASK_CANCELLED = "task_cancelled"


@dataclass(frozen=True, slots=True)
class Event:
    type: EventType
    task: str | None = None
    attempt: int = 0
    duration: float | None = None
    error: BaseException | None = None
    detail: str | None = None
    timestamp: float = field(default_factory=time.time)


Listener = Callable[[Event], None]


class EventBus:
    """Fan events out to listeners. A failing listener is logged, never fatal."""

    def __init__(self) -> None:
        self._listeners: list[Listener] = []

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        """Register ``listener`` and return a function that unregisters it."""
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener)

    def emit(self, event: Event) -> None:
        for listener in list(self._listeners):
            try:
                listener(event)
            except Exception:
                log.exception("event listener %r failed on %s", listener, event.type)
