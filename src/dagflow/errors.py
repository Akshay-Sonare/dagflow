"""Exception hierarchy for dagflow.

Every error raised by the library derives from :class:`DagflowError`, so callers
can catch the whole family with a single ``except`` clause.
"""

from __future__ import annotations

from collections.abc import Sequence


class DagflowError(Exception):
    """Base class for all dagflow errors."""


class DuplicateTaskError(DagflowError):
    """A task with the same name is already registered in the workflow."""

    def __init__(self, name: str) -> None:
        super().__init__(f"task {name!r} is already registered")
        self.name = name


class UnknownTaskError(DagflowError):
    """A task name was referenced that does not exist in the workflow."""

    def __init__(self, name: str, referenced_by: str | None = None) -> None:
        suffix = f" (referenced by {referenced_by!r})" if referenced_by else ""
        super().__init__(f"unknown task {name!r}{suffix}")
        self.name = name
        self.referenced_by = referenced_by


class CycleError(DagflowError):
    """The dependency graph contains a cycle and cannot be scheduled."""

    def __init__(self, cycle: Sequence[str]) -> None:
        self.cycle = list(cycle)
        super().__init__("dependency cycle detected: " + " -> ".join(self.cycle))


class MissingInputError(DagflowError):
    """A task parameter is neither an upstream task nor a supplied run parameter."""

    def __init__(self, task: str, inputs: Sequence[str]) -> None:
        self.task = task
        self.inputs = list(inputs)
        super().__init__(
            f"task {task!r} needs inputs that are neither tasks nor params: "
            + ", ".join(self.inputs)
        )


class TaskTimeoutError(DagflowError, TimeoutError):
    """A task attempt exceeded its configured timeout."""

    def __init__(self, task: str, timeout: float) -> None:
        super().__init__(f"task {task!r} timed out after {timeout:g}s")
        self.task = task
        self.timeout = timeout


class TaskFailedError(DagflowError):
    """Raised when reading the value of a task that did not succeed."""

    def __init__(self, task: str, state: str, cause: BaseException | None = None) -> None:
        detail = f": {type(cause).__name__}: {cause}" if cause else ""
        super().__init__(f"task {task!r} has no value (state={state}){detail}")
        self.task = task
        self.state = state
        self.__cause__ = cause
