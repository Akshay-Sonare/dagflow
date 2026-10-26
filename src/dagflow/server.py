"""Local web dashboard: ``dagflow ui <workflow>`` serves a live view of runs.

Standard library only. A :class:`Dashboard` starts each run on its own thread
(with its own event loop) and records every lifecycle event; the HTTP layer
exposes a small JSON API plus a Server-Sent Events stream the page subscribes
to, so task states update in the browser as they happen.

API:
    GET  /                       the single-page dashboard
    GET  /api/workflow           tasks, edges, layers and params
    GET  /api/runs               recent runs, newest first
    POST /api/runs               start a run: {"params": {}, "targets": [], "tags": []}
    GET  /api/runs/<id>          one run with its events and final result
    GET  /api/runs/<id>/events   live event stream (text/event-stream)
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any

from dagflow.cache import Cache
from dagflow.errors import DagflowError
from dagflow.events import Event
from dagflow.workflow import Workflow

_KEEPALIVE_SECONDS = 15.0


def _json_safe(value: Any) -> Any:
    """Round-trip through JSON so arbitrary task values can be sent to the browser."""
    return json.loads(json.dumps(value, default=repr))


def coerce_param(value: Any) -> Any:
    """Form fields arrive as text; parse them as JSON when possible, like the CLI."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def event_to_dict(event: Event) -> dict[str, Any]:
    return {
        "type": str(event.type),
        "task": event.task,
        "attempt": event.attempt,
        "duration": event.duration,
        "error": f"{type(event.error).__name__}: {event.error}" if event.error else None,
        "detail": event.detail,
        "timestamp": event.timestamp,
    }


def describe_workflow(workflow: Workflow) -> dict[str, Any]:
    """Everything the dashboard needs to draw the graph and the params form."""
    dag = workflow.dag()
    params: dict[str, dict[str, Any]] = {}
    tasks = []
    for item in workflow:
        upstream = sorted(dag.upstream(item.name))
        for name, param in item.signature.parameters.items():
            if name in workflow:
                continue
            entry = params.setdefault(name, {"name": name, "required": False, "used_by": []})
            entry["used_by"].append(item.name)
            if param.default is inspect.Parameter.empty:
                entry["required"] = True
            elif "default" not in entry:
                entry["default"] = _json_safe(param.default)
        tasks.append(
            {
                "name": item.name,
                "doc": inspect.getdoc(item.fn) or "",
                "upstream": upstream,
                "tags": sorted(item.tags),
                "retries": item.retry.max_attempts - 1,
                "timeout": item.timeout,
                "cache": item.cache,
                "is_async": item.is_async,
            }
        )
    return {
        "name": workflow.name,
        "tasks": tasks,
        "edges": dag.edges(),
        "layers": dag.layers(),
        "params": sorted(params.values(), key=lambda p: (not p["required"], p["name"])),
        "tags": sorted({tag for item in workflow for tag in item.tags}),
    }


@dataclass
class RunRecord:
    """One run's state, shared between the worker thread and HTTP handlers."""

    id: int
    params: dict[str, Any]
    targets: list[str]
    tags: list[str]
    status: str = "running"
    started: float = field(default_factory=time.time)
    finished: float | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None
    _cond: threading.Condition = field(default_factory=threading.Condition, repr=False)

    @property
    def done(self) -> bool:
        return self.status != "running"

    def add_event(self, event: Event) -> None:
        with self._cond:
            self.events.append(event_to_dict(event))
            self._cond.notify_all()

    def finish(self, status: str, result: dict[str, Any] | None, error: str | None = None) -> None:
        with self._cond:
            self.status, self.result, self.error = status, result, error
            self.finished = time.time()
            self._cond.notify_all()

    def wait(self, cursor: int, timeout: float) -> tuple[list[dict[str, Any]], bool]:
        """Block until there are events past ``cursor`` or the run ends."""
        with self._cond:
            self._cond.wait_for(lambda: len(self.events) > cursor or self.done, timeout)
            return self.events[cursor:], self.done

    def summary(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "params": self.params,
            "targets": self.targets,
            "tags": self.tags,
            "started": self.started,
            "finished": self.finished,
            "duration": self.result["duration"] if self.result else None,
            "error": self.error,
        }

    def to_dict(self) -> dict[str, Any]:
        with self._cond:
            return {**self.summary(), "events": list(self.events), "result": self.result}


class Dashboard:
    """Starts runs in background threads and keeps a bounded history of them."""

    def __init__(
        self,
        workflow: Workflow,
        *,
        cache: Cache | None = None,
        max_concurrency: int = 8,
        history: int = 25,
    ) -> None:
        self.workflow = workflow
        self.cache = cache
        self.max_concurrency = max_concurrency
        self.history = history
        self._runs: OrderedDict[int, RunRecord] = OrderedDict()
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def describe(self) -> dict[str, Any]:
        return describe_workflow(self.workflow)

    def runs(self) -> list[RunRecord]:
        with self._lock:
            return list(reversed(self._runs.values()))

    def get(self, run_id: int) -> RunRecord | None:
        with self._lock:
            return self._runs.get(run_id)

    def start(
        self,
        params: Mapping[str, Any],
        *,
        targets: Iterable[str] = (),
        tags: Iterable[str] = (),
    ) -> RunRecord:
        """Validate synchronously (so bad input is reported at once), then run in a thread.

        Raises:
            DagflowError: the workflow can't run with these params/targets/tags.
        """
        params = {key: coerce_param(value) for key, value in params.items() if value != ""}
        targets, tags = list(targets), list(tags)
        self.workflow.validate(params, targets=targets or None, tags=tags or None)
        with self._lock:
            record = RunRecord(next(self._ids), params, targets, tags)
            self._runs[record.id] = record
            while len(self._runs) > self.history:
                self._runs.popitem(last=False)
        thread = threading.Thread(
            target=self._execute, args=(record,), name=f"dagflow-run-{record.id}", daemon=True
        )
        thread.start()
        return record

    def _execute(self, record: RunRecord) -> None:
        try:
            result = asyncio.run(
                self.workflow.arun(
                    record.params,
                    targets=record.targets or None,
                    tags=record.tags or None,
                    max_concurrency=self.max_concurrency,
                    cache=self.cache,
                    listeners=[record.add_event],
                )
            )
        except Exception as exc:
            record.finish("error", None, f"{type(exc).__name__}: {exc}")
            return
        record.finish("succeeded" if result.ok else "failed", _json_safe(result.to_dict()))


class _Handler(BaseHTTPRequestHandler):
    server: DashboardServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        if self.server.verbose:
            super().log_message(format, *args)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json")

    def _error(self, status: int, message: str) -> None:
        self._json({"error": message}, status)

    def _run_from_path(self, raw_id: str) -> RunRecord | None:
        record = self.server.dashboard.get(int(raw_id)) if raw_id.isdigit() else None
        if record is None:
            self._error(HTTPStatus.NOT_FOUND, f"no run {raw_id!r}")
        return record

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        parts = path.strip("/").split("/")
        if path == "/":
            self._send(HTTPStatus.OK, self.server.page, "text/html; charset=utf-8")
        elif path == "/api/workflow":
            self._json(self.server.dashboard.describe())
        elif path == "/api/runs":
            self._json([run.summary() for run in self.server.dashboard.runs()])
        elif len(parts) == 3 and parts[:2] == ["api", "runs"]:
            if record := self._run_from_path(parts[2]):
                self._json(record.to_dict())
        elif len(parts) == 4 and parts[:2] == ["api", "runs"] and parts[3] == "events":
            if record := self._run_from_path(parts[2]):
                self._stream(record)
        else:
            self._error(HTTPStatus.NOT_FOUND, f"no route for {path}")

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/api/runs":
            self._error(HTTPStatus.NOT_FOUND, f"no route for {self.path}")
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(body, dict):
                raise TypeError("body must be a JSON object")
            record = self.server.dashboard.start(
                dict(body.get("params") or {}),
                targets=body.get("targets") or (),
                tags=body.get("tags") or (),
            )
        except (ValueError, TypeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, f"invalid request: {exc}")
        except DagflowError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        else:
            self._json(record.summary(), HTTPStatus.CREATED)

    def _stream(self, record: RunRecord) -> None:
        """Replay past events, then push new ones until the run ends."""
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        cursor = 0
        try:
            while True:
                events, done = record.wait(cursor, timeout=_KEEPALIVE_SECONDS)
                cursor += len(events)
                chunk = "".join(f"data: {json.dumps(e)}\n\n" for e in events)
                if done and not events:
                    chunk = f"event: end\ndata: {json.dumps(record.summary())}\n\n"
                elif not events:
                    chunk = ": keepalive\n\n"
                self.wfile.write(chunk.encode())
                self.wfile.flush()
                if done and not events:
                    return
        except (BrokenPipeError, ConnectionResetError):
            return  # the browser went away


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    # On Windows SO_REUSEADDR lets a second server bind a port that is already in
    # use, so a clash would go unnoticed instead of raising an error.
    allow_reuse_address = os.name != "nt"

    def __init__(
        self,
        dashboard: Dashboard,
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        verbose: bool = False,
    ) -> None:
        super().__init__((host, port), _Handler)
        self.dashboard = dashboard
        self.verbose = verbose

    @property
    def page(self) -> bytes:
        return resources.files("dagflow").joinpath("ui/index.html").read_bytes()

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host!s}:{port}/"
