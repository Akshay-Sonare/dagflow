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
    GET  /api/runs/<id>/events   live event stream (text/event-stream); each event
                                 carries ``id: <n>`` and ``Last-Event-ID`` resumes

Security: the server runs workflow code on request, so on a loopback address it
only answers requests whose ``Host`` is a loopback name (blocking DNS
rebinding), and ``POST`` requires a JSON body and, when the browser sends one,
a same-origin ``Origin`` (blocking cross-site requests from other pages).
"""

from __future__ import annotations

import asyncio
import inspect
import ipaddress
import itertools
import json
import os
import socketserver
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any
from urllib.parse import urlsplit

from dagflow.cache import Cache
from dagflow.errors import DagflowError
from dagflow.events import Event
from dagflow.workflow import Workflow

_KEEPALIVE_SECONDS = 15.0
_LOOPBACK_NAMES = frozenset({"localhost", "127.0.0.1", "::1"})


def _json_safe(value: Any) -> Any:
    """Round-trip through JSON so arbitrary task values can be sent to the browser."""
    return json.loads(json.dumps(value, default=repr))


def coerce_param(value: Any) -> Any:
    """Parse text as JSON when possible, else keep it as text (shared with the CLI)."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def is_loopback(host: str) -> bool:
    """True for ``localhost`` and loopback IP addresses (``127.0.0.0/8``, ``::1``)."""
    host = host.strip("[]")
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _hostname(host_header: str) -> str:
    """``Host`` header value without its port: ``127.0.0.1:8765`` -> ``127.0.0.1``."""
    return (urlsplit(f"//{host_header}").hostname or "").lower()


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
    tasks: list[str] = field(default_factory=list)
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
        with self._cond:
            return {
                "id": self.id,
                "status": self.status,
                "params": self.params,
                "targets": self.targets,
                "tags": self.tags,
                "tasks": self.tasks,
                "started": self.started,
                "finished": self.finished,
                "duration": self.result["duration"] if self.result else None,
                "error": self.error,
            }

    def to_dict(self) -> dict[str, Any]:
        with self._cond:
            return {**self.summary(), "events": list(self.events), "result": self.result}


def _string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise TypeError(f"{field_name!r} must be a list of strings")
    return value


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
        graph = self.workflow.validate(params, targets=targets or None, tags=tags or None)
        with self._lock:
            record = RunRecord(next(self._ids), params, targets, tags, tasks=list(graph))
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
        except BaseException as exc:
            # BaseException too: a task calling sys.exit() must not leave the run
            # "running" forever. The worker thread ends here either way.
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
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload: Any, status: int = HTTPStatus.OK) -> None:
        self._send(status, json.dumps(payload).encode(), "application/json")

    def _error(self, status: int, message: str) -> None:
        self._json({"error": message}, status)

    def _host_allowed(self) -> bool:
        """On a loopback bind, refuse other Host names (DNS rebinding protection)."""
        if not self.server.loopback_only:
            return True
        if _hostname(self.headers.get("Host", "")) in _LOOPBACK_NAMES:
            return True
        self.close_connection = True
        self._error(HTTPStatus.FORBIDDEN, "host not allowed")
        return False

    def _origin_allowed(self) -> bool:
        """Browsers send Origin on cross-site POSTs; it must match the Host we serve."""
        origin = self.headers.get("Origin")
        if origin is None:
            return True
        parts = urlsplit(origin)
        if parts.netloc.lower() == self.headers.get("Host", "").lower():
            return True
        self._error(HTTPStatus.FORBIDDEN, "cross-origin requests are not allowed")
        return False

    def _read_body(self) -> bytes | None:
        """Read the whole request body, so a keep-alive connection stays in sync."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length < 0:
                raise ValueError(length)
        except ValueError:
            self.close_connection = True  # can't find the end of the body
            self._error(HTTPStatus.BAD_REQUEST, "invalid Content-Length")
            return None
        return self.rfile.read(length)

    def _run_from_path(self, raw_id: str) -> RunRecord | None:
        record = self.server.dashboard.get(int(raw_id)) if raw_id.isdigit() else None
        if record is None:
            self._error(HTTPStatus.NOT_FOUND, f"no run {raw_id!r}")
        return record

    def do_GET(self) -> None:
        if not self._host_allowed():
            return
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
        raw = self._read_body()
        if raw is None or not self._host_allowed() or not self._origin_allowed():
            return
        if self.path.rstrip("/") != "/api/runs":
            self._error(HTTPStatus.NOT_FOUND, f"no route for {self.path}")
            return
        # Only "simple" content types can be sent cross-site without a CORS
        # preflight; requiring JSON means other pages can't start runs.
        content_type = self.headers.get("Content-Type", "").split(";")[0].strip().lower()
        if content_type != "application/json":
            self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json")
            return
        try:
            body = json.loads(raw or b"{}")
            if not isinstance(body, dict):
                raise TypeError("body must be a JSON object")
            params = body.get("params") or {}
            if not isinstance(params, dict):
                raise TypeError("'params' must be an object")
            record = self.server.dashboard.start(
                params,
                targets=_string_list(body.get("targets"), "targets"),
                tags=_string_list(body.get("tags"), "tags"),
            )
        except (ValueError, TypeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, f"invalid request: {exc}")
        except DagflowError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        else:
            self._json(record.summary(), HTTPStatus.CREATED)

    def _stream(self, record: RunRecord) -> None:
        """Push events from ``Last-Event-ID`` (default: the start) until the run ends.

        Event ``n`` is sent with ``id: n`` (1-based), so a browser that reconnects
        after a dropped connection resumes where it left off instead of replaying.
        """
        last_id = self.headers.get("Last-Event-ID", "")
        cursor = int(last_id) if last_id.isdigit() else 0
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            while True:
                events, done = record.wait(cursor, timeout=_KEEPALIVE_SECONDS)
                chunk = "".join(
                    f"id: {cursor + i}\ndata: {json.dumps(e)}\n\n"
                    for i, e in enumerate(events, start=1)
                )
                cursor += len(events)
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
        # Bound to a public interface on purpose: Host names can't be predicted.
        self.loopback_only = is_loopback(host)

    def server_bind(self) -> None:
        # HTTPServer.server_bind also calls socket.getfqdn(host), a reverse-DNS
        # lookup that can stall startup for seconds on 0.0.0.0. Nothing here
        # uses server_name, so skip the lookup.
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name, self.server_port = str(host), int(port)

    @property
    def page(self) -> bytes:
        return resources.files("dagflow").joinpath("ui/index.html").read_bytes()

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host!s}:{port}/"
