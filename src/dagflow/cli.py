"""Command-line interface: ``dagflow run | graph | plan | ui <file.py:workflow>``."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import sys
import webbrowser
from pathlib import Path
from typing import Any, ClassVar, TextIO

from dagflow import __version__
from dagflow.cache import SQLiteCache
from dagflow.errors import DagflowError
from dagflow.events import Event, EventType
from dagflow.executor import RunResult, TaskState
from dagflow.server import Dashboard, DashboardServer, coerce_param, is_loopback
from dagflow.workflow import Workflow


def split_spec(spec: str) -> tuple[str, str]:
    """Split ``target[:name]`` into ``(target, name)``; ``name`` defaults to ``workflow``.

    Only a trailing ``:identifier`` counts, so Windows drive letters such as
    ``C:\\flows\\etl.py`` stay part of the path.
    """
    head, sep, tail = spec.rpartition(":")
    if sep and head and tail.isidentifier():
        return head, tail
    return spec, "workflow"


def load_workflow(spec: str) -> Workflow:
    """Resolve ``path/to/file.py:name`` or ``package.module:name`` to a Workflow."""
    target, attr = split_spec(spec)
    if target.endswith(".py") or os.sep in target or (os.altsep and os.altsep in target):
        path = Path(target).resolve()
        if not path.is_file():
            raise DagflowError(f"no such file: {target}")
        module_name = f"_dagflow_user_{path.stem}"
        module_spec = importlib.util.spec_from_file_location(module_name, path)
        if module_spec is None or module_spec.loader is None:
            raise DagflowError(f"cannot import {target}")
        module = importlib.util.module_from_spec(module_spec)
        sys.modules[module_name] = module
        module_spec.loader.exec_module(module)
    else:
        module = importlib.import_module(target)
    found = getattr(module, attr, None)
    if not isinstance(found, Workflow):
        raise DagflowError(f"{spec!r} does not name a Workflow (looked for {attr!r})")
    return found


def parse_param(raw: str) -> tuple[str, Any]:
    """``key=value``; the value is parsed as JSON when possible, else kept as text."""
    key, sep, value = raw.partition("=")
    if not sep or not key.isidentifier():
        raise argparse.ArgumentTypeError(f"expected key=value, got {raw!r}")
    return key, coerce_param(value)


class ConsoleReporter:
    """Event listener that prints a live, optionally coloured progress log."""

    _STYLE: ClassVar[dict[EventType, tuple[str, str]]] = {
        EventType.TASK_STARTED: ("▶", "36"),
        EventType.TASK_RETRYING: ("↻", "33"),
        EventType.TASK_SUCCEEDED: ("✓", "32"),
        EventType.TASK_CACHED: ("●", "34"),
        EventType.TASK_FAILED: ("✗", "31"),
        EventType.TASK_SKIPPED: ("⊘", "90"),
        EventType.TASK_CANCELLED: ("■", "35"),
    }

    def __init__(self, stream: TextIO | None = None, *, color: bool | None = None) -> None:
        stream = stream or sys.stderr
        self.stream = stream
        if color is None:
            color = stream.isatty() and "NO_COLOR" not in os.environ
        self.color = color

    def _paint(self, text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def __call__(self, event: Event) -> None:
        if event.type is EventType.RUN_STARTED:
            self.stream.write(self._paint(f"running workflow {event.detail!r}\n", "1"))
            return
        if event.type is EventType.RUN_FINISHED:
            return
        symbol, code = self._STYLE[event.type]
        parts = [f"  {self._paint(symbol, code)} {event.task}"]
        if event.type is EventType.TASK_STARTED and event.attempt > 1:
            parts.append(f"(attempt {event.attempt})")
        if event.duration is not None:
            parts.append(self._paint(f"{event.duration:.2f}s", "90"))
        if event.error is not None:
            parts.append(f"— {type(event.error).__name__}: {event.error}")
        if event.detail:
            parts.append(self._paint(f"[{event.detail}]", "90"))
        self.stream.write(" ".join(parts) + "\n")
        self.stream.flush()


def format_summary(result: RunResult) -> str:
    width = max((len(name) for name in result.results), default=4)
    lines = [f"{'task':<{width}}  {'state':<9}  {'tries':>5}  {'time':>7}"]
    for name, r in result.results.items():
        lines.append(f"{name:<{width}}  {r.state:<9}  {r.attempts:>5}  {r.duration:>6.2f}s")
    counts = {state: len(result.by_state(state)) for state in TaskState}
    tally = ", ".join(f"{n} {state}" for state, n in counts.items() if n)
    verdict = "succeeded" if result.ok else "FAILED"
    lines.append(f"\nworkflow {result.workflow!r} {verdict} in {result.duration:.2f}s ({tally})")
    return "\n".join(lines)


def _cmd_run(args: argparse.Namespace) -> int:
    wf = load_workflow(args.workflow)
    listeners = [] if args.quiet else [ConsoleReporter()]
    cache = SQLiteCache(args.cache) if args.cache else None
    try:
        result = wf.run(
            dict(args.param),
            targets=args.target or None,
            tags=args.tag or None,
            max_concurrency=args.jobs,
            cache=cache,
            fail_fast=args.fail_fast,
            listeners=listeners,
        )
    finally:
        if cache is not None:
            cache.close()
    if args.json:
        print(json.dumps(result.to_dict(), indent=2, default=repr))
    else:
        print(format_summary(result), file=sys.stderr)
    return 0 if result.ok else 1


def _cmd_graph(args: argparse.Namespace) -> int:
    wf = load_workflow(args.workflow)
    dag = wf.dag()
    selected = wf.select(args.target, args.tag)
    if selected:
        dag = dag.subgraph(selected)
    print(dag.to_mermaid() if args.format == "mermaid" else dag.to_dot(wf.name))
    return 0


def _cmd_plan(args: argparse.Namespace) -> int:
    wf = load_workflow(args.workflow)
    for i, layer in enumerate(wf.plan(targets=args.target or None, tags=args.tag or None), start=1):
        print(f"stage {i}: {', '.join(layer)}")
    required = wf.required_params()
    if required:
        print("\nrequired params:")
        for param, users in sorted(required.items()):
            print(f"  {param}  (used by {', '.join(users)})")
    return 0


def _cmd_ui(args: argparse.Namespace) -> int:
    wf = load_workflow(args.workflow)
    cache = SQLiteCache(args.cache) if args.cache else None
    dashboard = Dashboard(wf, cache=cache, max_concurrency=args.jobs)
    try:
        server = DashboardServer(dashboard, args.host, args.port, verbose=args.verbose)
    except OSError as exc:
        raise DagflowError(f"cannot listen on {args.host}:{args.port}: {exc}") from None
    print(f"dagflow dashboard for {wf.name!r} at {server.url}  (Ctrl+C to stop)", file=sys.stderr)
    if not is_loopback(args.host):
        print(
            f"warning: listening on {args.host}, not just this computer. Anyone who can reach\n"
            "this port can run the workflow with any parameters and read every result.\n"
            "The dashboard has no login; only do this on a network you trust.",
            file=sys.stderr,
        )
    if not args.no_browser:
        webbrowser.open(server.url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)
    finally:
        server.server_close()
        if cache is not None:
            cache.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dagflow", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("workflow", help="path/to/file.py[:name] or module[:name]")
        p.add_argument(
            "-t", "--target", action="append", help="only this task and its dependencies"
        )
        p.add_argument(
            "-T", "--tag", action="append", help="also select tasks with this tag (and their deps)"
        )

    run = sub.add_parser("run", help="execute a workflow")
    common(run)
    run.add_argument(
        "-p", "--param", action="append", type=parse_param, default=[], metavar="KEY=VALUE"
    )
    run.add_argument("-j", "--jobs", type=int, default=8, help="max concurrent tasks")
    run.add_argument("--cache", metavar="PATH", help="SQLite file for cached task results")
    run.add_argument("--fail-fast", action="store_true", help="cancel everything on failure")
    run.add_argument("--json", action="store_true", help="print results as JSON on stdout")
    run.add_argument("-q", "--quiet", action="store_true", help="no live progress output")
    run.set_defaults(handler=_cmd_run)

    graph = sub.add_parser("graph", help="print the dependency graph")
    common(graph)
    graph.add_argument("-f", "--format", choices=("mermaid", "dot"), default="mermaid")
    graph.set_defaults(handler=_cmd_graph)

    plan = sub.add_parser("plan", help="show execution stages without running")
    common(plan)
    plan.set_defaults(handler=_cmd_plan)

    ui = sub.add_parser("ui", help="open a live web dashboard for a workflow")
    ui.add_argument("workflow", help="path/to/file.py[:name] or module[:name]")
    ui.add_argument(
        "--host", default="127.0.0.1", help="interface to bind (default: localhost only)"
    )
    ui.add_argument("--port", type=int, default=8765)
    ui.add_argument("-j", "--jobs", type=int, default=8, help="max concurrent tasks per run")
    ui.add_argument("--cache", metavar="PATH", help="SQLite file for cached task results")
    ui.add_argument("--no-browser", action="store_true", help="don't open a browser tab")
    ui.add_argument("-v", "--verbose", action="store_true", help="log every HTTP request")
    ui.set_defaults(handler=_cmd_ui)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        code: int = args.handler(args)
    except DagflowError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return code


if __name__ == "__main__":
    raise SystemExit(main())
