import asyncio
import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest

from dagflow import MemoryCache, Workflow
from dagflow.server import Dashboard, DashboardServer, coerce_param, describe_workflow


def build_workflow() -> Workflow:
    wf = Workflow("demo")

    @wf.task(tags={"extract"})
    async def numbers(count: int = 3) -> list[int]:
        """Produce some numbers."""
        await asyncio.sleep(0.01)
        return list(range(count))

    @wf.task(retries=1, cache=True)
    def total(numbers: list[int], offset: int) -> int:
        return sum(numbers) + offset

    @wf.task
    def report(total: int, explode: bool = False) -> str:
        if explode:
            raise RuntimeError("kaboom")
        return f"total={total}"

    return wf


@pytest.fixture
def server() -> Iterator[DashboardServer]:
    srv = DashboardServer(Dashboard(build_workflow(), cache=MemoryCache()), port=0)
    thread = threading.Thread(target=srv.serve_forever, args=(0.02,), daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def request(srv: DashboardServer, path: str, body: Any = None) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        srv.url.rstrip("/") + path, data=data, method="POST" if data else "GET"
    )
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            raw = res.read()
            status = res.status
    except urllib.error.HTTPError as err:
        raw, status = err.read(), err.code
    try:
        return status, json.loads(raw)
    except json.JSONDecodeError:
        return status, raw.decode()


def stream(srv: DashboardServer, run_id: int) -> list[dict[str, Any]]:
    """Read the SSE stream to the end and return the decoded events."""
    events: list[dict[str, Any]] = []
    with urllib.request.urlopen(f"{srv.url}api/runs/{run_id}/events", timeout=5) as res:
        assert res.headers["Content-Type"] == "text/event-stream"
        for raw in res:
            line = raw.decode().rstrip("\n")
            if line == "event: end":
                break
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
    return events


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("3", 3), ("true", True), ("EU", "EU"), ('["a"]', ["a"]), (5, 5)],
)
def test_coerce_param(raw: object, expected: object) -> None:
    assert coerce_param(raw) == expected


def test_describe_workflow() -> None:
    info = describe_workflow(build_workflow())
    assert info["name"] == "demo"
    assert info["layers"] == [["numbers"], ["total"], ["report"]]
    assert [list(edge) for edge in info["edges"]] == [["numbers", "total"], ["total", "report"]]
    assert info["tags"] == ["extract"]
    params = {p["name"]: p for p in info["params"]}
    assert params["offset"]["required"] is True
    assert params["count"]["default"] == 3
    assert params["explode"]["used_by"] == ["report"]
    assert info["params"][0]["name"] == "offset"  # required params first
    numbers = next(t for t in info["tasks"] if t["name"] == "numbers")
    assert numbers["doc"] == "Produce some numbers."
    assert numbers["is_async"] is True
    total = next(t for t in info["tasks"] if t["name"] == "total")
    assert total["upstream"] == ["numbers"]
    assert total["retries"] == 1
    assert total["cache"] is True


def test_serves_the_dashboard_page(server: DashboardServer) -> None:
    status, body = request(server, "/")
    assert status == 200
    assert "<title>dagflow dashboard</title>" in body


def test_workflow_endpoint(server: DashboardServer) -> None:
    status, body = request(server, "/api/workflow")
    assert status == 200
    assert [t["name"] for t in body["tasks"]] == ["numbers", "total", "report"]


def test_run_streams_events_and_records_result(server: DashboardServer) -> None:
    status, run = request(server, "/api/runs", {"params": {"offset": "10", "count": "4"}})
    assert status == 201
    assert run["status"] == "running"
    assert run["params"] == {"offset": 10, "count": 4}

    events = stream(server, run["id"])
    types = [e["type"] for e in events]
    assert types[0] == "run_started"
    assert types[-1] == "run_finished"
    assert types.count("task_succeeded") == 3

    status, detail = request(server, f"/api/runs/{run['id']}")
    assert status == 200
    assert detail["status"] == "succeeded"
    assert detail["result"]["tasks"]["report"]["value"] == "total=16"
    assert len(detail["events"]) == len(events)


def test_stream_replays_finished_runs(server: DashboardServer) -> None:
    _, run = request(server, "/api/runs", {"params": {"offset": 1}})
    first = stream(server, run["id"])
    assert stream(server, run["id"]) == first


def test_failed_run_is_reported(server: DashboardServer) -> None:
    _, run = request(server, "/api/runs", {"params": {"offset": 0, "explode": "true"}})
    events = stream(server, run["id"])
    failed = [e for e in events if e["type"] == "task_failed"]
    assert failed[0]["task"] == "report"
    assert failed[0]["error"] == "RuntimeError: kaboom"
    _, detail = request(server, f"/api/runs/{run['id']}")
    assert detail["status"] == "failed"


def test_cache_is_shared_between_runs(server: DashboardServer) -> None:
    for _ in range(2):
        _, run = request(server, "/api/runs", {"params": {"offset": 5}})
        events = stream(server, run["id"])
    assert any(e["type"] == "task_cached" and e["task"] == "total" for e in events)


def test_tag_selection_runs_only_the_subgraph(server: DashboardServer) -> None:
    _, run = request(server, "/api/runs", {"tags": ["extract"]})
    events = stream(server, run["id"])
    assert {e["task"] for e in events if e["task"]} == {"numbers"}


def test_history_lists_newest_first(server: DashboardServer) -> None:
    ids = []
    for _ in range(2):
        _, run = request(server, "/api/runs", {"params": {"offset": 0}})
        stream(server, run["id"])
        ids.append(run["id"])
    _, history = request(server, "/api/runs")
    assert [r["id"] for r in history] == ids[::-1]
    assert history[0]["status"] == "succeeded"


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"params": {}}, "offset"),  # missing required param
        ({"params": {"offset": 1}, "tags": ["nope"]}, "nope"),
        ([1, 2], "JSON object"),
    ],
)
def test_bad_run_requests(server: DashboardServer, body: Any, message: str) -> None:
    status, payload = request(server, "/api/runs", body)
    assert status == 400
    assert message in payload["error"]


def test_unknown_routes_and_runs(server: DashboardServer) -> None:
    assert request(server, "/api/runs/999")[0] == 404
    assert request(server, "/api/runs/abc/events")[0] == 404
    assert request(server, "/nope")[0] == 404


def test_history_is_bounded() -> None:
    dashboard = Dashboard(build_workflow(), history=2)
    records = [dashboard.start({"offset": 0}) for _ in range(3)]
    for record in records:
        record.wait(0, timeout=0)
    assert [r.id for r in dashboard.runs()] == [3, 2]
    assert dashboard.get(1) is None
