import asyncio
import http.client
import json
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from dagflow import MemoryCache, Workflow
from dagflow.cli import load_workflow
from dagflow.server import (
    Dashboard,
    DashboardServer,
    coerce_param,
    describe_workflow,
    is_loopback,
)


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
    serve(srv)
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


def stream(
    srv: DashboardServer, run_id: int, *, last_event_id: int | None = None
) -> list[dict[str, Any]]:
    """Read the SSE stream to the end; each decoded event gets its SSE ``id`` as ``_id``."""
    events: list[dict[str, Any]] = []
    req = urllib.request.Request(f"{srv.url}api/runs/{run_id}/events")
    if last_event_id is not None:
        req.add_header("Last-Event-ID", str(last_event_id))
    event_id = None
    with urllib.request.urlopen(req, timeout=5) as res:
        assert res.headers["Content-Type"] == "text/event-stream"
        for raw in res:
            line = raw.decode().rstrip("\n")
            if line == "event: end":
                break
            if line.startswith("id: "):
                event_id = int(line[4:])
            elif line.startswith("data: "):
                events.append({**json.loads(line[6:]), "_id": event_id})
    return events


def raw_request(
    srv: DashboardServer,
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    conn: http.client.HTTPConnection | None = None,
) -> tuple[int, Any]:
    """Like ``request`` but with full control over headers and the connection."""
    conn = conn or http.client.HTTPConnection("127.0.0.1", srv.server_address[1], timeout=5)
    conn.request(method, path, body=body, headers=headers or {})
    res = conn.getresponse()
    raw = res.read()
    try:
        return res.status, json.loads(raw)
    except json.JSONDecodeError:
        return res.status, raw.decode()


def serve(srv: DashboardServer) -> threading.Thread:
    thread = threading.Thread(target=srv.serve_forever, args=(0.02,), daemon=True)
    thread.start()
    return thread


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


EXAMPLE = Path(__file__).parent.parent / "examples" / "sales_report.py"


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("localhost", True),
        ("127.0.0.1", True),
        ("127.5.0.1", True),
        ("::1", True),
        ("[::1]", True),
        ("0.0.0.0", False),
        ("192.168.1.20", False),
        ("example.com", False),
    ],
)
def test_is_loopback(host: str, expected: bool) -> None:
    assert is_loopback(host) is expected


@pytest.mark.parametrize("path", ["/", "/api/workflow", "/api/runs"])
def test_foreign_host_header_is_rejected(server: DashboardServer, path: str) -> None:
    # A DNS-rebinding page reaches 127.0.0.1 but still sends its own hostname.
    status, payload = raw_request(server, "GET", path, headers={"Host": "evil.example:8765"})
    assert status == 403
    assert payload["error"] == "host not allowed"


def test_foreign_host_cannot_start_runs(server: DashboardServer) -> None:
    status, _ = raw_request(
        server,
        "POST",
        "/api/runs",
        body=b'{"params": {"offset": 1}}',
        headers={"Host": "evil.example", "Content-Type": "application/json"},
    )
    assert status == 403
    assert server.dashboard.runs() == []


def test_cross_origin_post_is_rejected(server: DashboardServer) -> None:
    status, payload = raw_request(
        server,
        "POST",
        "/api/runs",
        body=b'{"params": {"offset": 1}}',
        headers={"Content-Type": "application/json", "Origin": "https://evil.example"},
    )
    assert status == 403
    assert "cross-origin" in payload["error"]
    assert server.dashboard.runs() == []


def test_same_origin_post_is_allowed(server: DashboardServer) -> None:
    host = f"127.0.0.1:{server.server_address[1]}"
    status, run = raw_request(
        server,
        "POST",
        "/api/runs",
        body=b'{"params": {"offset": 1}}',
        headers={"Content-Type": "application/json", "Origin": f"http://{host}", "Host": host},
    )
    assert status == 201
    stream(server, run["id"])


@pytest.mark.parametrize("content_type", ["text/plain", "application/x-www-form-urlencoded", ""])
def test_post_requires_json_content_type(server: DashboardServer, content_type: str) -> None:
    # These are the "simple" types a page on another site can send without a preflight.
    headers = {"Content-Type": content_type} if content_type else {}
    status, payload = raw_request(
        server, "POST", "/api/runs", body=b'{"params": {"offset": 1}}', headers=headers
    )
    assert status == 415
    assert "application/json" in payload["error"]
    assert server.dashboard.runs() == []


def test_public_bind_accepts_any_host() -> None:
    srv = DashboardServer(Dashboard(build_workflow()), host="0.0.0.0", port=0)
    serve(srv)
    try:
        status, _ = raw_request(srv, "GET", "/api/workflow", headers={"Host": "my-laptop.lan"})
        assert status == 200
    finally:
        srv.shutdown()
        srv.server_close()


def test_wrong_post_path_keeps_connection_in_sync(server: DashboardServer) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    status, _ = raw_request(
        server,
        "POST",
        "/api/nope",
        body=b'{"params": {"offset": 1}}',
        headers={"Content-Type": "application/json"},
        conn=conn,
    )
    assert status == 404
    # The unread body would otherwise be parsed as this request's first line.
    status, body = raw_request(server, "GET", "/api/workflow", conn=conn)
    assert status == 200
    assert body["name"] == "demo"


def test_invalid_content_length(server: DashboardServer) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    conn.putrequest("POST", "/api/runs")
    conn.putheader("Content-Type", "application/json")
    conn.putheader("Content-Length", "nope")
    conn.endheaders()
    res = conn.getresponse()
    assert res.status == 400
    assert res.getheader("Connection") == "close"


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"params": {"offset": 1}, "tags": "extract"}, "'tags' must be a list of strings"),
        ({"params": {"offset": 1}, "targets": [1]}, "'targets' must be a list of strings"),
        ({"params": [["offset", 1]]}, "'params' must be an object"),
    ],
)
def test_request_field_types_are_validated(
    server: DashboardServer, body: Any, message: str
) -> None:
    status, payload = request(server, "/api/runs", body)
    assert status == 400
    assert message in payload["error"]


def test_run_records_its_selected_tasks(server: DashboardServer) -> None:
    _, everything = request(server, "/api/runs", {"params": {"offset": 0}})
    _, tagged = request(server, "/api/runs", {"tags": ["extract"]})
    assert sorted(everything["tasks"]) == ["numbers", "report", "total"]
    assert tagged["tasks"] == ["numbers"]
    stream(server, everything["id"])
    stream(server, tagged["id"])


def test_events_carry_ids_and_last_event_id_resumes(server: DashboardServer) -> None:
    _, run = request(server, "/api/runs", {"params": {"offset": 1}})
    events = stream(server, run["id"])
    assert [e["_id"] for e in events] == list(range(1, len(events) + 1))
    resumed = stream(server, run["id"], last_event_id=3)
    assert resumed == events[3:]


def test_base_exception_in_task_still_finishes_the_run() -> None:
    wf = Workflow("exits")

    @wf.task
    def quit_early() -> None:
        sys.exit(3)

    srv = DashboardServer(Dashboard(wf), port=0)
    serve(srv)
    try:
        _, run = request(srv, "/api/runs", {})
        stream(srv, run["id"])  # must terminate rather than keep-alive forever
        _, detail = request(srv, f"/api/runs/{run['id']}")
        assert detail["status"] == "error"
        assert detail["error"] == "SystemExit: 3"
    finally:
        srv.shutdown()
        srv.server_close()


def test_example_survives_concurrent_dashboard_runs() -> None:
    dashboard = Dashboard(load_workflow(str(EXAMPLE)))
    records = [dashboard.start({}) for _ in range(3)]
    for record in records:
        while not record.wait(len(record.events), timeout=5)[1]:
            pass
    assert [r.status for r in records] == ["succeeded"] * 3
    for record in records:
        retries = [e for e in record.events if e["type"] == "task_retrying"]
        assert len(retries) == 2  # every run still demonstrates the retry policy
