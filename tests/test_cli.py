import argparse
import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from dagflow.cache import SQLiteCache
from dagflow.cli import load_workflow, main, parse_param, split_spec

WORKFLOW_SOURCE = """
from dagflow import Workflow

workflow = Workflow("cli_demo")
other = Workflow("other")

@workflow.task
def numbers(count: int = 3):
    return list(range(count))

@workflow.task(tags=["summary"])
def total(numbers):
    return sum(numbers)

@workflow.task
def broken(total, explode: bool = False):
    if explode:
        raise RuntimeError("kaboom")
    return total
"""


@pytest.fixture
def wf_file(tmp_path: Path) -> Path:
    path = tmp_path / "pipeline.py"
    path.write_text(WORKFLOW_SOURCE)
    return path


EXAMPLE = Path(__file__).parent.parent / "examples" / "sales_report.py"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("n=3", ("n", 3)),
        ("flag=true", ("flag", True)),
        ("name=Ada", ("name", "Ada")),
        ("xs=[1, 2]", ("xs", [1, 2])),
        ("empty=", ("empty", "")),
    ],
)
def test_parse_param(raw: str, expected: tuple[str, object]) -> None:
    assert parse_param(raw) == expected


@pytest.mark.parametrize("raw", ["novalue", "=1", "bad key=1"])
def test_parse_param_rejects_garbage(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_param(raw)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("flows/etl.py", ("flows/etl.py", "workflow")),
        ("flows/etl.py:nightly", ("flows/etl.py", "nightly")),
        ("pkg.module:wf", ("pkg.module", "wf")),
        ("C:\\flows\\etl.py", ("C:\\flows\\etl.py", "workflow")),
        ("C:\\flows\\etl.py:nightly", ("C:\\flows\\etl.py", "nightly")),
    ],
)
def test_split_spec(spec: str, expected: tuple[str, str]) -> None:
    assert split_spec(spec) == expected


def test_load_workflow_by_path_and_attribute(wf_file: Path) -> None:
    assert load_workflow(str(wf_file)).name == "cli_demo"
    assert load_workflow(f"{wf_file}:other").name == "other"


def test_run_json_output(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "-p", "count=5", "--json", "-q"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["tasks"]["total"]["value"] == 10


def test_run_failure_exit_code(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "-p", "explode=true"]) == 1
    err = capsys.readouterr().err
    assert "kaboom" in err
    assert "FAILED" in err


def test_run_with_target(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "-t", "numbers", "--json", "-q"]) == 0
    assert list(json.loads(capsys.readouterr().out)["tasks"]) == ["numbers"]


def test_run_with_tag(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "-T", "summary", "--json", "-q"]) == 0
    assert list(json.loads(capsys.readouterr().out)["tasks"]) == ["numbers", "total"]
    assert main(["plan", str(wf_file), "-T", "summary"]) == 0
    assert "broken" not in capsys.readouterr().out
    assert main(["graph", str(wf_file), "-T", "summary"]) == 0
    assert "broken" not in capsys.readouterr().out
    assert main(["run", str(wf_file), "-T", "nope", "-q"]) == 2


def test_graph_and_plan(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["graph", str(wf_file)]) == 0
    assert "numbers --> total" in capsys.readouterr().out
    assert main(["graph", str(wf_file), "--format", "dot"]) == 0
    assert '"total" -> "broken";' in capsys.readouterr().out
    assert main(["plan", str(wf_file)]) == 0
    assert "stage 2: total" in capsys.readouterr().out


def test_errors_exit_with_code_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(tmp_path / "missing.py")]) == 2
    assert "no such file" in capsys.readouterr().err


def test_example_workflow_runs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["run", str(EXAMPLE), "--cache", str(tmp_path / "c.db"), "-p", "top_n=2"])
    out, err = capsys.readouterr()
    assert code == 0
    assert "Top customers (EU)" in out
    assert "↻ orders" in err  # the flaky task was retried


def test_ui_reports_busy_port(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        assert main(["ui", str(wf_file), "--port", str(port), "--no-browser"]) == 2
    assert "cannot listen" in capsys.readouterr().err


@pytest.mark.parametrize(("host", "warned"), [("127.0.0.1", False), ("0.0.0.0", True)])
def test_ui_warns_when_exposed_beyond_localhost(
    wf_file: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    host: str,
    warned: bool,
) -> None:
    def stop_immediately(self: object, poll_interval: float = 0.5) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("dagflow.cli.DashboardServer.serve_forever", stop_immediately)
    assert main(["ui", str(wf_file), "--host", host, "--port", "0", "--no-browser"]) == 0
    assert ("warning: listening on" in capsys.readouterr().err) is warned


def test_run_dry_run_executes_nothing(
    wf_file: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    marker = tmp_path / "ran"
    src = wf_file.read_text() + f"\n@workflow.task\ndef side(): open({str(marker)!r}, 'w')\n"
    wf_file.write_text(src)
    assert main(["run", str(wf_file), "--dry-run"]) == 0
    out = capsys.readouterr()
    assert "stage 1: numbers, side" in out.out
    assert "nothing was executed" in out.err
    assert not marker.exists()


def test_run_dry_run_reports_missing_params(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "needs.py"
    path.write_text(
        "from dagflow import Workflow\nworkflow = Workflow('n')\n"
        "@workflow.task\ndef a(region): return region\n"
    )
    assert main(["run", str(path), "--dry-run"]) == 2
    assert "region" in capsys.readouterr().err
    assert main(["run", str(path), "--dry-run", "-p", "region=EU"]) == 0


def test_summary_reports_critical_path(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "--json", "-q"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["critical_path"]
    assert all(name in out["tasks"] for name in out["critical_path"])


def test_run_events_written_as_json_lines(wf_file: Path, tmp_path: Path) -> None:
    out = tmp_path / "events.jsonl"
    assert main(["run", str(wf_file), "-q", "--events", str(out)]) == 0
    events = [json.loads(line) for line in out.read_text().splitlines()]
    assert events[0]["type"] == "run_started"
    assert events[-1]["type"] == "run_finished"
    assert {e["task"] for e in events if e["type"] == "task_succeeded"} == {
        "numbers",
        "total",
        "broken",
    }


def test_run_events_to_stdout(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(wf_file), "-q", "--events", "-"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert all(json.loads(line)["type"] for line in lines)


def test_run_events_stdout_conflicts_with_json(
    wf_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", str(wf_file), "--events", "-", "--json"]) == 2
    assert "stdout" in capsys.readouterr().err


def test_summary_reports_cache_hit_rate(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "c.py"
    path.write_text(
        "from dagflow import Workflow\nworkflow = Workflow('c')\n"
        "@workflow.task(cache=True)\ndef a(): return 1\n"
    )
    cache = str(tmp_path / "cache.db")
    assert main(["run", str(path), "-q", "--cache", cache]) == 0
    assert "cache: 0 hit, 1 miss (0%)" in capsys.readouterr().err
    assert main(["run", str(path), "-q", "--cache", cache]) == 0
    assert "cache: 1 hit, 0 miss (100%)" in capsys.readouterr().err


PRICE_EXAMPLE = Path(__file__).parent.parent / "examples" / "price_check.py"


def test_fan_out_example_output(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", str(PRICE_EXAMPLE), "-j", "4"]) == 0
    out, err = capsys.readouterr()
    assert "Checked 11 products" in out  # 12 SKUs, one discontinued and skipped
    assert "⇉ price fans out over 12 items" in err
    assert "↻ price[5]" in err  # the flaky SKU's retry is shown with its index
    assert "⊘ price[8]" in err
    assert "▶ price[" not in err  # per-item progress is kept out of the terminal
    assert "price ×12" in err  # summary row


def test_plan_marks_mapped_tasks(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", str(PRICE_EXAMPLE)]) == 0
    assert "stage 2: price (map over skus)" in capsys.readouterr().out


def test_events_jsonl_carries_item_index(tmp_path: Path) -> None:
    out = tmp_path / "events.jsonl"
    assert main(["run", str(PRICE_EXAMPLE), "-q", "--events", str(out)]) == 0
    events = [json.loads(line) for line in out.read_text().splitlines()]
    mapped = [e for e in events if e["type"] == "task_mapped"]
    assert mapped[0]["items"] == 12
    assert {e["index"] for e in events if e["task"] == "price" and e["index"] is not None} == set(
        range(12)
    )


def test_plan_json(wf_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", str(wf_file), "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["workflow"] == "cli_demo"
    assert [[t["name"] for t in stage] for stage in doc["stages"]] == [
        ["numbers"],
        ["total"],
        ["broken"],
    ]
    total = doc["stages"][1][0]
    assert total["depends_on"] == ["numbers"]
    assert total["tags"] == ["summary"]
    assert doc["required_params"] == {}


def test_plan_json_marks_mapped_tasks(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", str(PRICE_EXAMPLE), "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    mapped = [t for stage in doc["stages"] for t in stage if t["map"]]
    assert [(t["name"], t["map"]) for t in mapped] == [("price", "skus")]


def test_python_dash_m_dagflow_runs_the_cli() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "dagflow", "--help"], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0
    assert "dagflow" in proc.stdout


def _seed_cache(path: Path) -> None:
    with SQLiteCache(path) as cache:
        cache.set("a", [1, 2, 3])
        cache.set("b", "x" * 100)


def test_cache_info_and_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "c.db"
    _seed_cache(db)
    assert main(["cache", "info", str(db)]) == 0
    assert "2 entries" in capsys.readouterr().out
    assert main(["cache", "info", str(db), "--json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["entries"] == 2
    assert info["bytes"] > 100


def test_cache_clear_all_and_older_than(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "c.db"
    _seed_cache(db)
    assert main(["cache", "clear", str(db), "--older-than", "3600"]) == 0
    assert "removed 0 of 2" in capsys.readouterr().out
    assert main(["cache", "clear", str(db), "--older-than", "0"]) == 0
    assert "removed 2 of 2" in capsys.readouterr().out
    _seed_cache(db)
    assert main(["cache", "clear", str(db)]) == 0
    with SQLiteCache(db) as cache:
        assert len(cache) == 0
        assert cache.stats()["oldest"] is None


def test_cache_errors(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "nope.db"
    assert main(["cache", "info", str(missing)]) == 2
    assert not missing.exists()  # must not create an empty database
    junk = tmp_path / "junk.db"
    junk.write_text("not a database" * 100)
    assert main(["cache", "info", str(junk)]) == 2
    assert "error:" in capsys.readouterr().err
