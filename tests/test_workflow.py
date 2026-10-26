import pytest

from dagflow import (
    CycleError,
    DuplicateTaskError,
    MissingInputError,
    RetryPolicy,
    UnknownTaskError,
    Workflow,
    task,
)


def test_parameters_become_dependencies() -> None:
    wf = Workflow()

    @wf.task
    def raw() -> list[int]:
        return [3, 1, 2]

    @wf.task
    def ordered(raw: list[int]) -> list[int]:
        return sorted(raw)

    assert wf.dag().edges() == [("raw", "ordered")]
    assert wf.run()["ordered"] == [1, 2, 3]


def test_tasks_remain_plain_callables() -> None:
    @task
    def double(x: int) -> int:
        return x * 2

    assert double(21) == 42
    assert double.name == "double"


def test_after_adds_ordering_without_data() -> None:
    wf = Workflow()
    order: list[str] = []

    @wf.task
    def setup() -> None:
        order.append("setup")

    @wf.task(after=["setup"])
    def work() -> None:
        order.append("work")

    wf.run(max_concurrency=4)
    assert order == ["setup", "work"]


def test_after_accepts_task_objects() -> None:
    wf = Workflow()
    first = wf.task(name="first")(lambda: 1)
    wf.task(name="second", after=[first])(lambda: 2)
    assert wf.dag().edges() == [("first", "second")]


def test_duplicate_names_are_rejected() -> None:
    wf = Workflow()
    wf.task(name="a")(lambda: 1)
    with pytest.raises(DuplicateTaskError):
        wf.task(name="a")(lambda: 2)


def test_unknown_after_is_rejected() -> None:
    wf = Workflow()
    wf.task(name="a", after=["ghost"])(lambda: 1)
    with pytest.raises(UnknownTaskError, match="ghost"):
        wf.dag()


def test_cycles_are_rejected_before_running() -> None:
    wf = Workflow()
    ran: list[str] = []

    @wf.task
    def a(b: int) -> int:
        ran.append("a")
        return b

    @wf.task
    def b(a: int) -> int:
        ran.append("b")
        return a

    with pytest.raises(CycleError):
        wf.run()
    assert ran == []


def test_missing_params_are_reported() -> None:
    wf = Workflow()

    @wf.task
    def greet(name: str, punctuation: str = "!") -> str:
        return f"hi {name}{punctuation}"

    assert wf.required_params() == {"name": ["greet"]}
    with pytest.raises(MissingInputError, match="name"):
        wf.run()
    assert wf.run({"name": "Ada"})["greet"] == "hi Ada!"


def test_targets_run_only_the_needed_subgraph() -> None:
    wf = Workflow()
    ran: list[str] = []

    @wf.task
    def base() -> int:
        ran.append("base")
        return 1

    @wf.task
    def wanted(base: int) -> int:
        ran.append("wanted")
        return base + 1

    @wf.task
    def unrelated(needs_param: int) -> int:
        ran.append("unrelated")
        return needs_param

    # ``unrelated`` would fail validation, but it isn't part of the target subgraph.
    result = wf.run(targets=["wanted"])
    assert result["wanted"] == 2
    assert sorted(ran) == ["base", "wanted"]
    assert "unrelated" not in result


def test_plan_layers() -> None:
    wf = Workflow()
    wf.task(name="a")(lambda: 1)
    wf.task(name="b")(lambda: 1)

    @wf.task
    def c(a: int, b: int) -> int:
        return a + b

    assert wf.plan() == [["a", "b"], ["c"]]


def test_task_validation() -> None:
    def variadic(*args: int) -> None: ...

    def noop() -> None: ...

    with pytest.raises(TypeError, match="keyword"):
        task(variadic)
    with pytest.raises(ValueError, match="timeout"):
        task(timeout=0)(noop)
    with pytest.raises(ValueError, match="identifier"):
        task(name="not valid")(noop)


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 0.5), (2, 1.0), (3, 2.0), (10, 5.0)],
)
def test_retry_policy_backoff(attempt: int, expected: float) -> None:
    policy = RetryPolicy(max_attempts=20, backoff=0.5, max_backoff=5.0)
    assert policy.delay(attempt) == expected


def test_retry_policy_jitter_shortens_delay_within_bounds() -> None:
    policy = RetryPolicy(max_attempts=5, backoff=1.0, jitter=0.5)
    delays = {policy.delay(2) for _ in range(50)}
    assert len(delays) > 1
    assert all(1.0 <= d <= 2.0 for d in delays)


def _noop() -> None:
    return None


def test_retry_policy_jitter_validation() -> None:
    with pytest.raises(ValueError, match="jitter"):
        RetryPolicy(jitter=1.5)
    with pytest.raises(ValueError, match="jitter"):
        task(jitter=-0.1)(_noop)


def test_task_decorator_passes_jitter() -> None:
    assert task(retries=2, backoff=1, jitter=0.3)(_noop).retry.jitter == 0.3


def test_retry_policy_filters_exceptions() -> None:
    policy = RetryPolicy(max_attempts=3, retry_on=(ConnectionError,))
    assert policy.should_retry(ConnectionError(), 1)
    assert not policy.should_retry(ValueError(), 1)
    assert not policy.should_retry(ConnectionError(), 3)
