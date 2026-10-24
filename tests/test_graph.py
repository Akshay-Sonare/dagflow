import pytest

from dagflow import DAG, CycleError, UnknownTaskError


@pytest.fixture
def diamond() -> DAG:
    #   a
    #  / \
    # b   c
    #  \ /
    #   d
    return DAG("abcd", [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")])


def test_topological_order_is_deterministic(diamond: DAG) -> None:
    assert diamond.topological_order() == ["a", "b", "c", "d"]


def test_layers_group_parallel_nodes(diamond: DAG) -> None:
    assert diamond.layers() == [["a"], ["b", "c"], ["d"]]


def test_layers_use_longest_path() -> None:
    dag = DAG("abc", [("a", "b"), ("b", "c"), ("a", "c")])
    assert dag.layers() == [["a"], ["b"], ["c"]]


def test_roots_leaves_and_neighbours(diamond: DAG) -> None:
    assert diamond.roots() == ["a"]
    assert diamond.leaves() == ["d"]
    assert diamond.upstream("d") == {"b", "c"}
    assert diamond.downstream("a") == {"b", "c"}


def test_ancestors_and_descendants(diamond: DAG) -> None:
    assert diamond.ancestors("d") == {"a", "b", "c"}
    assert diamond.descendants("b") == {"d"}
    assert diamond.ancestors("a") == set()


def test_subgraph_keeps_only_required_nodes(diamond: DAG) -> None:
    sub = diamond.subgraph(["b"])
    assert sorted(sub) == ["a", "b"]
    assert sub.edges() == [("a", "b")]


def test_subgraph_rejects_unknown_target(diamond: DAG) -> None:
    with pytest.raises(UnknownTaskError):
        diamond.subgraph(["zzz"])


def test_edge_to_unknown_node_is_rejected() -> None:
    with pytest.raises(UnknownTaskError):
        DAG("a", [("a", "b")])


def test_cycle_is_reported_with_its_path() -> None:
    dag = DAG("abcx", [("x", "a"), ("a", "b"), ("b", "c"), ("c", "a")])
    with pytest.raises(CycleError) as info:
        dag.topological_order()
    assert info.value.cycle == ["a", "b", "c", "a"]


def test_self_loop_is_a_cycle() -> None:
    with pytest.raises(CycleError) as info:
        DAG("a", [("a", "a")]).topological_order()
    assert info.value.cycle == ["a", "a"]


def test_empty_graph() -> None:
    dag = DAG()
    assert dag.topological_order() == []
    assert dag.layers() == []


def test_exports(diamond: DAG) -> None:
    mermaid = diamond.to_mermaid()
    assert mermaid.startswith("graph TD")
    assert "a --> b" in mermaid
    dot = diamond.to_dot("demo")
    assert 'digraph "demo"' in dot
    assert '"c" -> "d";' in dot
