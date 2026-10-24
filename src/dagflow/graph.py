"""A small directed-acyclic-graph type with the algorithms a scheduler needs."""

from __future__ import annotations

import heapq
import re
from collections import deque
from collections.abc import Iterable, Iterator, Mapping

from dagflow.errors import CycleError, UnknownTaskError


class DAG:
    """Directed graph of named nodes; edges point from upstream to downstream.

    Cycles are allowed while building and reported by :meth:`topological_order`,
    which every scheduling method relies on. Iteration order is deterministic.
    """

    __slots__ = ("_down", "_up")

    def __init__(self, nodes: Iterable[str] = (), edges: Iterable[tuple[str, str]] = ()) -> None:
        self._up: dict[str, set[str]] = {}
        self._down: dict[str, set[str]] = {}
        for node in nodes:
            self.add_node(node)
        for upstream, downstream in edges:
            self.add_edge(upstream, downstream)

    def add_node(self, node: str) -> None:
        self._up.setdefault(node, set())
        self._down.setdefault(node, set())

    def add_edge(self, upstream: str, downstream: str) -> None:
        for node in (upstream, downstream):
            if node not in self._up:
                raise UnknownTaskError(node)
        self._up[downstream].add(upstream)
        self._down[upstream].add(downstream)

    def __contains__(self, node: object) -> bool:
        return node in self._up

    def __iter__(self) -> Iterator[str]:
        return iter(self._up)

    def __len__(self) -> int:
        return len(self._up)

    def __repr__(self) -> str:
        return f"DAG(nodes={len(self)}, edges={len(self.edges())})"

    def upstream(self, node: str) -> frozenset[str]:
        return frozenset(self._up[node])

    def downstream(self, node: str) -> frozenset[str]:
        return frozenset(self._down[node])

    def edges(self) -> list[tuple[str, str]]:
        return sorted((u, d) for u, downs in self._down.items() for d in downs)

    def roots(self) -> list[str]:
        return sorted(n for n, ups in self._up.items() if not ups)

    def leaves(self) -> list[str]:
        return sorted(n for n, downs in self._down.items() if not downs)

    def ancestors(self, node: str) -> set[str]:
        return self._walk(node, self._up)

    def descendants(self, node: str) -> set[str]:
        return self._walk(node, self._down)

    @staticmethod
    def _walk(start: str, adjacency: Mapping[str, set[str]]) -> set[str]:
        seen: set[str] = set()
        queue = deque(adjacency[start])
        while queue:
            node = queue.popleft()
            if node not in seen:
                seen.add(node)
                queue.extend(adjacency[node] - seen)
        return seen

    def topological_order(self) -> list[str]:
        """Kahn's algorithm; ties broken alphabetically so the order is stable.

        Raises:
            CycleError: with one offending cycle, if the graph is not acyclic.
        """
        indegree = {node: len(ups) for node, ups in self._up.items()}
        heap = [node for node, degree in indegree.items() if degree == 0]
        heapq.heapify(heap)
        order: list[str] = []
        while heap:
            node = heapq.heappop(heap)
            order.append(node)
            for child in self._down[node]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(heap, child)
        if len(order) != len(self._up):
            raise CycleError(self._find_cycle({n for n, d in indegree.items() if d > 0}))
        return order

    def _find_cycle(self, candidates: set[str]) -> list[str]:
        """Iterative DFS restricted to ``candidates``; returns ``[a, b, ..., a]``."""
        on_path: dict[str, int] = {}
        finished: set[str] = set()
        for start in sorted(candidates):
            if start in finished:
                continue
            path = [start]
            on_path[start] = 0
            stack = [iter(sorted(self._down[start] & candidates))]
            while stack:
                child = next(stack[-1], None)
                if child is None:
                    stack.pop()
                    done = path.pop()
                    del on_path[done]
                    finished.add(done)
                elif child in on_path:
                    return [*path[on_path[child] :], child]
                elif child not in finished:
                    on_path[child] = len(path)
                    path.append(child)
                    stack.append(iter(sorted(self._down[child] & candidates)))
        raise AssertionError("no cycle among candidates")  # pragma: no cover

    def layers(self) -> list[list[str]]:
        """Group nodes by longest distance from a root: each layer can run in parallel."""
        depth: dict[str, int] = {}
        for node in self.topological_order():
            depth[node] = max((depth[up] + 1 for up in self._up[node]), default=0)
        layers: list[list[str]] = [[] for _ in range(max(depth.values(), default=-1) + 1)]
        for node, level in depth.items():
            layers[level].append(node)
        return [sorted(layer) for layer in layers]

    def subgraph(self, targets: Iterable[str]) -> DAG:
        """The targets plus everything they (transitively) depend on."""
        keep: set[str] = set()
        for target in targets:
            if target not in self:
                raise UnknownTaskError(target)
            keep |= {target, *self.ancestors(target)}
        nodes = [n for n in self._up if n in keep]
        return DAG(nodes, ((u, d) for u, d in self.edges() if u in keep and d in keep))

    def to_mermaid(self) -> str:
        lines = ["graph TD"]
        lines += [f'    {_ident(n)}["{n}"]' for n in self.topological_order()]
        lines += [f"    {_ident(u)} --> {_ident(d)}" for u, d in self.edges()]
        return "\n".join(lines)

    def to_dot(self, name: str = "dagflow") -> str:
        lines = [f'digraph "{name}" {{', "    rankdir=LR;", "    node [shape=box];"]
        lines += [f'    "{n}";' for n in self.topological_order()]
        lines += [f'    "{u}" -> "{d}";' for u, d in self.edges()]
        lines.append("}")
        return "\n".join(lines)


def _ident(name: str) -> str:
    return re.sub(r"\W", "_", name)
