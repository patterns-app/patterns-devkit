from __future__ import annotations

import pytest

from dags.core.graph import Graph
from dags.modules import core
from tests.utils import (
    make_test_env,
    pipe_chain_t1_to_t2,
    pipe_generic,
    pipe_multiple_input,
    pipe_self,
    pipe_t1_source,
    pipe_t1_to_t2,
)


def make_graph() -> Graph:
    env = make_test_env()
    env.add_module(core)
    g = Graph(env)
    g.add_node("node1", pipe_t1_source)
    g.add_node("node2", pipe_t1_source)
    g.add_node("node3", pipe_t1_to_t2, upstream="node1")
    g.add_node("node4", pipe_t1_to_t2, upstream="node2")
    g.add_node("node5", pipe_generic, upstream="node4")
    g.add_node("node6", pipe_self, upstream="node4")
    g.add_node(
        "node7", pipe_multiple_input, upstream={"input": "node4", "other_t2": "node3"}
    )
    return g


def test_dupe_node():
    g = make_graph()
    with pytest.raises(KeyError):
        g.add_node("node1", pipe_t1_source)


def test_declared_graph():
    g = make_graph()
    n1 = g.get_node("node1")
    n2 = g.get_node("node2")
    n4 = g.get_node("node4")
    n5 = g.get_node("node5")
    assert len(list(g.all_nodes())) == 7
    assert g.get_all_upstream_dependencies_in_execution_order(n1) == [n1]
    assert g.get_all_upstream_dependencies_in_execution_order(n5) == [
        n2,
        n4,
        n5,
    ]


# Redundant
# def test_dataset_nodes():
#     g = make_graph()
#     dg = g.get_declared_graph_with_dataset_nodes()
#     assert len(list(dg.nodes())) == 27


def test_make_graph():
    g = make_graph()
    nodes = {
        "node1",
        "node2",
        "node3",
        "node4",
        "node5",
        "node6",
        "node7",
    }
    assert set(n.key for n in g.all_nodes()) == nodes
    n3 = g.get_node("node3")
    n7 = g.get_node("node7")
    assert len(g.get_all_upstream_dependencies_in_execution_order(n3)) == 2
    assert len(g.get_all_upstream_dependencies_in_execution_order(n7)) == 5
    assert len(g.get_all_nodes_in_execution_order()) == len(nodes)
    execution_order = [n.key for n in g.get_all_nodes_in_execution_order()]
    expected_orderings = [
        [
            "node2",
            "node4",
            "node5",
        ],
        [
            "node2",
            "node4",
            "node6",
        ],
        [
            "node1",
            "node3",
            "node7",
        ],
    ]
    # TODO: graph sort not stable!
    for ordering in expected_orderings:
        for i, n in enumerate(ordering[:-1]):
            assert execution_order.index(n) < execution_order.index(ordering[i + 1])
