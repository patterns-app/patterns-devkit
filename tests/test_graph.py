from __future__ import annotations

import pytest

from dags.core.graph import Graph
from dags.modules import core
from tests.utils import (
    make_test_env,
    pipe_chain_t1_to_t2,
    pipe_dataset_input,
    pipe_dataset_output,
    pipe_generic,
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
    g.add_node("node3", pipe_t1_to_t2, inputs="node1")
    g.add_node("node4", pipe_t1_to_t2, inputs="node2")
    g.add_node("node5", pipe_generic, inputs="node4")
    g.add_node("node6", pipe_self, inputs="node4")
    g.add_node(
        "node7", pipe_dataset_input, inputs={"input": "node4", "other_ds_t2": "node3"}
    )
    g.add_node("node8", pipe_dataset_output, inputs={"input": "node3"})
    g.add_node(
        "node9", pipe_dataset_input, inputs={"input": "node3", "other_ds_t2": "node8"}
    )
    return g


def test_dupe_node():
    g = make_graph()
    with pytest.raises(KeyError):
        g.add_node("node1", pipe_t1_source)


def test_declared_graph():
    g = make_graph()
    n1 = g.get_any_node("node1")
    n2 = g.get_any_node("node2")
    n4 = g.get_any_node("node4")
    n5 = g.get_any_node("node5")
    assert len(list(g.declared_nodes())) == 9
    dg = g.get_declared_graph_with_dataset_nodes()
    assert dg.get_all_upstream_dependencies_in_execution_order(n1) == [n1]
    assert dg.get_all_upstream_dependencies_in_execution_order(n5) == [
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
    # dg = g.get_declared_graph_with_dataset_nodes()
    fg = g.get_declared_graph_with_dataset_nodes()
    nodes = {
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3",
        # "node3__accumulator",
        # "node3__dedupe",
        "node7",
        "node8",
        # "node8__accumulator",
        # "node8__dedupe",
        "node9",
    }
    for n in list(nodes):
        nodes.update(g.get_any_node(n).get_dataset_node_keys())
    assert set(n.key for n in fg.nodes()) == nodes
    n3 = g.get_any_node("node3")
    n7 = g.get_any_node("node7")
    assert len(fg.get_all_upstream_dependencies_in_execution_order(n3)) == 2
    assert len(fg.get_all_upstream_dependencies_in_execution_order(n7)) == 7
    assert len(fg.get_all_nodes_in_execution_order()) == len(nodes)
    execution_order = [n.key for n in fg.get_all_nodes_in_execution_order()]
    expected_execution_order = [
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3",
        "node8",
        "node8__accumulator",
        "node8__dedupe",
        "node9",
        "node3__accumulator",
        "node3__dedupe",
        "node7",
    ]
    # TODO: graph sort not stable!
    for i, n in enumerate(expected_execution_order[:-1]):
        assert execution_order.index(n) < execution_order.index(
            expected_execution_order[i + 1]
        )
