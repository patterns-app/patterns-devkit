from __future__ import annotations

from pprint import pprint
from typing import Callable, Dict

import pytest
from pandas import DataFrame

from dags import Environment
from dags.core.data_block import DataBlock
from dags.core.graph import Graph
from dags.core.node import Node
from dags.core.pipe import Pipe, PipeInterface, PipeLike, pipe, pipe_chain
from dags.core.pipe_interface import PipeAnnotation
from dags.core.runnable import PipeContext
from dags.core.runtime import RuntimeClass
from dags.core.sql.pipe import sql_pipe
from dags.core.streams import DataBlockStream
from dags.modules import core
from dags.utils.typing import T, U
from tests.utils import (
    TestType1,
    TestType2,
    make_test_env,
    pipe_chain_t1_to_t2,
    pipe_dataset_input,
    pipe_dataset_output,
    pipe_generic,
    pipe_self,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)


def make_graph() -> Graph:
    env = make_test_env()
    env.add_module(core)
    g = Graph(env)
    n1 = g.add_node("node1", pipe_t1_source)
    n2 = g.add_node("node2", pipe_t1_source)
    n3 = g.add_node("node3", pipe_chain_t1_to_t2, inputs="node1")
    n4 = g.add_node("node4", pipe_t1_to_t2, inputs="node2")
    n5 = g.add_node("node5", pipe_generic, inputs="node4")
    n6 = g.add_node("node6", pipe_self, inputs="node4")
    n7 = g.add_node(
        "node7", pipe_dataset_input, inputs={"input": "node4", "other_ds_t2": "node3"}
    )
    n8 = g.add_node("node8", pipe_dataset_output, inputs={"input": "node3"})
    n9 = g.add_node(
        "node9", pipe_dataset_input, inputs={"input": "node3", "other_ds_t2": "node8"}
    )
    return g


def test_declared_graph():
    g = make_graph()
    n1 = g.get_any_node("node1")
    n2 = g.get_any_node("node2")
    n4 = g.get_any_node("node4")
    n5 = g.get_any_node("node5")
    assert len(list(g.declared_nodes())) == 9
    assert g.get_flattened_graph().get_all_upstream_dependencies_in_execution_order(
        n1
    ) == [n1]
    assert g.get_flattened_graph().get_all_upstream_dependencies_in_execution_order(
        n5
    ) == [n2, n4, n5]


def test_dataset_nodes():
    g = make_graph()
    dg = g.get_declared_graph_with_dataset_nodes()
    assert len(list(dg.nodes())) == 11


def test_flattened_graph():
    g = make_graph()
    dg = g.get_declared_graph_with_dataset_nodes()
    fg = g.get_flattened_graph()
    nodes = {
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3__pipe_t1_to_t2",
        "node3__pipe_generic",
        "node3__dataset__core.dataframe_accumulator",
        # "node3__dataset__core.dedupe_unique_keep_newest_row", # TODO: we need python dedupe
        "node3__dataset__core.as_dataset",
        "node7",
        "node8",
        "node8__dataset",
        "node9",
    }
    assert set(n.key for n in fg.nodes()) == nodes
    # pprint(dict(fg.get_compiled_networkx_graph().adj))
    n3 = g.get_any_node("node3")
    n7 = g.get_any_node("node7")
    pprint(fg.get_all_upstream_dependencies_in_execution_order(n7))
    pprint(fg._compiled_inputs)
    assert len(fg.get_all_upstream_dependencies_in_execution_order(n7)) == 8
    assert len(fg.get_all_nodes_in_execution_order()) == 13
    print([n.key for n in fg.get_all_nodes_in_execution_order()])
    execution_order = [
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3__pipe_t1_to_t2",
        "node3__pipe_generic",
        "node3__dataset__core.dataframe_accumulator",
        # "node3__dataset__core.dedupe_unique_keep_newest_row",
        "node3__dataset__core.as_dataset",
        "node7",
        "node8",
        "node8__dataset",
        "node9",
    ]
    # TODO: topographical sort is not unique
    #  unclear under what conditions networkx version is stable
    assert [n.key for n in fg.get_all_nodes_in_execution_order()] == execution_order
    assert (
        fg.get_flattened_root_node_for_declared_node(n3).key == "node3__pipe_t1_to_t2"
    )
