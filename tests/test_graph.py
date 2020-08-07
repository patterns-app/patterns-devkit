from __future__ import annotations

from pprint import pprint
from typing import Callable, Dict

import pytest
from pandas import DataFrame

from basis import Environment
from basis.core.component import ComponentType
from basis.core.data_block import DataBlock
from basis.core.graph import Graph
from basis.core.node import Node
from basis.core.pipe import Pipe, PipeInterface, PipeLike, pipe, pipe_chain
from basis.core.pipe_interface import PipeAnnotation
from basis.core.runnable import PipeContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.pipe import sql_pipe
from basis.core.streams import DataBlockStream
from basis.modules import core
from basis.modules.core.pipes.accumulate_as_dataset import accumulate_as_dataset
from basis.utils.typing import T, U
from tests.utils import (
    TestType1,
    TestType2,
    df_chain_t1_to_t2,
    df_dataset_input,
    df_dataset_output,
    df_generic,
    df_self,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
)


def make_graph() -> Environment:
    env = make_test_env()
    env.add_module(core)
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_source)
    n3 = env.add_node("node3", df_chain_t1_to_t2, inputs="node1")
    n4 = env.add_node("node4", df_t1_to_t2, inputs="node2")
    n5 = env.add_node("node5", df_generic, inputs="node4")
    n6 = env.add_node("node6", df_self, inputs="node4")
    n7 = env.add_node(
        "node7", df_dataset_input, inputs={"input": "node4", "other_ds_t2": "node3"}
    )
    n8 = env.add_node("node8", df_dataset_output, inputs={"input": "node3"})
    n9 = env.add_node(
        "node9", df_dataset_input, inputs={"input": "node3", "other_ds_t2": "node8"}
    )
    return env


def test_declared_graph():
    env = make_graph()
    g = env.get_graph()
    n1 = env.get_node("node1")
    n2 = env.get_node("node2")
    n4 = env.get_node("node4")
    n5 = env.get_node("node5")
    assert len(g.nodes()) == 9
    assert g.get_all_upstream_dependencies_in_execution_order(n1) == [n1]
    assert g.get_all_upstream_dependencies_in_execution_order(n5) == [n2, n4, n5]


def test_dataset_nodes():
    env = make_graph()
    g = env.get_graph()
    dg = g.add_dataset_nodes()
    assert len(dg.nodes()) == 11


def test_flattened_graph():
    env = make_graph()
    g = env.get_graph()
    dg = g.add_dataset_nodes()
    fg = dg.flatten()
    nodes = {
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3__df_t1_to_t2",
        "node3__df_generic",
        "node3__dataset__accumulator",
        "node3__dataset__dedupe_unique_keep_newest_row",
        "node3__dataset__as_dataset",
        "node7",
        "node8",
        "node8__dataset",
        "node9",
    }
    assert set(n.name for n in fg.nodes()) == nodes
    # pprint(dict(fg.get_compiled_networkx_graph().adj))
    n3 = env.get_node("node3")
    n7 = env.get_node("node7")
    assert len(fg.get_all_upstream_dependencies_in_execution_order(n7)) == 9
    assert len(fg.get_all_nodes_in_execution_order()) == 14
    print([n.name for n in fg.get_all_nodes_in_execution_order()])
    execution_order = [
        "node2",
        "node4",
        "node6",
        "node5",
        "node1",
        "node3__df_t1_to_t2",
        "node3__df_generic",
        "node3__dataset__accumulator",
        "node3__dataset__dedupe_unique_keep_newest_row",
        "node3__dataset__as_dataset",
        "node7",
        "node8",
        "node8__dataset",
        "node9",
    ]
    # TODO: topographical sort is not unique
    #  unclear under what conditions networkx version is stable
    assert [n.name for n in fg.get_all_nodes_in_execution_order()] == execution_order
    assert fg.get_flattened_root_node_for_declared_node(n3).name == "node3__df_t1_to_t2"
