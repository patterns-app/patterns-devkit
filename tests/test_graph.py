from __future__ import annotations

from pprint import pprint
from typing import Callable, Dict

import pytest
from pandas import DataFrame

from basis.core.component import ComponentType
from basis.core.data_block import DataBlock
from basis.core.data_function import (
    DataFunction,
    DataFunctionInterface,
    DataFunctionLike,
    data_function,
    data_function_chain,
)
from basis.core.data_function_interface import DataFunctionAnnotation
from basis.core.node import Node
from basis.core.runnable import DataFunctionContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.data_function import sql_data_function
from basis.core.streams import DataBlockStream
from basis.modules import core
from basis.modules.core.functions.accumulate_as_dataset import accumulate_as_dataset
from basis.utils.typing import T, U
from tests.utils import (
    TestType1,
    TestType2,
    df_chain_t1_to_t2,
    df_dataset,
    df_generic,
    df_self,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
)


def test_graph_resolution():
    env = make_test_env()
    env.add_module(core)
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_source)
    n3 = env.add_node("node3", df_chain_t1_to_t2, inputs="node1")
    n4 = env.add_node("node4", df_t1_to_t2, inputs="node2")
    n5 = env.add_node("node5", df_generic, inputs="node4")
    n6 = env.add_node("node6", df_self, inputs="node4")
    n7 = env.add_node(
        "node7", df_dataset, inputs={"input": "node4", "other_t2": "node3"}
    )
    g = env.get_graph()
    assert len(g.nodes()) == 7
    assert g.get_all_upstream_dependencies_in_execution_order("node1") == [n1]
    assert g.get_all_upstream_dependencies_in_execution_order("node5") == [n2, n4, n5]
    dg = g.add_dataset_nodes()
    assert len(dg.nodes()) == 8
    # pprint(dict(dg.get_compiled_networkx_graph().adj))
    fg = dg.flatten()
    assert len(fg.nodes()) == 11
    # pprint(dict(fg.get_compiled_networkx_graph().adj))
    assert len(fg.get_all_upstream_dependencies_in_execution_order("node7")) == 9
