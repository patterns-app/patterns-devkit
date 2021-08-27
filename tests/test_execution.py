from __future__ import annotations
from basis.core.execution.executable import instantiate_executable
from basis.core.declarative.node import NodeCfg
from basis.core.graph import instantiate_graph

from typing import Optional

import pandas as pd
import pytest
from basis.core.declarative.function import DEFAULT_OUTPUT_NAME
from basis.core.execution.execution import ExecutionManager
from basis.core.function import function, simple_function
from basis.core.persistence.block import Alias
from basis.core.persistence.state import ExecutionLog, Direction
from basis.modules import core
from dcp.data_format.formats.memory.records import Records
from loguru import logger
from pandas import DataFrame
from sqlalchemy.sql.expression import select
from tests.utils import (
    TestSchema1,
    TestSchema4,
    function_t1_source,
    function_t1_to_t2,
    make_test_env,
    make_test_run_context,
)

# logger.enable("basis")

mock_dl_output = [{"f1": "2"}, {"f2": 3}]


@simple_function
def function_dl_source() -> Records[TestSchema4]:
    return mock_dl_output


@simple_function
def function_error() -> Records[TestSchema4]:
    raise Exception("function FAIL")


@simple_function
def never_stop(input: Optional[Reference] = None) -> DataFrame:
    # Does not use input but doesn't matter cause reference
    pass


def test_exe():
    env = make_test_env()
    # env.add_function(function_t1_source)
    node = NodeCfg(key="node", function="tests.utils.function_t1_source")
    g = instantiate_graph(nodes=[node], lib=env.library)
    exe = env.get_executable(node.key, graph=g)
    result = ExecutionManager(instantiate_executable(exe)).execute()
    with env.md_api.begin():
        assert not result.output_blocks_emitted
        assert env.md_api.count(select(ExecutionLog)) == 1
        pl = env.md_api.execute(select(ExecutionLog)).scalar_one_or_none()
        assert pl.node_key == node.key


def test_exe_output():
    env = make_test_env()
    output_alias = "node_output"
    node = NodeCfg(
        key="node",
        function="tests.test_execution.function_dl_source",
        aliases=output_alias,
    )
    g = instantiate_graph(nodes=[node], lib=env.library)
    exe = env.get_executable(node.key, graph=g)
    result = ExecutionManager(instantiate_executable(exe)).execute()
    with env.md_api.begin():
        block = result.get_stdout_block()
        assert block is not None
        assert block.as_records() == mock_dl_output
        # assert block.nominal_schema_key == TestSchema4.key
        assert len(env.get_schema(block.realized_schema_key).fields) == len(
            TestSchema4.fields
        )
        # Test alias was created correctly
        # assert (
        #     env.md_api.execute(select(Alias).filter(Alias.name == output_alias))
        #     .scalar_one_or_none()
        #     .block_id
        #     == block.id
        # )
        # assert env.md_api.count(select(BlockLog)) == 1
        # dbl = env.md_api.execute(select(BlockLog)).scalar_one_or_none()
        # assert dbl.stream_name == DEFAULT_OUTPUT_NAME
        # assert dbl.block_id == block.id
        # assert dbl.direction == Direction.OUTPUT


def test_non_terminating_function():
    env = make_test_env()
    node = NodeCfg(key="node", function="tests.test_execution.never_stop")
    g = instantiate_graph(nodes=[node], lib=env.library)
    exe = env.get_executable(node.key, graph=g)
    result = ExecutionManager(instantiate_executable(exe)).execute()
    assert not result.output_blocks_emitted


def test_non_terminating_function_with_reference_input():

    env = make_test_env()
    source = NodeCfg(
        key="source",
        function="basis.modules.core.functions.import_dataframe",
        params={"dataframe": pd.DataFrame({"a": range(10)})},
    )
    node = NodeCfg(
        key="node", function="tests.test_execution.never_stop", inputs=source.key
    )
    g = instantiate_graph(nodes=[source, node], lib=env.library)
    exe = env.get_executable(source.key, graph=g)
    result = ExecutionManager(exe).execute()
    # TODO: reference inputs need to log too? (So they know when to update)
    # with env.md_api.begin():
    #     assert env.md_api.count(select(BlockLog)) == 1
    exe = env.get_executable(node.key, graph=g)
    result = ExecutionManager(exe).execute()
    assert not result.output_blocks_emitted
