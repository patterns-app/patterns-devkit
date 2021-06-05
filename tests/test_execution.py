from __future__ import annotations

from typing import Optional

import pandas as pd
import pytest
from dcp.data_format.formats.memory.records import Records
from loguru import logger
from pandas import DataFrame
from snapflow.core.persisted.data_block import Alias
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.execution import ExecutionManager
from snapflow.core.function import Input, datafunction
from snapflow.core.persisted.state import DataBlockLog, DataFunctionLog, Direction
from snapflow.modules import core
from sqlalchemy.sql.expression import select
from tests.utils import (
    TestSchema1,
    TestSchema4,
    function_t1_to_t2,
    make_test_env,
    make_test_run_context,
)

# logger.enable("snapflow")

mock_dl_output = [{"f1": "2"}, {"f2": 3}]


@datafunction
def function_dl_source() -> Records[TestSchema4]:
    return mock_dl_output


@datafunction
def function_error() -> Records[TestSchema4]:
    raise Exception("function FAIL")


@datafunction
def never_stop(input: Optional[Reference] = None) -> DataFrame:
    # Does not use input but doesn't matter cause reference
    pass


def test_exe():
    env = make_test_env()
    node = GraphCfg(key="node", function="function_t1_source")
    g = GraphCfg(nodes=[node])
    exe = env.get_executable(node, graph=g)
    result = ExecutionManager(env, exe).execute()
    with env.md_api.begin():
        assert not result.output_blocks
        assert env.md_api.count(select(DataFunctionLog)) == 1
        pl = env.md_api.execute(select(DataFunctionLog)).scalar_one_or_none()
        assert pl.node_key == node.key
        assert pl.node_start_state == {}
        assert pl.node_end_state == {}
        assert pl.function_key == node.function
        assert pl.function_params == {}


def test_exe_output():
    env = make_test_env()
    env.add_module(core)
    env.add_function(function_dl_source)
    # env.add_storage("python://test")
    # rt = env.runtimes[0]
    # TODO: this is error because no data copy between SAME storage engines (but DIFFERENT storage urls) currently
    # ec = env.get_run_context(g, current_runtime=rt, target_storage=env.storages[0])
    # ec = env.get_run_context(g, current_runtime=rt, target_storage=rt.as_storage())
    output_alias = "node_output"
    node = GraphCfg(key="node", function="function_dl_source", alias=output_alias)
    g = GraphCfg(nodes=[node])
    exe = env.get_executable(node, graph=g)
    result = ExecutionManager(env, exe).execute()
    with env.md_api.begin():
        block = result.get_output_block(env)
        assert block is not None
        assert block.as_records() == mock_dl_output
        assert block.nominal_schema is TestSchema4
        assert len(block.realized_schema.fields) == len(TestSchema4.fields)
        # Test alias was created correctly
        assert (
            env.md_api.execute(select(Alias).filter(Alias.name == output_alias))
            .scalar_one_or_none()
            .data_block_id
            == block.data_block_id
        )
        assert env.md_api.count(select(DataBlockLog)) == 1
        dbl = env.md_api.execute(select(DataBlockLog)).scalar_one_or_none()
        assert dbl.data_block_id == block.data_block_id
        assert dbl.direction == Direction.OUTPUT


def test_non_terminating_function():
    env = make_test_env()
    env.add_function(never_stop)
    node = GraphCfg(key="node", function="never_stop")
    g = GraphCfg(nodes=[node])
    exe = env.get_executable(node, graph=g)
    result = ExecutionManager(env, exe).execute()
    assert result.get_output_block(env) is None


def test_non_terminating_function_with_reference_input():

    env = make_test_env()
    env.add_function(never_stop)
    source = GraphCfg(
        function="core.import_dataframe",
        params={"dataframe": pd.DataFrame({"a": range(10)})},
    )
    node = GraphCfg(key="node", function="never_stop", input=source.key)
    g = GraphCfg(nodes=[source, node])
    exe = env.get_executable(source, graph=g)
    result = ExecutionManager(env, exe).execute()
    # TODO: reference inputs need to log too? (So they know when to update)
    # with env.md_api.begin():
    #     assert env.md_api.count(select(DataBlockLog)) == 1
    exe = env.get_executable(node, graph=g)
    result = ExecutionManager(env, exe).execute()
    assert result.get_output_block(env) is None
