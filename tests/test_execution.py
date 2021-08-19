from __future__ import annotations

from typing import Optional

import pandas as pd
import pytest
from basis.core.block import Reference, as_managed
from basis.core.declarative.function import DEFAULT_OUTPUT_NAME
from basis.core.declarative.graph import GraphCfg
from basis.core.execution.execution import ExecutionManager
from basis.core.function import Input, datafunction
from basis.core.persistence.block import Alias
from basis.core.persistence.state import BlockLog, FunctionLog, Direction
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
    g = GraphCfg(nodes=[node]).resolve_and_flatten(env.library)
    exe = env.get_executable(node.key, graph=g)
    results = ExecutionManager(exe).execute()
    assert len(results) == 1
    result = results[0]
    with env.md_api.begin():
        assert not result.output_blocks_emitted
        assert env.md_api.count(select(FunctionLog)) == 1
        pl = env.md_api.execute(select(FunctionLog)).scalar_one_or_none()
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
    g = GraphCfg(nodes=[node]).resolve_and_flatten(env.library)
    exe = env.get_executable(node.key, graph=g)
    results = ExecutionManager(exe).execute()
    assert len(results) == 1
    result = results[0]
    with env.md_api.begin():
        block = result.stdout()
        assert block is not None
        assert block.as_records() == mock_dl_output
        assert block.nominal_schema_key == TestSchema4.key
        assert len(env.get_schema(block.realized_schema_key).fields) == len(
            TestSchema4.fields
        )
        # Test alias was created correctly
        assert (
            env.md_api.execute(select(Alias).filter(Alias.name == output_alias))
            .scalar_one_or_none()
            .block_id
            == block.id
        )
        assert env.md_api.count(select(BlockLog)) == 1
        dbl = env.md_api.execute(select(BlockLog)).scalar_one_or_none()
        assert dbl.stream_name == DEFAULT_OUTPUT_NAME
        assert dbl.block_id == block.id
        assert dbl.direction == Direction.OUTPUT


def test_non_terminating_function():
    env = make_test_env()
    env.add_function(never_stop)
    node = GraphCfg(key="node", function="never_stop")
    g = GraphCfg(nodes=[node]).resolve_and_flatten(env.library)
    exe = env.get_executable(node.key, graph=g)
    results = ExecutionManager(exe).execute()
    assert len(results) == 1
    result = results[0]
    assert not result.output_blocks_emitted


def test_non_terminating_function_with_reference_input():

    env = make_test_env()
    env.add_function(never_stop)
    source = GraphCfg(
        function="core.import_dataframe",
        params={"dataframe": pd.DataFrame({"a": range(10)})},
    )
    node = GraphCfg(key="node", function="never_stop", input=source.key)
    g = GraphCfg(nodes=[source, node]).resolve_and_flatten(env.library)
    exe = env.get_executable(source.key, graph=g)
    result = ExecutionManager(exe).execute()
    # TODO: reference inputs need to log too? (So they know when to update)
    # with env.md_api.begin():
    #     assert env.md_api.count(select(BlockLog)) == 1
    exe = env.get_executable(node.key, graph=g)
    results = ExecutionManager(exe).execute()
    assert len(results) == 1
    result = results[0]
    assert not result.output_blocks_emitted
