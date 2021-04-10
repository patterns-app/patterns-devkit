from __future__ import annotations

from typing import Optional

import pandas as pd
import pytest
from loguru import logger
from pandas import DataFrame
from snapflow.core.data_block import Alias, DataBlock, DataBlockMetadata
from snapflow.core.execution import CompiledSnap, Executable, ExecutionManager, Worker
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, SnapLog
from snapflow.core.snap import Input
from snapflow.core.snap_interface import NodeInterfaceManager
from snapflow.modules import core
from snapflow.storage.data_formats import Records
from sqlalchemy.sql.expression import select
from tests.utils import (
    TestSchema1,
    TestSchema4,
    make_test_env,
    make_test_run_context,
    snap_generic,
    snap_t1_sink,
    snap_t1_source,
    snap_t1_to_t2,
)

mock_dl_output = [{"f1": "2"}, {"f2": 3}]


def snap_dl_source() -> Records[TestSchema4]:
    return mock_dl_output


def snap_error() -> Records[TestSchema4]:
    raise Exception("snap FAIL")


def test_worker():
    env = make_test_env()
    g = Graph(env)
    rt = env.runtimes[0]
    ec = env.get_run_context(g, current_runtime=rt)
    node = g.create_node(key="node", snap=snap_t1_source)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Executable(
        node.key,
        CompiledSnap(node.snap.key, node.snap),
        bdfi,
    )
    run_result = w.execute(r)
    with env.md_api.begin():
        assert run_result.output_block_id is None
        assert env.md_api.count(select(SnapLog)) == 1
        pl = env.md_api.execute(select(SnapLog)).scalar_one_or_none()
        assert pl.node_key == node.key
        assert pl.graph_id == g.get_metadata_obj().hash
        assert pl.node_start_state == {}
        assert pl.node_end_state == {}
        assert pl.snap_key == node.snap.key
        assert pl.snap_params == {}


def test_worker_output():
    env = make_test_env()
    env.add_module(core)
    g = Graph(env)
    # env.add_storage("python://test")
    rt = env.runtimes[0]
    # TODO: this is error because no data copy between SAME storage engines (but DIFFERENT storage urls) currently
    # ec = env.get_run_context(g, current_runtime=rt, target_storage=env.storages[0])
    ec = env.get_run_context(g, current_runtime=rt, target_storage=rt.as_storage())
    output_alias = "node_output"
    node = g.create_node(key="node", snap=snap_dl_source, output_alias=output_alias)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Executable(
        node.key,
        CompiledSnap(node.snap.key, node.snap),
        bdfi,
    )
    run_result = w.execute(r)
    with env.md_api.begin():
        outputblock = env.md_api.execute(
            select(DataBlockMetadata).filter(
                DataBlockMetadata.id == run_result.output_block_id
            )
        ).scalar_one_or_none()
        assert outputblock is not None
        # outputblock = env.md_api.merge(outputblock)
        block = outputblock.as_managed_data_block(ec)
        assert block.as_records() == mock_dl_output
        assert block.nominal_schema is TestSchema4
        assert len(block.realized_schema.fields) == len(TestSchema4.fields)
        # Test alias was created correctly
        assert (
            env.md_api.execute(select(Alias).filter(Alias.alias == output_alias))
            .scalar_one_or_none()
            .data_block_id
            == block.data_block_id
        )
        assert env.md_api.count(select(DataBlockLog)) == 1
        dbl = env.md_api.execute(select(DataBlockLog)).scalar_one_or_none()
        assert dbl.data_block_id == outputblock.id
        assert dbl.direction == Direction.OUTPUT


def test_non_terminating_snap():
    def never_stop(input: Optional[DataBlock] = None) -> DataFrame:
        pass

    env = make_test_env()
    g = Graph(env)
    rt = env.runtimes[0]
    ec = env.get_run_context(g, current_runtime=rt)
    node = g.create_node(key="node", snap=never_stop)
    em = ExecutionManager(ec)
    output = em.execute(node, to_exhaustion=True)
    assert output is None


def test_non_terminating_snap_with_reference_input():
    @Input("input", reference=True, required=False)
    def never_stop(input: Optional[DataBlock] = None) -> DataFrame:
        pass

    env = make_test_env()
    g = Graph(env)
    rt = env.runtimes[0]
    ec = env.get_run_context(g, current_runtime=rt)
    source = g.create_node(
        snap="core.import_dataframe",
        params={"dataframe": pd.DataFrame({"a": range(10)})},
    )
    node = g.create_node(key="node", snap=never_stop, input=source)
    em = ExecutionManager(ec)
    output = em.execute(source, to_exhaustion=True)
    output = em.execute(node, to_exhaustion=True)
    assert output is None
