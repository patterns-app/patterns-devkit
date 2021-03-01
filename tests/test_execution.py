from __future__ import annotations
from snapflow.core.snap_interface import NodeInterfaceManager

from typing import Optional

import pytest
from pandas import DataFrame
from snapflow.core.data_block import Alias, DataBlock, DataBlockMetadata
from snapflow.core.execution import (
    CompiledSnap,
    Executable,
    ExecutionManager,
    ExecutionSession,
    Worker,
)
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, Direction, SnapLog
from snapflow.modules import core
from snapflow.storage.data_formats import Records
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
    with env.session_scope() as sess:
        node = g.create_node(key="node", snap=snap_t1_source)
        w = Worker(ec)
        dfi_mgr = NodeInterfaceManager(ec, sess, node)
        bdfi = dfi_mgr.get_bound_interface()
        r = Executable(node.key, CompiledSnap(node.snap.key, node.snap), bdfi,)
        run_result = w.execute(r)
        assert run_result.output_block_id is None
        assert sess.query(SnapLog).count() == 1
        pl = sess.query(SnapLog).first()
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
    with env.session_scope() as sess:
        rt = env.runtimes[0]
        # TODO: this is error because no data copy between SAME storage engines (but DIFFERENT storage urls) currently
        # ec = env.get_run_context(g, current_runtime=rt, target_storage=env.storages[0])
        ec = env.get_run_context(g, current_runtime=rt, target_storage=rt.as_storage())
        output_alias = "node_output"
        node = g.create_node(key="node", snap=snap_dl_source, output_alias=output_alias)
        w = Worker(ec)
        dfi_mgr = NodeInterfaceManager(ec, sess, node)
        bdfi = dfi_mgr.get_bound_interface()
        r = Executable(node.key, CompiledSnap(node.snap.key, node.snap), bdfi,)
        run_result = w.execute(r)
        outputblock = sess.query(DataBlockMetadata).get(run_result.output_block_id)
        assert outputblock is not None
        outputblock = sess.merge(outputblock)
        block = outputblock.as_managed_data_block(ec, sess)
        assert block.as_records() == mock_dl_output
        assert block.nominal_schema is TestSchema4
        assert len(block.realized_schema.fields) == len(TestSchema4.fields)
        # Test alias was created correctly
        assert (
            sess.query(Alias).filter(Alias.alias == output_alias).first().data_block_id
            == block.data_block_id
        )
        assert sess.query(DataBlockLog).count() == 1
        dbl = sess.query(DataBlockLog).first()
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
