from __future__ import annotations

import pytest

from dags.core.data_block import Alias
from dags.core.data_formats import RecordsList
from dags.core.graph import Graph
from dags.core.pipe_interface import NodeInterfaceManager
from dags.core.runnable import CompiledPipe, Runnable, RunSession, Worker
from dags.modules import core
from tests.utils import (
    TestSchema1,
    TestSchema4,
    make_test_env,
    make_test_execution_context,
    pipe_generic,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)

mock_dl_output = [{"f1": 2}, {"f2": 3}]


def pipe_dl_source() -> RecordsList[TestSchema4]:
    return mock_dl_output


def pipe_error() -> RecordsList[TestSchema4]:
    raise Exception("pipe FAIL")


def test_worker():
    env = make_test_env()
    g = Graph(env)
    rt = env.runtimes[0]
    ec = env.get_execution_context(g, current_runtime=rt)
    node = g.create_node("node", pipe_t1_source)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        node.key,
        CompiledPipe(node.pipe.key, node.pipe),
        bdfi,
    )
    output = w.run(r)
    assert output is None


def test_worker_output():
    env = make_test_env()
    env.add_module(core)
    g = Graph(env)
    env.add_storage("memory://test")
    rt = env.runtimes[0]
    ec = env.get_execution_context(
        g, current_runtime=rt, target_storage=env.storages[0]
    )
    output_alias = "node_output"
    node = g.create_node("node", pipe_dl_source, output_alias=output_alias)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        node.key,
        CompiledPipe(node.pipe.key, node.pipe),
        bdfi,
    )
    outputblock = w.run(r)
    assert outputblock is not None
    block = outputblock.as_managed_data_block(ec)
    assert block.as_records_list() == mock_dl_output
    assert block.nominal_schema is TestSchema4
    assert len(block.realized_schema.fields) == len(TestSchema4.fields)
    # Test alias was created correctly
    assert (
        env.session.query(Alias)
        .filter(Alias.alias == output_alias)
        .first()
        .data_block_id
        == block.data_block_id
    )
