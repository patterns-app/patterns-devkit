from __future__ import annotations

import pytest

from basis.core.data_formats import RecordsList
from basis.core.data_function_interface import NodeInterfaceManager
from basis.core.runnable import CompiledDataFunction, Runnable, RunSession, Worker
from basis.modules import core
from tests.utils import (
    TestType1,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
    make_test_execution_context,
)

mock_dl_output = [{1: 2}, {2: 3}]


def df_dl_source() -> RecordsList[TestType1]:
    return mock_dl_output


def df_error() -> RecordsList[TestType1]:
    raise Exception("DF FAIL")


def test_worker():
    env = make_test_env()
    sess = env.get_new_metadata_session()
    rt = env.runtimes[0]
    ec = env.get_execution_context(sess, current_runtime=rt)
    node = env.add_node("node", df_t1_source)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        node.name,
        CompiledDataFunction(
            node.data_function.name, node.data_function.get_definition(rt.runtime_class)
        ),
        bdfi,
    )
    output = w.run(r)
    assert output is None


def test_worker_output():
    env = make_test_env()
    env.add_module(core)
    sess = env.get_new_metadata_session()
    env.add_storage("memory://test")
    rt = env.runtimes[0]
    ec = env.get_execution_context(
        sess, current_runtime=rt, target_storage=env.storages[0]
    )
    node = env.add_node("node", df_dl_source)
    w = Worker(ec)
    dfi_mgr = NodeInterfaceManager(ec, node)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        node.name,
        CompiledDataFunction(
            node.data_function.name, node.data_function.get_definition(rt.runtime_class)
        ),
        bdfi,
    )
    output = w.execute_data_function(r)
    assert output == mock_dl_output
    ws = RunSession(None, sess)
    outputblock = w.conform_output(ws, output, r)
    assert outputblock is not None
    block = outputblock.as_managed_data_block(ec)
    assert block.as_records_list() == mock_dl_output
