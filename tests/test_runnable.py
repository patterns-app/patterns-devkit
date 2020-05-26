from __future__ import annotations

import pytest

from basis.core.data_format import DictList
from basis.core.data_function import DataFunctionInterfaceManager
from basis.core.runnable import CompiledDataFunction, Runnable, RunSession, Worker
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


def df_dl_source() -> DictList[TestType1]:
    return mock_dl_output


def df_error() -> DictList[TestType1]:
    raise Exception("DF FAIL")


def test_worker():
    env = make_test_env()
    sess = env.get_new_metadata_session()
    ec = env.get_execution_context(sess, current_runtime=env.runtimes[0])
    cdf = env.add_node("cdf", df_t1_source)
    w = Worker(ec)
    dfi_mgr = DataFunctionInterfaceManager(ec, cdf)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        cdf.key, CompiledDataFunction(cdf.datafunction.key, cdf.datafunction), bdfi,
    )
    output = w.run(r)
    assert output is None


def test_worker_output():
    env = make_test_env()
    sess = env.get_new_metadata_session()
    env.add_storage("memory://test")
    ec = env.get_execution_context(
        sess, current_runtime=env.runtimes[0], target_storage=env.storages[0]
    )
    cdf = env.add_node("cdf", df_dl_source)
    w = Worker(ec)
    dfi_mgr = DataFunctionInterfaceManager(ec, cdf)
    bdfi = dfi_mgr.get_bound_interface()
    r = Runnable(
        cdf.key, CompiledDataFunction(cdf.datafunction.key, cdf.datafunction), bdfi,
    )
    output = w.execute_datafunction(r)
    assert output == mock_dl_output
    ws = RunSession(None, sess)
    output_dr = w.conform_output(ws, output, r)
    assert output_dr is not None
    dr = output_dr.as_managed_data_resource(ec)
    assert dr.as_dictlist() == mock_dl_output
