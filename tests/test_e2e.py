from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Generator, Iterator, Optional
import time

import pandas as pd
import pytest
from commonmodel.base import create_quick_schema
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import Storage
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from loguru import logger
from pandas._testing import assert_almost_equal
from snapflow import DataBlock, DataFunctionContext, datafunction
from snapflow.core.data_block import Consumable, Reference
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.execution import (
    ExecutableCfg,
    ExecutionResult,
    RemoteCallbackMetadataExecutionResultHandler,
    ResultHandler,
    get_global_metadata_result_handler,
)
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from snapflow.core.persistence.data_block import (
    Alias,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.persistence.state import (
    DataBlockLog,
    DataFunctionLog,
    Direction,
    NodeState,
    _reset_state,
    get_or_create_state,
    reset,
)
from snapflow.core.sql.sql_function import sql_function_factory
from snapflow.modules import core
from sqlalchemy import select
from tests.utils import get_stdout_block

# logger.enable("snapflow")

Customer = create_quick_schema(
    "Customer", [("name", "Text"), ("joined", "DateTime"), ("Meta data", "Json")]
)
Metric = create_quick_schema("Metric", [("metric", "Text"), ("value", "Decimal(12,2)")])


@datafunction
def shape_metrics(i1: DataBlock) -> Records[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@datafunction
def aggregate_metrics(
    i1: DataBlock, this: Optional[DataBlock] = None
) -> Records[Metric]:
    if this is not None:
        metrics = this.as_records()
    else:
        metrics = [
            {"metric": "row_count", "value": 0},
            {"metric": "blocks", "value": 0},
        ]
    df = i1.as_dataframe()
    rows = len(df)
    for m in metrics:
        if m["metric"] == "row_count":
            m["value"] += rows
        if m["metric"] == "blocks":
            m["value"] += 1
    return metrics


FAIL_MSG = "Failure triggered"
batch_size = 4
chunk_size = 2


@datafunction
def customer_source(
    ctx: DataFunctionContext, batches: int, fail: bool = False
) -> Iterator[Records[Customer]]:
    n = ctx.get_state_value("records_imported", 0)
    N = batches * batch_size
    if n >= N:
        return
    for i in range(batch_size // chunk_size):
        records = []
        for j in range(chunk_size):
            records.append(
                {
                    "name": f"name{n}",
                    "joined": datetime(2000, 1, n + 1),
                    "Meta data": {"idx": n},
                }
            )
            n += 1
        yield records
        ctx.emit_state_value("records_imported", n)
        if fail:
            # Fail AFTER yielding one record set
            raise Exception(FAIL_MSG)
        if n >= N:
            return


aggregate_metrics_sql = sql_function_factory(
    "aggregate_metrics_sql",
    sql="""
    select -- :Metric
        'row_count' as metric,
        count(*) as value
    from input
    """,
)


dataset_inputs_sql = sql_function_factory(
    "dataset_inputs_sql",
    sql="""
    select
        'input' as tble
        , count(*) as row_count
    from input
    union all
    select
        'metrics' as tble
        , count(*) as row_count
    from metrics
    """,
)


mixed_inputs_sql = sql_function_factory(
    "mixed_inputs_sql",
    sql="""
    select
        'input' as tble
        , count(*) as row_count
    from input -- :DataBlock
    union all
    select
        'metrics' as tble
        , count(*) as row_count
    from metrics
    """,
)


def get_env(key="_test", db_url=None):
    if db_url is None:
        db_url = get_tmp_sqlite_db_url()
    env = Environment(DataspaceCfg(key=key, metadata_storage=db_url))
    env.add_module(core)
    env.add_schema(Customer)
    env.add_schema(Metric)
    return env


def test_simple_import():
    dburl = get_tmp_sqlite_db_url()
    storage = get_tmp_sqlite_db_url()
    env = Environment(DataspaceCfg(metadata_storage=dburl, storages=[storage]))
    env.add_module(core)
    alias_name = "n1_alias"
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    n = GraphCfg(
        key="n1",
        function="import_dataframe",
        params={"dataframe": df},
        alias=alias_name,
    )
    results = env.produce("n1", graph=GraphCfg(nodes=[n]), target_storage=storage)
    block = get_stdout_block(results)
    # Data check
    assert_almost_equal(block.as_dataframe(), df, check_dtype=False)
    # Datablock checks
    assert block.created_at is not None
    assert block.updated_at is not None
    assert block.dataspace_key == env.dataspace.key
    assert block.nominal_schema_key == "core.Any"
    assert block.record_count == 10
    inferred = env.get_schema(block.inferred_schema_key)
    realized = env.get_schema(block.realized_schema_key)
    assert inferred.field_names() == ["a", "b"]
    assert realized.field_names() == ["a", "b"]
    assert block.created_by_node_key == n.key
    assert not block.deleted
    # Stored data block checks
    assert len(block.stored_data_blocks) == 1
    sdb = block.stored_data_blocks[0]
    assert sdb.created_at is not None
    assert sdb.updated_at is not None
    assert sdb.data_block_id == block.id
    assert sdb.name is not None
    assert sdb.storage_url == storage
    assert Storage(storage).get_api().exists(sdb.name)
    # Alias checks
    alias = env.md_api.execute(
        select(Alias).filter(Alias.name == alias_name, Alias.storage_url == storage)
    ).scalar_one_or_none()
    assert alias is not None
    assert Storage(storage).get_api().exists(alias_name)
    assert alias.data_block_id == block.id
    assert alias.stored_data_block_id == block.stored_data_blocks[0].id


def test_repeated_runs():
    env = get_env()
    s = env._local_python_storage
    # Initial graph
    batches = 2
    N = batches * batch_size
    source = GraphCfg(
        key="source", function=customer_source.key, params={"batches": batches}
    )
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="source")
    g = GraphCfg(nodes=[source, metrics])

    # Run first time
    results = env.produce("metrics", graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block.nominal_schema_key.endswith("Metric")
    records = block.as_records()
    expected_records = [
        {"metric": "row_count", "value": batch_size},
        {"metric": "col_count", "value": 3},
    ]
    # Logs
    dfl = env.md_api.execute(
        select(DataFunctionLog).filter(DataFunctionLog.node_key == metrics.key)
    ).scalar_one()
    dbls = list(
        env.md_api.execute(
            select(DataBlockLog)
            .filter(DataBlockLog.function_log_id == dfl.id)
            .order_by(DataBlockLog.direction)
        ).scalars()
    )
    assert len(dbls) == 2
    inpt = dbls[0]
    output = dbls[1]
    assert inpt.direction == Direction.INPUT
    assert output.direction == Direction.OUTPUT
    assert output.data_block_id == block.id
    # Records
    assert records == expected_records

    # Run again, should get next batch
    results = env.produce("metrics", graph=g, target_storage=s)
    block = get_stdout_block(results)
    records = block.as_records()
    assert records == expected_records
    # Test latest_output
    block = env.get_latest_output(metrics)
    records = block.as_records()
    assert records == expected_records

    # Run again, should be exhausted
    blocks = env.produce("metrics", graph=g, target_storage=s)
    assert len(blocks) == 0
    # Run again, should still be exhausted
    blocks = env.produce("metrics", graph=g, target_storage=s)
    assert len(blocks) == 0

    # now add new node and process all at once
    newnode = GraphCfg(
        key="new_accumulator", function="core.accumulator", input="source"
    )
    g = GraphCfg(nodes=g.nodes + [newnode])
    results = env.produce("new_accumulator", graph=g, target_storage=s)
    block = get_stdout_block(results)
    records = block.as_records()
    assert len(records) == N
    blocks = env.produce("new_accumulator", graph=g, target_storage=s)
    assert len(blocks) == 0


def test_alternate_apis():
    env = get_env()
    s = env._local_python_storage
    # Initial graph
    batches = 2
    source = GraphCfg(
        key="source", function=customer_source.key, params={"batches": batches}
    )
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="source")
    g = GraphCfg(nodes=[source, metrics])
    # Run first time
    results = env.produce(node=metrics, graph=g, target_storage=s)
    block = get_stdout_block(results)
    output = block
    assert output.nominal_schema_key.endswith("Metric")
    records = block.as_records()
    expected_records = [
        {"metric": "row_count", "value": batch_size},
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records


def test_function_failure():
    env = get_env()
    s = env._local_python_storage
    # Initial graph
    batches = 2
    cfg = {"batches": batches, "fail": True}
    source = GraphCfg(key="source", function=customer_source.key, params=cfg)
    g = GraphCfg(nodes=[source])
    results = env.produce(graph=g, node=source, target_storage=s)
    block = get_stdout_block(results)
    records = block.as_records()
    assert len(records) == 2
    with env.md_api.begin():
        assert env.md_api.count(select(DataFunctionLog)) == 1
        assert env.md_api.count(select(DataBlockLog)) == 1
        pl = env.md_api.execute(select(DataFunctionLog)).scalar_one_or_none()
        assert pl.node_key == source.key
        assert pl.node_start_state == {}
        assert pl.node_end_state == {"records_imported": chunk_size}
        assert pl.function_key == source.function
        assert pl.function_params == cfg
        assert pl.error is not None
        assert pl.started_at is not None
        assert pl.completed_at is not None
        assert FAIL_MSG in pl.error["error"]
        ns = env.md_api.execute(
            select(NodeState).filter(NodeState.node_key == pl.node_key)
        ).scalar_one_or_none()
        assert ns.state == {"records_imported": chunk_size}

    # Run again without failing, should see different result
    source.params["fail"] = False
    results = env.produce(graph=g, node=source, target_storage=s)
    block = get_stdout_block(results)
    records = block.as_records()
    assert len(records) == batch_size
    with env.md_api.begin():
        assert env.md_api.count(select(DataFunctionLog)) == 2
        assert env.md_api.count(select(DataBlockLog)) == 2
        pl = (
            env.md_api.execute(
                select(DataFunctionLog).order_by(DataFunctionLog.completed_at.desc())
            )
            .scalars()
            .first()
        )
        assert pl.node_key == source.key
        assert pl.node_start_state == {"records_imported": chunk_size}
        assert pl.node_end_state == {"records_imported": chunk_size + batch_size}
        assert pl.function_key == source.function
        assert pl.function_params == {"batches": batches, "fail": False}
        assert pl.error is None
        assert pl.started_at is not None
        assert pl.completed_at is not None
        ns = env.md_api.execute(
            select(NodeState).filter(NodeState.node_key == pl.node_key)
        ).scalar_one_or_none()
        assert ns.state == {"records_imported": chunk_size + batch_size}


# TODO
# def test_function_failure_with_input():
#     env = get_env()
#     s = env._local_python_storage
#     # Initial graph
#     batches = 2
#     cfg = {"batches": batches, "fail": True}
#     source = GraphCfg(key="source", function=customer_source.key, params=cfg)
#     g = GraphCfg(nodes=[source])
#     results = env.produce(graph=g, node=source, target_storage=s)
#     block = get_stdout_block(results)
#     records = block.as_records()
#     assert len(records) == 2
#     with env.md_api.begin():
#         assert env.md_api.count(select(DataFunctionLog)) == 1
#         assert env.md_api.count(select(DataBlockLog)) == 1
#         pl = env.md_api.execute(select(DataFunctionLog)).scalar_one_or_none()
#         assert pl.node_key == source.key
#         assert pl.node_start_state == {}
#         assert pl.node_end_state == {"records_imported": chunk_size}
#         assert pl.function_key == source.function
#         assert pl.function_params == cfg
#         assert pl.error is not None
#         assert pl.started_at is not None
#         assert pl.completed_at is not None
#         assert FAIL_MSG in pl.error["error"]
#         ns = env.md_api.execute(
#             select(NodeState).filter(NodeState.node_key == pl.node_key)
#         ).scalar_one_or_none()
#         assert ns.state == {"records_imported": chunk_size}

#     # Run again without failing, should see different result
#     source.params["fail"] = False
#     results = env.produce(graph=g, node=source, target_storage=s)
#     block = get_stdout_block(results)
#     records = block.as_records()
#     assert len(records) == batch_size
#     with env.md_api.begin():
#         assert env.md_api.count(select(DataFunctionLog)) == 2
#         assert env.md_api.count(select(DataBlockLog)) == 2
#         pl = (
#             env.md_api.execute(
#                 select(DataFunctionLog).order_by(DataFunctionLog.completed_at.desc())
#             )
#             .scalars()
#             .first()
#         )
#         assert pl.node_key == source.key
#         assert pl.node_start_state == {"records_imported": chunk_size}
#         assert pl.node_end_state == {"records_imported": chunk_size + batch_size}
#         assert pl.function_key == source.function
#         assert pl.function_params == {"batches": batches, "fail": False}
#         assert pl.error is None
#         assert pl.started_at is not None
#         assert pl.completed_at is not None
#         ns = env.md_api.execute(
#             select(NodeState).filter(NodeState.node_key == pl.node_key)
#         ).scalar_one_or_none()
#         assert ns.state == {"records_imported": chunk_size + batch_size}


def test_node_reset():
    env = get_env()
    s = env._local_python_storage
    # Initial graph
    batches = 2
    cfg = {"batches": batches}
    source = GraphCfg(key="source", function=customer_source.key, params=cfg)
    accum = GraphCfg(key="accum", function="core.accumulator", input="source")
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="accum")
    g = GraphCfg(nodes=[source, accum, metrics])
    # Run first time
    results = env.produce(node=source, graph=g, target_storage=s)
    og_block = get_stdout_block(results)
    # Now reset node
    with env.md_api.begin():
        state = get_or_create_state(env, "source")
        assert "records_imported" in state.state
        assert state.latest_log is not None
        reset(env, "source")
        state = get_or_create_state(env, "source")
        assert state.state is None
        assert state.latest_log is None

    results = env.produce(node=metrics, graph=g, target_storage=s)
    block = get_stdout_block(results)
    records = block.as_records()
    expected_records = [
        {"metric": "row_count", "value": batch_size},  # Just one run of source, not two
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records

    # Test deleting invalidated blocks
    # Run once more, so we have some stale blocks
    time.sleep(1.5)
    results = env.produce(node=metrics, graph=g, target_storage=s)
    with env.md_api.begin():
        assert env.md_api.count(select(DataBlockLog)) == 12
        assert env.md_api.count(select(DataBlockMetadata)) == 7
        assert env.md_api.count(select(StoredDataBlockMetadata)) == 12
        # Invalidate
        reset(env, "source")
    # Delete
    sdb = og_block.stored_data_blocks[0]
    assert s.get_api().exists(sdb.name)
    env.permanently_delete_invalidated_blocks()
    assert not s.get_api().exists(sdb.name)
    with env.md_api.begin():
        assert env.md_api.count(select(DataBlockLog)) == 12
        assert (
            env.md_api.count(
                select(DataBlockLog).filter(DataBlockLog.invalidated == True)
            )
            == 3
        )
        assert env.md_api.count(select(DataBlockMetadata)) == 7
        assert (
            env.md_api.count(
                select(DataBlockMetadata).filter(DataBlockMetadata.deleted == False)
            )
            == 5
        )
        assert env.md_api.count(select(StoredDataBlockMetadata)) == 9

        # Invalidate old accums and dedupes
        # Which is just the first accum in this case
        cnt = env.invalidate_stale_blocks()
        assert cnt == 1
        assert (
            env.md_api.count(
                select(DataBlockLog).filter(DataBlockLog.invalidated == True)
            )
            == 4
        )
        # Invalidate metric node too now
        cnt = env.invalidate_stale_blocks(all_nodes=True)
        assert cnt == 1
        assert (
            env.md_api.count(
                select(DataBlockLog).filter(DataBlockLog.invalidated == True)
            )
            == 5
        )


@datafunction
def with_latest_metrics_no_ref(metrics: DataBlock[Metric], cust: DataBlock[Customer]):
    m = metrics.as_dataframe()
    c = cust.as_dataframe()
    return pd.concat([m, c])


@datafunction
def with_latest_metrics(cust: Consumable[Customer], metrics: Reference[Metric]):
    m = metrics.as_dataframe()
    c = cust.as_dataframe()
    return pd.concat([m, c])


def test_ref_input():
    env = get_env()
    env.add_function(with_latest_metrics)
    env.add_function(with_latest_metrics_no_ref)
    s = env._local_python_storage
    # Initial graph
    batches = 2
    cfg = {"batches": batches}
    source = GraphCfg(key="source", function=customer_source.key, params=cfg)
    accum = GraphCfg(key="accum", function="core.accumulator", input="source")
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="source")
    join_ref = GraphCfg(
        key="join_ref",
        function=with_latest_metrics.key,
        inputs={"metrics": "metrics", "cust": "source"},
    )
    join = GraphCfg(
        key="join",
        function=with_latest_metrics_no_ref.key,
        inputs={"metrics": "metrics", "cust": "source"},
    )
    g = GraphCfg(nodes=[source, accum, metrics, join, join_ref])
    # Run once, for one metrics output
    results = env.produce(node=metrics, graph=g, target_storage=s)

    # Both joins work
    results = env.run_node(join_ref, graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block
    results = env.run_node(join, graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block
    # Run source to create new customers, but NOT new metrics
    results = env.run_node(source, graph=g, target_storage=s)
    # This time only ref will still have a metrics input
    results = env.run_node(join_ref, graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block
    results = env.run_node(join, graph=g, target_storage=s)
    assert not results  # Regular join has exhausted metrics


def test_multi_env():
    env1 = get_env(key="e1")
    s = env1._local_python_storage
    # Initial graph
    batches = 2
    source = GraphCfg(
        key="source", function=customer_source.key, params={"batches": batches}
    )
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="source")
    g = GraphCfg(nodes=[source, metrics])
    # Run first time
    results = env1.produce(node=metrics, graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block
    with env1.md_api.begin():
        assert env1.md_api.count(select(DataFunctionLog)) == 2
        assert env1.md_api.count(select(DataBlockLog)) == 3

    env2 = get_env(key="e2", db_url=env1.metadata_storage.url)
    s = env2._local_python_storage
    # Initial graph
    batches = 2
    # Run first time
    results = env2.produce(node=metrics, graph=g, target_storage=s)
    block = get_stdout_block(results)
    assert block
    with env2.md_api.begin():
        assert env2.md_api.count(select(DataFunctionLog)) == 2
        assert env2.md_api.count(select(DataBlockLog)) == 3


def test_remote_callback_listener():
    from flask import Flask, request

    app = Flask(__name__)

    calls = []

    @app.route("/", methods=["POST"])
    def handle_result():
        j = request.get_json()
        exe = ExecutableCfg(**j["executable"])
        result = ExecutionResult(**j["result"])
        get_global_metadata_result_handler()(exe, result)
        calls.append(j)
        return "Ok"

    port = 1618

    def run():
        app.run(port=port)

    import threading

    server = threading.Thread(
        target=run, daemon=True
    )  # Daemon so it dies with this process
    server.start()

    dburl = get_tmp_sqlite_db_url()
    storage = get_tmp_sqlite_db_url()
    env = Environment(DataspaceCfg(metadata_storage=dburl, storages=[storage]))
    env.add_module(core)

    with env.md_api.begin():
        assert env.md_api.count(select(DataFunctionLog)) == 0
        assert env.md_api.count(select(DataBlockLog)) == 0
        assert env.md_api.count(select(DataBlockMetadata)) == 0

    df = pd.DataFrame({"a": range(10), "b": range(10)})
    n = GraphCfg(key="n1", function="import_dataframe", params={"dataframe": df},)
    env.produce(
        "n1",
        graph=GraphCfg(nodes=[n]),
        target_storage=storage,
        result_handler=ResultHandler(
            type=RemoteCallbackMetadataExecutionResultHandler.__name__,
            cfg=dict(callback_url=f"http://localhost:{port}/"),
        ),
    )

    with env.md_api.begin():
        assert env.md_api.count(select(DataFunctionLog)) == 1
        assert env.md_api.count(select(DataBlockLog)) == 1
        assert env.md_api.count(select(DataBlockMetadata)) == 1
