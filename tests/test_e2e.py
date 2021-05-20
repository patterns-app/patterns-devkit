from __future__ import annotations

import sys
from datetime import datetime
from typing import Generator, Iterator, Optional

import pandas as pd
import pytest
from commonmodel.base import create_quick_schema
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from loguru import logger
from pandas._testing import assert_almost_equal
from snapflow import DataBlock, datafunction
from snapflow.core.data_block import Consumable, Reference
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment, produce
from snapflow.core.execution import DataFunctionContext
from snapflow.core.sql.sql_function import sql_function_factory
from snapflow.core.state import (
    DataBlockLog,
    DataFunctionLog,
    NodeState,
    _reset_state,
    get_state,
    reset,
)
from snapflow.modules import core
from sqlalchemy import select

# logger.enable("snapflow")

Customer = create_quick_schema(
    "Customer", [("name", "Text"), ("joined", "DateTime"), ("metadata", "Json")]
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
                    "metadata": {"idx": n},
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
    env = Environment(DataspaceCfg(metadata_storage=dburl))
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    n = GraphCfg(key="n1", function="import_dataframe", params={"dataframe": df})
    blocks = env.produce(GraphCfg(nodes=[n]), "n1")
    assert_almost_equal(blocks[0].as_dataframe(), df, check_dtype=False)


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
    blocks = env.produce(g, "metrics", target_storage=s)
    assert blocks[0].nominal_schema_key.endswith("Metric")
    records = blocks[0].as_records()
    expected_records = [
        {"metric": "row_count", "value": batch_size},
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records
    # Run again, should get next batch
    blocks = env.produce(g, "metrics", target_storage=s)
    records = blocks[0].as_records()
    assert records == expected_records
    # Test latest_output
    block = env.get_latest_output(metrics)
    records = block.as_records()
    assert records == expected_records
    # Run again, should be exhausted
    blocks = env.produce(g, "metrics", target_storage=s)
    assert len(blocks) == 0
    # Run again, should still be exhausted
    blocks = env.produce(g, "metrics", target_storage=s)
    assert len(blocks) == 0

    # now add new node and process all at once
    newnode = GraphCfg(
        key="new_accumulator", function="core.accumulator", input="source"
    )
    g = GraphCfg(nodes=g.nodes + [newnode])
    blocks = env.produce(g, "new_accumulator", target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == N
    blocks = env.produce(g, "new_accumulator", target_storage=s)
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
    blocks = produce(node=metrics, graph=g, target_storage=s, env=env)
    assert len(blocks) == 1
    output = blocks[0]
    assert output.nominal_schema_key.endswith("Metric")
    records = blocks[0].as_records()
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
    blocks = produce(graph=g, node=source, target_storage=s, env=env)
    assert len(blocks) == 1
    records = blocks[0].as_records()
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
        assert FAIL_MSG in pl.error["error"]
        ns = env.md_api.execute(
            select(NodeState).filter(NodeState.node_key == pl.node_key)
        ).scalar_one_or_none()
        assert ns.state == {"records_imported": chunk_size}

    # Run again without failing, should see different result
    source.params["fail"] = False
    blocks = produce(graph=g, node=source, target_storage=s, env=env)
    assert len(blocks) == 1
    records = blocks[0].as_records()
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
        ns = env.md_api.execute(
            select(NodeState).filter(NodeState.node_key == pl.node_key)
        ).scalar_one_or_none()
        assert ns.state == {"records_imported": chunk_size + batch_size}


def test_node_reset():
    env = get_env()
    s = env._local_python_storage
    # Initial graph
    batches = 2
    cfg = {"batches": batches}
    source = GraphCfg(key="source", function=customer_source.key, params=cfg)
    accum = GraphCfg(key="accum", function="core.accumulator", input="source")
    metrics = GraphCfg(key="metrics", function=shape_metrics.key, input="source")
    g = GraphCfg(nodes=[source, accum, metrics])
    # Run first time
    produce(node=source, graph=g, target_storage=s, env=env)

    # Now reset node
    with env.md_api.begin():
        state = get_state(env, "source")
        assert state.state is not None
        reset(env, "source")
        state = get_state(env, "source")
        assert state is None

    blocks = produce(node=metrics, graph=g, target_storage=s, env=env)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    expected_records = [
        {"metric": "row_count", "value": batch_size},  # Just one run of source, not two
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records


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
    output = produce(node=metrics, graph=g, target_storage=s, env=env)

    # Both joins work
    output = env.run_node(join_ref, g, target_storage=s)
    assert output.output_blocks
    output = env.run_node(join, g, target_storage=s)
    assert output.output_blocks
    # Run source to create new customers, but NOT new metrics
    output = env.run_node(source, g, target_storage=s)
    # This time only ref will still have a metrics input
    output = env.run_node(join_ref, g, target_storage=s)
    assert output.output_blocks
    output = env.run_node(join, g, target_storage=s)
    assert not output.output_blocks  # Regular join has exhausted metrics


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
    blocks = env1.produce(node=metrics, graph=g, target_storage=s)
    assert len(blocks) == 1
    with env1.md_api.begin():
        assert env1.md_api.count(select(DataFunctionLog)) == 2
        assert env1.md_api.count(select(DataBlockLog)) == 3

    env2 = get_env(key="e2", db_url=env1.metadata_storage.url)
    s = env2._local_python_storage
    # Initial graph
    batches = 2
    # Run first time
    blocks = env2.produce(node=metrics, graph=g, target_storage=s)
    assert len(blocks) == 1
    with env2.md_api.begin():
        assert env2.md_api.count(select(DataFunctionLog)) == 2
        assert env2.md_api.count(select(DataBlockLog)) == 3
