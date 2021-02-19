from __future__ import annotations

import sys
from datetime import datetime
from pprint import pprint
from typing import Generator

import pandas as pd
import pytest
from loguru import logger
from pandas._testing import assert_almost_equal
from snapflow.core.node import DataBlockLog, PipeLog
from snapflow import DataBlock, pipe, sql_pipe
from snapflow.core.environment import Environment, produce
from snapflow.core.execution import PipeContext
from snapflow.core.graph import Graph
from snapflow.modules import core
from snapflow.schema.base import create_quick_schema
from snapflow.storage.data_formats import Records, RecordsIterator
from snapflow.storage.storage import new_local_python_storage


def test_example():
    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    g.create_node(key="n1", pipe="extract_dataframe", config={"dataframe": df})
    output = env.produce("n1", g)
    assert_almost_equal(output.as_dataframe(), df)


Customer = create_quick_schema(
    "Customer", [("name", "Unicode"), ("joined", "DateTime"), ("metadata", "JSON")]
)
Metric = create_quick_schema(
    "Metric", [("metric", "Unicode"), ("value", "Numeric(12,2)")]
)


@pipe
def shape_metrics(i1: DataBlock) -> Records[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@pipe
def aggregate_metrics(i1: DataBlock) -> Records[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


FAIL_MSG = "Failure triggered"


@pipe
def customer_source(ctx: PipeContext) -> RecordsIterator[Customer]:
    N = ctx.get_config_value("total_records")
    fail = ctx.get_config_value("fail")
    n = ctx.get_state_value("records_extracted", 0)
    if n >= N:
        return
    for i in range(2):
        records = []
        for j in range(2):
            records.append(
                {
                    "name": f"name{n}",
                    "joined": datetime(2000, 1, n + 1),
                    "metadata": {"idx": n},
                }
            )
            n += 1
        yield records
        ctx.emit_state_value("records_extracted", n)
        if fail:
            # Fail AFTER yielding one record set
            raise Exception(FAIL_MSG)
        if n >= N:
            return


aggregate_metrics_sql = sql_pipe(
    "aggregate_metrics_sql",
    sql="""
    select -- :Metric
        'row_count' as metric,
        count(*) as value
    from input
    """,
)


dataset_inputs_sql = sql_pipe(
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


mixed_inputs_sql = sql_pipe(
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


def get_env():
    env = Environment(metadata_storage="sqlite://")
    env.add_module(core)
    env.add_schema(Customer)
    env.add_schema(Metric)
    return env


def test_repeated_runs():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    g.create_node(key="source", pipe=customer_source, config={"total_records": N})
    metrics = g.create_node(key="metrics", pipe=shape_metrics, upstream="source")
    # Run first time
    output = env.produce("metrics", g, target_storage=s)
    assert output.nominal_schema_key.endswith("Metric")
    records = output.as_records()
    expected_records = [
        {"metric": "row_count", "value": 4},
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records
    # Run again, should get next batch
    output = env.produce("metrics", g, target_storage=s)
    records = output.as_records()
    assert records == expected_records
    # Test latest_output
    output = env.latest_output(metrics)
    records = output.as_records()
    assert records == expected_records
    # Run again, should be exhausted
    output = env.produce("metrics", g, target_storage=s)
    assert output is None
    # Run again, should still be exhausted
    output = env.produce("metrics", g, target_storage=s)
    assert output is None

    # now add new node and process all at once
    g.create_node(
        key="new_accumulator", pipe="core.dataframe_accumulator", upstream="source"
    )
    output = env.produce("new_accumulator", g, target_storage=s)
    records = output.as_records()
    assert len(records) == N
    output = env.produce("new_accumulator", g, target_storage=s)
    assert output is None


def test_alternate_apis():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    source = g.create_node(customer_source, config={"total_records": N})
    metrics = g.create_node(shape_metrics, upstream=source)
    # Run first time
    output = produce(metrics, graph=g, target_storage=s, env=env)
    assert output.nominal_schema_key.endswith("Metric")
    records = output.as_records()
    expected_records = [
        {"metric": "row_count", "value": 4},
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records


def test_pipe_failure():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    cfg = {"total_records": N, "fail": True}
    source = g.create_node(customer_source, config=cfg)
    output = produce(source, graph=g, target_storage=s, env=env)
    assert output is not None
    records = output.as_records()
    assert len(records) == 2
    with env.session_scope() as sess:
        assert sess.query(PipeLog).count() == 1
        assert sess.query(DataBlockLog).count() == 1
        pl = sess.query(PipeLog).first()
        assert pl.node_key == source.key
        assert pl.graph_id == g.get_metadata_obj().hash
        assert pl.node_start_state == {}
        assert pl.node_end_state == {"records_extracted": 2}
        assert pl.pipe_key == source.pipe.key
        assert pl.pipe_config == cfg
        assert pl.error is not None
        assert FAIL_MSG in pl.error["error"]

    source.config["fail"] = False
    output = produce(source, graph=g, target_storage=s, env=env)
    with env.session_scope() as sess:
        assert sess.query(PipeLog).count() == 2
        assert sess.query(DataBlockLog).count() == 1
        pl = sess.query(PipeLog).order_by(PipeLog.completed_at.desc()).first()
        assert pl.node_key == source.key
        assert pl.graph_id == g.get_metadata_obj().hash
        assert pl.node_start_state == {}
        assert pl.node_end_state == {"records_extracted": 4}
        assert pl.pipe_key == source.pipe.key
        assert pl.pipe_config == cfg
        assert pl.error is None
