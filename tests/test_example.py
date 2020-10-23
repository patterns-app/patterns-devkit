from __future__ import annotations

import sys
from datetime import datetime
from typing import Generator

import pandas as pd
import pytest
from pandas._testing import assert_almost_equal

from dags import DataBlock, DataSet, pipe
from dags.core.data_formats import RecordsList, RecordsListGenerator
from dags.core.environment import Environment
from dags.core.graph import Graph
from dags.core.node import DataBlockLog, PipeLog
from dags.core.runnable import PipeContext
from dags.core.typing.object_type import create_quick_otype
from dags.modules import core
from loguru import logger


def test_example():
    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    env.add_storage("memory://test")
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    g.add_node("n1", "extract_dataframe", config={"dataframe": df})
    output = env.produce(g, "n1")
    assert_almost_equal(output.as_dataframe(), df)


Customer = create_quick_otype(
    "Customer", [("name", "Unicode"), ("joined", "DateTime"), ("metadata", "JSON"),]
)
Metric = create_quick_otype(
    "Metric", [("metric", "Unicode"), ("value", "Numeric(12,2"),]
)


@pipe
def shape_metrics(i1: DataBlock) -> RecordsList[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@pipe
def aggregate_metrics(i1: DataSet) -> RecordsList[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@pipe
def customer_source(ctx: PipeContext) -> RecordsListGenerator[Customer]:
    N = ctx.get_config_value("total_records")
    n = ctx.get_state_value("records_extracted", 0)
    if n >= N:
        return
    for i in range(2):
        records = []
        for j in range(2):
            print(n, N)
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
        if n >= N:
            return


def test_incremental():
    logger.enable("dags")
    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    s = env.add_storage("memory://test")
    env.add_module(core)
    N = 2 * 4
    g.add_node("source", customer_source, config={"total_records": N})
    g.add_node("metrics", shape_metrics, inputs="source")
    output = env.produce(g, "metrics", target_storage=s)
    records = output.as_records_list()
    assert_almost_equal(
        records,
        [{"metric": "row_count", "value": 4}, {"metric": "col_count", "value": 3}],
    )
    # Run again, should get next batch
    output = env.produce(g, "metrics", target_storage=s)
    records = output.as_records_list()
    assert_almost_equal(
        records,
        [{"metric": "row_count", "value": 4}, {"metric": "col_count", "value": 3}],
    )
    # Run again, should be exhausted
    output = env.produce(g, "metrics", target_storage=s)
    assert output is None
    # Run again, should still be exhausted
    output = env.produce(g, "metrics", target_storage=s)
    assert output is None

    # Now test DataSet aggregation
    g.add_node("aggregate_metrics", aggregate_metrics, inputs="source")
    output = env.produce(g, "aggregate_metrics", target_storage=s)
    with env.session_scope() as sess:
        for dbl in sess.query(DataBlockLog).all():
            print(
                f"{dbl.pipe_log.node_key:50}{dbl.data_block_id:20}{dbl.direction.value:10}{dbl.data_block.updated_at}"
            )
    records = output.as_records_list()
    assert_almost_equal(
        records,
        [{"metric": "row_count", "value": 8}, {"metric": "col_count", "value": 3}],
    )
