from __future__ import annotations

import sys
from datetime import datetime
from pprint import pprint
from typing import Generator

import pandas as pd
import pytest
from loguru import logger
from pandas._testing import assert_almost_equal

from snapflow import DataBlock, pipe, sql_pipe
from snapflow.core.data_formats import RecordsList, RecordsListGenerator
from snapflow.core.environment import Environment
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, PipeLog
from snapflow.core.runnable import PipeContext
from snapflow.core.typing.schema import create_quick_schema
from snapflow.modules import core
from snapflow.testing.utils import get_tmp_sqlite_db_url
from snapflow.utils.common import utcnow


def test_example():
    env = Environment(metadata_storage="sqlite://")
    g = Graph(env)
    env.add_storage("memory://test")
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    g.create_node("n1", "extract_dataframe", config={"dataframe": df})
    output = env.produce(g, "n1")
    assert_almost_equal(output.as_dataframe(), df)


Customer = create_quick_schema(
    "Customer", [("name", "Unicode"), ("joined", "DateTime"), ("metadata", "JSON")]
)
Metric = create_quick_schema(
    "Metric", [("metric", "Unicode"), ("value", "Numeric(12,2)")]
)


@pipe
def shape_metrics(i1: DataBlock) -> RecordsList[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@pipe
def aggregate_metrics(i1: DataBlock) -> RecordsList[Metric]:
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


def test_incremental():
    env = get_env()
    g = Graph(env)
    s = env.add_storage("memory://test")
    # Initial graph
    N = 2 * 4
    g.create_node("source", customer_source, config={"total_records": N})
    g.create_node("metrics", shape_metrics, upstream="source")
    # Run first time
    output = env.produce(g, "metrics", target_storage=s)
    assert output.nominal_schema_key.endswith("Metric")
    records = output.as_records_list()
    # print(DataBlockLog.summary(env))
    expected_records = [
        {"metric": "row_count", "value": 4},
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records
    # Run again, should get next batch
    output = env.produce(g, "metrics", target_storage=s)
    records = output.as_records_list()
    assert records == expected_records
    # Run again, should be exhausted
    output = env.produce(g, "metrics", target_storage=s)
    assert output is None
    # Run again, should still be exhausted
    output = env.produce(g, "metrics", target_storage=s)
    assert output is None


#     # Now test DataSet aggregation
#     g.create_node("aggregate_metrics", aggregate_metrics, upstream="source")
#     output = env.produce(g, "aggregate_metrics", target_storage=s)
#     records = output.as_records_list()
#     # print(DataBlockLog.summary(env))
#     expected_records = [
#         {"metric": "row_count", "value": 8},
#         {"metric": "col_count", "value": 3},
#     ]
#     assert records == expected_records
#     # Run again, should be exhausted
#     output = env.produce(g, "aggregate_metrics", target_storage=s)
#     assert output is None
#
#     # And test DataSet aggregation in SQL
#     sdb = env.add_storage(get_tmp_sqlite_db_url())
#     g.create_node("aggregate_metrics_sql", aggregate_metrics_sql, upstream="source")
#     output = env.produce(g, "aggregate_metrics_sql", target_storage=sdb)
#     records = output.as_records_list()
#     expected_records = [{"metric": "row_count", "value": 8}]
#     assert records == expected_records
#
#     # Run again, should be exhausted
#     output = env.produce(g, "aggregate_metrics_sql", target_storage=sdb)
#     assert output is None
#
#     # Test dataset output
#     output = env.produce_dataset(g, "aggregate_metrics_sql", target_storage=sdb)
#     alias = "aggregate_metrics_sql"
#     row_cnt = sdb.get_database_api(env).count(alias)
#     assert row_cnt == 1
#     alias = "aggregate_metrics_sql__latest"
#     row_cnt = sdb.get_database_api(env).count(alias)
#     assert row_cnt == 1
#
#
# def test_mixed_inputs():
#     env = get_env()
#     g = Graph(env)
#     s = env.add_storage(get_tmp_sqlite_db_url())
#     # Initial graph
#     N = 4 * 4
#     g.create_node("source", customer_source, config={"total_records": N})
#     g.create_node("aggregate_metrics", aggregate_metrics, upstream="source")
#     output = env.produce(g, "aggregate_metrics", target_storage=s)
#     records = output.as_records_list()
#     expected_records = [
#         {"metric": "row_count", "value": 4},
#         {"metric": "col_count", "value": 3},
#     ]
#     assert records == expected_records
#     # Mixed inputs
#     g.create_node(
#         "mixed_inputs",
#         mixed_inputs_sql,
#         upstream={"input": "source", "metrics": "aggregate_metrics"},
#     )
#     g.create_node(
#         "dataset_inputs",
#         dataset_inputs_sql,
#         upstream={"input": "source", "metrics": "aggregate_metrics"},
#     )
#
#     output = env.produce(g, "dataset_inputs", target_storage=s)
#     records = output.as_records_list()
#     expected_records = [
#         {"tble": "input", "row_count": 8},
#         {"tble": "metrics", "row_count": 4},
#     ]
#     assert records == expected_records
#
#     output = env.produce(g, "mixed_inputs", target_storage=s)
#     records = output.as_records_list()
#     expected_records = [
#         {"tble": "input", "row_count": 4},
#         {"tble": "metrics", "row_count": 6},
#     ]
#     assert records == expected_records
#
#     # Run again
#     output = env.produce(g, "dataset_inputs", target_storage=s)
#     records = output.as_records_list()
#     expected_records = [
#         {"tble": "input", "row_count": 16},
#         {"tble": "metrics", "row_count": 8},
#     ]
#     assert records == expected_records
#
#     output = env.run_node(g, "mixed_inputs", target_storage=s)
#     records = output.as_records_list()
#     expected_records = [
#         {"tble": "input", "row_count": 4},  # DataBlock input does NOT accumulate
#         {"tble": "metrics", "row_count": 8},  # DataSet input does
#     ]
#     assert records == expected_records


if __name__ == "__main__":
    # test_mixed_inputs()
    pass
