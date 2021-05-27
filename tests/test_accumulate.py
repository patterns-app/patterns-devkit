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
from snapflow.core.environment import Environment, produce
from snapflow.core.execution import DataFunctionContext
from snapflow.core.function_interface import Consumable, Reference
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, DataFunctionLog, NodeState
from snapflow.core.sql.sql_function import sql_function_factory
from snapflow.modules import core
from sqlalchemy import select
from tests.test_e2e import Customer


@datafunction
def funky_source(
    ctx: DataFunctionContext, batches: int, fail: bool = False
) -> Records[Customer]:
    # Gives different schema on each call
    runs = ctx.get_state_value("run_number", 0)
    records = [
        {
            "name": f"name{n}",
            "joined": datetime(2000, 1, n + 1),
            "metadata": {"idx": n},
        }
        for n in range(10)
    ]
    if runs == 1:
        # New field
        records = [
            {
                "name": f"name{n}",
                "joined": datetime(2000, 1, n + 1),
                "metadata": {"idx": n},
                "new_field": "suprise!",
            }
            for n in range(10)
        ]
    if runs == 2:
        # Different / bad datatype
        records = [
            {
                "name": f"name{n}",
                "joined": 123,
                "metadata": {"idx": n},
                "new_field": "suprise!",
            }
            for n in range(10)
        ]
    if runs > 3:
        # null field
        records = [
            {
                "name": None,
                "joined": datetime(2000, 1, n + 1),
                "metadata": {"idx": n},
                "new_field": "suprise!",
            }
            for n in range(10)
        ]
    if runs > 3:
        # missing field
        records = [
            {"joined": datetime(2000, 1, n + 1), "metadata": {"idx": n},}
            for n in range(10)
        ]
    ctx.emit_state_value("run_number", runs + 1)
    return records


def get_env(key="_test", db_url=None):
    if db_url is None:
        db_url = get_tmp_sqlite_db_url()
    env = Environment(key=key, metadata_storage=db_url)
    env.add_module(core)
    env.add_schema(Customer)
    return env


def test_source():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    g.create_node(key="source", function=funky_source)
    # Run first time
    blocks = env.produce("source", g, target_storage=s)
    assert blocks[0].nominal_schema_key == "Customer"
    assert len(blocks[0].realized_schema.fields) == 3
    records = blocks[0].as_records()
    assert len(records) == 10
    assert len(records[0]) == 3
    # RUn again
    blocks = env.produce("source", g, target_storage=s)
    assert blocks[0].nominal_schema_key == "Customer"
    assert len(blocks[0].realized_schema.fields) == 4


def test_accumulate():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    g.create_node(key="source", function=funky_source)
    g.create_node(key="accumulate", function="core.accumulator", input="source")
    # Run first time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 10
    assert len(records[0]) == 3
    # Run second time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 20
    assert len(records[0]) == 4
    # Run third time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 30
    assert len(records[0]) == 4
    # Run fourth time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 40
    assert len(records[0]) == 4


def test_accumulate_sql():
    env = get_env()
    g = Graph(env)
    s = get_tmp_sqlite_db_url()
    g.create_node(key="source", function=funky_source)
    g.create_node(key="accumulate", function="core.accumulator_sql", input="source")
    # Run first time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 10
    assert len(records[0]) == 3
    # Run second time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 20
    assert len(records[0]) == 4
    # Run third time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 30
    assert len(records[0]) == 4
    # Run fourth time
    blocks = env.produce("accumulate", g, target_storage=s)
    assert len(blocks) == 1
    records = blocks[0].as_records()
    assert len(records) == 40
    assert len(records[0]) == 4
