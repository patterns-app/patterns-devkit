from __future__ import annotations
from contextlib import contextmanager
import os

import sys
from datetime import datetime
from typing import Generator, Iterator, Optional

import pandas as pd
import pytest
from commonmodel.base import create_quick_schema
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.storage.database.engines.postgres import PostgresDatabaseStorageApi
from loguru import logger
from pandas._testing import assert_almost_equal
from snapflow import DataBlock, datafunction
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment, produce
from snapflow import DataFunctionContext
from snapflow.core.sql.sql_function import sql_function_factory
from snapflow.modules import core
from sqlalchemy import select
from tests.test_e2e import Customer

IS_CI = os.environ.get("CI")


@datafunction
def funky_source(
    ctx: DataFunctionContext, batches: int, fail: bool = False
) -> Records[Customer]:
    # Gives different schema on each call
    runs = ctx.get_state_value("run_number", 0)
    records = [
        {"name": f"name{n}", "joined": datetime(2000, 1, n + 1), "metadata": None,}
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
                "joined": None,
                "metadata": {"idx": n},
                "new_field": "suprise!",
            }
            for n in range(10)
        ]
    if runs == 3:
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


@contextmanager
def get_env(key="_test"):
    if not IS_CI:
        with PostgresDatabaseStorageApi.temp_local_database() as db_url:
            env = Environment(
                DataspaceCfg(
                    key=key, metadata_storage=get_tmp_sqlite_db_url(), storages=[db_url]
                )
            )
            env.add_module(core)
            env.add_schema(Customer)
            yield env
    else:
        db_url = get_tmp_sqlite_db_url()
        env = Environment(
            DataspaceCfg(
                key=key, metadata_storage=db_url, storages=[get_tmp_sqlite_db_url()]
            )
        )
        env.add_module(core)
        env.add_schema(Customer)
        yield env


def test_source():
    with get_env() as env:
        s = env._local_python_storage
        source = GraphCfg(key="source", function=funky_source.key)
        g = GraphCfg(nodes=[source])
        # Run first time
        blocks = env.produce("source", graph=g, target_storage=s)
        assert blocks[0].nominal_schema_key == "Customer"
        assert len(blocks[0].realized_schema.fields) == 3
        records = blocks[0].as_records()
        assert len(records) == 10
        assert len(records[0]) == 3
        # RUn again
        blocks = env.produce("source", graph=g, target_storage=s)
        assert blocks[0].nominal_schema_key == "Customer"
        assert len(blocks[0].realized_schema.fields) == 4


def test_accumulate():
    source = GraphCfg(key="source", function=funky_source.key)
    accumulate = GraphCfg(key="accumulate", function="core.accumulator", input="source")
    accumulate_sql = GraphCfg(
        key="accumulate", function="core.accumulator_sql", input="source"
    )

    def run_accumulate(env, g, s):
        blocks = env.produce("accumulate", graph=g, target_storage=s)
        assert len(blocks) == 1
        records = blocks[0].as_records()
        assert len(records) == 10
        assert len(records[0]) == 3
        # Run second time
        blocks = env.produce("accumulate", graph=g, target_storage=s)
        assert len(blocks) == 1
        records = blocks[0].as_records()
        assert len(records) == 20
        assert len(records[0]) == 4
        # Run third time
        blocks = env.produce("accumulate", graph=g, target_storage=s)
        assert len(blocks) == 1
        records = blocks[0].as_records()
        assert len(records) == 30
        assert len(records[0]) == 4
        # Run fourth time
        blocks = env.produce("accumulate", graph=g, target_storage=s)
        assert len(blocks) == 1
        records = blocks[0].as_records()
        assert len(records) == 40
        assert len(records[0]) == 4

    with get_env() as env:
        s = env._local_python_storage
        run_accumulate(env, GraphCfg(nodes=[source, accumulate]), s)
    with get_env() as env:
        dbs = env.get_storages()[0]
        run_accumulate(env, GraphCfg(nodes=[source, accumulate_sql]), dbs)
