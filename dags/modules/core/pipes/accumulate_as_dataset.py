from __future__ import annotations

from dags import Environment
from dags.core.node import DataBlockLog
from dags.core.pipe import Pipe, pipe_chain
from dags.modules.core.pipes.accumulator import dataframe_accumulator, sql_accumulator
from dags.modules.core.pipes.as_dataset import as_dataset, as_dataset_sql
from dags.modules.core.pipes.dedupe import (
    dataframe_dedupe_unique_keep_newest_row,
    sql_dedupe_unique_keep_newest_row,
)
from dags.testing.utils import (
    DataInput,
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
    str_as_dataframe,
)
from dags.utils.pandas import assert_dataframes_are_almost_equal

sql_accumulate_as_dataset = pipe_chain(
    name="sql_accumulate_as_dataset",
    module="core",
    pipe_chain=[sql_accumulator, sql_dedupe_unique_keep_newest_row, as_dataset_sql],
)

dataframe_accumulate_as_dataset = pipe_chain(
    name="dataframe_accumulate_as_dataset",
    module="core",
    pipe_chain=[
        dataframe_accumulator,
        dataframe_dedupe_unique_keep_newest_row,
        as_dataset,
    ],  # TODO: add dedupe
)


def test_accumulate_as_dataset():
    from dags.modules import core

    input_data = """
        k1,k2,f1,f2,f3
        1,2,abc,1.1,1
        1,2,def,1.1,{"1":2}
        1,3,abc,1.1,2
        1,4,,,"[1,2,3]"
        2,2,1.0,2.1,"[1,2,3]"
    """
    expected = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,3,abc,1.1,2,
        1,4,,,"[1,2,3]",
        2,2,1.0,2.1,"[1,2,3]",
    """
    # expected_df = str_as_dataframe(expected, schema=core.schemas.CoreTestSchema)
    expected_df = DataInput(
        expected, schema="CoreTestSchema", module=core
    ).as_dataframe()
    data_input = DataInput(input_data, schema="CoreTestSchema", module=core)
    s = get_tmp_sqlite_db_url()
    for p in [sql_accumulate_as_dataset, dataframe_accumulate_as_dataset]:
        db = produce_pipe_output_for_static_input(p, input=data_input, target_storage=s)
        df = db.as_dataframe()
        print(DataBlockLog.summary(db.manager.ctx.env))
        assert_dataframes_are_almost_equal(df, expected_df)
