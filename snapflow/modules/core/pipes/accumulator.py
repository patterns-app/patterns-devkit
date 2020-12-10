from __future__ import annotations

from typing import Optional

from loguru import logger
from pandas import DataFrame, concat
from snapflow.core.data_block import DataBlock
from snapflow.core.pipe import pipe
from snapflow.core.sql.pipe import sql_pipe
from snapflow.core.streams import Stream
from snapflow.core.typing.inference import conform_dataframe_to_schema
from snapflow.testing.utils import (
    DataInput,
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
    str_as_dataframe,
)
from snapflow.utils.pandas import assert_dataframes_are_almost_equal
from snapflow.utils.typing import T


@pipe("dataframe_accumulator", module="core")
def dataframe_accumulator(
    input: Stream[T],
    this: Optional[DataBlock[T]] = None,
) -> DataFrame[T]:
    accumulated_dfs = [block.as_dataframe() for block in input]
    if this is not None:
        accumulated_dfs = [this.as_dataframe()] + accumulated_dfs
    return concat(accumulated_dfs)


# TODO: this is no-op if "this" is empty... is there a way to shortcut?
sql_accumulator = sql_pipe(
    name="sql_accumulator",
    module="core",
    sql="""
    {% if inputs.this.bound_block %}
    select * from {{ inputs.this.bound_block.as_table_stmt() }}
    union all
    {% endif %}
    {% for block in inputs.input.bound_stream %}
    select
    * from {{ block.as_table_stmt() }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
    """,
    inputs={"this": "DataBlock[T]", "input": "Stream[T]"},
    output="DataBlock[T]",
)


def test_accumulator():
    from snapflow.modules import core

    input_data_1 = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
    """
    expected_1 = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
    """
    input_data_2 = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
        1,7,g,0,
    """
    expected_2 = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
        1,7,g,0,
    """
    s = get_tmp_sqlite_db_url()
    for input_data, expected in [
        (input_data_1, expected_1),
        (input_data_2, expected_2),
    ]:
        for p in [sql_accumulator, dataframe_accumulator]:
            data_input = DataInput(input_data, schema="CoreTestSchema", module=core)
            db = produce_pipe_output_for_static_input(
                p, input=data_input, target_storage=s
            )
            logger.debug(db)
            logger.debug("TEST df conversion")
            expected_df = DataInput(
                expected, schema="CoreTestSchema", module=core
            ).as_dataframe(db.manager.ctx.env)
            logger.debug("TEST df conversion 2")
            df = db.as_dataframe()
            assert_dataframes_are_almost_equal(
                df, expected_df, schema=core.schemas.CoreTestSchema
            )

    # TODO: how to test `this`?
    # test_recursive_input=dict(
    #     input="""
    #         schema: CoreTestSchema
    #         k1,k2,f1,f2,f3
    #         1,2,abc,1.1,1
    #         1,2,def,1.1,{"1":2}
    #         1,3,abc,1.1,2
    #         1,4,,,"[1,2,3]"
    #         2,2,1.0,2.1,"[1,2,3]"
    #     """,
    #     this="""
    #         schema: CoreTestSchema
    #         k1,k2,f1,f2,f3
    #         1,5,abc,1.1,
    #         1,6,abc,1.1,2
    #     """,
    #     output="""
    #         schema: CoreTestSchema
    #         k1,k2,f1,f2,f3
    #         1,2,def,1.1,{"1":2}
    #         1,3,abc,1.1,2
    #         1,4,,,"[1,2,3]"
    #         2,2,1.0,2.1,"[1,2,3]"
    #         1,5,abc,1.1,
    #         1,6,abc,1.1,2
    #     """,
    # ),
