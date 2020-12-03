from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from dags.core.data_block import DataBlock
from dags.core.pipe import pipe
from dags.core.sql.pipe import sql_pipe
from dags.core.typing.inference import conform_dataframe_to_schema
from dags.testing.utils import (
    DataInput,
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
    str_as_dataframe,
)
from dags.utils.pandas import assert_dataframes_are_almost_equal
from dags.utils.typing import T
from loguru import logger


@pipe("dataframe_accumulator", module="core")
def dataframe_accumulator(
    input: DataBlock[T],
    this: Optional[DataBlock[T]] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


# TODO: this is no-op if "this" is empty... is there a way to shortcut?
sql_accumulator = sql_pipe(
    name="sql_accumulator",
    module="core",
    sql="""
    {% if inputs.this.bound_data_block %}
    select * from this -- :Optional[DataBlock[T]]
    union all
    {% endif %}
    select -- :DataBlock[T]
    * from input -- :DataBlock[T]
    """,
)


def test_accumulator():
    from dags.modules import core

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
