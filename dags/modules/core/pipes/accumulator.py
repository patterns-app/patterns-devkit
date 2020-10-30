from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from dags.core.data_block import DataBlock, DataSet
from dags.core.pipe import pipe
from dags.core.sql.pipe import sql_pipe
from dags.testing.utils import (
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
    str_as_dataframe,
)
from dags.utils.pandas import assert_dataframes_are_almost_equal
from dags.utils.typing import T


# Note this pipe is special case where cannot specify dataset as output (even tho practically it is)
# Since it IS part of a dataset creation
# (TODO: this seems like a revealing bug? need tighter definition of DataSet vs other aggregates)
@pipe("dataframe_accumulator", module="core")
def dataframe_accumulator(
    input: DataBlock[T], this: Optional[DataBlock[T]] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


sql_accumulator = sql_pipe(
    name="sql_accumulator",
    module="core",
    sql="""
    select -- DataBlock[T]
    * from input -- DataBlock[T]
    {% if inputs.this.bound_data_block %}
    union all
    select * from this -- Optional[DataBlock[T]]
    {% endif %}
    """,
)


def test():
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
            expected_df = str_as_dataframe(expected)
            db = produce_pipe_output_for_static_input(p, input=input_data, target_storage=s)
            df = db.as_dataframe()
            assert_dataframes_are_almost_equal(df, expected_df)

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
