from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from dags.core.data_block import DataBlock, DataSet
from dags.core.pipe import pipe
from dags.core.sql.pipe import sql_pipe
from dags.testing.pipes import PipeTest
from dags.utils.typing import T

accumulator_test = PipeTest(
    pipe="core.sql_accumulator",
    tests=[
        {
            "name": "test_empty_this",
            "test_data": [
                {
                    "input": {
                        "schema": "CoreTestSchema",
                        "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,abc,1.1,1,
                        1,2,def,1.1,{"1":2},2012-01-01
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                    """,
                    },
                    "this": {},
                    "output": {
                        "schema": "CoreTestSchema",
                        "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,abc,1.1,1,
                        1,2,def,1.1,{"1":2},2012-01-01
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                    """,
                    },
                },
                {
                    "input": {
                        "schema": "CoreTestSchema",
                        "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,abc,1.1,1,
                        1,2,def,1.1,{"1":2},2012-01-01
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                        1,7,g,0,
                    """,
                    },
                    "this": {},
                    "output": {
                        "schema": "CoreTestSchema",
                        "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,abc,1.1,1,
                        1,2,def,1.1,{"1":2},2012-01-01
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                        1,7,g,0,
                    """,
                    },
                },
            ],
        },
        {
            "name": "test_empty_this",
            "test_data": {
                "input": {
                    "schema": "CoreTestSchema",
                    "data": """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
            """,
                },
                "this": {},
                "output": {
                    "schema": "CoreTestSchema",
                    "data": """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,
        1,2,def,1.1,{"1":2},2012-01-01
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
            """,
                },
            },
        },
    ],
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
)


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
