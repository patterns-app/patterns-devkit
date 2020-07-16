from __future__ import annotations

from pandas import DataFrame

from basis.core.data_block import DataBlock
from basis.core.data_function import data_function
from basis.core.sql.data_function import sql_data_function
from basis.testing.functions import DataFunctionTest
from basis.utils.typing import T

accumulator_test = DataFunctionTest(
    function="accumulator",
    tests=[
        {
            "name": "test_empty_this",
            "test_data": [
                {
                    "input": {
                        "otype": "CoreTestType",
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
                        "otype": "CoreTestType",
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
                        "otype": "CoreTestType",
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
                        "otype": "CoreTestType",
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
                    "otype": "CoreTestType",
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
                    "otype": "CoreTestType",
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
    #         otype: CoreTestType
    #         k1,k2,f1,f2,f3
    #         1,2,abc,1.1,1
    #         1,2,def,1.1,{"1":2}
    #         1,3,abc,1.1,2
    #         1,4,,,"[1,2,3]"
    #         2,2,1.0,2.1,"[1,2,3]"
    #     """,
    #     this="""
    #         otype: CoreTestType
    #         k1,k2,f1,f2,f3
    #         1,5,abc,1.1,
    #         1,6,abc,1.1,2
    #     """,
    #     output="""
    #         otype: CoreTestType
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


@data_function(name="accumulator")  # , test_data="test_accumulator.yml")
def dataframe_accumulator(
    input: DataBlock[T], this: DataBlock[T] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


sql_accumulator = sql_data_function(
    name="accumulator",
    sql="""
    select:T * from input:T
    {% if inputs.this %}
    union all
    select * from this:Optional[T]
    {% endif %}
    """,
)
