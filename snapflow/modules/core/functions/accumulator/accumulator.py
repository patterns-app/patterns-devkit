from __future__ import annotations

from typing import Optional

from pandas import DataFrame, concat
from snapflow.core.data_block import DataBlock
from snapflow.core.function import Input, Output, datafunction
from snapflow.core.function_interface import SelfReference
from snapflow.core.streams import Stream
from snapflow.utils.typing import T


@datafunction(namespace="core", display_name="Accumulate DataFrames")
def accumulator(
    input: Stream[T],
    previous: SelfReference[T] = None,
) -> DataFrame[T]:
    # TODO: make this return a dataframe iterator right?
    accumulated_dfs = [block.as_dataframe() for block in input]
    if previous is not None:
        accumulated_dfs = [previous.as_dataframe()] + accumulated_dfs
    return concat(accumulated_dfs)

    # input_data_1 = """
    #     k1,k2,f1,f2,f3,f4
    #     1,2,abc,1.1,1,
    #     1,2,def,1.1,{"1":2},2012-01-01
    #     1,3,abc,1.1,2,2012-01-01
    #     1,4,,,"[1,2,3]",2012-01-01
    #     2,2,1.0,2.1,"[1,2,3]",2012-01-01
    # """
    # expected_1 = """
    #     k1,k2,f1,f2,f3,f4
    #     1,2,abc,1.1,1,
    #     1,2,def,1.1,{"1":2},2012-01-01
    #     1,3,abc,1.1,2,2012-01-01
    #     1,4,,,"[1,2,3]",2012-01-01
    #     2,2,1.0,2.1,"[1,2,3]",2012-01-01
    # """
    # input_data_2 = """
    #     k1,k2,f1,f2,f3,f4
    #     1,2,abc,1.1,1,
    #     1,2,def,1.1,{"1":2},2012-01-01
    #     1,3,abc,1.1,2,2012-01-01
    #     1,4,,,"[1,2,3]",2012-01-01
    #     2,2,1.0,2.1,"[1,2,3]",2012-01-01
    #     1,7,g,0,
    # """
    # expected_2 = """
    #     k1,k2,f1,f2,f3,f4
    #     1,2,abc,1.1,1,
    #     1,2,def,1.1,{"1":2},2012-01-01
    #     1,3,abc,1.1,2,2012-01-01
    #     1,4,,,"[1,2,3]",2012-01-01
    #     2,2,1.0,2.1,"[1,2,3]",2012-01-01
    #     1,7,g,0,
    # """

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
    #
    # ),
