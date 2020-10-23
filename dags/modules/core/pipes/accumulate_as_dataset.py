from __future__ import annotations

from dags.core.pipe import Pipe, pipe_chain
from dags.modules.core.pipes.accumulator import dataframe_accumulator, sql_accumulator
from dags.modules.core.pipes.as_dataset import as_dataset, as_dataset_sql
from dags.modules.core.pipes.dedupe import dedupe_unique_keep_newest_row
from dags.testing.pipes import PipeTest

sql_accumulate_as_dataset = pipe_chain(
    key=f"core.sql_accumulate_as_dataset",
    pipe_chain=[sql_accumulator, dedupe_unique_keep_newest_row, as_dataset_sql],
)
dataframe_accumulate_as_dataset = pipe_chain(
    key=f"core.dataframe_accumulate_as_dataset",
    pipe_chain=[dataframe_accumulator, as_dataset],  # TODO: add dedupe
)


# def with_dataset(fn: Pipe) -> Pipe:
#     return pipe_chain(
#         name=f"accumulate_{fn.name}_as_dataset",
#         pipe_chain=[fn, "accumulator", dedupe_unique_keep_newest_row, as_dataset],
#     )


sql_accumulate_as_dataset_test = PipeTest(
    pipe="core.sql_accumulate_as_dataset",
    tests=[
        {
            "name": "test_dupe",
            "test_data": {
                "input": {
                    "otype": "CoreTestType",
                    "data": """
                            k1,k2,f1,f2,f3
                            1,2,abc,1.1,1
                            1,2,def,1.1,{"1":2}
                            1,3,abc,1.1,2
                            1,4,,,"[1,2,3]"
                            2,2,1.0,2.1,"[1,2,3]"
                        """,
                },
                "output": {
                    "otype": "CoreTestType",
                    "data": """
                            k1,k2,f1,f2,f3
                            1,2,abc,1.1,1
                            1,3,abc,1.1,2
                            1,4,,,"[1,2,3]"
                            2,2,1.0,2.1,"[1,2,3]"
                        """,
                },
            },
        }
    ],
)
