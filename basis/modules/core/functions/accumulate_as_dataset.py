from __future__ import annotations

from basis.core.data_function import datafunction_chain
from basis.modules.core.functions.as_dataset import as_dataset
from basis.modules.core.functions.dedupe import dedupe_unique_keep_newest_row
from basis.testing.functions import DataFunctionTest

accumulate_as_dataset = datafunction_chain(
    name=f"accumulate_as_dataset",
    function_chain=["accumulator", dedupe_unique_keep_newest_row, as_dataset],
)


accumulate_as_dataset_test = DataFunctionTest(
    function="accumulate_as_dataset",
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
