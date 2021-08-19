from __future__ import annotations

from typing import List, Optional

from basis.core.block import Block, SelfReference, Stream
from basis.core.declarative.dataspace import BasisCfg, DataspaceCfg
from basis.core.declarative.execution import ExecutionCfg, ExecutionResult
from basis.core.declarative.graph import GraphCfg
from basis.core.environment import Environment
from basis.core.execution.context import FunctionContext
from basis.core.function import datafunction
from basis.core.module import BasisModule
from basis.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from basis.utils.typing import T
from commonmodel.base import create_quick_schema
from dcp.storage.base import Storage
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from pandas import DataFrame

TestSchema1 = create_quick_schema("TestSchema1", [("f1", "Text")], namespace="_test")
TestSchema2 = create_quick_schema("TestSchema2", [("f1", "Text")], namespace="_test")
TestSchema3 = create_quick_schema("TestSchema3", [("f1", "Text")], namespace="_test")
TestSchema4 = create_quick_schema(
    "TestSchema4",
    [("f1", "Text"), ("f2", "Integer")],
    unique_on=["f1"],
    namespace="_test",
)


def make_test_env(**kwargs) -> Environment:
    if "metadata_storage" not in kwargs:
        url = get_tmp_sqlite_db_url()
        kwargs["metadata_storage"] = url
    ds_args = dict(
        graph=GraphCfg(key="_test"), basis=BasisCfg(abort_on_function_error=True)
    )
    ds_args.update(**kwargs)
    ds = DataspaceCfg(**ds_args)
    env = Environment(dataspace=ds)
    test_module = BasisModule("_test",)
    for schema in [TestSchema1, TestSchema2, TestSchema3, TestSchema4]:
        env.add_schema(schema)
    for fn in all_functions:
        env.add_function(datafunction(fn))
    env.add_module(test_module)
    return env


def make_test_run_context(env: Environment = None, **kwargs) -> ExecutionCfg:
    s = f"python://_test_default_{rand_str(6)}"
    env = env or make_test_env()
    args = dict(
        dataspace=env.dataspace, local_storage=s, target_storage=s, storages=[s],
    )
    args.update(**kwargs)
    return ExecutionCfg(**args)


def function_t1_sink(ctx: FunctionContext, input: Block[TestSchema1]):
    pass


def function_t1_to_t2(input: Block[TestSchema1]) -> DataFrame[TestSchema2]:
    pass


def function_generic(input: Block[T]) -> DataFrame[T]:
    pass


def function_t1_source(ctx: FunctionContext) -> DataFrame[TestSchema1]:
    pass


def function_stream(input: Stream) -> Block:
    pass


function_chain_t1_to_t2 = function_t1_to_t2  # function_chain("function_chain_t1_to_t2", [function_t1_to_t2, function_generic])


def function_self(input: Block[T], previous: SelfReference[T]) -> DataFrame[T]:
    pass


def function_multiple_input(
    input: Block[T], other_t2: Optional[Block[TestSchema2]]
) -> DataFrame[T]:
    pass


def function_kitchen_sink(
    input: Block[T],
    other_t2: Optional[Block[TestSchema2]],
    param1: Optional[str] = "default",
) -> DataFrame[T]:
    """
    Inputs:
        input: input desc
        other_t2: other_t2 desc
    Params:
        param1: param1 desc
    Output:
        output desc
    """
    pass


def get_stdout_block(results: List[ExecutionResult]) -> Optional[Block]:
    assert len(results) == 1
    result = results[0]
    block = result.stdout()
    return block


all_functions = [
    function_chain_t1_to_t2,
    function_generic,
    function_multiple_input,
    function_self,
    function_stream,
    function_t1_sink,
    function_t1_source,
    function_t1_to_t2,
]

sample_records = [
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "2020",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "1.3",
        "g": 123,
        "h": "null",
        "i": None,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "1/1/2020",
        "c": "12",
        "d": [1, 2, 3],
        "e": {1: 2},
        "f": "cookies",
        "g": 123,
        "h": "null",
        "i": None,
    },
    {
        "a": "2017-02-17T15:09:26-08:00",
        "b": "30/30/2020",
        "c": "12345",
        "d": [1, 2, 3],
        "e": "string",
        "f": "true",
        "g": 12345,
        "h": "helloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworldhelloworld",
    },
    {
        "a": None,
        "b": None,
        "c": None,
        "d": None,
        "e": None,
        "f": None,
        "g": None,
        "i": None,
    },
]
