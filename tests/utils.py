from __future__ import annotations

from pandas import DataFrame
from snapflow.core.data_block import DataBlock
from snapflow.core.environment import Environment
from snapflow.core.execution import ExecutionManager, PipeContext, RunContext
from snapflow.core.graph import Graph
from snapflow.core.module import SnapflowModule
from snapflow.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from snapflow.core.streams import DataBlockStream
from snapflow.schema.base import create_quick_schema
from snapflow.storage.db.utils import get_tmp_sqlite_db_url
from snapflow.storage.storage import Storage, StorageClass, StorageEngine
from snapflow.utils.common import rand_str
from snapflow.utils.typing import T

TestSchema1 = create_quick_schema(
    "TestSchema1", [("f1", "Unicode(256)")], module_name="_test"
)
TestSchema2 = create_quick_schema(
    "TestSchema2", [("f1", "Unicode(256)")], module_name="_test"
)
TestSchema3 = create_quick_schema(
    "TestSchema3", [("f1", "Unicode(256)")], module_name="_test"
)
TestSchema4 = create_quick_schema(
    "TestSchema4",
    [("f1", "Unicode(256)"), ("f2", "Integer")],
    unique_on=["f1"],
    module_name="_test",
)


def make_test_env(**kwargs) -> Environment:
    if "metadata_storage" not in kwargs:
        url = get_tmp_sqlite_db_url()
        metadata_storage = Storage.from_url(url)
        kwargs["metadata_storage"] = metadata_storage
    env = Environment(**kwargs)
    test_module = SnapflowModule(
        "_test", schemas=[TestSchema1, TestSchema2, TestSchema3, TestSchema4],
    )
    env.add_module(test_module)
    return env


def make_test_run_context(**kwargs) -> RunContext:
    s = Storage.from_url(url=f"python://_test_default_{rand_str(6)}",)
    env = make_test_env()
    g = Graph(env)
    args = dict(
        graph=g,
        env=env,
        runtimes=[Runtime.from_storage(s)],
        storages=[s],
        local_python_storage=s,
        target_storage=s,
    )
    args.update(**kwargs)
    return RunContext(**args)


def make_test_execution_manager(**kwargs) -> ExecutionManager:
    return ExecutionManager(make_test_run_context(**kwargs))


def pipe_t1_sink(ctx: PipeContext, input: DataBlock[TestSchema1]):
    pass


def pipe_t1_to_t2(input: DataBlock[TestSchema1]) -> DataFrame[TestSchema2]:
    pass


def pipe_generic(input: DataBlock[T]) -> DataFrame[T]:
    pass


def pipe_t1_source(ctx: PipeContext) -> DataFrame[TestSchema1]:
    pass


def pipe_stream(input: DataBlockStream) -> DataBlock:
    pass


pipe_chain_t1_to_t2 = (
    pipe_t1_to_t2  # pipe_chain("pipe_chain_t1_to_t2", [pipe_t1_to_t2, pipe_generic])
)


def pipe_self(input: DataBlock[T], this: DataBlock[T] = None) -> DataFrame[T]:
    pass


def pipe_multiple_input(
    input: DataBlock[T], other_t2: DataBlock[TestSchema2] = None
) -> DataFrame[T]:
    pass


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
