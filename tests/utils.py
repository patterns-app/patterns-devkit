from __future__ import annotations

from pandas import DataFrame

from dags.core.data_block import DataBlock
from dags.core.environment import Environment
from dags.core.graph import Graph
from dags.core.module import DagsModule
from dags.core.pipe import pipe_chain
from dags.core.runnable import ExecutionContext, ExecutionManager, PipeContext
from dags.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from dags.core.storage.storage import Storage, StorageClass, StorageEngine
from dags.core.typing.object_schema import create_quick_schema
from dags.utils.common import rand_str
from dags.utils.typing import T

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


def make_test_env(**kwargs):
    if "metadata_storage" not in kwargs:
        url = "sqlite://"
        metadata_storage = Storage.from_url(url)
        kwargs["metadata_storage"] = metadata_storage
    env = Environment(**kwargs)
    test_module = DagsModule(
        "_test",
        schemas=[TestSchema1, TestSchema2, TestSchema3, TestSchema4],
    )
    env.add_module(test_module)
    return env


def make_test_execution_context(**kwargs):
    s = Storage(  # type: ignore
        url=f"memory://_test_default_{rand_str(6)}",
        storage_class=StorageClass.MEMORY,
        storage_engine=StorageEngine.DICT,
    )
    env = make_test_env()
    g = Graph(env)
    args = dict(
        graph=g,
        env=env,
        runtimes=[
            Runtime(
                url="python://local",
                runtime_class=RuntimeClass.PYTHON,
                runtime_engine=RuntimeEngine.LOCAL,
            )
        ],
        storages=[s],
        local_memory_storage=s,
        target_storage=s,
        metadata_session=env.session,
    )
    args.update(**kwargs)
    return ExecutionContext(**args)


def make_test_execution_manager(**kwargs):
    return ExecutionManager(make_test_execution_context(**kwargs))


def pipe_t1_sink(ctx: PipeContext, input: DataBlock[TestSchema1]):
    pass


def pipe_t1_to_t2(input: DataBlock[TestSchema1]) -> DataFrame[TestSchema2]:
    pass


def pipe_generic(input: DataBlock[T]) -> DataFrame[T]:
    pass


def pipe_t1_source(ctx: PipeContext) -> DataFrame[TestSchema1]:
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


#
#
# def pipe_dataset_output(input: DataBlock[T]) -> DataSet[T]:
#     pass


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
