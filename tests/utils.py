from __future__ import annotations

from pandas import DataFrame

from basis.core.data_block import DataBlock
from basis.core.environment import Environment
from basis.core.module import BasisModule
from basis.core.runnable import DataFunctionContext, ExecutionContext, ExecutionManager
from basis.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from basis.core.storage import Storage, StorageClass, StorageEngine
from basis.core.typing.object_type import create_quick_otype
from basis.utils.common import rand_str
from basis.utils.typing import T

TestType1 = create_quick_otype(
    "TestType1", [("f1", "Unicode(256)")], module_name="_test"
)
TestType2 = create_quick_otype(
    "TestType2", [("f1", "Unicode(256)")], module_name="_test"
)
TestType3 = create_quick_otype(
    "TestType3", [("f1", "Unicode(256)")], module_name="_test"
)
TestType4 = create_quick_otype(
    "TestType4",
    [("f1", "Unicode(256)"), ("f2", "Integer")],
    unique_on=["f1"],
    module_name="_test",
)


def make_test_env(**kwargs):
    if "metadata_storage" not in kwargs:
        url = "sqlite://"
        metadata_storage = Storage.from_url(url)
        kwargs["metadata_storage"] = metadata_storage
    env = Environment(initial_modules=[], **kwargs)
    test_module = BasisModule(
        "_test", otypes=[TestType1, TestType2, TestType3, TestType4],
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
    args = dict(
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
        metadata_session=env.get_new_metadata_session(),
    )
    args.update(**kwargs)
    return ExecutionContext(**args)


def make_test_execution_manager(**kwargs):
    return ExecutionManager(make_test_execution_context(**kwargs))


def df_t1_sink(ctx: DataFunctionContext, input: DataBlock[TestType1]):
    pass


def df_t1_to_t2(input: DataBlock[TestType1]) -> DataFrame[TestType2]:
    pass


def df_generic(input: DataBlock[T]) -> DataFrame[T]:
    pass


def df_t1_source(ctx: DataFunctionContext) -> DataFrame[TestType1]:
    pass
