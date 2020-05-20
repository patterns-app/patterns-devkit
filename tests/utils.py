from __future__ import annotations

from pandas import DataFrame

from basis.core.data_resource import DataResource
from basis.core.environment import Environment
from basis.core.module import BasisModule
from basis.core.object_type import create_quick_otype
from basis.core.runnable import DataFunctionContext, ExecutionContext, ExecutionManager
from basis.core.runtime_resource import RuntimeClass, RuntimeEngine, RuntimeResource
from basis.core.storage_resource import StorageClass, StorageEngine, StorageResource
from basis.utils.common import rand_str
from basis.utils.registry import T

TestType1 = create_quick_otype(
    "TestType1", [("f1", "Unicode(256)")], module_key="_test"
)
TestType2 = create_quick_otype(
    "TestType2", [("f1", "Unicode(256)")], module_key="_test"
)
TestType3 = create_quick_otype(
    "TestType3", [("f1", "Unicode(256)")], module_key="_test"
)
TestType4 = create_quick_otype(
    "TestType4", [("f1", "Unicode(256)")], module_key="_test"
)


def make_test_env(**kwargs):
    if "metadata_storage_resource" not in kwargs:
        url = "sqlite://"
        metadata_storage_resource = StorageResource.from_url(url)
        kwargs["metadata_storage_resource"] = metadata_storage_resource
    env = Environment(**kwargs)
    test_module = BasisModule(
        "_test", otypes=[TestType1, TestType2, TestType3, TestType4],
    )
    env.add_module(test_module)
    return env


def make_test_execution_context(**kwargs):
    s = StorageResource(  # type: ignore
        url=f"memory://_test_default_{rand_str(6)}",
        storage_class=StorageClass.MEMORY,
        storage_engine=StorageEngine.DICT,
    )
    env = make_test_env()
    args = dict(
        env=env,
        runtime_resources=[
            RuntimeResource(
                url="python://local",
                runtime_class=RuntimeClass.PYTHON,
                runtime_engine=RuntimeEngine.LOCAL,
            )
        ],
        storage_resources=[s],
        local_memory_storage=s,
        target_storage=s,
        metadata_session=env.get_new_metadata_session(),
    )
    args.update(**kwargs)
    return ExecutionContext(**args)


def make_test_execution_manager(**kwargs):
    return ExecutionManager(make_test_execution_context(**kwargs))


def df_t1_sink(ctx: DataFunctionContext, dr: DataResource[TestType1]):
    pass


def df_t1_to_t2(dr: DataResource[TestType1]) -> DataFrame[TestType2]:
    pass


def df_generic(dr: DataResource[T]) -> DataFrame[T]:
    pass


def df_t1_source(ctx: DataFunctionContext) -> DataFrame[TestType1]:
    pass
