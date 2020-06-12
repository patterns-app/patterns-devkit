from basis.core.module import BasisModule

from ...core.component import ComponentType
from ...core.typing.object_type import ConflictBehavior, ObjectType
from .dataset import *
from .external.static import local_provider

AnyType = ObjectType(
    component_type=ComponentType.ObjectType,
    name="Any",
    module_name="core",
    version="0",
    description="Any super type is compatible with all ObjectTypes",
    on_conflict=ConflictBehavior.ReplaceWithNewer,
    unique_on=[],
    fields=[],
)

module = BasisModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=[AnyType, "otypes/core_test_type.yml"],
    functions=[
        accumulate_as_dataset,
        as_dataset,
        dedupe_unique_keep_first_value,
        sql_accumulator,
        dataframe_accumulator,
    ],
    providers=[local_provider],
    tests=[accumulator_test, dedupe_test],
)
module.export()
