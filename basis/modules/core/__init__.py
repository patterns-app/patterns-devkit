from basis.core.module import BasisModule

from ...core.component import ComponentType
from ...core.typing.object_type import ConflictBehavior, ObjectType
from .external.static import extract_csv, extract_dataframe
from .functions.accumulate_as_dataset import accumulate_as_dataset
from .functions.accumulator import (
    accumulator_test,
    dataframe_accumulator,
    sql_accumulator,
)
from .functions.as_dataset import as_dataset
from .functions.dedupe import dedupe_test, dedupe_unique_keep_newest_row

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
        dedupe_unique_keep_newest_row,
        sql_accumulator,
        dataframe_accumulator,
        extract_dataframe,
        extract_csv,
    ],
    tests=[accumulator_test, dedupe_test],
)
module.export()
