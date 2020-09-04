from dags.core.module import DagsModule

from ...core.typing.object_type import ConflictBehavior, ObjectType
from .external.static import extract_csv, extract_dataframe
from .pipes.accumulate_as_dataset import (
    dataframe_accumulate_as_dataset,
    sql_accumulate_as_dataset,
)
from .pipes.accumulator import accumulator_test, dataframe_accumulator, sql_accumulator
from .pipes.as_dataset import as_dataset
from .pipes.dedupe import dedupe_test, dedupe_unique_keep_newest_row

AnyType = ObjectType(
    name="Any",
    module_key="core",
    version="0",
    description="Any super type is compatible with all ObjectTypes",
    on_conflict=ConflictBehavior.ReplaceWithNewer,
    unique_on=[],
    fields=[],
)

module = DagsModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=[AnyType, "otypes/core_test_type.yml"],
    pipes=[
        sql_accumulate_as_dataset,
        dataframe_accumulate_as_dataset,
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
