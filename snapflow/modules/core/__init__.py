from snapflow.core.module import SnapflowModule

from ...core.typing.schema import ConflictBehavior, Schema
from .pipes import accumulator, dedupe, static

AnyType = Schema(
    name="Any",
    module_name="core",
    version="0",
    description="The Any super-type is compatible with all Schemas",
    on_conflict=ConflictBehavior.ReplaceWithNewer,
    unique_on=[],
    fields=[],
)

module = SnapflowModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[AnyType, "schemas/core_test_type.yml"],
    pipes=[
        dedupe.sql_dedupe_unique_keep_newest_row,
        dedupe.dataframe_dedupe_unique_keep_newest_row,
        accumulator.sql_accumulator,
        accumulator.dataframe_accumulator,
        static.extract_dataframe,
        static.extract_csv,
    ],
    tests=[
        accumulator.test_accumulator,
        dedupe.test_dedupe,
    ],
)
module.export()
