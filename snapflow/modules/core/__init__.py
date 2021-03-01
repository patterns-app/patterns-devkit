from snapflow.core.module import SnapflowModule
from snapflow.modules.core.snaps import conform_to_schema
from snapflow.schema.base import AnySchema

from .snaps import accumulator, dedupe, static

module = SnapflowModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[AnySchema, "schemas/core_test_type.yml"],
    snaps=[
        conform_to_schema.dataframe_conform_to_schema,
        conform_to_schema.sql_conform_to_schema,
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
        conform_to_schema.test_conform,
    ],
)
module.export()
