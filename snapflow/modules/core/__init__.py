from snapflow.core.module import SnapflowModule
from snapflow.schema.base import AnySchema

from .pipes import accumulator, dedupe, static

module = SnapflowModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[AnySchema, "schemas/core_test_type.yml"],
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
