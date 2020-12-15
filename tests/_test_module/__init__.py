from __future__ import annotations

from snapflow.core.data_block import DataBlock
from snapflow.core.execution import PipeContext
from snapflow.core.module import SnapflowModule
from snapflow.utils.typing import T


def df1(ctx: PipeContext) -> DataBlock[T]:
    pass


module = SnapflowModule(
    "_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/test_schema.yml"],
    pipes=["test_sql.sql", df1],
)
module.export()
