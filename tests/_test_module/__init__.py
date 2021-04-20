from __future__ import annotations

from snapflow.core.data_block import DataBlock
from snapflow.core.execution import FunctionContext
from snapflow.core.module import SnapflowModule
from snapflow.utils.typing import T


def df1(ctx: FunctionContext) -> DataBlock[T]:
    pass


module = SnapflowModule(
    namespace="_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
)
# Shortcuts, for tooling and convenience
# namespace = module.namespace
# all_functions = module.functions
# all_schemas = module.schemas
