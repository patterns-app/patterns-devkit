from __future__ import annotations

from basis import FunctionContext
from basis.core.block import Block
from basis.core.module import BasisModule
from basis.utils.typing import T


def df1(ctx: FunctionContext) -> Block[T]:
    pass


module = BasisModule(
    namespace="_test_module", py_module_path=__file__, py_module_name=__name__,
)
# Shortcuts, for tooling and convenience
# namespace = module.namespace
# all_functions = module.functions
# all_schemas = module.schemas
