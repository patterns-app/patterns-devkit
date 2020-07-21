from __future__ import annotations

from basis.core.component import ComponentType
from basis.core.data_block import DataBlock
from basis.core.module import BasisModule
from basis.core.runnable import DataFunctionContext
from basis.utils.typing import T


def df1(ctx: DataFunctionContext) -> DataBlock[T]:
    pass


module = BasisModule(
    "_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=["otypes/test_type.yml"],
    functions=["test_sql.sql", df1],
)
module.export()
