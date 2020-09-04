from __future__ import annotations

from dags.core.data_block import DataBlock
from dags.core.module import DagsModule
from dags.core.runnable import PipeContext
from dags.utils.typing import T


def df1(ctx: PipeContext) -> DataBlock[T]:
    pass


module = DagsModule(
    "_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=["otypes/test_type.yml"],
    pipes=["test_sql.sql", df1],
)
module.export()
