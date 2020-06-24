from __future__ import annotations

from typing import Type

import pandas as pd

from basis.core.data_formats.base import (
    DataFormatBase,
    MemoryDataFormatBase,
    ReusableGenerator,
)


class DataFrameGenerator(ReusableGenerator[pd.DataFrame]):
    pass


class DataFrameGeneratorFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls) -> Type:
        return DataFrameGenerator
