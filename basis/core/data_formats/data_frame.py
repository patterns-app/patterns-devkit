from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from basis.core.data_formats.base import MemoryDataFormatBase


class DataFrameFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return pd.DataFrame

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return len(obj)
