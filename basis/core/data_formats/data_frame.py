from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from pandas import DataFrame

from basis import ObjectType
from basis.core.data_formats import get_records_list_sample
from basis.core.data_formats.base import MemoryDataFormatBase
from basis.utils.typing import T


class DataFrameFormat(MemoryDataFormatBase[DataFrame]):
    @classmethod
    def type(cls):
        return pd.DataFrame

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return len(obj)

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        # DataFrame is unambiguous
        return cls.maybe_instance(obj)

    @classmethod
    def infer_otype_from_records(cls, records: DataFrame) -> ObjectType:
        from basis.core.typing.inference import infer_otype_from_records_list

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")  # TODO
        inferred_otype = infer_otype_from_records_list(dl)
        return inferred_otype

    @classmethod
    def conform_records_to_otype(cls, records: T) -> T:
        raise NotImplementedError
