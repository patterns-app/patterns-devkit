from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from pandas import DataFrame

from dags.core.data_formats.base import MemoryDataFormatBase
from dags.utils.typing import T

if TYPE_CHECKING:
    from dags import ObjectSchema


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
    def infer_schema_from_records(cls, records: DataFrame) -> ObjectSchema:
        from dags.core.typing.inference import infer_schema_from_records_list
        from dags.core.data_formats import get_records_list_sample

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")
        inferred_schema = infer_schema_from_records_list(dl)
        return inferred_schema

    @classmethod
    def conform_records_to_schema(cls, records: T) -> T:
        raise NotImplementedError
