from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

import pandas as pd
from pandas import DataFrame
from snapflow.core.data_formats.base import MemoryDataFormatBase
from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.core.data_block import LocalMemoryDataRecords
    from snapflow.core.typing.schema import SchemaTranslation, Schema


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
    def infer_schema_from_records(cls, records: DataFrame) -> Schema:
        from snapflow.core.typing.inference import infer_schema_from_records_list
        from snapflow.core.data_formats import get_records_list_sample

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")
        inferred_schema = infer_schema_from_records_list(dl)
        return inferred_schema

    @classmethod
    def conform_records_to_schema(cls, records: T) -> T:
        raise NotImplementedError

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, df: DataFrame
    ) -> DataFrame:
        return df.rename(translation.as_dict(), axis=1)
