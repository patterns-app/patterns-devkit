from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, cast

import pandas as pd
from pandas import DataFrame
from snapflow.storage.data_formats.base import (
    MemoryDataFormatBase,
    make_corresponding_iterator_format,
)
from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.storage.data_records import MemoryDataRecords
    from snapflow.schema import SchemaTranslation, Schema


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
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        return obj.to_dict(orient="records")[:n]

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        # DataFrame is unambiguous
        return cls.maybe_instance(obj)

    @classmethod
    def conform_records_to_schema(cls, records: T, schema: Schema) -> T:
        from snapflow.core.typing.inference import conform_dataframe_to_schema

        return conform_dataframe_to_schema(records, schema)

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, df: DataFrame
    ) -> DataFrame:
        return df.rename(translation.as_dict(), axis=1)


DataFrameIteratorFormat = make_corresponding_iterator_format(DataFrameFormat)
DataFrameIterator = Iterator[DataFrame]
