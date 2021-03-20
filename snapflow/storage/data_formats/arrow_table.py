from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, cast

import pyarrow as pa
from pandas import DataFrame
from snapflow.storage.data_formats.base import (
    MemoryDataFormatBase,
    make_corresponding_iterator_format,
)
from snapflow.utils.typing import T

if TYPE_CHECKING:
    from snapflow.storage.data_records import MemoryDataRecords
    from snapflow.schema import SchemaTranslation, Schema


ArrowTable = pa.Table


class ArrowTableFormat(MemoryDataFormatBase[DataFrame]):
    @classmethod
    def type(cls):
        return ArrowTable

    @classmethod
    def get_record_count(cls, obj: Any) -> Optional[int]:
        if obj is None:
            return None
        return obj.num_rows

    @classmethod
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        from snapflow.storage.data_formats.data_frame import DataFrameFormat

        # TODO: zero copy?
        return DataFrameFormat.get_records_sample(obj.to_pandas(), n)

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        # Arrow Table is unambiguous
        return cls.maybe_instance(obj)

    # TODO
    # @classmethod
    # def infer_schema_from_records(cls, records: ArrowTable) -> Schema:
    #     from snapflow.core.typing.inference import infer_schema_from_dataframe

    #     inferred_schema = infer_schema_from_arrow_table(records)
    #     return inferred_schema

    @classmethod
    def conform_records_to_schema(
        cls, records: ArrowTable, schema: Schema
    ) -> ArrowTable:
        from snapflow.core.typing.inference import conform_arrow_to_schema

        return conform_arrow_to_schema(records, schema)

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, t: ArrowTable
    ) -> pa.Table:
        td = translation.as_dict()
        return t.rename_columns([td.get(f.name, f.name) for f in t.schema])


ArrowTableIteratorFormat = make_corresponding_iterator_format(ArrowTableFormat)
ArrowTableIterator = Iterator[ArrowTable]
