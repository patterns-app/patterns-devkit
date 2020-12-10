from __future__ import annotations

from collections import abc
from typing import TYPE_CHECKING, Any, Generator, Iterator, Optional, Type

import pandas as pd
from pandas import DataFrame
from snapflow.core.data_formats import DataFrameFormat
from snapflow.core.data_formats.base import MemoryDataFormatBase, ReusableGenerator

if TYPE_CHECKING:
    from snapflow.core.data_block import LocalMemoryDataRecords
    from snapflow.core.typing.schema import SchemaTranslation, Schema


class DataFrameGenerator(ReusableGenerator[pd.DataFrame]):
    pass


class DataFrameGeneratorFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls) -> Type:
        return DataFrameGenerator

    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        return isinstance(obj, abc.Generator)

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        if isinstance(obj, cls.type()):
            return True
        if isinstance(obj, ReusableGenerator):
            one = obj.get_one()
            return DataFrameFormat.definitely_instance(one)
        return False

    @classmethod
    def infer_schema_from_records(cls, records: DataFrameGenerator) -> Schema:
        from snapflow.core.typing.inference import infer_schema_from_records_list
        from snapflow.core.data_formats import get_records_list_sample

        dl = get_records_list_sample(records)
        if dl is None:
            raise ValueError("Empty records object")
        inferred_schema = infer_schema_from_records_list(dl)
        return inferred_schema

    @classmethod
    def conform_records_to_schema(
        cls, records: DataFrameGenerator
    ) -> DataFrameGenerator:
        raise NotImplementedError

    @classmethod
    def apply_schema_translation(
        cls, translation: SchemaTranslation, dfg: DataFrameGenerator
    ) -> Iterator[DataFrame]:
        for df in dfg.get_generator():
            yield df.rename(translation.as_dict(), axis=1)
