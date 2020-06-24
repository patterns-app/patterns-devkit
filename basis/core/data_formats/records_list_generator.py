from __future__ import annotations

from typing import Any, Dict, Generic, List, Optional, Type

from basis.core.data_formats.base import (
    DataFormatBase,
    MemoryDataFormatBase,
    ReusableGenerator,
)
from basis.core.data_formats.records_list import RecordsList


class RecordsListGenerator(ReusableGenerator[RecordsList]):
    pass


class RecordsListGeneratorFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls) -> Type:
        return RecordsListGenerator
