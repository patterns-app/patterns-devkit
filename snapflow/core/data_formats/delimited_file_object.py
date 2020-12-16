from __future__ import annotations

import json
from io import IOBase
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
from loguru import logger
from snapflow.core.data_formats.base import (
    DataFormatBase,
    MemoryDataFormatBase,
    make_corresponding_iterator_format,
)
from snapflow.utils.data import SampleableIO, head, read_csv, read_json
from snapflow.utils.typing import T
from sqlalchemy.engine import ResultProxy


class FileObject(IOBase):
    pass


class DelimitedFileObject(FileObject):
    pass


class DelimitedFileObjectFormat(MemoryDataFormatBase):
    # TODO: How to make delimiter configurable? Right now only works with CSVs...
    @classmethod
    def type(cls):
        return DelimitedFileObject

    @classmethod
    def head(cls, obj: Any, n: int = 200) -> Optional[Iterator]:
        if isinstance(obj, SampleableIO):
            sample = obj.head(n)
        else:
            try:
                sample = head(obj, n)
            except Exception:
                logger.debug("Non-sampleable IO")
                return None
        return sample

    @classmethod
    def get_records_sample(cls, obj: Any, n: int = 200) -> Optional[List[Dict]]:
        sample = cls.head(obj, n)
        if sample is None:
            return None
        return read_csv(sample)

    @classmethod
    def maybe_instance(cls, obj: Any) -> bool:
        return isinstance(obj, IOBase)

    @classmethod
    def definitely_instance(cls, obj: Any) -> bool:
        try:
            cls.get_records_sample(obj, 2)
            try:
                # Bit of a hack, but check it is not JSON
                # (presumes this format knows about all other possible conflicting formats...)
                read_json(next(cls.head(obj, 1)))
                return False
            except Exception:
                return True
        except Exception:
            return False


DelimitedFileObjectIteratorFormat = make_corresponding_iterator_format(
    DelimitedFileObjectFormat
)
DelimitedFileObjectIterator = Iterator[DelimitedFileObject]
