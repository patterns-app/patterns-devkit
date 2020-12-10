from __future__ import annotations

import typing
from io import BytesIO
from itertools import tee
from typing import Any, Dict, Generic, List, Optional, Type

import pandas as pd
from snapflow.core.data_formats.base import DataFormatBase, MemoryDataFormatBase
from snapflow.utils.typing import T
from sqlalchemy.engine import ResultProxy


class FilePointer:
    def __init__(self, file_like: BytesIO):
        self.file_like = file_like


class DelimitedFilePointer(FilePointer):
    pass


class DelimitedFilePointerFormat(MemoryDataFormatBase):
    @classmethod
    def type(cls):
        return DelimitedFilePointer
