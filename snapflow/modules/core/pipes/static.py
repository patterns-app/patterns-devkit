from __future__ import annotations

from dataclasses import dataclass

from pandas import DataFrame
from snapflow.core.execution import PipeContext
from snapflow.core.pipe import pipe
from snapflow.schema.base import SchemaLike
from snapflow.storage.data_formats import DataFrameFormat, RecordsFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.utils.data import read_csv


@dataclass
class LocalExtractState:
    extracted: bool


@dataclass
class ExtractDataFrameConfig:
    dataframe: DataFrame
    schema: SchemaLike


@pipe(
    "extract_dataframe",
    module="core",
    config_class=ExtractDataFrameConfig,
    state_class=LocalExtractState,
)
def extract_dataframe(ctx: PipeContext) -> MemoryDataRecords:  # TODO optional
    extracted = ctx.get_state_value("extracted")
    if extracted:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_config_value("schema")
    df = ctx.get_config_value("dataframe")
    return as_records(df, data_format=DataFrameFormat, schema=schema)


@dataclass
class ExtractLocalCSVConfig:
    path: str
    schema: SchemaLike


@pipe(
    "extract_csv",
    module="core",
    config_class=ExtractLocalCSVConfig,
    state_class=LocalExtractState,
)
def extract_csv(ctx: PipeContext) -> MemoryDataRecords:
    extracted = ctx.get_state_value("extracted")
    if extracted:
        return
        # Static resource, if already emitted, return
    path = ctx.get_config_value("path")
    f = open(path)
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_config_value("schema")
    return as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)
