from __future__ import annotations

from dataclasses import dataclass

from pandas import DataFrame
from snapflow.core.execution import SnapContext
from snapflow.core.snap import Param, Snap
from snapflow.schema.base import SchemaLike
from snapflow.storage.data_formats import DataFrameFormat, RecordsFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryDataRecords, as_records
from snapflow.storage.storage import Storage
from snapflow.utils.data import read_csv


@dataclass
class LocalExtractState:
    extracted: bool


@dataclass
class ExtractDataFrameConfig:
    dataframe: DataFrame
    schema: SchemaLike


@Snap(
    module="core", state_class=LocalExtractState,
)
@Param("dataframe", datatype="DataFrame")
@Param("schema", datatype="str")
def extract_dataframe(ctx: SnapContext) -> MemoryDataRecords:  # TODO optional
    extracted = ctx.get_state_value("extracted")
    if extracted:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_param("schema")
    df = ctx.get_param("dataframe")
    return as_records(df, data_format=DataFrameFormat, schema=schema)


@dataclass
class ExtractLocalCSVConfig:
    path: str
    schema: SchemaLike


# TODO: ExtractLocalCsv?
@Snap(
    module="core", state_class=LocalExtractState,
)
@Param("path", datatype="str")
@Param("schema", datatype="str", required=False)
def extract_csv(ctx: SnapContext) -> MemoryDataRecords:
    extracted = ctx.get_state_value("extracted")
    if extracted:
        return
        # Static resource, if already emitted, return
    path = ctx.get_param("path")
    f = open(path)
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_param("schema")
    return as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)


@Snap(
    module="core", state_class=LocalExtractState,
)
@Param("name", datatype="str")
@Param("storage_url", datatype="str")
@Param("schema", datatype="str", required=False)
def extract_storage_csv(ctx: SnapContext) -> MemoryDataRecords:
    extracted = ctx.get_state_value("extracted")
    if extracted:
        return
        # Static resource, if already emitted, return
    name = ctx.get_param("name")
    storage_url = ctx.get_param("storage_url")
    fs_api = Storage(storage_url).get_api()
    f = fs_api.open_name(name)
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_param("schema")
    return as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)
