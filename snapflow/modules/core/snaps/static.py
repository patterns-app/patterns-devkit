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
class LocalImportState:
    imported: bool


@Snap(
    module="core",
    state_class=LocalImportState,
    display_name="Import Pandas DataFrame",
)
@Param("dataframe", datatype="DataFrame")
@Param("schema", datatype="str")
def import_dataframe(ctx: SnapContext) -> MemoryDataRecords:  # TODO optional
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    df = ctx.get_param("dataframe")
    return as_records(df, data_format=DataFrameFormat, schema=schema)


@Snap(module="core", state_class=LocalImportState, display_name="Import local CSV")
@Param("path", datatype="str")
@Param("schema", datatype="str", required=False)
def import_local_csv(ctx: SnapContext) -> MemoryDataRecords:
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    path = ctx.get_param("path")
    f = open(path)
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    return as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)


@Snap(
    module="core", state_class=LocalImportState, display_name="Import CSV from Storage"
)
@Param("name", datatype="str")
@Param("storage_url", datatype="str")
@Param("schema", datatype="str", required=False)
def import_storage_csv(ctx: SnapContext) -> MemoryDataRecords:
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    name = ctx.get_param("name")
    storage_url = ctx.get_param("storage_url")
    fs_api = Storage(storage_url).get_api()
    f = fs_api.open_name(name)
    ctx.emit_state_value("imported", True)
    schema = ctx.get_param("schema")
    return as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)
