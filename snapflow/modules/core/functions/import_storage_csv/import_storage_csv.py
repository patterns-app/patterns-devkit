from __future__ import annotations

from typing import Optional

from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.storage.base import Storage
from snapflow import DataFunctionContext, datafunction


@datafunction(namespace="core", display_name="Import CSV from Storage")
def import_storage_csv(
    ctx: DataFunctionContext, name: str, storage_url: str, schema: Optional[str] = None
):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    fs_api = Storage(storage_url).get_api()
    f = fs_api.open_name(name)
    ctx.emit_state_value("imported", True)
    ctx.emit(f, data_format=CsvFileFormat, schema=schema)
