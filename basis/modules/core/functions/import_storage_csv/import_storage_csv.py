from __future__ import annotations

from typing import Optional

from basis import DataFunctionContext, datafunction
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.storage.base import Storage


@datafunction(namespace="core", display_name="Import CSV from Storage")
def import_storage_csv(
    ctx: DataFunctionContext, name: str, storage_url: str, schema: Optional[str] = None
):
    """DEPRECATED: Use import_from_storage instead"""
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    ctx.emit_state_value("imported", True)
    ctx.emit(name=name, storage=storage_url, data_format=CsvFileFormat, schema=schema)
