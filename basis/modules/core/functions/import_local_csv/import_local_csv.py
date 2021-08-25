from __future__ import annotations

from typing import Optional

from basis import Context, function
from basis.utils.typing import T
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat


@function(namespace="core", display_name="Import local CSV")
def import_local_csv(ctx: Context, path: str, schema: Optional[str] = None):
    imported = ctx.get_state_value("imported")
    if imported:
        return
        # Static resource, if already emitted, return
    f = open(path)
    ctx.emit(f, data_format=CsvFileFormat, schema=schema)
    ctx.emit_state_value("imported", True)
