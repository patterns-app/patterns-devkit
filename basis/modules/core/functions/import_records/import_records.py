from __future__ import annotations

import json
from typing import Optional

from basis import Context
from basis.core.function import function
from dcp.data_format.formats.memory.records import RecordsFormat


@function(
    namespace="core",
    display_name="Import Records (List of dicts)",
)
def import_records(ctx: Context, records: str, schema: Optional[str] = None):
    imported = ctx.get_state_value("imported")
    if imported:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("imported", True)
    if isinstance(records, str):
        records = json.loads(records)
    ctx.emit(records, data_format=RecordsFormat, schema=schema)
