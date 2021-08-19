from __future__ import annotations

from typing import Optional

from basis import FunctionContext, datafunction


@datafunction(namespace="core", display_name="Import from Storage")
def import_from_storage(
    ctx: FunctionContext,
    name: str,
    storage_url: str,
    data_format: Optional[str] = None,
    schema: Optional[str] = None,
    static: bool = True,
):
    """
    Params:
        name: The name of the object on the storage (e.g. file name, table name)
        storage_url: The url to the storage (e.g. postgresql://35.12....., gs://bucket-name/path)
        data_format: The format of the object (e.g. csv, table, jsonl)
        schema: An optional CommonModel Schema describing the data structure (e.g. StripeCharge or bi.Transaction)
        static: True if object won't change and should be imported just once
    """
    if static:
        imported = ctx.get_state_value("imported")
        # Static resource, if already emitted, return
        if imported:
            return
    ctx.emit(name=name, storage=storage_url, data_format=data_format, schema=schema)
    ctx.emit_state_value("imported", True)
