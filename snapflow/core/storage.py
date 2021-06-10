from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional, Type

from dcp import StorageFormat
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import execute_copy_request, get_copy_path
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage
from loguru import logger
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.environment import Environment
from snapflow.core.persistence.data_block import get_stored_datablock_id, make_sdb_name
from snapflow.core.persistence.pydantic import (
    DataBlockMetadataCfg,
    StoredDataBlockMetadataCfg,
)
from sqlalchemy import or_
from sqlalchemy.sql.expression import select


def copy_sdb_cfg(
    request: CopyRequest,
    in_sdb: StoredDataBlockMetadataCfg,
    out_sdb: StoredDataBlockMetadataCfg,
    # target_storage: Storage,
    # storages: Optional[List[Storage]] = None,
    create_intermediate_sdbs: bool = True,
) -> List[StoredDataBlockMetadataCfg]:
    stored_blocks = []
    result = execute_copy_request(request)
    if create_intermediate_sdbs:
        for name, storage, fmt in result.intermediate_created:
            sid = get_stored_datablock_id()
            i_sdb = StoredDataBlockMetadataCfg(  # type: ignore
                id=sid,
                name=make_sdb_name(sid, in_sdb.data_block.created_by_node_key),
                data_block=in_sdb.data_block,
                data_format=fmt,
                storage_url=storage.url,
                data_is_written=True,
            )
            storage.get_api().create_alias(name, i_sdb.name)
            stored_blocks.append(i_sdb)
    return stored_blocks


def ensure_data_block_on_storage_cfg(
    block: DataBlockMetadataCfg,
    storage: Storage,
    stored_blocks: List[StoredDataBlockMetadataCfg],
    eligible_storages: List[Storage],
    fmt: Optional[DataFormat] = None,
    library: ComponentLibrary = global_library,
) -> List[StoredDataBlockMetadataCfg]:
    sdbs = stored_blocks
    match = [s for s in sdbs if s.storage.url == storage.url]
    if fmt:
        match = [s for s in match if s.data_format == fmt]
    if match:
        return [match[0]]

    fmt = fmt or storage.storage_engine.get_natural_format()
    target_storage_format = StorageFormat(storage.storage_engine, fmt)

    # Compute conversion costs
    eligible_conversion_paths = (
        []
    )  #: List[List[Tuple[ConversionCostLevel, Type[Converter]]]] = []
    existing_sdbs = sdbs
    for sdb in existing_sdbs:
        assert sdb.name is not None
        req = CopyRequest(
            from_name=sdb.name,
            from_storage=sdb.storage,
            to_name="placeholder",
            to_storage=storage,
            to_format=fmt,
            schema=library.get_schema(block.realized_schema_key),
            available_storages=eligible_storages,
        )
        pth = get_copy_path(req)
        if pth is not None:
            eligible_conversion_paths.append((pth.total_cost, pth, sdb, req))
    if not eligible_conversion_paths:
        raise NotImplementedError(
            f"No copy path to {target_storage_format} for existing StoredDataBlocks {existing_sdbs}"
        )
    cost, pth, in_sdb, req = min(eligible_conversion_paths, key=lambda x: x[0])
    sid = get_stored_datablock_id()
    out_sdb = StoredDataBlockMetadataCfg(  # type: ignore
        id=sid,
        name=make_sdb_name(sid, block.created_by_node_key),
        data_block_id=block.id,
        data_block=block,
        data_format=fmt.nickname,
        storage_url=storage.url,
        data_is_written=True,
    )
    req.to_name = out_sdb.name
    created_sdbs = copy_sdb_cfg(
        request=req,
        in_sdb=in_sdb,
        out_sdb=out_sdb,
    )
    return [out_sdb] + created_sdbs
